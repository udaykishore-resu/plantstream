// Package collectors turns field devices into streams of tag readings. A
// Source knows how to read one device; the Poller schedules reads, tracks
// per-source health and hands readings to the pipeline.
package collectors

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/udaykishore-resu/plantstream/internal/domain/asset"
)

// Reading is one raw (unscaled) sample of one tag.
type Reading struct {
	Tag   string
	Value float64
	At    time.Time
}

// Source reads a set of tags from one device or file.
type Source interface {
	Name() string
	// Read performs one poll cycle. An error means the whole cycle failed
	// (communication error); partial results are returned with nil error.
	Read(ctx context.Context) ([]Reading, error)
	Close() error
}

// New builds a Source from its definition and the tags assigned to it.
func New(src *asset.Source, tags []asset.TagRef, log *slog.Logger) (Source, error) {
	if len(tags) == 0 {
		return nil, fmt.Errorf("collectors: source %q has no tags", src.Name)
	}
	switch src.Type {
	case asset.SourceModbus:
		return NewModbusSource(src, tags, log)
	case asset.SourceSim:
		return NewSimSource(src, tags)
	case asset.SourceCSV:
		return NewCSVSource(src, tags)
	default:
		return nil, fmt.Errorf("collectors: unsupported source type %q", src.Type)
	}
}

// Status is a point-in-time health snapshot of a source.
type Status struct {
	Name                string        `json:"name"`
	Type                string        `json:"type"`
	Up                  bool          `json:"up"`
	Polls               int64         `json:"polls"`
	Failures            int64         `json:"failures"`
	ConsecutiveFailures int           `json:"consecutive_failures"`
	LastSuccess         *time.Time    `json:"last_success,omitempty"`
	LastError           string        `json:"last_error,omitempty"`
	LastErrorAt         *time.Time    `json:"last_error_at,omitempty"`
	LastLatency         time.Duration `json:"-"`
	LastLatencyMS       float64       `json:"last_latency_ms"`
	Readings            int64         `json:"readings"`
}

// Health tracks per-source status; safe for concurrent use.
type Health struct {
	mu     sync.RWMutex
	status map[string]*Status
	order  []string
}

// NewHealth creates an empty registry.
func NewHealth() *Health { return &Health{status: map[string]*Status{}} }

// Register declares a source (initially down until its first success).
func (h *Health) Register(name, typ string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.status[name]; !ok {
		h.order = append(h.order, name)
	}
	h.status[name] = &Status{Name: name, Type: typ}
}

func (h *Health) success(name string, at time.Time, latency time.Duration, readings int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.status[name]
	if s == nil {
		return
	}
	s.Up = true
	s.Polls++
	s.ConsecutiveFailures = 0
	t := at
	s.LastSuccess = &t
	s.LastLatency = latency
	s.LastLatencyMS = float64(latency.Microseconds()) / 1000
	s.Readings += int64(readings)
}

func (h *Health) failure(name string, at time.Time, latency time.Duration, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.status[name]
	if s == nil {
		return
	}
	s.Up = false
	s.Polls++
	s.Failures++
	s.ConsecutiveFailures++
	s.LastError = err.Error()
	t := at
	s.LastErrorAt = &t
	s.LastLatency = latency
	s.LastLatencyMS = float64(latency.Microseconds()) / 1000
}

// Snapshot returns a copy of every status in registration order.
func (h *Health) Snapshot() []Status {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]Status, 0, len(h.order))
	for _, n := range h.order {
		s := *h.status[n]
		out = append(out, s)
	}
	return out
}

// Get returns one source's status.
func (h *Health) Get(name string) (Status, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	s, ok := h.status[name]
	if !ok {
		return Status{}, false
	}
	return *s, true
}

// AllUp reports whether every registered source has succeeded at least once
// and is currently up.
func (h *Health) AllUp() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, s := range h.status {
		if !s.Up {
			return false
		}
	}
	return true
}

// Observer receives poll outcomes from a Poller.
type Observer interface {
	// OnReadings is called after every successful poll cycle.
	OnReadings(ctx context.Context, source string, readings []Reading)
	// OnError is called after a failed poll cycle.
	OnError(ctx context.Context, source string, err error)
}

// Poller drives one Source at a fixed interval.
type Poller struct {
	src      Source
	interval time.Duration
	health   *Health
	obs      Observer
	log      *slog.Logger
	clock    func() time.Time
}

// NewPoller creates a poller. interval must be > 0.
func NewPoller(src Source, interval time.Duration, health *Health, obs Observer, log *slog.Logger) *Poller {
	if interval <= 0 {
		interval = asset.DefaultPollInterval
	}
	if log == nil {
		log = slog.Default()
	}
	return &Poller{src: src, interval: interval, health: health, obs: obs, log: log, clock: time.Now}
}

// Run polls until ctx is cancelled, then closes the source. The first poll
// happens immediately.
func (p *Poller) Run(ctx context.Context) error {
	defer func() {
		if err := p.src.Close(); err != nil {
			p.log.Warn("source close failed", "source", p.src.Name(), "err", err)
		}
	}()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	p.pollOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.pollOnce(ctx)
		}
	}
}

// PollOnce performs a single poll cycle (exported for tests and one-shot tools).
func (p *Poller) PollOnce(ctx context.Context) { p.pollOnce(ctx) }

func (p *Poller) pollOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	start := p.clock()
	cctx, cancel := context.WithTimeout(ctx, p.interval*2+time.Second)
	readings, err := p.src.Read(cctx)
	cancel()
	latency := p.clock().Sub(start)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		p.health.failure(p.src.Name(), start, latency, err)
		p.log.Warn("poll failed", "source", p.src.Name(), "err", err, "latency", latency)
		p.obs.OnError(ctx, p.src.Name(), err)
		return
	}
	p.health.success(p.src.Name(), start, latency, len(readings))
	p.obs.OnReadings(ctx, p.src.Name(), readings)
}
