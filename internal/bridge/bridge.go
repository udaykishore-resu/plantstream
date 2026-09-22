// Package bridge forwards UNS messages from the edge broker to a cloud sink
// (Kafka) with store-and-forward and per-topic backpressure.
//
// Flow:
//
//	broker ──subscribe──▶ inbox (per-topic bounded FIFOs, round-robin) ──▶ forwarder ──▶ sink
//	                                                                          │
//	                                            sink down: spill in order ──▶ queue (bounded, on disk)
//	                                                                          │
//	                                            sink back: replay queue first ◀┘
//
// Two independent protections:
//
//   - Backpressure (memory): each topic gets a bounded FIFO; a hot topic that
//     overruns it loses its *oldest* buffered sample (latest value wins) and
//     the drop is counted. The forwarder dequeues topics round-robin so no
//     topic can starve or reorder another.
//   - Store-and-forward (disk): while the sink is down every message is
//     appended to the durable queue in arrival order; on recovery the queue
//     is replayed before live traffic resumes, so per-topic order is kept.
package bridge

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/udaykishore-resu/plantstream/internal/observability"
	"github.com/udaykishore-resu/plantstream/internal/ports"
)

// Config tunes the bridge.
type Config struct {
	// Filter selects which topics are bridged (default "#").
	Filter string
	// InboxSize is the total in-memory buffer (default 4096 messages).
	InboxSize int
	// PerTopicMax caps in-memory messages per topic (default 64).
	PerTopicMax int
	// RetryMin/RetryMax bound the exponential backoff while the sink is down.
	RetryMin time.Duration
	RetryMax time.Duration
	// ReplayBatch is how many queued messages are replayed per iteration
	// before the inbox is serviced again (default 256).
	ReplayBatch int
}

func (c *Config) defaults() {
	if c.Filter == "" {
		c.Filter = "#"
	}
	if c.InboxSize <= 0 {
		c.InboxSize = 4096
	}
	if c.PerTopicMax <= 0 {
		c.PerTopicMax = 64
	}
	if c.PerTopicMax > c.InboxSize {
		c.PerTopicMax = c.InboxSize
	}
	if c.RetryMin <= 0 {
		c.RetryMin = 200 * time.Millisecond
	}
	if c.RetryMax <= 0 || c.RetryMax < c.RetryMin {
		c.RetryMax = 10 * time.Second
	}
	if c.ReplayBatch <= 0 {
		c.ReplayBatch = 256
	}
}

// Bridge is the edge→cloud forwarder.
type Bridge struct {
	cfg     Config
	broker  ports.Broker
	sink    ports.Sink
	queue   ports.Queue
	metrics *observability.Metrics
	log     *slog.Logger
	in      *inbox

	sinkUp    atomic.Bool
	forwarded atomic.Int64
	spilled   atomic.Int64
	started   chan struct{}
}

// New creates a bridge.
func New(cfg Config, broker ports.Broker, sink ports.Sink, queue ports.Queue, metrics *observability.Metrics, log *slog.Logger) *Bridge {
	cfg.defaults()
	if log == nil {
		log = slog.Default()
	}
	if metrics == nil {
		metrics = observability.NewMetrics()
	}
	b := &Bridge{
		cfg: cfg, broker: broker, sink: sink, queue: queue, metrics: metrics, log: log,
		in:      newInbox(cfg.PerTopicMax, cfg.InboxSize),
		started: make(chan struct{}),
	}
	b.sinkUp.Store(true)
	return b
}

// Started is closed once the bridge has subscribed to the broker.
func (b *Bridge) Started() <-chan struct{} { return b.started }

// Stats is a snapshot for the API and tests.
type Stats struct {
	SinkUp    bool  `json:"sink_up"`
	Forwarded int64 `json:"forwarded"`
	Spilled   int64 `json:"spilled"`
	// Coalesced counts in-memory drops caused by per-topic backpressure.
	Coalesced int64 `json:"coalesced"`
	// Dropped counts messages evicted from the bounded disk queue.
	Dropped    int64 `json:"dropped"`
	QueueDepth int   `json:"queue_depth"`
	QueueBytes int64 `json:"queue_bytes"`
	Inbox      int   `json:"inbox"`
}

// Stats returns current counters.
func (b *Bridge) Stats() Stats {
	return Stats{
		SinkUp: b.sinkUp.Load(), Forwarded: b.forwarded.Load(), Spilled: b.spilled.Load(),
		Coalesced: b.in.droppedCount(), Dropped: b.queue.Dropped(),
		QueueDepth: b.queue.Len(), QueueBytes: b.queue.Bytes(), Inbox: b.in.len(),
	}
}

// Ready reports whether the bridge can currently deliver to the cloud.
func (b *Bridge) Ready() bool { return b.sinkUp.Load() }

// Run subscribes and forwards until ctx is cancelled. On shutdown, anything
// still in memory is persisted to the queue so nothing is lost.
func (b *Bridge) Run(ctx context.Context) error {
	unsub, err := b.broker.Subscribe(ctx, b.cfg.Filter, b.onMessage)
	if err != nil {
		return err
	}
	defer unsub()
	close(b.started)
	b.log.Info("bridge started", "filter", b.cfg.Filter, "queued", b.queue.Len())

	backoff := b.cfg.RetryMin
	timer := time.NewTimer(b.cfg.RetryMax)
	defer timer.Stop()
	for {
		// Replay before touching the inbox so per-topic ordering is preserved.
		if b.queue.Len() > 0 && b.sinkUp.Load() {
			if !b.replay(ctx) {
				backoff = b.fail(backoff, timer)
			} else if b.queue.Len() > 0 {
				if ctx.Err() != nil {
					b.drain()
					return nil
				}
				continue // keep draining, one batch at a time
			}
		}
		select {
		case <-ctx.Done():
			b.drain()
			return nil
		case <-timer.C:
			if !b.sinkUp.Load() {
				if b.queue.Len() == 0 {
					// Nothing to probe with; the next live message will.
					b.sinkUp.Store(true)
					b.metrics.BridgeSinkUp.Set(1)
				} else if b.replay(ctx) {
					backoff = b.cfg.RetryMin
				} else {
					backoff = b.fail(backoff, timer)
					continue
				}
			}
			timer.Reset(b.cfg.RetryMax)
		case <-b.in.notify:
			for {
				m, ok := b.in.pop()
				if !ok {
					break
				}
				if !b.sinkUp.Load() || b.queue.Len() > 0 {
					b.spill(m, "sink_down")
					continue
				}
				if err := b.send(ctx, m); err != nil {
					b.spill(m, "sink_down")
					if ctx.Err() != nil {
						b.drain()
						return nil
					}
					backoff = b.fail(b.cfg.RetryMin, timer)
				}
			}
		}
	}
}

// onMessage is the broker callback.
func (b *Bridge) onMessage(_ context.Context, m ports.Message) {
	if dropped := b.in.push(m); dropped > 0 {
		b.metrics.BridgeCoalesced.Add(float64(dropped))
	}
}

func (b *Bridge) spill(m ports.Message, reason string) {
	if err := b.queue.Push(m); err != nil {
		b.log.Error("store-and-forward push failed; message lost", "topic", m.Topic, "err", err)
		b.metrics.BridgeDropped.Inc()
		return
	}
	b.spilled.Add(1)
	b.metrics.BridgeSpilled.WithLabelValues(reason).Inc()
	b.updateQueueGauges()
}

func (b *Bridge) send(ctx context.Context, m ports.Message) error {
	if err := b.sink.Send(ctx, m); err != nil {
		b.metrics.BridgeErrors.Inc()
		return err
	}
	b.forwarded.Add(1)
	b.metrics.BridgeForwarded.Inc()
	if !m.At.IsZero() {
		b.metrics.BridgeLag.Observe(time.Since(m.At).Seconds())
	}
	return nil
}

// replay forwards up to ReplayBatch queued messages. It returns false when
// the sink rejected a message (the message stays queued).
func (b *Bridge) replay(ctx context.Context) bool {
	for i := 0; i < b.cfg.ReplayBatch; i++ {
		if ctx.Err() != nil {
			return true
		}
		m, ok, err := b.queue.Peek()
		if err != nil {
			// A corrupt head record can never be delivered: skip it.
			b.log.Error("queue peek failed; discarding unreadable records", "err", err)
			b.metrics.BridgeDropped.Inc()
			if aerr := b.queue.Ack(); aerr != nil {
				b.log.Error("queue ack failed", "err", aerr)
				return false
			}
			continue
		}
		if !ok {
			break
		}
		if err := b.send(ctx, m); err != nil {
			if ctx.Err() == nil && b.sinkUp.Load() {
				b.log.Warn("sink unavailable during replay", "err", err, "queued", b.queue.Len())
			}
			return false
		}
		if err := b.queue.Ack(); err != nil {
			b.log.Error("queue ack failed", "err", err)
			return false
		}
	}
	if !b.sinkUp.Load() {
		b.log.Info("sink recovered; replaying", "queued", b.queue.Len())
	}
	b.sinkUp.Store(true)
	b.metrics.BridgeSinkUp.Set(1)
	b.updateQueueGauges()
	return true
}

func (b *Bridge) fail(backoff time.Duration, timer *time.Timer) time.Duration {
	if b.sinkUp.Load() {
		b.log.Warn("sink down; storing to queue", "retry_in", backoff)
	}
	b.sinkUp.Store(false)
	b.metrics.BridgeSinkUp.Set(0)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(backoff)
	next := backoff * 2
	if next > b.cfg.RetryMax {
		next = b.cfg.RetryMax
	}
	return next
}

// drain persists everything still in memory.
func (b *Bridge) drain() {
	for {
		m, ok := b.in.pop()
		if !ok {
			break
		}
		b.spill(m, "shutdown")
	}
	b.updateQueueGauges()
	b.log.Info("bridge stopped", "forwarded", b.forwarded.Load(), "queued", b.queue.Len())
}

func (b *Bridge) updateQueueGauges() {
	b.metrics.BridgeQueueLen.Set(float64(b.queue.Len()))
	b.metrics.BridgeQueueByte.Set(float64(b.queue.Bytes()))
	b.metrics.BridgeEvicted.Set(float64(b.queue.Dropped()))
}
