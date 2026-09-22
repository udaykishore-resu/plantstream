package edge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/udaykishore-resu/plantstream/internal/collectors"
	"github.com/udaykishore-resu/plantstream/internal/domain/asset"
	"github.com/udaykishore-resu/plantstream/internal/domain/quality"
	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
	"github.com/udaykishore-resu/plantstream/internal/observability"
	"github.com/udaykishore-resu/plantstream/internal/ports"
)

// Config tunes the node.
type Config struct {
	// StaleCheckInterval is how often stale detection runs (default 1s).
	StaleCheckInterval time.Duration
	// DeathTimeout bounds the DEATH publish during shutdown (default 2s).
	DeathTimeout time.Duration
}

type sourceRun struct {
	def  *asset.Source
	src  collectors.Source
	tags map[string]asset.TagRef // by tag name
	seq  *uns.Sequencer
}

// Node is the edge pipeline. It implements collectors.Observer.
type Node struct {
	cfg     Config
	idx     *asset.Index
	broker  ports.Broker
	health  *collectors.Health
	metrics *observability.Metrics
	log     *slog.Logger
	latest  *Latest
	now     func() time.Time

	mu      sync.Mutex
	sources map[string]*sourceRun
	order   []string
	evals   map[string]*quality.Evaluator // by topic
}

// New builds the node and its sources from a validated plant index.
func New(cfg Config, idx *asset.Index, broker ports.Broker, health *collectors.Health, metrics *observability.Metrics, log *slog.Logger) (*Node, error) {
	if cfg.StaleCheckInterval <= 0 {
		cfg.StaleCheckInterval = time.Second
	}
	if cfg.DeathTimeout <= 0 {
		cfg.DeathTimeout = 2 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	if health == nil {
		health = collectors.NewHealth()
	}
	if metrics == nil {
		metrics = observability.NewMetrics()
	}
	n := &Node{
		cfg: cfg, idx: idx, broker: broker, health: health, metrics: metrics, log: log,
		latest: NewLatest(), now: time.Now,
		sources: map[string]*sourceRun{}, evals: map[string]*quality.Evaluator{},
	}
	plant := idx.Plant()
	for i := range plant.Sources {
		def := &plant.Sources[i]
		tags := idx.TagsForSource(def.Name)
		if len(tags) == 0 {
			log.Warn("source has no tags; skipping", "source", def.Name)
			continue
		}
		src, err := collectors.New(def, tags, log.With("source", def.Name))
		if err != nil {
			return nil, fmt.Errorf("edge: build source %q: %w", def.Name, err)
		}
		run := &sourceRun{def: def, src: src, tags: map[string]asset.TagRef{}, seq: &uns.Sequencer{}}
		for _, t := range tags {
			run.tags[t.Tag.Name] = t
			q := plant.EffectiveQuality(t.Tag, def.PollInterval)
			cfgq := quality.Config{StaleAfter: q.StaleAfter, FlatlineAfter: q.FlatlineAfter, FlatlineMinSamples: q.FlatlineMinSamples}
			if t.Tag.Range != nil {
				cfgq.HasRange, cfgq.Min, cfgq.Max = true, t.Tag.Range.Min, t.Tag.Range.Max
			}
			n.evals[t.Topic.String()] = quality.New(cfgq)
		}
		n.sources[def.Name] = run
		n.order = append(n.order, def.Name)
		health.Register(def.Name, string(def.Type))
		metrics.SourceUp.WithLabelValues(def.Name).Set(0)
	}
	if len(n.sources) == 0 {
		return nil, errors.New("edge: plant defines no sources with tags")
	}
	return n, nil
}

// Latest exposes the last-value cache.
func (n *Node) Latest() *Latest { return n.latest }

// Health exposes source health.
func (n *Node) Health() *collectors.Health { return n.health }

// Run publishes BIRTH for every source, polls until ctx is cancelled, then
// publishes DEATH and returns.
func (n *Node) Run(ctx context.Context) error {
	for _, name := range n.order {
		if err := n.publishLifecycle(ctx, n.sources[name], uns.Birth); err != nil {
			return fmt.Errorf("edge: publish BIRTH for %s: %w", name, err)
		}
	}
	g, gctx := errgroup.WithContext(ctx)
	for _, name := range n.order {
		run := n.sources[name]
		p := collectors.NewPoller(run.src, run.def.PollInterval, n.health, n, n.log)
		g.Go(func() error { return p.Run(gctx) })
	}
	g.Go(func() error {
		t := time.NewTicker(n.cfg.StaleCheckInterval)
		defer t.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
				n.checkStale(gctx)
			}
		}
	})
	err := g.Wait()

	dctx, cancel := context.WithTimeout(context.Background(), n.cfg.DeathTimeout)
	defer cancel()
	for _, name := range n.order {
		if derr := n.publishLifecycle(dctx, n.sources[name], uns.Death); derr != nil {
			n.log.Warn("publish DEATH failed", "source", name, "err", derr)
		}
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (n *Node) publishLifecycle(ctx context.Context, run *sourceRun, mt uns.MessageType) error {
	plant := n.idx.Plant()
	n.mu.Lock()
	var metrics []uns.Metric
	if mt == uns.Birth {
		run.seq.Reset()
		metrics = make([]uns.Metric, 0, len(run.tags))
		for _, name := range sortedKeys(run.tags) {
			tr := run.tags[name]
			ev := n.evals[tr.Topic.String()]
			m := n.metric(tr, run.def.Name, nil, n.now(), ev.Current())
			if v, at, ok := ev.Last(); ok {
				m.Value, m.Timestamp = v, uns.Millis(at)
			}
			metrics = append(metrics, m)
		}
	}
	p := uns.Payload{Type: mt, Timestamp: uns.Millis(n.now()), Seq: run.seq.Next(), Node: run.def.Name, Metrics: metrics}
	n.mu.Unlock()

	body, err := p.Marshal()
	if err != nil {
		return err
	}
	topic := uns.LifecycleTopic(plant.Enterprise, plant.Site, run.def.Name, mt)
	msg := ports.Message{Topic: topic, Payload: body, Retain: mt == uns.Birth, At: p.Timestamp.Time()}
	if err := n.broker.Publish(ctx, msg); err != nil {
		n.metrics.PublishErrors.Inc()
		return err
	}
	n.metrics.Published.WithLabelValues(string(mt)).Inc()
	n.log.Info("lifecycle published", "type", mt, "source", run.def.Name, "topic", topic, "metrics", len(metrics))
	return nil
}

// metric assembles a contextualised metric for a tag.
func (n *Node) metric(tr asset.TagRef, source string, value any, at time.Time, v quality.Verdict) uns.Metric {
	ctx := uns.Context{
		AssetID: tr.Asset.ID, AssetClass: tr.Asset.Class, Unit: tr.Tag.Unit,
		Source: source, Description: tr.Tag.Description,
	}
	if tr.Tag.Range != nil {
		lo, hi := tr.Tag.Range.Min, tr.Tag.Range.Max
		ctx.EngLow, ctx.EngHigh = &lo, &hi
	}
	if v.Quality != uns.QualityGood {
		ctx.QualityRule = v.Rule
	}
	return uns.Metric{
		Name: tr.Tag.Name, Timestamp: uns.Millis(at), DataType: tr.Tag.DataType,
		Value: value, Quality: v.Quality, Topic: tr.Topic.String(), Context: ctx,
	}
}

type outgoing struct {
	tr   asset.TagRef
	msg  ports.Message
	m    uns.Metric
	verd quality.Verdict
}

// OnReadings implements collectors.Observer: contextualise, evaluate, publish.
func (n *Node) OnReadings(ctx context.Context, source string, readings []collectors.Reading) {
	run, ok := n.sources[source]
	if !ok {
		return
	}
	n.metrics.SourceUp.WithLabelValues(source).Set(1)
	n.metrics.SourcePolls.WithLabelValues(source, "ok").Inc()
	if st, ok := n.health.Get(source); ok {
		n.metrics.PollDuration.WithLabelValues(source).Observe(st.LastLatency.Seconds())
	}
	if len(readings) == 0 {
		return
	}
	n.metrics.Samples.WithLabelValues(source).Add(float64(len(readings)))

	n.mu.Lock()
	out := make([]outgoing, 0, len(readings))
	for _, r := range readings {
		tr, ok := run.tags[r.Tag]
		if !ok {
			n.log.Warn("reading for unknown tag", "source", source, "tag", r.Tag)
			continue
		}
		at := r.At
		if at.IsZero() {
			at = n.now()
		}
		value := tr.Tag.Scale.Apply(r.Value)
		verd := n.evals[tr.Topic.String()].Observe(value, at)
		m := n.metric(tr, source, castValue(tr.Tag.DataType, value), at, verd)
		p := uns.Payload{Type: uns.Data, Timestamp: uns.Millis(at), Seq: run.seq.Next(), Node: source, Metrics: []uns.Metric{m}}
		body, err := p.Marshal()
		if err != nil {
			n.log.Error("marshal payload", "err", err)
			continue
		}
		out = append(out, outgoing{tr: tr, m: m, verd: verd, msg: ports.Message{Topic: tr.Topic.String(), Payload: body, Retain: true, At: at}})
	}
	n.mu.Unlock()
	n.publishAll(ctx, out)
}

// OnError implements collectors.Observer: flag every tag of the source BAD
// (once per outage) and republish its last value with the BAD quality.
func (n *Node) OnError(ctx context.Context, source string, err error) {
	run, ok := n.sources[source]
	if !ok {
		return
	}
	n.metrics.SourceUp.WithLabelValues(source).Set(0)
	n.metrics.SourcePolls.WithLabelValues(source, "error").Inc()

	n.mu.Lock()
	var out []outgoing
	for _, name := range sortedKeys(run.tags) {
		tr := run.tags[name]
		ev := n.evals[tr.Topic.String()]
		if ev.Current().Quality == uns.QualityBad {
			continue // already flagged for this outage
		}
		verd := ev.Comm(err)
		at := n.now()
		var value any
		if v, _, ok := ev.Last(); ok {
			value = castValue(tr.Tag.DataType, v)
		}
		m := n.metric(tr, source, value, at, verd)
		p := uns.Payload{Type: uns.Data, Timestamp: uns.Millis(at), Seq: run.seq.Next(), Node: source, Metrics: []uns.Metric{m}}
		body, merr := p.Marshal()
		if merr != nil {
			continue
		}
		out = append(out, outgoing{tr: tr, m: m, verd: verd, msg: ports.Message{Topic: tr.Topic.String(), Payload: body, Retain: true, At: at}})
	}
	n.mu.Unlock()
	n.publishAll(ctx, out)
}

// checkStale runs the staleness rule over every tag and publishes transitions.
func (n *Node) checkStale(ctx context.Context) {
	now := n.now()
	n.mu.Lock()
	var out []outgoing
	for _, name := range n.order {
		run := n.sources[name]
		for _, tag := range sortedKeys(run.tags) {
			tr := run.tags[tag]
			ev := n.evals[tr.Topic.String()]
			verd, changed := ev.Check(now)
			if !changed {
				continue
			}
			var value any
			if v, _, ok := ev.Last(); ok {
				value = castValue(tr.Tag.DataType, v)
			}
			m := n.metric(tr, name, value, now, verd)
			p := uns.Payload{Type: uns.Data, Timestamp: uns.Millis(now), Seq: run.seq.Next(), Node: name, Metrics: []uns.Metric{m}}
			body, err := p.Marshal()
			if err != nil {
				continue
			}
			out = append(out, outgoing{tr: tr, m: m, verd: verd, msg: ports.Message{Topic: tr.Topic.String(), Payload: body, Retain: true, At: now}})
		}
	}
	n.mu.Unlock()
	n.publishAll(ctx, out)
}

func (n *Node) publishAll(ctx context.Context, out []outgoing) {
	for _, o := range out {
		prev, had := n.latest.Set(o.tr.Asset.ID, o.m)
		if !had || prev != o.m.Quality {
			n.metrics.QualityTrans.WithLabelValues(string(o.m.Quality), o.verd.Rule).Inc()
			if o.m.Quality != uns.QualityGood {
				n.log.Info("quality transition", "topic", o.msg.Topic, "from", prev, "to", o.m.Quality, "rule", o.verd.Rule, "reason", o.verd.Reason)
			} else if had {
				n.log.Info("quality recovered", "topic", o.msg.Topic, "from", prev, "rule", o.verd.Rule)
			}
			if had {
				n.metrics.TagsByQuality.WithLabelValues(string(prev)).Dec()
			}
			n.metrics.TagsByQuality.WithLabelValues(string(o.m.Quality)).Inc()
		}
		if err := n.broker.Publish(ctx, o.msg); err != nil {
			n.metrics.PublishErrors.Inc()
			if ctx.Err() == nil {
				n.log.Warn("publish failed", "topic", o.msg.Topic, "err", err)
			}
			continue
		}
		n.metrics.Published.WithLabelValues(string(uns.Data)).Inc()
	}
}

// castValue renders the float64 pipeline value in the tag's declared type so
// integers do not appear as 2.0 in JSON.
func castValue(dt uns.DataType, v float64) any {
	switch dt {
	case uns.TypeInt16, uns.TypeInt32:
		return int64(v)
	case uns.TypeUInt16, uns.TypeUInt32:
		if v < 0 {
			return int64(v)
		}
		return uint64(v)
	case uns.TypeBoolean:
		return v != 0
	case uns.TypeFloat:
		// Render at float32 precision so a 32-bit register value does not
		// appear as 77.29000091552734 after widening.
		return float32(v)
	default:
		return v
	}
}

func sortedKeys(m map[string]asset.TagRef) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
