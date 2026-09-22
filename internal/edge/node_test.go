package edge

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/udaykishore-resu/plantstream/internal/adapters/memory"
	"github.com/udaykishore-resu/plantstream/internal/collectors"
	"github.com/udaykishore-resu/plantstream/internal/domain/asset"
	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
	"github.com/udaykishore-resu/plantstream/internal/ports"
)

const plantYAML = `
enterprise: acme
site: austin
quality:
  stale_after: 3s
sources:
  - name: plc-1
    type: sim
    poll_interval: 1s
areas:
  - name: packaging
    lines:
      - name: line-1
        cells:
          - name: filling
            assets:
              - id: filler-01
                class: RotaryFiller
                source: plc-1
                tags:
                  - name: speed
                    unit: bpm
                    range: { min: 0, max: 600 }
                    scale: { factor: 0.1 }
                    sim: { signal: speed }
                  - name: state
                    datatype: UInt16
                    quality: { flatline_after: 2s, flatline_min_samples: 2 }
                    sim: { signal: state }
`

type scripted struct {
	mu   sync.Mutex
	next []collectors.Reading
	err  error
}

func (s *scripted) Name() string { return "plc-1" }
func (s *scripted) Read(context.Context) ([]collectors.Reading, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	return s.next, nil
}
func (s *scripted) Close() error { return nil }

type capture struct {
	mu   sync.Mutex
	msgs []ports.Message
	ch   chan ports.Message
}

func newCapture(t *testing.T, b *memory.Broker, filter string) *capture {
	t.Helper()
	c := &capture{ch: make(chan ports.Message, 256)}
	_, err := b.Subscribe(context.Background(), filter, func(_ context.Context, m ports.Message) {
		c.mu.Lock()
		c.msgs = append(c.msgs, m)
		c.mu.Unlock()
		c.ch <- m
	})
	require.NoError(t, err)
	return c
}

func (c *capture) next(t *testing.T) uns.Payload {
	t.Helper()
	select {
	case m := <-c.ch:
		p, err := uns.Unmarshal(m.Payload)
		require.NoError(t, err)
		return p
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message")
		return uns.Payload{}
	}
}

func (c *capture) nextMsg(t *testing.T) (ports.Message, uns.Payload) {
	t.Helper()
	select {
	case m := <-c.ch:
		p, err := uns.Unmarshal(m.Payload)
		require.NoError(t, err)
		return m, p
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message")
		return ports.Message{}, uns.Payload{}
	}
}

func setup(t *testing.T) (*Node, *memory.Broker, *scripted, *time.Time) {
	t.Helper()
	p, err := asset.Parse([]byte(plantYAML))
	require.NoError(t, err)
	b := memory.NewBroker()
	t.Cleanup(func() { b.Close() })
	n, err := New(Config{StaleCheckInterval: 10 * time.Millisecond}, p.Index(), b, nil, nil, nil)
	require.NoError(t, err)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	n.now = func() time.Time { return now }
	src := &scripted{}
	n.sources["plc-1"].src = src
	return n, b, src, &now
}

func TestNode_ContextualisedDataFlow(t *testing.T) {
	n, b, src, now := setup(t)
	all := newCapture(t, b, "acme/austin/#")

	ctx := context.Background()
	src.next = []collectors.Reading{
		{Tag: "speed", Value: 4805, At: *now}, // raw ×0.1 → 480.5
		{Tag: "state", Value: 2, At: *now},
		{Tag: "ghost", Value: 1, At: *now}, // unknown tag ignored
	}
	n.OnReadings(ctx, "plc-1", src.next)

	m1, p1 := all.nextMsg(t)
	assert.Equal(t, "acme/austin/packaging/line-1/filling/speed", m1.Topic)
	assert.True(t, m1.Retain)
	assert.Equal(t, uns.Data, p1.Type)
	assert.Equal(t, "plc-1", p1.Node)
	require.Len(t, p1.Metrics, 1)
	sp := p1.Metrics[0]
	assert.Equal(t, 480.5, sp.Value)
	assert.Equal(t, uns.QualityGood, sp.Quality)
	assert.Equal(t, "filler-01", sp.Context.AssetID)
	assert.Equal(t, "RotaryFiller", sp.Context.AssetClass)
	assert.Equal(t, "bpm", sp.Context.Unit)
	assert.Equal(t, 0.0, *sp.Context.EngLow)
	assert.Equal(t, 600.0, *sp.Context.EngHigh)
	assert.Empty(t, sp.Context.QualityRule)
	assert.Equal(t, *now, sp.Timestamp.Time())

	_, p2 := all.nextMsg(t)
	assert.Equal(t, uint8(1), p2.Seq, "sequence increments per message")
	assert.Equal(t, float64(2), p2.Metrics[0].Value, "JSON numbers decode as float64; encoded as integer")
	assert.Equal(t, uns.TypeUInt16, p2.Metrics[0].DataType)

	latest, ok := n.Latest().Asset("filler-01")
	require.True(t, ok)
	require.Len(t, latest, 2)
	assert.Equal(t, "speed", latest[0].Name)
	assert.Equal(t, uint64(2), latest[1].Value, "typed value in the cache")
	assert.Equal(t, map[uns.Quality]int{uns.QualityGood: 2}, n.Latest().Counts())

	// Out of range → OUT_OF_RANGE with the versioned rule.
	*now = now.Add(time.Second)
	n.OnReadings(ctx, "plc-1", []collectors.Reading{{Tag: "speed", Value: 7000, At: *now}})
	p3 := all.next(t)
	assert.Equal(t, uns.QualityOutOfRange, p3.Metrics[0].Quality)
	assert.Equal(t, "quality.range@v1", p3.Metrics[0].Context.QualityRule)
	assert.Equal(t, map[uns.Quality]int{uns.QualityGood: 1, uns.QualityOutOfRange: 1}, n.Latest().Counts())

	// Flatline on state after 2s / 2 samples.
	for i := 0; i < 3; i++ {
		*now = now.Add(time.Second)
		n.OnReadings(ctx, "plc-1", []collectors.Reading{{Tag: "state", Value: 2, At: *now}})
	}
	var last uns.Payload
	for i := 0; i < 3; i++ {
		last = all.next(t)
	}
	assert.Equal(t, uns.QualityFlatline, last.Metrics[0].Quality)
	assert.Equal(t, "quality.flatline@v1", last.Metrics[0].Context.QualityRule)
}

func TestNode_CommFailureAndStale(t *testing.T) {
	n, b, _, now := setup(t)
	all := newCapture(t, b, "acme/austin/packaging/#")
	ctx := context.Background()

	n.OnReadings(ctx, "plc-1", []collectors.Reading{{Tag: "speed", Value: 100, At: *now}, {Tag: "state", Value: 1, At: *now}})
	all.next(t)
	all.next(t)

	// Comm failure flags both tags BAD once, carrying the last value.
	n.OnError(ctx, "plc-1", errors.New("connection refused"))
	pa := all.next(t)
	pb := all.next(t)
	assert.Equal(t, uns.QualityBad, pa.Metrics[0].Quality)
	assert.Equal(t, uns.QualityBad, pb.Metrics[0].Quality)
	assert.Equal(t, "quality.comm@v1", pa.Metrics[0].Context.QualityRule)
	assert.Equal(t, 10.0, pa.Metrics[0].Value, "last good value (scaled) is carried")
	n.OnError(ctx, "plc-1", errors.New("still down"))
	select {
	case m := <-all.ch:
		t.Fatalf("no republish expected during an outage, got %s", m.Topic)
	case <-time.After(50 * time.Millisecond):
	}
	st, _ := n.Health().Get("plc-1")
	assert.False(t, st.Up, "health is driven by the poller, unchanged by direct observer calls")

	// Recovery.
	*now = now.Add(time.Second)
	n.OnReadings(ctx, "plc-1", []collectors.Reading{{Tag: "speed", Value: 200, At: *now}, {Tag: "state", Value: 1, At: *now}})
	assert.Equal(t, uns.QualityGood, all.next(t).Metrics[0].Quality)
	all.next(t)

	// Staleness: 3s without samples.
	*now = now.Add(2 * time.Second)
	n.checkStale(ctx)
	select {
	case <-all.ch:
		t.Fatal("not stale yet")
	case <-time.After(30 * time.Millisecond):
	}
	*now = now.Add(2 * time.Second)
	n.checkStale(ctx)
	ps := all.next(t)
	assert.Equal(t, uns.QualityStale, ps.Metrics[0].Quality)
	assert.Equal(t, "quality.stale@v1", ps.Metrics[0].Context.QualityRule)
	assert.Equal(t, 20.0, ps.Metrics[0].Value)
	all.next(t)
	n.checkStale(ctx) // no duplicate transitions
	select {
	case <-all.ch:
		t.Fatal("stale must be published once")
	case <-time.After(30 * time.Millisecond):
	}

	// Unknown sources are ignored.
	n.OnReadings(ctx, "nope", nil)
	n.OnError(ctx, "nope", errors.New("x"))
	// Empty readings do nothing but count the poll.
	n.OnReadings(ctx, "plc-1", nil)
}

func TestNode_RunLifecycle(t *testing.T) {
	n, b, src, now := setup(t)
	life := newCapture(t, b, "acme/austin/_edge/#")
	data := newCapture(t, b, "acme/austin/packaging/#")
	src.next = []collectors.Reading{{Tag: "speed", Value: 1000, At: *now}, {Tag: "state", Value: 2, At: *now}}
	n.sources["plc-1"].def.PollInterval = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()

	birthMsg, birth := life.nextMsg(t)
	assert.Equal(t, "acme/austin/_edge/plc-1/BIRTH", birthMsg.Topic)
	assert.True(t, birthMsg.Retain)
	assert.Equal(t, uns.Birth, birth.Type)
	assert.Equal(t, uint8(0), birth.Seq)
	require.Len(t, birth.Metrics, 2)
	assert.Equal(t, "speed", birth.Metrics[0].Name)
	assert.Nil(t, birth.Metrics[0].Value, "no value before the first poll")
	assert.Equal(t, "bpm", birth.Metrics[0].Context.Unit)

	d := data.next(t)
	assert.Equal(t, uns.Data, d.Type)
	assert.Equal(t, uint8(1), d.Seq, "DATA sequence continues after BIRTH")

	require.Eventually(t, func() bool {
		st, ok := n.Health().Get("plc-1")
		return ok && st.Up && st.Polls >= 2
	}, 2*time.Second, 5*time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	var death uns.Payload
	for {
		m, p := life.nextMsg(t)
		if p.Type == uns.Death {
			assert.Equal(t, "acme/austin/_edge/plc-1/DEATH", m.Topic)
			assert.False(t, m.Retain)
			death = p
			break
		}
	}
	assert.Empty(t, death.Metrics)

	// A retained BIRTH is available to late subscribers.
	_, ok := b.Retained("acme/austin/_edge/plc-1/BIRTH")
	assert.True(t, ok)
}

func TestNode_RunFailsWhenBrokerClosed(t *testing.T) {
	n, b, _, _ := setup(t)
	require.NoError(t, b.Close())
	err := n.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BIRTH")
}

func TestNew_Errors(t *testing.T) {
	// A source referencing a missing CSV file is fine at construction time
	// but a plant whose only source has no tags is rejected.
	p, err := asset.Parse([]byte(plantYAML))
	require.NoError(t, err)
	p.Sources = append(p.Sources, asset.Source{Name: "orphan", Type: asset.SourceSim, PollInterval: time.Second})
	n, err := New(Config{}, p.Index(), memory.NewBroker(), nil, nil, nil)
	require.NoError(t, err)
	assert.Len(t, n.order, 1, "tagless source skipped")

	// Bad source config surfaces from collectors.New.
	bad := &asset.Plant{Enterprise: "e", Site: "s", Sources: []asset.Source{{Name: "m", Type: asset.SourceModbus}},
		Areas: []asset.Area{{Name: "a", Lines: []asset.Line{{Name: "l", Cells: []asset.Cell{{Name: "c", Assets: []asset.Asset{{ID: "x", Class: "C", Source: "m", Tags: []asset.Tag{{Name: "t"}}}}}}}}}}}
	_, err = New(Config{}, bad.Index(), memory.NewBroker(), nil, nil, nil)
	require.Error(t, err)
}

func TestCastValue(t *testing.T) {
	assert.Equal(t, int64(-3), castValue(uns.TypeInt16, -3.7))
	assert.Equal(t, uint64(3), castValue(uns.TypeUInt32, 3.2))
	assert.Equal(t, int64(-1), castValue(uns.TypeUInt16, -1))
	assert.Equal(t, true, castValue(uns.TypeBoolean, 1))
	assert.Equal(t, float32(1.5), castValue(uns.TypeFloat, 1.5))
	assert.Equal(t, 1.5, castValue(uns.TypeDouble, 1.5))
}

func TestNode_WithRealCSVSource(t *testing.T) {
	dir := t.TempDir()
	csv := filepath.Join(dir, "lab.csv")
	require.NoError(t, os.WriteFile(csv, []byte("pressure\n6.1\n6.2\n"), 0o644))
	p, err := asset.Parse([]byte(`
enterprise: acme
site: lab
sources:
  - name: rig
    type: csv
    poll_interval: 10ms
    csv: { path: "` + csv + `" }
areas:
  - name: a
    lines:
      - name: l
        cells:
          - name: c
            assets:
              - id: rig-01
                class: Rig
                source: rig
                tags:
                  - name: pressure
                    unit: bar
                    quality: { stale_after: 50ms }
                    csv: {}
`))
	require.NoError(t, err)
	b := memory.NewBroker()
	defer b.Close()
	n, err := New(Config{StaleCheckInterval: 10 * time.Millisecond}, p.Index(), b, nil, nil, nil)
	require.NoError(t, err)
	c := newCapture(t, b, "acme/lab/a/l/c/pressure")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = n.Run(ctx) }()

	assert.Equal(t, 6.1, c.next(t).Metrics[0].Value)
	assert.Equal(t, 6.2, c.next(t).Metrics[0].Value)
	// File exhausted → STALE within ~50ms.
	st := c.next(t)
	assert.Equal(t, uns.QualityStale, st.Metrics[0].Quality)
	assert.Equal(t, 6.2, st.Metrics[0].Value)
}
