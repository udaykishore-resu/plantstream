package collectors

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/udaykishore-resu/plantstream/internal/domain/asset"
	"github.com/udaykishore-resu/plantstream/internal/modbus"
)

func TestPlan(t *testing.T) {
	tags := []modbusTag{
		{name: "e", fn: 4, start: 0, words: 1},
		{name: "a", fn: 3, start: 0, words: 2},
		{name: "b", fn: 3, start: 2, words: 2},
		{name: "c", fn: 3, start: 10, words: 1},  // gap of 6 → merged with maxGap 8
		{name: "d", fn: 3, start: 200, words: 2}, // far away → new block
		{name: "f", fn: 4, start: 1, words: 2},
	}
	blocks := Plan(tags, 8)
	require.Len(t, blocks, 3)
	assert.Equal(t, byte(3), blocks[0].Function)
	assert.Equal(t, uint16(0), blocks[0].Start)
	assert.Equal(t, uint16(11), blocks[0].Count)
	assert.Equal(t, []string{"a", "b", "c"}, blocks[0].Tags())
	assert.Equal(t, uint16(200), blocks[1].Start)
	assert.Equal(t, []string{"d"}, blocks[1].Tags())
	assert.Equal(t, byte(4), blocks[2].Function)
	assert.Equal(t, uint16(3), blocks[2].Count)

	// Gap larger than maxGap splits.
	blocks = Plan(tags[1:4], 2)
	require.Len(t, blocks, 2)

	// Protocol limit of 125 registers is honoured.
	var many []modbusTag
	for i := 0; i < 200; i++ {
		many = append(many, modbusTag{name: "t", fn: 3, start: uint16(i), words: 1})
	}
	blocks = Plan(many, 8)
	require.Len(t, blocks, 2)
	assert.Equal(t, uint16(125), blocks[0].Count)
	assert.Equal(t, uint16(75), blocks[1].Count)

	// Overlapping tags (same register read as two types) are fine.
	blocks = Plan([]modbusTag{{name: "x", fn: 3, start: 5, words: 2}, {name: "y", fn: 3, start: 5, words: 1}}, 0)
	require.Len(t, blocks, 1)
	assert.Equal(t, uint16(2), blocks[0].Count)

	assert.Nil(t, Plan(nil, 8))
}

func modbusPlant(t *testing.T, addr string) (*asset.Source, []asset.TagRef) {
	t.Helper()
	p, err := asset.Parse([]byte(`
enterprise: acme
site: austin
sources:
  - name: plc
    type: modbus
    poll_interval: 100ms
    modbus: { address: "` + addr + `", unit_id: 1, timeout: 500ms }
areas:
  - name: a
    lines:
      - name: l
        cells:
          - name: c
            assets:
              - id: filler
                class: Filler
                source: plc
                tags:
                  - name: speed
                    modbus: { register: 0, type: float32 }
                  - name: temperature
                    scale: { factor: 0.1 }
                    modbus: { function: input, register: 0, type: int16 }
                  - name: state
                    modbus: { register: 6, type: uint16 }
                  - name: count
                    modbus: { register: 7, type: uint32, byte_order: CDAB }
`))
	require.NoError(t, err)
	idx := p.Index()
	src, _ := idx.Source("plc")
	return src, idx.TagsForSource("plc")
}

func startModbus(t *testing.T, bank modbus.RegisterBank) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = modbus.NewServer(bank, nil).Serve(ctx, ln)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })
	return ln.Addr().String()
}

func TestModbusSource_EndToEnd(t *testing.T) {
	bank := modbus.NewMemoryBank(32, 8)
	speed, _ := modbus.Encode(modbus.Float32, modbus.ABCD, 480.5)
	require.NoError(t, bank.SetHolding(0, speed))
	require.NoError(t, bank.SetHolding(6, []uint16{2}))
	count, _ := modbus.Encode(modbus.UInt32, modbus.CDAB, 70000)
	require.NoError(t, bank.SetHolding(7, count))
	require.NoError(t, bank.SetInput(0, []uint16{715}))

	addr := startModbus(t, bank)
	src, tags := modbusPlant(t, addr)
	s, err := New(src, tags, nil)
	require.NoError(t, err)
	ms := s.(*ModbusSource)
	require.Len(t, ms.Blocks(), 2, "holding 0..8 coalesced; input separate")

	ctx := context.Background()
	readings, err := s.Read(ctx)
	require.NoError(t, err)
	byTag := map[string]float64{}
	for _, r := range readings {
		byTag[r.Tag] = r.Value
	}
	assert.InDelta(t, 480.5, byTag["speed"], 1e-6)
	assert.Equal(t, 715.0, byTag["temperature"], "raw value; scaling happens in the pipeline")
	assert.Equal(t, 2.0, byTag["state"])
	assert.Equal(t, 70000.0, byTag["count"])

	// Second read reuses the connection.
	_, err = s.Read(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

func TestModbusSource_ConnectFailureAndRecovery(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close()) // nothing listening now

	src, tags := modbusPlant(t, addr)
	s, err := NewModbusSource(src, tags, nil)
	require.NoError(t, err)
	_, err = s.Read(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connect")
}

type fakeConn struct {
	mu     sync.Mutex
	err    error
	closed int
	regs   []uint16
}

func (f *fakeConn) ReadRegisters(_ context.Context, _ byte, _ byte, _ uint16, qty uint16) ([]uint16, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := make([]uint16, qty)
	copy(out, f.regs)
	return out, nil
}

func (f *fakeConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func TestModbusSource_ReconnectsOnTransportErrorOnly(t *testing.T) {
	src, tags := modbusPlant(t, "127.0.0.1:1")
	s, err := NewModbusSource(src, tags, nil)
	require.NoError(t, err)
	fc := &fakeConn{regs: make([]uint16, 16)}
	dials := 0
	s.dial = func(context.Context, string, time.Duration) (modbusConn, error) {
		dials++
		return fc, nil
	}
	ctx := context.Background()
	_, err = s.Read(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, dials)

	// Protocol exception: keep the connection.
	fc.mu.Lock()
	fc.err = &modbus.ExceptionError{Function: 3, Code: modbus.ExIllegalDataAddress}
	fc.mu.Unlock()
	_, err = s.Read(ctx)
	require.Error(t, err)
	assert.Equal(t, 0, fc.closed)

	// Transport error: drop and redial next time.
	fc.mu.Lock()
	fc.err = errors.New("broken pipe")
	fc.mu.Unlock()
	_, err = s.Read(ctx)
	require.Error(t, err)
	assert.Equal(t, 1, fc.closed)
	fc.mu.Lock()
	fc.err = nil
	fc.mu.Unlock()
	_, err = s.Read(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, dials)
}

func TestNewModbusSource_Errors(t *testing.T) {
	_, err := NewModbusSource(&asset.Source{Name: "x"}, nil, nil)
	assert.Error(t, err)
	src := &asset.Source{Name: "x", Modbus: &asset.ModbusSource{Address: "a"}}
	_, err = NewModbusSource(src, []asset.TagRef{{Tag: &asset.Tag{Name: "t"}}}, nil)
	assert.Error(t, err)
	_, err = NewModbusSource(src, []asset.TagRef{{Tag: &asset.Tag{Name: "t", Modbus: &asset.ModbusAddress{Type: "int64"}}}}, nil)
	assert.Error(t, err)
	_, err = NewModbusSource(src, []asset.TagRef{{Tag: &asset.Tag{Name: "t", Modbus: &asset.ModbusAddress{Type: "int16", ByteOrder: "XXXX"}}}}, nil)
	assert.Error(t, err)
	_, err = New(src, nil, nil)
	assert.Error(t, err)
	_, err = New(&asset.Source{Name: "x", Type: "opcua"}, []asset.TagRef{{Tag: &asset.Tag{Name: "t"}}}, nil)
	assert.Error(t, err)
}

func simPlant(t *testing.T) (*asset.Source, []asset.TagRef) {
	t.Helper()
	p, err := asset.Parse([]byte(`
enterprise: acme
site: austin
sources:
  - name: sim
    type: sim
    sim: { seed: 7 }
areas:
  - name: a
    lines:
      - name: l
        cells:
          - name: c
            assets:
              - id: filler
                class: Filler
                source: sim
                tags:
                  - name: speed
                    sim: { signal: speed }
                  - name: state
                    sim: { signal: state }
                  - name: vib
                    sim: { signal: vibration }
`))
	require.NoError(t, err)
	idx := p.Index()
	src, _ := idx.Source("sim")
	return src, idx.TagsForSource("sim")
}

func TestSimSource(t *testing.T) {
	src, tags := simPlant(t)
	s, err := New(src, tags, nil)
	require.NoError(t, err)
	sim := s.(*SimSource)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sim.now = func() time.Time { return now }

	var last float64
	seen, changed := false, false
	for i := 0; i < 120; i++ {
		now = now.Add(time.Second)
		readings, err := s.Read(context.Background())
		require.NoError(t, err)
		require.Len(t, readings, 3)
		for _, r := range readings {
			assert.Equal(t, now, r.At)
			if r.Tag == "speed" {
				if seen && r.Value != last {
					changed = true
				}
				last, seen = r.Value, true
			}
		}
	}
	assert.Greater(t, last, 0.0, "line should be moving after two minutes")
	assert.True(t, changed, "speed should vary over time")
	assert.NoError(t, s.Close())

	_, err = NewSimSource(src, []asset.TagRef{{Tag: &asset.Tag{Name: "x", Sim: &asset.SimAddress{Signal: "nope"}}}})
	assert.Error(t, err)
	_, err = NewSimSource(src, []asset.TagRef{{Tag: &asset.Tag{Name: "x"}}})
	assert.Error(t, err)
}

func csvPlant(t *testing.T, path string, loop bool) (*asset.Source, []asset.TagRef) {
	t.Helper()
	loopS := "false"
	if loop {
		loopS = "true"
	}
	p, err := asset.Parse([]byte(`
enterprise: acme
site: austin
sources:
  - name: lab
    type: csv
    csv: { path: "` + path + `", loop: ` + loopS + ` }
areas:
  - name: a
    lines:
      - name: l
        cells:
          - name: c
            assets:
              - id: rig
                class: Rig
                source: lab
                tags:
                  - name: pressure
                    csv: {}
                  - name: temp
                    csv: { column: temperature }
`))
	require.NoError(t, err)
	idx := p.Index()
	src, _ := idx.Source("lab")
	return src, idx.TagsForSource("lab")
}

func TestCSVSource(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lab.csv")
	require.NoError(t, os.WriteFile(path, []byte("timestamp,pressure,temperature\n2026-01-01T00:00:00Z,6.1, 71.5\n2026-01-01T00:00:01Z,6.2,72.0\n"), 0o644))

	src, tags := csvPlant(t, path, false)
	s, err := New(src, tags, nil)
	require.NoError(t, err)
	ctx := context.Background()

	r1, err := s.Read(ctx)
	require.NoError(t, err)
	vals := map[string]float64{}
	for _, r := range r1 {
		vals[r.Tag] = r.Value
	}
	assert.Equal(t, map[string]float64{"pressure": 6.1, "temp": 71.5}, vals)

	r2, err := s.Read(ctx)
	require.NoError(t, err)
	require.Len(t, r2, 2)

	// EOF without loop → no readings, no error, forever.
	r3, err := s.Read(ctx)
	require.NoError(t, err)
	assert.Empty(t, r3)
	r3, err = s.Read(ctx)
	require.NoError(t, err)
	assert.Empty(t, r3)
	require.NoError(t, s.Close())

	// Loop mode wraps around.
	src, tags = csvPlant(t, path, true)
	s, err = New(src, tags, nil)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		r, err := s.Read(ctx)
		require.NoError(t, err)
		require.Len(t, r, 2, "iteration %d", i)
	}
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

func TestCSVSource_Errors(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Missing file surfaces as a read error, not a constructor error.
	src, tags := csvPlant(t, filepath.Join(dir, "missing.csv"), false)
	s, err := New(src, tags, nil)
	require.NoError(t, err)
	_, err = s.Read(ctx)
	assert.Error(t, err)

	// Missing column.
	bad := filepath.Join(dir, "bad.csv")
	require.NoError(t, os.WriteFile(bad, []byte("pressure\n1\n"), 0o644))
	src, tags = csvPlant(t, bad, false)
	s, _ = New(src, tags, nil)
	_, err = s.Read(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "temperature")

	// Non-numeric value.
	nan := filepath.Join(dir, "nan.csv")
	require.NoError(t, os.WriteFile(nan, []byte("pressure,temperature\nabc,1\n"), 0o644))
	src, tags = csvPlant(t, nan, false)
	s, _ = New(src, tags, nil)
	_, err = s.Read(ctx)
	assert.Error(t, err)

	// Header only, loop → error on wrap.
	empty := filepath.Join(dir, "empty.csv")
	require.NoError(t, os.WriteFile(empty, []byte("pressure,temperature\n"), 0o644))
	src, tags = csvPlant(t, empty, true)
	s, _ = New(src, tags, nil)
	_, err = s.Read(ctx)
	assert.Error(t, err)

	// Empty file → header error.
	require.NoError(t, os.WriteFile(empty, nil, 0o644))
	src, tags = csvPlant(t, empty, true)
	s, _ = New(src, tags, nil)
	_, err = s.Read(ctx)
	assert.Error(t, err)

	_, err = NewCSVSource(&asset.Source{Name: "x"}, nil)
	assert.Error(t, err)
}

type recorder struct {
	mu       sync.Mutex
	readings [][]Reading
	errs     []error
}

func (r *recorder) OnReadings(_ context.Context, _ string, rs []Reading) {
	r.mu.Lock()
	r.readings = append(r.readings, rs)
	r.mu.Unlock()
}

func (r *recorder) OnError(_ context.Context, _ string, err error) {
	r.mu.Lock()
	r.errs = append(r.errs, err)
	r.mu.Unlock()
}

type flakySource struct {
	mu    sync.Mutex
	calls int
	fail  bool
}

func (f *flakySource) Name() string { return "flaky" }
func (f *flakySource) Read(context.Context) ([]Reading, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail {
		return nil, errors.New("down")
	}
	return []Reading{{Tag: "t", Value: float64(f.calls)}}, nil
}
func (f *flakySource) Close() error { return errors.New("close failed") }

func TestPoller_HealthTracking(t *testing.T) {
	src := &flakySource{}
	h := NewHealth()
	h.Register("flaky", "test")
	assert.False(t, h.AllUp(), "down until first success")
	rec := &recorder{}
	p := NewPoller(src, 0, h, rec, nil)
	assert.Equal(t, asset.DefaultPollInterval, p.interval)

	ctx := context.Background()
	p.PollOnce(ctx)
	st, ok := h.Get("flaky")
	require.True(t, ok)
	assert.True(t, st.Up)
	assert.Equal(t, int64(1), st.Polls)
	assert.Equal(t, int64(1), st.Readings)
	assert.NotNil(t, st.LastSuccess)
	assert.True(t, h.AllUp())

	src.mu.Lock()
	src.fail = true
	src.mu.Unlock()
	p.PollOnce(ctx)
	p.PollOnce(ctx)
	st, _ = h.Get("flaky")
	assert.False(t, st.Up)
	assert.Equal(t, 2, st.ConsecutiveFailures)
	assert.Equal(t, int64(2), st.Failures)
	assert.Equal(t, "down", st.LastError)
	assert.NotNil(t, st.LastErrorAt)
	assert.False(t, h.AllUp())

	src.mu.Lock()
	src.fail = false
	src.mu.Unlock()
	p.PollOnce(ctx)
	st, _ = h.Get("flaky")
	assert.True(t, st.Up)
	assert.Equal(t, 0, st.ConsecutiveFailures)

	rec.mu.Lock()
	assert.Len(t, rec.readings, 2)
	assert.Len(t, rec.errs, 2)
	rec.mu.Unlock()

	snap := h.Snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, "flaky", snap[0].Name)
	_, ok = h.Get("nope")
	assert.False(t, ok)

	// Unregistered source names are ignored, not panics.
	h.success("ghost", time.Now(), 0, 1)
	h.failure("ghost", time.Now(), 0, errors.New("x"))
}

func TestPoller_RunStopsOnContext(t *testing.T) {
	src := &flakySource{}
	h := NewHealth()
	h.Register("flaky", "test")
	rec := &recorder{}
	p := NewPoller(src, 10*time.Millisecond, h, rec, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.readings) >= 3
	}, 2*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)

	// Cancelled context → poll is a no-op.
	before := src.calls
	p.PollOnce(ctx)
	assert.Equal(t, before, src.calls)
}

func TestHealth_Order(t *testing.T) {
	h := NewHealth()
	h.Register("b", "sim")
	h.Register("a", "sim")
	h.Register("b", "modbus") // re-register keeps position
	snap := h.Snapshot()
	names := []string{snap[0].Name, snap[1].Name}
	assert.Equal(t, []string{"b", "a"}, names)
	assert.Equal(t, "modbus", snap[0].Type)
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	assert.Equal(t, []string{"a", "b"}, sorted)
}
