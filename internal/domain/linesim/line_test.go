package linesim

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLine_Deterministic(t *testing.T) {
	a, b := NewLine(42), NewLine(42)
	for i := 0; i < 2000; i++ {
		a.Step(500 * time.Millisecond)
		b.Step(500 * time.Millisecond)
	}
	assert.Equal(t, a.Snapshot(), b.Snapshot())
	assert.Equal(t, 1000*time.Second, a.Elapsed())

	c := NewLine(7)
	for i := 0; i < 2000; i++ {
		c.Step(500 * time.Millisecond)
	}
	assert.NotEqual(t, a.Snapshot(), c.Snapshot())
}

func TestLine_RealisticDynamics(t *testing.T) {
	l := NewLine(1)
	seen := map[State]bool{}
	var maxSpeed, maxTemp float64
	prevCount := uint32(0)
	for i := 0; i < 20*60*2; i++ { // 20 simulated minutes at 2 Hz
		l.Step(500 * time.Millisecond)
		s := l.Snapshot()
		seen[s.State] = true
		require.GreaterOrEqual(t, s.Speed, 0.0)
		require.LessOrEqual(t, s.Speed, 600.0)
		require.Greater(t, s.Temperature, 15.0)
		require.Less(t, s.Temperature, 150.0)
		require.GreaterOrEqual(t, s.Vibration, 0.0)
		require.GreaterOrEqual(t, s.Count, prevCount, "counter must be monotonic")
		require.InDelta(t, 6.2, s.Pressure, 1.5)
		prevCount = s.Count
		maxSpeed = max(maxSpeed, s.Speed)
		maxTemp = max(maxTemp, s.Temperature)
	}
	assert.True(t, seen[Running], "line should run at some point")
	assert.True(t, seen[Starting])
	assert.Greater(t, len(seen), 2, "state machine should visit several states")
	assert.Greater(t, maxSpeed, 400.0, "line should approach target speed")
	assert.Greater(t, maxTemp, 60.0, "sealing head should warm up under load")
	assert.Greater(t, prevCount, uint32(1000), "bottles should be produced")
}

func TestLine_ZeroStepIgnored(t *testing.T) {
	l := NewLine(1)
	before := l.Snapshot()
	l.Step(0)
	l.Step(-time.Second)
	assert.Equal(t, before, l.Snapshot())
}

func TestSnapshot_Get(t *testing.T) {
	s := Snapshot{Speed: 1, Temperature: 2, Vibration: 3, State: Running, Count: 5, Ambient: 6, Pressure: 7}
	for _, name := range Signals() {
		v, ok := s.Get(name)
		assert.True(t, ok, name)
		assert.NotZero(t, v, name)
	}
	_, ok := s.Get("nope")
	assert.False(t, ok)
	assert.False(t, IsSignal("nope"))
	assert.True(t, IsSignal(SignalSpeed))
}

func TestState_String(t *testing.T) {
	assert.Equal(t, "RUNNING", Running.String())
	assert.Equal(t, "FAULTED", Faulted.String())
	assert.Equal(t, "UNKNOWN", State(99).String())
}
