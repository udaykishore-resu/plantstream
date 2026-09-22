// Package linesim is a deterministic model of a packaging line used by both
// cmd/plc-sim (exposed over Modbus TCP) and the in-process "sim" collector
// source. Given the same seed and step sequence it produces the same values,
// which keeps tests reproducible.
package linesim

import (
	"math"
	"math/rand"
	"sort"
	"time"
)

// State is the line's machine state (ISA-88/PackML-inspired subset).
type State uint16

// Machine states.
const (
	Stopped    State = 0
	Starting   State = 1
	Running    State = 2
	Starved    State = 3
	Faulted    State = 4
	Changeover State = 5
)

func (s State) String() string {
	switch s {
	case Stopped:
		return "STOPPED"
	case Starting:
		return "STARTING"
	case Running:
		return "RUNNING"
	case Starved:
		return "STARVED"
	case Faulted:
		return "FAULTED"
	case Changeover:
		return "CHANGEOVER"
	default:
		return "UNKNOWN"
	}
}

// Signal names exposed by the model.
const (
	SignalSpeed       = "speed"       // bottles per minute
	SignalTemperature = "temperature" // °C at the sealing head
	SignalVibration   = "vibration"   // mm/s RMS on the main drive
	SignalState       = "state"       // State enum
	SignalCount       = "count"       // good bottles since start
	SignalAmbient     = "ambient"     // °C hall temperature
	SignalPressure    = "pressure"    // bar, pneumatic supply
)

// Signals lists all signal names in a stable order.
func Signals() []string {
	s := []string{SignalSpeed, SignalTemperature, SignalVibration, SignalState, SignalCount, SignalAmbient, SignalPressure}
	sort.Strings(s)
	return s
}

// IsSignal reports whether name is a known signal.
func IsSignal(name string) bool {
	for _, s := range Signals() {
		if s == name {
			return true
		}
	}
	return false
}

// Snapshot is the model's observable state at one instant.
type Snapshot struct {
	Speed       float64
	Temperature float64
	Vibration   float64
	State       State
	Count       uint32
	Ambient     float64
	Pressure    float64
}

// Get returns a signal by name widened to float64.
func (s Snapshot) Get(name string) (float64, bool) {
	switch name {
	case SignalSpeed:
		return s.Speed, true
	case SignalTemperature:
		return s.Temperature, true
	case SignalVibration:
		return s.Vibration, true
	case SignalState:
		return float64(s.State), true
	case SignalCount:
		return float64(s.Count), true
	case SignalAmbient:
		return s.Ambient, true
	case SignalPressure:
		return s.Pressure, true
	}
	return 0, false
}

// Line holds the dynamic model. It is not safe for concurrent use; wrap it if
// several goroutines step or read it.
type Line struct {
	rng *rand.Rand

	targetSpeed float64 // bpm when RUNNING
	speed       float64
	temp        float64
	vib         float64
	state       State
	count       float64
	dwell       time.Duration // time left in current state
	ambient     float64
	pressure    float64
	elapsed     time.Duration
}

// NewLine builds a line in STOPPED state with the given random seed.
func NewLine(seed int64) *Line {
	return &Line{
		rng:         rand.New(rand.NewSource(seed)), //nolint:gosec // non-cryptographic: deterministic simulation noise
		targetSpeed: 480,
		temp:        24,
		vib:         0.4,
		state:       Stopped,
		dwell:       3 * time.Second,
		ambient:     22,
		pressure:    6.2,
	}
}

// Snapshot returns the current observable values.
func (l *Line) Snapshot() Snapshot {
	return Snapshot{
		Speed:       round(l.speed, 2),
		Temperature: round(l.temp, 2),
		Vibration:   round(l.vib, 3),
		State:       l.state,
		Count:       uint32(l.count),
		Ambient:     round(l.ambient, 2),
		Pressure:    round(l.pressure, 2),
	}
}

// Elapsed is the total simulated time.
func (l *Line) Elapsed() time.Duration { return l.elapsed }

// Step advances the model by dt.
func (l *Line) Step(dt time.Duration) {
	if dt <= 0 {
		return
	}
	l.elapsed += dt
	s := dt.Seconds()

	l.dwell -= dt
	if l.dwell <= 0 {
		l.transition()
	}

	// Speed: first-order lag towards a state-dependent set point.
	var set float64
	switch l.state {
	case Running:
		set = l.targetSpeed + l.noise(4)
	case Starting:
		set = l.targetSpeed * 0.6
	case Starved:
		set = l.targetSpeed * 0.25
	default:
		set = 0
	}
	tau := 4.0 // seconds
	if l.state == Faulted {
		tau = 1.2 // emergency stop decelerates hard
	}
	l.speed += (set - l.speed) * (1 - math.Exp(-s/tau))
	if l.speed < 0.5 && set == 0 {
		l.speed = 0
	}

	// Ambient drifts slowly around 22 °C.
	l.ambient += (22 - l.ambient) * 0.01 * s
	l.ambient += l.noise(0.02) * s

	// Sealing head temperature follows load with a long lag.
	tempSet := l.ambient + 55 + 0.09*l.speed
	l.temp += (tempSet-l.temp)*(1-math.Exp(-s/60)) + l.noise(0.05)

	// Vibration scales with speed; faults add a broadband spike.
	vibSet := 0.4 + 0.0045*l.speed
	if l.state == Faulted {
		vibSet += 6
	}
	l.vib += (vibSet-l.vib)*(1-math.Exp(-s/1.5)) + math.Abs(l.noise(0.03))

	// Pneumatic pressure sags with consumption.
	l.pressure = 6.4 - 0.0009*l.speed + l.noise(0.02)

	// Good-bottle counter.
	if l.state == Running || l.state == Starting || l.state == Starved {
		l.count += l.speed * s / 60
		if l.count > math.MaxUint32 {
			l.count = 0
		}
	}
}

// transition picks the next state with realistic dwell times.
func (l *Line) transition() {
	r := l.rng.Float64()
	switch l.state {
	case Stopped, Changeover:
		l.state, l.dwell = Starting, l.jitter(8*time.Second, 0.3)
	case Starting:
		l.state, l.dwell = Running, l.jitter(90*time.Second, 0.5)
	case Running:
		switch {
		case r < 0.12:
			l.state, l.dwell = Faulted, l.jitter(20*time.Second, 0.5)
		case r < 0.35:
			l.state, l.dwell = Starved, l.jitter(15*time.Second, 0.5)
		case r < 0.42:
			l.state, l.dwell = Changeover, l.jitter(40*time.Second, 0.3)
		default:
			l.dwell = l.jitter(90*time.Second, 0.5)
		}
	case Starved:
		l.state, l.dwell = Running, l.jitter(90*time.Second, 0.5)
	case Faulted:
		l.state, l.dwell = Stopped, l.jitter(10*time.Second, 0.4)
	}
}

func (l *Line) jitter(d time.Duration, spread float64) time.Duration {
	f := 1 + (l.rng.Float64()*2-1)*spread
	return time.Duration(float64(d) * f)
}

func (l *Line) noise(sigma float64) float64 { return l.rng.NormFloat64() * sigma }

func round(v float64, places int) float64 {
	p := math.Pow10(places)
	return math.Round(v*p) / p
}
