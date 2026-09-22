package quality

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestObserve_Table(t *testing.T) {
	type step struct {
		v    float64
		dt   time.Duration
		want uns.Quality
		rule string
	}
	tests := []struct {
		name  string
		cfg   Config
		steps []step
	}{
		{
			name: "range",
			cfg:  Config{HasRange: true, Min: 0, Max: 600},
			steps: []step{
				{300, 0, uns.QualityGood, RuleGood},
				{600, time.Second, uns.QualityGood, RuleGood},
				{600.1, time.Second, uns.QualityOutOfRange, RuleRange},
				{-1, time.Second, uns.QualityOutOfRange, RuleRange},
				{10, time.Second, uns.QualityGood, RuleGood},
			},
		},
		{
			name: "flatline needs min samples and duration",
			cfg:  Config{FlatlineAfter: 3 * time.Second, FlatlineMinSamples: 3},
			steps: []step{
				{5, 0, uns.QualityGood, RuleGood},
				{5, time.Second, uns.QualityGood, RuleGood},         // 2 samples, 1s
				{5, time.Second, uns.QualityGood, RuleGood},         // 3 samples, 2s (< 3s)
				{5, time.Second, uns.QualityFlatline, RuleFlatline}, // 4 samples, 3s
				{5, time.Second, uns.QualityFlatline, RuleFlatline}, // still flat
				{6, time.Second, uns.QualityGood, RuleGood},         // changed
				{6, 4 * time.Second, uns.QualityGood, RuleGood},     // 2 samples only
				{6, time.Second, uns.QualityFlatline, RuleFlatline}, // 3 samples, 5s
			},
		},
		{
			name: "flatline disabled by default",
			cfg:  Config{},
			steps: []step{
				{1, 0, uns.QualityGood, RuleGood},
				{1, time.Hour, uns.QualityGood, RuleGood},
				{1, time.Hour, uns.QualityGood, RuleGood},
				{1, time.Hour, uns.QualityGood, RuleGood},
				{1, time.Hour, uns.QualityGood, RuleGood},
				{1, time.Hour, uns.QualityGood, RuleGood},
			},
		},
		{
			name: "range beats flatline",
			cfg:  Config{HasRange: true, Min: 0, Max: 10, FlatlineAfter: time.Second, FlatlineMinSamples: 2},
			steps: []step{
				{50, 0, uns.QualityOutOfRange, RuleRange},
				{50, time.Second, uns.QualityOutOfRange, RuleRange},
				{50, time.Second, uns.QualityOutOfRange, RuleRange},
			},
		},
		{
			name: "epsilon treats noise as unchanged",
			cfg:  Config{FlatlineAfter: time.Second, FlatlineMinSamples: 2, Epsilon: 0.01},
			steps: []step{
				{1.000, 0, uns.QualityGood, RuleGood},
				{1.005, time.Second, uns.QualityFlatline, RuleFlatline},
				{1.100, time.Second, uns.QualityGood, RuleGood},
			},
		},
		{
			name: "non-finite is BAD",
			cfg:  Config{},
			steps: []step{
				{math.NaN(), 0, uns.QualityBad, RuleComm},
				{math.Inf(1), time.Second, uns.QualityBad, RuleComm},
				{1, time.Second, uns.QualityGood, RuleGood},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := New(tc.cfg)
			at := t0
			for i, s := range tc.steps {
				at = at.Add(s.dt)
				got := e.Observe(s.v, at)
				assert.Equal(t, s.want, got.Quality, "step %d", i)
				assert.Equal(t, s.rule, got.Rule, "step %d", i)
				assert.Equal(t, got, e.Current())
				if s.want != uns.QualityGood {
					assert.NotEmpty(t, got.Reason)
				}
			}
		})
	}
}

func TestStaleTransition(t *testing.T) {
	e := New(Config{StaleAfter: 3 * time.Second})

	// Nothing seen yet → never stale.
	_, changed := e.Check(t0.Add(time.Hour))
	assert.False(t, changed)

	e.Observe(1, t0)
	_, changed = e.Check(t0.Add(2 * time.Second))
	assert.False(t, changed)

	v, changed := e.Check(t0.Add(3 * time.Second))
	require.True(t, changed)
	assert.Equal(t, uns.QualityStale, v.Quality)
	assert.Equal(t, RuleStale, v.Rule)

	// Only one transition is reported.
	v, changed = e.Check(t0.Add(10 * time.Second))
	assert.False(t, changed)
	assert.Equal(t, uns.QualityStale, v.Quality)

	// A new sample clears staleness.
	got := e.Observe(2, t0.Add(11*time.Second))
	assert.Equal(t, uns.QualityGood, got.Quality)
	_, changed = e.Check(t0.Add(12 * time.Second))
	assert.False(t, changed)

	last, at, ok := e.Last()
	assert.True(t, ok)
	assert.Equal(t, 2.0, last)
	assert.Equal(t, t0.Add(11*time.Second), at)
}

func TestStaleDisabled(t *testing.T) {
	e := New(Config{})
	e.Observe(1, t0)
	_, changed := e.Check(t0.Add(24 * time.Hour))
	assert.False(t, changed)
}

func TestComm(t *testing.T) {
	e := New(Config{StaleAfter: time.Second})
	e.Observe(1, t0)
	v := e.Comm(errors.New("connection refused"))
	assert.Equal(t, uns.QualityBad, v.Quality)
	assert.Equal(t, RuleComm, v.Rule)
	assert.Equal(t, "connection refused", v.Reason)

	// BAD is not downgraded to STALE while comm is down.
	_, changed := e.Check(t0.Add(time.Minute))
	assert.False(t, changed)

	v = e.Comm(nil)
	assert.Equal(t, "source read failed", v.Reason)

	// Recovery.
	assert.Equal(t, uns.QualityGood, e.Observe(3, t0.Add(2*time.Minute)).Quality)
}

func TestNewDefaults(t *testing.T) {
	e := New(Config{})
	assert.Equal(t, 5, e.Config().FlatlineMinSamples)
	assert.Equal(t, Good, e.Current())
}
