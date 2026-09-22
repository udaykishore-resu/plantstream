// Package quality implements deterministic, versioned data-quality rules for
// tag streams: communication failure, out-of-range, flatline and staleness.
// Every verdict names the rule (and version) that fired so downstream
// consumers and auditors can explain a quality code.
package quality

import (
	"fmt"
	"math"
	"time"

	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
)

// Rule identifiers, versioned. Bump the version when the semantics change.
const (
	RuleComm     = "quality.comm@v1"
	RuleRange    = "quality.range@v1"
	RuleFlatline = "quality.flatline@v1"
	RuleStale    = "quality.stale@v1"
	RuleGood     = "quality.good@v1"
)

// Config tunes one tag's evaluator.
type Config struct {
	// StaleAfter: no sample for this long → STALE. Zero disables.
	StaleAfter time.Duration
	// FlatlineAfter: value unchanged for this long → FLATLINE. Zero disables.
	FlatlineAfter time.Duration
	// FlatlineMinSamples: minimum unchanged samples before FLATLINE can fire.
	FlatlineMinSamples int
	// Min/Max: engineering range; HasRange enables the check.
	HasRange bool
	Min, Max float64
	// Epsilon: values closer than this count as unchanged (default 0 → exact).
	Epsilon float64
}

// Verdict is the outcome of a rule evaluation.
type Verdict struct {
	Quality uns.Quality
	Rule    string
	Reason  string
}

// Good is the verdict for a healthy sample.
var Good = Verdict{Quality: uns.QualityGood, Rule: RuleGood}

// Evaluator tracks one tag's history. It is not safe for concurrent use.
type Evaluator struct {
	cfg Config

	seen       bool
	last       float64
	lastAt     time.Time
	lastChange time.Time
	unchanged  int
	// current is the last verdict emitted (for change detection in Check).
	current Verdict
	// commDown is set by Comm and cleared by the next Observe.
	commDown bool
}

// New creates an evaluator.
func New(cfg Config) *Evaluator {
	if cfg.FlatlineMinSamples <= 0 {
		cfg.FlatlineMinSamples = 5
	}
	return &Evaluator{cfg: cfg, current: Good}
}

// Config returns the effective configuration.
func (e *Evaluator) Config() Config { return e.cfg }

// Observe records a sample taken at `at` and returns its quality.
// Rule order (worst wins): range → flatline → good. Staleness is a property of
// the absence of samples, so it is evaluated by Check, not here.
func (e *Evaluator) Observe(v float64, at time.Time) Verdict {
	e.commDown = false
	if math.IsNaN(v) || math.IsInf(v, 0) {
		e.lastAt = at
		e.current = Verdict{Quality: uns.QualityBad, Rule: RuleComm, Reason: "value is not a finite number"}
		return e.current
	}
	if e.seen && math.Abs(v-e.last) <= e.cfg.Epsilon {
		e.unchanged++
	} else {
		e.unchanged = 1
		e.lastChange = at
	}
	e.seen = true
	e.last = v
	e.lastAt = at

	if e.cfg.HasRange && (v < e.cfg.Min || v > e.cfg.Max) {
		e.current = Verdict{
			Quality: uns.QualityOutOfRange, Rule: RuleRange,
			Reason: fmt.Sprintf("value %g outside engineering range [%g, %g]", v, e.cfg.Min, e.cfg.Max),
		}
		return e.current
	}
	if e.cfg.FlatlineAfter > 0 && e.unchanged >= e.cfg.FlatlineMinSamples && at.Sub(e.lastChange) >= e.cfg.FlatlineAfter {
		e.current = Verdict{
			Quality: uns.QualityFlatline, Rule: RuleFlatline,
			Reason: fmt.Sprintf("value unchanged for %s over %d samples", at.Sub(e.lastChange).Truncate(time.Millisecond), e.unchanged),
		}
		return e.current
	}
	e.current = Good
	return e.current
}

// Comm records a communication failure and returns a BAD verdict.
func (e *Evaluator) Comm(err error) Verdict {
	e.commDown = true
	reason := "source read failed"
	if err != nil {
		reason = err.Error()
	}
	e.current = Verdict{Quality: uns.QualityBad, Rule: RuleComm, Reason: reason}
	return e.current
}

// Check evaluates staleness at time now. It returns (verdict, true) only when
// the tag has just become STALE, so callers can publish a single transition
// instead of one message per tick. A tag that is BAD (comm down) is left as is:
// BAD is more severe than STALE.
func (e *Evaluator) Check(now time.Time) (Verdict, bool) {
	if e.cfg.StaleAfter <= 0 || !e.seen || e.commDown {
		return e.current, false
	}
	if e.current.Quality == uns.QualityStale {
		return e.current, false
	}
	age := now.Sub(e.lastAt)
	if age < e.cfg.StaleAfter {
		return e.current, false
	}
	e.current = Verdict{
		Quality: uns.QualityStale, Rule: RuleStale,
		Reason: fmt.Sprintf("no sample for %s (limit %s)", age.Truncate(time.Millisecond), e.cfg.StaleAfter),
	}
	return e.current, true
}

// Current returns the last verdict without evaluating anything.
func (e *Evaluator) Current() Verdict { return e.current }

// Last returns the last finite value observed and whether one exists.
func (e *Evaluator) Last() (float64, time.Time, bool) { return e.last, e.lastAt, e.seen }
