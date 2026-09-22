# ADR 0003: Deterministic, versioned data-quality rules

- Status: Accepted
- Date: 2026-03-05

## Context

Bad data is worse than no data: a frozen temperature reading that still looks
"live" silently corrupts every downstream KPI. Analytics teams routinely
discover, months later, that a sensor was flatlined or a PLC was unreachable
and the historian repeated the last value. Machine-learned anomaly detection
is often proposed here, but its verdicts are not explainable to an operator on
a 3 a.m. call.

## Decision

Quality is computed at the edge by four deterministic rules, each identified
by a versioned rule id that is stamped on the metric (`properties.quality_rule`)
and on the `plantstream_quality_transitions_total{rule=...}` metric:

| Rule                 | Code           | Fires when                                                       |
|----------------------|----------------|------------------------------------------------------------------|
| `quality.comm@v1`    | `BAD`          | the source poll failed, or the value is NaN/Inf                  |
| `quality.range@v1`   | `OUT_OF_RANGE` | the engineering value is outside `range.min..max`                |
| `quality.flatline@v1`| `FLATLINE`     | unchanged (± epsilon) for `flatline_after` and ≥ N samples       |
| `quality.stale@v1`   | `STALE`        | no sample for `stale_after` (default 3 × poll interval)          |

Severity order (worst wins): BAD > OUT_OF_RANGE > FLATLINE > STALE > GOOD.
Transitions are published once (with the last known value) rather than on
every poll, so an outage is one message, not a flood. Recovery publishes GOOD
with the fresh value.

Rule parameters come from plant.yaml (plant-wide defaults, per-tag overrides).
Changing what a rule *means* bumps its version; changing a threshold does not.

Machine learning is confined to the consumer side (e.g. predictive
maintenance) where a human can review its output; it never decides quality on
the edge.

## Consequences

- Every quality code can be explained by pointing at a rule and a threshold.
- Flatline is disabled unless configured, because slowly-changing tags (state
  enums, counters at standstill) would otherwise be flagged constantly.
- Rules are pure functions over one tag's history; they are 100% covered by
  table-driven tests and trivially cheap.
- Cross-tag rules (e.g. speed > 0 while state = STOPPED) are not in scope;
  they belong to a consumer with a wider view.
