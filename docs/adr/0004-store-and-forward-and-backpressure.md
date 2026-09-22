# ADR 0004: Store-and-forward with a bounded on-disk queue and per-topic backpressure

- Status: Accepted
- Date: 2026-03-09

## Context

Plant WAN links fail; Kafka clusters get upgraded. The edge must keep
collecting and must not lose the data it collected while the link was down —
but it also must not fill its disk and take the whole edge box down, and one
chatty tag must not starve the others.

## Decision

The bridge has two independent protections:

1. **Store-and-forward (sink outage).** When a `Sink.Send` fails, the bridge
   marks the sink down, appends the message to a durable queue and switches to
   exponential-backoff probing. Every subsequent message is appended *in
   arrival order*. On recovery the queue is replayed to completion before live
   traffic resumes, so per-topic order is preserved end to end. The queue
   (`internal/adapters/diskqueue`) is a segmented append-only log: CRC-32 per
   record, torn tails truncated on recovery, a `head` cursor persisted on Ack.
   It is **bounded by bytes**; when full the *oldest segment* is deleted and
   the eviction is counted (`plantstream_bridge_queue_evicted_total`).
   In-memory messages are flushed to the queue on shutdown, so a rolling
   restart loses nothing.

2. **Per-topic backpressure (slow sink).** Between the broker callback and the
   forwarder sits an inbox of per-topic bounded FIFOs dequeued round-robin. A
   topic that outruns its FIFO loses its *oldest* buffered sample (latest value
   wins) and the drop is counted (`plantstream_bridge_coalesced_total`). Other
   topics are unaffected.

Delivery semantics to Kafka are at-least-once: a crash between `Send` and
`Ack` replays that record. Records are keyed by UNS topic, so consumers can
deduplicate on `(topic, seq, timestamp)` and log-compacted topics keep the
latest value per tag.

## Alternatives considered

- **Spill backpressure overflow to disk too.** Rejected: newer messages would
  land on disk *behind nothing* while older ones sit in memory, and the replay
  would reorder a topic's samples. Coalescing to the latest value is what an
  operator expects from process data; the drop is explicit and measured.
- **Drop-newest when the disk budget is exhausted.** Rejected: after a long
  outage the most valuable data is the most recent. Drop-oldest guarantees the
  newest sample always survives.
- **fsync every record.** Off by default (`PLANTSTREAM_QUEUE_SYNC=false`):
  process crashes are handled by the append-only design; only power loss can
  lose the last few records. Sites with unreliable power can turn it on.
- **Rely on the MQTT broker's persistent session.** Brokers buffer per client
  with opaque limits and no metrics; we would still need the disk queue for
  the Kafka leg.

## Consequences

- WAN outages up to the disk budget are invisible to consumers except for
  delivery lag (`plantstream_bridge_delivery_lag_seconds`).
- Disk usage is capped and predictable; the operator picks the budget.
- Data loss is possible only in two explicit, counted places: queue eviction
  and per-topic coalescing.
