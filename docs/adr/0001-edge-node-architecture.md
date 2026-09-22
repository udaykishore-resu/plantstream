# ADR 0001: Edge node architecture — collectors → UNS → bridge, one process per site

- Status: Accepted
- Date: 2026-03-02

## Context

Plants accumulate one integration per consumer: the MES polls PLC A, the
historian polls PLC A again, a data-science notebook scrapes the historian.
Every plant ends up a snowflake because context (what asset, what unit, what
range) lives in each consumer's head, not in the data. The Unified Namespace
(UNS) pattern fixes this by publishing every signal once, with context, on a
hierarchical topic tree that every consumer subscribes to.

We need a component that sits at the OT/IT boundary, talks industrial
protocols, produces that contextualised stream, and forwards it to the cloud
without losing data when the WAN is down.

## Decision

One Go process (`cmd/plantstream`) per site, composed of four stages behind
small interfaces:

1. **Collectors** (`internal/collectors`): a `Source` interface with a shared
   `Poller` that owns scheduling and per-source health. Implementations: Modbus
   TCP (own protocol implementation, see ADR 0006), an in-process line
   simulator and a CSV replayer.
2. **Edge pipeline** (`internal/edge`): contextualises each reading from the
   validated asset model (ADR 0002), runs the quality rules (ADR 0003), builds
   Sparkplug-B-inspired payloads and publishes them to the UNS broker port.
3. **UNS transport** (`internal/ports.Broker`): MQTT v5 via paho.golang in
   production, an in-process pub/sub with the same wildcard semantics for tests
   and `make run`.
4. **Bridge** (`internal/bridge`): subscribes to the broker and forwards to a
   `Sink` (Kafka via franz-go) with store-and-forward and per-topic
   backpressure (ADR 0004).

The process also serves a read-only HTTP API (assets, latest values, topics,
source health, SSE stream) used by operators and local dashboards.

Scaling unit is the *site*: one Deployment per site (Helm release), never
multiple replicas polling the same PLC. PLCs have tight connection limits and
duplicate pollers double the load on the weakest link.

## Consequences

- Zero-infrastructure local run: swap the two adapters for their in-memory
  twins and the whole pipeline runs in one binary (`make run`).
- Every stage is unit-testable in isolation; the pipeline test uses a scripted
  source and the memory broker.
- A single process per site is a single point of failure for that site's data.
  Mitigation: Kubernetes restarts it, BIRTH/DEATH make the outage visible on
  the UNS, and the on-disk queue survives restarts. Active/passive failover is
  on the roadmap, not in scope.
- Consumers never talk to PLCs; they subscribe to the UNS or read Kafka.
