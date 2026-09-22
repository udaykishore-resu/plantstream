# ADR 0002: Config-as-code asset model and ISA-95 topic hierarchy

- Status: Accepted
- Date: 2026-03-02

## Context

The value of a UNS is that a message is self-describing. That requires an
authoritative model of what exists in the plant and how raw registers map to
engineering values. Historically this lived in a SCADA tag database or an
integrator's spreadsheet — unversioned, unreviewable, and different per site.

## Decision

- One `plant.yaml` per site is the single source of truth. It declares the
  ISA-95 hierarchy (`enterprise/site/area/line/cell`), the assets in each cell
  (id, class, description), their tags (unit, datatype, engineering range,
  scaling, quality overrides) and the sources that feed them (Modbus address,
  poll interval, ...). It is parsed with `KnownFields(true)` and validated
  exhaustively at startup; a bad file fails the pod before it publishes a byte
  (`internal/domain/asset`).
- Topic = `enterprise/site/area/line/cell/tag`. Segments are validated
  identifiers (no `/`, `+`, `#`, whitespace, no leading `$`). Topic uniqueness
  is enforced across the whole plant. Edge-node lifecycle uses the reserved
  segment `_edge`: `enterprise/site/_edge/<source>/BIRTH|DEATH`.
- Payload is JSON, Sparkplug-B-inspired: `type` (BIRTH/DATA/DEATH), epoch-ms
  `timestamp`, per-node wrapping `seq`, `node`, and `metrics[]` each with
  `name`, `datatype`, `value`, `quality` and `properties` carrying asset id,
  class, unit, engineering range, source and the quality rule that fired.
- The file is deployed as a ConfigMap by GitOps (`docs/gitops.md`), so a tag
  rename is a reviewed pull request, not a change in a vendor tool.

## Alternatives considered

- **Sparkplug-B binary (protobuf) payloads.** Better on the wire, but the
  ecosystem of plant tools that can read JSON off MQTT without a decoder is
  far larger, and our payload is small. We keep the Sparkplug semantics
  (BIRTH/DEATH, seq, metric metadata) and lose the encoding. Revisit if
  bandwidth becomes the constraint.
- **Asset id in the topic.** ISA-95 says cell → equipment; putting the asset
  id in the topic makes seven segments and breaks consumers that assume the
  standard six. The asset id is in every metric's properties instead, and the
  validator guarantees a cell does not publish two tags with the same name.
- **Runtime-editable model via API.** Rejected: config drift is the disease we
  are treating.

## Consequences

- Consumers can join at any time and understand any message without lookups.
- A bad plant.yaml is caught at startup with every problem listed, not one at
  a time.
- Adding a tag is a PR + rollout; there is no hot reload (roadmap).
