# Runbook

## Service level objectives

| SLO | Target | Measured by |
|---|---|---|
| Freshness: share of tags with `GOOD` quality | ≥ 99% per site over 30 d | `sum(plantstream_quality_tags{quality="GOOD"}) / sum(plantstream_quality_tags)` |
| Source availability | ≥ 99.5% of poll cycles succeed | `rate(plantstream_source_polls_total{result="ok"}[5m]) / rate(plantstream_source_polls_total[5m])` |
| Cloud delivery lag (p99) | ≤ 5 s while WAN is up | `histogram_quantile(0.99, rate(plantstream_bridge_delivery_lag_seconds_bucket[5m]))` |
| Data loss | 0 evictions / coalesced samples per month | `increase(plantstream_bridge_queue_evicted_total[30d])`, `increase(plantstream_bridge_coalesced_total[30d])` |
| API availability | 99.9% of requests < 500 ms and non-5xx | `plantstream_http_requests_total`, `plantstream_http_request_duration_seconds` |

## Metrics

| Metric | Type | Meaning |
|---|---|---|
| `plantstream_http_requests_total{method,route,status}` | counter | RED: requests |
| `plantstream_http_request_duration_seconds{method,route}` | histogram | RED: latency |
| `plantstream_http_inflight_requests` | gauge | in-flight requests |
| `plantstream_sse_clients` | gauge | connected `/v1/stream` clients |
| `plantstream_source_up{source}` | gauge | 1 if last poll succeeded |
| `plantstream_source_polls_total{source,result}` | counter | poll cycles ok/error |
| `plantstream_source_poll_duration_seconds{source}` | histogram | PLC round trip per cycle |
| `plantstream_samples_total{source}` | counter | tag samples collected |
| `plantstream_uns_published_total{type}` | counter | BIRTH/DATA/DEATH published |
| `plantstream_uns_publish_errors_total` | counter | broker publish failures |
| `plantstream_quality_transitions_total{quality,rule}` | counter | quality changes by rule version |
| `plantstream_quality_tags{quality}` | gauge | tags currently in each quality code |
| `plantstream_bridge_forwarded_total` | counter | messages accepted by the sink |
| `plantstream_bridge_spilled_total{reason}` | counter | messages written to the disk queue |
| `plantstream_bridge_coalesced_total` | counter | oldest samples dropped by per-topic backpressure |
| `plantstream_bridge_queue_depth` / `_queue_bytes` | gauge | store-and-forward backlog |
| `plantstream_bridge_queue_evicted_total` | gauge | records evicted by the byte budget (data loss) |
| `plantstream_bridge_sink_up` | gauge | 1 while Kafka accepts sends |
| `plantstream_bridge_sink_errors_total` | counter | failed sends |
| `plantstream_bridge_delivery_lag_seconds` | histogram | edge timestamp → Kafka ack |

## Alerts (Prometheus rules)

```yaml
groups:
  - name: plantstream
    rules:
      - alert: PlantstreamSourceDown
        expr: plantstream_source_up == 0
        for: 2m
        labels: { severity: warning }
        annotations: { summary: "Source {{ $labels.source }} unreachable for 2m" }
      - alert: PlantstreamQualityDegraded
        expr: sum by (instance) (plantstream_quality_tags{quality!="GOOD"}) / sum by (instance) (plantstream_quality_tags) > 0.05
        for: 10m
        labels: { severity: warning }
        annotations: { summary: "More than 5% of tags are not GOOD on {{ $labels.instance }}" }
      - alert: PlantstreamSinkDown
        expr: plantstream_bridge_sink_up == 0
        for: 5m
        labels: { severity: warning }
        annotations: { summary: "Kafka unreachable; buffering to disk ({{ $value }} queued)" }
      - alert: PlantstreamQueueNearBudget
        expr: plantstream_bridge_queue_bytes / on() group_left() (256*1024*1024) > 0.8
        for: 5m
        labels: { severity: critical }
        annotations: { summary: "Store-and-forward queue above 80% of budget; data loss imminent" }
      - alert: PlantstreamDataLoss
        expr: increase(plantstream_bridge_queue_evicted_total[15m]) > 0 or increase(plantstream_bridge_coalesced_total[15m]) > 0
        labels: { severity: critical }
        annotations: { summary: "Samples were dropped on {{ $labels.instance }}" }
      - alert: PlantstreamNotReady
        expr: up{job="plantstream"} == 0 or probe_success{job="plantstream-readyz"} == 0
        for: 3m
        labels: { severity: critical }
```

## Dashboards

Grafana panels worth having per site (all from the metrics above):

1. **Quality mix** – stacked `plantstream_quality_tags` by code; a healthy site
   is a flat green band.
2. **Source health** – `source_up` state timeline + `poll_duration_seconds` p95
   per source. Rising latency precedes PLC connection limits.
3. **Bridge** – `sink_up`, `queue_depth`, `queue_bytes` vs budget,
   `delivery_lag_seconds` p99, `forwarded_total` rate.
4. **API RED** – requests/s by route, error ratio, latency p95, SSE clients.
5. **Quality transitions** – `rate(quality_transitions_total[5m])` by rule to
   spot flapping sensors.

## Common failures

### A source shows `up: false`

1. `curl :8080/v1/health/sources` – read `last_error`.
   - `connect ... connection refused` / `i/o timeout`: network or PLC down.
     Check the PLC, firewall and the NetworkPolicy egress CIDRs.
   - `modbus: function 0x03: illegal data address`: the register map in
     plant.yaml does not match the device. Compare with the vendor map; the
     read planner coalesces addresses, so a gap that includes an unmapped
     register also fails — lower `max_gap` for that source.
   - `illegal function`: the device does not support FC 4 (input registers);
     change `function: input` to `holding`.
2. Tags of that source are `BAD` with their last value; consumers should
   already be ignoring them. Nothing to do on the UNS side.

### Tags are `STALE` although the source is up

The source returns no readings (CSV exhausted, simulator paused) or the poll
interval is longer than `stale_after`. Set `quality.stale_after` ≥ 2 × the
slowest poll interval.

### Tags flap between `FLATLINE` and `GOOD`

`flatline_after` is too short for a slowly changing signal, or the sensor
really is intermittent. Raise `flatline_after` / `flatline_min_samples` for
the tag or disable flatline for enum/counter tags (leave it unset).

### `/readyz` returns 503

Components are listed in the body. `broker: not connected` means MQTT is down
or credentials are wrong; the node keeps collecting but cannot publish.
Fix the broker first — the node reconnects automatically.

### `bridge_sink_up == 0`

Kafka is unreachable. Nothing is lost while `queue_bytes` is below the budget.
Check `queue_bytes` growth rate against the remaining budget to estimate time
to eviction; if the outage will outlast it, raise `PLANTSTREAM_QUEUE_MAX_BYTES`
(and the PVC) *before* eviction starts — the queue is bounded by bytes, not
time.

### Queue evictions or coalescing happened

Data was lost. Check Kafka consumer lag: `coalesced_total` means the sink was
*slow*, not down — usually a Kafka broker near capacity or a WAN link
saturated. Raise `PLANTSTREAM_BRIDGE_PER_TOPIC_MAX` only if memory allows; the
real fix is downstream capacity.

## Operational procedures

### Rolling out a plant.yaml change

Change the file in the site's GitOps repo → PR → Argo CD syncs the ConfigMap →
the Deployment's checksum annotation changes → pod restarts (strategy
Recreate). Validate locally first:

```sh
PLANTSTREAM_PLANT_FILE=path/to/plant.yaml PLANTSTREAM_HTTP_ADDR=:0 go run ./cmd/plantstream
```

A validation error lists every problem and exits non-zero.

### Rollback

`helm rollback plantstream <rev>` or revert the Git commit. The disk queue
format is forward and backward compatible within a major version; a
downgraded node replays whatever the upgraded one queued.

### Draining a node

`kubectl rollout restart deployment/plantstream` sends SIGTERM: pollers stop,
`DEATH` messages are published, in-memory bridge messages are flushed to the
queue, HTTP drains for `PLANTSTREAM_SHUTDOWN_TIMEOUT`. Nothing needs manual
intervention.

## Security notes

The HTTP API is unauthenticated and read-only. Keep the Service `ClusterIP`
and let the NetworkPolicy restrict ingress to monitoring and operator
namespaces. MQTT/Kafka credentials come from Secrets referenced in values.
