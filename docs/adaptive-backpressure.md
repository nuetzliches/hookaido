# Adaptive Backpressure Tuning

This guide provides a production-focused tuning workflow for `defaults.adaptive_backpressure`.

Goal:
- stabilize ingress tail latency (p95/p99) before hard queue saturation
- reduce hard `queue_full` rejects (`503`) without over-throttling ingress

## Prerequisites

Enable metrics and collect:

- `hookaido_queue_total`
- `hookaido_queue_ready_lag_seconds`
- `hookaido_queue_oldest_queued_age_seconds`
- `hookaido_ingress_adaptive_backpressure_applied_total`
- `hookaido_ingress_adaptive_backpressure_total{reason}`
- `hookaido_ingress_rejected_by_reason_total{reason,status}`

Also capture ingress request latency percentiles from your HTTP telemetry (`p95`, `p99`).

## Starting Profiles

Pick one profile and adjust from there.

| Profile | Use when | `min_total` | `queued_percent` | `ready_lag` | `oldest_queued_age` | `sustained_growth` |
| --- | --- | ---: | ---: | --- | --- | --- |
| `balanced` | default for most teams | `200` | `80` | `30s` | `60s` | `on` |
| `latency_first` | strict p95/p99 protection | `100` | `72` | `15s` | `30s` | `on` |
| `throughput_first` | fewer early rejects, more queue elasticity | `400` | `88` | `45s` | `90s` | `on` |

Example (`latency_first`):

```hcl
defaults {
  adaptive_backpressure {
    enabled on
    min_total 100
    queued_percent 72
    ready_lag 15s
    oldest_queued_age 30s
    sustained_growth on
  }
}
```

## Tuning Loop

Run each step with production-like traffic for at least 15-30 minutes.

1. Set one starting profile.
2. Observe latency (`p95`/`p99`), adaptive rejects, and hard queue rejects.
3. Change one parameter at a time.
4. Keep the change only if tail latency improves without unacceptable throughput loss.

## Decision Matrix

If `p95/p99` rises and `queue_full` rejects increase before adaptive rejects appear:
- lower `queued_percent` by `3-5`
- lower `ready_lag` by `5-10s`
- lower `oldest_queued_age` by `10-20s`
- optionally lower `min_total` by `50-100`

If adaptive rejects are high but queue lag/age stay low:
- raise `queued_percent` by `3-5`
- raise `ready_lag` by `5-10s`
- raise `oldest_queued_age` by `10-20s`

If `reason="sustained_growth"` dominates during normal bursts:
- keep `sustained_growth on`
- raise `min_total` gradually so short bursts do not trigger early

## Guardrails

- Keep hard `queue_limits.max_depth` as the final safety boundary.
- Avoid large jumps; use small deltas and re-measure.
- In bursty enterprise workloads, keep `sustained_growth on` unless you have strong evidence to disable it.
- Treat adaptive rejects as an intentional control signal, not only as an error.

## Benchmark Alignment

Use Pull benchmarks (`make bench-pull*`, `make bench-pull-mixed*`) to compare internal queue/drain optimization slices.

Use production-like ingress load tests for threshold tuning decisions. The benchmark suite is best for relative runtime regressions/improvements, while threshold tuning depends on workload shape and SLO targets.

## Dashboard Compatibility

When dashboards span versions, gate adaptive panels/alerts with:

- `hookaido_metrics_schema_info{schema="1.2.0"} == 1`

This avoids interpreting missing pre-`1.2.0` series as zero.
