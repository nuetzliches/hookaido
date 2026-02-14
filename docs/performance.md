# Performance Baselines

This page defines the reproducible benchmark workflow used for #39 optimization slices across Pull and Push drain paths.

## Scope

Current benchmarks cover:

- dequeue + `ack` (single)
- dequeue + `ack` (batch size 15, sustained-drain profile)
- dequeue + `ack` (batch size 32)
- dequeue + `nack` (single)
- dequeue + repeated `extend` on an active lease (single)
- duplicate `ack`/`nack` retry path under parallel load (contention profile)
- mixed ingress + pull drain profile with latency percentiles (`p95_ms`, `p99_ms`)
- mixed ingress + push drain saturation profile with ingress reject and delivery counts
- mixed ingress + push skewed-target saturation profile (fast + slow target) for cross-target drain fairness checks
- adaptive backpressure A/B runtime harness (`off` vs `on`) for mixed-focused saturation analysis (with optional pull reference), final metrics/health artifacts, and side-by-side comparison tables including Pull ACK conflict ratio

Each benchmark runs against both queue backends:

- `memory`
- `sqlite`

Benchmarks are implemented in:

- `internal/pullapi/bench_test.go`
- `internal/dispatcher/bench_test.go`
- `scripts/adaptive-ab.sh` (runtime A/B harness, non-`go test`)

## Reproducible Runbook

Run all commands from the repository root.

1. Capture a baseline before changes:

```bash
make bench-pull-baseline
```

This writes `./.bench/pull-baseline.txt`.

2. Apply code changes.

3. Capture current results:

```bash
make bench-pull
```

This writes `./.bench/pull.txt`.

4. Compare baseline vs current:

```bash
make bench-pull-compare
```

This writes `./.bench/pull-compare.txt` and prints a `benchstat` diff table.

### Isolated Extend Check

When you want to validate only the active-lease `extend` path:

```bash
make bench-pull-extend
make bench-pull-extend-compare
```

This uses a longer, higher-count run to reduce variance from unrelated Pull-path benchmarks.

### Sustained Drain Check (Batch 15)

For an issue-#39-style pull workload (dequeue + batch ack with `batch=15`):

```bash
make bench-pull-drain-baseline
make bench-pull-drain
make bench-pull-drain-compare
```

This writes:

- `./.bench/pull-drain-baseline.txt`
- `./.bench/pull-drain.txt`
- `./.bench/pull-drain-compare.txt`

The drain profile uses a longer run (`-benchtime=5s`, `-count=10`) for lower variance.

### ACK/NACK Contention Check

For high-parallel duplicate-retry pressure on Pull `ack`/`nack`:

```bash
make bench-pull-contention-baseline
make bench-pull-contention
make bench-pull-contention-compare
```

This writes:

- `./.bench/pull-contention-baseline.txt`
- `./.bench/pull-contention.txt`
- `./.bench/pull-contention-compare.txt`

The contention profile runs with `GOMAXPROCS=4` and `-cpu 1,4` to expose scaling behavior and conflict-path costs.

### Mixed Ingress + Drain Tail-Latency Check

For a mixed workload (concurrent ingress writes while pull workers dequeue+ack in the background):

```bash
make bench-pull-mixed-baseline
make bench-pull-mixed
make bench-pull-mixed-compare
```

This writes:

- `./.bench/pull-mixed-baseline.txt`
- `./.bench/pull-mixed.txt`
- `./.bench/pull-mixed-compare.txt`

`BenchmarkMixedIngressDrain` also reports custom metrics per backend:

- `p95_ms`
- `p99_ms`
- `ingress_rejects`
- `drain_errors`

### Push Ingress + Drain Saturation Check

For push-mode saturation behavior (ingress while dispatcher drains a single-target route):

```bash
make bench-push-mixed-baseline
make bench-push-mixed
make bench-push-mixed-compare
```

This writes:

- `./.bench/push-mixed-baseline.txt`
- `./.bench/push-mixed.txt`
- `./.bench/push-mixed-compare.txt`

`BenchmarkPushIngressDrainSaturation` reports:

- `ingress_rejects`
- `ingress_rejects_queue_full`
- `ingress_rejects_adaptive_backpressure`
- `ingress_rejects_memory_pressure`
- `ingress_rejects_other`
- `p95_ms`
- `p99_ms`
- `deliveries`

### Push Ingress + Skewed-Target Saturation Check

For push-mode cross-target fairness under saturation (single route with one fast and one slow target):

```bash
make bench-push-skewed-baseline
make bench-push-skewed
make bench-push-skewed-compare
```

This writes:

- `./.bench/push-skewed-baseline.txt`
- `./.bench/push-skewed.txt`
- `./.bench/push-skewed-compare.txt`

`BenchmarkPushIngressDrainSkewedTargets` reports:

- `ingress_rejects`
- `ingress_rejects_queue_full`
- `ingress_rejects_adaptive_backpressure`
- `ingress_rejects_memory_pressure`
- `ingress_rejects_other`
- `p95_ms`
- `p99_ms`
- `deliveries_fast`
- `deliveries_slow`

### Adaptive Backpressure A/B Runtime Check (Issues #53/#54/#55)

For explicit `adaptive_backpressure.enabled=off` vs `on` runs (same load profile):

```bash
make adaptive-ab
```

`make adaptive-ab` defaults to the currently open validation scope (`mixed`).

Scenario-specific runs:

```bash
make adaptive-ab-pull
make adaptive-ab-mixed
make adaptive-ab-all
make adaptive-ab-mixed-saturation
```

`make adaptive-ab-all` executes:

- `pull-off`, `pull-on` (reference profile)
- `mixed-off`, `mixed-on` (remaining decision profile)

`make adaptive-ab-mixed-saturation` is a calibrated high-pressure profile for issue validation (`#53/#54/#55`):

- duration: `30s` per mode
- ingress workers: `256`
- mixed drain workers: `8`
- dequeue batch: `5`
- queue max depth: `2000`

Use it when baseline `make adaptive-ab` does not reach sustained pressure on your host.

Decision note: these runs are intended for relative same-host A/B evidence and tuning guidance. They are not a standalone basis for global default policy across heterogeneous production hardware.

Artifacts are written under:

- `./.artifacts/adaptive-ab/<run-id>/<scenario>-<mode>/`

Each run directory includes:

- `final-metrics.txt`
- `final-health.json`
- `monitor-output.log`
- `run-meta.json` (binary hash/version, git revision, runtime profile)
- `summary.env` and `summary.json`

Comparison tables are generated as:

- `./.artifacts/adaptive-ab/<run-id>/comparison-pull.md`
- `./.artifacts/adaptive-ab/<run-id>/comparison-mixed.md`
- `./.artifacts/adaptive-ab/<run-id>/comparison.md`

The comparison table includes:

- `hookaido_ingress_adaptive_backpressure_applied_total`
- `hookaido_ingress_rejected_by_reason_total{reason="adaptive_backpressure",status="503"}`
- `hookaido_ingress_rejected_by_reason_total{reason="queue_full",status="503"}`
- `hookaido_queue_ready_lag_seconds`
- `hookaido_queue_oldest_queued_age_seconds`
- ingress `p95_ms` / `p99_ms`
- accepted request rate (requests/sec)
- `hookaido_pull_acked_total` (sum across routes)
- `hookaido_pull_ack_conflict_total` (sum across routes)
- `hookaido_pull_nack_conflict_total` (sum across routes)
- `pull_ack_conflict_ratio_percent` (`ack_conflict / acked * 100`)

## Reproducibility Defaults

The Make targets enforce:

- `GOMAXPROCS=1`
- `-cpu 1`
- `-count=5` and `-benchtime=3s` for the default pull suite
- `-count=10` and `-benchtime=5s` for isolated extend/drain profiles
- `GOMAXPROCS=4` and `-cpu 1,4` for the contention profile
- `GOMAXPROCS=4` and `-cpu 4` for the mixed ingress+drain profile
- `GOMAXPROCS=4` and `-cpu 4` for the push saturation profile
- `GOMAXPROCS=4` and `-cpu 4` for the push skewed-target profile
- adaptive A/B harness defaults: `duration=120s`, `ingress_workers=16`, `mixed_drain_workers=8`, `dequeue_batch=15`, `queue_max_depth=50000`

This reduces host variance and gives stable median trends across runs.

## Interpreting Results

- Focus first on `sec/op` deltas for the same benchmark/backend pair.
- Use `B/op` and `allocs/op` to catch regressions hidden by throughput changes.
- For SQLite, compare both single and batch paths; batch wins should show up most clearly in `AckBatch32`.
- For mixed profile runs, track `p95_ms`/`p99_ms` first, then check `ingress_rejects` and `drain_errors` to interpret latency shifts.
- For push saturation runs, track `p95_ms`/`p99_ms` and `ingress_rejects_queue_full` first, then compare `deliveries`.
- For push skewed-target runs, track `p95_ms`/`p99_ms`, `deliveries_slow`, and `ingress_rejects_queue_full`; improving slow-target drain without growing queue-full rejects or tail latency indicates better cross-target fairness.
- For adaptive A/B runs, first confirm `adaptive_applied_total=0` in `off`, then compare `queue_full` delta and latency/rate trade-offs in `on`.
- For mixed A/B (`#55`), track `pull_ack_conflict_ratio_percent` alongside ingress metrics; large conflict-ratio regressions can hide behind stable ingress acceptance.
- Keep policy decisions tied to workload SLOs: same-host gains do not imply cross-environment default changes.

## Notes

- Benchmark artifacts are written under `./.bench/` and ignored by git.
- Adaptive A/B artifacts are written under `./.artifacts/adaptive-ab/` and ignored by git.
- `bench-pull-compare` uses a pinned `benchstat` module version in `Makefile` to avoid tool drift.
- For production threshold tuning of adaptive ingress pressure, use [Adaptive Backpressure Tuning](adaptive-backpressure.md).
