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

Each benchmark runs against both queue backends:

- `memory`
- `sqlite`

Benchmarks are implemented in:

- `internal/pullapi/bench_test.go`
- `internal/dispatcher/bench_test.go`

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

This reduces host variance and gives stable median trends across runs.

## Interpreting Results

- Focus first on `sec/op` deltas for the same benchmark/backend pair.
- Use `B/op` and `allocs/op` to catch regressions hidden by throughput changes.
- For SQLite, compare both single and batch paths; batch wins should show up most clearly in `AckBatch32`.
- For mixed profile runs, track `p95_ms`/`p99_ms` first, then check `ingress_rejects` and `drain_errors` to interpret latency shifts.
- For push saturation runs, track `p95_ms`/`p99_ms` and `ingress_rejects_queue_full` first, then compare `deliveries`.
- For push skewed-target runs, track `p95_ms`/`p99_ms`, `deliveries_slow`, and `ingress_rejects_queue_full`; improving slow-target drain without growing queue-full rejects or tail latency indicates better cross-target fairness.

## Notes

- Benchmark artifacts are written under `./.bench/` and ignored by git.
- `bench-pull-compare` uses a pinned `benchstat` module version in `Makefile` to avoid tool drift.
- For production threshold tuning of adaptive ingress pressure, use [Adaptive Backpressure Tuning](adaptive-backpressure.md).
