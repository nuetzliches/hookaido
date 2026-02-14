# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Postgres queue backend (`queue { backend postgres }` / `queue postgres`) with durable queue semantics parity (lease lifecycle, message management, attempts, retention, and batch lease mutation support) plus runtime wiring via `hookaido run --postgres-dsn` (or `HOOKAIDO_POSTGRES_DSN`).
- Prometheus queue saturation gauges: `hookaido_queue_oldest_queued_age_seconds`, `hookaido_queue_ready_lag_seconds`, and `hookaido_queue_total` on `/metrics` (alongside `hookaido_queue_depth{state}`) for direct lag/age alerting without Admin health JSON scraping.
- Memory-backend observability on `/metrics`: `hookaido_store_memory_items{state}`, `hookaido_store_memory_retained_bytes{state}`, `hookaido_store_memory_retained_bytes_total`, and `hookaido_store_memory_evictions_total{reason}`.
- Ingress rejection breakdown counters via `hookaido_ingress_rejected_by_reason_total{reason,status}` now include `memory_pressure` (`status="503"`) for memory-backend pressure rejects.
- Pull API `POST {endpoint}/ack` and `POST {endpoint}/nack` now support batch form via `lease_ids` (up to 100 IDs per request), returning aggregate success/conflict output for high-throughput worker lease operations.
- Optional gRPC worker listener via `pull_api.grpc_listen`: starts WorkerService (`Dequeue`, `Ack`, `Nack`, `Extend`) on shared Pull operation semantics, applies `pull_api.tls` for TLS/mTLS, enforces dedicated-listener conflict guards, and keeps Pull token auth parity (global + per-route override).
- Backend-agnostic store runtime metric families on `/metrics`: `hookaido_store_operation_seconds{backend,operation}` (histogram), `hookaido_store_operation_total{backend,operation}`, and `hookaido_store_errors_total{backend,operation,kind}` to support backend-neutral dashboards and future PostgreSQL wiring.
- Reproducible Pull benchmark workflow docs (`docs/performance.md`) plus Make targets for baseline/current capture and `benchstat` diff (`bench-pull-baseline`, `bench-pull`, `bench-pull-compare`).
- Isolated Extend benchmarking targets (`bench-pull-extend`, `bench-pull-extend-compare`) for lower-variance validation of active-lease Pull `extend` changes.
- Sustained-drain Pull benchmarking targets (`bench-pull-drain-baseline`, `bench-pull-drain`, `bench-pull-drain-compare`) focused on dequeue + batch `ack` with `batch=15`.
- Pull contention benchmarking targets (`bench-pull-contention-baseline`, `bench-pull-contention`, `bench-pull-contention-compare`) focused on parallel duplicate-retry `ack`/`nack` paths.
- Mixed ingress+drain Pull benchmarking targets (`bench-pull-mixed-baseline`, `bench-pull-mixed`, `bench-pull-mixed-compare`) with per-backend tail-latency metrics (`p95_ms`, `p99_ms`) plus rejection/error counters.
- Mixed ingress+drain Push saturation benchmarking targets (`bench-push-mixed-baseline`, `bench-push-mixed`, `bench-push-mixed-compare`) via `BenchmarkPushIngressDrainSaturation` with `ingress_rejects` and `deliveries` metrics.
- Push skewed-target saturation benchmarking targets (`bench-push-skewed-baseline`, `bench-push-skewed`, `bench-push-skewed-compare`) via `BenchmarkPushIngressDrainSkewedTargets` with `ingress_rejects`, `deliveries_fast`, and `deliveries_slow`.
- Push saturation benchmarks now also emit reject-reason splits (`ingress_rejects_queue_full`, `ingress_rejects_adaptive_backpressure`, `ingress_rejects_memory_pressure`, `ingress_rejects_other`) for clearer ingress-vs-drain tuning under load.
- Push saturation/skewed benchmarks now emit ingress tail-latency metrics (`p95_ms`, `p99_ms`) for queue-pressure tuning based on latency guardrails, not only throughput/reject counters.
- Adaptive backpressure production tuning guide (`docs/adaptive-backpressure.md`) with data-driven starting profiles (`balanced`, `latency_first`, `throughput_first`), metric-driven decision matrix, and rollout guardrails for enterprise workloads.
- Metrics schema marker `hookaido_metrics_schema_info{schema="1.3.0"}` for dashboard compatibility gating across mixed Hookaido versions.

### Changed

- MCP queue tool backend routing now treats non-SQLite backends (`memory`, `postgres`) as Admin-proxy mode; local SQLite access remains only for `queue.backend sqlite`.
- Worker gRPC scope is now explicitly fixed to pull-worker lease transport (`dequeue`/`ack`/`nack`/`extend`) and documented as out-of-scope for admin/publish/control-plane and MCP lease mutation tools.
- SQLite runtime instrumentation now populates both backend-agnostic store metric families and legacy `hookaido_store_sqlite_*` series for compatibility during dashboard migration.
- PostgreSQL store runtime now populates backend-agnostic store metric families (`hookaido_store_operation_seconds`, `hookaido_store_operation_total`, `hookaido_store_errors_total`) with normalized error kinds for queue/store operations.
- Memory backend now emits explicit `ErrMemoryPressure` admission rejects when retained (non-active) memory footprint crosses pressure guard thresholds; ingress surfaces these as HTTP `503` with rejection reason `memory_pressure` instead of generic store-unavailable.
- Admin health diagnostics (`GET /healthz?details=1`) now include ingress `rejected_by_reason` and memory-store runtime diagnostics (`items_by_state`, retained bytes, eviction counters, and memory pressure state) when the memory backend is active.
- Ingress rejection breakdown metric `hookaido_ingress_rejected_by_reason_total{reason,status}` for bounded-cardinality attribution across queue pressure, adaptive backpressure, auth, routing, policy, and fallback reject paths.
- SQLite dequeue leasing now uses a bulk lease update per batch (single `UPDATE` with per-item lease IDs) instead of per-item update statements, reducing pull-path transaction roundtrips under sustained backlog.
- SQLite dequeue for `batch=1` now leases the next candidate via a single CTE-based `UPDATE ... RETURNING` statement, avoiding extra select/update roundtrips on the hottest pull path.
- Batch Pull `ack`/`nack`/`mark dead` lease handling now executes as true store-side batches (`MemoryStore`: single lock scope, `SQLiteStore`: single write transaction), avoiding per-lease transaction loops under high worker throughput.
- SQLite batch lease operations now use set-based batch lookup and bulk item mutation (`id IN (...)`) inside the single write transaction, reducing SQL roundtrips and allocations on Pull batch ack paths.
- SQLite single-lease Pull mutations (`ack`/`nack`/`extend`/`mark dead`) now use a lease-id fast path (direct mutation query + conflict resolution fallback) to reduce per-request allocations while preserving lease-expired requeue semantics.
- Pull API now treats recent duplicate retries of successful `ack`/`nack` lease operations as idempotent success, reducing avoidable `lease_conflict` churn under high retry/parallel worker pressure.
- Push dispatcher runtime now uses route-shared dequeue workers across all push routes (single- and multi-target), enforcing `deliver_concurrency` as a shared route budget and allowing idle-target capacity to drain active-target backlog under saturation.
- Push route workers now lease small dequeue micro-batches (single-target routes: bounded by route concurrency, capped at 4; multi-target routes: capped at 2) to balance throughput and fairness under saturation.
- Push dispatcher now applies batched lease mutations (`ack`/`nack`/`mark dead`) on single-target routes when the backend supports `LeaseBatchStore`, with automatic fallback to per-lease mutations and multi-target safety guardrails to avoid skewed-route regressions.

### Fixed

- Memory backend retention safety: with `delivered_retention` enabled, `queue_limits.max_depth` now also caps `queued + leased + delivered` items so sustained pull/ack traffic cannot grow delivered retention unbounded in RAM.

## [1.1.0] - 2026-02-12

### Added

- Official container publishing to GHCR (`ghcr.io/nuetzliches/hookaido`) via tag-triggered multi-arch workflow (`linux/amd64`, `linux/arm64`) with registry provenance attestation.
- Vault secret adapter for secret refs via `vault:...` (HashiCorp Vault-compatible HTTP API), including KV v1/v2 field extraction and env-configured namespace/TLS options.
- Optional strict secret preflight in config validation: `hookaido config validate --strict-secrets` and MCP `config_validate` argument `strict_secrets` now actively load refs to catch missing env vars, unreadable files, and Vault connectivity/access issues before runtime start.
- First-class Pull Prometheus metrics by route: dequeue totals (`status` labels `200|204|4xx|5xx`), ack/nack totals, ack/nack conflict totals, active lease gauge, and lease-expired totals.
- SQLite/store contention metrics on `/metrics`: write/dequeue/checkpoint duration histograms plus busy/retry, transaction commit/rollback, and checkpoint success/error counters.
- Adaptive ingress backpressure guardrails (`defaults.adaptive_backpressure`) with soft-pressure 503 admission before hard `max_depth`, plus reason-labeled Prometheus counters and health diagnostics (`adaptive_backpressure_applied_total`, `adaptive_backpressure_by_reason`).

### Changed

- Documentation UX refresh: docs navigation is now grouped by workflow area, `docs/index.md` is rebuilt as a landing page with quick-start/task-oriented entry points, search now supports command-palette style `Ctrl+K`, and docs stack evaluation is documented in `docs/documentation-platform.md` (decision: keep MkDocs Material for current roadmap window).
- CI now runs on pull requests and pushes to `main` only, and cancels superseded in-progress runs per ref to reduce duplicate workflow executions.
- Release workflow now exports Sigstore attestation bundles as `*.intoto.jsonl` assets (plus compatibility `*.attestation.json` copies), and `hookaido verify-release` now auto-detects either naming scheme with `.intoto.jsonl` preference.
- Control-plane hardening under saturation: `/metrics` queue depth and `/healthz?details=1` queue diagnostics now use short-TTL stale-while-refresh snapshots, reducing contention-coupled latency spikes during high queue pressure.
- SQLite `max_depth` admission checks now use trigger-maintained active-depth counters (`queue_counters`) instead of per-enqueue `COUNT(*)` scans, reducing write-path contention near saturation.

### Fixed

- Go module path now matches the repository path (`github.com/nuetzliches/hookaido`), fixing module resolution and `go install` for `cmd/hookaido`.
- Docker image build metadata flags now target the correct package variables, so `hookaido version --long` reports release metadata correctly in container builds.
- `config validate` now rejects invalid secret-reference schemes (`pull_api/admin_api auth token`, `pull.auth token`, direct `auth hmac` secrets, direct `deliver sign hmac`, and `secrets.value`) instead of failing later at runtime start.
- Docs landing page links now use MkDocs directory URLs (`/page/`) so GitHub Pages navigation no longer points to missing `*.md` endpoints.

## [1.0.0] - 2026-02-10

### Added

- **CLI:** `hookaido run`, `config fmt`, `config validate`, `config diff`, `mcp serve`, `verify-release`, `version` (with `--long`/`--json` build metadata).
- **Config DSL:** Caddyfile-inspired syntax with `config fmt` round-trip stability. Env/file/vars placeholders (`{$VAR}`, `{env.VAR}`, `{file.PATH}`, `{vars.NAME}`), multi-value directives, and hot reload via `--watch`/`SIGHUP`. Channel types: `inbound` (default), `outbound` (deliver required, no ingress directives), `internal` (pull required, no ingress directives) with wrapper and shorthand forms. Channel type compile constraints enforced. Route-level `publish.direct`/`publish.managed` dot-notation shorthand.
- **Config validation:** Deliver target URLs validated at compile time (must use `http`/`https` scheme with non-empty host). Deliver concurrency upper-bounded to 10 000.
- **VS Code Extension:** TextMate grammar syntax highlighting and snippets for Hookaidofile DSL (`editors/vscode/`).
- **Ingress:** HTTP server with optional TLS/mTLS. Route matching (path, method, host wildcards, headers, query, remote IP CIDR, named matchers). Per-route/global rate limiting (token-bucket, `429` on over-limit). Auth: Basic, HMAC (replay protection, secret rotation, nonce, tolerance), and forward auth callouts.
- **Pull API:** HTTP/JSON server (`POST {endpoint}/dequeue|ack|nack|extend`) with bearer-token auth, configurable dequeue limits (`max_batch`, lease TTL caps, long-poll wait caps), per-route token overrides, strict JSON parsing, and structured error responses.
- **Queue:** SQLite/WAL persistent store with leasing, ack/nack/extend, lease expiry requeue, long-poll, dead-lettering (`dead_reason`), queue limits (`max_depth`/`drop_policy`), retention/pruning, and duplicate-ID rejection. In-memory store for dev/tests. Per-route `queue { backend sqlite|memory }`.
- **Push Dispatcher:** Delivers queued items for `deliver` targets with retry/timeout/backoff, per-route concurrency enforcement, and optional per-target outbound HMAC signing (multi-secret rotation with `newest_valid`/`oldest_valid` selection).
- **Admin API:** DLQ management, message lifecycle (publish/cancel/requeue/resume by ID and by filter with `preview_only`), backlog drill-down (top queued, oldest, aging summary, trend rollups with operator-action playbooks), delivery attempts listing, management model projection, and endpoint mapping lifecycle (PUT/DELETE with config-source-of-truth mutations, atomic write, reload, rollback). Structured JSON errors, audit headers (`X-Hookaido-Audit-Reason`/`Actor`/`Request-Id`), publish-policy enforcement (direct/managed paths, route-level controls, actor identity hooks), and managed-ownership drift guardrails throughout.
- **MCP:** Stdio JSON-RPC server with role-gated access (`--role read|operate|admin`). Read tools: `config_parse`, `config_validate`, `config_compile`, `config_fmt_preview`, `config_diff`, `admin_health`, `management_model`, `backlog_*`, `messages_list`, `attempts_list`, `dlq_list`. Mutation tools (gated via `--enable-mutations`): `config_apply`, `management_endpoint_upsert`/`delete`, queue mutations, `messages_publish`. Runtime control tools (gated via `--enable-runtime-control`): `instance_status`, `instance_logs_tail`, `instance_start`/`stop`/`reload`. Principal-authoritative actor binding, JSONL audit events, Admin-proxy mode with endpoint allowlist, and structured error surfacing.
- **Observability:** Structured JSON logs (access + runtime) with configurable sinks. Prometheus metrics endpoint (publish mutation counters, managed-ownership rejection counters, tracing diagnostics, ingress request counters, delivery attempt/outcome counters, on-scrape queue depth gauge by state). OpenTelemetry tracing (OTLP/HTTP) with TLS/proxy/retry options. Health diagnostics (`GET /healthz?details=1`) with trend signals. MCP `admin_health` surfaces tracing config and runtime diagnostics.
- **Defaults:** `egress { allow, deny, https_only, redirects, dns_rebind_protection }`, `deliver { retry, timeout, concurrency }`, `publish_policy { direct, managed, allow_pull_routes, allow_deliver_routes, require_actor, require_request_id, fail_closed, actor_allow, actor_prefix }`, `trend_signals { ... }`.
- **Secrets:** `secrets { secret "ID" { value, valid_from, valid_until? } }` with `auth hmac secret_ref` and deliver `sign hmac secret_ref` support. Validity-window selection for signing.
- **Release:** Cross-platform archives (`make dist`), signed checksums (`make dist-signed`, Ed25519), SPDX SBOM, `hookaido verify-release` with `--require-signature`/`--require-sbom`/`--require-provenance` gates, Sigstore DSSE/in-toto attestation bundle validation (provenance + SBOM attestation subject-digest cross-check), provenance manifest, and tag-based GitHub release automation with build-provenance/SBOM attestations.
- **Graceful shutdown:** Push dispatcher drains in-flight deliveries on SIGTERM/SIGINT (15s default timeout) before process exit. Delivery contexts decoupled from signal context so in-flight HTTP requests complete cleanly.
- **CI:** Windows added to the test matrix (`windows-latest`); pure-Go SQLite and fsnotify support Windows natively.
- **License:** Apache-2.0.

### Changed

- `config diff` extracts unified diff engine from MCP to shared `config.FormatDiff` — canonical parse→format→LCS diff with configurable context lines.
- **Breaking:** The `hooks { }` route wrapper has been removed. Use `inbound { }` or bare top-level routes (implicit inbound). `outbound` and `internal` channel wrappers are new.
- **Breaking:** DSL restructuring — flat defaults consolidated into nested blocks: `egress_allow`/`egress_deny`/`egress_policy` → `egress { ... }`; `deliver_defaults`/`deliver_concurrency` → `deliver { ... }`; tracing `tls_*` → `tls { ... }`.
- **Breaking:** DSL renames — `tracing { endpoint }` → `collector`; `pull { endpoint }` → `path`; route `publish_direct`/`publish_managed` → `publish { direct, managed }`; `publish_policy` inner directives shortened (e.g. `global_direct_enabled` → `direct`, `require_audit_actor` → `require_actor`).
- DSL now supports multi-value directives across `match`, `egress`, `auth hmac`, `deliver sign`, `publish_policy`, and `auth forward copy_headers`.
- DSL now supports shorthand forms for `metrics on|off`, `tracing on|off`, `queue sqlite|memory`, and `auth hmac` with inline options.
- Parser hardening: duplicate scalar directives fail fast at parse-time across all blocks.
- Observability: tracing headers validated as HTTP-safe at compile-time.
- Admin strict body parsing: unknown JSON fields and trailing documents rejected across all mutation endpoints.
- Admin structured JSON errors with stable `code`/`detail` across all endpoints (auth, routing, mutations, reads).
- Admin publish policy: full preflight validation before enqueue (no partial side effects), route-level publish directives, managed-selector ownership enforcement, and ownership-source drift guardrails.
- Admin endpoint mapping: active-backlog guardrails, managed-publish ownership constraints, target-profile compatibility on moves, and specific conflict codes.
- Pull API: strict JSON parsing and structured error responses.
- MCP: strict argument allowlists, mutation audit length limits, Admin-proxy harmonized error mapping with retry and rollback, managed-selector routing to endpoint-scoped Admin paths.
- `config fmt` preserves quoted/unquoted style and channel-type wrappers.
- Windows: runtime-control compatibility (`instance_reload` returns unsupported-signal error).

### Fixed

- Memory store `max_depth` now counts only queued+leased items, matching SQLite semantics (dead/delivered/canceled no longer consume depth budget).
- Managed endpoint upsert/delete TOCTOU race: post-write backlog re-check with automatic rollback on concurrent enqueue.
- Batch publish (`EnqueueBatch`) is now transactional — all-or-nothing semantics prevent partial commits on queue-full or duplicate-ID errors.
- Windows compatibility for runtime/config workflows (PID handling, directory fsync).
- Route auth validation correctly rejects `auth basic` + `auth hmac secret_ref` combination.
- Queue backends consistently reject duplicate IDs on enqueue.
- Egress wildcard `*` host matching.
- MCP Admin-proxy preserves original item indexing across multi-batch publish.
- MCP Admin-proxy best-effort rollback on partial publish failures.
- Mixed queue backends rejected at compile time.
- Hot reload now correctly rejects changes to `defaults.max_body`, `defaults.max_headers`, and `defaults.publish_policy` (previously silently ignored).
