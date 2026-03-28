# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `deliver exec` directive: deliver webhook payloads by executing a local subprocess. Payload on stdin, metadata as env vars (`HOOKAIDO_ROUTE`, `HOOKAIDO_EVENT_ID`, `HOOKAIDO_CONTENT_TYPE`, `HOOKAIDO_ATTEMPT`, `HOOKAIDO_HEADER_*`), user-defined `env` vars from config. Exit code mapping for retry/DLQ semantics. Cross-platform via `os/exec`.

## [2.0.1] - 2026-03-14

### Added

- CI pipeline hardening: race detector on Linux test runs, golangci-lint (errcheck, staticcheck, unused, ineffassign), and coverage profile artifact upload.
- `.golangci.yml` (v2) with `std-error-handling` exclusion preset for idiomatic Go patterns.
- Binary-level E2E tests: build hookaido as subprocess, test ingress-to-pull round-trip, config validate/fmt, and invalid config rejection.
- Unit tests for 5 previously untested packages: path matching, rate limiter, module registry, backlog analysis, and worker API bearer-token auth.
- Extended Postgres test coverage: unit tests for options/helpers/error-mapping and DSN-gated integration tests with `docker-compose.test.yml`.

### Changed

- MCP server split into focused files: `runtime_control.go` (729 lines) and `admin_proxy.go` (1,091 lines) extracted from `protocol.go`; test file split into `runtime_control_test.go` and `admin_proxy_test.go`.
- Removed dead `internal/router` package; `matchPath` relocated to `internal/app` as package-private helper.
- Dockerfile Go version aligned with `go.mod` (`golang:1.25-alpine`).

### Fixed

- Possible nil pointer dereference in Admin API publish body/header size resolution (`internal/admin/params.go`).
- Data race in push dispatcher test stub (`stubPushStore`) detected by `-race` flag.
- HMAC canonical string order in `docs/ingress.md` now matches code (METHOD first, not TIMESTAMP).

## [2.0.0] - 2026-03-09

### Added

- Modular architecture with module registry and build variants for compile-time feature selection.

### Fixed

- SQLite dequeue allocation hardening: candidate collection no longer preallocates slice capacity directly from request-sized batch input, while preserving the existing hard cap of 100 items per dequeue.

## [1.5.1] - 2026-02-16

### Changed

- SQLite dequeue leasing now uses a bulk lease update per batch (single `UPDATE` with per-item lease IDs) instead of per-item update statements, reducing pull-path transaction roundtrips under sustained backlog.
- SQLite dequeue for `batch=1` now leases the next candidate via a single CTE-based `UPDATE ... RETURNING` statement, avoiding extra select/update roundtrips on the hottest pull path.
- SQLite dequeue now throttles expired-lease sweep updates to a short fixed interval instead of sweeping on every dequeue call, reducing write-path contention and pull saturation overhead under sustained worker polling.
- SQLite batch lease operations now use set-based batch lookup and bulk item mutation inside the single write transaction, reducing SQL roundtrips and allocations on Pull batch ack paths.
- SQLite single-lease Pull mutations (`ack`/`nack`/`extend`/`mark dead`) now use a lease-id fast path to reduce per-request allocations while preserving lease-expired requeue semantics.

### Fixed

- SQLite pull ack hot-path contention under sustained pull traffic: `Ack` now uses a direct lease-scoped update/delete path with expired-lease fallback requeue, reducing write-lock time and queue-full backpressure spikes at high ingest rates.

## [1.5.0] - 2026-02-15

### Added

- Postgres queue backend (`queue { backend postgres }` / `queue postgres`) with durable queue semantics parity (lease lifecycle, message management, attempts, retention, and batch lease mutation support) plus runtime wiring via `hookaido run --postgres-dsn` (or `HOOKAIDO_POSTGRES_DSN`).
- Delivery dead-letter reason attribution via `hookaido_delivery_dead_by_reason_total{reason}` (`max_retries`, `no_retry`, `policy_denied`, `unspecified`, `other`) plus matching health diagnostics field `delivery.dead_by_reason`.
- Pull API `POST {endpoint}/ack` and `POST {endpoint}/nack` now support batch form via `lease_ids` (up to 100 IDs per request), returning aggregate success/conflict output for high-throughput worker lease operations.
- Backend-agnostic store runtime metric families on `/metrics`: `hookaido_store_operation_seconds{backend,operation}` (histogram), `hookaido_store_operation_total{backend,operation}`, and `hookaido_store_errors_total{backend,operation,kind}` to support backend-neutral dashboards.
- Adaptive backpressure production tuning guide (`docs/adaptive-backpressure.md`) with data-driven starting profiles and metric-driven decision matrix.
- Reproducible adaptive backpressure A/B runtime harness (`scripts/adaptive-ab.sh`) plus Make targets for side-by-side comparison of `adaptive off` vs `on` configurations.
- Mixed Pull ACK conflict guardrail workflow and queue lag/age guardrail workflow for adaptive backpressure validation.
- Metrics schema marker `hookaido_metrics_schema_info{schema="1.3.0"}` for dashboard compatibility gating across mixed Hookaido versions.

### Changed

- Adaptive backpressure policy decision for v1.5 is now explicit in docs: keep runtime default `enabled off`, with recommended opt-in enterprise starting profile and same-host-only interpretation guardrails for benchmark evidence.
- MCP queue tool backend routing now treats non-SQLite backends (`memory`, `postgres`) as Admin-proxy mode; local SQLite access remains only for `queue.backend sqlite`.
- SQLite runtime instrumentation now populates both backend-agnostic store metric families and legacy `hookaido_store_sqlite_*` series for compatibility during dashboard migration.
- PostgreSQL store runtime now populates backend-agnostic store metric families with normalized error kinds for queue/store operations.
- Postgres backend now implements backlog trend snapshot capture/listing, enabling stable `/admin/backlog/trends` responses instead of backend-unsupported `503` responses.
- Batch Pull `ack`/`nack`/`mark dead` lease handling now executes as true store-side batches, avoiding per-lease transaction loops under high worker throughput.
- Pull API now treats recent duplicate retries of successful `ack`/`nack` lease operations as idempotent success, reducing avoidable `lease_conflict` churn under high retry/parallel worker pressure.
- Push dispatcher runtime now uses route-shared dequeue workers across all push routes (single- and multi-target), enforcing `deliver_concurrency` as a shared route budget.
- Push route workers now lease small dequeue micro-batches to balance throughput and fairness under saturation.
- Push dispatcher now applies batched lease mutations on single-target routes when the backend supports `LeaseBatchStore`, with automatic fallback and multi-target safety guardrails.
- Push single-target lease mutations now batch up to the route dequeue micro-batch size, reducing store roundtrips on saturated single-target routes.
- Push dispatcher lease TTL now scales with route dequeue micro-batch size, reducing avoidable lease-expiry conflicts/requeues.

### Fixed

- Memory backend retention safety: with `delivered_retention` enabled, `queue_limits.max_depth` now also caps `queued + leased + delivered` items so sustained pull/ack traffic cannot grow delivered retention unbounded in RAM.
- Push retry exhaustion semantics now align with docs: `deliver.retry.max` is treated as maximum retry attempts (not total attempts), so `max 1` allows one retry before `max_retries` dead-lettering.

## [1.4.0] - 2026-02-14

### Added

- Optional gRPC worker listener via `pull_api.grpc_listen`: starts WorkerService (`Dequeue`, `Ack`, `Nack`, `Extend`) on shared Pull operation semantics, applies `pull_api.tls` for TLS/mTLS, enforces dedicated-listener conflict guards, and keeps Pull token auth parity (global + per-route override).

### Changed

- Worker gRPC scope is now explicitly fixed to pull-worker lease transport (`dequeue`/`ack`/`nack`/`extend`) and documented as out-of-scope for admin/publish/control-plane and MCP lease mutation tools.

### Fixed

- SQLite dequeue candidate helper now clamps batch size correctly inside the candidate collection path.

## [1.3] - 2026-02-14

### Added

- Reproducible Pull benchmark workflow docs (`docs/performance.md`) plus Make targets for baseline/current capture and `benchstat` diff (`bench-pull-baseline`, `bench-pull`, `bench-pull-compare`).
- Isolated Extend, sustained-drain, contention, and mixed ingress+drain Pull benchmarking targets for comprehensive pull-path performance validation.
- Mixed ingress+drain Push saturation and push skewed-target saturation benchmarking targets with per-backend tail-latency metrics and reject-reason splits.

## [1.2.0] - 2026-02-13

### Added

- Prometheus queue saturation gauges: `hookaido_queue_oldest_queued_age_seconds`, `hookaido_queue_ready_lag_seconds`, and `hookaido_queue_total` on `/metrics` for direct lag/age alerting without Admin health JSON scraping.
- Memory-backend observability on `/metrics`: `hookaido_store_memory_items{state}`, `hookaido_store_memory_retained_bytes{state}`, `hookaido_store_memory_retained_bytes_total`, and `hookaido_store_memory_evictions_total{reason}`.
- Ingress rejection breakdown counters via `hookaido_ingress_rejected_by_reason_total{reason,status}` including `memory_pressure` for memory-backend pressure rejects.

### Changed

- Memory backend now emits explicit `ErrMemoryPressure` admission rejects when retained memory footprint crosses pressure guard thresholds; ingress surfaces these as HTTP `503` with rejection reason `memory_pressure` instead of generic store-unavailable.
- Admin health diagnostics now include ingress `rejected_by_reason` and memory-store runtime diagnostics when the memory backend is active.
- Ingress rejection breakdown metric `hookaido_ingress_rejected_by_reason_total{reason,status}` for bounded-cardinality attribution across queue pressure, adaptive backpressure, auth, routing, policy, and fallback reject paths.

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

## [1.0.3] - 2026-02-10

### Added

- GHCR container publishing documentation and Dependabot configuration for automated dependency updates.

## [1.0.2] - 2026-02-10

### Added

- Container publish workflow with corrected ldflags for release metadata.
- Package-level Go documentation for `pkg.go.dev`.

### Fixed

- Admin API now bounds strict JSON request body size to prevent unbounded memory consumption.

## [1.0.1] - 2026-02-10

### Fixed

- Go module path corrected to match repository path for proper module resolution.

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

[Unreleased]: https://github.com/nuetzliches/hookaido/compare/v2.0.1...HEAD
[2.0.1]: https://github.com/nuetzliches/hookaido/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/nuetzliches/hookaido/compare/v1.5.1...v2.0.0
[1.5.1]: https://github.com/nuetzliches/hookaido/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.com/nuetzliches/hookaido/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/nuetzliches/hookaido/compare/v1.3...v1.4.0
[1.3]: https://github.com/nuetzliches/hookaido/compare/v1.2.0...v1.3
[1.2.0]: https://github.com/nuetzliches/hookaido/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/nuetzliches/hookaido/compare/v1.0.3...v1.1.0
[1.0.3]: https://github.com/nuetzliches/hookaido/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/nuetzliches/hookaido/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/nuetzliches/hookaido/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/nuetzliches/hookaido/releases/tag/v1.0.0
