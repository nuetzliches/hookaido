# Backlog

Prioritized work items for Hookaido. Items are grouped by priority tier and roughly ordered within each tier.

## P1 - Medium Priority

- [x] **~~Remove or integrate internal/router~~** — Moved to Completed.
- [x] **~~Improve workerapi test coverage~~** — Moved to Completed.
- [x] **~~Provider-compatible HMAC verification~~** — Moved to Completed.
- [x] **~~Custom outbound headers in deliver blocks~~** — Moved to Completed.
- [x] **~~Mixed-workload tail latency playbook~~** — Moved to Completed.
- [x] **~~Drain fairness under saturation~~** — Moved to Completed.
- [x] **~~Adaptive backpressure production tuning guide~~** — Moved to Completed.
- [x] **~~Management model runtime wiring~~** — Moved to Completed.
- [x] **~~Config `validate --format json`~~** — Moved to Completed.
- [x] **~~Egress policy enforcement~~** — Moved to Completed.
- [x] **~~Admin API integration tests~~** — Moved to Completed.
- [x] **~~MCP Admin-proxy mode tests~~** — Moved to Completed.
- [x] **~~Attestation bundle validation~~** — Moved to Completed.

## P2 - Nice to Have (v2.0+)

- [ ] **xhookaido build tool** — CLI tool for building custom Hookaido binaries with selected modules (like xcaddy). Deferred until module system is stable.
- [x] **~~Vault secret adapter~~** — Moved to Completed.
- [x] **~~Full code review and polish pass~~** — Moved to Completed.
- [x] **~~Documentation UX refresh~~** — Moved to Completed.
- [x] **~~Scorecard: fuzzing baseline~~** — Moved to Completed.
- [x] **~~Scorecard: API visibility/auth follow-up~~** — Moved to Completed.
- [x] **~~CII Best Practices badge~~** — Moved to Completed.
- [x] **~~Config `diff` CLI command~~** — Moved to Completed.
- [x] **~~VS Code Extension (Hookaidofile)~~** — Moved to Completed. _Optional: LSP backed by `config validate`/`config compile` for live diagnostics._
- [x] **~~Graceful shutdown draining~~** — Moved to Completed.
- [x] **~~Shared listener mode~~** — Moved to Completed.
- [x] **~~Windows CI~~** — Moved to Completed.

## Completed (move here when done)

- [x] **Improve workerapi test coverage** — 14 new test functions (57 sub-tests) in `modules/grpcworker/server_test.go` covering nil requests, blank endpoints, Pull-nil guards, invalid durations, lease ID normalization edge cases (both-set, all-empty, max-batch, dedup), error mapping (all status codes), route resolution fallback chain, custom MaxLeaseBatch, nack-dead via gRPC, nack-batch, and large-batch dequeue.
- [x] **Provider-compatible HMAC verification** — `auth hmac { provider github; secret env:SECRET }` and `auth hmac { provider gitea; secret env:SECRET }` DSL surface with compile-time validation (mutual exclusivity with signature_header/timestamp_header/nonce_header/tolerance). GitHub verifies `X-Hub-Signature-256` (`sha256=hex(HMAC-SHA256(secret, body))`), Gitea/Forgejo verifies `X-Gitea-Signature` (`hex(HMAC-SHA256(secret, body))`). 14 config tests + 9 HMAC verification tests.
- [x] **Custom outbound headers in deliver blocks** — `header "Name" "Value"` directive in deliver blocks with placeholder interpolation at compile time. Duplicate detection (case-insensitive), HTTP token validation, headers set on outbound requests before HMAC signing. 5 config tests + 2 dispatcher tests.

- [x] **Remove or integrate internal/router** — Dead `Router` interface removed; `MatchPath` relocated as unexported helper in `internal/app`. Test-only copy inlined in `internal/ingress/http_test.go`.
- [x] **Phase 1a: Extract shared backlog analytics** — Move duplicated backlog analysis types, constants, and algorithms from `admin/http.go` and `mcp/server.go` into `internal/backlog/`. Both packages import the shared package. Design: `docs/plans/2026-03-08-modular-architecture-design.md`.
- [x] **Phase 1b: Split mcp/server.go** — Break 8,406-line god file into protocol.go, tools_messages.go, tools_dlq.go, tools_config.go, tools_backlog.go. No behavior change.
- [x] **Phase 1c: Split admin/http.go** — Break 5,285-line god file into handler groups, parameter parsing, routing. No behavior change.
- [x] **Phase 2a: Module registry** — Create `internal/hookaido/` with typed module interfaces (QueueBackend, TracingProvider, MCPProvider, WorkerTransport) and registration functions.
- [x] **Phase 2b: Wire queue backend registry** — `app/run.go` selects queue backend through registry instead of direct constructor calls. Backends register from current location.
- [x] **Phase 2c: Config parser backend validation** — Parser validates backend names against registry. Unknown backends produce actionable error ("not compiled in").
- [x] **Phase 3a: Extract sqlite module** — Move SQLite backend from `internal/queue/` to `modules/sqlite/` with init() registration.
- [x] **Phase 3b: Extract postgres module** — Move Postgres backend to `modules/postgres/`.
- [x] **Phase 3c: Extract grpcworker module** — Move workerapi to `modules/grpcworker/`.
- [x] **Phase 3d: Extract otel module** — Extract OpenTelemetry setup from `app/run.go` to `modules/otel/`.
- [x] **Phase 3e: Extract mcp module** — Move MCP server to `modules/mcp/` (already split in Phase 1b).
- [x] **Phase 4: Build variants** — Standard main.go (all modules), minimal main.go (core only), Makefile targets for build/build-minimal/build-custom.
- [x] **Branding: project logo** — Create a production-ready Hookaido logo (SVG + PNG variants) and define basic usage guidance (light/dark backgrounds, minimum size, spacing).
- [x] **Queue lag/age recovery tuning (#56)** — Added lag/age regression acceptance guardrails (`scripts/adaptive-lag-guardrail.sh`, `adaptive-ab-lag-guardrail-check`, `adaptive-ab-mixed-lag-guardrail`) and completed runtime tuning for push drain (`routeMutationBatch` up to dequeue micro-batch size, lease TTL scaling by micro-batch) with mixed saturation revalidation pass (`run 20260215-003728`).
- [x] **Delivery dead-letter growth tuning (#57)** — Completed attribution + runtime + regression-gate path: dead reason metrics/health diagnostics (`#62`), dead-growth E2E saturation guardrail (`#64`), retry max semantics fix (`#65`), and mixed saturation revalidation with `hookaido_delivery_dead_total=0` (`run 20260215-003728`).
- [x] **Mixed Pull ACK conflict guardrail (#55)** — Added reproducible guardrail validation via `scripts/adaptive-guardrail.sh` + Make targets (`adaptive-ab-guardrail-check`, `adaptive-ab-mixed-guardrail`) with acceptance thresholds on `pull_ack_conflict_ratio_percent` and per-route drill-down tables from `final-metrics.txt` for mixed A/B regression checks.
- [x] **Adaptive backpressure mixed decision slice (#53/#54)** — Reproducible mixed `adaptive off` vs `on` saturation runs completed (including calibrated high-pressure profile), artifacts captured, and v1.5 decision recorded: keep runtime default `enabled off`; recommended opt-in enterprise start profile `min_total 400`, `queued_percent 88`, `ready_lag 45s`, `oldest_queued_age 90s`, `sustained_growth on`; hardware results treated as relative same-host evidence, not universal default proof.
- [x] **Store observability backend-agnostic metrics (#38)** — Unified store runtime metric vocabulary with backend/operation labels (`hookaido_store_operation_seconds`, `hookaido_store_operation_total`, `hookaido_store_errors_total`) across `sqlite`, `memory`, and `postgres`, while retaining SQLite compatibility series.
- [x] **Optional gRPC worker API (Phase 2)** — Added worker transport contract and handlers, shared Pull operation core, opt-in runtime listener/config wiring via `pull_api.grpc_listen` with listener guardrails, auth parity (global + route override), integration/E2E parity coverage, and docs for operations. Scope is fixed to pull-worker lease transport (`dequeue`/`ack`/`nack`/`extend`) with explicit MCP non-goal for worker lease ops.
- [x] **Drain fairness under saturation** — Completed saturation tuning across push drain paths: route-shared workers with target-aware dequeue micro-batching (`single-target` up to 4, `multi-target` up to 2), single-target lease-mutation batching with fallback safety, and reproducible push benchmarks with reject-reason and tail-latency (`p95_ms`/`p99_ms`) guardrails.
- [x] **Mixed-workload tail latency playbook** — Added reproducible mixed ingress+drain benchmark profile in `internal/pullapi/bench_test.go` (`BenchmarkMixedIngressDrain`) with `p95_ms`/`p99_ms` reporting and Makefile targets `bench-pull-mixed-baseline`, `bench-pull-mixed`, `bench-pull-mixed-compare`.
- [x] **Adaptive backpressure production tuning guide** — Added dedicated operations guide `docs/adaptive-backpressure.md` with recommended starting profiles (`balanced`, `latency_first`, `throughput_first`), a metrics-first decision matrix, and guardrails for dashboard/version compatibility.
- [x] **CII Best Practices badge** — OpenSSF Best Practices badge published at <https://www.bestpractices.dev/projects/11921>; `README.md` badge/link and docs references updated. Ongoing evidence/maintenance notes live in `docs/ossf-best-practices.md`.
- [x] **Documentation UX refresh** — Refreshed docs information architecture in `mkdocs.yml` (grouped navigation), rebuilt `docs/index.md` with a landing hero + task-oriented quick paths, added command-palette style search shortcut (`Ctrl+K`) via `docs/assets/javascripts/command-palette.js`, added docs UX styling in `docs/assets/stylesheets/extra.css`, and documented docs-stack evaluation/decision in `docs/documentation-platform.md` (keep MkDocs Material for current roadmap window).
- [x] **CII badge readiness docs** — Added `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, `SUPPORT.md`, `GOVERNANCE.md`, and `.github/CODEOWNERS`; linked governance/security docs from `README.md` and `docs/index.md` to prepare badge evidence links.
- [x] **Scorecard: API visibility/auth follow-up** — Updated `scorecard.yml` with explicit read permissions (`contents`, `issues`, `pull-requests`, `checks`) to prevent check-run auth gaps, and added optional `SCORECARD_TOKEN` passthrough for classic branch-protection visibility.
- [x] **Scorecard: fuzzing baseline** — Added baseline Go fuzz targets for config parse/format round-trip, Pull API auth/HTTP handlers, and ingress HMAC verification; wired scheduled fuzz smoke runs into `dependency-health` CI.
- [x] **Scorecard: branch protection + review policy enforcement** — Applied `main` branch protection policy in GitHub (required PR reviews: 1 approval + last-push approval, stale-review dismissal, required conversation resolution, linear history, enforce admins, and required CI checks).
- [x] **Scorecard CI hardening (permissions + pinning)** — Updated workflows to least-privilege permission scopes, pinned GitHub Actions by commit SHA, pinned Docker base images by digest, and pinned CI tool install versions for Scorecard `Token-Permissions`/`Pinned-Dependencies` improvements.
- [x] **Secret preflight validation mode (nice-to-have)** — Added optional strict validation preflight for secret refs (`hookaido config validate --strict-secrets` and MCP `config_validate strict_secrets=true`) to load refs and fail early on missing env vars, unreadable files, or Vault access/connectivity errors.
- [x] **Full code review and polish pass** — End-to-end review executed (`go test ./...`, `go vet ./...`) with prioritized findings and targeted fix: compile-time secret-ref scheme validation added for token/signing/value refs; docs/changelog synchronized.
- [x] **Vault secret adapter** — Added `vault:` secret refs with Vault HTTP API support (KV v1/v2 field extraction), optional namespace/TLS env settings, and unit tests.
- [x] **DSL surface complete** — All directives from DESIGN.md implemented: `vars`, `delivered_retention`, `dlq_retention`, named matchers, `match @name`, `publish` block/shorthand, `publish.direct`/`publish.managed` dot-notation, channel types.
- [x] **Runtime reload completeness** — Fixed silently-ignored defaults (`max_body`, `max_headers`, `publish_policy`) by adding to `requiresRestartForReload`. Documented full live-reloadable vs restart-required matrix in `docs/configuration.md`.
- [x] **Queue publish hardening** — 22 new tests covering empty/oversized batch, duplicate IDs, queue-full (single + mid-batch partial), round-trip field fidelity, store unavailable, invalid timestamps/base64, missing ID, malformed JSON, ObservePublishResult callback, audit request-id policy, payload-too-large global fallback, scoped-path empty batch/queue-full/store-unavailable/endpoint-not-found/resolver-missing/no-targets.
- [x] **E2E test suite** — 9 tests in `internal/e2e`: ingress→pull round-trip, ingress→push round-trip, push DLQ lifecycle (fail→dead→requeue→deliver), fanout delivery, queue backpressure (max_depth reject), pull nack/requeue, pull lease extend, unknown route 404, 50-concurrent ingress drain.
- [x] **SQLite WAL recovery tests** — 5 tests: crash recovery no-close, leased items requeued after expiry, concurrent enqueue/dequeue, stress (10×50 producers + 5 consumers), integrity check after stress.
- [x] **Management model runtime wiring** — All Admin API management fields (ResolveManaged, ManagedRouteInfoForRoute, ManagedRouteSet, ManagementModel, UpsertManagedEndpoint, DeleteManagedEndpoint, AuditManagementMutation, ObservePublishResult) wired in `run.go` via `runtimeState` methods and mutation closures.
- [x] **Config `validate --format json`** — Parse/file errors now respect `--format` flag; 7 CLI tests cover valid/parse-error/missing-file/compile-error paths in both JSON and text formats.
- [x] **Egress policy enforcement** — Full test coverage: deny-before-allow ordering, CIDR-deny-overrides-allow, subdomain wildcards, deny-only mode, empty policy, non-HTTP scheme, redirect blocked/followed/hop-recheck, HTTPS-only delivery denial.
- [x] **Admin API integration tests** — All mutation endpoints covered: requeue/resume by-ID audit-reason enforcement, DLQ requeue/delete audit-reason enforcement, resume empty-IDs bad request. 192 tests total.
- [x] **MCP Admin-proxy mode tests** — resume_by_filter via admin proxy: scoped managed path, structured error, not-found fallback detail. ~198 MCP tests total.
- [x] **Attestation bundle validation** — `verify-release` validates Sigstore DSSE/in-toto provenance and SBOM attestation bundles (`--require-provenance`, subject-digest cross-check, auto-detection). 15 CLI tests total.
- [x] **Shared listener mode** — Auto-detected when `pull_api.listen == admin_api.listen`; prefix routing via `sharedPrefixMux` with per-component tracing/access-log wrappers.
- [x] **Config `diff` CLI command** — `hookaido config diff [--context N] old.hcl new.hcl` with unified diff output, exit code semantics (0=identical, 1=changed, 2=error). Diff engine extracted from MCP to `config.FormatDiff`. 6 CLI tests.
- [x] **Graceful shutdown draining** — `PushDispatcher.Drain(timeout)` completes in-flight deliveries on SIGTERM. Internal `stopCh` + `sync.WaitGroup` lifecycle decoupled from signal context. Idempotent via `sync.Once`. 15s drain timeout in `run.go`. 3 drain unit tests.
- [x] **Windows CI** — Added `windows-latest` to CI test matrix. Pure-Go SQLite (`modernc.org/sqlite`) and `fsnotify` support Windows natively; OS-specific signal handling via build-tagged files.
- [x] **Observability metrics** — Prometheus endpoint now emits ingress counters (`accepted`/`rejected`/`enqueued`), delivery counters (`attempts`/`acked`/`retry`/`dead`), and on-scrape queue depth gauge (`queued`/`leased`/`dead`). Health diagnostics include all new counter sections. 8 new tests.
- [x] **Reload flow tests** — 5 unit tests for `reloadConfig` full flow: success, parse error, compile error, restart-required, missing file.
- [x] **Memory store max_depth bug** — Fixed `max_depth` to count only queued+leased items (matching SQLite). 1 new test.
- [x] **MCP tracing health** — `admin_health` now surfaces tracing config (enabled/collector) and propagates runtime tracing diagnostics from admin probe. 2 new tests.
- [x] **Managed endpoint TOCTOU guard** — Post-write backlog re-check in `mutateManagedEndpointConfig` with automatic rollback. 1 new test.
- [x] **E2E observability tests** — Ingress observe-result callback + push delivery attempt observation. 2 new E2E tests.
- [x] **Metrics prefix integration test** — Verifies custom `metrics.prefix` routes correctly and default path returns 404. 1 new test.
- [x] **Batch publish atomicity** — `EnqueueBatch` (all-or-nothing) for both MemoryStore and SQLiteStore, wired into admin publish handlers. 4 new store tests + 1 admin test updated.
- [x] **v1.0 hardening pass** — Deliver URL compile validation (4 tests), deliver concurrency upper bound (1 test), Pull API `handleExtend` tests (5 tests), ingress enqueue-failure 503 test, dispatcher lease-expired tolerance tests (3 tests), dispatcher `RecordAttempt` error tolerance test, MCP `attempts_list` admin-proxy endpoint-not-found test.
- [x] **VS Code Extension** — TextMate grammar for full DSL syntax highlighting (top-level blocks, route paths, directives, auth keywords, channel types, placeholders, durations, built-in constants). 18 snippets for common blocks. File association for `Hookaidofile`, `*.hookaido`, `*.hkd`. Located in `editors/vscode/`.
- [x] **Score hardening pass (round 2)** — Ingress body-too-large 413 + body-read-error 400 tests, egress DNS resolver error test, memory store Extend edge cases (unknown lease, expired lease, zero-duration noop), pull API dequeue-store-error 503 + unknown-operation 404 tests.
- [x] **Score hardening pass (round 3)** — Pull API Ack/Nack/MarkDead store-error + lease-expired paths (6 tests), config secrets validation edge cases (5 subtests), HTTP deliverer signing-header-missing error (4 subtests), SQLite Extend zero/negative noop test.
