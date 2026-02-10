# Backlog

Prioritized work items for Hookaido pre-v1.0. Items are grouped by priority tier and roughly ordered within each tier.

## P0 — Must Have for v1.0

- [x] **~~Remaining DSL directives~~** — All directives from DESIGN.md now implemented (moved to Completed).- [x] **~~Runtime reload completeness~~** — Silent-bug reload guards fixed and full reload behavior documented (moved to Completed).
- [x] **~~Queue publish hardening~~** — Moved to Completed.
- [x] **~~E2E test suite~~** — Moved to Completed.
- [x] **~~SQLite WAL recovery tests~~** — Moved to Completed.

## P1 — Should Have for v1.0

- [x] **~~Management model runtime wiring~~** — All Admin API management fields wired in `run.go` (moved to Completed).
- [x] **~~Config `validate --format json`~~** — Parse/file errors now respect `--format` flag; 7 CLI tests added (moved to Completed).
- [x] **~~Egress policy enforcement~~** — Full test coverage added: deny-before-allow ordering, CIDR-deny-overrides-allow, subdomain wildcards, deny-only mode, empty policy, non-HTTP scheme, redirect blocked/followed/hop-recheck, HTTPS-only delivery denial (moved to Completed).
- [x] **~~Admin API integration tests~~** — Added audit-reason enforcement for requeue/resume by-ID and DLQ requeue/delete, plus resume empty-IDs bad request (moved to Completed).
- [x] **~~MCP Admin-proxy mode tests~~** — Added resume_by_filter via admin proxy: scoped path, structured error, not-found fallback. ~198 MCP tests total (moved to Completed).
- [x] **~~Attestation bundle validation~~** — `verify-release` now validates Sigstore DSSE/in-toto provenance and SBOM attestation bundles with `--require-provenance` flag, subject-digest cross-check, auto-detection; 7 new tests (moved to Completed).

## P2 — Nice to Have / Post-v1.0

- [ ] **Vault secret adapter** — Secrets from HashiCorp Vault (or compatible API) in addition to env/file refs. _Deferred: env/file covers most deployments pre-v1.0._
- [ ] **Full code review and polish pass** — End-to-end review with prioritized findings, targeted fixes, explicit "nice to have" follow-ups, and synchronized docs updates for changed behavior.
- [ ] **Branding: project logo** — Create a production-ready Hookaido logo (SVG + PNG variants) and define basic usage guidance (light/dark backgrounds, minimum size, spacing).
- [ ] **Documentation UX refresh** — Improve docs structure and landing experience (hero page), add command-palette style Ctrl+K search, and evaluate whether to keep the current docs stack or migrate to an alternative OSS docs solution.
- [x] **~~Config `diff` CLI command~~** — `hookaido config diff old.hcl new.hcl` with exit code semantics (0=identical, 1=changed, 2=error); diff engine extracted to shared `config.FormatDiff`; 6 CLI tests (moved to Completed).
- [x] **~~VS Code Extension (Hookaidofile)~~** — TextMate grammar, 18 snippets, file association for `Hookaidofile`/`.hookaido`/`.hkd` (moved to Completed). _Optional Phase 2: LSP backed by `config validate`/`config compile` for live diagnostics._
- [x] **~~Graceful shutdown draining~~** — PushDispatcher uses internal stopCh+WaitGroup lifecycle; Drain(timeout) completes in-flight deliveries on SIGTERM; 3 drain unit tests + E2E coverage (moved to Completed).
- [x] **~~Shared listener mode~~** — Auto-detected when pull_api.listen == admin_api.listen; prefix routing via sharedPrefixMux (moved to Completed).
- [x] **~~Windows CI~~** — Added `windows-latest` to CI matrix; pure-Go SQLite + fsnotify support Windows natively (moved to Completed).

## Completed (move here when done)

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
