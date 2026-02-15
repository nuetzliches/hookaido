# AGENTS.md

Project: Hookaido (Webhook ingress queue, Caddy-style).

## Language

- Repo language is English (docs, code, comments).

## Open Source Policy

- Use open-source dependencies only (build/runtime/test), ideally OSI-approved licenses.
- Any non-OSS dependency (including SDKs) requires explicit approval.
- Ship Hookaido under an OSI-approved license (Apache-2.0).

## Goal (Short)

- Single binary, clear defaults, compact DSL, fast reloads without restart.
- Config file is the source of truth.

## Scope & Phasing

- MVP Core: ingress, routing, queue, pull API, retry/DLQ, HMAC+replay, SSRF policies, observability, config reload.
- Phase 2 (opt-in): incremental expansions only after explicit prioritization in `BACKLOG.md`.

## Hard Guardrails

- Default deployment: `dmz-queue pull` (Ingress + Queue in the DMZ, internal workers pull).
- Pull mode is explicit: `pull { ... }` and it excludes `deliver`.
- Worker gRPC stays limited to pull-worker lease transport (`dequeue`, `ack`, `nack`, `extend`) and must not become an admin/publish/control-plane surface.
- `path` is a path relative to `pull_api`; full endpoint = `pull_api.prefix` + `pull.path` (slash-normalized).
- Admin API and Pull API are logically separated.
- Default: separate listeners. Optional: shared listener with strict `/pull/*` vs `/admin/*` prefixes.
- Queue backend DSL surface is `sqlite|memory|postgres`; runtime remains single-backend per process (mixed route backends rejected).
- Config changes must be round-trip safe (`config fmt` stable, diff-friendly).

## Delivery Semantics

- Delivery is at-least-once.
- Ingress ACK only after durable enqueue (Queue ACK).
- Retry on network errors, timeouts, 5xx, and 429/408. No retry on other 4xx.
- Backpressure: 429 (rate limit) or 503 (queue overload).

## Defaults (80/20)

- `max_body`: `2mb`
- `queue_limits`: `max_depth 10000`, `drop_policy "reject"`
- `deliver`: `retry "exponential" max 8 base "2s" cap "2m" jitter "0.2"`, `timeout "10s"`, `concurrency 20`

## Security

- Secrets via env, file, or Vault adapter.
- Pull API: mTLS or token, explicit allowlist.
- Admin API: default localhost, stricter auth.
- Outbound signing (HMAC): per `deliver`, include timestamp header and sign method + path + timestamp + body hash.

## Tests

- Target mix: 70% unit, 20% integration, 10% E2E.
- Required: DSL parser/merge rules, retry logic, Queue API (dequeue/ack/lease), backend parity contract tests, SQLite WAL/recovery.

## Go Version

- Pin in `go.mod` and CI.
- Prefer the same minimum Go version as Caddy unless there is a strong reason to diverge.
- Track Caddy's minimum supported Go version for the `go` directive.
- CI uses the latest patch of that minor (e.g., `1.25.x`).
- Bump the minimum only when Caddy does, or when a required feature/bugfix justifies it.

## Git & Release Workflow

- `main` is protected: no direct pushes, no local-only merges to `main`; use PRs.
- Keep `main` protection hardening enabled: no branch deletion and no force-push.
- SemVer tags are strict `vX.Y.Z` only. Never create `vX.Y` tags.
- Create release tags only from the merged/up-to-date `origin/main` state.
- Container/release publishing is tag-driven; treat tagging as a production action.

## Documentation Hygiene

- `STATUS.md` is the lightweight project snapshot — update only for milestone changes (new capability area, progress score change, or "What's Missing" shifts). Do not add per-feature bullets.
- `STATUS.md` must keep `Current release` aligned with the latest shipped tag.
- `CHANGELOG.md` tracks user-visible behavior changes (API/DSL/defaults/runtime semantics) under `Unreleased` using condensed thematic bullets grouped as `Added`, `Changed`, `Fixed`. Skip pure refactors or test-only edits.
- `BACKLOG.md` is the prioritized work list — update when priorities change, items are completed, or new work is identified. Move completed items to the "Completed" section.
- `DESIGN.md` is the canonical spec — update when adding or changing DSL, API, or runtime semantics.
- `README.md` should be updated whenever user-visible setup/runtime flags or supported backends change.

## MCP Coverage Policy

- For every new user-visible feature (DSL, Admin API, runtime operation), explicitly decide MCP coverage in the same change.
- If covered: update `internal/mcp/spec.md`, tool schemas, and tests in `internal/mcp/server_test.go`.
- If deferred: record the reason and follow-up in `STATUS.md` under missing work.
- Keep MCP tool naming and guardrails aligned with `DESIGN.md` (`read` default, opt-in mutations/runtime control).
