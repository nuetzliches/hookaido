# Development Status

Last updated: 2026-09-01
Current release: v2.14.0

Lightweight project snapshot. Canonical spec: `DESIGN.md`. Detailed change history: `CHANGELOG.md`. Prioritized work items: `BACKLOG.md`.

## Capabilities Overview

**Ingress & Routing** - HTTP ingress with optional TLS/mTLS, HMAC signature verification (replay protection, secret rotation, provider-compatible formats for GitHub/Gitea/Forgejo), Basic auth, forward auth callouts, query-token auth for sources that can only be given a URL, and per-route/global rate limiting. Route matching supports path, method, host (wildcards), headers, query params, remote IP (CIDR, optionally resolved through `X-Forwarded-For` for configured `trusted_proxies`), and named matchers. Channel types (`inbound`/`outbound`/`internal`) enforce directive constraints at compile time. Listeners default to separate ports but can opt into a single prefix-muxed port (ingress + `pull_api`/`admin_api` on the same `listen` address) for single-port deployments.

**Queue & Delivery** - SQLite/WAL and PostgreSQL durable queue backends (in-memory available for dev/tests), held to one shared `queue.Store` contract that CI exercises against all three, with lease semantics, long-poll dequeue, dead-lettering, and queue limits/retention/pruning. Push dispatcher with retry/backoff, per-route concurrency, custom outbound headers, and optional outbound HMAC signing (multi-secret rotation). Pull API (HTTP/JSON) with bearer-token auth, dequeue limits, per-route token overrides, and opt-in consumer groups that fan one route out to several independent queues instead of having consumers compete for one. Optional Worker API (gRPC) mirrors Pull lease operations (`dequeue`, `ack`, `nack`, `extend`) and is intentionally scoped to pull-worker transport only.

**Admin API** - Full queue lifecycle: DLQ management, message publish/cancel/requeue/resume (by ID and by filter), backlog drill-down (top queued, oldest, aging summary, trends with operator-action playbooks). Management model projection and endpoint mapping lifecycle with config-source-of-truth mutations. Structured JSON errors, audit headers, and publish-policy enforcement throughout.

**MCP** - Stdio JSON-RPC server (`hookaido mcp serve`) with role-gated access (`read`/`operate`/`admin`). Read tools for config inspection, queue state, and health diagnostics. Mutation tools (gated) for config apply, queue operations, and endpoint management. Runtime control tools (gated) for process lifecycle. Structured JSONL audit events, principal-authoritative actor binding, and Admin-proxy mode for non-SQLite backends (memory/postgres) deployments.

**Config DSL** - Caddyfile-inspired syntax with `config fmt` round-trip stability. Env/file/vars placeholders, multi-value directives, hot reload via `--watch`/`SIGHUP` (with an optional `--watch-interval` content poll for mounts that cannot deliver file events), and channel-type wrappers. Defaults blocks for egress policy, deliver settings, publish policy, and trend signal tuning. Secret refs support `env:`, `file:`, `vault:`, and `raw:`.

**Observability** - Structured JSON logs (access + runtime), Prometheus metrics endpoint, OpenTelemetry tracing (OTLP/HTTP), and health diagnostics with trend signals. Queue backlog is reported per queue as well as instance-wide, labeled by `route` and `consumer_group` so a stalled route is visible beside a busy one and can be joined against the pull connection gauge; every configured route gets a series, empty queue included. Pull consumers are identifiable rather than only countable: an Admin API listing of the SSE streams attached to each route, and connect/disconnect log lines, both naming the credential by its configured reference and never its value. Secret rotation health is reported the same way: per-pool version gauges with an expiry countdown, a health-endpoint rollup, and WARN lines at startup, on reload and on the sweep that observes a pool running out of valid versions, alongside auth rejects classified by cause and route.

**Release** - Cross-platform archives, signed checksums (Ed25519), SPDX SBOM, GitHub provenance/SBOM attestations, and `hookaido verify-release` CLI.

## Progress Matrix (MVP Core)

Weighted score from implemented behavior + regression coverage (weight sum = 100).

| Area | Weight | Score |
| --- | ---: | ---: |
| Ingress + routing | 15 | 95% |
| Queue + delivery semantics | 20 | 95% |
| Pull API | 10 | 97% |
| Retry/DLQ + queue mutations | 10 | 94% |
| Security/policy guardrails | 15 | 96% |
| Observability + health/trends | 10 | 95% |
| Config lifecycle/reload/fmt | 10 | 95% |
| Management model + MCP coverage | 10 | 93% |
| Release verification (attestation bundles) | 5 | 95% |

Current weighted implementation grade: **~95%**.

## What's Missing (MVP Core)

- Runtime reload intentionally keeps restart-required edges for topology/startup-bound changes (listeners, API prefixes, dispatcher-affecting settings).
- Test coverage is 77.3% against the 80% target tracked in `BACKLOG.md` (`make cover`, generated protobuf excluded). Ranked by uncovered statements, the gaps are `internal/config`, `internal/app`, `internal/mcp` and `internal/admin`; `internal/app` is the largest percentage gap and the hardest, since what is left are `run.go` startup paths needing a real server bring-up.

See `BACKLOG.md` for prioritized next steps.
