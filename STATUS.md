# Development Status

Last updated: 2026-02-12
Current release: v1.0.3

Lightweight project snapshot. Canonical spec: `DESIGN.md`. Detailed change history: `CHANGELOG.md`. Prioritized work items: `BACKLOG.md`.

## Capabilities Overview

**Ingress & Routing** - HTTP ingress with optional TLS/mTLS, HMAC signature verification (replay protection, secret rotation), Basic auth, forward auth callouts, and per-route/global rate limiting. Route matching supports path, method, host (wildcards), headers, query params, remote IP (CIDR), and named matchers. Channel types (`inbound`/`outbound`/`internal`) enforce directive constraints at compile time.

**Queue & Delivery** - SQLite/WAL persistent queue (in-memory available for dev/tests) with lease semantics, long-poll dequeue, dead-lettering, and queue limits/retention/pruning. Push dispatcher with retry/backoff, per-route concurrency, and optional outbound HMAC signing (multi-secret rotation). Pull API (HTTP/JSON) with bearer-token auth, dequeue limits, and per-route token overrides.

**Admin API** - Full queue lifecycle: DLQ management, message publish/cancel/requeue/resume (by ID and by filter), backlog drill-down (top queued, oldest, aging summary, trends with operator-action playbooks). Management model projection and endpoint mapping lifecycle with config-source-of-truth mutations. Structured JSON errors, audit headers, and publish-policy enforcement throughout.

**MCP** - Stdio JSON-RPC server (`hookaido mcp serve`) with role-gated access (`read`/`operate`/`admin`). Read tools for config inspection, queue state, and health diagnostics. Mutation tools (gated) for config apply, queue operations, and endpoint management. Runtime control tools (gated) for process lifecycle. Structured JSONL audit events, principal-authoritative actor binding, and Admin-proxy mode for memory-backend deployments.

**Config DSL** - Caddyfile-inspired syntax with `config fmt` round-trip stability. Env/file/vars placeholders, multi-value directives, hot reload via `--watch`/`SIGHUP`, and channel-type wrappers. Defaults blocks for egress policy, deliver settings, publish policy, and trend signal tuning. Secret refs support `env:`, `file:`, `vault:`, and `raw:`.

**Observability** - Structured JSON logs (access + runtime), Prometheus metrics endpoint, OpenTelemetry tracing (OTLP/HTTP), and health diagnostics with trend signals.

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

- Runtime reload has restart-required edges (listener/prefix changes, dispatcher-affecting settings).

See `BACKLOG.md` for prioritized next steps.
