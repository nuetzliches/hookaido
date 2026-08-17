# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- **The Stripe/Cituro rejection log no longer carries anything derived from the secret** ([GHSA-cpfq-rj4r-hh5c](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-cpfq-rj4r-hh5c)). On the `no_secret_matched` path the code recomputed `HMAC-SHA256(secrets[0], "<ts>.<body>")` and logged its first 8 hex characters as `want_prefix_first_secret`. Both halves of that message are attacker-controlled, so every rejected request handed out 32 verified bits of the correct MAC for a message of the caller's choosing — a chosen-message oracle that materially assists offline brute force of a weak secret, reachable unauthenticated and written into the runtime log sink. The field is removed entirely; a mismatch remains diagnosable from `secrets_tried`, `body_len` and `got_prefix`, none of which is secret-derived. Two related echoes are closed in the same pass: the `ts_parse_error` branch logged the unbounded attacker-supplied timestamp, and — less obviously — its `err.Error()`, since `strconv.ParseInt` returns a `*strconv.NumError` whose message embeds the entire input. Both are replaced by a length and an error classification (`not_a_number` / `out_of_range`), with no header material logged at all — matching the `parse_incomplete` path, which already reported `pairs_parsed` and `header_value_len` instead of the header value. Also resolves CodeQL alerts #66 and #68, both `go/clear-text-logging` on this line.

- **A config reload can no longer drop route authentication for an in-flight request** ([GHSA-rcpp-44rf-jxvj](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-rcpp-44rf-jxvj)). A single ingress request read the runtime state through eight independent lock acquisitions — first resolving the route, then looking up limits, basic auth, forward auth, HMAC auth and targets by route name. `reloadConfig` swaps the auth maps and the route table separately, so a request that resolved its route against the old table could miss in the new auth maps; a miss yields nil, and ingress reads nil as "no auth configured". Renaming or removing a route while traffic was flowing could therefore serve an unauthenticated request. The window normally closes in under a millisecond but spans the full request-body read, and reloads are routine — `--watch` triggers them, as does every applied Admin API managed-endpoint mutation. Route, authenticators, limits and targets are now captured in one `RouteSnapshot` under a single read lock. Rate limiting and adaptive backpressure deliberately remain live lookups, since they decide on current load rather than on the configuration the request was resolved against.

- **MCP admin proxy: TLS is verified off loopback, and an undeterminable queue backend no longer falls back to the local database** ([GHSA-r796-6hmg-f838](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-r796-6hmg-f838)). Two separate weaknesses. First, all three admin HTTP clients set `InsecureSkipVerify: true` whenever `admin_api.tls` was enabled, each commented as a local channel — but locality was never enforced, so with a non-loopback admin listener anyone able to answer for that address received the admin bearer token on the first call, and `admin_health` is a **read**-role tool. Verification is now skipped only for genuinely loopback endpoints, where a self-signed certificate is the normal setup; everything else is verified. The clients also no longer honour `HTTP(S)_PROXY`, which would otherwise hand the same token to a proxy. Second, `queueToolsUseAdminProxy` answered "false" for any config it could not load or compile — and false does not mean "unknown", it means "sqlite, open `--db` directly". A Postgres or memory deployment with a briefly unreadable config therefore lost the Admin API's authorization and policy enforcement and mutated whatever stale database file sat beside it, reporting success. It now returns an error and the eleven queue tools refuse. **Operator note: a deployment using a self-signed certificate on a non-loopback `admin_api` listener will start failing MCP admin calls until that certificate is trusted — that failure is the point.**

- **`dns_rebind_protection` now checks the address actually connected to** ([GHSA-g55x-rjfh-gxvm](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-g55x-rjfh-gxvm)). `checkEgressPolicyURL` resolved the delivery host, validated every returned address — and then discarded them, handing the *hostname* to the transport, which resolved it again independently. The checked address was never the connected address, so the setting did not stop the attack it is named for: a target answering with a short-TTL record could pass the check on a public address and be connected on `169.254.169.254`. Delivery now runs through a transport whose dialer re-validates the concrete peer address at connect time, which closes the window rather than narrowing it. Deny CIDR rules are enforced there too; allow rules stay at the URL level, since a hostname allow rule cannot be decided from an address alone. **Known limitation:** the transport still honours `HTTP(S)_PROXY`, and with a proxy configured the connection is made to the proxy — so the address checks apply to it and not to the delivery target. Deployments relying on `dns_rebind_protection` should keep proxy variables out of the process environment.

- **MCP `config_diff` no longer expands placeholders from caller-supplied content** ([GHSA-9r4r-gfq7-995c](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-9r4r-gfq7-995c)). `{file.*}` performed an unrestricted `os.ReadFile` and `{env.*}` / `{$...}` an `os.LookupEnv` at compile time, and the resolved value was interpolated verbatim into validation errors. `config_diff` is a **read**-role tool that accepts free-form `content`, so a read-only MCP session could pass `"/x{file./etc/hookaido/secrets/gh.key}"` and receive the file's contents back in the error message — bypassing the config-path allowlist entirely, since the content never goes through it. `{file./dev/zero}` was additionally an unbounded read. A new `config.CompileUntrusted` skips expansion for content that did not come from the operator's own config file, warning about each placeholder it left alone; `config_diff` uses it. Trusted compilation of the operator's own Hookaidofile is unchanged, and validation of a placeholder-using candidate is correspondingly less precise — that is the intended trade.

- **The ingress HMAC nonce is claimed only after the signature verifies** ([GHSA-cmwq-5829-xw24](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-cmwq-5829-xw24)). The nonce was inserted into the replay cache *before* the signature comparison, so any unauthenticated caller could write to it. Two consequences: the cache grew without bound from rejected requests — with no entry cap, no nonce-length limit, and a full-map sweep on every call that made each request O(entries) under the lock — and anyone able to observe or predict a nonce could claim it first, causing the genuine signed webhook carrying it to be rejected as a replay. Claiming now happens only on the success path, which removes the unauthenticated write entirely. Additionally: nonces longer than 256 bytes are rejected rather than remembered, the cache is capped at 100k entries and evicts the entry closest to expiring rather than refusing a validly signed request, and the sweep runs at most every 30s instead of on every call.

- **An `auth hmac { }` block that declares no secret is now a compile error** ([GHSA-5v3w-hjh4-4q9q](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-5v3w-hjh4-4q9q)). `Route.AuthHMACBlockSet` records that a block was written, but `Compile` never read it — only the formatter did. The missing-secret guard covered `signature_header`, `timestamp_header`, `nonce_header`, `tolerance` and `provider`, so a block containing none of those and no secret compiled clean. At runtime the route then mapped to a nil authenticator, which ingress reads as "no auth configured": the route accepted every unsigned request. The adjacent case was already handled — `auth hmac { provider github }` without a secret errored — so only the fully empty block slipped through, reachable by the plausible operator action of commenting out a `secret` line during debugging or rotation. The mutual-exclusion checks against `auth basic` and `auth forward` now also treat an empty block as a declared HMAC surface. **Operator action: audit Hookaidofiles for `auth hmac` blocks with no `secret` or `secret_ref`; any such route has been serving unauthenticated.**

- **`auth basic` now rejects secret-reference syntax instead of using it as the credential** ([GHSA-q2r7-wm3j-xppw](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-q2r7-wm3j-xppw)). Unlike `auth token`, `auth hmac` and `secret` blocks, basic-auth credentials are never passed through `secrets.LoadRef` — the value is compared literally. A route configured as `auth basic "user" "env:WEBHOOK_PASSWORD"` therefore accepted the string `env:WEBHOOK_PASSWORD` as the password, with no error and no warning. That exact example was published in `docs/ingress.md` and `docs/security.md`, so any deployment that copied it was protected by a password printed in this project's own documentation. The compiler now rejects a basic-auth user or password that parses as an `env:` / `file:` / `vault:` / `raw:` reference, and points at the `{env.NAME}` placeholder form, which does resolve. **Operator action: rotate any basic-auth credential that was configured with reference syntax, and treat it as disclosed.**

- **Every HTTP listener now sets `ReadHeaderTimeout` and `IdleTimeout`** ([#203](https://github.com/nuetzliches/hookaido/issues/203)). Both `http.Server` constructions — the shared/per-component ingress, pull_api and admin_api servers, and the metrics server — were built without any timeout, so a client dribbling one header byte at a time pinned a goroutine and a file descriptor indefinitely (Slowloris). Ingress is the component most likely to face the open internet, and nothing else bounded header read time. Servers are now built through a single `newHTTPServer` helper with a 15s header-read and 120s idle timeout, and a test guards that no listener is constructed around it. `ReadTimeout` and `WriteTimeout` remain deliberately unset — a write deadline would truncate the Pull API's SSE stream, and a read deadline would cap the time available to receive a legitimate `max_body`-sized payload over a slow link.

### Fixed

- **Push delivery never fired on the Postgres queue backend** ([#200](https://github.com/nuetzliches/hookaido/issues/200)). `Dequeue` filtered unconditionally on `route = $1 AND target = $2`, while the memory and SQLite backends treat an empty field as "any". The push dispatcher relies on that wildcard — it dequeues per route without naming a target and resolves the target per item afterwards — so on Postgres the query resolved to `target = ''` and matched nothing. Ingress enqueued normally, nothing was ever delivered, and the backlog grew until `max_depth` began rejecting, with no error logged because the queue simply looked empty to the dispatcher. Pull routes were unaffected, since those pass an explicit target. The `WHERE` clause is now built conditionally, matching the other two backends and the existing `ListMessages` query in the same file. A new `queue.Store` contract case covers all three wildcard combinations — every pre-existing contract test named both fields explicitly, which is why the divergence went unnoticed.

- **A rejected enqueue no longer destroys the messages it evicted** ([#202](https://github.com/nuetzliches/hookaido/issues/202)). With `drop_policy drop_oldest`, the memory backend evicted queued messages to make room *before* the remaining admission checks ran. When the enqueue was then rejected — `ErrQueueFull` because the evictions did not free enough room, `ErrMemoryPressure`, or a duplicate ID — those evictions stayed in effect. `EnqueueBatch` documents itself as all-or-nothing, so the caller saw "nothing was enqueued" while previously accepted messages had silently been thrown away; single `Enqueue` had the same shape. SQLite performs the identical drops inside `BEGIN IMMEDIATE` and was never affected. The memory backend now records what it evicts and restores it on every rejection path, using the same `committed`-plus-`defer` idiom the SQLite store already uses; rolled-back evictions are also no longer counted in `hookaido_queue_memory_evictions_total`.

- **Graceful shutdown mid-batch no longer causes duplicate delivery** ([#201](https://github.com/nuetzliches/hookaido/issues/201)). On single-target routes the dispatcher accumulates lease actions and flushed them only when the mutation batch filled or the batch loop finished. A stop signal arriving mid-batch requeued the unprocessed tail and returned immediately, discarding every pending ack, nack and mark-dead for items that had *already* been delivered. Those leases then sat untouched until the lease TTL expired (70s with defaults), after which the messages were dequeued and delivered a second time; items classified dead in the same window were redelivered instead of dead-lettered. Reachable with stock configuration — `deliver_concurrency 20` yields a dequeue and mutation batch of 4 — so it fired on any graceful restart or delivery-config reload that landed mid-batch. The batch loop now lives in its own function with the flush on a `defer`, so no early exit can drop pending actions.

### Security

- The `hmac_stripe_failed` / `reason=parse_incomplete` rejection log no longer contains the raw signature header. Previously the `header_value` field carried the untouched `cfg.Header` value, so a request that omitted `t=` but supplied a full `v1=<64 hex chars>` signature wrote that signature verbatim into the WARN log — an attacker-triggerable path, since malformed input is exactly what reaches it. The field is replaced by `pairs_parsed` (number of `k=v` pairs found) and `header_value_len`, which together with the already-logged, config-derived `sig_tag` still identify the realistic cause: the sender emits a different tag name than the route expects. This restores the fingerprints-only contract the other five rejection paths already followed. Reported by CodeQL as `go/clear-text-logging`.

- Bump Go toolchain `1.26.4` → `1.26.6` to remediate eight reachable standard-library vulnerabilities reported by `govulncheck`:
  - **GO-2026-6218** (`net/url`, quadratic complexity in `resolvePath` — reachable via `dispatcher.HTTPDeliverer.Deliver` → `http.Client.Do` → `url.URL.Parse`)
  - **GO-2026-6091** (`html/template`, JavaScript regexp context tracking — reachable via `app.serveOnListener` → `http.Server.Serve` → `template.Template.Execute`)
  - **GO-2026-6090** (`crypto/tls`, unbounded post-handshake messages — reachable via `app.serveOnListener` → `http.Server.Serve` and `grpcworker.grpcWorkerModule.Serve` → `grpc.Server.Serve`)
  - **GO-2026-6089** (`net/http`, `ReadHeaderTimeout` not applied during the unencrypted HTTP/2 check — reachable via `app.serveOnListener` → `http.Server.Serve`)
  - **GO-2026-6088** (`encoding/xml`, missing recursion-depth guard during decode — reachable via `sqlite.Store.ListAttempts` → `sql.Rows.Next` → `xml.Unmarshal`)
  - **GO-2026-5972** (`encoding/asn1`, unbounded recursion depth — reachable via `release.loadEd25519PrivateKey` → `x509.ParsePKCS8PrivateKey`)
  - **GO-2026-5856** (`crypto/tls`, Encrypted Client Hello privacy leak — reachable via the same TLS entry points as GO-2026-6090)
  - **GO-2026-5026** (`net/http`, `golang.org/x/net/idna` failing to reject ASCII-only Punycode-encoded labels — reachable via `dispatcher.HTTPDeliverer.Deliver` → `http.Client.Do`)

  `govulncheck ./...` now reports no findings.

## [2.9.0] - 2026-07-01

### Added

- **Single-port deployments — `ingress` can share a listener with `pull_api` (and, transitively, `admin_api`)** ([#183](https://github.com/nuetzliches/hookaido/issues/183)). When `ingress.listen` equals `pull_api.listen` (and/or `admin_api.listen`), the components are served on one `net.Listener`, disambiguated by path prefix: ingress serves its bare route paths (`/webhooks/...`) as the default handler while the API servers serve under their `prefix` values (e.g. `/pull`, `/admin`). This mirrors the existing `pull_api == admin_api` shared listener and is strictly opt-in via matching listen addresses — the default topology remains one listener per component. Compile-time validation requires co-listening API servers to have non-empty, distinct, non-overlapping prefixes, additionally rejects any ingress route path that collides with a co-listening API prefix, and enforces identical TLS across the shared address. `pull_api.grpc_listen` and `observability.metrics.listen` remain dedicated listeners. Motivated by reverse-proxy path-routing and single-published-port orchestrators. A new `ingress_shared` flag is surfaced alongside `shared_listener` in the MCP `config_compile` summary and runtime-control status; toggling the shared-listener mode is restart-required.

## [2.8.1] - 2026-06-04

### Security

- Bump Go toolchain `1.25.9` → `1.26.4` to remediate two reachable standard-library vulnerabilities reported by `govulncheck`: **GO-2026-5039** (`net/textproto`, unescaped input in errors — reachable via `ingress.Server.ServeHTTP` → `ReadMIMEHeader`) and **GO-2026-5037** (`crypto/x509`, inefficient candidate hostname parsing — reachable via `dispatcher.HTTPDeliverer.Deliver` → `Certificate.Verify`/`VerifyHostname`). `govulncheck ./...` now reports no findings.

### Changed

- Routine dependency bumps since 2.8.0 (Go modules and GitHub Actions, via Dependabot `go-patch-minor` / `actions-patch-minor` groups), including the `golang:1.26-alpine` build-image digest.

## [2.8.0] - 2026-05-09

### Changed

- **Module path now `github.com/nuetzliches/hookaido/v2`** (Go modules v2+ rule). Tools that resolve modules via `proxy.golang.org` (`go install`, `go get`, pkg.go.dev, Go Report Card, awesome-go) previously fell back to v1.5.1 because the unsuffixed import path is not allowed for v2.x.x tags. After this release:
  - **User action**: `go install github.com/nuetzliches/hookaido/v2/cmd/hookaido@latest` instead of the unversioned path. Existing `docker pull ghcr.io/nuetzliches/hookaido:<tag>` workflows are unaffected.
  - All internal imports in this repo now reference `/v2/...`.
  - Release LDFLAGS (`internal/tools/release/main.go`, `Dockerfile`) updated to inject `-X` into the `/v2/internal/app` symbol path.
  - `modules/grpcworker/proto/workerapi.proto` `go_package` option and the generated `workerapi.pb.go` descriptor updated to `/v2`.
  - Repo-wide `gofmt -s` cleanup of 10 files (whitespace alignment) so Go Report Card stays at A+ once it can finally resolve current tags.

### Fixed

- Postgres `Enqueue` no longer rejects envelopes with nil `Payload`. The `payload BYTEA NOT NULL` schema constraint previously made the cross-package `queue.Store` contract tests fail for the Postgres backend while memory/sqlite tolerated it; nil is now coerced to `[]byte{}` to match contract behavior. Surfaced during the coverage uplift that lifted the project total from 70.6% to 75.2% (modules/postgres 14.4% → 80.8%, modules/otel 18.4% → 93.4%, modules/mcp 24.5% → 96.1%, internal/tools/release 32.4% → 84.3%, internal/release/sbom 67.6% → 97.2%, internal/app 54.9% → 61.1%). New `make test-pg` target runs the suite serialized (`-p 1`) so cross-package Postgres integration tests don't race on the shared DSN.

## [2.7.1] - 2026-04-21

### Fixed

- Bare `GET /healthz` (no query string) now bypasses the `admin_api.auth token` guard so orchestrator liveness probes (Docker healthcheck, Kubernetes `livenessProbe`, cloud load balancers) can reach it without a bearer token ([#153](https://github.com/nuetzliches/hookaido/issues/153)). `/healthz?details=1` (and any other query string, including `?details=0`) still follows admin auth — the diagnostic payload leaks queue/backlog/ingress telemetry that must stay gated. Removes the `CMD-SHELL` Bearer-token workaround required by v2.7.0 deployments with admin auth enabled; the static `ok\n` response body exposes no deployment-sensitive information.

## [2.7.0] - 2026-04-21

### Added

- Runtime HMAC secret rotation via admin API (issue #147): declare a pool with `secret "<name>" { runtime true; max_versions N }`, then push new verification secrets at runtime via `POST /admin/secrets/{name}` (plus `GET` for metadata-only listing and `DELETE /admin/secrets/{name}/{id}` for revocation). Persistence to SQLite (`schemaV7`) and Postgres with AES-256-GCM sealing via `HOOKAIDO_SECRET_ENCRYPTION_KEY` (32 bytes, base64). Route closures reference pools by pointer so admin mutations are visible to in-flight verification without a reload. Enables zero-downtime rotation flows where the issuer service (e.g. soapNEO for Cituro) receives the fresh secret from the upstream provider and pushes it to Hookaido (DMZ) without a redeploy.
- Background sweeper for expired runtime-secret versions: every 5 minutes (plus once on startup) the process prunes versions whose `not_after` is in the past from every registered pool and from the persisted `runtime_secrets` table. Complements `Pool.Add`'s opportunistic prune, which only fires at `max_versions`, so deployments with short overlap windows no longer accumulate expired rows in `GET /admin/secrets/<name>` or in the DB. New Prometheus counter `hookaido_runtime_secret_gc_pruned_total{pool="<name>"}` and `secret_gc_pruned` log lines expose what was swept.
- MCP `rotate_secret` tool (role `admin`, requires `--enable-mutations`): proxies `add`/`list`/`delete` operations to `/admin/secrets/...` for AI-assisted rotation workflows. Audit reason/actor propagate via principal; list responses contain metadata only (never the plaintext value).

### Planned

- Pluggable Sealer backends (env / AWS KMS / HashiCorp Vault Transit) for compliance-regulated deployments and automated KEK rotation — see [#151](https://github.com/nuetzliches/hookaido/issues/151). The v2.7.0 sealed-record layout reserves version bytes `0x02`/`0x03` so records written by this release stay readable when the alternative backends ship.

## [2.6.0] - 2026-04-20

### Added

- Stripe-compatible HMAC verification: `auth hmac { provider stripe; secret env:SECRET }` verifies the `Stripe-Signature: t=<ts>,v1=<hex>` header format with the `<ts>.<body>` signed payload and a 5-minute timestamp tolerance. Multiple comma-separated `<tag>=<hex>` pairs in the header are accepted (covers Stripe's v0/v1 rotation).
- Cituro HMAC verification: `auth hmac { provider cituro; secret env:SECRET }` reuses the Stripe scheme with header `X-CITURO-SIGNATURE` and signature tag `s` instead of `v1`. Matches the wire format documented in Cituro's API spec.

## [2.4.0] - 2026-04-16

### Added

- SSE endpoint for Pull API (`GET {pull.path}/stream`): consumers can receive webhook messages in real-time over a persistent Server-Sent Events connection instead of polling. Each SSE message creates a lease (same semantics as dequeue); ACK/NACK remain via existing POST endpoints. Supports `batch` and `lease_ttl` query parameters, configurable keepalive interval (`sse_keepalive`, default 15s) and max connection duration (`sse_max_connection`). Multiple concurrent SSE connections act as competing consumers. New Prometheus metrics: `hookaido_pull_sse_connections_total`, `hookaido_pull_sse_messages_sent_total`, `hookaido_pull_sse_connection_active`.

## [2.2.2] - 2026-04-15

### Fixed

- Queue dequeue loop now uses event-driven wake-up instead of aggressive 25ms polling. Enqueue signals waiting Dequeue goroutines immediately via channel notification; polling interval raised to 1s as fallback for delayed/retry items only. Reduces idle CPU from ~26% to <1% (SQLite and PostgreSQL backends).

## [2.2.1] - 2026-03-30

### Fixed

- Dispatcher now logs all delivery attempts: `delivery_ok` (INFO), `delivery_retry` (INFO), and `delivery_dead` (WARN) with route, target, status, attempt number, and event ID. Previously, deliveries were completely silent in logs.
- Routes with zero targets now emit a `dispatcher_route_no_targets` warning instead of being silently skipped.
- Dispatcher delivery config changes (targets, signing, egress policy) are now hot-reloaded via `--watch`/SIGHUP without requiring a full process restart. Previously, config reload detected the mismatch but did not recreate the dispatcher, causing webhooks to be accepted by ingress but never delivered.

## [2.2.0] - 2026-03-28

### Added

- `deliver exec` directive: deliver webhook payloads by executing a local subprocess. Payload on stdin, metadata as env vars (`HOOKAIDO_ROUTE`, `HOOKAIDO_EVENT_ID`, `HOOKAIDO_CONTENT_TYPE`, `HOOKAIDO_ATTEMPT`, `HOOKAIDO_HEADER_*`), user-defined `env` vars from config. Exit code mapping for retry/DLQ semantics. Cross-platform via `os/exec`.
- Documentation: new `docs/recipes.md` with four practical patterns (GitHub→Deploy Script, Stripe→Billing, Multi-Provider Fan-Out, CI/CD Job Queue). Exec delivery documented across configuration, delivery, ingress, and getting-started guides.

### Fixed

- Docker entrypoint volume ownership fix: container starts as root, `chown`s `/app/.data` to `hookaido` (UID 1000), then drops privileges via `su-exec`. Prevents `SQLITE_CANTOPEN` on first start with Docker volumes. Rootless-compatible (skips `chown` when run with `--user`).
- HMAC string-to-sign ordering in Hookaidofile comment corrected to match canonical format (`METHOD + PATH + TIMESTAMP + SHA256`).

### Changed

- README restructured: grouped features (Core/Security/Operations), added use-cases section, reduced badges, streamlined config examples and documentation table.
- CI workflow version comments updated for precision across all pinned actions.

## [2.1.0] - 2026-03-25

### Added

- Provider-compatible HMAC verification: `auth hmac { provider github; secret env:SECRET }` and `auth hmac { provider gitea; secret env:SECRET }` for GitHub (`X-Hub-Signature-256`) and Gitea/Forgejo (`X-Gitea-Signature`) webhook signature formats without timestamp/nonce replay protection.
- Custom outbound headers in deliver blocks: `header "Name" "Value"` with placeholder interpolation (`{env.VAR}`, `{$VAR}`, `{file.PATH}`, `{vars.NAME}`); case-insensitive duplicate detection at compile time.
- WorkerAPI gRPC transport edge-case test coverage: 14 new tests (57 sub-tests) covering nil requests, blank endpoints, Pull-nil guards, lease ID normalization, error mapping, route resolution fallback, nack-dead, nack-batch, and large-batch dequeue.

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
