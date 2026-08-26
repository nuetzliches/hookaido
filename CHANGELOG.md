# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **`match header` and `match query` compare values in constant time.** Every authentication path in the project already did — basic auth, HMAC, and the pull/worker/admin bearer tokens — but the matchers used a plain `==`. That is harmless while a matcher only selects a route, and it stops being harmless because `match header`/`match query` are the *only* credential check available for an event source whose entire configuration surface is a single URL field, which is common with telephony platforms, appliance webhooks and older ERP systems. In that setup the matcher *is* the credential comparison, and it was the one in the codebase that leaked timing. The comparison no longer returns early on the first matching value either, so the number of values in the request does not influence timing; value *lengths* stay distinguishable, which is not worth padding away for a route matcher. ([#274](https://github.com/nuetzliches/hookaido/issues/274))

## [2.11.0] - 2026-08-18

A correctness release. It closes the ten umbrella issues from the second full
code review ([#249](https://github.com/nuetzliches/hookaido/issues/249)–[#258](https://github.com/nuetzliches/hookaido/issues/258)),
which covered config parsing, the reload lifecycle, ingress replay protection,
the dispatcher, all three queue backends, the pull/worker transports and the MCP
tools.

The importable Go API is backward compatible — nothing exported by `modules/*`,
`cmd/*` or the root package was removed or changed — so this stays on the `/v2`
module path. What does change is runtime behaviour, in the places listed below;
`attempts_retention` is the only new configuration surface, and the only entry
that deletes data on upgrade.

### Added

- **`GET /healthz?details=true` reports the running config's identity** under `diagnostics.config`: `fingerprint` (SHA-256 of the config file bytes as loaded), `generation` and `loaded_at`. Liveness alone cannot tell you which config a process is running, so this is what lets a deployment pipeline — or the MCP tools below — confirm that a config it wrote was actually adopted.

- **`attempts_retention { max_age, max_rows }`** bounds delivery-attempt history, which was append-only in every backend — nothing anywhere deleted or capped it. A push deployment doing 10 attempts/s added ~860k records a day forever: the SQLite file and its indexes grew until the disk filled and enqueue started failing, and the memory backend grew until the process was OOM-killed, invisible to the memory-pressure guard because that counts only queued payloads. Defaults are deliberately finite (`max_age 7d`, `max_rows 200000`); set both `off` to restore the previous unbounded behaviour. Pruning uses the existing `queue_retention.prune_interval` cadence, and the memory backend enforces `max_rows` as it writes.

### Fixed

- **Postgres: routine retention no longer stalls ingest.** `maybePrune` held the store mutex — which also guards `now()`, the notify channel and every store operation's first step — across up to four unbounded `DELETE` statements. A retention pass with millions of eligible rows blocked all enqueues, dequeues, SSE notify registrations and long-poll wakeups for the duration of the delete. Pruning now runs under its own mutex, as it does on SQLite, and deletes in bounded chunks. On both backends a store operation that arrives while a prune is running now proceeds instead of queueing behind it.

- **Postgres: batch enqueues no longer skip the depth-limit lock.** The lock-free margin was derived from the caller's own batch size, which only holds while every concurrent enqueue is the same size. With `max_depth 10000` and an empty queue, several 1000-item batches could each pass the check against the same committed count and insert thousands of rows past the limit. Batches now always take the advisory lock. Single-item enqueues keep the fast path with the same threshold as before, so the residual overshoot is at most one item per pooled connection — documented rather than promised away, because closing it entirely means locking every enqueue at roughly a quarter of the ingress throughput.

- **Ready messages wake connected consumers instead of waiting for a timeout.** Three gaps, none of which lost a message but each of which added latency to a consumer that was already connected and idle. The SSE stream took its notify channel *after* the empty dequeue, so a message published in that window handed it a channel that had already fired and the message sat ready until the next keepalive (15s by default, and `sse_keepalive` has no upper bound). The memory backend signalled on enqueue and requeue but not on `Nack` or the expired-lease sweep. The Postgres backend signalled on enqueue only — so a `Nack(lease, 0)` or an operator DLQ requeue was invisible to a waiter until it timed out, while SQLite signalled on all of them; documented backend parity is restored, including the expiry sweep, which no backend announced.

- **Memory backend: delivered history no longer fills the queue.** With `delivered_retention` enabled, the depth admission check counted delivered tombstones against `max_depth` — so completed work exerted backpressure on ingest. With `max_depth 10000`, 9,999 tombstones and five live messages, the default `reject` policy answered 503 to a near-empty backlog, and `drop_oldest` was worse: it picked eviction candidates from queued items only, destroying live undelivered webhooks one by one to preserve already-delivered history, and still failed once none were left. Depth is now `queued + leased`, as on the SQLite and Postgres backends. The memory bound the check was reaching for is kept, but applied to history itself: at most `max_depth` tombstones, oldest evicted first, under the new `delivered_retention_depth` eviction reason.

- **An exec target can no longer wedge a route worker forever.** `cmd.Stderr` is a buffer, so os/exec copies it through a pipe and `Wait` blocks until that pipe reaches EOF — which killing the direct child on the per-attempt deadline does not achieve if the script left a background process holding it (`mydaemon &`). The worker goroutine never returned: route concurrency degraded to zero worker by worker, the leased message came back only through lease expiry, and every `Drain()` on reload or shutdown timed out from then on. Delivery now waits at most 2s past the process exit or the deadline. If the command itself exited `0`, the delivery counts as delivered — its exit status is authoritative — and `exec_lingering_output` is logged.

- **Requeued messages get their retry budget back.** `RequeueDead` and the message-management requeue endpoints reset state, lease and reason but kept the attempt count, so a message that dead-lettered at attempt 9 came back at 9: the next dequeue made it 10, the dispatcher's retry gate was false for every retryable outcome, and a single 503 or connection-refused dead-lettered it again after one delivery. The configured exponential schedule never applied to requeued messages at all, so requeueing a cohort during a brief target blip re-dead-lettered the whole cohort. Requeue now resets the attempt count in all three backends. Resume is deliberately unchanged: it continues a canceled message rather than re-injecting it.

- **A reload that changes only a delivery header or an exec target's `env` now reaches the dispatcher.** The comparison that decides whether the push dispatcher must be swapped looked at target URL, timeout, retry and signing only — it had never been extended for `CustomHeaders`, `IsExec` and `ExecEnv`. Rotating a bearer token in `deliver … { header Authorization … }` and reloading logged `config_reloaded_ok`, and every observability surface reported the new value, while deliveries kept sending the old token until restart. The comparison now covers every field of the dispatcher's target config, lives next to that struct, and is guarded by a test that fails if a field is added without extending it.

- **Admin API managed-endpoint mutations reconcile the dispatcher.** They reload the config and advance the running config without touching the dispatcher, so a `deliver` change already written to the Hookaidofile but not yet reloaded was folded into the running config while the dispatcher kept the old routes — and since every later reload diffs against the running config, no reload could correct it. Deliveries went to the old targets until process restart.

- **A SIGHUP during startup no longer leaks a dispatcher.** The signal handler began running before the initial dispatcher was installed, so a reload landing in that window started a dispatcher for the updated config which the startup path then overwrote: the reload's dispatcher stayed running and undrainable with its workers still dequeuing, two dispatchers delivered concurrently, and the retained one ran the stale config. Signal *registration* still happens as early as before (SIGHUP would otherwise terminate the process) but handling waits for startup, and the signal is buffered rather than dropped.

- **A 503'd webhook can be retried again.** On an HMAC route with nonce replay protection, the nonce was burned the moment the signature matched — before the enqueue. When the queue then rejected the message the server answered 503, explicitly inviting a retry, but the sender's identical signed retry hit the claimed nonce and got 401 for the rest of the tolerance window, so transient backpressure turned into permanently lost webhooks. The claim is now provisional: it still blocks a concurrent replay immediately, but it only becomes permanent once every target is durably enqueued, and it is released on the 503 and 413 paths. On a multi-target route this also fixes a retry never being able to reach the targets that failed.

- **Stripe/Cituro webhooks verify during a secret roll.** Only the first `v1=` entry in the signature header was compared. Stripe signs with *all* active secrets while an endpoint secret is rolled, emitting several `v1=` entries, so whenever the one matching the configured secret was not listed first, validly signed webhooks were rejected with 401 for the whole roll window — up to 24h. Every entry carrying the configured tag is now a candidate (bounded at 16), which is what the documentation already described. The `sig_hex_decode_error` log line drops the `sig_hex_prefix` field — it echoed attacker-controlled header material, which the rest of that function deliberately avoids — and gains `sig_index` and `sig_tag`.

- **Config reloads no longer reset replay protection.** Rebuilding a route's HMAC authorizer gave it an empty nonce cache, so every reload forgot every nonce inside the tolerance window and reopened the replay window for that long — and Admin API managed-endpoint mutations reload too, making it reachable on demand. A route now carries its nonce cache across reloads.

- **Basic-auth verification is constant time.** An unknown username returned before any comparison, and the password compare returned early on a length mismatch, so response timing leaked which usernames exist and how long the configured password is. Credentials are now stored as SHA-256 digests, unknown users are compared against a decoy digest, and the comparison is fixed-width.

- **Quoted values keep their backslashes.** An unknown escape sequence dropped the backslash and kept only the escaped character, so `cert_file "C:\certs\server.pem"` parsed as `C:certsserver.pem` and `"^/hooks/\d+$"` as `^/hooks/d+$` — silently, with the failure surfacing later as a missing file or a matcher that never matches. `config fmt` and Admin API config rewrites then wrote the corrupted value back. `\\`, `\"`, `\n`, `\t` and `\r` are unchanged; every other backslash is now preserved, and the formatter re-escapes it so values survive any number of format cycles.

- **Admin API managed-endpoint mutations no longer delete the comments in your Hookaidofile.** Every applied upsert or delete rewrote the file through the formatter, which regenerates it from a parse tree that keeps comments only above the first statement — one admin call erased every route annotation and rotation note in the file the project declares the source of truth. Mutations now splice just the `application`/`endpoint_name` directives into the existing file, leaving comments, blank lines and formatting untouched, and verify the spliced result against the canonical form before writing. `config fmt` still regenerates the file (documented under [Values, Quoting and Comments](docs/configuration.md#values-quoting-and-comments)).

- **Parse errors in a value position report what actually went wrong.** An unterminated string or invalid UTF-8 was reported as `expected value` at position `0:0` — a position that cannot exist, since lines are 1-based — instead of `unterminated string at 12:18`.

- **MCP `config_apply` no longer reports a reload that did not happen.** The "reload health check" polled `GET /healthz` for a 200 — the liveness endpoint a running instance answers regardless of which config it runs. With `hookaido run` started without `--watch` (the default) the file write triggered nothing at all, and the tool still returned `ok/applied/reloaded`: a token revoked through `config_apply` was reported applied while the old token kept authenticating. A reload that failed at apply time looked identical. Because the check could not fail for those reasons, the rollback path was effectively dead code. `config_apply` now signals the instance when a pid file is configured, then waits for it to report the fingerprint of the bytes just written, and rolls the file back when it does not. `instance_reload` verifies the same way. An instance too old to report its config identity yields `applied: true, reloaded: false` plus a `reload_verification` note rather than a false success.

- **MCP `messages_publish` is atomic on the direct SQLite path.** A batch was enqueued item by item, so a mid-batch `ErrQueueFull` left items `[0,k)` queued while the tool returned only an error — and retrying the same batch then failed at item 0 with "already exists", leaving the operation permanently half-applied and un-retryable without manual cleanup. It now uses the backend's transactional batch enqueue, matching the contract the admin-proxy path already kept.

- **MCP `instance_stop` on Windows no longer hard-kills while reporting a graceful stop.** Windows maps every signal to `TerminateProcess`, so the SIGTERM phase terminated the queue server with no drain and no shutdown hooks, and both the tool output (`forced: false`) and the audit event recorded it as graceful. An un-forced stop is now refused there with an explanation; `force` terminates and is reported as `forced: true`.

- **MCP read-only tool sets open the database read-only.** Without `--enable-mutations`, `mcp serve --db` still called the full store constructor on every tool request: `BEGIN IMMEDIATE` migration transactions and `PRAGMA journal_mode=WAL` against the running server's database, plus a checkpoint loop. Pointing a newer binary's MCP server at an older still-running server migrated its schema forward, and the older server then failed its downgrade guard at the next restart; every call also contended for the write lock with the live server.

- **A cancelled dequeue no longer parks a goroutine or leases into a dead connection.** `queue.Store.Dequeue` takes no context, so a gRPC worker whose own deadline fired left the handler blocked inside the store for the rest of `max_wait` — and any item that arrived meanwhile was leased into a stream gRPC had already discarded, invisible for the full lease TTL (30s by default) until the expiry sweep reclaimed it. Every client timeout thus became a lease-TTL delivery delay. All three backends now implement a context-aware dequeue, and the pull API passes the request context through from HTTP, SSE and gRPC: the long poll ends with its caller, and a caller that is already gone is never handed a lease.

### Security

- **Per-route pull tokens are now enforced on lease operations.** `ack`, `nack`, `dead` and `extend` passed only the lease ID to the store; the route resolved from the request path was used for metrics and nothing else. A client authorized for one endpoint could therefore settle another route's in-flight message if it learned that lease ID — from a shared dashboard, a log line, a support ticket. Exploiting it requires the random `lease_…` value, so severity is low, but the route-scoped credential model the config offers was not enforced where it mattered. A lease belonging to another route is now rejected with the same `409` as an unknown lease, so the response cannot be used to probe for lease IDs either. The check runs only when the config actually uses per-route pull tokens: with a single global token every client is authorized for every route anyway, and the ack path keeps its current cost.

## [2.10.1] - 2026-08-18

Publishes the container images for the 2.10.0 release. 2.10.0 itself is complete
on GitHub Releases — archives, checksums, signature, SBOM and attestations all
published — but the GHCR image build failed, so `ghcr.io/nuetzliches/hookaido`
still points at 2.9.0. Upgrade notes for the actual changes are under
[2.10.0](#2100---2026-08-18).

### Fixed

- **The container image builds again.** The Dockerfile pinned a `golang:1.26-alpine` digest shipping Go 1.26.5 by content, while 2.10.0 bumped `go.mod` to require `go >= 1.26.6` (a `govulncheck` remediation). With `GOTOOLCHAIN=local` in that image, `go mod download` refused with `go.mod requires go >= 1.26.6 (running go 1.26.5)` and the multi-arch build failed. The pin is bumped to a digest shipping 1.26.6. Nothing caught this before the tag because the image was only ever built by the tag-triggered `container` workflow — v2.9.0 published fine, since `go.mod` still asked for 1.26.4 then.

- **CI now builds the container image and smoke-tests it on every pull request** (`container-build`), so toolchain drift between `go.mod` and the Dockerfile fails a PR instead of a release. Build only, never push — publishing stays tag-driven. It runs `linux/amd64` only, since what it guards against is architecture-independent and a QEMU multi-arch build would cost minutes per PR for no added signal, and it runs `hookaido version --long` inside the image afterwards: building is not enough to prove an image can start.

- **`*.sh` is pinned to LF in `.gitattributes`.** `docker-entrypoint.sh` is the image `ENTRYPOINT`; on a Windows checkout `* text=auto` gave it CRLF, so its shebang kept a trailing carriage return and the locally built image died with `exec /usr/local/bin/docker-entrypoint.sh: no such file or directory`. CI checks out on Linux and never saw it, so published images were always fine — but a local `docker build` on Windows produced a broken one, which is also what blocked verifying the fix above.

## [2.10.0] - 2026-08-18

A hardening release. It closes the six umbrella issues from a full code and
documentation review ([#206](https://github.com/nuetzliches/hookaido/issues/206),
[#207](https://github.com/nuetzliches/hookaido/issues/207),
[#208](https://github.com/nuetzliches/hookaido/issues/208),
[#209](https://github.com/nuetzliches/hookaido/issues/209),
[#210](https://github.com/nuetzliches/hookaido/issues/210),
[#211](https://github.com/nuetzliches/hookaido/issues/211)) together with seven
security advisories, and adds the CI coverage that would have caught most of it.

The importable Go API is backward compatible — nothing exported by `modules/*`,
`cmd/*` or the root package was removed or changed — so this stays on the `/v2`
module path. What does change is configuration validation and runtime behaviour.

### Upgrade notes

**Configs that used to compile may now be rejected.** Each of these was already
broken or already insecure; the compiler now says so instead of starting anyway.

- An `admin_api` with no `auth token` on a non-loopback `listen` address, or
  co-listening with `ingress`. **The endpoint was open — treat it as disclosed
  and add a token.** `docs/docker.md` recommended exactly this without a token
  and has been corrected.
- A route that an earlier, unconstrained route prefix-shadows (`/hooks/github`
  after `/hooks`, or anything after a `/` catch-all). The shadowed route was
  never receiving traffic; the shadowing one was. Reorder so the more specific
  path comes first.
- Two components whose `listen` addresses differ as strings but name one socket
  (`:8080` vs `0.0.0.0:8080`). These never started — the failure moves from
  `EADDRINUSE` to a compile error naming both.
- A size value above `1024gb`, or one whose multiplication overflows.
  `max_body 18014398509481985k` previously yielded a **1 KiB** limit.
- An `auth hmac { }` block with no secret, and `auth basic` credentials written
  in `env:`/`file:`/`vault:`/`raw:` reference syntax. **Rotate any basic-auth
  credential configured that way and treat it as disclosed.**

**Behaviour changes worth planning for.**

- **MCP `config_apply` now requires a `reason` argument.** Existing clients
  calling it without one are rejected. It is the same audit triple every other
  mutating tool already takes.
- **`pull_api.max_lease_batch` (new, default 100)** replaces the accidental use
  of `pull_api.max_batch` as the gRPC lease-batch cap. A deployment that raised
  `max_batch` above 100 and relied on the larger gRPC lease batch must now set
  `max_lease_batch` explicitly. HTTP behaviour is unchanged.
- **`Retry-After` is honoured**, so messages can sit in the queue much longer
  before dead-lettering: `retry max` counts attempts, not elapsed time.
- **Egress CIDR allow rules now require every resolved address**, so a hostname
  answering with one in-range and one out-of-range address is refused. Prefer a
  host rule (`allow "*.internal"`) for private-network targets.
- **`dns_rebind_protection` blocks more ranges**, including `100.64.0.0/10`.
  Delivering into CGNAT space now needs an explicit allow rule.
- **A `max_body` smaller than the default now applies to Admin API bodies too.**
- **Rate-limit buckets survive a reload**, so a deployment that was unknowingly
  benefiting from the refill will see `429`s at its configured rate.
- **Postgres batch metrics** record `enqueue_batch` / `ack_batch` / `nack_batch`
  / `mark_dead_batch` instead of one per-item operation. Dashboards counting
  acks through the store-operation metric need the new names.
- **A reload can now wait up to 60s** (was 15s) before replacing the dispatcher,
  on configs with long target timeouts.

### Security

- **The Stripe/Cituro rejection log no longer carries anything derived from the secret** ([GHSA-cpfq-rj4r-hh5c](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-cpfq-rj4r-hh5c)). On the `no_secret_matched` path the code recomputed `HMAC-SHA256(secrets[0], "<ts>.<body>")` and logged its first 8 hex characters as `want_prefix_first_secret`. Both halves of that message are attacker-controlled, so every rejected request handed out 32 verified bits of the correct MAC for a message of the caller's choosing — a chosen-message oracle that materially assists offline brute force of a weak secret, reachable unauthenticated and written into the runtime log sink. The field is removed entirely; a mismatch remains diagnosable from `secrets_tried`, `body_len` and `got_prefix`, none of which is secret-derived. Two related echoes are closed in the same pass: the `ts_parse_error` branch logged the unbounded attacker-supplied timestamp, and — less obviously — its `err.Error()`, since `strconv.ParseInt` returns a `*strconv.NumError` whose message embeds the entire input. Both are replaced by a length and an error classification (`not_a_number` / `out_of_range`), with no header material logged at all — matching the `parse_incomplete` path, which already reported `pairs_parsed` and `header_value_len` instead of the header value. Also resolves CodeQL alerts #66 and #68, both `go/clear-text-logging` on this line.

- **A config reload can no longer drop route authentication for an in-flight request** ([GHSA-rcpp-44rf-jxvj](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-rcpp-44rf-jxvj)). A single ingress request read the runtime state through eight independent lock acquisitions — first resolving the route, then looking up limits, basic auth, forward auth, HMAC auth and targets by route name. `reloadConfig` swaps the auth maps and the route table separately, so a request that resolved its route against the old table could miss in the new auth maps; a miss yields nil, and ingress reads nil as "no auth configured". Renaming or removing a route while traffic was flowing could therefore serve an unauthenticated request. The window normally closes in under a millisecond but spans the full request-body read, and reloads are routine — `--watch` triggers them, as does every applied Admin API managed-endpoint mutation. Route, authenticators, limits and targets are now captured in one `RouteSnapshot` under a single read lock. Rate limiting and adaptive backpressure deliberately remain live lookups, since they decide on current load rather than on the configuration the request was resolved against.

- **MCP admin proxy: TLS is verified off loopback, and an undeterminable queue backend no longer falls back to the local database** ([GHSA-r796-6hmg-f838](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-r796-6hmg-f838)). Two separate weaknesses. First, all three admin HTTP clients set `InsecureSkipVerify: true` whenever `admin_api.tls` was enabled, each commented as a local channel — but locality was never enforced, so with a non-loopback admin listener anyone able to answer for that address received the admin bearer token on the first call, and `admin_health` is a **read**-role tool. Verification is now skipped only for genuinely loopback endpoints, where a self-signed certificate is the normal setup; everything else is verified. The clients also no longer honour `HTTP(S)_PROXY`, which would otherwise hand the same token to a proxy. Second, `queueToolsUseAdminProxy` answered "false" for any config it could not load or compile — and false does not mean "unknown", it means "sqlite, open `--db` directly". A Postgres or memory deployment with a briefly unreadable config therefore lost the Admin API's authorization and policy enforcement and mutated whatever stale database file sat beside it, reporting success. It now returns an error and the eleven queue tools refuse. **Operator note: a deployment using a self-signed certificate on a non-loopback `admin_api` listener will start failing MCP admin calls until that certificate is trusted — that failure is the point.**

- **`dns_rebind_protection` now checks the address actually connected to** ([GHSA-g55x-rjfh-gxvm](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-g55x-rjfh-gxvm)). `checkEgressPolicyURL` resolved the delivery host, validated every returned address — and then discarded them, handing the *hostname* to the transport, which resolved it again independently. The checked address was never the connected address, so the setting did not stop the attack it is named for: a target answering with a short-TTL record could pass the check on a public address and be connected on `169.254.169.254`. Delivery now runs through a transport whose dialer re-validates the concrete peer address at connect time, which closes the window rather than narrowing it. Deny CIDR rules are enforced there too; allow rules stay at the URL level, since a hostname allow rule cannot be decided from an address alone. **Known limitation:** the transport still honours `HTTP(S)_PROXY`, and with a proxy configured the connection is made to the proxy — so the address checks apply to it and not to the delivery target. Deployments relying on `dns_rebind_protection` should keep proxy variables out of the process environment.

- **MCP `config_diff` no longer expands placeholders from caller-supplied content** ([GHSA-9r4r-gfq7-995c](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-9r4r-gfq7-995c)). `{file.*}` performed an unrestricted `os.ReadFile` and `{env.*}` / `{$...}` an `os.LookupEnv` at compile time, and the resolved value was interpolated verbatim into validation errors. `config_diff` is a **read**-role tool that accepts free-form `content`, so a read-only MCP session could pass `"/x{file./etc/hookaido/secrets/gh.key}"` and receive the file's contents back in the error message — bypassing the config-path allowlist entirely, since the content never goes through it. `{file./dev/zero}` was additionally an unbounded read. A new `config.CompileUntrusted` skips expansion for content that did not come from the operator's own config file, warning about each placeholder it left alone; `config_diff` uses it. Trusted compilation of the operator's own Hookaidofile is unchanged, and validation of a placeholder-using candidate is correspondingly less precise — that is the intended trade.

- **The ingress HMAC nonce is claimed only after the signature verifies** ([GHSA-cmwq-5829-xw24](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-cmwq-5829-xw24)). The nonce was inserted into the replay cache *before* the signature comparison, so any unauthenticated caller could write to it. Two consequences: the cache grew without bound from rejected requests — with no entry cap, no nonce-length limit, and a full-map sweep on every call that made each request O(entries) under the lock — and anyone able to observe or predict a nonce could claim it first, causing the genuine signed webhook carrying it to be rejected as a replay. Claiming now happens only on the success path, which removes the unauthenticated write entirely. Additionally: nonces longer than 256 bytes are rejected rather than remembered, the cache is capped at 100k entries and evicts the entry closest to expiring rather than refusing a validly signed request, and the sweep runs at most every 30s instead of on every call.

- **An `auth hmac { }` block that declares no secret is now a compile error** ([GHSA-5v3w-hjh4-4q9q](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-5v3w-hjh4-4q9q)). `Route.AuthHMACBlockSet` records that a block was written, but `Compile` never read it — only the formatter did. The missing-secret guard covered `signature_header`, `timestamp_header`, `nonce_header`, `tolerance` and `provider`, so a block containing none of those and no secret compiled clean. At runtime the route then mapped to a nil authenticator, which ingress reads as "no auth configured": the route accepted every unsigned request. The adjacent case was already handled — `auth hmac { provider github }` without a secret errored — so only the fully empty block slipped through, reachable by the plausible operator action of commenting out a `secret` line during debugging or rotation. The mutual-exclusion checks against `auth basic` and `auth forward` now also treat an empty block as a declared HMAC surface. **Operator action: audit Hookaidofiles for `auth hmac` blocks with no `secret` or `secret_ref`; any such route has been serving unauthenticated.**

- **`auth basic` now rejects secret-reference syntax instead of using it as the credential** ([GHSA-q2r7-wm3j-xppw](https://github.com/nuetzliches/hookaido/security/advisories/GHSA-q2r7-wm3j-xppw)). Unlike `auth token`, `auth hmac` and `secret` blocks, basic-auth credentials are never passed through `secrets.LoadRef` — the value is compared literally. A route configured as `auth basic "user" "env:WEBHOOK_PASSWORD"` therefore accepted the string `env:WEBHOOK_PASSWORD` as the password, with no error and no warning. That exact example was published in `docs/ingress.md` and `docs/security.md`, so any deployment that copied it was protected by a password printed in this project's own documentation. The compiler now rejects a basic-auth user or password that parses as an `env:` / `file:` / `vault:` / `raw:` reference, and points at the `{env.NAME}` placeholder form, which does resolve. **Operator action: rotate any basic-auth credential that was configured with reference syntax, and treat it as disclosed.**

- **`admin_api` without an `auth token` is now rejected off loopback** ([#211](https://github.com/nuetzliches/hookaido/issues/211)). `Compile` required an auth-token allowlist for `pull_api` whenever pull routes existed, but imposed no equivalent check on `admin_api` — and with an empty token list `admin.BearerTokenAuthorizer` short-circuits to "authorized" for every request. The default listen is `127.0.0.1:2019`, so the out-of-the-box posture was safe, but overriding it is the ordinary Docker and Kubernetes change: `admin_api { listen :2019 }` silently produced a fully open control plane, including DLQ delete, `messages/publish`, `cancel_by_filter` and management-endpoint mutations that rewrite the Hookaidofile and trigger a reload. `docs/docker.md` recommended exactly that line, without a token, and has been corrected. Compilation now fails when the token list is empty and the listen address is not loopback-only — wildcard forms (`:2019`, `0.0.0.0:2019`, `[::]:2019`) count as non-loopback, as does a hostname other than `localhost`, which cannot be resolved at compile time — or when `admin_api` co-listens with `ingress`, where loopback alone is not sufficient reassurance. **Operator action: a deployment that exposed `admin_api` beyond loopback without a token will now fail to start; add `auth token` and treat the endpoint as having been open.**

- **Routes that an earlier route provably shadows are now rejected** ([#211](https://github.com/nuetzliches/hookaido/issues/211)). Collision detection compared route paths as exact strings, while runtime matching is segment-boundary prefix matching with first-match-wins. A route nested under an earlier one therefore compiled clean and was unreachable: `"/hooks"` followed by `"/hooks/github"` meant every `POST /hooks/github` was answered by the first route — with *its* auth and *its* target — so an authenticated route could sit dead in the config behind an unauthenticated parent, and a leading `"/"` route swallowed everything. The compiler already guarded the analogous ingress-path-vs-API-prefix case on shared listeners, so the omission was inconsistent rather than deliberate. An earlier route that carries `match` criteria can legitimately act as a filter and let the rest fall through, so only an unconstrained earlier route shadows; ordering the specific path first is unaffected. **Operator action: a config with a shadowed route will now fail to start — reorder so the more specific path comes first, and check whether the shadowing route has been receiving that traffic.**
- **Egress CIDR allow rules now require every resolved address, not any one of them** ([#206](https://github.com/nuetzliches/hookaido/issues/206)). `matchEgressRules` returned true as soon as one of a hostname's addresses fell inside a rule's CIDR. That is correct and fail-closed for a denylist, but fail-open for an allowlist: the dialer picks freely among the addresses, so a host answering with one in-range address passed the allowlist and could then be connected on any of the others. Under `allow "10.0.0.0/8"` — the workaround `docs/delivery.md` recommends for private-network delivery targets — a host resolving to `10.1.1.5` and `169.254.169.254` was permitted. Allow CIDR rules now require every resolved address to be covered by some allow CIDR rule; hostname rules keep any-match semantics, since a request carries exactly one host. This matches the rebind check, which has always required all addresses to pass.

- **`dns_rebind_protection` covers the non-routable ranges `net.IP.IsPrivate` misses** ([#206](https://github.com/nuetzliches/hookaido/issues/206)). The address check combined loopback/link-local/multicast/unspecified with `IsPrivate`, which is strictly RFC1918 plus `fc00::/7`. That left `100.64.0.0/10`, `192.0.0.0/24`, `198.18.0.0/15`, `240.0.0.0/4`, `0.0.0.0/8`, `::/96` and `64:ff9b::/96` reachable — three of them cloud metadata surfaces in their own right (`100.100.100.200` on Alibaba, `192.0.0.192` on Oracle) or a route to the well-known `169.254.169.254` by translation (`64:ff9b::a9fe:a9fe` behind NAT64, `::a9fe:a9fe` as the deprecated IPv4-compatible form). The check now runs against an explicit prefix table naming every blocked range. IPv4-mapped forms were already handled correctly and still are. The RFC5737 documentation ranges are deliberately not blocked: they are never routed, but they are not a metadata surface either, and they serve as public stand-ins throughout this repo's tests.
- **Every HTTP listener now sets `ReadHeaderTimeout` and `IdleTimeout`** ([#203](https://github.com/nuetzliches/hookaido/issues/203)). Both `http.Server` constructions — the shared/per-component ingress, pull_api and admin_api servers, and the metrics server — were built without any timeout, so a client dribbling one header byte at a time pinned a goroutine and a file descriptor indefinitely (Slowloris). Ingress is the component most likely to face the open internet, and nothing else bounded header read time. Servers are now built through a single `newHTTPServer` helper with a 15s header-read and 120s idle timeout, and a test guards that no listener is constructed around it. `ReadTimeout` and `WriteTimeout` remain deliberately unset — a write deadline would truncate the Pull API's SSE stream, and a read deadline would cap the time available to receive a legitimate `max_body`-sized payload over a slow link.

### Added

- **`EnqueueBatch` on the Postgres backend** ([#207](https://github.com/nuetzliches/hookaido/issues/207)). Postgres did not implement `queue.BatchEnqueuer`, which memory and SQLite both do, so `internal/admin` fell back to a per-item loop — the batch-publish atomicity guarantee silently degraded to best-effort on the recommended backend. A 50-item publish failing at item 30 left items 0–29 committed and visible while returning an error, so the client retried the whole batch and those 30 were delivered twice. The whole batch now commits in one transaction with a single depth check, and a duplicate id maps to `queue.ErrEnvelopeExists` as it does elsewhere. A compile-time interface assertion keeps the implementation from silently going missing again.

- **`pull_api.max_lease_batch`** ([#210](https://github.com/nuetzliches/hookaido/issues/210)), default `100`, bounding how many lease IDs one ack/nack/extend call may carry. It replaces a conflation: the gRPC transport's lease cap was fed from `pull_api.max_batch` — the *dequeue* cap — while the HTTP pull server's was never assigned at all and kept `NewServer`'s own default of 100. With `max_batch 1000`, a gRPC `Ack` accepted 1000 lease IDs while `POST /pull/<route>/ack` rejected anything over 100. Both transports now read the new setting, so they agree. **Operator note: a deployment that raised `max_batch` above 100 and relied on the larger gRPC lease batch must now set `max_lease_batch` explicitly; the HTTP behaviour is unchanged.**

### Fixed

- **Postgres lease batches are atomic** ([#207](https://github.com/nuetzliches/hookaido/issues/207)). `AckBatch`, `NackBatch` and `MarkDeadBatch` looped over their single-lease counterparts, so every lease got its own `BeginTx`/`Commit` — contradicting the `queue.LeaseBatchStore` contract, which asks implementations to settle the whole batch in one store transaction, and which SQLite honours via `withLeaseBatch`. All three now share a Postgres `withLeaseBatch`: one transaction, one `SELECT … FOR UPDATE` over the lease IDs, and one bulk statement over the item IDs. An unexpected error mid-batch therefore settles nothing at all and leaves every lease in place for the worker to retry, rather than committing a prefix of the batch. Conflict semantics are unchanged and now match SQLite exactly — a blank or unknown lease ID and every repeat of an ID already named in the same batch are conflicts, and an expired lease is requeued inside the same transaction and reported as an expired conflict. **Metrics note:** batch settles now record one `ack_batch` / `nack_batch` / `mark_dead_batch` store operation instead of one `ack` / `nack` / `mark_dead` per lease, matching the `enqueue_batch` name introduced with `EnqueueBatch`.

- **`max_depth` is enforced atomically on Postgres** ([#207](https://github.com/nuetzliches/hookaido/issues/207)). The count, the optional `drop_oldest` evictions and the insert were three independent autocommit statements with no transaction and no lock, so concurrent enqueues at the limit each observed room and all inserted — measured at 16 racing enqueues against a single free slot all being admitted, taking a `max_depth` of 40 to 55. With `drop_oldest` the mirror image over-dropped, evicting messages to make room for an enqueue that was then rejected. SQLite performs the same sequence under `BEGIN IMMEDIATE` and memory holds its store mutex throughout, so neither was affected. The three steps now run in one transaction, which is what keeps evictions from outliving a rejected enqueue, and the depth check is serialized by a transaction-scoped advisory lock, which is what a transaction alone cannot do — under READ COMMITTED every concurrent enqueue still reads the same pre-insert depth. The lock is skipped while the queue is far enough from the limit that no interleaving can cross it, because taking it unconditionally cost roughly fourfold enqueue throughput in measurement and enqueue is the ingress hot path.

- **A failed reload no longer leaves part of the new config in force** ([#209](https://github.com/nuetzliches/hookaido/issues/209)). `loadAuth` called `Replace` on static secret pools and `Unregister` for dropped pools *before* the per-route token and HMAC loops, which can still fail. `secrets.Pool.Replace` swaps the version slice under the pool's own lock and route HMAC closures hold `*secrets.Pool` pointers, so the swap was live immediately — and when a later step then failed, `reloadConfig` logged `config_reload_failed` and reported that nothing had changed. An edit that both rotated an existing secret and added a route whose key file was not yet deployed therefore applied the rotation, failed on the new route, and reported failure, while inbound webhooks were already being verified against a secret the senders had not adopted: every request answered 401 with nothing in the log to explain it. `loadAuth` is now strictly two-phase — every secret ref is resolved and validated into locals, and only then are pools registered, replaced or unregistered and the authorizer maps swapped. The apply phase performs nothing that plan-phase validation has not already shown can succeed.

- **The dispatcher drain budget follows the configuration, and the log no longer lies** ([#209](https://github.com/nuetzliches/hookaido/issues/209)). On a delivery-config reload the drain was given a hardcoded 15s, and when that elapsed the code logged a warning and then `dispatcher_stopped_for_reload` regardless — a line asserting something that had not happened. The budget is now derived from the dispatcher's own configuration (the longer of its dequeue long-poll and the longest target `timeout`, plus headroom, clamped to 15s–60s), because that is how long a worker can legitimately need to notice the stop signal: 15s was too short for a route with `timeout 60s`, where an in-flight delivery looked like a stuck worker, and longer than necessary for short timeouts. If the budget is exceeded, `dispatcher_drain_incomplete` is logged with the budget and the actual consequence, and the replacement still starts — `Drain` closes `stopCh` through a `sync.Once`, so the old dispatcher is terminal either way and refusing the swap would leave no dispatcher at all. The old workers stop dequeuing the moment the signal is set and each finishes at most the delivery already in flight, so the overlap is bounded to that plus per-route concurrency briefly exceeding its configured value. The shutdown drain uses the same budget.

- **`Retry-After` is honoured on a retryable delivery response** ([#206](https://github.com/nuetzliches/hookaido/issues/206)). `shouldRetry` has always retried 429 and every 5xx, but `Result` carried only a status code and an error — response headers were discarded where the body is drained, and a repo-wide search for `Retry-After` found nothing. A target answering `429 Retry-After: 3600` therefore received all eight attempts within roughly six minutes on the default schedule and was then dead-lettered, when waiting would have succeeded. Both RFC 7231 forms are now parsed (delta-seconds and HTTP-date) and surfaced on `Result`, and the nack delay becomes `max(scheduled backoff, Retry-After)` clamped to one hour. The hint can only extend the wait, never shorten it, so a target asking to be retried sooner cannot defeat the backoff; an absent, unparseable, non-positive or already-past value leaves the schedule untouched. The honoured value is logged as `retry_after` on `delivery_retry`. Note that `retry max` counts attempts rather than elapsed time, so a target asking for an hour will hold a message for hours before dead-lettering.

- **Outbound signing secrets are no longer cached for the life of the process** ([#206](https://github.com/nuetzliches/hookaido/issues/206)). `loadSigningSecret` memoized each ref in a `sync.Map` with no TTL and no invalidation, and `secrets.LoadRef` does not cache, so the dispatcher was the only source of staleness. The cache was discarded only when the whole `HTTPDeliverer` was rebuilt, which happens only on a compiled-config change — so rotating a `vault:` or `file:` backed signing secret without editing the Hookaidofile had no effect, SIGHUP included, and **revoking a leaked signing key required a full process restart**. `file:` and `vault:` refs are now re-read once their cached value is 60 seconds old; `env:` and `raw:` stay cached for the life of the deliverer, since their value cannot change while the process runs and re-reading could not observe a rotation. The 60-second window is the maximum time a revoked key can still sign.
- **Rate-limit buckets survive a config reload** ([#209](https://github.com/nuetzliches/hookaido/issues/209)). `updateAll` called `configureIngressRateLimits` unconditionally, which built fresh `tokenBucketLimiter`s seeded with `tokens = burst`, and nothing compared the old rate-limit config against the new one. Every successful reload therefore refilled every bucket — including reloads an operator would not think of as reloads: `mutateManagedEndpointConfig` calls `reloadConfig` on each applied Admin API managed-endpoint upsert or delete, and `--watch` triggers one per config write. Repeated at reload frequency the effective rate limit was unbounded, which defeats the control entirely. A limiter whose `rps` and `burst` are unchanged is now carried over with its current token balance; only a changed limit or a new route produces a fresh bucket, a removed route's bucket is dropped, and disabling the global limit still clears it.

- **A SIGHUP arriving during startup can no longer leave push delivery dead** ([#209](https://github.com/nuetzliches/hookaido/issues/209)). The SIGHUP goroutine was started before `store` and `currentPush` were assigned, and both were written by the main goroutine without holding `reloadMu` while `reloadNow` reads them under it — no happens-before edge. A supervisor that writes the config and signals in one step could land inside that window, and `reloadNow` would then hand a nil `queue.Store` to a fresh dispatcher, whose `Start()` silently no-ops on `d.Store == nil`: push delivery dead until restart, with nothing logged. The queue store is now opened before the signal handler is registered, and the initial `currentPush` assignment takes `reloadMu`.

- **The runtime sealer is installed under the lock that guards its readers** ([#209](https://github.com/nuetzliches/hookaido/issues/209)). `loadAuth` wrote `s.sealer` outside `s.mu` while `hydrateRuntimeSecrets` and `sealSecretValue` read it under `RLock`. `loadAuth` runs on every reload — including each applied Admin API managed-endpoint mutation — with the Admin API already serving, so the write raced every secret-persisting request. Both the presence check and the write now take the lock, with a re-check before assigning.

- **The shutdown drain is registered unconditionally** ([#209](https://github.com/nuetzliches/hookaido/issues/209)). The drain `defer` sat inside `if compiled.HasDeliverRoutes`, which is evaluated against the *initial* config, so a dispatcher created by a later reload would have had none. In practice that case is unreachable: `requiresRestartForReload` refuses any reload where `HasDeliverRoutes` changes, so a reload cannot create the process's first dispatcher — verified, and now pinned by a test. The registration is hoisted out anyway, so the drain no longer depends on an invariant enforced two thousand lines away, and it stays registered after the `closeStore` defer so LIFO ordering keeps draining before the store closes. No behaviour change today; the coupling is what is removed.

- **The admin body cap follows `defaults.max_body`** ([#210](https://github.com/nuetzliches/hookaido/issues/210)). `decodeJSONBodyStrict` hardcoded 2 MiB, and every admin JSON body goes through it — while `parseSecretUpsertBody` and the publish payload check *did* honour the configured value, so the same knob applied inconsistently within one package and `docs/configuration.md` stated it applied to admin servers. With `max_body 64kb` the admin API still buffered 2 MiB before rejecting; with `max_body 10mb` a legitimate 3 MiB publish was refused citing a limit that appeared nowhere in the config. The configured value is now used, with the constant as the zero-value fallback, and the error names the limit actually in force. One consequence worth knowing: with no per-route override, an oversized publish is now caught by the request-body cap rather than the per-item payload check, since a payload larger than `max_body` cannot arrive in a request smaller than it. The per-route payload override still applies, and can be smaller than the body cap.

- **Runtime secret upsert rejects unknown fields** ([#210](https://github.com/nuetzliches/hookaido/issues/210)). `parseSecretUpsertBody` used a plain `json.Unmarshal`, so unknown fields were silently ignored and trailing documents accepted, while every other admin body went through `decodeJSONBodyStrict` with `DisallowUnknownFields`. `{"value":"…","notAfter":"…"}` — a camelCase typo for `not_after` — returned 201 with `not_after` zero: the secret was created with **no expiry** instead of the intended time-boxed window, and both the response and the audit event reported the empty window, so the mistake was easy to miss. It now goes through the same strict decoder as everything else.

- **A catch-all `"/"` route no longer compiles clean against a co-listening API prefix** ([#211](https://github.com/nuetzliches/hookaido/issues/211)). The single-port collision guard tested `hasPathPrefix` in both directions, and for the root route both are false — the reverse direction evaluates `HasPrefix("/pull", "//")`. But `normalizePathValue` accepts `"/"` as a route path and `matchPath` returns true for *every* request path when the route path is `"/"`. With `ingress` and `pull_api` sharing a port and a `"/"` route present, compilation succeeded while `prefixMux` sent `/pull/anything` to the pull API, which answered 401 — webhooks dropped with a misleading status, and the catch-all route receiving nothing. The root route is now treated as colliding with every non-empty co-listening prefix.

- **Listen addresses are compared as bind targets, not as strings** ([#211](https://github.com/nuetzliches/hookaido/issues/211)). `validateSharedListeners` keyed its groups on the raw `listen` string, and the `grpc_listen` / `metrics.listen` conflict checks used `==` on raw strings, so `:8080` and `0.0.0.0:8080` were three-quarters of the way to being the same socket and none of the way to being the same key. `ingress { listen :8080 }` with `admin_api { listen 0.0.0.0:8080 }` validated `{"ok": true}` — skipping the prefix, collision and TLS-parity checks that a shared address requires — and then failed at startup with `EADDRINUSE`, from a config `config validate` and CI had both declared good. Aliased spellings across two components are now a compile error naming both, and the dedicated-listener conflict checks compare canonical bind targets. A wildcard is deliberately *not* equated with a specific address (`:8080` vs `127.0.0.1:8080`): those conflict on Linux but can both be bound on Windows. Hostnames are not resolved either, since a false collision would reject a working config.

- **`config fmt` no longer corrupts a quoted `env` key** ([#211](https://github.com/nuetzliches/hookaido/issues/211)). The parser discarded the quoted flag on a `deliver exec { env … }` key and the formatter wrote it back with a bare `%s`, so `env "MY KEY" "value"` was reformatted to `env MY KEY "value"` — with exit 0, and output that no longer parses. A user redirecting `config fmt` over their own file lost the config. `Compile` rejects such a key, so only an already-invalid config could be hit, but `config fmt` and the read-role MCP `config_fmt_preview` are parse-only and never reach that check. `ExecEnvVar` now records `KeyQuoted` and the key goes through `formatValue` like every other value. `FuzzParseFormatRoundTrip` was always strong enough to catch this — its corpus simply never contained a quoted value needing re-quoting — so a seed covering that now does.

- **An overflowing byte size is rejected instead of wrapping to a small limit** ([#211](https://github.com/nuetzliches/hookaido/issues/211)). `parseByteSize` multiplied before checking, and the `size <= 0` guard caught a wrap landing negative or on zero but not one landing on a small positive. `defaults { max_body 18014398509481985k }` validated clean and yielded `MaxBodyBytes = 1024`, so the config read as an enormous limit while every webhook over 1 KiB was rejected with a 413 — the more surprising because the neighbouring `9007199254740993k` wraps negative and was correctly rejected. The multiplication is now range-checked beforehand and sizes are capped at `1024gb`, well above anything Hookaido legitimately buffers.
- **`publish_policy.fail_closed` now covers a config that does not parse** ([#208](https://github.com/nuetzliches/hookaido/issues/208)). The MCP helper behind scoped-managed mutation checks answered with an `(enabled, known)` pair and reported "could not determine the policy" identically to "determined it to be off" — which both callers read as permission to proceed. A *compile* failure therefore failed closed as intended while a *parse* failure did not: with `fail_closed on` and a syntax error in the Hookaidofile, an `operate`-role session could run `messages_cancel` against a managed route with an actor that is not on the allowlist. The two are now separated — an absent or explicitly-off policy permits the mutation, while a config that cannot be read or parsed, or whose `fail_closed` value is not a boolean, refuses it. Those are exactly the cases where the operator's intent is unreadable, which is what `fail_closed` exists to decide.

- **`config_apply` requires an audit reason** ([#208](https://github.com/nuetzliches/hookaido/issues/208)). It was the only mutating MCP tool that took none — `management_endpoint_upsert`/`delete`, every queue mutation and `rotate_secret` all require one — while being the highest-blast-radius call in the surface: under `--watch` (the `instance_start` default) the write alone triggers a live reload. The audit line recorded `path`, `mode` and an `input_hash`, from which a reviewer could recover neither what was applied nor why. It now takes the same `reason` (required) plus optional `actor` / `request_id` as the others, echoes them under `audit` on the response, and records them in `metadata.config_mutation` together with a `content_sha256` digest and byte count of the applied config — `input_hash` covers the whole argument object, so it changes with the reason or the mode and cannot identify the config text. `management_endpoint_upsert`/`delete` apply through this tool internally and now pass their own audit triple down rather than being asked for a second reason. **Caller action: an MCP client calling `config_apply` must add a `reason` argument; the call is rejected without one.**

- **A delivery response body is drained with a ceiling** ([#206](https://github.com/nuetzliches/hookaido/issues/206)). After each attempt the response body was discarded through an unbounded `io.Copy`, read only so keep-alive could reuse the connection. Ingress caps inbound bodies at 2 MiB, but nothing bounded what a delivery target streamed back, so a target answering 2xx and then writing at line rate held a delivery goroutine for the entire per-attempt timeout (10s by default) — and the message was still acked, because the status was 2xx. At the default `deliver_concurrency` of 20 that is the whole route's delivery capacity parked on one uncooperative target. The drain now stops at 64 KiB and drops the connection instead of reusing it.

- **A single MCP frame can no longer ask for an unbounded allocation** ([#208](https://github.com/nuetzliches/hookaido/issues/208)). `readFrame` sized the payload buffer straight from `Content-Length`, checking only that it was not negative, so one header line could make the process reserve an arbitrary amount of memory before any content had been read -- a corrupt or hostile frame could take the MCP server down without sending a payload. Frames are now capped at 8 MiB, which is well above realistic traffic (the largest carry a whole Hookaidofile via `config_apply`, or a batch of base64 payloads via `messages_publish`, against an Admin API body limit of 2 MiB). An oversized declaration is answered with a parse error instead.

- **`instance_status` and `instance_logs_tail` now reject undeclared arguments** ([#208](https://github.com/nuetzliches/hookaido/issues/208)). They were the only two runtime-control tools without an argument allowlist, even though both declare `"additionalProperties": false` in their input schemas -- so the server was not enforcing the contract it advertises. An undeclared key was silently ignored, which meant a misspelled `max_lines` quietly returned the 200-line default rather than reporting the mistake. Both now validate against the keys their schema declares, matching the other three runtime-control tools.

- **A busy SSE stream ignored cancellation** ([#210](https://github.com/nuetzliches/hookaido/issues/210)). The Pull API's SSE loop handled `ctx.Done()` only in the select it reaches when no items are available, and the item-writing path ends in `continue` -- so a stream with messages always ready never checked it. A client that disconnected mid-stream kept having messages dequeued and leased on its behalf that nobody would ever ack, the handler goroutine never exited, and `sse_max_connection` failed to bound exactly the long-lived busy connections it exists for. `hookaido_pull_sse_connection_active` also only ever counted up, because the disconnect observer never ran. Cancellation is now checked once per iteration, and the per-item write error -- previously discarded -- is treated like the keepalive writes already were.

- **`bearer` in lowercase was accepted over gRPC and rejected over HTTP** ([#210](https://github.com/nuetzliches/hookaido/issues/210)). RFC 7235 defines the auth-scheme token as case-insensitive, and the worker gRPC API parsed it that way; the Pull and Admin HTTP APIs required exactly `Bearer ` on an untrimmed header. The same client credential therefore authenticated on one transport and returned 401 on the other. All three now share `httpheader.ParseBearerToken`, which trims surrounding whitespace and compares the scheme case-insensitively. Tokens that already worked are unaffected.

- **A failed secret delete left the secret revoked from the running process** ([#210](https://github.com/nuetzliches/hookaido/issues/210)). `DELETE /secrets/{pool}/{id}` removed the version from the live pool before persisting the deletion, so a persistence failure returned `500` -- which the caller reads as "nothing was deleted" -- while the secret was already gone from memory. Signature verification for senders still using it began failing immediately, and the secret reappeared on the next restart because the record was never removed. The deletion is now persisted first, mirroring the add path, which persists before mutating the pool and rolls back if the pool rejects the value.

- **A partly-applied lease batch on Postgres no longer reports that nothing succeeded** ([#207](https://github.com/nuetzliches/hookaido/issues/207)). `AckBatch`, `NackBatch` and `MarkDeadBatch` settle one lease per transaction on this backend, so when an unexpected error surfaced on lease N, leases 1..N-1 were already committed — but all three returned an empty `queue.LeaseBatchResult` with the error, telling the caller nothing had been settled. The dispatcher treats an un-acked delivered item as still leased, so those already-acked messages were dequeued and delivered a second time once the lease TTL expired (70s with defaults), and items already classified dead by `MarkDeadBatch` were redelivered instead of staying dead-lettered. SQLite was never affected: it settles the whole batch in one transaction via `withLeaseBatch`, so an empty result there is accurate. The accumulated result was returned alongside the error, which made the report honest. The Postgres batch is now atomic as well (see below), so there is no longer a partial result to report.

- **Delivered-item retention on Postgres measured the wrong interval** ([#207](https://github.com/nuetzliches/hookaido/issues/207)). Pruning keyed on `received_at`, but `Ack` stamps `next_run_at` with the delivery time — which is why the memory and SQLite backends both age delivered items from `next_run_at`. An item that had sat queued for longer than `delivered_retention_max_age` was therefore eligible for deletion the moment it was delivered, so a slow route lost its delivery history immediately while a fast route kept the configured window. The queued and dead predicates additionally used `<` where the other backends use `<=`, and now match.

- **Postgres `Stats` reported no age or ready-lag for the top backlog buckets** ([#207](https://github.com/nuetzliches/hookaido/issues/207)). The per-bucket query selected only route, target and count, leaving `OldestQueuedAge` and `ReadyLag` at zero — so a route stalled for hours showed an age of `0s`, which is the number an operator reads to spot a stuck target. The process-wide aggregates were already correct; only the per-bucket values were missing. The query now derives them from `MIN(received_at)` and `MIN(next_run_at)` exactly as the SQLite backend does.

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

[Unreleased]: https://github.com/nuetzliches/hookaido/compare/v2.11.0...HEAD
[2.11.0]: https://github.com/nuetzliches/hookaido/compare/v2.10.1...v2.11.0
[2.10.1]: https://github.com/nuetzliches/hookaido/compare/v2.10.0...v2.10.1
[2.10.0]: https://github.com/nuetzliches/hookaido/compare/v2.9.0...v2.10.0
[2.9.0]: https://github.com/nuetzliches/hookaido/compare/v2.8.1...v2.9.0
[2.8.1]: https://github.com/nuetzliches/hookaido/compare/v2.8.0...v2.8.1
[2.8.0]: https://github.com/nuetzliches/hookaido/compare/v2.7.3...v2.8.0
[2.7.1]: https://github.com/nuetzliches/hookaido/compare/v2.7.0...v2.7.1
[2.7.0]: https://github.com/nuetzliches/hookaido/compare/v2.6.0...v2.7.0
[2.6.0]: https://github.com/nuetzliches/hookaido/compare/v2.5.3...v2.6.0
[2.4.0]: https://github.com/nuetzliches/hookaido/compare/v2.2.2...v2.4.0
[2.2.2]: https://github.com/nuetzliches/hookaido/compare/v2.2.1...v2.2.2
[2.2.1]: https://github.com/nuetzliches/hookaido/compare/v2.2.0...v2.2.1
[2.2.0]: https://github.com/nuetzliches/hookaido/compare/v2.1.0...v2.2.0
[2.1.0]: https://github.com/nuetzliches/hookaido/compare/v2.0.1...v2.1.0
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
