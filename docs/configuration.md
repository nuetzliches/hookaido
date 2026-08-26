# Configuration Reference

Hookaido uses a Caddyfile-inspired DSL. The config file (typically `Hookaidofile`) is the source of truth for all routing, authentication, queue behavior, and observability settings.

## File Structure

A Hookaidofile consists of **global blocks** and **route blocks**, optionally organized by channel type:

```hcl
# Global blocks
ingress { ... }
pull_api { ... }
admin_api { ... }
queue_limits { ... }
queue_retention { ... }
defaults { ... }
secrets { ... }
observability { ... }
vars { ... }

# Inbound routes (implicit — bare top-level routes receive ingress traffic)
/webhooks/github {
  auth hmac env:HOOKAIDO_SECRET
  pull { path /pull/github }
}

# Explicit inbound wrapper (optional, equivalent to bare routes)
inbound {
  /webhooks/stripe { ... }
}

# Outbound routes (API → queue → push, no ingress traffic)
outbound /jobs/deploy {
  deliver "https://ci.internal/deploy" { timeout 10s }
}

# Internal routes (job queues, no ingress traffic)
internal {
  /jobs/report {
    pull { path /pull/reports }
  }
}
```

Route paths can be quoted or unquoted but must start with `/`.

## Values, Quoting and Comments

Values are bare words or double-quoted strings. Quote a value when it contains
whitespace, `#`, `{`, `}`, or `"`.

Inside a quoted string, `\\`, `\"`, `\n`, `\t` and `\r` are escape sequences.
Any other backslash is kept verbatim, so `"^/hooks/\d+$"` is the regex you wrote
and `"C:\certs\server.pem"` is the path you wrote.

The escapes that do exist still apply, so a Windows path whose next character is
`n`, `t`, `r`, `"` or `\` needs the backslash doubled — `"C:\new\tools"` contains
a newline and a tab. Writing `"C:\\new\\tools"` (or using forward slashes, which
Windows accepts) avoids the question entirely. `config fmt` re-escapes
backslashes when it writes a value back, so a value survives any number of
format cycles unchanged.

`#` starts a comment that runs to the end of the line. Comments are preserved by
Admin API config rewrites, which splice only the directive they change.
`config fmt` regenerates the file from the parsed config and keeps only the
comments above the first statement — run it on a file whose comments you intend
to keep, and review the diff.

## Channel Types

Routes are organized into three channel types that control which directives are allowed:

| Channel             | Ingress traffic | `auth`/`match`/`rate_limit` |    `pull`     |   `deliver`   | `publish` | `queue { backend }` | Labels  |
| ------------------- | :-------------: | :-------------------------: | :-----------: | :-----------: | :-------: | :-----------------: | :-----: |
| `inbound` (default) |       yes       |           allowed           |    allowed    |    allowed    |  allowed  |       allowed       | allowed |
| `outbound`          |     **no**      |        **forbidden**        | **forbidden** | **required**  |  allowed  |       allowed       | allowed |
| `internal`          |     **no**      |        **forbidden**        | **required**  | **forbidden** |  allowed  |       allowed       | allowed |

### `inbound` (default)

Bare top-level routes are implicit `inbound` — they receive ingress HTTP traffic and support the full set of per-route directives. The `inbound { }` wrapper is optional syntactic sugar.

### `outbound`

For API-to-queue-to-push flows where your application publishes events via the Admin API or MCP, and Hookaido delivers them to external targets. No ingress listener serves these routes.

```hcl
outbound /notifications/slack {
  deliver "https://hooks.slack.com/..." { timeout 5s }
}
```

Compile constraints:

- `deliver` is **required** (at least one target).
- `auth`, `match`, `rate_limit`, and `pull` are **forbidden**.

### `internal`

For internal job queues consumed by your workers via the Pull API. No ingress listener serves these routes, and no push delivery is configured.

```hcl
internal /jobs/nightly-report {
  pull { path /pull/nightly }
}
```

Compile constraints:

- `pull` is **required**.
- `auth`, `match`, `rate_limit`, and `deliver` are **forbidden**.

### Wrapper Form

All three channel types support both single-route shorthand and multi-route wrapper form:

```hcl
# Single route
outbound /jobs/deploy {
  deliver "https://ci.internal/deploy" { timeout 10s }
}

# Multiple routes
internal {
  /jobs/report { pull { path /pull/reports } }
  /jobs/cleanup { pull { path /pull/cleanup } }
}
```

## Global Blocks

### `ingress`

Controls the ingress HTTP listener.

```hcl
ingress {
  listen :8080
  rate_limit {
    rps 100
    burst 200     # optional; defaults to ceil(rps)
  }
  tls {
    cert_file /path/to/cert.pem
    key_file  /path/to/key.pem
    client_ca /path/to/ca.pem     # optional, enables mTLS
    client_auth require_and_verify # optional
  }
  trusted_proxies "10.0.0.0/8" "fd00::/8"   # optional; empty by default
}
```

| Directive         | Default | Description                                                   |
| ----------------- | ------- | ------------------------------------------------------------- |
| `listen`          | `:8080` | Bind address                                                  |
| `rate_limit`      | —       | Global ingress rate limit (token-bucket)                       |
| `tls`             | —       | TLS and optional mTLS configuration                           |
| `trusted_proxies` | empty   | Peer prefixes whose `X-Forwarded-For` is believed (see below) |

#### `trusted_proxies`

By default the client address is the **transport peer address** — what the socket
reports — and `X-Forwarded-For` is ignored entirely. That is the safe default:
believing the header unconditionally would let any client name its own source
address.

It is also a trap behind a reverse proxy, which is how Hookaido is most often
deployed. Every request then arrives with the proxy's address, so a
`match remote_ip` allowlist of the source's published egress range matches
nothing and the route answers `404` for legitimate traffic. Widening the range to
the proxy's subnet makes the traffic flow again — and matches **everything** the
proxy forwards, from any origin. The config still reads like an origin
restriction; it no longer is one.

`trusted_proxies` is the opt-in that resolves this. When the peer address is
inside one of the configured prefixes, the **right-most `X-Forwarded-For` entry
that is not itself trusted** becomes the client address for `match remote_ip`.
Walking from the right is what makes it sound: entries further left were appended
by hops you have not vouched for, and a client can write anything it likes there.
A request whose peer is *not* a trusted proxy keeps its peer address and the
header is ignored — so a client talking to Hookaido directly gains nothing by
sending one.

Accepts IPs and CIDRs, IPv4 and IPv6, any number of values. Live-reloadable.
An unparsable value fails `config validate`; a duplicate is dropped with a
warning.

If every hop in the chain is a trusted proxy, or the chain contains an entry that
cannot be parsed, the peer address stands — the walk stops rather than reaching
past a hop it cannot identify.

Rate limiting is unaffected: Hookaido's ingress limiter is keyed per route (and
globally), never per client address, so there is nothing for a forwarded address
to change there.

### `pull_api`

Controls the Pull API listener for consumer workers.

```hcl
pull_api {
  listen :9443
  grpc_listen 127.0.0.1:9943  # optional gRPC worker listener
  prefix /pull        # optional URL prefix
  auth token env:HOOKAIDO_PULL_TOKEN

  max_batch 100              # max items per dequeue (default 100)
  max_lease_batch 100        # max lease IDs per ack/nack/extend (default 100)
  default_lease_ttl 30s      # default lease duration (default 30s)
  max_lease_ttl 5m           # optional upper bound for lease TTL
  default_max_wait 0         # default long-poll wait (default 0 = no wait)
  max_wait 30s               # optional upper bound for long-poll wait
  sse_keepalive 15s          # SSE keepalive comment interval (default 15s)
  sse_max_connection 1h      # optional max SSE connection duration (default unlimited)

  tls { ... }
}
```

| Directive            | Default      | Description                                |
| -------------------- | ------------ | ------------------------------------------ |
| `listen`             | `:9443`      | Bind address                               |
| `grpc_listen`        | —            | Optional gRPC worker listener address      |
| `prefix`             | —            | URL path prefix for all pull endpoints     |
| `auth token`         | **required** | Bearer token allowlist (`env:`/`file:`/`vault:`/`raw:` ref) |
| `max_batch`          | `100`        | Max items per dequeue request              |
| `max_lease_batch`    | `100`        | Max lease IDs per ack/nack/extend request   |
| `default_lease_ttl`  | `30s`        | Lease TTL when client omits it             |
| `max_lease_ttl`      | off          | Optional upper cap for effective lease TTL |
| `default_max_wait`   | `0`          | Long-poll wait when client omits it        |
| `max_wait`           | off          | Optional upper cap for long-poll wait      |
| `sse_keepalive`      | `15s`        | Interval for SSE keepalive comments        |
| `sse_max_connection` | off          | Optional max duration for SSE connections  |
| `tls`                | —            | TLS and optional mTLS configuration        |

> Pull API auth is required when pull routes are present. Deliver-only configs can omit it entirely — the Pull API server is skipped in that case.
>
> `max_batch` and `max_lease_batch` bound different calls: the first caps how many items one dequeue returns, the second how many lease IDs one ack/nack/extend may carry. Both apply identically to the HTTP and gRPC transports. Changing either requires a restart.
>
> `grpc_listen` is optional and only valid when at least one pull route exists. It must use a dedicated listener address (it cannot share with ingress/pull/admin/metrics listeners).

### `admin_api`

Controls the Admin API listener for operator tooling.

```hcl
admin_api {
  listen 127.0.0.1:2019
  prefix /admin       # optional URL prefix
  auth token env:HOOKAIDO_ADMIN_TOKEN   # optional
  tls { ... }
}
```

| Directive    | Default          | Description                         |
| ------------ | ---------------- | ----------------------------------- |
| `listen`     | `127.0.0.1:2019` | Bind address                                            |
| `prefix`     | —                | URL path prefix                                         |
| `auth token` | —                | Bearer token allowlist; required off loopback (see below) |
| `tls`        | —                | TLS and optional mTLS configuration                     |

> **`auth token` is mandatory unless the listener is loopback-only.** An empty token list authorizes every request, and the Admin API is a full control plane — DLQ delete, `messages/publish`, `cancel_by_filter`, and management-endpoint mutations that rewrite the Hookaidofile and trigger a reload. `config validate` therefore rejects an `admin_api` with no `auth token` when `listen` is anything other than a loopback address (`127.0.0.0/8`, `::1`, `localhost`) — including the wildcard forms `:2019`, `0.0.0.0:2019` and `[::]:2019` — or when it co-listens with `ingress`. A hostname that is not `localhost` cannot be resolved at compile time and is treated as non-loopback.

> **Shared listener (single-port deployments):** `ingress`, `pull_api`, and `admin_api` can share one port by giving them the same `listen` address — Hookaido then serves them on a single listener and dispatches by path prefix. Ingress serves its bare route paths (e.g. `/webhooks/...`) as the default handler, while the co-listening API servers serve under their `prefix` values (e.g. `/pull`, `/admin`). This is strictly opt-in (inferred from equal `listen` addresses); separate ports remain the default and recommended posture.
>
> When an address is shared, `config validate` enforces:
>
> - each co-listening API server (`pull_api`/`admin_api`) has a **non-empty, distinct, non-overlapping** `prefix`;
> - **no ingress route path collides** with (is shadowed by, or shadows) a co-listening API prefix — e.g. an `ingress` route `/pull/...` is rejected when `pull_api.prefix` is `/pull`. A catch-all `"/"` route collides with **every** co-listening prefix, since it matches every request path;
> - **identical TLS** settings across everything on the shared address.
>
> Because sharing is inferred from equal `listen` addresses, the address must be written **identically** on each component. `:8080` and `0.0.0.0:8080` are the same socket but not the same string, so they would be treated as separate listeners and the second bind would fail at startup with `EADDRINUSE`; `config validate` rejects that pair instead. Wildcard forms are not equated with a specific address (`:8080` vs `127.0.0.1:8080`), because those can both be bound on Windows.
>
> `pull_api.grpc_listen` and `observability.metrics.listen` always stay on dedicated listeners and may not share an address.

### `queue_limits`

```hcl
queue_limits {
  max_depth 10000    # max queued items before backpressure
  drop_policy reject # "reject" (429) or "drop_oldest"
}
```

Depth is `queued + leased` on every backend. The memory and SQLite backends
enforce the limit exactly. On Postgres, batch enqueues are serialized through an
advisory lock and are exact, while single-item enqueues take a lock-free fast
path close to the limit — with concurrent ingress across connections, depth can
therefore exceed `max_depth` by a handful of items (at most one per pooled
connection), and `drop_oldest` can under-drop by the same amount. Locking every
single enqueue would cost roughly four times the ingress throughput, which is
not a trade worth making for that margin.

### `queue_retention`

```hcl
queue_retention {
  max_age 7d             # prune queued items older than this
  prune_interval 5m      # how often the pruner runs
}
```

Set `max_age off` (or `0`) to disable retention.

### `delivered_retention`

```hcl
delivered_retention {
  max_age 24h    # keep delivered items for this long
}
```

Pruning uses the same `queue_retention.prune_interval` cadence. Set `max_age off` to disable.

Delivered items are retention history, not backlog: they never count against
`queue_limits.max_depth`, which measures `queued + leased` on every backend.

With `queue { backend memory }` the tombstones hold their payloads in memory until they age out, so
their number is bounded separately: at most `queue_limits.max_depth` of them are kept, and the oldest
are evicted first (metric label `delivered_retention_depth`). That bound never rejects an enqueue and
never evicts a queued message — it trims history only.

### `dlq_retention`

```hcl
dlq_retention {
  max_age 30d       # prune dead items older than this
  max_depth 10000   # cap the dead-letter set
}
```

### `attempts_retention`

```hcl
attempts_retention {
  max_age 7d        # prune delivery attempts older than this
  max_rows 200000   # cap the attempt history
}
```

Every delivery attempt is recorded (see [Delivery → Delivery Attempts](delivery.md#delivery-attempts)).
That history is append-only, so both limits default to finite values rather than
unbounded growth: without them a push deployment doing 10 attempts/s adds ~860k
records a day forever — the SQLite file grows until the disk fills, and the
memory backend grows until the process is killed, invisible to the
memory-pressure guard, which counts only queued payloads.

Pruning uses the same `queue_retention.prune_interval` cadence; the memory
backend additionally enforces `max_rows` as it writes. Set `max_age off` and
`max_rows off` to keep attempts forever, which is what earlier versions did.

### `secrets`

Define named secrets with validity windows for key rotation:

```hcl
secrets {
  secret "S1" {
    value env:MY_SECRET_V1
    valid_from "2026-01-01T00:00:00Z"
    valid_until "2026-07-01T00:00:00Z"
  }
  secret "S2" {
    value env:MY_SECRET_V2
    valid_from "2026-06-01T00:00:00Z"
  }
}
```

- `valid_from` is inclusive, `valid_until` is exclusive.
- Omit `valid_until` for "no expiry".
- Referenced via `secret_ref "S1"` in auth and signing blocks.
- `value` accepts `env:`, `file:`, `vault:`, and `raw:` refs.

**Runtime-mutable pools** (v2.7.0+): add `runtime true` to allow the admin API (`POST /admin/secrets/{name}`) to register additional versions at runtime. Useful when the issuing service rotates secrets itself and needs to push them into Hookaido without a redeploy.

```hcl
secrets {
  secret "cituro" {
    runtime true
    max_versions 16            # optional, default 32
    value env:CITURO_BOOTSTRAP # optional; only seeded when the DB pool is empty
    valid_from "2026-04-21T00:00:00Z"
  }
}
```

- Requires `HOOKAIDO_SECRET_ENCRYPTION_KEY` (32 bytes, base64) at startup — runtime secrets are AES-GCM sealed before persisting.
- Persisted to the same store as the queue (SQLite/Postgres). An in-memory queue degrades to memory-only secrets (lost on restart, warning logged).
- `max_versions` is a hard cap to protect against operator error; `Pool.Add` prunes expired versions automatically before returning `secret_pool_full`.
- `max_versions` only applies when `runtime true`.
- See [docs/admin-api.md → Runtime Secret Rotation](admin-api.md#runtime-secret-rotation) for the HTTP contract and [docs/security.md](security.md#runtime-rotation-via-admin-api) for operational notes.

### `vars`

Reusable values with nested expansion:

```hcl
vars {
  BASE_URL https://internal.example.com
  BUILD_TARGET {vars.BASE_URL}/build
}
```

Referenced as `{vars.NAME}` in any value position. Cycles are detected at compile time.

### `defaults`

Global defaults for body limits, delivery, egress policy, and publish policy:

```hcl
defaults {
  max_body 2mb
  max_headers 64kb

  egress {
    allow "*.internal.example.com"
    deny  "169.254.0.0/16"
    https_only on
    redirects off
    dns_rebind_protection on
  }

  deliver {
    retry exponential max 8 base 2s cap 2m jitter 0.2
    timeout 10s
    concurrency 20
  }

  publish_policy {
    direct on
    managed on
    allow_pull_routes on
    allow_deliver_routes on
    require_actor off
    require_request_id off
    fail_closed off
    actor_allow "ci-bot"
    actor_prefix "deploy-"
  }

  trend_signals {
    window 15m
    expected_capture_interval 1m
    stale_grace_factor 3
    sustained_growth_consecutive 3
    sustained_growth_min_samples 5
    sustained_growth_min_delta 10
    recent_surge_min_total 20
    recent_surge_min_delta 10
    recent_surge_percent 50
    dead_share_high_min_total 10
    dead_share_high_percent 20
    queued_pressure_min_total 20
    queued_pressure_percent 75
    queued_pressure_leased_multiplier 2
  }

  adaptive_backpressure {
    enabled off
    min_total 200
    queued_percent 80
    ready_lag 30s
    oldest_queued_age 60s
    sustained_growth on
  }
}
```

**Size values** (`max_body`, `max_headers`, `body_limit`, …) accept a positive integer with an optional `b`/`kb`/`k`/`mb`/`m`/`gb`/`g` suffix (powers of 1024; bare numbers are bytes). The upper bound is `1024gb` — a larger value, or one whose multiplication would overflow, is a compile error rather than a silently wrapped small limit.

`adaptive_backpressure` is an optional soft-pressure ingress guardrail that applies `503` before hard `queue_limits.max_depth` is reached.
- `enabled`: turn adaptive backpressure on/off.
- `min_total`: minimum queue total (`queued+leased+dead`) before guardrails evaluate.
- `queued_percent`: reject when queued share reaches this percentage.
- `ready_lag`: reject when queue `ready_lag_seconds` reaches this duration.
- `oldest_queued_age`: reject when `oldest_queued_age_seconds` reaches this duration.
- `sustained_growth`: when `on`, also reject on sustained backlog growth signals (if trend samples are available).

For production threshold tuning profiles, see [Adaptive Backpressure Tuning](adaptive-backpressure.md).

### `observability`

See [Observability](observability.md) for full reference.

```hcl
observability {
  access_log { enabled on; output stderr; format json }
  runtime_log { level info; output stderr; format json }
  metrics { listen ":9900"; prefix "/metrics" }
  tracing { enabled on; collector "https://otel.example.com/v1/traces" }
}
```

## Route Blocks

Each route block defines a webhook endpoint path and its processing pipeline:

```hcl
/webhooks/github {
  # Optional management labels
  application "github"
  endpoint_name "push-events"

  # Optional matchers (ANDed with path)
  match {
    method POST
    host "hooks.example.com"
    header "X-GitHub-Event" "push"
    header_exists "X-GitHub-Delivery"
    query "env" "production"
    query_exists "token"
    remote_ip "203.0.113.0/24"
  }

  # Optional rate limit override
  rate_limit { rps 50 }

  # Authentication (pick one)
  auth hmac env:HOOKAIDO_GITHUB_SECRET
  # auth basic "user" "pass"
  # auth forward "https://auth.example/check"

  # Route-level publish control
  publish {
    enabled on             # default; set "off" to block manual publish
    direct on              # controls global direct publish path
    managed on             # controls endpoint-scoped managed publish path
  }

  # Queue backend
  queue { backend sqlite }  # or "memory" / "postgres"

  # Mode: pull OR deliver (not both)
  pull { path /pull/github }

  # Or push mode:
  # deliver "https://ci.internal/build" {
  #   retry exponential max 5 base 1s cap 30s jitter 0.1
  #   timeout 10s
  #   sign hmac env:DELIVER_SECRET
  # }
}
```

### Routing Semantics

- Evaluation is **top-down, first match wins**.
- Path match uses URL path only (query ignored).
- `"/path"` matches `/path` and `/path/...` (segment boundary), but not `/path-foo`.
- Route paths must be unique (path is the queue key).
- Match criteria within a route are ANDed.
- A route that an **earlier route provably shadows is rejected**. Because matching is prefix-based and first-match-wins, a route sitting underneath an earlier one — `/hooks/github` after `/hooks`, or anything after a `/` catch-all — can never be reached: every request that would select it is answered by the earlier route, with that route's auth and targets. Put the more specific path first. An earlier route that carries `match` criteria can legitimately act as a filter and let the rest fall through, so only an unconstrained earlier route shadows.

### Named Matchers

Define reusable matchers and reference them:

```hcl
@github-push {
  method POST
  header "X-GitHub-Event" "push"
}

/webhooks/github {
  match @github-push
  pull { path /pull/github }
}
```

### Authentication Options

**HMAC** (shorthand):

```hcl
auth hmac env:HOOKAIDO_SECRET
# or with secret rotation:
auth hmac secret_ref "S1"
```

**HMAC** (block form):

```hcl
auth hmac {
  secret env:HOOKAIDO_SECRET
  # or: secret_ref "S1"
  signature_header "X-Signature"
  timestamp_header "X-Timestamp"
  nonce_header "X-Nonce"
  tolerance 5m
}
```

**HMAC** (provider mode — GitHub, Gitea/Forgejo, Stripe, Cituro):

```hcl
auth hmac {
  provider github
  secret env:GITHUB_WEBHOOK_SECRET
}

auth hmac {
  provider gitea
  secret env:GITEA_WEBHOOK_SECRET
}

auth hmac {
  provider stripe
  secret env:STRIPE_WEBHOOK_SECRET
}

auth hmac {
  provider cituro
  secret env:CITURO_WEBHOOK_SECRET
}
```

Provider mode uses the provider's native signature format. `signature_header`, `timestamp_header`, `nonce_header`, and `tolerance` are forbidden in provider mode.

| Provider | Signature header | Signed payload | Replay protection |
|---|---|---|---|
| `github` | `X-Hub-Signature-256: sha256=<hex>` | `body` | none (GitHub omits a timestamp) |
| `gitea` | `X-Gitea-Signature: <hex>` | `body` | none |
| `stripe` | `Stripe-Signature: t=<ts>,v1=<hex>` | `<ts>.<body>` | 5 min fixed tolerance |
| `cituro` | `X-CITURO-SIGNATURE: t=<ts>,s=<hex>` | `<ts>.<body>` | 5 min fixed tolerance |

`stripe` and `cituro` do carry replay protection — they verify a timestamped signature against a fixed 5-minute window. Only `github` and `gitea` have none, because those providers send no timestamp to bind. See [Security](security.md#providers) for the full comparison.

**Basic auth:**

```hcl
auth basic "username" "password"
auth basic "webhook-user" "{env.WEBHOOK_PASSWORD}"
```

Credentials are compared literally. The `env:` / `file:` / `vault:` / `raw:` reference syntax accepted by `auth token`, `auth hmac` and `secret` blocks is **not** resolved here and is rejected at compile time — use the `{env.NAME}` placeholder form, which is expanded when the config is compiled.

**Forward auth:**

```hcl
auth forward "https://auth.example/check"
# or with options:
auth forward "https://auth.example/check" {
  timeout 5s
  copy_headers "X-User-ID"
  copy_headers "X-Org-ID"
  body_limit 64kb
}
```

> `auth forward` is mutually exclusive with `auth basic` and `auth hmac`.

### Deliver Blocks (Push Mode)

```hcl
deliver "https://ci.internal/build" {
  retry exponential max 8 base 2s cap 2m jitter 0.2
  timeout 10s

  # Optional outbound HMAC signing
  sign hmac env:DELIVER_SECRET
  sign signature_header "X-Hookaido-Signature"    # default
  sign timestamp_header "X-Hookaido-Timestamp"     # default

  # Or with secret rotation:
  sign hmac secret_ref "S1"
  sign hmac secret_ref "S2"
  sign secret_selection newest_valid   # or oldest_valid
}
```

Per-route concurrency can override the global default:

```hcl
/webhooks/github {
  deliver_concurrency 5
  deliver "https://ci.internal/build" { ... }
}
```

`deliver_concurrency` is enforced as a shared per-route budget across all route targets.

### Exec Blocks (Subprocess Delivery)

Deliver webhooks by executing a local command. The payload is piped to stdin; metadata is passed as environment variables.

```hcl
deliver exec "/opt/hooks/deploy.sh" {
  timeout 30s
  retry exponential max 3 base 500ms cap 30s jitter 0.1

  env DEPLOY_ENV production
  env API_KEY {env.HANDLER_API_KEY}
}
```

**Metadata environment variables** (always set):

| Variable | Description |
|---|---|
| `HOOKAIDO_ROUTE` | Route path (e.g., `/webhooks/github`) |
| `HOOKAIDO_EVENT_ID` | Message UUID |
| `HOOKAIDO_CONTENT_TYPE` | Content-Type header from inbound request |
| `HOOKAIDO_ATTEMPT` | Retry attempt number (1-indexed) |
| `HOOKAIDO_HEADER_*` | Inbound headers (e.g., `HOOKAIDO_HEADER_X_GITHUB_EVENT`) |
| `PATH` | Inherited from host for command resolution |

**Exit code semantics:**

| Exit Code | Behaviour |
|---|---|
| `0` | Success — message is acked |
| `75` | Temporary failure (EX_TEMPFAIL) — retriable |
| Any other non-zero exit code | General failure — retriable with backoff. This includes `126` and `127`: once the process has run and exited, Hookaido sees only an exit code, so a shell that exits `127` because an inner tool was missing is retried like any other failure |
| Signal | Process killed — retriable |
| Command could not be started | Non-retriable, immediate DLQ. Applies when the binary is not on `PATH`, the file does not exist, or it is not executable — detected before the process runs, so no exit code exists |

Custom `env` values support all placeholder syntaxes (`{env.VAR}`, `{$VAR}`, `{file.PATH}`, `{vars.NAME}`).

`sign` directives are **not supported** with exec blocks (compile error). Cross-platform via `os/exec` (Linux, macOS, Windows).

## Placeholders

Hookaido supports four placeholder syntaxes in config values:

| Syntax           | Resolved       | Description                                |
| ---------------- | -------------- | ------------------------------------------ |
| `{$VAR}`         | Compile time   | Environment variable (Hookaidofile only)   |
| `{$VAR:default}` | Compile time   | Environment variable with fallback         |
| `{env.VAR}`      | Startup/reload | Environment variable at runtime            |
| `{file.PATH}`    | Startup/reload | File content (read failure = config error) |
| `{vars.NAME}`    | Startup/reload | Value from the `vars` block                |

Placeholders resolve within a single value (no cross-token expansion).

## Defaults Table

| Setting                          | Default Value                                 |
| -------------------------------- | --------------------------------------------- |
| `max_body`                       | `2mb`                                         |
| `max_headers`                    | `64kb`                                        |
| `queue_limits.max_depth`         | `10000`                                       |
| `queue_limits.drop_policy`       | `reject`                                      |
| `queue_retention.max_age`        | `7d`                                          |
| `queue_retention.prune_interval` | `5m`                                          |
| `dlq_retention.max_age`          | `30d`                                         |
| `dlq_retention.max_depth`        | `10000`                                       |
| `attempts_retention.max_age`     | `7d`                                          |
| `attempts_retention.max_rows`    | `200000`                                      |
| `deliver.retry`                  | `exponential max 8 base 2s cap 2m jitter 0.2` |
| `deliver.timeout`                | `10s`                                         |
| `deliver.concurrency`            | `20`                                          |
| `defaults.adaptive_backpressure.enabled` | `false`                              |
| `defaults.adaptive_backpressure.min_total` | `200`                               |
| `defaults.adaptive_backpressure.queued_percent` | `80`                         |
| `defaults.adaptive_backpressure.ready_lag` | `30s`                              |
| `defaults.adaptive_backpressure.oldest_queued_age` | `60s`                     |
| `defaults.adaptive_backpressure.sustained_growth` | `true`                      |
| `pull_api.max_batch`             | `100`                                         |
| `pull_api.max_lease_batch`       | `100`                                         |
| `pull_api.default_lease_ttl`     | `30s`                                         |

## Config Management

```bash
# Validate (JSON or text output)
hookaido config validate --config ./Hookaidofile --format json

# Optional strict secret preflight (env/file/vault/raw refs are loaded)
hookaido config validate --config ./Hookaidofile --strict-secrets --format text

# Format (canonical, idempotent, preserves quoting style)
hookaido config fmt --config ./Hookaidofile
```

Config changes are **round-trip safe** — `config fmt` is stable and diff-friendly.
`config validate` checks secret-reference syntax for token/signing/value refs (`env:`, `file:`, `vault:`, `raw:`).
Use `--strict-secrets` when you also want availability/reachability preflight (for example missing env vars, unreadable files, or Vault access failures).

## Hot Reload

With `--watch` or `SIGHUP`, Hookaido reloads the Hookaidofile and applies changes live where safe. If a change requires a restart, the reload is **rejected** and a `config_reloaded_restart_required` log is emitted — the previous config stays active.

```bash
# File-watch mode (automatic)
hookaido run --config ./Hookaidofile --watch

# Manual signal reload
kill -HUP $(cat ./hookaido.pid)
```

### `--watch` and Single-File Mounts

`--watch` watches the **directory** containing the config and filters by
basename. That is the right pattern for editors and for atomic
replace-by-rename, and it has one consequence worth knowing before you rely on
it in a container: **mount the directory, not the file.**

```yaml
# Works with --watch
volumes:
  - ./config-dir:/etc/hookaido:ro

# Does NOT work with --watch
volumes:
  - ./Hookaidofile:/etc/hookaido/Hookaidofile:ro
```

With a single-file bind mount, `/etc/hookaido` inside the container is not the
host directory — it is the container's own directory holding one bind-mounted
entry. Replacing the file on the host (`rsync`, `scp` to a temp name plus `mv`,
or any deploy tool that writes atomically) creates a **new inode**, and the
existing mount still resolves to the old one. The container's directory
genuinely did not change, so there is no event to receive. Kubernetes has the
same failure mode with `subPath` ConfigMap mounts, which are documented not to
receive updates — mount the ConfigMap as a volume instead.

The symptom is silence: `watching_config` is logged, no reload happens, and a
new route stays `404` while the config on disk says otherwise.

Two things help when you cannot change the mount:

- **`--watch-interval`** re-reads the config path on a fixed interval and reloads
  when its **content hash** changes, through the same path as an fsnotify event.
  Off by default; minimum `1s`; requires `--watch`.

  ```bash
  hookaido run --config /etc/hookaido/Hookaidofile --watch --watch-interval 30s
  ```

  A poll that finds no change costs one read and one hash. The hash advances on
  every read, not only on a successful reload, so a rejected reload — an invalid
  config, or a change that requires a restart — is reported once rather than on
  every tick; your next edit changes the hash again and is picked up.

- **A startup warning.** On Linux, if the config file turns out to live on a
  different filesystem than its own directory — which is what a single-file bind
  mount looks like from inside the container — Hookaido logs
  `watch_may_not_fire` at startup with the remedy, instead of leaving you to
  rediscover it. The warning is skipped when `--watch-interval` is set, since
  polling covers the case.

### Live-Reloadable (no restart)

| Config area                                                    | Notes                    |
| -------------------------------------------------------------- | ------------------------ |
| Route table (add/remove/reorder routes, paths, match rules)    |                          |
| Pull endpoint mappings (`pull { path ... }`)                   |                          |
| Auth settings (HMAC secrets, basic auth, forward auth, tokens) | Per-route and global     |
| Rate limits (global + per-route)                               |                          |
| `ingress.trusted_proxies`                                      |                          |
| Management model labels (`application`, `endpoint_name`)       |                          |
| Route-level `max_body` / `max_headers`                         | Per-route overrides only |
| Route-level `publish` / `publish.direct` / `publish.managed`   |                          |
| Trend signals config                                           |                          |
| Deliver targets, URLs, retry, timeout, concurrency, signing     | Dispatcher is restarted in place after a drain; in-flight deliveries finish first (see below) |
| Deliver headers (`header ...`) and exec target `env` values     | Same dispatcher restart; a rotated bearer token in a `header` takes effect on reload |
| Egress policy (`defaults.egress.*`)                            | Applied with the dispatcher restart above |

### Reload Atomicity

A reload either applies in full or changes nothing. Every secret reference in the candidate config is resolved and validated before any of it goes live, so a config that both rotates a secret and contains an unrelated error — an `auth hmac` key file that is not deployed yet, say — leaves the old secret in force rather than rotating it and then reporting failure.

### Dispatcher Drain on Reload

When delivery config changes, the running dispatcher is drained before its replacement starts. The drain budget is derived from the dispatcher's own configuration — the longer of its dequeue long-poll wait and the longest target `timeout`, plus headroom for the lease writes, clamped to between 15s and 60s — because that is how long a worker can legitimately need to notice the stop signal: one waiting for work returns from its dequeue, and one mid-delivery finishes the attempt in flight. Neither dequeues again once the stop signal is set.

The dispatcher is reconciled by every path that advances the running config — a SIGHUP, a `--watch` pickup, and an Admin API managed-endpoint mutation. The last one matters because a mutation reloads the whole file: if the Hookaidofile already carries a `deliver` edit that has not been reloaded yet, the mutation applies it, dispatcher included.

If the budget is exceeded, Hookaido logs `dispatcher_drain_incomplete` and starts the replacement anyway. The old workers are already terminal at that point and refusing would leave no dispatcher at all; each finishes at most the delivery already in flight, so the effect is that per-route concurrency is briefly above the configured value. Delivery is at-least-once regardless. A successful drain logs `dispatcher_stopped_for_reload` instead — that line means the old workers really have exited.

### Restart Required

If any of these change, Hookaido rejects the reload and requires a process restart:

| Config area                                                                  | Reason                                  |
| ---------------------------------------------------------------------------- | --------------------------------------- |
| Listener addresses (`ingress.listen`, `pull_api.listen`, `pull_api.grpc_listen`, `admin_api.listen`) | Socket rebind                           |
| Listener TLS (`tls { ... }` on any listener)                                 | TLS config baked at startup             |
| API prefixes (`pull_api.prefix`, `admin_api.prefix`)                         | HTTP mux topology                       |
| Shared listener mode toggle                                                  | Server topology                         |
| Pull API limits (`max_batch`, `max_lease_batch`, `*_lease_ttl`, `*_max_wait`) | Set on server struct at startup         |
| `defaults.max_body` / `defaults.max_headers` (global defaults)               | Set on ingress/admin servers at startup |
| `defaults.publish_policy.*` (all publish policy fields)                      | Set on admin server at startup          |
| Queue backend type (`sqlite`/`memory`/`postgres`)                            | No migration path                       |
| Queue limits / retention / DLQ retention                                     | Set on queue store at startup           |
| Observability (log sinks, tracing, metrics)                                  | Exporter/sink initialized once          |
| Adding/removing first pull or last deliver route                             | Creates/destroys server/dispatcher      |

---

← [Documentation Index](index.md)
