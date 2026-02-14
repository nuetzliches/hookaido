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
}
```

| Directive    | Default | Description                              |
| ------------ | ------- | ---------------------------------------- |
| `listen`     | `:8080` | Bind address                             |
| `rate_limit` | —       | Global ingress rate limit (token-bucket) |
| `tls`        | —       | TLS and optional mTLS configuration      |

### `pull_api`

Controls the Pull API listener for consumer workers.

```hcl
pull_api {
  listen :9443
  grpc_listen 127.0.0.1:9943  # optional gRPC worker listener
  prefix /pull        # optional URL prefix
  auth token env:HOOKAIDO_PULL_TOKEN

  max_batch 100              # max items per dequeue (default 100)
  default_lease_ttl 30s      # default lease duration (default 30s)
  max_lease_ttl 5m           # optional upper bound for lease TTL
  default_max_wait 0         # default long-poll wait (default 0 = no wait)
  max_wait 30s               # optional upper bound for long-poll wait

  tls { ... }
}
```

| Directive           | Default      | Description                                |
| ------------------- | ------------ | ------------------------------------------ |
| `listen`            | `:9443`      | Bind address                               |
| `grpc_listen`       | —            | Optional gRPC worker listener address      |
| `prefix`            | —            | URL path prefix for all pull endpoints     |
| `auth token`        | **required** | Bearer token allowlist (`env:`/`file:`/`vault:`/`raw:` ref) |
| `max_batch`         | `100`        | Max items per dequeue request              |
| `default_lease_ttl` | `30s`        | Lease TTL when client omits it             |
| `max_lease_ttl`     | off          | Optional upper cap for effective lease TTL |
| `default_max_wait`  | `0`          | Long-poll wait when client omits it        |
| `max_wait`          | off          | Optional upper cap for long-poll wait      |
| `tls`               | —            | TLS and optional mTLS configuration        |

> Pull API auth is required when pull routes are present. Deliver-only configs can omit it entirely — the Pull API server is skipped in that case.
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
| `listen`     | `127.0.0.1:2019` | Bind address                        |
| `prefix`     | —                | URL path prefix                     |
| `auth token` | —                | Optional bearer token allowlist     |
| `tls`        | —                | TLS and optional mTLS configuration |

> **Shared listener:** If `pull_api.listen == admin_api.listen`, both must define non-overlapping `prefix` values (e.g., `/pull` and `/admin`).

### `queue_limits`

```hcl
queue_limits {
  max_depth 10000    # max queued items before backpressure
  drop_policy reject # "reject" (429) or "drop_oldest"
}
```

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

When `queue { backend memory }` is used and delivered retention is enabled, `queue_limits.max_depth`
also guards `queued + leased + delivered` items. This prevents unbounded delivered-retention growth
under sustained pull/ack workloads. If the guard is reached, new enqueues are rejected until retention
pruning frees capacity.

### `dlq_retention`

```hcl
dlq_retention {
  max_age 30d       # prune dead items older than this
  max_depth 10000   # cap the dead-letter set
}
```

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
  queue { backend sqlite }  # or "memory"

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

**Basic auth:**

```hcl
auth basic "username" "password"
```

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

### Live-Reloadable (no restart)

| Config area                                                    | Notes                    |
| -------------------------------------------------------------- | ------------------------ |
| Route table (add/remove/reorder routes, paths, match rules)    |                          |
| Pull endpoint mappings (`pull { path ... }`)                   |                          |
| Auth settings (HMAC secrets, basic auth, forward auth, tokens) | Per-route and global     |
| Rate limits (global + per-route)                               |                          |
| Management model labels (`application`, `endpoint_name`)       |                          |
| Route-level `max_body` / `max_headers`                         | Per-route overrides only |
| Route-level `publish` / `publish.direct` / `publish.managed`   |                          |
| Trend signals config                                           |                          |

### Restart Required

If any of these change, Hookaido rejects the reload and requires a process restart:

| Config area                                                                  | Reason                                  |
| ---------------------------------------------------------------------------- | --------------------------------------- |
| Listener addresses (`ingress.listen`, `pull_api.listen`, `pull_api.grpc_listen`, `admin_api.listen`) | Socket rebind                           |
| Listener TLS (`tls { ... }` on any listener)                                 | TLS config baked at startup             |
| API prefixes (`pull_api.prefix`, `admin_api.prefix`)                         | HTTP mux topology                       |
| Shared listener mode toggle                                                  | Server topology                         |
| Pull API limits (`max_batch`, `*_lease_ttl`, `*_max_wait`)                   | Set on server struct at startup         |
| `defaults.max_body` / `defaults.max_headers` (global defaults)               | Set on ingress/admin servers at startup |
| `defaults.publish_policy.*` (all publish policy fields)                      | Set on admin server at startup          |
| Deliver targets, URLs, retry, timeout, concurrency, signing                  | Dispatcher goroutine topology           |
| Egress policy (`defaults.egress.*`) when deliver routes exist                | Baked into dispatcher                   |
| Queue backend type (`sqlite` ↔ `memory`)                                     | No migration path                       |
| Queue limits / retention / DLQ retention                                     | Set on queue store at startup           |
| Observability (log sinks, tracing, metrics)                                  | Exporter/sink initialized once          |
| Adding/removing first pull or last deliver route                             | Creates/destroys server/dispatcher      |

---

← [Documentation Index](index.md)
