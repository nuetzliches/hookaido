# Hookaido Design (MVP Core)

Status: draft, canonical English spec.

## Goals
- Single binary, sane defaults, minimal config.
- Config file is the source of truth; supports hot reload.
- Default deployment: DMZ queue + internal workers pull (outbound-only from internal to DMZ).

## Open Source Policy
- Use open-source dependencies only (build/runtime/test), ideally OSI-approved licenses.
- Avoid proprietary runtime dependencies; external services must be optional.
- Add a `LICENSE` file (OSI-approved) before the first public release.

## Non-Goals (MVP Core)
- Full management API (applications/endpoints/messages).
- Built-in workers for pull mode (operator provides workers).
- Arbitrary transformation plugins.

## Deployment Modes
- `single-node`: ingress + queue + (optional) dispatcher in one process.
- `dmz-queue pull` (default): ingress + queue in DMZ, internal workers consume via Pull API.
- `dmz+internal push` (optional): ingress in DMZ, queue/dispatcher internal.

## Configuration (Hookaidofile)
Config is Caddyfile-inspired: short forms first, explicit blocks available when needed.

Minimal push:
```hcl
/webhooks/github {
  deliver https://ci.internal/build {}
}
```

Minimal pull:
```hcl
pull_api {
  auth token "env:HOOKAIDO_PULL_TOKEN"
}

/webhooks/github {
  # optional ingress auth
  auth hmac "env:HOOKAIDO_INGRESS_SECRET"
  pull { path /pull/github }
}
```

### Config Blocks (MVP)
The full DSL will evolve, but MVP should support these blocks with sane defaults:

Global:
- `ingress { listen, tls?, rate_limit? }`
- `inbound { ... }` (optional channel wrapper; bare top-level routes are implicit inbound)
- `outbound { ... }` (channel wrapper for API-to-queue-to-push flows; `deliver` required, no `auth`/`match`/`rate_limit`/`pull`)
- `internal { ... }` (channel wrapper for internal job queues; `pull` required, no `auth`/`match`/`rate_limit`/`deliver`)
- `vars { NAME value ... }` (top-level reusable values)
- `pull_api { listen, prefix?, tls?, auth?, max_batch?, default_lease_ttl?, max_lease_ttl?, default_max_wait?, max_wait? }`
- `admin_api { listen, prefix?, tls?, auth? }`
- `queue_limits { max_depth, drop_policy }`
- `queue_retention { max_age, prune_interval }`
- `delivered_retention { max_age }`
- `dlq_retention { max_age, max_depth }`
- `secrets { secret "ID" { value, valid_from, valid_until? } }`
- `defaults { max_body, max_headers, egress?, publish_policy?, deliver?, trend_signals? }`
- `observability { access_log, runtime_log?, metrics?, tracing? }`:
  - `access_log`: `on|off` or block `access_log { enabled?, output?, path?, format? }`
  - `runtime_log`: `debug|info|warn|error|off` or block `runtime_log { level?, output?, path?, format? }`
  - `metrics`: shorthand `metrics on|off` or block `metrics { enabled?, listen?, prefix? }` (`enabled` default `on` when metrics is set)
  - `tracing`: shorthand `tracing on|off` or block `tracing { enabled?, collector?, url_path?, timeout?, compression?, insecure?, proxy_url?, tls { ca_file, cert_file?, key_file?, server_name?, insecure_skip_verify? }, retry?, header* }`
- TLS lives inside `ingress`/`pull_api`/`admin_api` as `tls { cert_file, key_file, client_ca?, client_auth? }`.

Per-route:
- `application "..."` + `endpoint_name "..."` (optional management labels; must be set together and match `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
- `match { method, host, header, header_exists, query, query_exists, remote_ip }` (optional; additional matchers are ANDed)
- `rate_limit { rps, burst? }` (optional; per-route ingress rate limit override)
- `auth ...` (`basic`, `hmac`, `forward`)
  - HMAC shorthand: `auth hmac "env:HOOKAIDO_INGRESS_SECRET"` or `auth hmac secret_ref "S1"` (optional inline options block after shorthand, e.g. `auth hmac secret_ref "S1" { signature_header ... timestamp_header ... nonce_header ... tolerance ... }`)
  - HMAC block (richer): `auth hmac { secret ... | secret_ref ... ; signature_header ... ; timestamp_header ... ; nonce_header ... ; tolerance ... }` (`signature_header`/`timestamp_header`/`nonce_header` must be distinct after defaults are applied)
  - Forward shorthand: `auth forward "https://auth.example/check"`
  - Forward block (optional): `auth forward "https://auth.example/check" { timeout ... ; copy_headers ... ; body_limit ... }`
- `publish on|off` (optional; defaults `on`; when `off`, Admin/MCP publish mutations reject this route)
- `publish.direct on|off` (optional; defaults `on`; when `off`, global direct publish path rejects this route)
- `publish.managed on|off` (optional; defaults `on`; when `off`, endpoint-scoped managed publish path rejects this route)
- `queue "sqlite|memory"` shorthand or block `queue { backend "sqlite|memory" }` (defaults to SQLite; runtime currently uses one backend process-wide, so mixed route backends are rejected)
- `pull { path, auth? }` (pull mode; excludes `deliver`)
- `deliver "https://..." { retry?, timeout?, sign ... }` (push mode; optional in MVP core)
  - signing directives: `sign hmac <secret-ref>` or repeated `sign hmac secret_ref <ID>`, optional `sign signature_header <name>`, optional `sign timestamp_header <name>`, optional `sign secret_selection <newest_valid|oldest_valid>` (requires `sign hmac secret_ref ...`)
  - `sign signature_header` / `sign timestamp_header` require `sign hmac`
  - signing headers default to `X-Hookaido-Signature` and `X-Hookaido-Timestamp`; names must be valid header tokens and must differ
- `match @name` to attach a named matcher (see below)

### Channel Types
Routes can be declared at top-level (implicit `inbound`) or wrapped in a channel-type block:

| Channel | Ingress traffic | `auth`/`match`/`rate_limit` | `pull` | `deliver` | `publish` | `queue { backend }` | Labels |
|---------|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| `inbound` (default) | yes | allowed | allowed | allowed | allowed | allowed | allowed |
| `outbound` | **no** | **forbidden** | **forbidden** | **required** | allowed | allowed | allowed |
| `internal` | **no** | **forbidden** | **required** | **forbidden** | allowed | allowed | allowed |

Bare top-level routes are implicit `inbound`. The `inbound { }` wrapper is optional syntactic sugar.

Single-route shorthand:
```hcl
outbound /jobs/deploy {
  deliver "https://ci.internal/deploy" { timeout 10s }
}
```

Wrapper form:
```hcl
internal {
  /jobs/report {
    pull { path /pull/reports }
  }
  /jobs/cleanup {
    pull { path /pull/cleanup }
  }
}
```

Compile constraints:
- `outbound` routes that omit `deliver` → compile error.
- `outbound` routes that declare `auth`, `match`, `rate_limit`, or `pull` → compile error.
- `internal` routes that omit `pull` → compile error.
- `internal` routes that declare `auth`, `match`, `rate_limit`, or `deliver` → compile error.

### Placeholders & Env Substitution
- `{$VAR}` or `{$VAR:default}` expands from environment when the config is compiled (Hookaidofile only).
- `{env.VAR}` expands from environment at runtime (currently resolved at startup/reload).
- `{file.PATH}` expands to file content (resolved at startup/reload; read failure is a config error).
- `{vars.NAME}` expands from the top-level `vars` block (supports nested vars; cycles are rejected).
- Placeholders resolve within a single value (no token/line expansion).

### Routing Semantics (Hard Rules)
- Evaluation is top-down; first match wins.
- Match criteria within a route are ANDed.
- Path match uses URL path only (query ignored).
- `"/path"` matches `/path` and `/path/...` (segment boundary), but not `/path-foo`.
- Route paths can be quoted or unquoted, but must start with `/`.
- Route paths must be unique (path is the queue key in MVP).
- `match.host` matches the request host (case-insensitive, port ignored); supports exact hosts, `*`, and `*.example.com` (subdomains only, apex excluded).
- `match.method` matches the HTTP method (case-insensitive; defaults to POST when omitted).
- `match.header` matches exact header values (name is case-insensitive).
- `match.header_exists` requires that the header is present (name is case-insensitive).
- `match.query` matches exact query parameter values.
- `match.query_exists` requires that the query parameter key is present.
- `match.remote_ip` matches request source IP from connection `RemoteAddr`; accepts single IPs (`203.0.113.7`, `2001:db8::1`) or CIDRs (`203.0.113.0/24`, `2001:db8::/32`).
- Ingress rate limiting is token-bucket based: global `ingress.rate_limit` applies to all matched routes unless a route defines its own `rate_limit` override; over-limit requests return `429`.
- Named matchers: define `@name { ... }` at top-level, then attach with `match @name`.
- `auth forward` performs pre-enqueue auth callouts: `2xx` allows, `401/403` denies, and transport/timeouts/other statuses fail closed with `503`.
- `auth forward` is mutually exclusive with route `auth basic` and `auth hmac`.
- If a route defines `pull { auth token ... }`, those tokens override the global `pull_api` token allowlist for that route.

## Queue Model
### Envelope
Persisted, replayable envelope:
- `id`, `route`, `target`, `state`, `received_at`, `attempt`, `next_run_at`, `payload`, `headers`, `trace`, `dead_reason`, `schema_version`.

### State Machine
- `queued` -> `leased` -> `delivered` | `dead`.
- `canceled` is used by admin lifecycle controls (`cancel`) and can be resumed/requeued.

### Leases (Pull Mode)
- `dequeue` creates a lease and makes the item invisible until `lease_until`.
- `ack` removes the item permanently.
- `nack` requeues (optionally with delay). With `dead: true`, it moves the item to the DLQ.
- `extend` extends lease TTL.
- Invalid/expired lease operations return 409.

### Queue Retention & Pruning
- Controlled by `queue_retention { max_age, prune_interval }`.
- Pruning removes queued items whose `received_at` is older than `max_age`.
- Leased items are only eligible once they requeue.
- Set `max_age off` (or `0`) to disable retention.

### Delivered Retention & Pruning
- Controlled by `delivered_retention { max_age }` (opt-in).
- Pruning removes delivered items older than `max_age`.
- Pruning cadence uses `queue_retention.prune_interval`.
- Set `max_age off` (or `0`) to disable delivered retention.

### DLQ Retention & Pruning
- Controlled by `dlq_retention { max_age, max_depth }`.
- Pruning removes dead items older than `max_age` and caps the dead-letter set at `max_depth` (oldest first).
- Pruning cadence uses `queue_retention.prune_interval`.
- Set `max_age off` and `max_depth 0` to disable DLQ retention.
- Dead items persist `dead_reason` for DLQ inspection/drain output.

### Delivery Attempt Store
- Push dispatcher records each delivery attempt (`acked`, `retry`, `dead`) with timestamp and result metadata.
- Attempt fields: `id`, `event_id`, `route`, `target`, `attempt`, `status_code`, `error`, `outcome`, `dead_reason`, `created_at`.
- SQLite persists attempts in `delivery_attempts`; in-memory backend keeps attempts for runtime diagnostics/tests.

### SQLite Migrations
- Schema version is tracked in `schema_migrations` (single row `version`).
- Migrations are forward-only; if the DB version is newer than the binary, startup fails.
- Older versions are migrated on startup before serving traffic.

## Pull API (HTTP/JSON)
- Base URL: `pull_api.listen` + optional `pull_api.prefix` + `pull.path` (slash-normalized).
- Content-Type: `application/json`.
- Optional pull dequeue controls in `pull_api`:
  - `max_batch` (default `100`): caps request `batch`.
  - `default_lease_ttl` (default `30s`): used when dequeue request omits `lease_ttl`.
  - `max_lease_ttl` (default `off`/uncapped): optional upper bound for effective dequeue `lease_ttl`.
  - `default_max_wait` (default `0`): used when dequeue request omits `max_wait`.
  - `max_wait` (default `off`/uncapped): optional upper bound for request `max_wait`.

Endpoints:
- `POST {endpoint}/dequeue`
- `POST {endpoint}/ack`
- `POST {endpoint}/nack`
- `POST {endpoint}/extend`

`nack` request (dead-letter supported):
```json
{ "lease_id": "lease_...", "delay": "5s", "dead": true, "reason": "no_retry" }
```
Notes:
- `dead: true` moves the item to the DLQ and ignores `delay`.

Status codes (MVP):
- 200 (dequeue; may return `items: []`)
- 204 (ack/nack/extend)
- 400 (invalid JSON body; unknown fields or trailing JSON documents are rejected)
- 401 (auth)
- 404 (pull endpoint or operation not found)
- 409 (invalid/expired lease)
- 500 (unexpected store failure)
- 503 (store unavailable / queue overload)

Error body (non-2xx):
```json
{ "code": "invalid_body", "detail": "..." }
```

## Admin API (HTTP/JSON)
- Base URL: `admin_api.listen` + optional `admin_api.prefix`.
- Auth (optional): bearer token allowlist via `admin_api { auth token "env:..." }`.

Endpoints (MVP):
- `GET /healthz`
- `GET /healthz?details=1` (JSON health diagnostics; includes queue rollups, queued age/lag indicators, top route/target backlog buckets, persisted-trend `trend_signals` evaluated with `defaults.trend_signals` policy, explicit `operator_actions` playbooks, and tracing counters when runtime metrics are available)
- `GET /backlog/top_queued` (operator backlog drill-down from queue stats `top_queued`; query params: `route`, `target`, `limit` (default 100, max 1000); when provided, `route` must be an absolute Hookaido route path starting with `/`; response includes bounded-source/truncation indicators)
- `GET /backlog/oldest_queued` (operator backlog drill-down for oldest queued items; query params: `route`, `target`, `limit` (default 100, max 1000); when provided, `route` must be an absolute Hookaido route path starting with `/`; response ordered by `received_at` ascending with per-item age/ready-lag indicators)
- `GET /backlog/aging_summary` (operator backlog drill-down summary by `route+target`; query params: `route`, `target`, `states` (`queued|leased|dead`; default all three), `limit` (default 100, max 1000); when provided, `route` must be an absolute Hookaido route path starting with `/`; response aggregates oldest-first scan windows per selected state with aging/overdue indicators, age-window distribution, age percentiles (`p50/p90/p99`), and source truncation metadata)
- `GET /backlog/trends` (operator backlog trend rollup from persisted snapshots; query params: `route`, `target`, `window` (default `1h`, max `168h`), `step` (default `5m`, min `1m`, max `1h`), `until` (RFC3339, default now); when provided, `route` must be an absolute Hookaido route path starting with `/`; response contains fixed buckets with `*_last`/`*_max` state counts, sample density metadata, derived `signals` evaluated with `defaults.trend_signals` policy, and explicit `operator_actions` playbooks)
- `GET /dlq` (query params: `route`, `limit` (default 100, max 1000), `before` (RFC3339), `include_payload`, `include_headers`, `include_trace`; when provided, `route` must be an absolute Hookaido route path starting with `/`)
- `POST /dlq/requeue` (body: `{ "ids": ["evt_1", ...] }`; requeues matching dead items; validation/audit/store failures return JSON `{ "code", "detail" }`; when targeted IDs resolve to managed routes, optional endpoint-scoped identity hooks from `defaults.publish_policy` (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`) are enforced)
- `POST /dlq/delete` (body: `{ "ids": ["evt_1", ...] }`; deletes matching dead items; validation/audit/store failures return JSON `{ "code", "detail" }`; when targeted IDs resolve to managed routes, optional endpoint-scoped identity hooks from `defaults.publish_policy` (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`) are enforced)
- `GET /messages` (query params: `route` or `application` + `endpoint_name`, `target`, `state` (`queued|leased|delivered|dead|canceled`), `limit` (default 100, max 1000), `before` (RFC3339), `include_payload`, `include_headers`, `include_trace`; when selector mode is `route`, it must be an absolute Hookaido route path starting with `/`; managed selectors return `404` with `code=managed_endpoint_not_found` when selector labels do not resolve, `503` with `code=managed_resolver_missing` when resolver wiring is unavailable, and `503` with `code=managed_target_mismatch` when selector-resolved route ownership is out of sync with route ownership policy mapping)
- `POST /messages/publish` (body: `{ "items": [{ "id", "route"?, "target"?, "application"?, "endpoint_name"?, "payload_b64"?, "headers"?, "trace"?, "received_at"?, "next_run_at"? }, ...] }`; publishes queued items via direct route/target only; route must be absolute (`/` prefix) and resolve to configured targets; management-labeled routes are rejected here and must use endpoint-scoped publish; managed selectors (`application` + `endpoint_name`) are rejected on this global path (`scoped_publish_required`); when `target` is omitted it is auto-selected only for unambiguous single-target routes; decoded payload bytes must fit route `max_body` and item headers must fit route `max_headers`; item header names/values must also be valid HTTP headers (invalid field-name/value returns `code=invalid_header`); duplicate IDs return `409`; requires `X-Hookaido-Audit-Reason`; can be disabled via `defaults.publish_policy.direct off`; route-mode publish can also be limited by `defaults.publish_policy.allow_pull_routes` / `allow_deliver_routes`; route-level `publish off` or `publish.direct off` also rejects publish (`code=route_publish_disabled`); optional audit identity requirements can be enforced via `defaults.publish_policy.require_actor` (`X-Hookaido-Audit-Actor`) and `defaults.publish_policy.require_request_id` (`X-Request-ID`); item validation/policy/target and existing duplicate-ID checks are preflighted across the batch before enqueue; failures return JSON `{ "code", "detail", "item_index?" }`, with `target_unresolvable` details including allowed targets and `payload_too_large`/`headers_too_large` details including bytes-vs-limit values)
- `POST /messages/cancel` (body: `{ "ids": ["evt_1", ...] }`; moves matching `queued|leased|dead` items to `canceled`; validation/audit/store failures return JSON `{ "code", "detail" }`; when targeted IDs resolve to managed routes, optional endpoint-scoped identity hooks from `defaults.publish_policy` (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`) are enforced)
- `POST /messages/cancel_by_filter` (body: `{ "route"?, "application"?, "endpoint_name"?, "target"?, "state"?, "before"?, "limit"?, "preview_only"? }`; cancels matching `queued|leased|dead` items, or returns `matched` count only when `preview_only=true`; when provided, `route` must be an absolute Hookaido route path starting with `/`; validation/policy failures return JSON `{ "code", "detail" }`; route selectors that resolve to management-labeled routes, and unscoped selectors when managed endpoints exist, are rejected with `code=managed_selector_required` and must use `application` + `endpoint_name`; scoped selectors (`application` + `endpoint_name`) that do not resolve return `404` with `code=managed_endpoint_not_found`; when scoped selectors are valid, optional audit identity hooks from `defaults.publish_policy` (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`) are enforced; managed-scope evaluation returns `503` with `code=managed_resolver_missing` only when fail-closed policy is enabled and model context is unavailable)
- `POST /messages/requeue` (body: `{ "ids": ["evt_1", ...] }`; moves matching `dead|canceled` items to `queued`; validation/audit/store failures return JSON `{ "code", "detail" }`; when targeted IDs resolve to managed routes, optional endpoint-scoped identity hooks from `defaults.publish_policy` (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`) are enforced)
- `POST /messages/requeue_by_filter` (body: `{ "route"?, "application"?, "endpoint_name"?, "target"?, "state"?, "before"?, "limit"?, "preview_only"? }`; requeues matching `dead|canceled` items, or returns `matched` count only when `preview_only=true`; when provided, `route` must be an absolute Hookaido route path starting with `/`; validation/policy failures return JSON `{ "code", "detail" }`; route selectors that resolve to management-labeled routes, and unscoped selectors when managed endpoints exist, are rejected with `code=managed_selector_required` and must use `application` + `endpoint_name`; scoped selectors (`application` + `endpoint_name`) that do not resolve return `404` with `code=managed_endpoint_not_found`; when scoped selectors are valid, optional audit identity hooks from `defaults.publish_policy` (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`) are enforced; managed-scope evaluation returns `503` with `code=managed_resolver_missing` only when fail-closed policy is enabled and model context is unavailable)
- `POST /messages/resume` (body: `{ "ids": ["evt_1", ...] }`; moves matching `canceled` items to `queued`; validation/audit/store failures return JSON `{ "code", "detail" }`; when targeted IDs resolve to managed routes, optional endpoint-scoped identity hooks from `defaults.publish_policy` (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`) are enforced)
- `POST /messages/resume_by_filter` (body: `{ "route"?, "application"?, "endpoint_name"?, "target"?, "state"?, "before"?, "limit"?, "preview_only"? }`; resumes matching `canceled` items, or returns `matched` count only when `preview_only=true`; when provided, `route` must be an absolute Hookaido route path starting with `/`; validation/policy failures return JSON `{ "code", "detail" }`; route selectors that resolve to management-labeled routes, and unscoped selectors when managed endpoints exist, are rejected with `code=managed_selector_required` and must use `application` + `endpoint_name`; scoped selectors (`application` + `endpoint_name`) that do not resolve return `404` with `code=managed_endpoint_not_found`; when scoped selectors are valid, optional audit identity hooks from `defaults.publish_policy` (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`) are enforced; managed-scope evaluation returns `503` with `code=managed_resolver_missing` only when fail-closed policy is enabled and model context is unavailable)
- For ID-based mutation routes (`POST /dlq/requeue`, `POST /dlq/delete`, `POST /messages/cancel`, `POST /messages/requeue`, `POST /messages/resume`), ownership-source drift between `ManagementModel` and route-policy ownership callbacks fails closed with `503` and `code=managed_target_mismatch` when targeted IDs resolve to conflicting managed-route ownership.
- For global managed selectors on `GET /messages`, `GET /attempts`, and `POST /messages/*_by_filter` (`application` + `endpoint_name`), resolved route ownership drift vs route ownership policy mapping, or managed endpoint target drift vs route target resolution, fails closed with `503` and `code=managed_target_mismatch`.
- For global route-selector `POST /messages/publish` and route-selector/unscoped `POST /messages/*_by_filter`, when `defaults.publish_policy.fail_closed on` is enabled and managed-route context cannot be evaluated, requests fail closed with `503` and `code=managed_resolver_missing`.
- For global route-selector `POST /messages/*_by_filter`, ownership-source drift between `ManagementModel` and route-policy ownership callbacks for the selected route fails closed with `503` and `code=managed_target_mismatch`.
- `GET /attempts` (query params: `route` or `application` + `endpoint_name`, `target`, `event_id`, `outcome` (`acked|retry|dead`), `limit` (default 100, max 1000), `before` (RFC3339); when selector mode is `route`, it must be an absolute Hookaido route path starting with `/`; managed selectors return `404` with `code=managed_endpoint_not_found` when selector labels do not resolve, `503` with `code=managed_resolver_missing` when resolver wiring is unavailable, and `503` with `code=managed_target_mismatch` when selector-resolved route ownership is out of sync with route ownership policy mapping)
- `GET /management/model` (returns runtime management projection with `application_count`, `path_count`, and `applications[].endpoints[]` entries (`name`, `route`, `mode`, `targets`, `publish_policy` with `enabled` / `direct_enabled` / `managed_enabled`))
- `GET /applications` (returns management application list with per-application endpoint counts)
- `GET /applications/{application}/endpoints` (returns endpoint entries for one application; entries include `publish_policy`; `application` is path-escaped and matched exactly)
- `GET /applications/{application}/endpoints/{endpoint_name}` (returns one endpoint projection with `route`, `mode`, `targets`, and `publish_policy`; path segments are URL-decoded)
- `PUT /applications/{application}/endpoints/{endpoint_name}` (body: `{ "route": "/path" }`; `route` must be an absolute Hookaido route path starting with `/`; target route must allow managed publish (`route.publish on` and `route.publish.managed on`) or mapping upsert is rejected as conflict; when moving an existing endpoint mapping to another route, target publish profile (mode + targets) must match the current route or upsert is rejected as conflict (`management_route_target_mismatch`); moves are also rejected while the current route still has active `queued`/`leased` backlog (`management_route_backlog_active`); upserts application/endpoint mapping onto an existing route label in the config source-of-truth, then atomically writes + reloads; requires `X-Hookaido-Audit-Reason`; optional audit identity requirements can be enforced via `defaults.publish_policy.require_actor` / `defaults.publish_policy.require_request_id`; optional scoped managed identity hooks can also be enforced via `defaults.publish_policy.actor_allow` / `actor_prefix`, returning `code=audit_actor_not_allowed` on mismatch; mutation failures return JSON `{ "code", "detail" }`, with conflict details including the specific cause and specific conflict codes (`management_route_already_mapped`, `management_route_publish_disabled`, `management_route_target_mismatch`, `management_route_backlog_active`, or generic `management_conflict`))
- `DELETE /applications/{application}/endpoints/{endpoint_name}` (removes mapping labels from the currently mapped route in config source-of-truth, then atomically writes + reloads; deletes are rejected while the current route still has active `queued`/`leased` backlog (`management_route_backlog_active`); requires `X-Hookaido-Audit-Reason`; optional audit identity requirements can be enforced via `defaults.publish_policy.require_actor` / `defaults.publish_policy.require_request_id`; optional scoped managed identity hooks can also be enforced via `defaults.publish_policy.actor_allow` / `actor_prefix`, returning `code=audit_actor_not_allowed` on mismatch; mutation failures return JSON `{ "code", "detail" }`)
- `GET /applications/{application}/endpoints/{endpoint_name}/messages` (endpoint-scoped message listing; selector hints `route`/`application`/`endpoint_name` are not allowed in query params; when managed endpoint ownership mapping is out of sync with route ownership policy resolution, or managed endpoint targets are out of sync with route target resolution, requests fail closed with `503` and `code=managed_target_mismatch`)
- `POST /applications/{application}/endpoints/{endpoint_name}/messages/publish` (same body shape as `POST /messages/publish`, but scoped to the endpoint resource and required for managed publish; selector hints (`route`, `application`, `endpoint_name`) are not allowed in items on this scoped path; decoded payload bytes must fit scoped route `max_body`, item headers must fit scoped route `max_headers`, and header names/values must be valid HTTP headers (`code=invalid_header` on invalid field-name/value); oversize details include bytes-vs-limit values; requires `X-Hookaido-Audit-Reason` header; can be disabled via `defaults.publish_policy.managed off`; route-mode publish can also be limited by `defaults.publish_policy.allow_pull_routes` / `allow_deliver_routes`; route-level `publish off` or `publish.managed off` also rejects publish (`code=route_publish_disabled`); when managed endpoint targets or ownership mapping are out of sync with route target/ownership resolution, publish fails closed with `503` and `code=managed_target_mismatch`; optional audit identity requirements can be enforced via `defaults.publish_policy.require_actor` / `defaults.publish_policy.require_request_id`; optional scoped managed identity hooks can also be enforced via `defaults.publish_policy.actor_allow` / `defaults.publish_policy.actor_prefix`, returning `code=audit_actor_not_allowed` on mismatch; item validation/policy/target and existing duplicate-ID checks are preflighted across the batch before enqueue)
- `POST /applications/{application}/endpoints/{endpoint_name}/messages/cancel_by_filter` (same filter fields as `POST /messages/cancel_by_filter`, scoped to the endpoint resource; selector hints `route`/`application`/`endpoint_name` are not allowed in body payload; requires `X-Hookaido-Audit-Reason` header; optional audit identity requirements can be enforced via `defaults.publish_policy.require_actor` / `defaults.publish_policy.require_request_id`; optional scoped managed identity hooks can also be enforced via `defaults.publish_policy.actor_allow` / `defaults.publish_policy.actor_prefix`, returning `code=audit_actor_not_allowed` on mismatch)
- `POST /applications/{application}/endpoints/{endpoint_name}/messages/requeue_by_filter` (same filter fields as `POST /messages/requeue_by_filter`, scoped to the endpoint resource; selector hints `route`/`application`/`endpoint_name` are not allowed in body payload; requires `X-Hookaido-Audit-Reason` header; optional audit identity requirements can be enforced via `defaults.publish_policy.require_actor` / `defaults.publish_policy.require_request_id`; optional scoped managed identity hooks can also be enforced via `defaults.publish_policy.actor_allow` / `defaults.publish_policy.actor_prefix`, returning `code=audit_actor_not_allowed` on mismatch)
- `POST /applications/{application}/endpoints/{endpoint_name}/messages/resume_by_filter` (same filter fields as `POST /messages/resume_by_filter`, scoped to the endpoint resource; selector hints `route`/`application`/`endpoint_name` are not allowed in body payload; requires `X-Hookaido-Audit-Reason` header; optional audit identity requirements can be enforced via `defaults.publish_policy.require_actor` / `defaults.publish_policy.require_request_id`; optional scoped managed identity hooks can also be enforced via `defaults.publish_policy.actor_allow` / `defaults.publish_policy.actor_prefix`, returning `code=audit_actor_not_allowed` on mismatch)
- For endpoint-scoped managed `POST .../messages/*_by_filter`, when managed endpoint ownership mapping is out of sync with route ownership policy resolution, or managed endpoint targets are out of sync with route target resolution, requests fail closed with `503` and `code=managed_target_mismatch`.
- For `/applications/...` resource paths, URL-decoded `{application}` and `{endpoint_name}` must match `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`; invalid path segments return `400` with `code=invalid_body`.
- For managed selectors on global read/filter/publish routes (`application`, `endpoint_name` in query/body/item fields), labels must also match `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`; invalid labels return `400` with `code=invalid_body`.
- Top-level Admin router/auth failures (`401`, `404`, `405`) return structured JSON errors (`code`, `detail`) with stable codes (`unauthorized`, `not_found`, `method_not_allowed`).

`GET /dlq` response:
```json
{
  "items": [
    { "id": "evt_1", "route": "/r", "target": "pull", "received_at": "2026-02-06T12:00:00Z", "attempt": 3, "dead_reason": "no_retry" }
  ]
}
```

`GET /attempts` response:
```json
{
  "items": [
    { "id": "att_1", "event_id": "evt_1", "route": "/r", "target": "https://ci.internal/build", "attempt": 2, "status_code": 502, "outcome": "dead", "dead_reason": "max_retries", "created_at": "2026-02-06T12:00:00Z" }
  ]
}
```

## Delivery Semantics
- At-least-once.
- Ingress ACK only after durable enqueue (queue ACK).
- Retry on network errors, timeouts, 5xx, 429/408; no retry on other 4xx.
- Backpressure: 429 (rate limit) or 503 (queue overload).

## Defaults (80/20)
- `max_body`: `2mb`
- `max_headers`: `64kb`
- `pull_api`: `max_batch 100`, `default_lease_ttl 30s`, `max_lease_ttl off` (uncapped), `default_max_wait 0`, `max_wait off` (uncapped)
- `queue_limits`: `max_depth 10000`, `drop_policy "reject"`
- `queue_retention`: `max_age "7d"`, `prune_interval "5m"`
- `dlq_retention`: `max_age "30d"`, `max_depth 10000`
- `deliver`: retry exponential (`max 8`, `base 2s`, `cap 2m`, `jitter 0.2`), `timeout 10s`, `concurrency 20`
- deliver signing headers (when `sign hmac` is set): `signature_header "X-Hookaido-Signature"`, `timestamp_header "X-Hookaido-Timestamp"`
- deliver signing secret selection (when multiple `sign hmac secret_ref` are set): `secret_selection "newest_valid"`
- `egress`: `https_only=true`, `redirects=false`, `dns_rebind_protection=true`
- `publish_policy`: `direct=true`, `managed=true`, `allow_pull_routes=true`, `allow_deliver_routes=true`, `require_actor=false`, `require_request_id=false`, `fail_closed=false`, `actor_allow=[]`, `actor_prefix=[]`
- `trend_signals`: `window 15m`, `expected_capture_interval 1m`, `stale_grace_factor 3`, `sustained_growth_consecutive 3`, `sustained_growth_min_samples 5`, `sustained_growth_min_delta 10`, `recent_surge_min_total 20`, `recent_surge_min_delta 10`, `recent_surge_percent 50`, `dead_share_high_min_total 10`, `dead_share_high_percent 20`, `queued_pressure_min_total 20`, `queued_pressure_percent 75`, `queued_pressure_leased_multiplier 2`

Egress policy notes:
- `egress.allow` / `egress.deny` accept hosts, IPs, or CIDRs (wildcards `*` for any host and `*.example.com` for subdomains).
- Deny rules apply first; allowlist (if present) must match.
- Policy is enforced for push deliveries (dispatcher).

## Security
- Secrets via env/file/Vault refs (with `raw:` kept for dev/tests).
- Inbound HMAC supports replay protection (timestamp + nonce + tolerance).
- Outbound `deliver` HMAC signing is optional per target via `sign hmac`.
- Egress is SSRF-safe by default (see defaults).
- Admin API and Pull API are logically separated; separate listeners by default.
- TLS client auth: `tls.client_auth` values are `none`, `request`, `require`, `verify_if_given`, `require_and_verify` (`require` is the default when `client_ca` is set).

### Outbound Deliver Signing (MVP)
- Configure per-target signing in `deliver` blocks with `sign hmac <secret-ref>`.
- `sign hmac` supports direct refs (`env:...`, `file:...`, `vault:...`, `raw:...`) and indirection via one or more `sign hmac secret_ref <ID>` entries from the top-level `secrets` block.
- `sign secret_selection` controls multi-`secret_ref` selection: `newest_valid` (default) or `oldest_valid`; it requires `sign hmac secret_ref ...` entries.
- For multiple `secret_ref` entries, signing selects a secret version valid at signing timestamp (`valid_from` inclusive, `valid_until` exclusive) using configured selection mode.
- Canonical string:
  - `<METHOD_UPPER>\n<URL_PATH>\n<UNIX_TIMESTAMP_UTC_SECONDS>\n<SHA256_HEX(body)>`
- `URL_PATH` is the escaped URL path only (query string excluded).
- Signature is `hex(HMAC-SHA256(secret, canonical))`.
- Dispatcher writes timestamp and signature headers on each outbound request (`X-Hookaido-Timestamp`, `X-Hookaido-Signature` by default).

### Secret References (MVP)
To keep configs diff-friendly, prefer referencing secrets by ID:
- `secret "S1" { value "env:MY_SECRET" valid_from "..." valid_until "..." }`
- `auth hmac ... secret_ref "S1" { ... }`
- direct refs support `env:`, `file:`, `vault:`, and `raw:` schemes.

Secret rotation semantics:
- `valid_from` is inclusive; `valid_until` is exclusive (omit `valid_until` for "no end").
- Signing uses the configured selection mode for valid `secret_ref` versions at signing timestamp (`newest_valid` by default).
- Verification tries all secrets valid at the request timestamp (from the signed timestamp header), not just the wall clock at verification time.

## Observability
- Separate access logs: ingress, pull API, admin API.
- Runtime logs: structured JSON.
- Log sinks: `stdout`/`stderr`/`file` with `path` required for `file`.
- Metrics: Prometheus endpoint (default listen `127.0.0.1:9900`, prefix `/metrics` when enabled).
- Tracing: OpenTelemetry OTLP/HTTP exporter when enabled; supports config-driven `collector`, `url_path`, `timeout`, `compression` (`none|gzip`), `insecure`, `proxy_url`, `tls.ca_file`, `tls.cert_file`, `tls.key_file`, `tls.server_name`, `tls.insecure_skip_verify`, `retry { enabled, initial_interval, max_interval, max_elapsed_time }`, and repeated `header` directives; HTTP servers and outbound dispatcher client are instrumented.
- Tracing headers must be valid HTTP header name/value pairs; invalid entries fail config validation.
- Metrics include tracing diagnostics counters: `hookaido_tracing_enabled`, `hookaido_tracing_init_failures_total`, `hookaido_tracing_export_errors_total`.
- Metrics include publish mutation counters: `hookaido_publish_accepted_total`, `hookaido_publish_rejected_total`, rejection-class counters (`hookaido_publish_rejected_validation_total`, `hookaido_publish_rejected_policy_total`, `hookaido_publish_rejected_conflict_total`, `hookaido_publish_rejected_queue_full_total`, `hookaido_publish_rejected_store_total`), managed-ownership policy counters (`hookaido_publish_rejected_managed_target_mismatch_total`, `hookaido_publish_rejected_managed_resolver_missing_total`), and scoped counters (`hookaido_publish_scoped_accepted_total`, `hookaido_publish_scoped_rejected_total`).
- Metrics include ingress counters: `hookaido_ingress_accepted_total`, `hookaido_ingress_rejected_total`, `hookaido_ingress_enqueued_total`.
- Metrics include delivery counters: `hookaido_delivery_attempts_total`, `hookaido_delivery_acked_total`, `hookaido_delivery_retry_total`, `hookaido_delivery_dead_total`.
- Metrics include on-scrape queue depth gauge: `hookaido_queue_depth{state}` (queued, leased, dead).

## CLI
- `hookaido run --config ./Hookaidofile [--db ./hookaido.db] [--pid-file ./hookaido.pid] [--watch] [--log-level info] [--dotenv ./.env]`
- `hookaido config fmt --config ./Hookaidofile`
- `hookaido config validate --config ./Hookaidofile --format json|text`

## MCP Integration (Optional RFC)
This section defines an optional MCP server for AI-assisted operations. It is not required for MVP runtime behavior.

Goals:
- Expose stable, typed operations to AI agents instead of shell-only workflows.
- Keep the config file as source of truth while enabling controlled operational workflows.
- Reduce operator error for repetitive tasks (inspect, validate, apply, observe).

Non-goals:
- Arbitrary shell execution via MCP.
- Bypassing existing authz/authn controls of Admin/Pull APIs.

### Principles
- Default mode is read-only.
- Mutating tools are explicit opt-in and role-gated.
- Every mutating call is auditable.
- Secrets are always redacted in MCP responses.

### Tool Families
Config tools:
- `config_parse` (parse Hookaidofile; return AST/errors)
- `config_compile` (compile + normalized view + warnings/errors)
- `config_validate` (validation result equivalent to `hookaido config validate`)
- `config_fmt_preview` (format preview without writing)
- `config_diff` (current vs candidate normalized diff)
- `config_apply` (atomic write, only after successful validate/compile)

Runtime tools:
- `instance_status` (process/listener/tracing/queue backend status)
- `instance_logs_tail` (bounded log tail)
- `instance_reload` (reload config)
- `instance_start`, `instance_stop` (optional; disabled by default)

Queue/Admin tools:
- `admin_health`
- `backlog_top_queued`
- `backlog_oldest_queued`
- `backlog_aging_summary`
- `backlog_trends`
- `dlq_list`, `dlq_requeue`, `dlq_delete`
- `messages_list`, `messages_publish`, `messages_cancel`, `messages_requeue`, `messages_resume`, `messages_cancel_by_filter`, `messages_requeue_by_filter`, `messages_resume_by_filter`
- `management_model` (application/endpoint projection from compiled config)
- `management_endpoint_upsert`, `management_endpoint_delete` (application/endpoint mapping CUD in config source-of-truth)
- `attempts_list`

Backend note:
- For `queue.backend sqlite`, MCP queue tools can read/mutate via direct SQLite access.
- For `queue.backend memory`, MCP queue tools should proxy the configured Admin API endpoints (running instance required).

### Access Model
- `read` role: inspect-only tools.
- `operate` role: runtime tools + safe queue mutations.
- `admin` role: config apply + process control.
- Tool-level authorization must be enforced server-side even if transport auth is valid.

### Guardrails (Required)
- Path allowlist for config writes (for example: `./Hookaidofile` only).
- Endpoint allowlist for Admin/Pull API reachability.
- Per-tool timeout and payload limits.
- Hard caps on list operations (`limit <= 1000`) and log-tail size.
- For route-filtered queue tools, `route` inputs must be absolute Hookaido route paths (`/` prefix).
- Idempotency and dedup for mutating requests where applicable.
- Structured audit log for every mutating call:
  - `timestamp`, `principal`, `tool`, `input_hash`, `result`, `duration_ms`

### Config Apply Semantics
- `config_apply` must be atomic (temp file + fsync + rename).
- `config_apply` must fail if `config_validate`/`config_compile` fail.
- Optional write policy:
  - `preview_only`: never writes
  - `write_only`: writes without process control
  - `write_and_reload`: writes then reloads and verifies health
- On reload failure, return structured error and preserve last-known-good config pointer.

### Rollout Plan
1. Stage 1 (read-only): config inspect/validate + health/DLQ/messages/attempts read.
2. Stage 2 (controlled mutations): DLQ/message mutations and gated `config_apply`.
3. Stage 3 (runtime control): start/stop/reload in dedicated ops environments.

### Example Tool Contracts (Compact)
`messages_list` request:
```json
{
  "route": "/webhooks/github",
  "state": "dead",
  "limit": 100,
  "before": "2026-02-06T12:00:00Z",
  "include_payload": false
}
```

`config_apply` request:
```json
{
  "path": "./Hookaidofile",
  "content": "...",
  "mode": "write_and_reload"
}
```

## Implementation Roadmap (Suggested)
1. Implement DSL parsing + validation for minimal pull routes and `pull_api` listener.
2. Implement in-memory queue with correct lease semantics + unit tests.
3. Implement Pull API handlers (dequeue/ack/nack/extend) + integration tests.
4. Add SQLite backend (WAL) + crash recovery tests.
5. Add ingress enqueue path + E2E slice.
