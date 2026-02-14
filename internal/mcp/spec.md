# Hookaido MCP Spec (Stage 1 + Stage 2 + Stage 3 Slice)

Status: implemented (read tools + guarded mutation tools + guarded runtime control tools).

Transport:
- JSON-RPC 2.0 over stdio with `Content-Length` framing.
- Supported methods: `initialize`, `ping`, `tools/list`, `tools/call`, `notifications/initialized`.

Server capabilities:
- `tools` only.
- No resources/prompts in this slice.

Authorization role:
- MCP tool authorization role is configured via `hookaido mcp serve --role read|operate|admin` (default `read`).
- `read`: inspect-only tools.
- `operate`: read tools plus safe queue mutations and runtime inspect tools (`instance_status`, `instance_logs_tail`) when the corresponding feature flags are enabled.
- `admin`: full tool surface, including config-lifecycle mutations and runtime process-control tools, when feature flags are enabled.
- Mutating tools additionally require an explicit session principal via `hookaido mcp serve --principal ...`.

## Tool List

### `config_parse`
Parse Hookaidofile and return structured AST or parser errors.

Arguments:
```json
{
  "path": "./Hookaidofile"
}
```

Notes:
- `path` is optional.
- If set, it must match the configured `--config` path (allowlist guardrail).
- Parse failures are returned as structured output (`ok=false`, `errors[]`, `parse_only=true`) instead of MCP tool-call transport errors.
- Successful responses include `ast` (parsed config object with parser/formatter fidelity fields, including quote/set flags).

### `config_validate`
Validate Hookaidofile and return warnings/errors.

Arguments:
```json
{
  "path": "./Hookaidofile",
  "strict_secrets": false
}
```

Notes:
- `path` is optional.
- If set, it must match the configured `--config` path (allowlist guardrail).
- `strict_secrets` is optional (`false` by default). When `true`, validation preflights all compiled secret refs by loading them (`env`, `file`, `vault`, `raw`) so missing env vars, unreadable files, or unreachable Vault refs fail validation.

### `config_compile`
Compile Hookaidofile and return normalized runtime summary + validation result.

Arguments:
```json
{
  "path": "./Hookaidofile"
}
```

Notes:
- `summary.queue_backend` reports the compiled runtime queue backend (`sqlite`, `memory`, or `mixed` when config is invalid).
- `summary.publish_policy_direct_enabled`, `summary.publish_policy_managed_enabled`, `summary.publish_policy_allow_pull_routes`, `summary.publish_policy_allow_deliver_routes`, `summary.publish_policy_require_actor`, `summary.publish_policy_require_request_id`, `summary.publish_policy_fail_closed`, `summary.publish_policy_actor_allowlist`, and `summary.publish_policy_actor_prefixes` report compiled publish-path policy settings from `defaults.publish_policy`.
- Compile errors include ingress HMAC header-collision validation for route `auth hmac` (`signature_header`, `timestamp_header`, and `nonce_header` must be distinct after defaults are applied).

### `config_fmt_preview`
Format Hookaidofile without writing to disk.

Arguments:
```json
{
  "path": "./Hookaidofile"
}
```

Notes:
- Preserves observability metrics shorthand (`observability { metrics on|off }`) in formatted output.
- Preserves observability tracing shorthand (`observability { tracing on|off }`) in formatted output.
- Preserves route queue shorthand (`queue sqlite|memory`) in formatted output.
- Normalizes mixed route HMAC shorthand+inline options (`auth hmac secret_ref "S1" { ... }`) into stable block output (`auth hmac { ... }`).

### `config_diff`
Compute normalized unified diff between current and candidate Hookaidofile content.

Arguments:
```json
{
  "path": "./Hookaidofile",
  "content": "\"/webhooks/github\" { deliver \"https://ci.internal/build\" {} }\n",
  "context": 3
}
```

Notes:
- `content` is required.
- `context` is optional (`0..20`, default `3`).

### `config_apply` (requires `--enable-mutations`)
Validate + compile candidate config content and optionally apply atomically.

Arguments:
```json
{
  "path": "./Hookaidofile",
  "content": "\"/webhooks/github\" { deliver \"https://ci.internal/build\" {} }\n",
  "mode": "preview_only",
  "reload_timeout": "5s"
}
```

Mode enum:
- `preview_only` (default): validate/compile only; no write
- `write_only`: atomic write to configured path after successful validate/compile
- `write_and_reload`: atomic write + bounded admin health check (`GET {admin_prefix}/healthz`), rollback to previous file on failed health check

Notes:
- `reload_timeout` is optional and only used by `write_and_reload` (default `5s`).
- Admin auth tokens are loaded for the health check; unresolved token refs fail `write_and_reload`.
- Token refs used by MCP health/reload flows support `env:`, `file:`, `vault:`, and `raw:` schemes via the shared secrets resolver.

### `admin_health`
Return local read-only snapshot (`config_readable`, `db_exists`, `db_readable`).

Arguments:
```json
{}
```

Notes:
- Includes local queue rollups in `queue` (`checked`, `ok`, `total`, `by_state`, `oldest_queued_received_at`, `oldest_queued_age_seconds`, `earliest_queued_next_run_at`, `ready_lag_seconds`, `top_queued`, `trend_signals`, `error`).
- Also probes configured Admin API health endpoint (`/healthz?details=1`) when config compiles successfully.
- Includes `admin_api` probe result (`checked`, `ok`, `status_code`, `error`) and optional diagnostics details payload.
- Passes through Admin diagnostics payloads (for example `diagnostics.publish` counters), including publish rejection diagnostics such as `rejected_managed_target_mismatch_total` and `rejected_managed_resolver_missing_total` when provided by Admin runtime metrics.
- Passes through ingress adaptive backpressure diagnostics when available (`diagnostics.ingress.adaptive_backpressure_applied_total` and `diagnostics.ingress.adaptive_backpressure_by_reason`).
- Passes through ingress rejection-by-reason diagnostics (`diagnostics.ingress.rejected_by_reason`) and memory-store pressure diagnostics (`diagnostics.store.memory_pressure`, retained bytes/items, eviction counters) when present in Admin health details.
- Includes MCP-local Admin-proxy publish rollback counters under `mcp.admin_proxy_publish` (`rollback_attempts_total`, `rollback_succeeded_total`, `rollback_failed_total`, `rollback_ids_total`).
- `trend_signals` evaluation uses compiled `defaults.trend_signals` policy when available; otherwise built-in defaults.
- `trend_signals` include `operator_actions` playbooks with stable action IDs, severity, alert-routing keys, and suggested Admin/MCP operations.

### `management_model`
Return compiled management projection for routes with management labels.

Arguments:
```json
{}
```

Notes:
- Includes `applications[]` with endpoint entries (`name`, `route`, `mode`, `targets`, `publish_policy`).
- `publish_policy` contains route-level publish controls (`enabled`, `direct_enabled`, `managed_enabled`).
- Uses route labels `application` + `endpoint_name`; unlabeled routes are omitted.

Queue tool backend behavior:
- When compiled `queue_backend` is `sqlite`, MCP queue tools use direct SQLite access.
- When compiled `queue_backend` is `memory`, MCP queue tools proxy the configured Admin API endpoints.
- Memory backend proxy mode requires a running Hookaido instance that serves the Admin API.
- Worker lease operations are an explicit MCP non-goal for v1.x: MCP does not expose dequeue/ack/nack/extend endpoints (including Pull API batch ack/nack via `lease_ids`), and lease control remains on Pull/Worker runtime transports.
- Coverage decision: Pull API lease-retry idempotency (`ack`/`nack` duplicate success window) remains Pull runtime behavior only; no MCP tool surface changes required.
- In proxy mode, queue read (`GET`) calls use bounded retries for transient failures (`408/429/5xx` and transport errors).
- Proxy errors map common Admin statuses (`400/401/403/404/409/429/503`) to explicit MCP error messages and include structured response metadata when available (`code`, `item_index`, `detail`).
- Mutation tools that accept audit metadata enforce Admin-aligned limits: `reason` max `512` chars, `actor` max `256`, `request_id` max `256`.
- For mutating tools, `actor` is principal-authoritative: when omitted it is derived from MCP `--principal`; when provided it must exactly match `--principal` before local mutation/audit handling and Admin-proxy forwarding (when applicable).

Managed selector label format:
- Any tool argument using `application` and/or `endpoint_name` requires labels matching `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`; invalid labels are rejected before SQLite/Admin-proxy calls.

### `backlog_top_queued`
Return bounded top queued route/target backlog buckets from queue stats.

Arguments:
```json
{
  "route": "/webhooks/github",
  "target": "pull",
  "limit": 100
}
```

Notes:
- All arguments are optional.
- `route` (when provided) must be an absolute Hookaido route path (starts with `/`).
- `limit` defaults to `100` and is capped at `1000`.
- Returns queue-level and per-bucket queued age/lag indicators when available.
- Includes `truncated`, `source_bounded`, `source_bucket_n`, and selector metadata to signal bounded-source behavior.

### `backlog_oldest_queued`
Return oldest queued messages ordered by `received_at` ascending.

Arguments:
```json
{
  "route": "/webhooks/github",
  "target": "pull",
  "limit": 100
}
```

Notes:
- All arguments are optional.
- `route` (when provided) must be an absolute Hookaido route path (starts with `/`).
- `limit` defaults to `100` and is capped at `1000`.
- Returns queue-level queued age/lag indicators and per-item queued age/ready lag when available.
- Includes `truncated`, `returned`, selector metadata, and fixed ordering metadata (`state=queued`, `order=received_at_asc`).

### `backlog_aging_summary`
Return route/target backlog aging summaries from oldest-first scans across selected states.

Arguments:
```json
{
  "route": "/webhooks/github",
  "target": "pull",
  "limit": 100,
  "states": ["queued", "leased", "dead"]
}
```

Notes:
- All arguments are optional.
- `route` (when provided) must be an absolute Hookaido route path (starts with `/`).
- `limit` defaults to `100` and is capped at `1000`.
- `states` defaults to `["queued","leased","dead"]`; allowed values are `queued`, `leased`, `dead`.
- Source scan is oldest-first (`received_at asc`) per selected state, then aggregated by `route+target`.
- Includes `states`, `state_scan`, `scanned`, `bucket_count`, `truncated`, `source_bounded`, selector metadata, age windows (`age_windows`), age percentiles (`age_percentiles_seconds` with `p50/p90/p99`), and per-bucket overdue/age indicators plus `state_counts`.

### `backlog_trends`
Return persisted backlog trend rollups from runtime-captured snapshots.

Arguments:
```json
{
  "route": "/webhooks/github",
  "target": "pull",
  "window": "2h",
  "step": "5m",
  "until": "2026-02-07T12:00:00Z"
}
```

Notes:
- All arguments are optional.
- `route` (when provided) must be an absolute Hookaido route path (starts with `/`).
- `window` defaults to `1h` (max `168h`).
- `step` defaults to `5m` (min `1m`, max `1h`), and must be `<= window`.
- `until` defaults to current UTC time when omitted.
- Runtime captures snapshots periodically (`1m`) and persists them in the queue DB.
- Includes fixed rollup buckets with `queued_last`/`leased_last`/`dead_last`, `*_max`, `sample_count`, and top-level trend summary (`latest_total`, `max_total`).
- Includes derived `signals` for sustained growth/surge/dead-share/queued-pressure/stale-sampling indicators over the selected window.
- Derived `signals` thresholds and staleness policy are driven by compiled `defaults.trend_signals` values.
- Derived `signals` include `operator_actions` playbooks with stable action IDs, severity, alert-routing keys, and suggested Admin/MCP operations.

### `management_endpoint_upsert` (requires `--enable-mutations`)
Upsert application/endpoint mapping onto an existing route in Hookaidofile.

Arguments:
```json
{
  "path": "./Hookaidofile",
  "application": "billing",
  "endpoint_name": "invoice.created",
  "route": "/webhooks/billing",
  "reason": "oncall_rewire",
  "actor": "ops@example.test",
  "request_id": "req-123",
  "mode": "write_only",
  "reload_timeout": "5s"
}
```

Notes:
- `application`, `endpoint_name`, `route`, and `reason` are required.
- `path` is optional and, when set, must match configured `--config`.
- `application` and `endpoint_name` must match `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`.
- `route` must be an absolute Hookaido route path (starts with `/`).
- `reason` aligns with Admin API audit-reason semantics (`X-Hookaido-Audit-Reason`).
- `defaults.publish_policy.require_request_id on` requires `request_id`.
- Scoped managed actor policy hooks (`defaults.publish_policy.actor_allow` / `actor_prefix`) are enforced; actor (derived from principal when omitted) must match allowlist/prefix policy.
- Target route must allow managed publish (`route.publish on` and `route.publish.managed on`) or upsert is rejected.
- When moving an existing endpoint mapping to another route, target publish profile (mode + targets) must match the current route; drift is rejected with `management_route_target_mismatch`.
- When runtime queue context is available, moves are rejected while the current mapped route still has active `queued`/`leased` backlog (`management_route_backlog_active`):
  - `queue.backend sqlite`: checked from local SQLite queue (`--db`).
  - `queue.backend memory`: checked via Admin API proxy `GET /messages` state probes.
  - If runtime queue context is unavailable (for example offline config mutation), this guardrail is best-effort and may not trigger.
- `mode` defaults to `write_only`; supported values: `write_only`, `write_and_reload`, `preview_only`.
- Route must exist in compiled config.
- Conflicts when target route is already mapped to a different application/endpoint.
- Local mutation failures return normalized structured error text (`code=...`, `detail=...`), including `management_route_not_found`, `management_route_already_mapped`, `management_route_publish_disabled`, `management_route_target_mismatch`, and `management_route_backlog_active`.

### `management_endpoint_delete` (requires `--enable-mutations`)
Delete application/endpoint mapping labels from Hookaidofile.

Arguments:
```json
{
  "path": "./Hookaidofile",
  "application": "billing",
  "endpoint_name": "invoice.created",
  "reason": "decommission",
  "actor": "ops@example.test",
  "request_id": "req-456",
  "mode": "write_only",
  "reload_timeout": "5s"
}
```

Notes:
- `application`, `endpoint_name`, and `reason` are required.
- `path` is optional and, when set, must match configured `--config`.
- `application` and `endpoint_name` must match `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`.
- `reason` aligns with Admin API audit-reason semantics (`X-Hookaido-Audit-Reason`).
- `defaults.publish_policy.require_request_id on` requires `request_id`.
- Scoped managed actor policy hooks (`defaults.publish_policy.actor_allow` / `actor_prefix`) are enforced; actor (derived from principal when omitted) must match allowlist/prefix policy.
- `mode` defaults to `write_only`; supported values: `write_only`, `write_and_reload`, `preview_only`.
- When runtime queue context is available, delete is rejected while the current mapped route still has active `queued`/`leased` backlog (`management_route_backlog_active`) with the same backend rules as `management_endpoint_upsert`.
- Returns an error if the management endpoint mapping does not exist.
- Local mutation failures return normalized structured error text (`code=...`, `detail=...`), including `management_endpoint_not_found` when the mapping does not exist and `management_route_backlog_active` when active backlog guardrails block the delete.

### `messages_list`
List queue messages with optional filters.

Arguments:
```json
{
  "route": "/webhooks/github",
  "application": "billing",
  "endpoint_name": "invoice.created",
  "target": "pull",
  "state": "queued",
  "limit": 100,
  "before": "2026-02-06T12:00:00Z",
  "include_payload": false,
  "include_headers": false,
  "include_trace": false
}
```

Notes:
- Route selector can be either:
  - `route`, or
  - `application` + `endpoint_name` (not both together with `route`).
- `route` (when provided) must be an absolute Hookaido route path (starts with `/`).
- In Admin-proxy mode, managed selectors are resolved onto endpoint-scoped Admin list path (`/applications/{application}/endpoints/{endpoint_name}/messages`); selector hints are not forwarded as query params.
- In Admin-proxy mode, structured Admin selector/ownership/target-alignment errors are surfaced as MCP tool errors (`code=... detail=...`), including `managed_endpoint_not_found` (`404`) and `managed_target_mismatch` (`503`) with fallback detail when Admin omits `detail`.

State enum:
- `queued`
- `leased`
- `delivered`
- `dead`
- `canceled`

### `attempts_list`
List delivery attempts with optional filters.

Arguments:
```json
{
  "route": "/webhooks/github",
  "application": "billing",
  "endpoint_name": "invoice.created",
  "target": "https://ci.internal/build",
  "event_id": "evt_1",
  "outcome": "retry",
  "limit": 100,
  "before": "2026-02-06T12:00:00Z"
}
```

Notes:
- Route selector can be either:
  - `route`, or
  - `application` + `endpoint_name` (not both together with `route`).
- `route` (when provided) must be an absolute Hookaido route path (starts with `/`).
- In Admin-proxy mode, managed selectors are forwarded to Admin `/attempts` as `application` + `endpoint_name` query params (no local route-hint rewrite).
- In Admin-proxy mode, structured Admin selector/ownership errors are surfaced as MCP tool errors (`code=... detail=...`), including fallback detail mapping when Admin omits `detail` (for example `managed_endpoint_not_found`, `managed_target_mismatch`).

Outcome enum:
- `acked`
- `retry`
- `dead`

### `dlq_list`
List dead-letter items with optional filters.

Arguments:
```json
{
  "route": "/webhooks/github",
  "limit": 100,
  "before": "2026-02-06T12:00:00Z",
  "include_payload": false,
  "include_headers": false,
  "include_trace": false
}
```

Notes:
- `route` (when provided) must be an absolute Hookaido route path (starts with `/`).

### `dlq_requeue` (requires `--enable-mutations`)
Requeue dead-letter items by ID.

Arguments:
```json
{
  "reason": "operator_cleanup",
  "actor": "ops@example.test",
  "request_id": "req-123",
  "ids": ["evt_1", "evt_2"]
}
```

Notes:
- `reason` is required and aligns with Admin API audit-reason semantics (`X-Hookaido-Audit-Reason`).
- In SQLite mode, when targeted IDs resolve to managed routes, endpoint-scoped audit identity policy hooks are enforced (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`).
- When `defaults.publish_policy.fail_closed` is enabled, unresolved managed-policy context (for example config compile unavailable) fails closed before ID mutation.
- In Admin-proxy mode, structured Admin error payloads are surfaced in tool errors with explicit `code`/`detail` text when provided (including ownership-drift failures as `code=managed_target_mismatch`).

### `dlq_delete` (requires `--enable-mutations`)
Delete dead-letter items by ID.

Arguments:
```json
{
  "reason": "operator_cleanup",
  "actor": "ops@example.test",
  "request_id": "req-124",
  "ids": ["evt_1", "evt_2"]
}
```

Notes:
- `reason` is required and aligns with Admin API audit-reason semantics (`X-Hookaido-Audit-Reason`).
- In SQLite mode, when targeted IDs resolve to managed routes, endpoint-scoped audit identity policy hooks are enforced (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`).
- When `defaults.publish_policy.fail_closed` is enabled, unresolved managed-policy context (for example config compile unavailable) fails closed before ID mutation.
- In Admin-proxy mode, structured Admin error payloads are surfaced in tool errors with explicit `code`/`detail` text when provided (including ownership-drift failures as `code=managed_target_mismatch`).

### `messages_cancel` (requires `--enable-mutations`)
Cancel queue messages by ID (`queued|leased|dead -> canceled`).

Arguments:
```json
{
  "reason": "operator_cleanup",
  "actor": "ops@example.test",
  "request_id": "req-125",
  "ids": ["evt_1", "evt_2"]
}
```

Notes:
- `reason` is required and aligns with Admin API audit-reason semantics (`X-Hookaido-Audit-Reason`).
- In SQLite mode, when targeted IDs resolve to managed routes, endpoint-scoped audit identity policy hooks are enforced (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`).
- When `defaults.publish_policy.fail_closed` is enabled, unresolved managed-policy context (for example config compile unavailable) fails closed before ID mutation.
- In Admin-proxy mode, structured Admin error payloads are surfaced in tool errors with explicit `code`/`detail` text when provided (including ownership-drift failures as `code=managed_target_mismatch`).

### `messages_requeue` (requires `--enable-mutations`)
Requeue queue messages by ID (`dead|canceled -> queued`).

Arguments:
```json
{
  "reason": "operator_cleanup",
  "actor": "ops@example.test",
  "request_id": "req-126",
  "ids": ["evt_1", "evt_2"]
}
```

Notes:
- `reason` is required and aligns with Admin API audit-reason semantics (`X-Hookaido-Audit-Reason`).
- In SQLite mode, when targeted IDs resolve to managed routes, endpoint-scoped audit identity policy hooks are enforced (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`).
- When `defaults.publish_policy.fail_closed` is enabled, unresolved managed-policy context (for example config compile unavailable) fails closed before ID mutation.
- In Admin-proxy mode, structured Admin error payloads are surfaced in tool errors with explicit `code`/`detail` text when provided (including ownership-drift failures as `code=managed_target_mismatch`).

### `messages_resume` (requires `--enable-mutations`)
Resume canceled queue messages by ID (`canceled -> queued`).

Arguments:
```json
{
  "reason": "operator_cleanup",
  "actor": "ops@example.test",
  "request_id": "req-127",
  "ids": ["evt_1", "evt_2"]
}
```

Notes:
- `reason` is required and aligns with Admin API audit-reason semantics (`X-Hookaido-Audit-Reason`).
- In SQLite mode, when targeted IDs resolve to managed routes, endpoint-scoped audit identity policy hooks are enforced (`require_actor`, `require_request_id`, `actor_allow`, `actor_prefix`).
- When `defaults.publish_policy.fail_closed` is enabled, unresolved managed-policy context (for example config compile unavailable) fails closed before ID mutation.
- In Admin-proxy mode, structured Admin error payloads are surfaced in tool errors with explicit `code`/`detail` text when provided (including ownership-drift failures as `code=managed_target_mismatch`).

### `messages_publish` (requires `--enable-mutations`)
Publish queued messages.

Arguments:
```json
{
  "reason": "operator_publish",
  "actor": "ops@example.test",
  "request_id": "req-789",
  "items": [
    {
      "id": "evt_1",
      "route": "/webhooks/github",
      "target": "pull",
      "payload_b64": "eyJvayI6dHJ1ZX0=",
      "received_at": "2026-02-06T12:00:00Z",
      "next_run_at": "2026-02-06T12:00:05Z",
      "headers": { "X-Event": "push" },
      "trace": { "trace_id": "abc" }
    }
  ]
}
```

Notes:
- `reason` is required and aligns with Admin API audit-reason semantics (`X-Hookaido-Audit-Reason`).
- Each item must provide `id`, plus either:
  - `route` (and optionally `target`), or
  - `application` + `endpoint_name` (and optionally `target`).
- Selector modes are exclusive: `route` is rejected when `application` + `endpoint_name` is provided.
- Selector mode is request-scoped: all items must use the same selector mode (`route` for every item, or `application` + `endpoint_name` for every item).
- `route` (when provided) must be an absolute Hookaido route path (starts with `/`) and must resolve in compiled config.
- If a `route` is management-labeled (`application` + `endpoint_name` in config), publish must use those managed selectors instead of route-only publish.
- Decoded `payload_b64` bytes must respect the resolved route `max_body` limit, and item headers must respect the resolved route `max_headers` limit; oversize errors include bytes-vs-limit details.
- Item header names/values must be valid HTTP headers (field-name token syntax; values must not contain control characters like CR/LF); invalid headers are rejected before enqueue/proxy calls.
- Admin global publish policy is direct-route only (`POST /messages/publish` rejects managed selectors with `scoped_publish_required`).
- When Admin global direct publish route-target resolution is unavailable, Admin returns `code=route_resolver_missing`; MCP Admin-proxy error mapping preserves that code with normalized fallback detail text.
- When Admin endpoint-scoped managed operations detect target/ownership drift vs route policy resolution, Admin returns `code=managed_target_mismatch`; MCP Admin-proxy error mapping preserves that code with normalized fallback detail text.
- MCP enforces compiled `defaults.publish_policy` locally before enqueue/proxy calls:
  - `direct off` rejects direct route publish items.
  - `managed off` rejects managed selector publish items.
  - `allow_pull_routes off` rejects publish items resolved onto pull routes.
  - `allow_deliver_routes off` rejects publish items resolved onto deliver routes.
  - `require_actor on` requires `actor` on `messages_publish`.
  - `require_request_id on` requires `request_id` on `messages_publish`.
  - `actor_allow` / `actor_prefix` require `actor` for managed selector publish items and enforce actor allowlist/prefix matching.
- Route-level publish directives also reject `messages_publish` for that route (in both SQLite mode and Admin-proxy mode):
  - `publish off` rejects all manual publish paths.
  - `publish.direct off` rejects direct route publish path.
  - `publish.managed off` rejects managed endpoint-scoped publish path.
- If `target` is omitted, it is auto-selected only when the resolved route has exactly one target.
- When `target` is missing for a multi-target route (or mismatched), errors include the resolved route's allowed targets using Admin-aligned `item.target ...` detail wording.
- Duplicate IDs (already present in queue) fail the call with an explicit item-scoped error.
- In direct SQLite mode, item validation/policy/target checks and existing duplicate-ID detection are preflighted across the batch before enqueueing, so validation/policy/duplicate failures do not partially enqueue earlier items.
- In Admin-proxy mode, managed selector items are sent to endpoint-scoped Admin publish routes (`/applications/{application}/endpoints/{endpoint_name}/messages/publish`) instead of global `/messages/publish`.
- In Admin-proxy mode, MCP preflights full item validation/policy/target checks across the full request before issuing the first Admin call, so local validation/policy errors do not produce partial cross-batch proxy publish side effects.
- Endpoint-scoped Admin publish rejects selector hints in item bodies; MCP proxy mode therefore strips `route`/`application`/`endpoint_name` from scoped publish payload items.
- In Admin-proxy mode, structured Admin publish errors are surfaced in MCP error text with `code` and optional `item_index`.
- Admin-proxy error text is normalized to `code=...`, optional `item_index=...`, `detail=...`; when Admin omits `detail`, MCP applies code-based fallback detail text (including specific mapping conflict classes such as `management_route_already_mapped`, `management_route_publish_disabled`, and `management_route_target_mismatch`).
- When Admin-proxy publish is internally split into multiple batches (for example multiple endpoint-scoped batches), surfaced `item_index` values are normalized back to the original input item order.
- When a later internal Admin-proxy publish batch fails, MCP issues a best-effort rollback (`POST /messages/cancel`) for IDs published by earlier successful batches and, when Admin returns `item_index`, for the failed-batch item prefix before that index; the original publish error remains the surfaced tool error.
- Successful mutation responses include an `audit` object echoing `reason`, optional `actor`/`request_id`, and MCP-bound `principal`.
- Unknown top-level arguments and unknown per-item keys are rejected (strict argument surface).

### `messages_cancel_by_filter` (requires `--enable-mutations`)
Cancel queue messages by filter (`queued|leased|dead -> canceled`).

Arguments:
```json
{
  "reason": "operator_cleanup",
  "actor": "ops@example.test",
  "request_id": "req-128",
  "route": "/webhooks/github",
  "application": "billing",
  "endpoint_name": "invoice.created",
  "target": "pull",
  "state": "queued",
  "before": "2026-02-07T12:00:00Z",
  "limit": 100,
  "preview_only": true
}
```

Notes:
- `reason` is required and aligns with Admin API audit-reason semantics (`X-Hookaido-Audit-Reason`).
- Route selector can be either `route` or `application` + `endpoint_name` (not both together).
- `route` (when provided) must be an absolute Hookaido route path (starts with `/`).
- For managed selectors (`application` + `endpoint_name`), MCP enforces endpoint-scoped audit identity policy locally before SQLite/Admin-proxy calls:
  - `require_actor on` requires `actor`.
  - `require_request_id on` requires `request_id`.
  - `actor_allow` / `actor_prefix` require `actor` and enforce allowlist/prefix matching (`audit_actor_not_allowed` on mismatch).
- `route` selectors that resolve to management-labeled routes, and unscoped selectors when managed routes exist, are rejected and must use `application` + `endpoint_name`.
- For `route`/unscoped selectors, if managed-route selector context cannot be evaluated from config, MCP fails closed only when `defaults.publish_policy.fail_closed on`; otherwise filtering remains fail-open.
- `state` is optional and restricted to `queued|leased|dead`.
- Default `limit` is `100` (max `1000`).
- `preview_only: true` returns `matched` count without mutating queue state.
- In Admin-proxy mode, managed selectors are resolved onto endpoint-scoped Admin mutation path (`/applications/{application}/endpoints/{endpoint_name}/messages/cancel_by_filter`); selector hints are not forwarded in body payload.
- In Admin-proxy mode, `route` selectors keep the global Admin mutation path only for non-managed routes; management-labeled routes require `application` + `endpoint_name`, and unscoped selectors are rejected when managed routes exist.
- Unknown arguments are rejected (strict argument surface).
- In Admin-proxy mode, structured Admin error payloads are surfaced in tool errors with explicit `code`/`detail` text when provided.
- In Admin-proxy mode, structured selector errors also preserve `managed_endpoint_not_found` (`404`) with fallback detail when Admin omits `detail`.
- In Admin-proxy mode, route-selector ownership-source drift failures from Admin are surfaced as `code=managed_target_mismatch` (with fallback detail when Admin omits `detail`).

### `messages_requeue_by_filter` (requires `--enable-mutations`)
Requeue queue messages by filter (`dead|canceled -> queued`).

Arguments:
```json
{
  "reason": "operator_cleanup",
  "actor": "ops@example.test",
  "request_id": "req-129",
  "route": "/webhooks/github",
  "application": "billing",
  "endpoint_name": "invoice.created",
  "target": "pull",
  "state": "dead",
  "before": "2026-02-07T12:00:00Z",
  "limit": 100,
  "preview_only": true
}
```

Notes:
- `reason` is required and aligns with Admin API audit-reason semantics (`X-Hookaido-Audit-Reason`).
- Route selector can be either `route` or `application` + `endpoint_name` (not both together).
- `route` (when provided) must be an absolute Hookaido route path (starts with `/`).
- For managed selectors (`application` + `endpoint_name`), MCP enforces endpoint-scoped audit identity policy locally before SQLite/Admin-proxy calls:
  - `require_actor on` requires `actor`.
  - `require_request_id on` requires `request_id`.
  - `actor_allow` / `actor_prefix` require `actor` and enforce allowlist/prefix matching (`audit_actor_not_allowed` on mismatch).
- `route` selectors that resolve to management-labeled routes, and unscoped selectors when managed routes exist, are rejected and must use `application` + `endpoint_name`.
- For `route`/unscoped selectors, if managed-route selector context cannot be evaluated from config, MCP fails closed only when `defaults.publish_policy.fail_closed on`; otherwise filtering remains fail-open.
- `state` is optional and restricted to `dead|canceled`.
- Default `limit` is `100` (max `1000`).
- `preview_only: true` returns `matched` count without mutating queue state.
- In Admin-proxy mode, managed selectors are resolved onto endpoint-scoped Admin mutation path (`/applications/{application}/endpoints/{endpoint_name}/messages/requeue_by_filter`); selector hints are not forwarded in body payload.
- In Admin-proxy mode, `route` selectors keep the global Admin mutation path only for non-managed routes; management-labeled routes require `application` + `endpoint_name`, and unscoped selectors are rejected when managed routes exist.
- Unknown arguments are rejected (strict argument surface).
- In Admin-proxy mode, structured Admin error payloads are surfaced in tool errors with explicit `code`/`detail` text when provided.
- In Admin-proxy mode, structured selector errors also preserve `managed_endpoint_not_found` (`404`) with fallback detail when Admin omits `detail`.
- In Admin-proxy mode, route-selector ownership-source drift failures from Admin are surfaced as `code=managed_target_mismatch` (with fallback detail when Admin omits `detail`).

### `messages_resume_by_filter` (requires `--enable-mutations`)
Resume queue messages by filter (`canceled -> queued`).

Arguments:
```json
{
  "reason": "operator_cleanup",
  "actor": "ops@example.test",
  "request_id": "req-130",
  "route": "/webhooks/github",
  "application": "billing",
  "endpoint_name": "invoice.created",
  "target": "pull",
  "state": "canceled",
  "before": "2026-02-06T12:00:00Z",
  "limit": 100,
  "preview_only": false
}
```

Notes:
- `reason` is required and aligns with Admin API audit-reason semantics (`X-Hookaido-Audit-Reason`).
- Route selector can be either `route` or `application` + `endpoint_name` (not both together).
- `route` (when provided) must be an absolute Hookaido route path (starts with `/`).
- For managed selectors (`application` + `endpoint_name`), MCP enforces endpoint-scoped audit identity policy locally before SQLite/Admin-proxy calls:
  - `require_actor on` requires `actor`.
  - `require_request_id on` requires `request_id`.
  - `actor_allow` / `actor_prefix` require `actor` and enforce allowlist/prefix matching (`audit_actor_not_allowed` on mismatch).
- `route` selectors that resolve to management-labeled routes, and unscoped selectors when managed routes exist, are rejected and must use `application` + `endpoint_name`.
- For `route`/unscoped selectors, if managed-route selector context cannot be evaluated from config, MCP fails closed only when `defaults.publish_policy.fail_closed on`; otherwise filtering remains fail-open.
- `state` is optional and restricted to `canceled`.
- `preview_only: true` returns `matched` count without mutating queue state.
- In Admin-proxy mode, managed selectors are resolved onto endpoint-scoped Admin mutation path (`/applications/{application}/endpoints/{endpoint_name}/messages/resume_by_filter`); selector hints are not forwarded in body payload.
- In Admin-proxy mode, `route` selectors keep the global Admin mutation path only for non-managed routes; management-labeled routes require `application` + `endpoint_name`, and unscoped selectors are rejected when managed routes exist.
- Unknown arguments are rejected (strict argument surface).
- In Admin-proxy mode, structured Admin error payloads are surfaced in tool errors with explicit `code`/`detail` text when provided.
- In Admin-proxy mode, structured selector errors also preserve `managed_endpoint_not_found` (`404`) with fallback detail when Admin omits `detail`.
- In Admin-proxy mode, route-selector ownership-source drift failures from Admin are surfaced as `code=managed_target_mismatch` (with fallback detail when Admin omits `detail`).

### `instance_start` (requires `--enable-runtime-control`)
Start `hookaido run` as a subprocess using configured `--config`, `--db`, and `--pid-file`, then verify admin health.

Arguments:
```json
{
  "pid_file": "./hookaido.pid",
  "timeout": "10s"
}
```

### `instance_status` (requires `--enable-runtime-control`)
Return process/config status derived from pid file and compiled config; when running, probe admin health.

Arguments:
```json
{
  "pid_file": "./hookaido.pid",
  "timeout": "2s"
}
```

Notes:
- `summary.queue_backend` reports the compiled runtime queue backend (`sqlite`, `memory`, or `mixed` when config is invalid).
- `summary.publish_policy_direct_enabled`, `summary.publish_policy_managed_enabled`, `summary.publish_policy_allow_pull_routes`, `summary.publish_policy_allow_deliver_routes`, `summary.publish_policy_require_actor`, `summary.publish_policy_require_request_id`, `summary.publish_policy_fail_closed`, `summary.publish_policy_actor_allowlist`, and `summary.publish_policy_actor_prefixes` report compiled publish-path policy settings from `defaults.publish_policy`.

### `instance_logs_tail` (requires `--enable-runtime-control`)
Tail runtime logs from `observability.runtime_log` file sink.

Arguments:
```json
{
  "pid_file": "./hookaido.pid",
  "max_lines": 200,
  "max_bytes": 65536
}
```

### `instance_stop` (requires `--enable-runtime-control`)
Stop a running `hookaido run` process via pid file (`SIGTERM`, optional `SIGKILL` fallback).

Arguments:
```json
{
  "pid_file": "./hookaido.pid",
  "timeout": "10s",
  "force": true
}
```

Notes:
- On Windows, stop signals are mapped to process termination semantics.

### `instance_reload` (requires `--enable-runtime-control`)
Reload a running `hookaido run` process via `SIGHUP` and verify admin health.

Arguments:
```json
{
  "pid_file": "./hookaido.pid",
  "timeout": "5s"
}
```

Notes:
- Requires runtime `SIGHUP` support. On Windows, `instance_reload` returns an explicit unsupported-signal error.

## Guardrails (Implemented)

- Mutation tools are disabled by default and only enabled with `--enable-mutations`.
- Runtime control tools are disabled by default and only enabled with `--enable-runtime-control`.
- Tool-level authorization is enforced server-side via `--role`:
  - `read`: inspect-only tools
  - `operate`: adds safe queue mutations + runtime inspect tools
  - `admin`: required for config-lifecycle mutations (`config_apply`, `management_endpoint_*`) and runtime process control (`instance_start|stop|reload`)
- Mutating tool calls require a non-empty `--principal` identity.
- Mutation tools use strict argument allowlists: unknown top-level arguments are rejected (and `messages_publish` additionally rejects unknown per-item keys).
- Mutating tool calls emit structured JSONL audit events to stderr with:
  - `timestamp`, `principal`, `role`, `tool`, `input_hash`, `result`, `duration_ms` (and `error` when applicable)
  - Config-lifecycle mutation tools (`config_apply`, `management_endpoint_upsert`, `management_endpoint_delete`) include `metadata.config_mutation` (`operation`, `mode`, path/selector fields, and apply outcome flags such as `ok`, `applied`, `reloaded`, `rolled_back` when available).
  - Runtime process-control tools (`instance_start`, `instance_stop`, `instance_reload`) include `metadata.runtime_control` (`operation`, pid/timeout context, and outcome flags such as `started|stopped|reloaded`, `already_running|already_stopped`, `signaled` when available).
  - ID-based queue mutation tools (`dlq_requeue`, `dlq_delete`, `messages_cancel`, `messages_requeue`, `messages_resume`) include `metadata.id_mutation` (`operation`, `changed_field`, `ids_requested`, `ids_unique`, `changed` when available).
  - `messages_publish` also includes `metadata.admin_proxy_publish` rollback counters (`rollback_attempts`, `rollback_succeeded`, `rollback_failed`, `rollback_ids`) plus running totals (`*_total`) for Admin-proxy publish rollback observability.
  - `messages_*_by_filter` also include `metadata.filter_mutation` (`operation`, `changed_field`, `matched`, `changed`, `preview_only`) for mutation-result observability.
- Admin-proxy endpoint allowlist is supported via `--admin-endpoint-allowlist` (comma-separated `host:port` or URL-prefix entries).
- Config path allowlist: only configured `--config` is accepted.
- DB path allowlist: only configured `--db` is used.
- PID file allowlist: only configured `--pid-file` is accepted.
- Runtime log tail is constrained by `max_lines <= 1000` and `max_bytes <= 1MiB`.
- List limits capped at `1000`.
- DB file must exist for queue tools.
- `ids` mutation arguments must be non-empty string arrays (max `1000`, deduplicated).
