# Admin API

The Admin API provides health checks, queue inspection, DLQ management, message lifecycle operations, backlog analytics, and management model endpoints.

## Base URL

```
{admin_api.listen} + {admin_api.prefix}
```

Default: `http://127.0.0.1:2019`

Optional bearer token authentication:

```hcl
admin_api {
  listen 127.0.0.1:2019
  auth token env:HOOKAIDO_ADMIN_TOKEN
}
```

## Health

### `GET /healthz`

Returns `200` when the instance is healthy.

### `GET /healthz?details=1`

Returns detailed JSON diagnostics:

```json
{
  "ok": true,
  "time": "2026-02-12T20:15:00Z",
  "diagnostics": {
    "queue": {
      "total": 1523,
      "by_state": { "queued": 1200, "leased": 300, "dead": 23 },
      "oldest_queued_received_at": "2026-02-09T08:00:00Z",
      "oldest_queued_age_seconds": 7200,
      "ready_lag_seconds": 0,
      "top_queued": [...],
      "trend_signals": {
        "signals": [...],
        "operator_actions": [...]
      }
    },
    "ingress": {
      "accepted_total": 100000,
      "rejected_total": 4123,
      "rejected_by_reason": {
        "memory_pressure": 12,
        "queue_full": 41,
        "adaptive_backpressure": 345
      },
      "adaptive_backpressure_applied_total": 345,
      "adaptive_backpressure_by_reason": {
        "queued_pressure": 221,
        "ready_lag": 79,
        "oldest_queued_age": 31,
        "sustained_growth": 14
      }
    },
    "store": {
      "backend": "memory",
      "items_by_state": { "queued": 1200, "leased": 300, "delivered": 4000, "dead": 23 },
      "retained_bytes_total": 91234567,
      "memory_pressure": {
        "active": false,
        "reason": "",
        "retained_item_limit": 10000,
        "retained_bytes_limit": 268435456,
        "rejected_total": 12
      }
    }
  }
}
```

The health endpoint includes:

- Queue state rollups with age/lag indicators
- Top route/target backlog buckets
- Persisted trend signals with operator action playbooks
- Ingress counters, rejection-by-reason counters, and adaptive-backpressure diagnostics (when runtime metrics are enabled)
- Memory-store runtime diagnostics (`store.*`) when queue backend is `memory`
- Tracing counters (when metrics are enabled)

## Queue Reads

### `GET /messages`

List queue messages across states.

| Parameter         | Type  | Description                                                                           |
| ----------------- | ----- | ------------------------------------------------------------------------------------- |
| `route`           | query | Filter by route path (must start with `/`). Mutually exclusive with managed selectors |
| `application`     | query | Filter by application label (managed selector)                                        |
| `endpoint_name`   | query | Filter by endpoint name (managed selector)                                            |
| `target`          | query | Filter by delivery target                                                             |
| `state`           | query | Filter by state: `queued`, `leased`, `delivered`, `dead`, `canceled`                  |
| `limit`           | query | Max results (default 100, max 1000)                                                   |
| `before`          | query | Only items before this timestamp (RFC 3339)                                           |
| `include_payload` | query | Include base64 payload in response                                                    |
| `include_headers` | query | Include headers in response                                                           |
| `include_trace`   | query | Include trace context in response                                                     |

Selector modes (`route` vs `application`+`endpoint_name`) are mutually exclusive.

### `GET /dlq`

List dead-letter queue items.

| Parameter         | Type  | Description                                 |
| ----------------- | ----- | ------------------------------------------- |
| `route`           | query | Filter by route path (must start with `/`)  |
| `limit`           | query | Max results (default 100, max 1000)         |
| `before`          | query | Only items before this timestamp (RFC 3339) |
| `include_payload` | query | Include base64 payload                      |
| `include_headers` | query | Include headers                             |
| `include_trace`   | query | Include trace context                       |

Response:

```json
{
  "items": [
    {
      "id": "evt_1",
      "route": "/webhooks/github",
      "target": "pull",
      "received_at": "2026-02-09T10:00:00Z",
      "attempt": 3,
      "dead_reason": "max_retries"
    }
  ]
}
```

### `GET /attempts`

List delivery attempts (push mode).

| Parameter       | Type  | Description                                                     |
| --------------- | ----- | --------------------------------------------------------------- |
| `route`         | query | Filter by route path. Mutually exclusive with managed selectors |
| `application`   | query | Filter by application label                                     |
| `endpoint_name` | query | Filter by endpoint name                                         |
| `target`        | query | Filter by delivery target URL                                   |
| `event_id`      | query | Filter by source event ID                                       |
| `outcome`       | query | Filter: `acked`, `retry`, `dead`                                |
| `limit`         | query | Max results (default 100, max 1000)                             |
| `before`        | query | Only items before this timestamp (RFC 3339)                     |

## Backlog Analytics

### `GET /backlog/top_queued`

Top queued route/target backlog buckets.

| Parameter | Type  | Description                           |
| --------- | ----- | ------------------------------------- |
| `route`   | query | Filter by route (must start with `/`) |
| `target`  | query | Filter by target                      |
| `limit`   | query | Max buckets (default 100, max 1000)   |

### `GET /backlog/oldest_queued`

Oldest queued messages ordered by `received_at` ascending.

| Parameter | Type  | Description                           |
| --------- | ----- | ------------------------------------- |
| `route`   | query | Filter by route (must start with `/`) |
| `target`  | query | Filter by target                      |
| `limit`   | query | Max results (default 100, max 1000)   |

### `GET /backlog/aging_summary`

Route/target backlog aging summaries with age-window distribution and percentiles.

| Parameter | Type  | Description                                                      |
| --------- | ----- | ---------------------------------------------------------------- |
| `route`   | query | Filter by route (must start with `/`)                            |
| `target`  | query | Filter by target                                                 |
| `states`  | query | Comma-separated: `queued`, `leased`, `dead` (default: all three) |
| `limit`   | query | Max buckets (default 100, max 1000)                              |

Response includes per-bucket `state_counts`, `age_windows`, `age_percentiles_seconds` (p50/p90/p99), and truncation metadata.

### `GET /backlog/trends`

Persisted backlog trend rollups over time.

| Parameter | Type  | Description                                         |
| --------- | ----- | --------------------------------------------------- |
| `route`   | query | Filter by route (must start with `/`)               |
| `target`  | query | Filter by target                                    |
| `window`  | query | Time window (default `1h`, max `168h`)              |
| `step`    | query | Bucket step size (default `5m`, min `1m`, max `1h`) |
| `until`   | query | End of window (RFC 3339, default: now)              |

Response includes per-bucket `queued_last`/`leased_last`/`dead_last`, `*_max`, `sample_count`, derived `signals`, and `operator_actions` playbooks.

## DLQ Management

### `POST /dlq/requeue`

Requeue dead-letter items back to the queue.

```json
{
  "ids": ["evt_1", "evt_2"]
}
```

### `POST /dlq/delete`

Permanently delete dead-letter items.

```json
{
  "ids": ["evt_1", "evt_2"]
}
```

## Message Lifecycle

### `POST /messages/publish`

Publish new messages directly into the queue (direct route mode only).

```json
{
  "items": [
    {
      "id": "evt_custom_1",
      "route": "/webhooks/github",
      "target": "pull",
      "payload_b64": "eyJoZWxsbyI6ICJ3b3JsZCJ9",
      "headers": { "Content-Type": "application/json" }
    }
  ]
}
```

**Required headers:**

- `X-Hookaido-Audit-Reason` — reason for the mutation (max 512 chars)
- `X-Hookaido-Audit-Actor` — optional, required when `publish_policy.require_actor on`
- `X-Request-ID` — optional, required when `publish_policy.require_request_id on`

**Constraints:**

- `route` must start with `/` and resolve to a configured route
- `target` is auto-resolved for single-target routes; required for multi-target routes
- Management-labeled routes are rejected on this global path — use the [endpoint-scoped publish](#endpoint-scoped-operations) instead
- Decoded payload must fit the route's `max_body`; headers must fit `max_headers`
- Header names/values must be valid HTTP headers
- Duplicate message IDs return `409`
- Batch is validated/preflighted before enqueue — no partial writes on failure

**Policy controls:**

- Can be disabled with `defaults.publish_policy.direct off`
- Route-mode restrictions via `allow_pull_routes`/`allow_deliver_routes`
- Per-route `publish off` or `publish.direct off` blocks publish

### `POST /messages/cancel`

Cancel queued/leased/dead items by ID.

```json
{
  "ids": ["evt_1", "evt_2"]
}
```

### `POST /messages/requeue`

Requeue dead/canceled items by ID.

```json
{
  "ids": ["evt_1", "evt_2"]
}
```

### `POST /messages/resume`

Resume canceled items by ID.

```json
{
  "ids": ["evt_1"]
}
```

### Filter Mutations

Bulk operations by filter criteria. Available for cancel, requeue, and resume:

- `POST /messages/cancel_by_filter`
- `POST /messages/requeue_by_filter`
- `POST /messages/resume_by_filter`

```json
{
  "route": "/webhooks/github",
  "target": "pull",
  "state": "dead",
  "before": "2026-02-09T00:00:00Z",
  "limit": 100,
  "preview_only": false
}
```

| Field           | Description                                                                      |
| --------------- | -------------------------------------------------------------------------------- |
| `route`         | Filter by route (must start with `/`). Mutually exclusive with managed selectors |
| `application`   | Filter by application label                                                      |
| `endpoint_name` | Filter by endpoint name                                                          |
| `target`        | Filter by target                                                                 |
| `state`         | Filter by state                                                                  |
| `before`        | Only items before this timestamp                                                 |
| `limit`         | Max items to mutate                                                              |
| `preview_only`  | If `true`, return `matched` count without mutating                               |

**Managed ownership enforcement:** Route selectors that resolve to management-labeled routes (and unscoped selectors when managed routes exist) are rejected — use `application` + `endpoint_name` selectors instead.

## Endpoint-Scoped Operations

For routes with [management labels](management-model.md), use endpoint-scoped paths:

```
/applications/{application}/endpoints/{endpoint_name}/messages
/applications/{application}/endpoints/{endpoint_name}/messages/publish
/applications/{application}/endpoints/{endpoint_name}/messages/cancel_by_filter
/applications/{application}/endpoints/{endpoint_name}/messages/requeue_by_filter
/applications/{application}/endpoints/{endpoint_name}/messages/resume_by_filter
```

These paths are **authoritative** — selector hints (`route`, `application`, `endpoint_name`) in request bodies/query params are rejected. The scope is determined entirely by the URL path.

Endpoint-scoped publish:

```json
{
  "items": [
    {
      "id": "evt_managed_1",
      "payload_b64": "eyJoZWxsbyI6ICJ3b3JsZCJ9",
      "headers": { "Content-Type": "application/json" }
    }
  ]
}
```

> Target, route, and scope are resolved from the endpoint mapping — do not include selector fields in items.

## Management Model

### `GET /management/model`

Returns the compiled application/endpoint projection for labeled routes.

```json
{
  "application_count": 2,
  "path_count": 5,
  "applications": [
    {
      "name": "billing",
      "endpoints": [
        {
          "name": "invoice.created",
          "route": "/webhooks/billing",
          "mode": "pull",
          "targets": ["pull"],
          "publish_policy": {
            "enabled": true,
            "direct_enabled": true,
            "managed_enabled": true
          }
        }
      ]
    }
  ]
}
```

### `GET /applications`

List all applications with endpoint counts.

### `GET /applications/{application}/endpoints`

List endpoints for one application.

### `GET /applications/{application}/endpoints/{endpoint_name}`

Get one endpoint projection (route, mode, targets, publish policy).

### `PUT /applications/{application}/endpoints/{endpoint_name}`

Upsert application/endpoint mapping onto a route.

```json
{
  "route": "/webhooks/billing"
}
```

The target route must:

- Start with `/`
- Have `publish on` and `publish.managed on`
- Not already be mapped to another endpoint

### `DELETE /applications/{application}/endpoints/{endpoint_name}`

Remove mapping labels from the currently mapped route.

> Both `PUT` and `DELETE` require `X-Hookaido-Audit-Reason` and perform atomic config write + reload with rollback on failure.

## Audit Requirements

All mutation endpoints require the `X-Hookaido-Audit-Reason` header (max 512 chars). Additional audit headers:

| Header                    | Required When                               |
| ------------------------- | ------------------------------------------- |
| `X-Hookaido-Audit-Reason` | Always (mutations)                          |
| `X-Hookaido-Audit-Actor`  | When `publish_policy.require_actor on`      |
| `X-Request-ID`            | When `publish_policy.require_request_id on` |

All mutations emit structured runtime audit log events.

## Error Responses

All errors return structured JSON:

```json
{
  "code": "invalid_body",
  "detail": "route must start with /",
  "item_index": 0
}
```

`item_index` is included for batch operations to identify the problematic item.

Common error codes:

| Code                         | Status | Description                            |
| ---------------------------- | ------ | -------------------------------------- |
| `invalid_body`               | 400    | Malformed request body                 |
| `invalid_query`              | 400    | Invalid query parameter                |
| `unauthorized`               | 401    | Missing/invalid auth token             |
| `not_found`                  | 404    | Route/endpoint/resource not found      |
| `method_not_allowed`         | 405    | HTTP method not supported              |
| `duplicate_id`               | 409    | Message ID already exists              |
| `queue_full`                 | 429    | Queue at `max_depth`                   |
| `route_publish_disabled`     | 403    | Route has `publish off`                |
| `global_publish_disabled`    | 403    | `publish_policy.direct off`            |
| `scoped_publish_disabled`    | 403    | `publish_policy.managed off`           |
| `managed_selector_required`  | 400    | Must use application/endpoint selector |
| `scoped_publish_required`    | 400    | Must use endpoint-scoped publish path  |
| `managed_endpoint_not_found` | 404    | Managed selector does not resolve      |
| `managed_resolver_missing`   | 503    | Resolver unavailable                   |
| `payload_too_large`          | 413    | Payload exceeds route `max_body`       |
| `headers_too_large`          | 413    | Headers exceed route `max_headers`     |
| `invalid_header`             | 400    | Header name/value not HTTP-safe        |
| `audit_actor_required`       | 400    | Missing `X-Hookaido-Audit-Actor`       |
| `audit_request_id_required`  | 400    | Missing `X-Request-ID`                 |
| `audit_actor_not_allowed`    | 403    | Actor not in scoped allowlist          |

> Mutation body parsing is strict: unknown JSON fields and trailing JSON documents are rejected with `400`.

---

← [Documentation Index](index.md)
