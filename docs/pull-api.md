# Pull API

The Pull API is Hookaido's consumer protocol. Internal workers connect to the Pull API to dequeue messages, process them, and acknowledge completion — all via outbound-only HTTP calls.

If your workers prefer gRPC transport, see [Worker gRPC API](worker-api.md). It reuses the same runtime semantics and auth model.

## Base URL

The full endpoint for a pull route is:

```
{pull_api.listen} + {pull_api.prefix} + {pull.path}
```

For example, with:

```hcl
pull_api {
  listen :9443
  prefix /pull
  auth token env:HOOKAIDO_PULL_TOKEN
}

/webhooks/github {
  pull { path /github }
}
```

The endpoints become:

- `POST http://localhost:9443/pull/github/dequeue`
- `POST http://localhost:9443/pull/github/ack`
- `POST http://localhost:9443/pull/github/nack`
- `POST http://localhost:9443/pull/github/extend`

All requests use `Content-Type: application/json`.

## Authentication

All Pull API requests require a bearer token:

```
Authorization: Bearer <token>
```

Tokens are configured globally or per-route:

```hcl
# Global token (applies to all pull routes)
pull_api {
  auth token env:HOOKAIDO_PULL_TOKEN
}

# Per-route override
/webhooks/github {
  pull {
    path /pull/github
    auth token env:HOOKAIDO_GITHUB_PULL_TOKEN
  }
}
```

Per-route tokens replace (not extend) the global allowlist for that route.

## Endpoints

### `POST {endpoint}/dequeue`

Fetches a batch of messages from the queue.

**Request:**

```json
{
  "batch": 10,
  "lease_ttl": "30s",
  "max_wait": "10s"
}
```

| Field       | Default                      | Description                                                                        |
| ----------- | ---------------------------- | ---------------------------------------------------------------------------------- |
| `batch`     | `1`                          | Number of messages to dequeue (capped by `pull_api.max_batch`)                     |
| `lease_ttl` | `pull_api.default_lease_ttl` | How long the lease is held before auto-requeue. Capped by `pull_api.max_lease_ttl` |
| `max_wait`  | `pull_api.default_max_wait`  | Long-poll wait if the queue is empty. Capped by `pull_api.max_wait`                |

**Response (200):**

```json
{
  "items": [
    {
      "id": "evt_abc123",
      "lease_id": "lease_xyz789",
      "route": "/webhooks/github",
      "target": "pull",
      "payload_b64": "eyJhY3Rpb24iOiAicHVzaCJ9",
      "headers": {
        "Content-Type": "application/json",
        "X-GitHub-Event": "push"
      },
      "received_at": "2026-02-09T10:00:00Z",
      "attempt": 1
    }
  ]
}
```

- The response always returns a `200` with an `items` array — an empty array means no messages were available.
- `payload_b64` is the base64-encoded original webhook body.
- `headers` contains the original request headers captured at ingress (plus any forward-auth copied headers).
- `lease_id` is required for all subsequent operations (ack/nack/extend).

### `POST {endpoint}/ack`

Acknowledges successful processing. The message is permanently removed from the queue.

**Request:**

```json
{
  "lease_id": "lease_xyz789"
}
```

Batch form (single HTTP roundtrip for multiple leases):

```json
{
  "lease_ids": ["lease_xyz789", "lease_xyz790"]
}
```

- Use either `lease_id` or `lease_ids`, not both.
- `lease_ids` is deduplicated server-side and bounded to 100 items per request.

**Responses:**

- Single-lease form: `204 No Content`
- Batch form success: `200 OK`

```json
{
  "acked": 2
}
```

- Batch form with invalid/expired leases: `409 Conflict`

```json
{
  "code": "lease_conflict",
  "detail": "one or more leases are invalid or expired",
  "acked": 1,
  "conflicts": [
    { "lease_id": "lease_xyz790", "reason": "lease_not_found" }
  ]
}
```

- Duplicate `ack` retries for a recently completed lease are treated as idempotent success (`204` for single, counted as success in batch).

### `POST {endpoint}/nack`

Rejects a message, putting it back into the queue for reprocessing.

**Request:**

```json
{
  "lease_id": "lease_xyz789",
  "delay": "5s",
  "dead": false,
  "reason": "transient_error"
}
```

| Field      | Default      | Description                                                           |
| ---------- | ------------ | --------------------------------------------------------------------- |
| `lease_id` | **required** | The lease to reject                                                   |
| `delay`    | `0`          | Requeue delay before the message becomes visible again                |
| `dead`     | `false`      | If `true`, move to the dead-letter queue instead of requeuing         |
| `reason`   | —            | Optional reason string (persisted as `dead_reason` when `dead: true`) |

- When `dead: true`, the `delay` is ignored and the message moves to the DLQ immediately.

Batch form:

```json
{
  "lease_ids": ["lease_xyz789", "lease_xyz790"],
  "delay": "5s",
  "dead": false
}
```

- Use either `lease_id` or `lease_ids`, not both.
- `lease_ids` is deduplicated server-side and bounded to 100 items per request.
- `dead: true` works with batch form as well.

**Responses:**

- Single-lease form: `204 No Content`
- Batch form success: `200 OK`

```json
{
  "succeeded": 2
}
```

- Batch form with invalid/expired leases: `409 Conflict`

```json
{
  "code": "lease_conflict",
  "detail": "one or more leases are invalid or expired",
  "succeeded": 1,
  "conflicts": [
    { "lease_id": "lease_xyz790", "reason": "lease_not_found" }
  ]
}
```

- Duplicate `nack`/`dead` retries for a recently completed lease are treated as idempotent success (`204` for single, counted as success in batch).

### `POST {endpoint}/extend`

Extends the lease TTL for a message that needs more processing time.

**Request:**

```json
{
  "lease_id": "lease_xyz789",
  "lease_ttl": "30s"
}
```

**Response:** `204 No Content`

## Lease Semantics

Messages use a lease-based visibility model:

1. **Dequeue** creates a lease — the message becomes invisible to other consumers.
2. The consumer has until `lease_until` to process and `ack` the message.
3. If the lease expires without an `ack` or `nack`, the message is **automatically requeued**.
4. Use `extend` to renew the lease if processing takes longer than expected.
5. `nack` explicitly requeues (with optional delay) or dead-letters the message.

**Invalid/expired lease operations return `409 Conflict`.** Recent duplicate retries of an already successful `ack`/`nack` operation may be accepted as idempotent success.

## Error Responses

All non-2xx responses return structured JSON:

```json
{
  "code": "invalid_body",
  "detail": "unknown field \"foo\" in request body"
}
```

| Status | Code             | Meaning                                               |
| ------ | ---------------- | ----------------------------------------------------- |
| `400`  | `invalid_body`   | Malformed JSON, unknown fields, or trailing documents |
| `401`  | `unauthorized`   | Missing or invalid bearer token                       |
| `403`  | `forbidden`      | Token not in allowlist for this route                 |
| `409`  | `lease_conflict` | Lease ID is invalid or has expired                    |
| `429`  | `rate_limited`   | Rate limit exceeded                                   |
| `503`  | `queue_overload` | Queue backend is unavailable                          |

> Request bodies are parsed strictly: unknown JSON fields and trailing JSON documents are rejected with `400`.

## Dequeue Controls

Fine-tune Pull API behavior in the config:

```hcl
pull_api {
  auth token env:HOOKAIDO_PULL_TOKEN

  max_batch 100           # cap per-request batch size (default 100)
  default_lease_ttl 30s   # when client omits lease_ttl (default 30s)
  max_lease_ttl 5m        # hard upper bound for lease TTL
  default_max_wait 0      # when client omits max_wait (default 0 = no wait)
  max_wait 30s            # hard upper bound for long-poll wait
}
```

## Consumer Implementation Tips

1. **Use long-polling** (`max_wait`) to reduce empty-response overhead.
2. **Process in batches** — dequeue multiple messages, process in parallel, ack individually.
3. **Extend leases proactively** — if processing takes >50% of your `lease_ttl`, extend early.
4. **Dead-letter on permanent failures** — use `nack { dead: true, reason: "..." }` for non-retryable errors.
5. **Idempotent processing** — Hookaido provides at-least-once delivery, so your handler should tolerate duplicates.

---

← [Documentation Index](index.md)
