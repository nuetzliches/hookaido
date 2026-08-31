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
- `GET  http://localhost:9443/pull/github/stream` (SSE)

POST requests use `Content-Type: application/json`. The SSE endpoint returns `Content-Type: text/event-stream`.

A route with [consumer groups](#consumer-groups) serves one endpoint per group instead, under the same `pull.path`.

## Consumer Groups

By default a pull route is a **competing-consumer** queue: every message is leased to exactly one consumer. That is the right default for scaling workers — add a second worker and the two share the load.

It makes one topology impossible to express, though: **one inbound source, several independent consumers that each need every message.** That comes up whenever the source cannot be configured to deliver more than once — appliances and telephony systems that hold exactly one webhook URL, older ERP systems, anything where the URL is set in a vendor portal. Attach a long-lived integration environment and a developer machine to such a queue and they silently split the traffic instead of both seeing it.

`consumer_group` turns the route into one independent queue per group:

```hcl
/webhooks/appliance {
  pull {
    path /appliance
    consumer_group "integration"
    consumer_group "workstation"
  }
}
```

Every inbound event is now enqueued once per group, and each group gets its own endpoint under the pull path:

- `POST http://localhost:9443/pull/appliance/integration/dequeue` (and `/ack`, `/nack`, `/extend`, `/stream`)
- `POST http://localhost:9443/pull/appliance/workstation/dequeue`

**The bare `/pull/appliance/...` path stops resolving and answers `404 route_not_found`.** That is deliberate rather than an oversight: silently keeping it would leave an unmigrated consumer competing for a share of a queue it was meant to receive in full, which is precisely the failure this feature exists to prevent — and a failure that, from inside the consumer, is indistinguishable from delivery loss. Adding groups to an existing route is therefore a change consumers must follow; add the group segment to their URL.

Semantics inside a group are unchanged. Two workers on `/pull/appliance/integration` still compete with each other, and leases still prevent double-delivery — so you can scale workers within a group and fan out across groups at the same time.

Group names must match `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`, must be unique within a route, and cannot be a Pull API operation name (`dequeue`, `ack`, `nack`, `extend`, `stream`).

Notes:

- **Not an authorization boundary.** Groups on one route share that route's pull credentials, so a client that can reach one group's endpoint can reach the others' — including settling their leases if a lease ID passes between them. Separate routes are the way to get an actual isolation boundary.
- **Each group is a queue of its own**, with its own depth, backlog, retries and DLQ entries. A group whose consumer is down accumulates backlog independently and counts against `queue_limits.max_depth` like any other queue. Its messages carry the target `pull:<group>`, which is what `GET /admin/messages?target=...` and the backlog tools filter on.
- **Admin publish needs an explicit target.** `POST /admin/messages/publish` auto-selects the target only for a route with exactly one; a grouped route has several, so an item without `target` is rejected with `target_unresolvable` listing the group targets. Publish to `pull:<group>` to reach one group, or send one item per group to reach them all — there is no "publish to every group" shorthand, because unlike an ingress event a republished message is usually meant for the one consumer that needs it again.
- **Observability separates groups.** Pull metrics carry a `consumer_group` label, and [`GET /admin/pull/consumers`](admin-api.md#get-pullconsumers) reports each consumer's group. Without a group configured the label is empty, which Prometheus treats as absent — existing series are unchanged.
- A route without `consumer_group` keeps the single `pull` target it always had, so nothing changes for existing configs, including messages already queued in a durable backend.

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

When any route defines its own token, lease operations are scoped to their route
as well: `ack`, `nack`, `dead` and `extend` reject a lease that belongs to a
different route with the same `409` conflict as an unknown lease, so a token for
one endpoint cannot settle another endpoint's in-flight message. With a single
global token the check is skipped — every client is authorized for every route
anyway — so it costs nothing in that setup.

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
- `lease_ids` is deduplicated server-side and bounded by `pull_api.max_lease_batch` (default 100). The same bound applies over gRPC.

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
- `lease_ids` is deduplicated server-side and bounded by `pull_api.max_lease_batch` (default 100). The same bound applies over gRPC.
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

## SSE Streaming

### `GET {endpoint}/stream`

Opens a Server-Sent Events connection for real-time message delivery. Each SSE message creates a lease (same semantics as dequeue). ACK/NACK remain via the existing POST endpoints.

**Query parameters:**

| Parameter   | Default                      | Description                                                    |
| ----------- | ---------------------------- | -------------------------------------------------------------- |
| `batch`     | `1`                          | Messages to dequeue per cycle (capped by `pull_api.max_batch`) |
| `lease_ttl` | `pull_api.default_lease_ttl` | Lease duration per message. Capped by `pull_api.max_lease_ttl` |

**Request:**

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  http://localhost:9443/pull/github/stream
```

**SSE event format:**

```
id: lease_abc123
event: message
data: {"id":"evt_1","lease_id":"lease_abc123","route":"/webhooks/github","received_at":"...","attempt":1,"payload_b64":"...","headers":{...}}

: keepalive

```

- `id` is the `lease_id` — used as `Last-Event-ID` on reconnect.
- `event: message` for queued items; `: keepalive` comments keep the connection alive through proxies.
- The `data` JSON payload is identical to items returned by `POST /dequeue`.

**Behavior:**

- Auth is identical to all other Pull API endpoints (bearer token).
- Multiple concurrent SSE connections on the same endpoint act as competing consumers — leases prevent double-delivery. To have several consumers each receive every message, use [consumer groups](#consumer-groups).
- On reconnect, the consumer sends `Last-Event-ID`. Since leases were already created, reconnect simply resumes dequeuing new items.
- The server sends keepalive comments at the interval configured by `sse_keepalive` (default 15s). Keepalives are for proxies, not for delivery: a message becoming ready wakes the stream immediately, whether it was newly published, nacked for retry, requeued from the DLQ, resumed, or reclaimed from an expired lease. This holds on every backend.
- Optionally, `sse_max_connection` limits the maximum connection duration for resource hygiene.

**Error event:**

If the store becomes unavailable, the server sends an error event and closes the connection:

```
event: error
data: dequeue is temporarily unavailable
```

### Who Is Attached

An unexpected second consumer on a route is the failure mode worth planning for, because from inside either consumer it does not look like a second consumer — it looks like delivery loss. The queue is competing-consumer, so the two split the traffic: ingress answers `202 {"status":"queued"}` for every event, and each side sees a fluctuating fraction of them arrive. It is easy to end up there by accident, with a shared token or a second environment pointed at the same host.

Two surfaces answer it:

- `hookaido_pull_sse_connection_active{route}` — **how many** consumers are attached. If this says `2` and you expect `1`, that is the problem, and it is the fastest signal.
- [`GET /pull/consumers`](admin-api.md#get-pullconsumers) on the Admin API — **which** ones: remote address, connected-since, messages sent, and the configured token reference each authenticated with.

Both cover SSE streams only. A consumer polling `POST {endpoint}/dequeue` holds no connection between calls, so it is counted by neither.

The runtime log carries the same lifecycle at INFO, which is what you need after the fact:

```json
{"level":"INFO","msg":"pull_sse_connected","consumer_id":"con_9f2c...","route":"/webhooks/appliance","endpoint":"/appliance","remote_addr":"10.0.0.5:41234","user_agent":"hookaido-worker/1.0","token_ref":"env.PULL_TOKEN"}
{"level":"INFO","msg":"pull_sse_disconnected","consumer_id":"con_9f2c...","route":"/webhooks/appliance","remote_addr":"10.0.0.5:41234","token_ref":"env.PULL_TOKEN","status_code":200,"messages_sent":81,"duration_seconds":3612.4}
```

The teardown line matters as much as the establish: a stream logs its HTTP request once, when it opens, and then stays open for hours, so the access log alone cannot tell you who is still attached.

If you need every consumer to receive every message rather than a share of them, the split is not the problem to fix — see [Consumer Groups](#consumer-groups).

## Dequeue Controls

Fine-tune Pull API behavior in the config:

```hcl
pull_api {
  auth token env:HOOKAIDO_PULL_TOKEN

  max_batch 100           # cap per-request dequeue size (default 100)
  max_lease_batch 100     # cap lease IDs per ack/nack/extend (default 100)
  default_lease_ttl 30s   # when client omits lease_ttl (default 30s)
  max_lease_ttl 5m        # hard upper bound for lease TTL
  default_max_wait 0      # when client omits max_wait (default 0 = no wait)
  max_wait 30s            # hard upper bound for long-poll wait
  sse_keepalive 15s       # SSE keepalive comment interval (default 15s)
  sse_max_connection 1h   # optional max SSE connection duration (default unlimited)
}
```

## Consumer Implementation Tips

1. **Use SSE streaming** (`GET .../stream`) for zero-latency delivery, or **long-polling** (`max_wait`) as a simpler alternative.
2. **Process in batches** — dequeue multiple messages, process in parallel, ack individually.
3. **Extend leases proactively** — if processing takes >50% of your `lease_ttl`, extend early.
4. **Dead-letter on permanent failures** — use `nack { dead: true, reason: "..." }` for non-retryable errors.
5. **Idempotent processing** — Hookaido provides at-least-once delivery, so your handler should tolerate duplicates.

---

← [Documentation Index](index.md)
