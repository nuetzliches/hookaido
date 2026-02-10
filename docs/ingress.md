# Ingress

The ingress is Hookaido's inbound HTTP listener that receives webhooks, authenticates them, and enqueues them into the durable queue.

## Overview

When a webhook arrives:

1. **Path matching** — find the first route whose path matches the request URL.
2. **Matcher evaluation** — check additional match criteria (method, host, headers, etc.).
3. **Rate limit check** — enforce global or per-route rate limits.
4. **Authentication** — verify HMAC signature, basic credentials, or forward auth.
5. **Enqueue** — durably persist the payload and headers into the queue.
6. **ACK** — return `200 OK` to the webhook provider only after successful enqueue.

## Route Matching

Routes are evaluated **top-down, first match wins**.

```hcl
/webhooks/github { ... }      # matches /webhooks/github and /webhooks/github/foo
/webhooks/stripe { ... }      # matches /webhooks/stripe and /webhooks/stripe/events
```

Path matching rules:

- Matches the URL path only (query string is ignored).
- `"/path"` matches `/path` and `/path/...` at segment boundaries.
- `"/path"` does **not** match `/path-foo` (segment boundary enforced).
- Route paths must start with `/` and must be unique.

### Additional Matchers

Matchers further narrow which requests a route accepts. All matchers within a route are **ANDed**:

```hcl
/webhooks/github {
  match {
    method POST                      # HTTP method (case-insensitive; defaults to POST)
    host "hooks.example.com"         # exact host, "*", or "*.example.com"
    header "X-GitHub-Event" "push"   # exact header value match
    header_exists "X-GitHub-Delivery" # header presence check
    query "env" "production"         # exact query parameter match
    query_exists "token"             # query parameter presence check
    remote_ip "203.0.113.0/24"       # source IP or CIDR
  }
  pull { path /pull/github }
}
```

| Matcher         | Description                                                                |
| --------------- | -------------------------------------------------------------------------- |
| `method`        | HTTP method (case-insensitive). Default: `POST`                            |
| `host`          | Request host. Supports exact, `*` (any), `*.example.com` (subdomains only) |
| `header`        | Exact header value (name is case-insensitive)                              |
| `header_exists` | Header must be present (any value)                                         |
| `query`         | Exact query parameter value                                                |
| `query_exists`  | Query parameter key must be present                                        |
| `remote_ip`     | Source IP or CIDR (from `RemoteAddr`). IPv4 and IPv6 supported             |

### Named Matchers

Define reusable matchers at the top level:

```hcl
@stripe-invoice {
  method POST
  header "Stripe-Event-Type" "invoice.paid"
}

/webhooks/stripe {
  match @stripe-invoice
  pull { path /pull/stripe }
}
```

## Authentication

Each route can use one authentication method. Authentication runs before enqueue — rejected requests never enter the queue.

### HMAC Verification

Verifies webhook signatures with replay protection (timestamp + nonce + tolerance).

**Shorthand:**

```hcl
/webhooks/github {
  auth hmac env:HOOKAIDO_GITHUB_SECRET
  pull { path /pull/github }
}
```

**With secret rotation:**

```hcl
/webhooks/github {
  auth hmac secret_ref "S1"
  pull { path /pull/github }
}
```

**Block form** (full control):

```hcl
/webhooks/github {
  auth hmac {
    secret env:HOOKAIDO_GITHUB_SECRET
    # or: secret_ref "S1"
    signature_header "X-Hub-Signature-256"
    timestamp_header "X-Timestamp"
    nonce_header "X-Nonce"
    tolerance 5m
  }
  pull { path /pull/github }
}
```

String-to-sign: `TIMESTAMP + "\n" + METHOD + "\n" + PATH + "\n" + hex(sha256(body))`

Verification tries all secrets valid at the request timestamp (from the timestamp header), not just wall-clock time. This allows safe key rotation with overlapping validity windows.

### Basic Auth

```hcl
/webhooks/simple {
  auth basic "webhook-user" "env:WEBHOOK_PASSWORD"
  pull { path /pull/simple }
}
```

### Forward Auth

Delegates authentication to an external service:

```hcl
/webhooks/custom {
  auth forward "https://auth.example.com/check"
  pull { path /pull/custom }
}
```

**With options:**

```hcl
/webhooks/custom {
  auth forward "https://auth.example.com/check" {
    timeout 5s
    copy_headers "X-User-ID"
    copy_headers "X-Org-ID"
    body_limit 64kb
  }
  pull { path /pull/custom }
}
```

Behavior:

- `2xx` → allow, enqueue
- `401` or `403` → deny
- All other outcomes (transport errors, timeouts, 5xx) → **fail closed** with `503`

Response headers specified in `copy_headers` are copied from the auth response into the stored envelope headers.

> `auth forward` is mutually exclusive with `auth basic` and `auth hmac`.

## Rate Limiting

Token-bucket rate limiting with global and per-route scopes.

**Global** (applies to all routes unless overridden):

```hcl
ingress {
  rate_limit {
    rps 100
    burst 200   # optional; defaults to ceil(rps)
  }
}
```

**Per-route override:**

```hcl
/webhooks/high-volume {
  rate_limit { rps 500 }
  pull { path /pull/hv }
}
```

Over-limit requests receive `429 Too Many Requests`.

## Body and Header Limits

Ingress enforces size limits from `defaults` or per-route config:

- **`max_body`** (default `2mb`) — payload size limit. Oversized → `413`.
- **`max_headers`** (default `64kb`) — total header size limit (including copied forward-auth headers). Oversized → `413`.

## Enqueue Behavior

- Ingress ACKs the webhook provider (`200 OK`) **only after** durable queue persistence.
- If the queue is full (`queue_limits.max_depth`), ingress returns `429` (with `drop_policy "reject"`) or silently drops the oldest item.
- Queue key is the route path — all webhooks matching a route share the same queue.

## Response Codes

| Status | Meaning                                |
| ------ | -------------------------------------- |
| `200`  | Webhook received and enqueued          |
| `400`  | Invalid request                        |
| `401`  | Authentication failed                  |
| `403`  | Forbidden (forward auth denied)        |
| `404`  | No route matches the path              |
| `413`  | Body or headers exceed size limits     |
| `429`  | Rate limit exceeded or queue full      |
| `503`  | Queue overload or forward auth failure |

---

← [Documentation Index](index.md)
