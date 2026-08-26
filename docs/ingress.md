# Ingress

The ingress is Hookaido's inbound HTTP listener that receives webhooks, authenticates them, and enqueues them into the durable queue.

## Overview

When a webhook arrives:

1. **Path matching** — find the first route whose path matches the request URL.
2. **Matcher evaluation** — check additional match criteria (method, host, headers, etc.).
3. **Rate limit check** — enforce global or per-route rate limits.
4. **Authentication** — verify HMAC signature, basic credentials, or forward auth.
5. **Enqueue** — durably persist the payload and headers into the queue.
6. **ACK** — return `202 Accepted` to the webhook provider only after successful enqueue.

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
- Order matters: put the **more specific path first**. `/hooks` listed before `/hooks/github` swallows every request the latter was meant to handle, so `config validate` rejects that arrangement as unreachable. See [Routing Semantics](configuration.md#routing-semantics).

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
| `remote_ip`     | Client IP or CIDR. IPv4 and IPv6 supported                                 |

#### `remote_ip` behind a reverse proxy

`remote_ip` is compared against the **transport peer address** — what the socket
reports. `X-Forwarded-For` is ignored unless `ingress.trusted_proxies` is set.

That matters because Hookaido is very often deployed behind a TLS-terminating
reverse proxy, including in the Docker examples in these docs. In that topology
every request arrives with the *proxy's* address, so:

```hcl
match { remote_ip "203.0.113.0/24" }   # the source's published egress range
```

matches nothing and the route silently returns `404` for legitimate traffic.
Widening the range to the proxy's subnet makes traffic flow again — and now
matches every request the proxy forwards, from any origin. Both outcomes read
like a working IP allowlist in the config file; the second one is not.

Two ways out, in order of preference:

1. **Put the IP restriction in the proxy.** It is the component that sees the real
   client address, and it is where the restriction belongs if the proxy is not
   yours to configure otherwise.
2. **Set `ingress.trusted_proxies`** to the prefixes your proxy connects from.
   Hookaido then takes the right-most `X-Forwarded-For` entry that is not itself
   trusted as the client address for `remote_ip`. Requests from an untrusted peer
   keep their peer address and the header is ignored, so a direct client cannot
   spoof its way past the allowlist. See
   [`ingress.trusted_proxies`](configuration.md#trusted_proxies).

`header` and `query` compare their values in **constant time**. Matchers normally
only select a route — `X-GitHub-Event: push` is not a secret — but a `query`
matcher on a shared token was for a long time the only credential check available
to an event source whose entire configuration surface is a single URL field, so
the comparison is held to the same standard as the `auth` paths. Value lengths
remain distinguishable.

For that case, prefer [`auth query`](#query-token-auth): it is an authenticator
rather than a matcher, so the route does not report as unauthenticated, and it
reaches `secret_ref` and the runtime secrets pool for rotation.

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

| Variant | What the source must be able to send |
| --- | --- |
| [`auth hmac`](#hmac-verification) | A signature header (plus timestamp/nonce depending on mode) |
| [`auth basic`](#basic-auth) | An `Authorization` header |
| [`auth forward`](#forward-auth) | Whatever the external checker expects |
| [`auth query`](#query-token-auth) | Nothing but the URL — the token is a query parameter |

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

String-to-sign: `METHOD + "\n" + PATH + "\n" + TIMESTAMP + "\n" + hex(sha256(body))`

Verification tries all secrets valid at the request timestamp (from the timestamp header), not just wall-clock time. This allows safe key rotation with overlapping validity windows.

A nonce is claimed when the signature verifies and becomes permanent once the request is durably enqueued. A request that is refused after verification — a 503 from queue backpressure, a 413 from oversized headers — releases the claim, so the sender's identical signed retry is accepted instead of being rejected as a replay for the rest of the tolerance window. A replay arriving while the first request is still in flight is rejected either way, and claims survive config reloads.

**Provider mode** (GitHub, Gitea/Forgejo, Stripe, Cituro):

For webhook providers with their own signature format, use provider mode. This verifies the provider's native signature:

```hcl
/webhooks/github {
  auth hmac {
    provider github
    secret env:GITHUB_WEBHOOK_SECRET
  }
  pull { path /pull/github }
}

/webhooks/gitea {
  auth hmac {
    provider gitea
    secret env:GITEA_WEBHOOK_SECRET
  }
  pull { path /pull/gitea }
}

/webhooks/stripe {
  auth hmac {
    provider stripe
    secret env:STRIPE_WEBHOOK_SECRET
  }
  pull { path /pull/stripe }
}
```

| Provider | Signature Header | Format | Signed payload | Replay protection |
|---|---|---|---|---|
| `github` | `X-Hub-Signature-256` | `sha256=<hex>` | raw body | none (GitHub omits a timestamp) |
| `gitea` | `X-Gitea-Signature` | `<hex>` | raw body | none |
| `stripe` | `Stripe-Signature` | `t=<ts>,v1=<hex>` | `<ts>.<body>` | 5 min fixed tolerance |
| `cituro` | `X-CITURO-SIGNATURE` | `t=<ts>,s=<hex>` | `<ts>.<body>` | 5 min fixed tolerance |

`stripe` and `cituro` share the timestamped scheme Stripe invented; `cituro` differs only in header name and signature tag. Both accept several comma-separated `<tag>=<hex>` pairs, and any matching signature verifies the request — which is what makes Stripe's `v0`/`v1` rotation work.

When `provider` is set, `signature_header`, `timestamp_header`, `nonce_header`, and `tolerance` are forbidden (compile error) — the format is fixed by the provider. Replay protection therefore applies to `stripe` and `cituro` only, and its 5-minute window is not configurable.

### Basic Auth

```hcl
/webhooks/simple {
  auth basic "webhook-user" "{env.WEBHOOK_PASSWORD}"
  pull { path /pull/simple }
}
```

!!! warning "Basic auth does not take secret references"

    Unlike `auth token`, `auth hmac` and `secret` blocks, basic-auth credentials
    are compared **literally**. The `env:` / `file:` / `vault:` / `raw:` reference
    syntax is not resolved here — use the `{env.NAME}` placeholder form shown
    above, which is expanded at compile time.

    Configs using reference syntax are rejected at compile time. Before that
    check existed, `auth basic "u" "env:PASSWORD"` silently accepted the string
    `env:PASSWORD` as the password.

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

### Query Token Auth

For event sources that can be given **nothing but a URL**:

```hcl
/webhooks/source {
  auth query "t" secret_ref "source-token"
  pull { path /pull/source }
}
```

Some sources have no header, no signing secret and no basic-auth field to
configure — the UI has one "URL" input and that is the whole contract. This is
common with telephony/PBX platforms, appliance webhooks and older ERP systems.
None of the three variants above can be satisfied by such a source, so the token
travels in a query parameter.

**Shorthand and rotation:**

```hcl
auth query "t" "env:URL_TOKEN"             # env / file / vault / raw ref
auth query "t" secret_ref "source-token"   # pool-backed, rotatable
```

Repeating the directive with the same parameter name adds another accepted token,
which is how a rotation gets an overlap window:

```hcl
/webhooks/source {
  auth query "t" secret_ref "token-v1"
  auth query "t" secret_ref "token-v2"
  pull { path /pull/source }
}
```

Behavior:

- A missing or wrong token → **`404`**, the same answer an unmatched request
  gets. There is no realistic client here that benefits from a distinguishable
  auth error, and `404` does not confirm that the path exists.
- Checked **before the rate limiter and before the body is read**, so a wrong
  token costs no queue work and cannot consume the route's token budget.
- The token is never written to the access log, the envelope, the queue, metrics
  labels or the delivery target. That is what makes a token in the *query*
  preferable to one in the *path* — the path is simultaneously queue key,
  access-log field, envelope trace and Prometheus label.
- Comparison is constant-time over SHA-256 digests, so neither the token length
  nor the number of configured tokens is observable.
- Rejections are reported with reason `auth` and status `404` in the ingress
  metrics, so an operator can still tell a wrong token from an unknown path.
- A parameter with no usable secret **fails compilation** — commenting out the
  secret line during a rotation must not silently open the route.

> `auth query` is mutually exclusive with `auth basic`, `auth hmac` and
> `auth forward`. It also rejects a `match query` or `query_exists` on the *same*
> parameter: both would read it, the matcher runs first, and a wrong token would
> fall through to a later route instead of being rejected here.

**Honest limitation.** A URL token cannot be made replay-safe: there is no nonce
and no timestamp, so anyone who learns the URL can inject events. That is a
property of the source, not of Hookaido. Use `auth hmac` whenever the source can
sign — `auth query` is a gate against opportunistic traffic, not a signature.

**Why not `match query`?** Using the matcher as a credential check works and was
the only option before this variant existed. It reports the route as having no
authenticator at all to anyone auditing the config, though, and it cannot reach
`secret_ref` or the runtime secrets pool — so the token can only be rotated by
editing the config. Prefer `auth query`.

**Why not a token in the path?** The path is the queue key, an access-log field,
an envelope trace value and a Prometheus label. A secret placed there ends up in
all four.

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

**Across a config reload**, a bucket whose `rps` and `burst` are unchanged keeps its current token balance — it is not refilled. Only a limiter whose limits actually changed, or one belonging to a new route, starts full. This matters because reloads are frequent and not always deliberate: `hookaido run --watch` triggers one per config write, and every applied Admin API managed-endpoint mutation triggers one too. Refilling on each of those would make the effective limit unbounded at reload frequency.

## Body and Header Limits

Ingress enforces size limits from `defaults` or per-route config:

- **`max_body`** (default `2mb`) — payload size limit. Oversized → `413`.
- **`max_headers`** (default `64kb`) — total header size limit (including copied forward-auth headers). Oversized → `413`.

## Enqueue Behavior

- Ingress ACKs the webhook provider (`202 Accepted`) **only after** durable queue persistence.
- If the queue is full (`queue_limits.max_depth`), ingress returns `429` (with `drop_policy "reject"`) or silently drops the oldest item.
- Queue key is the route path — all webhooks matching a route share the same queue.

## Response Codes

| Status | Meaning                                |
| ------ | -------------------------------------- |
| `202`  | Webhook received and enqueued          |
| `400`  | Invalid request                        |
| `401`  | Authentication failed                  |
| `403`  | Forbidden (forward auth denied)        |
| `404`  | No route matches the path              |
| `413`  | Body or headers exceed size limits     |
| `429`  | Rate limit exceeded or queue full      |
| `503`  | Queue overload or forward auth failure |

On success the response body is `{"status":"queued"}` with `Content-Type: application/json`.

---

← [Documentation Index](index.md)
