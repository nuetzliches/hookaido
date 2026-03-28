# Delivery (Push Mode)

In push mode, Hookaido delivers webhooks directly to your internal endpoints. The push dispatcher handles retry, backoff, concurrency limits, dead-lettering, and optional outbound HMAC signing.

## Basic Configuration

```hcl
/webhooks/github {
  deliver "https://ci.internal/build" {
    retry exponential max 8 base 2s cap 2m jitter 0.2
    timeout 10s
  }
}
```

A route can have multiple deliver targets — ingress fans out to all configured targets:

```hcl
/webhooks/github {
  deliver "https://ci.internal/build" { timeout 10s }
  deliver "https://analytics.internal/events" { timeout 5s }
}
```

### Outbound Channels

For API-to-queue-to-push flows (no ingress traffic), use the `outbound` channel type:

```hcl
outbound /notifications/slack {
  deliver "https://hooks.slack.com/services/..." {
    timeout 5s
  }
}
```

Messages are enqueued via the Admin API or MCP, then pushed by the dispatcher. See [Channel Types](configuration.md#channel-types) for details.

## Delivery Semantics

- **At-least-once delivery** — your endpoint may receive the same webhook more than once.
- Ingress acknowledges the webhook provider only after durable enqueue.
- Each deliver target is processed independently.

## Retry Policy

Hookaido retries on:

- Network errors and timeouts
- HTTP `5xx` responses
- HTTP `429` (rate limited) and `408` (request timeout)

**No retry** on other `4xx` responses (client errors are considered permanent).

### Default Retry Settings

```hcl
defaults {
  deliver {
    retry exponential max 8 base 2s cap 2m jitter 0.2
    timeout 10s
    concurrency 20
  }
}
```

| Setting   | Default | Description                                |
| --------- | ------- | ------------------------------------------ |
| `max`     | `8`     | Maximum retry attempts                     |
| `base`    | `2s`    | Base delay between retries                 |
| `cap`     | `2m`    | Maximum delay (exponential backoff cap)    |
| `jitter`  | `0.2`   | Jitter factor (0.0–1.0) to randomize delay |
| `timeout` | `10s`   | HTTP request timeout per attempt           |

### Per-Target Override

Each `deliver` block can override the defaults:

```hcl
/webhooks/stripe {
  deliver "https://billing.internal/stripe" {
    retry exponential max 3 base 500ms cap 30s jitter 0.1
    timeout 5s
  }
}
```

### Backoff Calculation

Delay for attempt `n`:

$$delay = \min(base \times 2^n,\ cap) \times (1 + jitter \times random(-1, 1))$$

## Concurrency

The dispatcher limits parallel deliveries per route:

```hcl
defaults {
  deliver {
    concurrency 20    # global default
  }
}

/webhooks/high-throughput {
  deliver_concurrency 50    # per-route override
  deliver "https://fast.internal/hook" { timeout 5s }
}
```

`deliver_concurrency` is a shared per-route budget across all route targets.
When a route has multiple targets, dispatcher workers are not pinned permanently to one target; idle-target capacity can drain backlog from active targets under saturation.

## Custom Outbound Headers

Add custom headers to outbound delivery requests with the `header` directive:

```hcl
/webhooks/github {
  deliver "https://ci.internal/build" {
    header "Authorization" "Bearer mytoken"
    header "X-Source" "hookaido"
    timeout 10s
  }
}
```

### Placeholder Support

Header values support the same placeholder syntax as other config values:

```hcl
/webhooks/github {
  deliver "https://ci.internal/build" {
    header "Authorization" "token {env.FORGEJO_TOKEN}"
    header "X-Environment" "{vars.DEPLOY_ENV}"
  }
}
```

Available placeholders: `{env.VAR}`, `{$VAR}`, `{file.PATH}`, `{vars.NAME}`.

### Validation Rules

- Header names must be valid HTTP tokens (RFC 7230).
- Duplicate header names (case-insensitive) are rejected at compile time.
- Headers are set on outbound requests **before** HMAC signing — they do not affect the signature.

## Subprocess Execution (`deliver exec`)

Deliver webhooks by executing a local command instead of making HTTP requests. The payload is piped to the subprocess's stdin as raw bytes.

```hcl
/webhooks/github {
  auth hmac { provider github; secret env:GITHUB_SECRET }
  deliver exec "/opt/hooks/deploy.sh" {
    timeout 30s
    retry exponential max 3 base 1s cap 1m jitter 0.1
    env DEPLOY_ENV production
  }
}
```

### Payload and Metadata

| Variable | Description |
|---|---|
| `HOOKAIDO_ROUTE` | Route path (e.g., `/webhooks/github`) |
| `HOOKAIDO_EVENT_ID` | Message UUID |
| `HOOKAIDO_CONTENT_TYPE` | Content-Type from inbound request |
| `HOOKAIDO_ATTEMPT` | Retry attempt number (1-indexed) |
| `HOOKAIDO_HEADER_*` | Inbound headers (e.g., `HOOKAIDO_HEADER_X_GITHUB_EVENT` for `X-GitHub-Event`) |
| `PATH` | Inherited from host environment |

Custom environment variables via `env <KEY> <VALUE>` (repeatable, supports placeholders):

```hcl
deliver exec "python /app/handler.py" {
  env API_ENDPOINT {env.INTERNAL_API_URL}
  env BATCH_SIZE "100"
}
```

### Exit Code Semantics

| Exit Code | Behaviour |
|---|---|
| `0` | Success — message is acked |
| `75` | Temporary failure (EX_TEMPFAIL) — retriable |
| `1-125` | General failure — retriable with backoff |
| `126`, `127` | Command not found / not executable — non-retriable, immediate DLQ |
| Signal | Process killed by signal — retriable |
| Timeout | Context deadline exceeded — retriable |

### Constraints

- `sign` directives are **not supported** with exec (compile error).
- Timeout is enforced via context cancellation; the process receives `SIGKILL` on expiry.
- `deliver_concurrency` applies to exec delivery the same as HTTP delivery.
- Dead-lettering follows the same rules as HTTP push (max retries exhausted, non-retryable errors).
- Stderr output is captured and logged (truncated to 4 KB) at debug level.

## Dead-Lettering

Messages are moved to the DLQ when:

- All retry attempts are exhausted (outcome: `max_retries`)
- A non-retryable `4xx` response is received on the first attempt

Dead items persist a `dead_reason` for inspection. Manage the DLQ via the [Admin API](admin-api.md):

- `GET /dlq` — list dead items
- `POST /dlq/requeue` — requeue for reprocessing
- `POST /dlq/delete` — permanently remove

## Delivery Attempts

Each delivery attempt is recorded with:

- `event_id` — source message ID
- `route`, `target` — delivery target
- `attempt` — attempt number
- `status_code` — HTTP response status (if any)
- `error` — transport error message (if any)
- `outcome` — `acked` (success), `retry`, or `dead`
- `dead_reason` — reason when dead-lettered
- `created_at` — timestamp

Query attempts via `GET /attempts` on the [Admin API](admin-api.md).

## Outbound Signing

Hookaido can sign outbound delivery requests with HMAC-SHA256 so your backend can verify authenticity.

### Basic Signing

```hcl
/webhooks/github {
  deliver "https://ci.internal/build" {
    sign hmac env:DELIVER_SECRET
  }
}
```

This adds two headers to each outbound request:

- `X-Hookaido-Signature` — HMAC-SHA256 hex signature
- `X-Hookaido-Timestamp` — Unix timestamp (UTC seconds)

### Custom Header Names

```hcl
deliver "https://ci.internal/build" {
  sign hmac env:DELIVER_SECRET
  sign signature_header "X-Webhook-Signature"
  sign timestamp_header "X-Webhook-Timestamp"
}
```

Signature and timestamp header names must be valid HTTP header tokens and must differ from each other.

### Secret Rotation

Use named secret references for zero-downtime key rotation:

```hcl
secrets {
  secret "deliver-v1" {
    value env:DELIVER_SECRET_V1
    valid_from "2026-01-01T00:00:00Z"
    valid_until "2026-07-01T00:00:00Z"
  }
  secret "deliver-v2" {
    value env:DELIVER_SECRET_V2
    valid_from "2026-06-01T00:00:00Z"
  }
}

/webhooks/github {
  deliver "https://ci.internal/build" {
    sign hmac secret_ref "deliver-v1"
    sign hmac secret_ref "deliver-v2"
    sign secret_selection newest_valid   # default
  }
}
```

- At signing time, Hookaido selects the newest secret whose `valid_from ≤ now < valid_until`.
- Use `sign secret_selection oldest_valid` to prefer the oldest valid key instead.
- `sign secret_selection` requires `sign hmac secret_ref` entries (not inline secrets).

### Canonical Signature Format

The signed string is:

```
<METHOD>\n<URL_PATH>\n<UNIX_TIMESTAMP>\n<SHA256_HEX(body)>
```

- `METHOD` — uppercase HTTP method (e.g., `POST`)
- `URL_PATH` — URL path only (query string excluded)
- `UNIX_TIMESTAMP` — UTC seconds since epoch
- `SHA256_HEX(body)` — hex-encoded SHA-256 of the request body

Signature: `hex(HMAC-SHA256(secret, canonical_string))`

### Verifying Signatures (Receiver Side)

```python
import hmac, hashlib, time

def verify(secret, method, path, body, sig_header, ts_header, tolerance=300):
    ts = int(ts_header)
    if abs(time.time() - ts) > tolerance:
        return False  # replay protection

    body_hash = hashlib.sha256(body).hexdigest()
    canonical = f"{method}\n{path}\n{ts}\n{body_hash}"
    expected = hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig_header)
```

## Egress Policy

Hookaido enforces SSRF-safe defaults for all outbound deliveries:

| Setting                 | Default | Description                       |
| ----------------------- | ------- | --------------------------------- |
| `https_only`            | `on`    | Only allow HTTPS delivery targets |
| `redirects`             | `off`   | Do not follow HTTP redirects      |
| `dns_rebind_protection` | `on`    | Block DNS rebinding attacks       |

Allow/deny lists can be configured per host, IP, or CIDR:

```hcl
defaults {
  egress {
    allow "*.internal.example.com"
    deny "169.254.0.0/16"
    deny "10.0.0.0/8"
    https_only on
    redirects off
    dns_rebind_protection on
  }
}
```

- Deny rules are evaluated first.
- If an allowlist is configured, the target must match.
- Wildcards: `*` matches any host, `*.example.com` matches subdomains only.

See [Security](security.md) for more on egress protection.

!!! note "Docker and Private Networks"
    The default `dns_rebind_protection on` may block delivery to private-network targets in Docker environments where DNS resolves to internal IPs. If your deliver targets are on private networks (e.g., `*.internal`, `10.x.x.x`), either add them to the `allow` list or set `dns_rebind_protection off` in your egress defaults.

---

← [Documentation Index](index.md)
