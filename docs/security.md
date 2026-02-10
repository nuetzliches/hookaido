# Security

Hookaido provides layered security across ingress authentication, transport encryption, secret management, API access control, and egress protection.

## Inbound Authentication (Ingress)

### HMAC Signature Verification

Verify webhook signatures using HMAC-SHA256 with replay protection:

```hcl
/webhooks/github {
  auth hmac env:HOOKAIDO_GITHUB_SECRET
}
```

Full control via block form:

```hcl
/webhooks/github {
  auth hmac {
    secret env:HOOKAIDO_GITHUB_SECRET
    signature_header "X-Hub-Signature-256"
    timestamp_header "X-Timestamp"
    nonce_header "X-Nonce"
    tolerance 5m
  }
}
```

**Replay protection:** The timestamp header is checked against the tolerance window. The nonce header (when configured) provides additional replay defense.

**Secret rotation:** With `secret_ref`, verification tries all secrets valid at the request timestamp (from the signed timestamp header), allowing overlapping key rotation with zero downtime.

### Basic Auth

```hcl
auth basic "webhook-user" "env:WEBHOOK_PASSWORD"
```

### Forward Auth

Delegate to an external auth service:

```hcl
auth forward "https://auth.example/check" {
  timeout 5s
  copy_headers "X-User-ID"
  body_limit 64kb
}
```

Forward auth **fails closed**: transport errors, timeouts, and non-2xx/401/403 responses all result in `503`.

## TLS and mTLS

Every listener (ingress, Pull API, Admin API) supports TLS with optional mutual TLS:

```hcl
ingress {
  listen :8080
  tls {
    cert_file /path/to/cert.pem
    key_file  /path/to/key.pem
    client_ca /path/to/ca.pem         # enables mTLS
    client_auth require_and_verify    # optional
  }
}

pull_api {
  listen :9443
  tls {
    cert_file /path/to/pull-cert.pem
    key_file  /path/to/pull-key.pem
    client_ca /path/to/pull-ca.pem
  }
}
```

### Client Auth Modes

| Value                | Description                                           |
| -------------------- | ----------------------------------------------------- |
| `none`               | No client certificate requested                       |
| `request`            | Request certificate, don't require it                 |
| `require`            | Require certificate (default when `client_ca` is set) |
| `verify_if_given`    | Verify certificate only if provided                   |
| `require_and_verify` | Require and verify a valid client certificate         |

### Production TLS: Reverse Proxy Recommended

Hookaido does **not** include automatic certificate provisioning (ACME / Let's Encrypt). For production deployments, terminate TLS at a reverse proxy or cloud load balancer and forward plain HTTP to Hookaido's ingress listener:

| Approach      | Example                                      |
| ------------- | -------------------------------------------- |
| Reverse proxy | Caddy, nginx, Traefik (automatic ACME)       |
| Cloud LB      | AWS ALB/NLB, GCP HTTPS LB, Azure App Gateway |
| Service mesh  | Istio, Linkerd sidecar TLS                   |

Use Hookaido's built-in `tls { cert_file, key_file }` when you need:

- **mTLS** between Hookaido and internal callers (Pull API, Admin API).
- **Direct TLS** with certificates from a private CA or cert-manager.

> **Tip:** In the common DMZ-pull deployment, the ingress listener sits behind a TLS-terminating reverse proxy while the Pull API and Admin API use mTLS or stay on localhost.

## Secret Management

Secrets should never appear as plaintext in config files. Hookaido supports several reference types:

| Reference | Example                 | Description             |
| --------- | ----------------------- | ----------------------- |
| `env:`    | `env:MY_SECRET`         | Environment variable    |
| `file:`   | `file:/run/secrets/key` | File content            |
| `raw:`    | `raw:literal-value`     | Inline value (dev only) |

### Named Secrets with Rotation

Define secrets with validity windows in a `secrets` block:

```hcl
secrets {
  secret "hmac-v1" {
    value env:HMAC_SECRET_V1
    valid_from "2026-01-01T00:00:00Z"
    valid_until "2026-07-01T00:00:00Z"
  }
  secret "hmac-v2" {
    value env:HMAC_SECRET_V2
    valid_from "2026-06-01T00:00:00Z"
  }
}
```

- `valid_from` is inclusive, `valid_until` is exclusive.
- Omit `valid_until` for keys that don't expire.
- Referenced via `secret_ref "hmac-v1"` in auth and signing blocks.

**Rotation semantics:**

- **Signing** selects the newest (or oldest) valid secret at signing time.
- **Verification** tries all secrets valid at the request timestamp — not wall-clock time — enabling safe rotation with overlapping windows.

## API Access Control

### Pull API

Token-based authentication is required:

```hcl
pull_api {
  auth token env:HOOKAIDO_PULL_TOKEN
}
```

Per-route tokens can override the global allowlist:

```hcl
/webhooks/github {
  pull {
    path /pull/github
    auth token env:GITHUB_PULL_TOKEN
  }
}
```

### Admin API

Default bind: `127.0.0.1:2019` (localhost only). Optional token auth:

```hcl
admin_api {
  listen 127.0.0.1:2019
  auth token env:HOOKAIDO_ADMIN_TOKEN
}
```

> The Admin API should only be exposed over a secure channel. Use TLS + token auth or mTLS when exposing beyond localhost.

### Audit Trail

All Admin API mutations require `X-Hookaido-Audit-Reason` and emit structured audit log events with:

- Timestamp, actor, reason, request ID
- Operation details (state transitions, queue counts, config changes)

Optional stricter audit requirements via `defaults.publish_policy`:

- `require_actor on` — require `X-Hookaido-Audit-Actor`
- `require_request_id on` — require `X-Request-ID`

Scoped managed operations can additionally restrict actors:

- `actor_allow "ci-bot"` — only listed actors allowed
- `actor_prefix "deploy-"` — actors must match prefix

## Egress Protection (SSRF)

All outbound deliveries (push mode) enforce SSRF-safe defaults:

```hcl
defaults {
  egress {
    allow "*.internal.example.com"
    deny "169.254.0.0/16"     # AWS metadata
    deny "10.0.0.0/8"         # private ranges
    https_only on                # only HTTPS targets (default)
    redirects off                # don't follow redirects (default)
    dns_rebind_protection on     # block DNS rebinding (default)
  }
}
```

**Evaluation order:** Deny rules first, then allowlist (if configured, target must match).

**Wildcard support:**

- `*` — matches any host
- `*.example.com` — matches subdomains only (not the apex)

## Publish Policy

Control who can publish messages programmatically via the Admin API:

```hcl
defaults {
  publish_policy {
    direct on        # allow POST /messages/publish
    managed on       # allow endpoint-scoped publish
    allow_pull_routes on            # allow publish to pull-mode routes
    allow_deliver_routes on         # allow publish to push-mode routes
    require_actor off         # require X-Hookaido-Audit-Actor
    require_request_id off    # require X-Request-ID
    fail_closed off  # fail closed when context unavailable
    actor_allow "ci-bot"     # explicit actor allowlist
    actor_prefix "deploy-"   # actor prefix match
  }
}
```

Per-route publish control:

```hcl
/webhooks/github {
  publish off            # block all manual publish to this route
  # or selectively:
  publish {
    direct off           # block global direct publish path
    managed off          # block endpoint-scoped managed publish path
  }
}
```

## Security Checklist

- [ ] Use HMAC or forward auth on all ingress routes
- [ ] Never put secrets as plaintext in the Hookaidofile — use `env:` or `file:` refs
- [ ] Enable TLS on all externally accessible listeners (ingress, Pull API)
- [ ] Admin API: keep on localhost or protect with mTLS + token auth
- [ ] Configure `egress.deny` for internal network ranges
- [ ] Enable audit requirements (`require_actor`, `require_request_id`) in production
- [ ] Rotate secrets using named `secret_ref` entries with overlapping validity windows
- [ ] Review `publish_policy` defaults — restrict by actor/route mode as needed

---

← [Documentation Index](index.md)
