# Worker gRPC API

Hookaido provides an optional gRPC transport for pull workers. It uses the same queue semantics as the HTTP Pull API (`dequeue`, `ack`, `nack`, `extend`) but exposes them as gRPC methods for internal worker clients.

## Enable It

```hcl
pull_api {
  auth token env:HOOKAIDO_PULL_TOKEN
  grpc_listen 127.0.0.1:9943

  # Optional: shared TLS/mTLS config for Pull HTTP + Worker gRPC
  tls {
    cert_file /etc/hookaido/tls/server.crt
    key_file  /etc/hookaido/tls/server.key
    client_ca /etc/hookaido/tls/ca.crt
    client_auth require_and_verify
  }
}

"/webhooks/github" {
  pull { path /pull/github }
}
```

## Method Parity

| gRPC method | HTTP Pull API equivalent |
| --- | --- |
| `Dequeue` | `POST {endpoint}/dequeue` |
| `Ack` | `POST {endpoint}/ack` |
| `Nack` | `POST {endpoint}/nack` |
| `Extend` | `POST {endpoint}/extend` |

Behavior parity includes:

- lease semantics and conflict handling
- batch `ack`/`nack` conflict payloads
- idempotent duplicate retry handling for recently completed lease ops

## Auth and Route Mapping

- Auth uses the same token rules as Pull HTTP:
  - global allowlist from `pull_api { auth token ... }`
  - per-route override from `pull { auth token ... }` (replaces global list for that route)
- `endpoint` in gRPC requests uses the configured pull endpoint path (same path workers use for HTTP Pull routes).

## Listener Guardrails

When `pull_api.grpc_listen` is set:

- at least one pull route must exist
- `pull_api.grpc_listen` must not equal:
  - `ingress.listen`
  - `pull_api.listen`
  - `admin_api.listen`
  - `observability.metrics.listen`

## TLS and mTLS

Worker gRPC reuses `pull_api.tls`:

- no `pull_api.tls`: gRPC serves plaintext TCP
- with `pull_api.tls`: gRPC uses TLS
- with `client_ca` + `client_auth require*`: gRPC enforces mTLS

## Operations Guidance

- Bind `grpc_listen` to an internal-only interface/network segment.
- Keep `grpc_listen` separate from Pull/Admin/Ingress listeners (required by compile guards).
- Use distinct worker tokens per route when blast-radius isolation is needed.
- Changes to `pull_api.grpc_listen` or `pull_api.tls` are restart-required.

## MCP Scope Decision

Worker lease operations are intentionally **not** exposed as MCP tools. MCP keeps lease mutations on existing Admin/Pull operational surfaces to avoid duplicating high-impact mutation paths.

## Scope Guardrail

Worker gRPC is intentionally constrained to pull-worker lease operations only:

- `Dequeue`
- `Ack`
- `Nack`
- `Extend`

It is not a second admin/publish/control-plane API and is not planned to become one.

See:

- [Configuration](configuration.md)
- [Pull API](pull-api.md)
- [MCP Integration](mcp.md)

---

← [Documentation Index](index.md)
