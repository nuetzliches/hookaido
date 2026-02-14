# Getting Started

This guide walks you through installing Hookaido and running your first webhook ingress queue.

## Install

**Pre-built binary** (no Go required):

Download the latest binary for your platform from [GitHub Releases](https://github.com/nuetzliches/hookaido/releases).

**Build from source** (requires Go 1.25+):

```bash
go build -o hookaido ./cmd/hookaido
```

**Docker:**

See [Docker Quickstart](docker.md) for a full Docker / Docker Compose guide.

```bash
docker build -t hookaido .
```

Verify:

```bash
./hookaido version
```

## Minimal Pull-Mode Setup

Pull mode is the recommended default: Hookaido receives webhooks in the DMZ, and your internal workers pull messages via outbound-only connections.

### 1. Create a Hookaidofile

```hcl
ingress {
  listen :8080
}

pull_api {
  auth token env:HOOKAIDO_PULL_TOKEN
}

/webhooks/github {
  auth hmac env:HOOKAIDO_INGRESS_SECRET
  pull { path /pull/github }
}
```

This config:

- Listens for incoming webhooks on `:8080`
- Exposes a Pull API on `:9443` (default) with token authentication
- Routes `POST /webhooks/github` through HMAC verification, then enqueues

### 2. Set Environment Variables

```bash
export HOOKAIDO_PULL_TOKEN="your-pull-secret"
export HOOKAIDO_INGRESS_SECRET="your-hmac-secret"
```

For local development, use a dotenv file instead:

```bash
# .env
HOOKAIDO_PULL_TOKEN=devtoken
HOOKAIDO_INGRESS_SECRET=devsecret
```

### 3. Start Hookaido

```bash
./hookaido run --config ./Hookaidofile --db ./.data/hookaido.db
```

With dotenv:

```bash
./hookaido run --config ./Hookaidofile --db ./.data/hookaido.db --dotenv ./.env
```

Hookaido starts three listeners:

- **Ingress** on `:8080` — receives webhooks
- **Pull API** on `:9443` — workers consume from here
- **Admin API** on `127.0.0.1:2019` — health checks, DLQ management

### 4. Send a Test Webhook

```bash
curl -X POST http://localhost:8080/webhooks/github \
  -H "Content-Type: application/json" \
  -d '{"action": "push", "ref": "refs/heads/main"}'
```

> If HMAC auth is configured, you also need to send the appropriate signature headers. For testing without auth, remove the `auth hmac` line from your route.

### 5. Consume Messages

Your worker polls the Pull API:

```bash
# Dequeue a batch
curl -X POST http://localhost:9443/pull/github/dequeue \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/json" \
  -d '{"batch": 10, "lease_ttl": "30s"}'
```

Response:

```json
{
  "items": [
    {
      "id": "evt_abc123",
      "lease_id": "lease_xyz",
      "route": "/webhooks/github",
      "target": "pull",
      "payload_b64": "eyJhY3Rpb24iOiAicHVzaCJ9",
      "headers": { "Content-Type": "application/json" },
      "received_at": "2026-02-09T10:00:00Z",
      "attempt": 1
    }
  ]
}
```

After processing, acknowledge:

```bash
curl -X POST http://localhost:9443/pull/github/ack \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/json" \
  -d '{"lease_id": "lease_xyz"}'
```

Or reject (requeue with delay):

```bash
curl -X POST http://localhost:9443/pull/github/nack \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/json" \
  -d '{"lease_id": "lease_xyz", "delay": "5s"}'
```

## Minimal Push-Mode Setup

In push mode, Hookaido delivers webhooks directly to your internal endpoints:

```hcl
/webhooks/github {
  deliver https://ci.internal/build {}
}
```

No Pull API is needed — Hookaido handles retry, backoff, and (optional) outbound signing. See [Delivery (Push)](delivery.md) for details.

## Validate Your Config

Before starting:

```bash
./hookaido config validate --config ./Hookaidofile
```

Format (canonical style, idempotent):

```bash
./hookaido config fmt --config ./Hookaidofile
```

## Check Health

```bash
curl http://127.0.0.1:2019/healthz
# Detailed diagnostics:
curl http://127.0.0.1:2019/healthz?details=1
```

## CLI Reference

| Command                    | Description                     |
| -------------------------- | ------------------------------- |
| `hookaido run`             | Start the server                |
| `hookaido config fmt`      | Format the Hookaidofile         |
| `hookaido config validate` | Validate config (exit code 0/1) |
| `hookaido mcp serve`       | Start MCP server (AI tooling)   |
| `hookaido version`         | Print version                   |

### `hookaido run` Flags

| Flag          | Default               | Description                              |
| ------------- | --------------------- | ---------------------------------------- |
| `--config`    | `./Hookaidofile`      | Path to config file                      |
| `--db`        | `./.data/hookaido.db` | Path to SQLite database                  |
| `--postgres-dsn` | —                  | PostgreSQL DSN (when `queue.backend` is `postgres`) |
| `--pid-file`  | —                     | Write PID file (enables `SIGHUP` reload) |
| `--watch`     | `false`               | Hot-reload on config file changes        |
| `--log-level` | `info`                | Runtime log level                        |
| `--dotenv`    | —                     | Load environment variables from file     |

## Next Steps

- [Configuration Reference](configuration.md) — full DSL documentation
- [Deployment Modes](deployment-modes.md) — pull vs push, DMZ topology
- [Ingress](ingress.md) — route matching and authentication
- [Pull API](pull-api.md) — consumer protocol details
- [Docker Quickstart](docker.md) — run with Docker / Docker Compose
- [Security](security.md) — TLS, HMAC, SSRF protection
- [Observability](observability.md) — logging, metrics, tracing

---

← [Documentation Index](index.md)
