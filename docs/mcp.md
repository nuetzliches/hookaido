# MCP Integration

Hookaido includes an optional [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server for AI-assisted operations. It exposes typed, role-gated tools over JSON-RPC, enabling AI agents to inspect config, query queue state, and perform controlled mutations.

## Overview

```bash
# Read-only mode (default)
hookaido mcp serve --config ./Hookaidofile --db ./hookaido.db

# With queue mutations
hookaido mcp serve --config ./Hookaidofile --db ./hookaido.db \
  --enable-mutations --role operate --principal ops@example.test

# Full access (config + runtime control)
hookaido mcp serve --config ./Hookaidofile --db ./hookaido.db \
  --enable-mutations --enable-runtime-control --role admin \
  --principal ops@example.test --pid-file ./hookaido.pid
```

Transport: JSON-RPC 2.0 over stdio with `Content-Length` framing.

## Roles

| Role      | Capabilities                                                                                  |
| --------- | --------------------------------------------------------------------------------------------- |
| `read`    | Config inspection, queue/backlog reads, health checks                                         |
| `operate` | Read tools + safe queue mutations + runtime inspect (`instance_status`, `instance_logs_tail`) |
| `admin`   | Full surface: config apply, endpoint mapping, process control                                 |

Mutating tools require `--enable-mutations` and explicit `--principal`.

Runtime control tools (`instance_start`, `instance_stop`, `instance_reload`) require `--enable-runtime-control`.

## CLI Flags

| Flag                         | Default          | Description                                    |
| ---------------------------- | ---------------- | ---------------------------------------------- |
| `--config`                   | `./Hookaidofile` | Path to config file                            |
| `--db`                       | `./hookaido.db`  | Path to SQLite database (used when `queue.backend=sqlite`) |
| `--role`                     | `read`           | Authorization role: `read`, `operate`, `admin` |
| `--enable-mutations`         | `false`          | Enable mutation tools                          |
| `--enable-runtime-control`   | `false`          | Enable runtime control tools                   |
| `--principal`                | —                | Session principal (required for mutations)     |
| `--pid-file`                 | —                | PID file path (required for runtime control)   |
| `--admin-endpoint-allowlist` | —                | Restrict Admin-proxy targets (comma-separated) |

## Tool Reference

### Config Tools

| Tool                 | Role    | Description                                              |
| -------------------- | ------- | -------------------------------------------------------- |
| `config_parse`       | `read`  | Parse Hookaidofile, return AST or errors                 |
| `config_validate`    | `read`  | Validate config, return warnings/errors                  |
| `config_compile`     | `read`  | Compile config, return normalized summary                |
| `config_fmt_preview` | `read`  | Format preview without writing                           |
| `config_diff`        | `read`  | Unified diff between current and candidate content       |
| `config_apply`       | `admin` | Validate + compile + atomic write (with optional reload) |

`config_validate` supports optional `strict_secrets: true` to preflight all compiled secret refs (`env`, `file`, `vault`, `raw`) during validation.

`config_apply` modes:

- `preview_only` — validate/compile only, no write
- `write_only` — atomic write after successful validation
- `write_and_reload` — write + health check + rollback on failure

`write_and_reload` does not send a reload signal to a running process. Runtime adoption happens via file watch (`hookaido run --watch`) or an explicit `instance_reload` call.

### Queue/Admin Read Tools

| Tool                    | Role   | Description                                     |
| ----------------------- | ------ | ----------------------------------------------- |
| `admin_health`          | `read` | Local health snapshot + Admin API probe         |
| `management_model`      | `read` | Application/endpoint projection from config     |
| `dlq_list`              | `read` | List dead-letter queue items                    |
| `messages_list`         | `read` | List queue messages (all states)                |
| `attempts_list`         | `read` | List delivery attempts                          |
| `backlog_top_queued`    | `read` | Top queued route/target buckets                 |
| `backlog_oldest_queued` | `read` | Oldest queued messages                          |
| `backlog_aging_summary` | `read` | Aging distribution with percentiles             |
| `backlog_trends`        | `read` | Trend rollups with signals and operator actions |

### Queue Mutation Tools

| Tool                         | Role      | Description               |
| ---------------------------- | --------- | ------------------------- |
| `dlq_requeue`                | `operate` | Requeue dead-letter items |
| `dlq_delete`                 | `operate` | Delete dead-letter items  |
| `messages_publish`           | `operate` | Publish new messages      |
| `messages_cancel`            | `operate` | Cancel messages by ID     |
| `messages_requeue`           | `operate` | Requeue messages by ID    |
| `messages_resume`            | `operate` | Resume canceled messages  |
| `messages_cancel_by_filter`  | `operate` | Bulk cancel by filter     |
| `messages_requeue_by_filter` | `operate` | Bulk requeue by filter    |
| `messages_resume_by_filter`  | `operate` | Bulk resume by filter     |

### Management Mutation Tools

| Tool                         | Role    | Description                         |
| ---------------------------- | ------- | ----------------------------------- |
| `management_endpoint_upsert` | `admin` | Map application/endpoint to a route |
| `management_endpoint_delete` | `admin` | Remove endpoint mapping             |

### Runtime Control Tools

| Tool                 | Role      | Description                   |
| -------------------- | --------- | ----------------------------- |
| `instance_status`    | `operate` | Process/listener/queue status |
| `instance_logs_tail` | `operate` | Bounded log tail              |
| `instance_start`     | `admin`   | Start the Hookaido process    |
| `instance_stop`      | `admin`   | Stop the Hookaido process     |
| `instance_reload`    | `admin`   | Reload configuration          |

## Queue Backend Awareness

MCP tools adapt to the configured queue backend:

| Backend  | Behavior                                                    |
| -------- | ----------------------------------------------------------- |
| `sqlite` | Direct SQLite access (no running instance needed for reads) |
| `memory` | Proxy via configured Admin API (running instance required)  |
| `postgres` | Proxy via configured Admin API (running instance required) |

In Admin-proxy mode:

- Read tools retry transient failures (408/429/5xx) with bounded backoff
- Errors map to explicit MCP error messages with structured metadata
- `--admin-endpoint-allowlist` restricts which Admin API targets may be proxied

Worker gRPC lease operations (`dequeue`, `ack`, `nack`, `extend`) are intentionally out of MCP scope. MCP keeps lease mutation control on existing Admin/Pull operational paths.

## Audit

All mutating tool calls emit structured JSONL audit events to stderr:

```json
{
  "timestamp": "2026-02-09T10:00:00Z",
  "principal": "ops@example.test",
  "role": "operate",
  "tool": "messages_publish",
  "input_hash": "sha256:abc...",
  "result": "ok",
  "duration_ms": 42,
  "metadata": { ... }
}
```

Mutation tools require `reason` (max 512 chars). Optional `actor` and `request_id` (max 256 chars each).

When `actor` is omitted, MCP derives it from `--principal`. Explicit actor must match `--principal` exactly.

Mutation responses include `audit.principal` in structured output for client consumption.

## Managed Selectors

Queue read and mutation tools support managed selectors (`application` + `endpoint_name`) as alternatives to `route`:

```json
{
  "application": "billing",
  "endpoint_name": "invoice.created",
  "state": "dead",
  "limit": 100
}
```

Selector rules:

- `route` and `application`+`endpoint_name` are mutually exclusive
- Labels must match `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`
- `route` must start with `/` when provided

`messages_publish` supports both direct routing (`route` + `target`) and managed routing (`application` + `endpoint_name`), but not mixed in the same call.

## Guardrails

- Config writes are restricted to the configured `--config` path
- Admin-proxy targets are restricted by `--admin-endpoint-allowlist`
- List operations hard-capped at `limit <= 1000`
- Log tailing is bounded
- All mutation tools reject unknown arguments
- Secrets are always redacted in MCP responses

## Example: AI-Assisted Queue Triage

```
Agent: Use admin_health to check system status.
→ { ok: true, queue: { total: 1523, dead: 23 }, trend_signals: [...] }

Agent: Use backlog_trends to inspect the last 2 hours.
→ { signals: ["queued_pressure"], operator_actions: [{ action: "check_consumers", ... }] }

Agent: Use dlq_list to see failed messages.
→ { items: [{ id: "evt_1", dead_reason: "max_retries", ... }] }

Agent: Use dlq_requeue to retry failed messages.
→ { changed: 5 }
```

---

← [Documentation Index](index.md)
