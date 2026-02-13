# Observability

Hookaido provides structured logging, Prometheus metrics, and OpenTelemetry tracing, all configurable via the `observability` block.

## Quick Start

```hcl
observability {
  access_log {
    enabled on
    output stderr
    format json
  }

  runtime_log {
    level info
    output stderr
    format json
  }

  metrics {
    listen ":9900"
    prefix "/metrics"
  }

  tracing {
    enabled on
    collector "https://otel.example.com/v1/traces"
  }
}
```

## Logging

Hookaido produces two log streams, both structured JSON:

### Access Log

Per-request logs for ingress, Pull API, and Admin API.

**Shorthand:**

```hcl
observability {
  access_log on    # enable to stderr with JSON format
}
```

**Block form:**

```hcl
observability {
  access_log {
    enabled on
    output stderr       # stdout, stderr, or file
    path /var/log/hookaido/access.log   # required when output=file
    format json
  }
}
```

### Runtime Log

Application-level structured logs (startup, reload, errors, queue events).

**Shorthand:**

```hcl
observability {
  runtime_log info    # level as shorthand: debug, info, warn, error, off
}
```

**Block form:**

```hcl
observability {
  runtime_log {
    level info         # debug, info, warn, error, off
    output stderr      # stdout, stderr, or file
    path /var/log/hookaido/runtime.log
    format json
  }
}
```

### Log Sinks

| Sink     | Description                   |
| -------- | ----------------------------- |
| `stdout` | Standard output               |
| `stderr` | Standard error (default)      |
| `file`   | File output (requires `path`) |

The `--log-level` CLI flag overrides the runtime log level from config.

## Metrics

Prometheus-compatible metrics endpoint.

```hcl
observability {
  metrics {
    listen ":9900"           # default: 127.0.0.1:9900
    prefix "/metrics"        # default: /metrics
    enabled on               # explicitly enable/disable
  }
}
```

Set `enabled off` to disable the metrics listener while keeping config in place.

### Available Metrics

**Queue metrics:**

| Metric                          | Type    | Description                                   |
| ------------------------------- | ------- | --------------------------------------------- |
| `hookaido_queue_depth`          | gauge   | Current items by state (queued, leased, dead) |
| `hookaido_queue_total`          | gauge   | Current total items across all queue states   |
| `hookaido_queue_oldest_queued_age_seconds` | gauge | Age of the oldest queued item in seconds |
| `hookaido_queue_ready_lag_seconds` | gauge | Ready lag of the earliest runnable queued item in seconds |

**Ingress metrics:**

| Metric                            | Type    | Description                                       |
| --------------------------------- | ------- | ------------------------------------------------- |
| `hookaido_ingress_accepted_total` | counter | Ingress requests accepted and enqueued            |
| `hookaido_ingress_rejected_total` | counter | Ingress requests rejected (auth, rate-limit, etc) |
| `hookaido_ingress_rejected_by_reason_total{reason,status}` | counter | Ingress rejects by normalized reason + status |
| `hookaido_ingress_enqueued_total` | counter | Items enqueued via ingress (>accepted if fanout)  |
| `hookaido_ingress_adaptive_backpressure_total{reason}` | counter | Ingress requests rejected by adaptive backpressure (by trigger reason) |
| `hookaido_ingress_adaptive_backpressure_applied_total` | counter | Total ingress requests rejected by adaptive backpressure |

**Delivery metrics:**

| Metric                             | Type    | Description                    |
| ---------------------------------- | ------- | ------------------------------ |
| `hookaido_delivery_attempts_total` | counter | Total push delivery attempts   |
| `hookaido_delivery_acked_total`    | counter | Deliveries acknowledged (2xx)  |
| `hookaido_delivery_retry_total`    | counter | Deliveries scheduled for retry |
| `hookaido_delivery_dead_total`     | counter | Deliveries moved to DLQ        |

**Pull metrics:**

| Metric                                | Type    | Description                                                                  |
| ------------------------------------- | ------- | ---------------------------------------------------------------------------- |
| `hookaido_pull_dequeue_total`         | counter | Pull dequeue requests by `route` and status label (`200`, `204`, `4xx`, `5xx`) |
| `hookaido_pull_acked_total`           | counter | Successful Pull ack operations by `route`                                    |
| `hookaido_pull_nacked_total`          | counter | Successful Pull nack/mark-dead operations by `route`                         |
| `hookaido_pull_ack_conflict_total`    | counter | Pull ack lease conflicts (`409`) by `route`                                  |
| `hookaido_pull_nack_conflict_total`   | counter | Pull nack lease conflicts (`409`) by `route`                                 |
| `hookaido_pull_lease_active`          | gauge   | Active Pull leases currently tracked by `route`                              |
| `hookaido_pull_lease_expired_total`   | counter | Lease expirations observed during Pull `ack`/`nack`/`extend` by `route`      |

**Store/SQLite metrics:**

| Metric                                           | Type      | Description                                                                |
| ------------------------------------------------ | --------- | -------------------------------------------------------------------------- |
| `hookaido_store_sqlite_write_seconds`            | histogram | SQLite write transaction duration (queue mutation paths)                   |
| `hookaido_store_sqlite_dequeue_seconds`          | histogram | SQLite dequeue transaction duration                                        |
| `hookaido_store_sqlite_checkpoint_seconds`       | histogram | SQLite WAL checkpoint duration (periodic passive checkpoints)              |
| `hookaido_store_sqlite_busy_total`               | counter   | SQLite busy/locked errors observed in instrumented paths                   |
| `hookaido_store_sqlite_retry_total`              | counter   | SQLite begin-transaction retry attempts after busy/locked errors           |
| `hookaido_store_sqlite_tx_commit_total`          | counter   | Committed SQLite transactions in instrumented queue paths                  |
| `hookaido_store_sqlite_tx_rollback_total`        | counter   | Rolled-back SQLite transactions in instrumented queue paths                |
| `hookaido_store_sqlite_checkpoint_total`         | counter   | Successful periodic SQLite WAL checkpoints                                 |
| `hookaido_store_sqlite_checkpoint_errors_total`  | counter   | Failed periodic SQLite WAL checkpoints                                     |

**Publish metrics:**

| Metric                                       | Type    | Description                       |
| -------------------------------------------- | ------- | --------------------------------- |
| `hookaido_publish_accepted_total`            | counter | Accepted publish mutations        |
| `hookaido_publish_rejected_total`            | counter | Rejected publish mutations        |
| `hookaido_publish_rejected_validation_total` | counter | Rejections: validation errors     |
| `hookaido_publish_rejected_policy_total`     | counter | Rejections: policy violations     |
| `hookaido_publish_rejected_conflict_total`   | counter | Rejections: duplicate IDs         |
| `hookaido_publish_rejected_queue_full_total` | counter | Rejections: queue at capacity     |
| `hookaido_publish_rejected_store_total`      | counter | Rejections: store errors          |
| `hookaido_publish_scoped_accepted_total`     | counter | Accepted scoped (managed) publish |
| `hookaido_publish_scoped_rejected_total`     | counter | Rejected scoped (managed) publish |

**Tracing diagnostics:**

| Metric                                 | Type    | Description                     |
| -------------------------------------- | ------- | ------------------------------- |
| `hookaido_tracing_enabled`             | gauge   | Whether tracing is configured   |
| `hookaido_tracing_init_failures_total` | counter | Tracing initialization failures |
| `hookaido_tracing_export_errors_total` | counter | Tracing export errors           |

## Tracing

OpenTelemetry OTLP/HTTP traces for request-level observability. HTTP servers (ingress, Pull API, Admin API) and the outbound push dispatcher client are instrumented.

### Minimal Config

```hcl
observability {
  tracing {
    enabled on
    collector "https://otel.example.com/v1/traces"
  }
}
```

### Full Config

```hcl
observability {
  tracing {
    enabled on
    collector "https://otel.example.com/v1/traces"
    url_path "/v1/traces"
    timeout "10s"
    compression gzip           # none or gzip
    insecure off               # allow plain HTTP (dev only)

    # TLS options
    tls {
      ca_file /path/to/ca.pem
      cert_file /path/to/cert.pem
      key_file /path/to/key.pem
      server_name "otel.example.com"
      insecure_skip_verify off
    }

    # Proxy
    proxy_url "http://proxy.internal:3128"

    # Retry on export failure
    retry {
      enabled on
      initial_interval "5s"
      max_interval "30s"
      max_elapsed_time "1m"
    }

    # Custom headers (e.g., for auth)
    header "Authorization" "Bearer otel-token"
    header "X-Custom-Header" "value"
  }
}
```

| Directive                  | Default      | Description                       |
| -------------------------- | ------------ | --------------------------------- |
| `enabled`                  | `off`        | Enable/disable tracing            |
| `collector`                | —            | OTLP/HTTP collector endpoint      |
| `url_path`                 | `/v1/traces` | URL path on the collector         |
| `timeout`                  | `10s`        | Export timeout                    |
| `compression`              | `none`       | `none` or `gzip`                  |
| `insecure`                 | `off`        | Allow HTTP (non-TLS) transport    |
| `proxy_url`                | —            | HTTP proxy for exporter           |
| `tls.ca_file`              | —            | CA certificate file for TLS       |
| `tls.cert_file`            | —            | Client certificate file for mTLS  |
| `tls.key_file`             | —            | Client key file for mTLS          |
| `tls.server_name`          | —            | Override TLS server name          |
| `tls.insecure_skip_verify` | `off`        | Skip TLS certificate verification |
| `retry.enabled`            | `off`        | Retry failed exports              |
| `retry.initial_interval`   | —            | First retry delay                 |
| `retry.max_interval`       | —            | Maximum retry delay               |
| `retry.max_elapsed_time`   | —            | Total retry time budget           |
| `header`                   | —            | Custom HTTP headers (repeatable)  |

> Header entries must be valid HTTP header name/value pairs. Invalid entries fail config validation.

## Health Diagnostics

The Admin API health endpoint (`GET /healthz?details=1`) aggregates observability data:

- Queue state rollups with age/lag indicators
- Backlog trend signals with operator action playbooks
- Tracing counters (init failures, export errors)
- Ingress adaptive-backpressure diagnostics (`adaptive_backpressure_applied_total`, `adaptive_backpressure_by_reason`)
- Top route/target backlog buckets
- Queue diagnostics are cached (short TTL) and served stale-while-refresh under heavy load to keep control-plane endpoints responsive.

Operational guidance for control-plane responsiveness:

- Keep SLO probes on `GET /healthz` (without details) for the fastest liveness path.
- Use `GET /healthz?details=1` and `GET /metrics` for diagnostics/monitoring; under queue saturation these endpoints prioritize bounded latency over strictly real-time queue snapshots.

## Saturation Notes

Queue saturation analysis showed one hot path in the ingest/admission write flow: with `queue_limits.max_depth` enabled, each enqueue previously executed `COUNT(*)` over active queue states (`queued` + `leased`) inside a write transaction.

At high occupancy, that repeated count increased write transaction time and lock contention (`hookaido_store_sqlite_write_seconds`, `hookaido_store_sqlite_busy_total`, `hookaido_store_sqlite_retry_total`).

Hookaido now maintains O(1) active-depth counters (`queue_counters`) via SQLite triggers and uses them for `max_depth` admission checks.

To validate improvements in your environment, compare before/after load runs using:
- p95/p99 ingress latency and 503 rate
- `hookaido_store_sqlite_write_seconds` histogram shape
- `hookaido_store_sqlite_busy_total` and `hookaido_store_sqlite_retry_total` growth rate

See [Admin API](admin-api.md) for details.

## Audit Logging

All Admin API and MCP mutations emit structured JSONL audit events (to stderr or configured runtime log):

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

Audit metadata varies by operation:

- **Config mutations:** `config_mutation` (operation, mode, outcome)
- **Runtime control:** `runtime_control` (operation, outcome)
- **ID-based mutations:** `id_mutation` (operation, IDs requested/unique/changed)
- **Filter mutations:** `filter_mutation` (operation, matched/changed, preview flag)
- **Publish:** `admin_proxy_publish` (rollback counters, if Admin-proxy mode)

---

← [Documentation Index](index.md)
