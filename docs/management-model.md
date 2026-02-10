# Management Model

The management model lets you organize webhook routes into **applications** and **endpoints** — a higher-level abstraction for multi-tenant or multi-service setups.

## Overview

By default, Hookaido routes are identified by their path (`/webhooks/github`). Management labels add a logical layer:

```hcl
/webhooks/billing {
  application "billing"
  endpoint_name "invoice.created"
  pull { path /pull/billing-invoices }
}

/webhooks/billing-refunds {
  application "billing"
  endpoint_name "refund.processed"
  pull { path /pull/billing-refunds }
}
```

This creates:

- **Application** `billing` with two **endpoints**: `invoice.created` and `refund.processed`
- Each endpoint maps to a specific route

### Labels on Outbound and Internal Channels

Management labels also work on `outbound` and `internal` routes:

```hcl
outbound /notifications/slack {
  application "notifications"
  endpoint_name "slack.alerts"
  deliver "https://hooks.slack.com/services/..." { timeout 5s }
}

internal /jobs/nightly-report {
  application "jobs"
  endpoint_name "nightly-report"
  pull { path /pull/nightly }
}
```

This enables endpoint-scoped publish and filter operations on any channel type.

## Label Format

Both `application` and `endpoint_name`:

- Must be set together (one without the other is a config error)
- Must match `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`
- Are URL-segment-safe (used in Admin API paths)

Valid examples: `billing`, `my-service`, `payments.v2`, `org:team:hook`

## Why Use Management Labels?

1. **Endpoint-scoped operations** — publish, cancel, requeue, and resume via `/applications/{app}/endpoints/{ep}/messages/...` without knowing the underlying route path.
2. **Managed publish** — the endpoint-scoped publish API (`POST .../messages/publish`) auto-resolves the target route and enforces scoped access control.
3. **Ownership enforcement** — global filter mutations that affect managed routes are redirected to use `application` + `endpoint_name` selectors, preventing accidental cross-tenant operations.
4. **Application views** — query the management model to see all applications, their endpoints, routes, and publish policies.

## Querying the Management Model

### List All Applications

```bash
curl http://127.0.0.1:2019/applications
```

```json
{
  "applications": [
    { "name": "billing", "path_count": 2 },
    { "name": "notifications", "path_count": 1 }
  ]
}
```

### List Endpoints for an Application

```bash
curl http://127.0.0.1:2019/applications/billing/endpoints
```

```json
{
  "endpoints": [
    {
      "name": "invoice.created",
      "route": "/webhooks/billing",
      "mode": "pull",
      "targets": ["pull"],
      "publish_policy": {
        "enabled": true,
        "direct_enabled": true,
        "managed_enabled": true
      }
    },
    {
      "name": "refund.processed",
      "route": "/webhooks/billing-refunds",
      "mode": "pull",
      "targets": ["pull"],
      "publish_policy": {
        "enabled": true,
        "direct_enabled": true,
        "managed_enabled": true
      }
    }
  ]
}
```

### Full Management Model

```bash
curl http://127.0.0.1:2019/management/model
```

Returns the complete projection: application count, endpoint count, and nested application→endpoint→route mappings with publish policies.

## Endpoint-Scoped Operations

With management labels, use endpoint-scoped Admin API paths:

### Publish Messages

```bash
curl -X POST \
  http://127.0.0.1:2019/applications/billing/endpoints/invoice.created/messages/publish \
  -H "Content-Type: application/json" \
  -H "X-Hookaido-Audit-Reason: backfill" \
  -d '{
    "items": [
      {
        "id": "evt_backfill_1",
        "payload_b64": "eyJpbnZvaWNlIjogIklOVi0xMjMifQ==",
        "headers": { "Content-Type": "application/json" }
      }
    ]
  }'
```

> Do **not** include `route`, `application`, or `endpoint_name` in items — the scope is determined by the URL path. Selector hints are rejected.

### List Messages

```bash
curl "http://127.0.0.1:2019/applications/billing/endpoints/invoice.created/messages?state=queued&limit=50"
```

> Do **not** include `route`, `application`, or `endpoint_name` in query params.

### Filter Mutations

```bash
# Cancel all dead messages for this endpoint
curl -X POST \
  http://127.0.0.1:2019/applications/billing/endpoints/invoice.created/messages/cancel_by_filter \
  -H "Content-Type: application/json" \
  -H "X-Hookaido-Audit-Reason: cleanup" \
  -d '{ "state": "dead", "limit": 100 }'
```

Available scoped filter operations:

- `.../messages/cancel_by_filter`
- `.../messages/requeue_by_filter`
- `.../messages/resume_by_filter`

## Managed Selectors on Global APIs

Global Admin API endpoints like `GET /messages` and `POST /messages/*_by_filter` also accept managed selectors as an alternative to `route`:

```bash
# List messages by managed selector (instead of route)
curl "http://127.0.0.1:2019/messages?application=billing&endpoint_name=invoice.created&state=queued"
```

Selector rules:

- `route` and `application`+`endpoint_name` are **mutually exclusive**
- Route selectors that resolve to management-labeled routes are **rejected** — use managed selectors instead
- Unscoped selectors (no filter) are rejected when managed routes exist — scope is required

## Endpoint Mapping Lifecycle

Mappings can be managed dynamically via the Admin API:

### Upsert Mapping

```bash
curl -X PUT \
  http://127.0.0.1:2019/applications/billing/endpoints/invoice.created \
  -H "Content-Type: application/json" \
  -H "X-Hookaido-Audit-Reason: initial setup" \
  -d '{ "route": "/webhooks/billing" }'
```

The target route must:

- Be an absolute path starting with `/`
- Have `publish on` and `publish.managed on` (not disabled)
- Not already be mapped to another endpoint

The operation performs an atomic config write + reload, with rollback on failure.

### Delete Mapping

```bash
curl -X DELETE \
  http://127.0.0.1:2019/applications/billing/endpoints/invoice.created \
  -H "X-Hookaido-Audit-Reason: decommission"
```

Removes the `application` and `endpoint_name` labels from the mapped route in the Hookaidofile.

## Publish Policy

Control who can publish to managed endpoints:

```hcl
defaults {
  publish_policy {
    managed on               # master switch for managed publish
    require_actor on                  # require X-Hookaido-Audit-Actor
    require_request_id on             # require X-Request-ID
    actor_allow "ci-bot"     # explicit actor allowlist
    actor_prefix "deploy-"   # actor prefix match
    fail_closed off          # fail closed when context unavailable
  }
}
```

Per-route publish control:

```hcl
/webhooks/billing {
  application "billing"
  endpoint_name "invoice.created"
  publish {
    enabled on              # master switch (default on)
    managed on              # managed publish path (default on)
    direct on               # global direct publish path (default on)
  }
  pull { path /pull/billing }
}
```

| Directive                 | Default | Scope                                 |
| ------------------------- | ------- | ------------------------------------- |
| `publish { enabled off }` | `on`    | Block all manual publish              |
| `publish { direct off }`  | `on`    | Block global `POST /messages/publish` |
| `publish { managed off }` | `on`    | Block endpoint-scoped publish         |

## Error Codes (Management)

| Code                                | Status | Description                                     |
| ----------------------------------- | ------ | ----------------------------------------------- |
| `managed_selector_required`         | 400    | Must use `application`+`endpoint_name` selector |
| `scoped_publish_required`           | 400    | Must use endpoint-scoped publish path           |
| `managed_endpoint_not_found`        | 404    | Managed selector does not resolve               |
| `managed_resolver_missing`          | 503    | Resolver context unavailable                    |
| `management_route_already_mapped`   | 409    | Route already mapped to another endpoint        |
| `management_route_publish_disabled` | 409    | Route has managed publish disabled              |
| `management_route_target_mismatch`  | 409    | Route mode/targets differ on move               |
| `audit_actor_not_allowed`           | 403    | Actor not in scoped allowlist/prefix            |

---

← [Documentation Index](index.md)
