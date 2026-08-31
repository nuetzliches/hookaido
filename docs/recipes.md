# Recipes

Practical patterns for common webhook scenarios. Each recipe is a self-contained Hookaidofile snippet you can adapt.

## GitHub Push → Deploy Script

Receive GitHub push events and trigger a local deploy script. Uses provider-compatible HMAC verification and subprocess delivery — no HTTP server needed on the receiving end.

```hcl
/webhooks/github {
  match {
    header X-GitHub-Event push
  }

  auth hmac {
    provider github
    secret env:GITHUB_WEBHOOK_SECRET
  }

  deliver exec "/opt/hooks/deploy.sh" {
    timeout 60s
    retry exponential max 3 base 2s cap 30s jitter 0.2
    env DEPLOY_ENV production
    env NOTIFY_SLACK {env.SLACK_WEBHOOK_URL}
  }
}
```

**Handler script** (`/opt/hooks/deploy.sh`):

```bash
#!/usr/bin/env bash
set -euo pipefail

# Payload arrives on stdin as JSON
PAYLOAD=$(cat)
REF=$(echo "$PAYLOAD" | jq -r '.ref')
REPO=$(echo "$PAYLOAD" | jq -r '.repository.full_name')

if [[ "$REF" != "refs/heads/main" ]]; then
  echo "Skipping non-main push: $REF" >&2
  exit 0
fi

echo "Deploying $REPO..." >&2
cd /opt/app && git pull origin main && make deploy
```

**Key points:**
- Exit code `0` = ack, non-zero = retry with backoff
- `HOOKAIDO_HEADER_X_GITHUB_EVENT` is available as env var
- Provider mode skips replay protection (GitHub does not send timestamps)

---

## Stripe → Billing Service

Forward Stripe webhook events to an internal billing service with push delivery, outbound HMAC signing, and aggressive retry policy.

```hcl
/webhooks/stripe {
  auth hmac env:STRIPE_SIGNING_SECRET

  deliver "https://billing.internal/hooks/stripe" {
    timeout 10s
    retry exponential max 8 base 2s cap 2m jitter 0.2

    sign hmac env:HOOKAIDO_DELIVER_SECRET

    header "X-Source" "hookaido"
    header "X-Route" "{route}"
  }
}
```

**Key points:**
- Inbound: Hookaido verifies Stripe's HMAC signature
- Outbound: re-signs the payload with a separate secret for internal auth
- Custom headers propagate routing metadata to the billing service
- 8 retries with exponential backoff cover transient failures (total ~8.5 min window)

---

## Multi-Provider Fan-Out (Pull Mode)

Receive webhooks from multiple providers into separate routes, consumed by a single internal worker through pull endpoints.

```hcl
ingress {
  listen :8080
}

pull_api {
  listen :9443
  auth token env:HOOKAIDO_PULL_TOKEN
}

/webhooks/github {
  auth hmac {
    provider github
    secret env:GITHUB_SECRET
  }
  pull { path /pull/github }
}

/webhooks/gitlab {
  auth hmac env:GITLAB_SECRET
  pull { path /pull/gitlab }
}

/webhooks/bitbucket {
  auth hmac env:BITBUCKET_SECRET
  pull { path /pull/bitbucket }
}
```

**Worker pseudocode:**

```python
import requests

ENDPOINTS = ["/pull/github", "/pull/gitlab", "/pull/bitbucket"]
BASE = "https://hookaido.dmz:9443"
TOKEN = os.environ["HOOKAIDO_PULL_TOKEN"]

for endpoint in ENDPOINTS:
    resp = requests.post(f"{BASE}{endpoint}/dequeue",
        headers={"Authorization": f"Bearer {TOKEN}"},
        json={"batch": 10, "lease_ttl": "30s"})

    for item in resp.json()["items"]:
        process(item)
        requests.post(f"{BASE}{endpoint}/ack",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json={"lease_id": item["lease_id"]})
```

**Key points:**
- Each provider gets its own route and pull endpoint
- Single worker polls all endpoints (or use separate workers per provider)
- All traffic is outbound from internal network — no inbound firewall rules needed

---

## One Source, Several Independent Consumers (Consumer Groups)

The recipe above fans *inbound* traffic out across routes. This one fans a *single* route out across consumers — for a source that can only be handed one webhook URL (an appliance, a telephony platform, a vendor portal with a single "URL" field) while two environments each need every event.

Attaching both to one pull endpoint would not do it: the queue is competing-consumer, so they would split the traffic and each would see a fluctuating fraction arrive. Declare a group per consumer instead:

```hcl
pull_api {
  listen :9443
  prefix /pull
  auth token env:HOOKAIDO_PULL_TOKEN
}

/webhooks/appliance {
  auth query "t" env:APPLIANCE_URL_TOKEN
  pull {
    path /appliance
    consumer_group "integration"
    consumer_group "workstation"
  }
}
```

Each group is its own queue with its own endpoint:

```python
# The long-lived integration environment
BASE = "https://hookaido.dmz:9443/pull/appliance/integration"

# A developer machine, receiving the same events independently
BASE = "https://hookaido.dmz:9443/pull/appliance/workstation"
```

**Key points:**
- Every event is enqueued once per group, so both consumers receive all of it
- Within a group, workers still compete — scale a group by adding workers to it
- The bare `/pull/appliance/...` path stops resolving once groups exist, so a consumer that was not migrated fails with `404` instead of quietly taking half the traffic
- A group whose consumer is down accumulates its own backlog; it does not hold up the other group
- See [Consumer Groups](pull-api.md#consumer-groups) for the full semantics, including why groups are not an authorization boundary

---

## CI/CD Job Queue (Internal Channel)

Use an internal channel as a durable job queue. Jobs are published via Admin API and consumed by gRPC workers, with dead-lettering for failed jobs.

```hcl
admin_api {
  listen :2019
  auth token "env:HOOKAIDO_ADMIN_TOKEN"
}

pull_api {
  listen :8081
  auth token "env:HOOKAIDO_PULL_TOKEN"
}

dlq_retention {
  max_age 30d
  max_depth 10000
}

internal {
  /jobs/deploy {
    queue { backend postgres }
    pull { path /pull/deploy }
  }
}

outbound {
  /jobs/reports {
    queue { backend postgres }
    deliver "https://reports.internal/generate" {
      timeout 120s
      retry exponential max 5 base 5s cap 1m jitter 0.1
    }
  }
}
```

Note that the two jobs live in different channels. `internal` routes must use `pull` and forbid `deliver`; `outbound` routes are the reverse. See [Deployment Modes](deployment-modes.md) for the full channel constraint matrix.

**Publish a job:**

```bash
curl -X POST http://localhost:2019/messages/publish \
  -H "Authorization: Bearer $HOOKAIDO_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Hookaido-Audit-Reason: deploy v2.1.0 to production" \
  -d '{
    "items": [
      {
        "route": "/jobs/deploy",
        "target": "pull",
        "payload_b64": "eyJyZWYiOiJ2Mi4xLjAiLCJlbnYiOiJwcm9kdWN0aW9uIn0="
      }
    ]
  }'
```

`payload_b64` is the base64 encoding of `{"ref":"v2.1.0","env":"production"}`. The `X-Hookaido-Audit-Reason` header is required on every publish; see [Admin API](admin-api.md#post-messagespublish) for the full request shape.

**Key points:**
- Internal channels have no ingress listener — jobs enter only via Admin API or gRPC
- `/jobs/deploy` uses pull mode: workers lease and ack jobs at their own pace
- `/jobs/reports` uses push mode: Hookaido delivers with retry and timeout
- Failed jobs land in DLQ after the configured `retry ... max` attempts, recoverable via `POST /dlq/requeue`

---

## Next Steps

- [Getting Started](getting-started.md) — first run and local validation
- [Configuration Reference](configuration.md) — full DSL documentation
- [Delivery](delivery.md) — push, exec, retry, and signing details
- [Deployment Modes](deployment-modes.md) — pull vs push topology

---

← [Documentation Index](index.md)
