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

## CI/CD Job Queue (Internal Channel)

Use an internal channel as a durable job queue. Jobs are published via Admin API and consumed by gRPC workers, with dead-lettering for failed jobs.

```hcl
admin_api {
  listen :2019
}

queue {
  backend postgres
  dead_letter {
    max_retries 5
  }
}

internal {
  /jobs/deploy {
    pull { path /pull/deploy }
  }

  /jobs/reports {
    deliver "https://reports.internal/generate" {
      timeout 120s
      retry exponential max 3 base 5s cap 1m jitter 0.1
    }
  }
}
```

**Publish a job:**

```bash
curl -X POST http://localhost:2019/messages/publish \
  -H "Content-Type: application/json" \
  -d '{
    "route": "/jobs/deploy",
    "payload": {"ref": "v2.1.0", "env": "production"}
  }'
```

**Key points:**
- Internal channels have no ingress listener — jobs enter only via Admin API or gRPC
- `/jobs/deploy` uses pull mode: workers lease and ack jobs at their own pace
- `/jobs/reports` uses push mode: Hookaido delivers with retry and timeout
- Failed jobs land in DLQ after 5 attempts, recoverable via `POST /dlq/requeue`

---

## Next Steps

- [Getting Started](getting-started.md) — first run and local validation
- [Configuration Reference](configuration.md) — full DSL documentation
- [Delivery](delivery.md) — push, exec, retry, and signing details
- [Deployment Modes](deployment-modes.md) — pull vs push topology

---

← [Documentation Index](index.md)
