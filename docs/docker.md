# Docker Quickstart

Run Hookaido without installing Go. Docker is enough.

## Use the Official Image (Recommended)

Pull from GitHub Container Registry (GHCR):

```bash
docker pull ghcr.io/nuetzliches/hookaido:latest
```

Tag guidance:

- `:latest` tracks the newest stable release.
- `:vX.Y.Z` pins an exact release (recommended for production).
- Digest pinning (`@sha256:...`) is the strongest immutable option.

Published architectures:

- `linux/amd64`
- `linux/arm64`

## Run with a Hookaidofile

1. Create a `Hookaidofile` in your project directory (see [Getting Started](getting-started.md)).
2. Set environment variables and start the container:

```bash
docker run -d \
  --name hookaido \
  -p 8080:8080 \
  -p 9443:9443 \
  -p 2019:2019 \
  -e HOOKAIDO_PULL_TOKEN=mytoken \
  -e HOOKAIDO_INGRESS_SECRET=mysecret \
  -v $(pwd)/Hookaidofile:/app/Hookaidofile:ro \
  -v hookaido-data:/app/.data \
  ghcr.io/nuetzliches/hookaido:latest
```

This mounts config as read-only and persists SQLite data in a named volume.

## Docker Compose

```yaml
# docker-compose.yml
services:
  hookaido:
    image: ghcr.io/nuetzliches/hookaido:latest
    ports:
      - "8080:8080" # Ingress
      - "9443:9443" # Pull API
      - "2019:2019" # Admin API
    environment:
      HOOKAIDO_PULL_TOKEN: ${HOOKAIDO_PULL_TOKEN}
      HOOKAIDO_INGRESS_SECRET: ${HOOKAIDO_INGRESS_SECRET}
    volumes:
      - ./Hookaidofile:/app/Hookaidofile:ro
      - hookaido-data:/app/.data
    restart: unless-stopped

volumes:
  hookaido-data:
```

Start with:

```bash
docker compose up -d
```

## Build Locally (Optional)

If you want to test local Dockerfile changes:

```bash
docker build -t hookaido:local .
```

Or with explicit build metadata:

```bash
docker build \
  --build-arg VERSION=v0.1.0 \
  --build-arg COMMIT=$(git rev-parse --short HEAD) \
  --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
  -t hookaido:local .
```

Run local build:

```bash
docker run -d \
  --name hookaido-local \
  -p 8080:8080 -p 9443:9443 -p 2019:2019 \
  -e HOOKAIDO_PULL_TOKEN=mytoken \
  -v $(pwd)/Hookaidofile:/app/Hookaidofile:ro \
  -v hookaido-data:/app/.data \
  hookaido:local
```

## Health Check

```bash
curl http://localhost:2019/healthz
```

## Hot Reload

Mount config read-write and pass `--watch`:

```bash
docker run -d \
  --name hookaido \
  -p 8080:8080 -p 9443:9443 -p 2019:2019 \
  -e HOOKAIDO_PULL_TOKEN=mytoken \
  -v $(pwd)/Hookaidofile:/app/Hookaidofile \
  -v hookaido-data:/app/.data \
  ghcr.io/nuetzliches/hookaido:latest \
  run --config /app/Hookaidofile --db /app/.data/hookaido.db --watch
```

## Production Notes

- Use a named volume (not a bind mount) for `/app/.data` to keep SQLite WAL durable.
- The image runs as non-root user `hookaido`.
- For TLS, mount cert/key files and reference them in your `Hookaidofile`.
- Admin API defaults to `127.0.0.1:2019`. To expose it from Docker, set `admin_api { listen :2019 }` in your Hookaidofile.

---

- [Documentation Index](index.md)
