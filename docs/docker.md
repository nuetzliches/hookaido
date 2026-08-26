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
  --build-arg VERSION=v2.0.0 \
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

Mount the config **directory** — not the config file — read-write, and pass `--watch`:

```bash
docker run -d \
  --name hookaido \
  -p 8080:8080 -p 9443:9443 -p 2019:2019 \
  -e HOOKAIDO_PULL_TOKEN=mytoken \
  -v $(pwd)/config:/app/config \
  -v hookaido-data:/app/.data \
  ghcr.io/nuetzliches/hookaido:latest \
  run --config /app/config/Hookaidofile --db /app/.data/hookaido.db --watch
```

The directory mount is the part that matters. `--watch` watches the directory
containing the config and filters by basename, which is what makes an atomic
replace-by-rename work. With a **single-file** bind mount —
`-v $(pwd)/Hookaidofile:/app/Hookaidofile` — `/app` inside the container is the
container's own directory holding one bind-mounted entry, so replacing the file
on the host creates a new inode that the existing mount does not resolve to. The
container's directory genuinely did not change, no event arrives, and `--watch`
silently never fires: a new route stays `404` while `watching_config` sits in the
log.

Kubernetes has the same failure mode with `subPath` ConfigMap mounts, which are
documented not to receive updates. Mount the ConfigMap as a volume instead.

If the mount shape is not yours to change, add a polling fallback:

```bash
  run --config /app/Hookaidofile --db /app/.data/hookaido.db --watch --watch-interval 30s
```

`--watch-interval` re-reads the config path on that interval and reloads when its
content hash changes, through the same path as an fsnotify event. Without it,
Hookaido logs `watch_may_not_fire` at startup on Linux when it detects that the
config file is on a different filesystem than its own directory — the signature
of a single-file mount.

## Production Notes

- Use a named volume (not a bind mount) for `/app/.data` to keep SQLite WAL durable.
- With `--watch`, mount the config **directory**, not the config file. A single-file bind mount cannot deliver file-change events; see [Hot Reload](#hot-reload).
- The image starts as root to fix volume ownership (`chown`), then drops to non-root user `hookaido` (UID 1000) via `su-exec`. This prevents `SQLITE_CANTOPEN` errors when Docker creates volumes as `root:root`. If you run with `--user hookaido`, the entrypoint skips `chown` and runs directly.
- For TLS, mount cert/key files and reference them in your `Hookaidofile`.
- Behind a reverse proxy, `match remote_ip` sees the **proxy** address, not the client — every request arrives from the proxy. Either put the IP restriction in the proxy, or set `ingress { trusted_proxies "..." }` so Hookaido reads the client address from `X-Forwarded-For`. See [`remote_ip` behind a reverse proxy](ingress.md#remote_ip-behind-a-reverse-proxy).
- Admin API defaults to `127.0.0.1:2019`, which inside a container is not reachable from the host. To expose it, bind a wider address **and set a token** — `admin_api { listen :2019, auth token "env:HOOKAIDO_ADMIN_TOKEN" }`. With an empty token list every request is authorized, so `config validate` rejects a non-loopback `admin_api` listener that has no `auth token`.

---

- [Documentation Index](index.md)
