# Modular Architecture Design

Date: 2026-03-08
Status: approved

## Motivation

Hookaido at ~43K production lines bundles all features and dependencies into a single binary.
Users who need only webhook routing still carry SQLite, PostgreSQL, gRPC, and OpenTelemetry
dependencies. The project identity ("Caddy-style webhook queue") is clear, but the monolithic
structure limits both flexibility for users and maintainability for contributors.

Goals:
- Users choose which capabilities to compile in (like Caddy modules)
- Core stays small (~20K lines) with zero heavy external dependencies
- Standard build remains "batteries included" (identical to today)
- Third parties can write queue backends (Redis, Kafka, SQS) without touching the core repo

## Core vs. Module Boundary

### Core (always present, ~20K lines)

| Component | Package | Rationale |
|-----------|---------|-----------|
| Config DSL | `internal/config` | Project identity |
| Ingress + Routing | `internal/ingress`, `internal/router` | Core function |
| Memory Queue | `internal/queue` (memory only) | Minimal backend for dev/test, no external deps |
| Push Delivery | `internal/dispatcher` | Core delivery mode, no heavy deps |
| Pull API (HTTP) | `internal/pullapi` | Core delivery mode, no heavy deps |
| Admin API | `internal/admin` | Most users need queue management |
| Secrets | `internal/secrets` | Small (517 lines), required by config |
| Health + Prometheus | In `internal/app` | Lightweight observability, no external deps |
| Shared Analytics | `internal/backlog` (new) | Extracted from admin+mcp duplication |

### Modules (optional, selected per build)

| Module | Current Location | Lines | Heavy Dependencies |
|--------|-----------------|------:|-------------------|
| `sqlite` | `internal/queue/sqlite.go` | ~3,400 | `modernc.org/sqlite` |
| `postgres` | `internal/queue/postgres.go` | ~2,100 | `jackc/pgx/v5` |
| `grpcworker` | `internal/workerapi/` | ~1,400 | `grpc`, `protobuf` |
| `otel` | OTel setup in `internal/app/run.go` | ~500 | 4 OpenTelemetry modules |
| `mcp` | `internal/mcp/` | ~8,500 | none (but large code surface) |

Effect: standard build includes everything (identical to today). Minimal build: ~20K lines,
zero heavy external dependencies.

## Module Interface Design

Typed module interfaces per category rather than a single generic Module interface.
Hookaido's scope is narrower than Caddy's, so simplicity wins over generality.

### Interfaces

```go
// internal/hookaido/module.go

type Module interface {
    Name() string
}

type QueueBackend interface {
    Module
    OpenStore(cfg QueueBackendConfig) (queue.Store, error)
}

type TracingProvider interface {
    Module
    Setup(cfg TracingConfig) (shutdown func(context.Context) error, err error)
}

type MCPProvider interface {
    Module
    RegisterTools(registry ToolRegistry)
}

type WorkerTransport interface {
    Module
    Serve(listener net.Listener, handler PullHandler) error
    Stop(ctx context.Context) error
}
```

### Registration

Caddy-style init() pattern:

```go
// internal/hookaido/registry.go

var (
    queueBackends    = map[string]QueueBackend{}
    tracingProviders = map[string]TracingProvider{}
    mcpProviders     []MCPProvider
    workerTransports = map[string]WorkerTransport{}
)

func RegisterQueueBackend(b QueueBackend)       { queueBackends[b.Name()] = b }
func RegisterTracingProvider(p TracingProvider)  { tracingProviders[p.Name()] = p }
func RegisterMCPProvider(p MCPProvider)          { mcpProviders = append(mcpProviders, p) }
func RegisterWorkerTransport(t WorkerTransport) { workerTransports[t.Name()] = t }
```

### Module files

Each module is a package under `modules/` with an `init()` function:

```go
// modules/sqlite/sqlite.go
package sqlite

import "hookaido/internal/hookaido"

func init() {
    hookaido.RegisterQueueBackend(&Backend{})
}
```

### Config integration

The config parser validates backend names against the registry. Unknown backends produce a
compile error with actionable guidance: "sqlite backend not compiled in; build with the sqlite
module or use xhookaido".

### Build variants

```go
// cmd/hookaido/main.go (standard build — all modules)
import (
    _ "hookaido/modules/sqlite"
    _ "hookaido/modules/postgres"
    _ "hookaido/modules/grpcworker"
    _ "hookaido/modules/otel"
    _ "hookaido/modules/mcp"
)

// cmd/hookaido-minimal/main.go (minimal build — core only)
// No module imports: memory queue only
```

## Target Directory Structure

```
hookaido/
├── cmd/
│   └── hookaido/
│       └── main.go                # Standard build (all modules)
├── internal/
│   ├── hookaido/                  # NEW: module registry + interfaces
│   │   ├── module.go
│   │   └── registry.go
│   ├── backlog/                   # NEW: extracted shared analytics
│   │   ├── analysis.go
│   │   └── trends.go
│   ├── config/                    # Stays (DSL parser/compiler)
│   ├── ingress/                   # Stays
│   ├── queue/                     # SHRINKS: interface + memory backend only
│   │   ├── queue.go               # Store interface
│   │   └── memory.go              # Memory backend
│   ├── dispatcher/                # Stays
│   ├── pullapi/                   # Stays
│   ├── admin/                     # Stays (uses backlog/)
│   ├── secrets/                   # Stays
│   ├── app/                       # Stays (uses registry instead of direct imports)
│   └── ...
├── modules/                       # NEW: optional modules
│   ├── sqlite/
│   │   └── sqlite.go
│   ├── postgres/
│   │   └── postgres.go
│   ├── grpcworker/
│   │   ├── grpcworker.go
│   │   └── proto/
│   ├── otel/
│   │   └── otel.go
│   └── mcp/
│       ├── mcp.go
│       ├── tools_messages.go
│       ├── tools_dlq.go
│       ├── tools_config.go
│       └── tools_backlog.go
└── go.mod
```

## Migration Plan

Four phases, each resulting in green tests and a standalone PR.

### Phase 1: Groundwork (structural, no behavior change)

1. Extract `internal/backlog/` — shared analytics code from admin and mcp.
   Both packages import `backlog/` instead of duplicating algorithms.
2. Split `mcp/server.go` (8,406 lines) into protocol.go and per-domain tool files.
3. Split `admin/http.go` (5,285 lines) into handler groups, parameter parsing, routing.

### Phase 2: Module Foundation (introduce registry, no file moves)

4. Create `internal/hookaido/` with module interfaces and registry.
5. Wire queue backend selection through registry instead of direct `queue.NewSQLiteStore()`.
   Backends register from their current location.
6. Update config parser to validate backends against registry.

### Phase 3: Module Extraction (one module per PR)

7. `modules/sqlite/` — move SQLite code from `internal/queue/`, add init() + Register.
8. `modules/postgres/` — move Postgres code.
9. `modules/grpcworker/` — move workerapi.
10. `modules/otel/` — extract OTel setup from `app/run.go`.
11. `modules/mcp/` — move MCP server (already split in Phase 1).

### Phase 4: Build Variants

12. Standard `main.go` with all blank imports.
13. Minimal `main.go` without module imports.
14. Makefile targets: `build`, `build-minimal`, `build-custom MODULES="sqlite otel"`.
15. Optional: `xhookaido` build tool for arbitrary module combinations.

### Risk mitigation

- Each phase is a separate PR.
- Contract tests (`store_contract_test.go`) ensure backend parity after moves.
- E2E tests run after every phase with the standard build.
- CI builds and tests both standard and minimal variants.

## Side Effects

This migration simultaneously resolves the architectural findings documented in
`docs/plans/2026-03-08-architecture-findings.md`:

- God files: mcp/server.go and admin/http.go are split during Phase 1.
- Code duplication: backlog analytics extracted to shared package in Phase 1.
- MCP config coupling: MCP becomes a module with injected dependencies in Phase 3.
- Unused router package: addressed during Phase 2 (integrate or remove).

## Decisions Not Made

- Whether `xhookaido` is needed for v1.x or deferred to Phase 2+.
- Whether modules should be separate Go modules (multi-module repo) or single go.mod.
- Exact shape of `QueueBackendConfig` and other module config types.
- Whether Admin API should eventually become a module (currently stays in core).
