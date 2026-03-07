# Architecture Analysis Findings

Date: 2026-03-08

Systematic analysis of the Hookaido codebase to identify refactoring opportunities.
Status: findings documented, no implementation decisions made yet.

## God Files

| File | Lines | Methods | Problem |
|------|------:|--------:|---------|
| `internal/mcp/server.go` | 8,406 | 63 | JSON-RPC protocol, tool dispatch, admin proxy, config reading, backlog analysis — all in one file |
| `internal/admin/http.go` | 5,285 | 69 | HTTP routing, auth, all handlers, analytics algorithms, query parsing — all in one file |
| `internal/app/run.go` | 2,755 | — | Bootstrap, runtime state, hot-reload, wiring mixed together |
| `internal/config/parser.go` | 3,620 | — | Large but inherent to hand-written recursive-descent parser |
| `internal/config/compile.go` | 3,347 | — | Large but focused responsibility |
| `internal/queue/sqlite.go` | 3,355 | — | Large due to SQL queries and 17-method Store interface |

## Code Duplication (admin <-> mcp)

Duplicated between `internal/admin/http.go` and `internal/mcp/server.go`:

- **Backlog analysis structs**: `backlogAgeWindows`, `backlogAgePercentiles`, `backlogStateScanSummary` — identical type definitions
- **Backlog algorithms**: `backlogReferenceNow()`, `observeBacklogAgeWindow()`, `backlogAgePercentilesFromSamples()`, `backlogAgePercentileNearestRank()` — identical implementations
- **Backlog trend computation**: MCP `*Map()` variants are near-clones of admin `*FromStats()`/`*FromMessages()`/`*FromSamples()`
- **Constants**: `maxListLimit`, `defaultBacklogTrendWindow`, `defaultBacklogTrendStep`, `minBacklogTrendStep`, `maxBacklogTrendStep`, `maxBacklogTrendWindow`, `maxBacklogTrendSamples`, `healthTrendSignalSamples`, `maxAuditReasonLength`, `maxAuditActorLength`, audit header names
- **Parsing helpers**: `parseBoolParam`, `parseTimeParam`, `parseDurationParam`, `parseQueueState`, `parseAttemptOutcome` — parallel implementations

Within `internal/admin/http.go` itself:
- `handleMessagesCancelByFilter`, `handleMessagesRequeueByFilter`, `handleMessagesResumeByFilter` are ~95% identical (differ only in store method called)

## MCP Config Coupling

`mcp.Server` bypasses the application config pipeline:
- Reads config from disk on every tool call via `loadCompiledConfig()`
- Opens SQLite stores directly via `openSQLiteStore()`
- Creates a parallel config path that could diverge from the running app during reload

Should receive compiled config and store handles via dependency injection.

## Interface Design

- `queue.Store` has 17 methods. Optional capabilities (`BacklogTrendStore`, `BatchEnqueuer`, `LeaseBatchStore`, `RuntimeMetricsProvider`) are properly factored into separate interfaces, but core is still broad.
- `router.Router` interface is defined but appears unused in production code. Routing done directly in `app/run.go`.
- `admin.Server` has 30+ fields (14 function-typed injection points) — too many responsibilities for one struct.

## Dead / Underused Code

- `internal/router/` — `Router` interface defined but unused, `MatchPath` utility exists, zero tests
- `internal/workerapi/` — 175 test lines vs 1,380 prod lines (minimal coverage)

## Test Organization

- `internal/mcp/server_test.go` — 13,429 lines, mirrors the god-file problem
- `queue/store_contract_test.go` — Good pattern: contract tests run against all backend implementations
- E2E tests in `internal/e2e/` well structured (~1,327 lines)
- Fuzz tests exist for config, ingress, pullapi
- Benchmark tests exist for pullapi, dispatcher

## Suggested Refactoring Priorities

1. Extract shared `internal/backlog` (or `internal/analytics`) package for duplicated types, constants, algorithms
2. Split `mcp/server.go` into protocol, tool groups (per-domain files), admin client
3. Split `admin/http.go` into handler groups, parameter parsing, routing
4. Fix MCP config coupling via dependency injection
5. Extract shared constants and parameter parsing helpers
6. Add test coverage for `router` and `workerapi`
7. Remove or integrate unused `internal/router` package
