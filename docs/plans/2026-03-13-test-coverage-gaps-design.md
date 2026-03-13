# Test Coverage Gaps: Design

**Date:** 2026-03-13
**Status:** Approved

## Problem

The project has 1,035 test functions but several critical gaps:

- **5 packages with zero test coverage**, including security-relevant code (path matching, rate limiting, bearer token auth) and the module registry.
- **No binary-level E2E tests.** All E2E tests run in-process with `httptest`. AGENTS.md targets 10% E2E (actual: ~2%).
- **Postgres backend has only 3 stub tests.** Real behavior testing is gated on `HOOKAIDO_TEST_POSTGRES_DSN` via contract tests, but no Postgres-specific unit tests exist for options, helpers, or error mapping.

## Approach

**Parallel units + sequential infrastructure** (Approach A from brainstorming).

- Phase 1: Unit tests for all 5 untested packages (independent, parallelizable).
- Phase 2: Binary-level E2E tests (requires build step, subprocess management).
- Phase 3: Postgres test extension (unit tests without DB + DSN-gated integration tests with Docker).

## Conventions

All tests follow project conventions:

- Standard library `testing` package only. No external test dependencies.
- `t.Helper()` for test helper functions.
- Table-driven tests where applicable.
- File naming: `<source>_test.go` in the same package.

## Phase 1: Unit Tests

### 1.1 `internal/router/pathmatch_test.go` (~12 tests)

Tests `MatchPath(requestPath, routePath)`. Security-relevant: route resolution determines which webhooks are accepted.

Test cases:
- Exact match (`/webhooks/stripe` == `/webhooks/stripe`)
- Prefix match on segment boundary (`/webhooks/stripe/events` matches `/webhooks/stripe`)
- No partial match (`/webhooks/stripeXYZ` does NOT match `/webhooks/stripe`)
- Root route `/` matches everything
- Empty route path matches nothing
- Trailing slash variants
- Edge cases: empty request path, same length without match

### 1.2 `internal/app/ratelimit_test.go` (~10 tests)

Tests `tokenBucketLimiter`. Concurrency-critical: uses Mutex and time-based token replenishment.

Test cases:
- Burst allows N immediate requests when burst=N
- Burst exhausted: request N+1 rejected
- Token replenishment after wait
- Nil receiver: `AllowAt` returns true (guard clause)
- Zero-time fallback: `AllowAt(time.Time{})` uses `time.Now()`
- Negative/zero RPS normalized to 1
- Negative/zero burst normalized to 1
- Concurrent access: goroutine safety via `sync.WaitGroup`

### 1.3 `internal/hookaido/registry_test.go` (~14 tests)

Tests the module registry. `ResetForTesting()` already provided for test isolation.

Test cases:
- Register + Lookup for each type (QueueBackend, TracingProvider, WorkerTransport, MCPProvider)
- Duplicate registration panics (recover in test)
- Lookup for unregistered name returns false
- `HasQueueBackend` true/false
- `QueueBackendNames` returns sorted names
- `MCPProviders` returns a copy (not the original slice)
- `ResetForTesting` clears all registries
- Concurrent registration (goroutine safety via RWMutex)

### 1.4 `internal/backlog/analysis_test.go` (~15 tests)

Tests pure functions: percentile calculation, age windows, ReferenceNow.

Test cases:
- `AgePercentileNearestRank`: empty slice, 1 element, 3 elements, 100 elements, boundary percentiles (0, 100)
- `AgePercentilesFromSamples`: empty samples returns false, normal samples, uniform samples
- `ObserveAgeWindow`: each bucket individually (<=5m, 5-15m, 15m-1h, 1h-6h, >6h), boundary values
- `ReferenceNow`: with OldestQueued data, with ReadyLag data, fallback to time.Now()

### 1.5 `internal/workerapi/auth_test.go` (~10 tests)

Tests `BearerTokenAuthorizer`. Security-relevant: constant-time compare.

Test cases:
- Valid token accepted
- Invalid token rejected
- Empty token list: everything allowed
- No gRPC metadata in context: rejected
- Wrong prefix (not "Bearer "): rejected
- Case-insensitive "bearer " prefix
- Empty token after "Bearer ": rejected
- Whitespace trimming
- Multiple allowed tokens: any one suffices
- `parseBearerToken`: too short (<7 chars) returns false

## Phase 2: Binary-Level E2E Tests

### `internal/e2e/binary_test.go` (~5 tests)

Architecture:
- `TestMain` builds the binary (`go build -o hookaido-test ./cmd/hookaido`)
- Each test writes a temp Hookaidofile with dynamic ports, starts the binary as a subprocess, waits for health endpoint readiness, runs HTTP tests, then cleans up.

Test helpers:
- `buildBinary(t)` -- returns path to pre-built binary
- `startHookaido(t, config)` -- writes temp config, starts subprocess, waits for health, returns cleanup func
- `waitForHealth(t, adminURL, timeout)` -- retry loop against `/healthz`
- `freePort(t)` -- finds free TCP port for dynamic port assignment

Test cases:
1. **IngressToPull** -- POST webhook via ingress, dequeue via pull API, ack. Full stack happy path.
2. **AdminHealth** -- Start binary, GET `/healthz`, verify JSON response.
3. **ConfigValidate** -- `hookaido config validate --config <file>`, check exit code 0.
4. **ConfigFmt** -- `hookaido config fmt --config <file>`, output is idempotent.
5. **RejectInvalidConfig** -- `hookaido run --config <invalid>`, exit code != 0, error on stderr.

## Phase 3: Postgres Test Extension

### Part A: Unit tests without Postgres (~20 tests)

Added to `modules/postgres/postgres_test.go`:
- Option functions (~5 tests): `WithQueueLimits`, `WithRetention`, `WithDeliveredRetention`, `WithDLQRetention`, `WithPollInterval`, `WithNowFunc` -- verify options set Store fields correctly.
- `postgresMetricErrorKind` (~8 tests): each error type (queue_full, duplicate, lease_not_found, lease_expired, timeout, canceled, PgError codes, other, nil).
- Helper functions (~4 tests): `clampSliceCap`, `normalizeUniqueIDs`, `nullIfEmpty`, `nullTime`, `nullInt`.
- `mapPostgresInsertError` (~3 tests): duplicate key → ErrEnvelopeExists, other PgErrors, normal errors.

### Part B: DSN-gated integration tests (~6 tests)

Skip pattern:
```go
func requirePostgresDSN(t *testing.T) string {
    dsn := os.Getenv("HOOKAIDO_TEST_POSTGRES_DSN")
    if dsn == "" {
        t.Skip("HOOKAIDO_TEST_POSTGRES_DSN not set")
    }
    return dsn
}
```

Test cases:
1. Schema init: open store, verify tables exist
2. Enqueue + dequeue round-trip: envelope in, envelope out, fields correct
3. Queue limits: maxDepth=2, third enqueue → ErrQueueFull
4. Retention pruning: old messages pruned
5. Backlog trend capture: sample recorded, retrieved via ListBacklogTrend
6. Concurrent dequeue: parallel goroutines, no item double-leased (FOR UPDATE SKIP LOCKED)

### Docker infrastructure

`docker-compose.test.yml` in project root:
- PostgreSQL 17 Alpine on port 5433 (avoids conflicts with local Postgres)
- tmpfs for speed (data not persisted)
- Usage: `docker compose -f docker-compose.test.yml up -d` then set DSN and run tests

## Summary

| Phase | New files | New tests | Dependencies |
|-------|-----------|-----------|-------------|
| 1: Unit tests | 5 test files | ~61 | None |
| 2: Binary E2E | 1 test file | ~5 | Binary build |
| 3a: Postgres units | 1 file (extended) | ~20 | None |
| 3b: Postgres integration | 1 file (extended) | ~6 | Docker Postgres |
| 3: Docker infra | docker-compose.test.yml | - | Docker |
| **Total** | **7 new + 1 extended** | **~92** | |
