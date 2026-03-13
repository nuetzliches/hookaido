# Test Coverage Gaps Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close all test coverage gaps: unit tests for 5 untested packages, binary-level E2E tests, and extended Postgres tests.

**Architecture:** Phase 1 (Tasks 1-5) adds unit tests to 5 independent packages -- these can run in parallel. Phase 2 (Task 6) adds binary-level E2E tests that build the hookaido binary and test it as a subprocess. Phase 3 (Tasks 7-8) extends Postgres test coverage with unit tests (no DB) and DSN-gated integration tests (Docker).

**Tech Stack:** Go stdlib `testing` only. No external test dependencies. Docker for Postgres integration tests.

---

## Phase 1: Unit Tests (Independent, Parallelizable)

### Task 1: Path Matching Tests

**Files:**
- Create: `internal/router/pathmatch_test.go`
- Reference: `internal/router/pathmatch.go`

**Step 1: Write the test file**

```go
package router

import "testing"

func TestMatchPath(t *testing.T) {
	tests := []struct {
		name        string
		requestPath string
		routePath   string
		want        bool
	}{
		// exact match
		{name: "exact match", requestPath: "/webhooks/stripe", routePath: "/webhooks/stripe", want: true},
		{name: "exact root paths", requestPath: "/", routePath: "/", want: true},

		// prefix match on segment boundary
		{name: "prefix with subpath", requestPath: "/webhooks/stripe/events", routePath: "/webhooks/stripe", want: true},
		{name: "prefix with deep subpath", requestPath: "/webhooks/stripe/events/charge", routePath: "/webhooks/stripe", want: true},

		// root matches everything
		{name: "root matches any path", requestPath: "/webhooks/stripe", routePath: "/", want: true},
		{name: "root matches deep path", requestPath: "/a/b/c/d", routePath: "/", want: true},

		// no partial match (not on segment boundary)
		{name: "no partial match", requestPath: "/webhooks/stripeXYZ", routePath: "/webhooks/stripe", want: false},
		{name: "no partial match suffix", requestPath: "/webhooks/stripe-v2", routePath: "/webhooks/stripe", want: false},

		// empty route path matches nothing
		{name: "empty route matches nothing", requestPath: "/webhooks", routePath: "", want: false},

		// shorter request than route
		{name: "shorter request than route", requestPath: "/web", routePath: "/webhooks/stripe", want: false},

		// same length no match
		{name: "same length no match", requestPath: "/webhooks/github", routePath: "/webhooks/stripe", want: false},

		// trailing slash variations
		{name: "request with trailing slash exact", requestPath: "/webhooks/stripe/", routePath: "/webhooks/stripe", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchPath(tt.requestPath, tt.routePath)
			if got != tt.want {
				t.Errorf("MatchPath(%q, %q) = %v, want %v", tt.requestPath, tt.routePath, got, tt.want)
			}
		})
	}
}
```

**Step 2: Run test to verify it passes**

Run: `go test ./internal/router/ -v -run TestMatchPath`
Expected: All subtests PASS.

**Step 3: Commit**

```
git add internal/router/pathmatch_test.go
git commit -m "test(router): add unit tests for MatchPath"
```

---

### Task 2: Rate Limiter Tests

**Files:**
- Create: `internal/app/ratelimit_test.go`
- Reference: `internal/app/ratelimit.go`

**Step 1: Write the test file**

```go
package app

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucketLimiter_BurstAllowed(t *testing.T) {
	now := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	l := newTokenBucketLimiter(10, 5, now)

	for i := 0; i < 5; i++ {
		if !l.AllowAt(now) {
			t.Fatalf("request %d should be allowed within burst", i+1)
		}
	}
}

func TestTokenBucketLimiter_BurstExhausted(t *testing.T) {
	now := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	l := newTokenBucketLimiter(10, 3, now)

	for i := 0; i < 3; i++ {
		l.AllowAt(now)
	}
	if l.AllowAt(now) {
		t.Fatal("request beyond burst should be rejected")
	}
}

func TestTokenBucketLimiter_Replenishment(t *testing.T) {
	now := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	l := newTokenBucketLimiter(1, 1, now) // 1 rps, burst 1

	if !l.AllowAt(now) {
		t.Fatal("first request should be allowed")
	}
	if l.AllowAt(now) {
		t.Fatal("second request at same time should be rejected")
	}

	// After 1 second at 1 rps, one token should be replenished.
	later := now.Add(time.Second)
	if !l.AllowAt(later) {
		t.Fatal("request after replenishment should be allowed")
	}
}

func TestTokenBucketLimiter_ReplenishmentCappedAtBurst(t *testing.T) {
	now := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	l := newTokenBucketLimiter(100, 2, now) // 100 rps, burst 2

	// Exhaust burst.
	l.AllowAt(now)
	l.AllowAt(now)

	// Wait 10 seconds -- would add 1000 tokens, but burst is 2.
	later := now.Add(10 * time.Second)
	if !l.AllowAt(later) {
		t.Fatal("first request after wait should be allowed")
	}
	if !l.AllowAt(later) {
		t.Fatal("second request after wait should be allowed")
	}
	if l.AllowAt(later) {
		t.Fatal("third request should be rejected (tokens capped at burst=2)")
	}
}

func TestTokenBucketLimiter_NilReceiver(t *testing.T) {
	var l *tokenBucketLimiter
	now := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)

	if !l.AllowAt(now) {
		t.Fatal("nil limiter should always allow")
	}
}

func TestTokenBucketLimiter_ZeroTimeUsesNow(t *testing.T) {
	now := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	l := newTokenBucketLimiter(10, 1, now)

	// AllowAt with zero time should not panic and should work.
	l.AllowAt(time.Time{})
}

func TestTokenBucketLimiter_NegativeRPSNormalized(t *testing.T) {
	now := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	l := newTokenBucketLimiter(-5, 2, now)

	if l.rate != 1 {
		t.Fatalf("negative rps should be normalized to 1, got %v", l.rate)
	}
}

func TestTokenBucketLimiter_ZeroBurstNormalized(t *testing.T) {
	now := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	l := newTokenBucketLimiter(10, 0, now)

	if l.burst != 1 {
		t.Fatalf("zero burst should be normalized to 1, got %v", l.burst)
	}
}

func TestTokenBucketLimiter_ZeroRPSNormalized(t *testing.T) {
	now := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	l := newTokenBucketLimiter(0, 3, now)

	if l.rate != 1 {
		t.Fatalf("zero rps should be normalized to 1, got %v", l.rate)
	}
}

func TestTokenBucketLimiter_ConcurrentAccess(t *testing.T) {
	now := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	l := newTokenBucketLimiter(1000, 100, now)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.AllowAt(now) {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	got := allowed.Load()
	if got > 100 {
		t.Fatalf("allowed %d requests, should be at most 100 (burst)", got)
	}
	if got < 1 {
		t.Fatalf("allowed %d requests, should be at least 1", got)
	}
}
```

**Step 2: Run test to verify it passes**

Run: `go test ./internal/app/ -v -run TestTokenBucketLimiter -count=1`
Expected: All subtests PASS.

**Step 3: Commit**

```
git add internal/app/ratelimit_test.go
git commit -m "test(app): add unit tests for tokenBucketLimiter"
```

---

### Task 3: Module Registry Tests

**Files:**
- Create: `internal/hookaido/registry_test.go`
- Reference: `internal/hookaido/registry.go`
- Reference: `internal/hookaido/module.go`

**Step 1: Write the test file**

```go
package hookaido

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

// Minimal test stubs implementing the module interfaces.

type stubQueueBackend struct{ name string }

func (s stubQueueBackend) Name() string { return s.name }
func (s stubQueueBackend) OpenStore(_ QueueBackendConfig) (any, func() error, error) {
	return nil, nil, nil
}

type stubTracingProvider struct{ name string }

func (s stubTracingProvider) Name() string { return s.name }
func (s stubTracingProvider) Init(_ context.Context, _ any, _ string, _ func(error)) (func(context.Context) error, error) {
	return nil, nil
}
func (s stubTracingProvider) WrapHandler(_ string, h http.Handler) http.Handler { return h }
func (s stubTracingProvider) HTTPClient() *http.Client                          { return nil }

type stubWorkerTransport struct{ name string }

func (s stubWorkerTransport) Name() string                                { return s.name }
func (s stubWorkerTransport) Serve(_ net.Listener, _ WorkerTransportConfig) error { return nil }
func (s stubWorkerTransport) Stop(_ context.Context) error                { return nil }

type stubMCPProvider struct{ name string }

func (s stubMCPProvider) Name() string           { return s.name }
func (s stubMCPProvider) ServeCommand(_ []string) int { return 0 }

func TestRegisterAndLookupQueueBackend(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterQueueBackend(stubQueueBackend{name: "memory"})

	b, ok := LookupQueueBackend("memory")
	if !ok {
		t.Fatal("expected to find registered backend")
	}
	if b.Name() != "memory" {
		t.Fatalf("name = %q, want memory", b.Name())
	}
}

func TestLookupQueueBackend_NotFound(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	_, ok := LookupQueueBackend("nonexistent")
	if ok {
		t.Fatal("expected not found for unregistered backend")
	}
}

func TestHasQueueBackend(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	if HasQueueBackend("sqlite") {
		t.Fatal("should not have sqlite before registration")
	}
	RegisterQueueBackend(stubQueueBackend{name: "sqlite"})
	if !HasQueueBackend("sqlite") {
		t.Fatal("should have sqlite after registration")
	}
}

func TestQueueBackendNames_Sorted(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterQueueBackend(stubQueueBackend{name: "sqlite"})
	RegisterQueueBackend(stubQueueBackend{name: "memory"})
	RegisterQueueBackend(stubQueueBackend{name: "postgres"})

	names := QueueBackendNames()
	if len(names) != 3 {
		t.Fatalf("len = %d, want 3", len(names))
	}
	if names[0] != "memory" || names[1] != "postgres" || names[2] != "sqlite" {
		t.Fatalf("names = %v, want [memory postgres sqlite]", names)
	}
}

func TestRegisterQueueBackend_DuplicatePanics(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterQueueBackend(stubQueueBackend{name: "memory"})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	RegisterQueueBackend(stubQueueBackend{name: "memory"})
}

func TestRegisterAndLookupTracingProvider(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterTracingProvider(stubTracingProvider{name: "otel"})

	p, ok := LookupTracingProvider("otel")
	if !ok {
		t.Fatal("expected to find registered tracing provider")
	}
	if p.Name() != "otel" {
		t.Fatalf("name = %q, want otel", p.Name())
	}
}

func TestRegisterTracingProvider_DuplicatePanics(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterTracingProvider(stubTracingProvider{name: "otel"})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	RegisterTracingProvider(stubTracingProvider{name: "otel"})
}

func TestRegisterAndLookupWorkerTransport(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterWorkerTransport(stubWorkerTransport{name: "grpc"})

	tr, ok := LookupWorkerTransport("grpc")
	if !ok {
		t.Fatal("expected to find registered transport")
	}
	if tr.Name() != "grpc" {
		t.Fatalf("name = %q, want grpc", tr.Name())
	}
}

func TestRegisterWorkerTransport_DuplicatePanics(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterWorkerTransport(stubWorkerTransport{name: "grpc"})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	RegisterWorkerTransport(stubWorkerTransport{name: "grpc"})
}

func TestRegisterMCPProvider(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterMCPProvider(stubMCPProvider{name: "mcp"})
	RegisterMCPProvider(stubMCPProvider{name: "mcp2"})

	providers := MCPProviders()
	if len(providers) != 2 {
		t.Fatalf("len = %d, want 2", len(providers))
	}
}

func TestMCPProviders_ReturnsCopy(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterMCPProvider(stubMCPProvider{name: "mcp"})

	a := MCPProviders()
	b := MCPProviders()
	if len(a) != 1 || len(b) != 1 {
		t.Fatal("expected 1 provider in each copy")
	}

	// Modifying one slice should not affect the other.
	a[0] = stubMCPProvider{name: "modified"}
	got := MCPProviders()
	if got[0].Name() != "mcp" {
		t.Fatalf("MCPProviders returned reference not copy, got %q", got[0].Name())
	}
}

func TestResetForTesting(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterQueueBackend(stubQueueBackend{name: "a"})
	RegisterTracingProvider(stubTracingProvider{name: "b"})
	RegisterWorkerTransport(stubWorkerTransport{name: "c"})
	RegisterMCPProvider(stubMCPProvider{name: "d"})

	ResetForTesting()

	if HasQueueBackend("a") {
		t.Fatal("queue backend should be cleared")
	}
	if _, ok := LookupTracingProvider("b"); ok {
		t.Fatal("tracing provider should be cleared")
	}
	if _, ok := LookupWorkerTransport("c"); ok {
		t.Fatal("worker transport should be cleared")
	}
	if len(MCPProviders()) != 0 {
		t.Fatal("mcp providers should be cleared")
	}
}

func TestConcurrentRegistration(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// Use unique names to avoid duplicate panics.
			name := "backend_" + time.Now().Format("150405.000000000") + "_" + string(rune('A'+n))
			RegisterQueueBackend(stubQueueBackend{name: name})
		}(i)
	}
	wg.Wait()

	names := QueueBackendNames()
	if len(names) != 50 {
		t.Fatalf("expected 50 backends, got %d", len(names))
	}
}
```

Note: The `stubWorkerTransport` uses an unused import of `crypto/tls` from module.go's `WorkerTransportConfig.TLSConfig` field, but since the tests don't call `Serve` with a real config, we don't need to import it. Remove the `crypto/tls` and `time` imports from the above if unused after writing (the compiler will tell you).

**Step 2: Run test to verify it passes**

Run: `go test ./internal/hookaido/ -v -count=1`
Expected: All subtests PASS.

**Step 3: Commit**

```
git add internal/hookaido/registry_test.go
git commit -m "test(hookaido): add unit tests for module registry"
```

---

### Task 4: Backlog Analysis Tests

**Files:**
- Create: `internal/backlog/analysis_test.go`
- Reference: `internal/backlog/analysis.go`

**Step 1: Write the test file**

```go
package backlog

import (
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

func TestAgePercentileNearestRank_EmptySlice(t *testing.T) {
	got := AgePercentileNearestRank(nil, 50)
	if got != 0 {
		t.Fatalf("got %d, want 0 for empty slice", got)
	}
}

func TestAgePercentileNearestRank_SingleElement(t *testing.T) {
	got := AgePercentileNearestRank([]int{42}, 50)
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

func TestAgePercentileNearestRank_BoundaryPercentiles(t *testing.T) {
	ordered := []int{10, 20, 30, 40, 50}

	if got := AgePercentileNearestRank(ordered, 0); got != 10 {
		t.Fatalf("p0 = %d, want 10", got)
	}
	if got := AgePercentileNearestRank(ordered, 100); got != 50 {
		t.Fatalf("p100 = %d, want 50", got)
	}
}

func TestAgePercentileNearestRank_Typical(t *testing.T) {
	ordered := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	p50 := AgePercentileNearestRank(ordered, 50)
	if p50 != 5 {
		t.Fatalf("p50 = %d, want 5", p50)
	}

	p90 := AgePercentileNearestRank(ordered, 90)
	if p90 != 9 {
		t.Fatalf("p90 = %d, want 9", p90)
	}

	p99 := AgePercentileNearestRank(ordered, 99)
	if p99 != 10 {
		t.Fatalf("p99 = %d, want 10", p99)
	}
}

func TestAgePercentilesFromSamples_Empty(t *testing.T) {
	_, ok := AgePercentilesFromSamples(nil)
	if ok {
		t.Fatal("expected ok=false for empty samples")
	}
}

func TestAgePercentilesFromSamples_Normal(t *testing.T) {
	// Unsorted input -- function should sort internally.
	samples := []int{50, 10, 30, 20, 40}

	p, ok := AgePercentilesFromSamples(samples)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if p.P50 != 30 {
		t.Fatalf("p50 = %d, want 30", p.P50)
	}
}

func TestAgePercentilesFromSamples_DoesNotMutateInput(t *testing.T) {
	samples := []int{5, 3, 1, 4, 2}
	orig := make([]int, len(samples))
	copy(orig, samples)

	AgePercentilesFromSamples(samples)

	for i := range samples {
		if samples[i] != orig[i] {
			t.Fatalf("input was mutated: samples[%d]=%d, orig=%d", i, samples[i], orig[i])
		}
	}
}

func TestObserveAgeWindow_AllBuckets(t *testing.T) {
	tests := []struct {
		name       string
		ageSeconds int
		wantField  string
	}{
		{name: "le_5m", ageSeconds: 60, wantField: "LE5M"},           // 1 min
		{name: "le_5m_boundary", ageSeconds: 300, wantField: "LE5M"}, // exactly 5m
		{name: "gt_5m_le_15m", ageSeconds: 600, wantField: "GT5MLE15M"},
		{name: "gt_5m_le_15m_boundary", ageSeconds: 900, wantField: "GT5MLE15M"}, // exactly 15m
		{name: "gt_15m_le_1h", ageSeconds: 1800, wantField: "GT15MLE1H"},
		{name: "gt_15m_le_1h_boundary", ageSeconds: 3600, wantField: "GT15MLE1H"}, // exactly 1h
		{name: "gt_1h_le_6h", ageSeconds: 7200, wantField: "GT1HLE6H"},
		{name: "gt_1h_le_6h_boundary", ageSeconds: 21600, wantField: "GT1HLE6H"}, // exactly 6h
		{name: "gt_6h", ageSeconds: 86400, wantField: "GT6H"},                     // 24h
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var w AgeWindows
			ObserveAgeWindow(tt.ageSeconds, &w)

			total := w.LE5M + w.GT5MLE15M + w.GT15MLE1H + w.GT1HLE6H + w.GT6H
			if total != 1 {
				t.Fatalf("expected exactly 1 observation, got total=%d (%+v)", total, w)
			}

			// Verify the correct bucket got incremented.
			switch tt.wantField {
			case "LE5M":
				if w.LE5M != 1 {
					t.Fatalf("LE5M = %d, want 1", w.LE5M)
				}
			case "GT5MLE15M":
				if w.GT5MLE15M != 1 {
					t.Fatalf("GT5MLE15M = %d, want 1", w.GT5MLE15M)
				}
			case "GT15MLE1H":
				if w.GT15MLE1H != 1 {
					t.Fatalf("GT15MLE1H = %d, want 1", w.GT15MLE1H)
				}
			case "GT1HLE6H":
				if w.GT1HLE6H != 1 {
					t.Fatalf("GT1HLE6H = %d, want 1", w.GT1HLE6H)
				}
			case "GT6H":
				if w.GT6H != 1 {
					t.Fatalf("GT6H = %d, want 1", w.GT6H)
				}
			}
		})
	}
}

func TestReferenceNow_FromOldestQueued(t *testing.T) {
	received := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	age := 5 * time.Minute

	stats := queue.Stats{
		OldestQueuedReceivedAt: received,
		OldestQueuedAge:        age,
	}
	got := ReferenceNow(stats)
	want := received.Add(age).UTC()
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestReferenceNow_FromReadyLag(t *testing.T) {
	nextRun := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	lag := 2 * time.Minute

	stats := queue.Stats{
		EarliestQueuedNextRun: nextRun,
		ReadyLag:              lag,
	}
	got := ReferenceNow(stats)
	want := nextRun.Add(lag).UTC()
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestReferenceNow_FallbackToNow(t *testing.T) {
	before := time.Now().UTC()
	got := ReferenceNow(queue.Stats{})
	after := time.Now().UTC()

	if got.Before(before) || got.After(after) {
		t.Fatalf("got %v, expected between %v and %v", got, before, after)
	}
}

func TestReferenceNow_OldestQueuedTakesPrecedence(t *testing.T) {
	received := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	nextRun := time.Date(2026, 3, 13, 9, 0, 0, 0, time.UTC)

	stats := queue.Stats{
		OldestQueuedReceivedAt: received,
		OldestQueuedAge:        5 * time.Minute,
		EarliestQueuedNextRun:  nextRun,
		ReadyLag:               2 * time.Minute,
	}

	got := ReferenceNow(stats)
	want := received.Add(5 * time.Minute).UTC()
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v (OldestQueued should take precedence)", got, want)
	}
}
```

**Step 2: Run test to verify it passes**

Run: `go test ./internal/backlog/ -v -count=1`
Expected: All subtests PASS.

**Step 3: Commit**

```
git add internal/backlog/analysis_test.go
git commit -m "test(backlog): add unit tests for analysis functions"
```

---

### Task 5: Worker API Auth Tests

**Files:**
- Create: `internal/workerapi/auth_test.go`
- Reference: `internal/workerapi/auth.go`

**Step 1: Write the test file**

```go
package workerapi

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func ctxWithAuth(token string) context.Context {
	md := metadata.Pairs("authorization", token)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestBearerTokenAuthorizer_ValidToken(t *testing.T) {
	auth := BearerTokenAuthorizer([][]byte{[]byte("secret123")})
	ctx := ctxWithAuth("Bearer secret123")

	if !auth(ctx, "/pull") {
		t.Fatal("valid token should be authorized")
	}
}

func TestBearerTokenAuthorizer_InvalidToken(t *testing.T) {
	auth := BearerTokenAuthorizer([][]byte{[]byte("secret123")})
	ctx := ctxWithAuth("Bearer wrongtoken")

	if auth(ctx, "/pull") {
		t.Fatal("invalid token should be rejected")
	}
}

func TestBearerTokenAuthorizer_EmptyTokenList(t *testing.T) {
	auth := BearerTokenAuthorizer(nil)
	ctx := context.Background()

	if !auth(ctx, "/pull") {
		t.Fatal("empty token list should allow all")
	}
}

func TestBearerTokenAuthorizer_NoMetadata(t *testing.T) {
	auth := BearerTokenAuthorizer([][]byte{[]byte("secret")})
	ctx := context.Background()

	if auth(ctx, "/pull") {
		t.Fatal("no metadata should be rejected")
	}
}

func TestBearerTokenAuthorizer_WrongPrefix(t *testing.T) {
	auth := BearerTokenAuthorizer([][]byte{[]byte("secret")})
	ctx := ctxWithAuth("Basic c2VjcmV0")

	if auth(ctx, "/pull") {
		t.Fatal("non-Bearer prefix should be rejected")
	}
}

func TestBearerTokenAuthorizer_CaseInsensitivePrefix(t *testing.T) {
	auth := BearerTokenAuthorizer([][]byte{[]byte("secret")})
	ctx := ctxWithAuth("bearer secret")

	if !auth(ctx, "/pull") {
		t.Fatal("lowercase bearer prefix should be accepted")
	}
}

func TestBearerTokenAuthorizer_EmptyTokenAfterPrefix(t *testing.T) {
	auth := BearerTokenAuthorizer([][]byte{[]byte("secret")})
	ctx := ctxWithAuth("Bearer ")

	if auth(ctx, "/pull") {
		t.Fatal("empty token after prefix should be rejected")
	}
}

func TestBearerTokenAuthorizer_MultipleAllowedTokens(t *testing.T) {
	auth := BearerTokenAuthorizer([][]byte{[]byte("token1"), []byte("token2"), []byte("token3")})
	ctx := ctxWithAuth("Bearer token2")

	if !auth(ctx, "/pull") {
		t.Fatal("any valid token from the list should be accepted")
	}
}

func TestBearerTokenAuthorizer_WhitespaceTrimmed(t *testing.T) {
	auth := BearerTokenAuthorizer([][]byte{[]byte("secret")})
	ctx := ctxWithAuth("  Bearer   secret  ")

	if !auth(ctx, "/pull") {
		t.Fatal("whitespace should be trimmed")
	}
}

func TestParseBearerToken_TooShort(t *testing.T) {
	_, ok := parseBearerToken("Bear")
	if ok {
		t.Fatal("input shorter than 7 chars should return false")
	}
}

func TestParseBearerToken_ExactlyBearer(t *testing.T) {
	_, ok := parseBearerToken("Bearer ")
	if ok {
		t.Fatal("Bearer with empty token should return false")
	}
}

func TestBearerTokenAuthorizer_SkipsEmptyTokensInConfig(t *testing.T) {
	// Empty tokens in the allowed list should be filtered out.
	auth := BearerTokenAuthorizer([][]byte{[]byte(""), []byte("valid")})
	ctx := ctxWithAuth("Bearer valid")

	if !auth(ctx, "/pull") {
		t.Fatal("should still accept valid token even when empty tokens in config")
	}
}
```

**Step 2: Run test to verify it passes**

Run: `go test ./internal/workerapi/ -v -count=1`
Expected: All subtests PASS.

**Step 3: Commit**

```
git add internal/workerapi/auth_test.go
git commit -m "test(workerapi): add unit tests for BearerTokenAuthorizer"
```

---

## Phase 2: Binary-Level E2E Tests

### Task 6: Binary E2E Test Suite

**Files:**
- Create: `internal/e2e/binary_test.go`
- Reference: `internal/mcp/server_test.go:13308` (existing `buildHookaidoBinary` pattern)
- Reference: `internal/e2e/e2e_test.go` (existing helpers)

**Step 1: Write the test file**

```go
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// testBinaryPath is set by TestMain to the path of the compiled hookaido binary.
var testBinaryPath string

func TestMain(m *testing.M) {
	// Build the binary once for all binary E2E tests.
	tmpDir, err := os.MkdirTemp("", "hookaido-e2e-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: mktemp: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	binName := "hookaido-e2e"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	binPath := filepath.Join(tmpDir, binName)

	// Build from repo root (internal/e2e -> ../.. -> repo root).
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/hookaido")
	cmd.Dir = filepath.Clean(filepath.Join("..", ".."))
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: go build failed: %v\n%s\n", err, string(out))
		os.Exit(1)
	}

	testBinaryPath = binPath
	os.Exit(m.Run())
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

type hookaidoInstance struct {
	cmd        *exec.Cmd
	ingressURL string
	pullURL    string
	adminURL   string
	configFile string
}

func startHookaido(t *testing.T, ingressPort, pullPort, adminPort int) *hookaidoInstance {
	t.Helper()

	config := fmt.Sprintf(`ingress {
    listen ":%d"
}

route /webhooks {
    target "default"
    queue memory
    pull {
        path "/webhooks"
    }
}

pull_api {
    listen ":%d"
    prefix "/pull"
}

admin {
    listen ":%d"
}
`, ingressPort, pullPort, adminPort)

	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(config), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cmd := exec.Command(testBinaryPath, "run", "--config", cfgPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start hookaido: %v", err)
	}

	inst := &hookaidoInstance{
		cmd:        cmd,
		ingressURL: fmt.Sprintf("http://127.0.0.1:%d", ingressPort),
		pullURL:    fmt.Sprintf("http://127.0.0.1:%d/pull", pullPort),
		adminURL:   fmt.Sprintf("http://127.0.0.1:%d", adminPort),
		configFile: cfgPath,
	}

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	// Wait for the admin health endpoint to become available.
	waitForHealth(t, inst.adminURL+"/healthz", 10*time.Second)

	return inst
}

func waitForHealth(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}

	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("health endpoint %s not ready after %v", url, timeout)
}

func TestBinaryE2E_AdminHealth(t *testing.T) {
	if testBinaryPath == "" {
		t.Skip("binary not built")
	}

	ingressPort := freePort(t)
	pullPort := freePort(t)
	adminPort := freePort(t)
	inst := startHookaido(t, ingressPort, pullPort, adminPort)

	resp, err := http.Get(inst.adminURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !json.Valid(body) {
		t.Fatalf("response is not valid JSON: %s", string(body))
	}
}

func TestBinaryE2E_IngressToPull(t *testing.T) {
	if testBinaryPath == "" {
		t.Skip("binary not built")
	}

	ingressPort := freePort(t)
	pullPort := freePort(t)
	adminPort := freePort(t)
	inst := startHookaido(t, ingressPort, pullPort, adminPort)

	// Send a webhook via ingress.
	payload := []byte(`{"event":"test"}`)
	resp, err := http.Post(inst.ingressURL+"/webhooks", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("POST ingress: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("ingress status = %d, want 2xx", resp.StatusCode)
	}

	// Dequeue via pull API.
	dequeueBody, _ := json.Marshal(map[string]any{
		"endpoint":  "/webhooks",
		"batch":     1,
		"lease_ttl": "30s",
	})
	resp, err = http.Post(inst.pullURL+"/dequeue", "application/json", bytes.NewReader(dequeueBody))
	if err != nil {
		t.Fatalf("POST dequeue: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("dequeue status = %d, body = %s", resp.StatusCode, string(body))
	}

	var dequeueResp struct {
		Items []struct {
			ID      string `json:"id"`
			LeaseID string `json:"lease_id"`
			Payload string `json:"payload"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&dequeueResp); err != nil {
		t.Fatalf("decode dequeue: %v", err)
	}
	if len(dequeueResp.Items) != 1 {
		t.Fatalf("dequeued %d items, want 1", len(dequeueResp.Items))
	}

	// Ack the message.
	ackBody, _ := json.Marshal(map[string]string{
		"lease_id": dequeueResp.Items[0].LeaseID,
	})
	resp, err = http.Post(inst.pullURL+"/ack", "application/json", bytes.NewReader(ackBody))
	if err != nil {
		t.Fatalf("POST ack: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ack status = %d, want 200", resp.StatusCode)
	}
}

func TestBinaryE2E_ConfigValidate(t *testing.T) {
	if testBinaryPath == "" {
		t.Skip("binary not built")
	}

	config := `ingress {
    listen ":8080"
}

route /test {
    target "default"
    queue memory
    pull {
        path "/test"
    }
}

pull_api {
    listen ":9443"
}

admin {
    listen ":2019"
}
`
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(config), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cmd := exec.Command(testBinaryPath, "config", "validate", "--config", cfgPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("config validate failed: %v\n%s", err, string(out))
	}
}

func TestBinaryE2E_ConfigFmtIdempotent(t *testing.T) {
	if testBinaryPath == "" {
		t.Skip("binary not built")
	}

	config := `ingress {
    listen ":8080"
}

route /test {
    target "default"
    queue memory
    pull {
        path "/test"
    }
}

pull_api {
    listen ":9443"
}

admin {
    listen ":2019"
}
`
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(config), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// First format pass.
	cmd1 := exec.Command(testBinaryPath, "config", "fmt", "--config", cfgPath)
	out1, err := cmd1.CombinedOutput()
	if err != nil {
		t.Fatalf("config fmt (1st): %v\n%s", err, string(out1))
	}

	// Second format pass -- should be identical.
	cmd2 := exec.Command(testBinaryPath, "config", "fmt", "--config", cfgPath)
	out2, err := cmd2.CombinedOutput()
	if err != nil {
		t.Fatalf("config fmt (2nd): %v\n%s", err, string(out2))
	}

	if !bytes.Equal(out1, out2) {
		t.Fatalf("config fmt is not idempotent:\n--- first ---\n%s\n--- second ---\n%s", out1, out2)
	}
}

func TestBinaryE2E_RejectInvalidConfig(t *testing.T) {
	if testBinaryPath == "" {
		t.Skip("binary not built")
	}

	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte("this is not valid config {{{"), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cmd := exec.Command(testBinaryPath, "run", "--config", cfgPath)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit for invalid config, got success\n%s", string(out))
	}

	combined := strings.ToLower(string(out))
	if !strings.Contains(combined, "error") && !strings.Contains(combined, "parse") && !strings.Contains(combined, "invalid") {
		t.Fatalf("expected error message in output, got: %s", string(out))
	}
}
```

**Step 2: Run test to verify it passes**

Run: `go test ./internal/e2e/ -v -run TestBinaryE2E -count=1 -timeout 120s`
Expected: All subtests PASS (binary builds, starts, serves requests, CLI commands work).

**Important:** The `TestMain` in this file will take over from the existing test runner for the `e2e` package. The existing in-process E2E tests in `e2e_test.go` will still run since they don't depend on `testBinaryPath`. Verify all existing E2E tests still pass.

Run: `go test ./internal/e2e/ -v -count=1 -timeout 120s`
Expected: All tests PASS (both existing and new).

**Step 3: Commit**

```
git add internal/e2e/binary_test.go
git commit -m "test(e2e): add binary-level E2E tests"
```

---

## Phase 3: Postgres Test Extension

### Task 7: Postgres Unit Tests (No DB Required)

**Files:**
- Modify: `modules/postgres/postgres_test.go`
- Reference: `modules/postgres/postgres.go:2028-2160` (helper functions)

**Step 1: Add unit tests for helpers and options**

Append to `modules/postgres/postgres_test.go`:

```go
// --- Option function tests ---

func TestWithQueueLimits(t *testing.T) {
	s := &Store{}
	WithQueueLimits(500, "drop_oldest")(s)

	if s.maxDepth != 500 {
		t.Fatalf("maxDepth = %d, want 500", s.maxDepth)
	}
	if s.dropPolicy != "drop_oldest" {
		t.Fatalf("dropPolicy = %q, want drop_oldest", s.dropPolicy)
	}
}

func TestWithQueueLimits_Normalization(t *testing.T) {
	s := &Store{}
	WithQueueLimits(0, "  REJECT  ")(s)

	if s.maxDepth != 0 {
		t.Fatalf("maxDepth = %d, want 0 for zero input", s.maxDepth)
	}
	if s.dropPolicy != "reject" {
		t.Fatalf("dropPolicy = %q, want reject (lowercased+trimmed)", s.dropPolicy)
	}
}

func TestWithRetention(t *testing.T) {
	s := &Store{}
	WithRetention(24*time.Hour, 5*time.Minute)(s)

	if s.retentionMaxAge != 24*time.Hour {
		t.Fatalf("retentionMaxAge = %v, want 24h", s.retentionMaxAge)
	}
	if s.pruneInterval != 5*time.Minute {
		t.Fatalf("pruneInterval = %v, want 5m", s.pruneInterval)
	}
}

func TestWithRetention_Zero(t *testing.T) {
	s := &Store{retentionMaxAge: time.Hour, pruneInterval: time.Minute}
	WithRetention(0, 0)(s)

	if s.retentionMaxAge != 0 {
		t.Fatalf("retentionMaxAge = %v, want 0", s.retentionMaxAge)
	}
	if s.pruneInterval != 0 {
		t.Fatalf("pruneInterval = %v, want 0", s.pruneInterval)
	}
}

func TestWithDeliveredRetention(t *testing.T) {
	s := &Store{}
	WithDeliveredRetention(48 * time.Hour)(s)
	if s.deliveredRetentionMaxAge != 48*time.Hour {
		t.Fatalf("deliveredRetentionMaxAge = %v, want 48h", s.deliveredRetentionMaxAge)
	}
}

func TestWithDLQRetention(t *testing.T) {
	s := &Store{}
	WithDLQRetention(72*time.Hour, 5000)(s)
	if s.dlqRetentionMaxAge != 72*time.Hour {
		t.Fatalf("dlqRetentionMaxAge = %v, want 72h", s.dlqRetentionMaxAge)
	}
	if s.dlqMaxDepth != 5000 {
		t.Fatalf("dlqMaxDepth = %d, want 5000", s.dlqMaxDepth)
	}
}

func TestWithPollInterval(t *testing.T) {
	s := &Store{}
	WithPollInterval(100 * time.Millisecond)(s)
	if s.pollInterval != 100*time.Millisecond {
		t.Fatalf("pollInterval = %v, want 100ms", s.pollInterval)
	}
}

func TestWithPollInterval_Negative(t *testing.T) {
	s := &Store{pollInterval: 50 * time.Millisecond}
	WithPollInterval(-1)(s)
	if s.pollInterval != 50*time.Millisecond {
		t.Fatalf("negative poll interval should be ignored, got %v", s.pollInterval)
	}
}

func TestWithNowFunc(t *testing.T) {
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &Store{}
	WithNowFunc(func() time.Time { return fixed })(s)
	if s.nowFn == nil {
		t.Fatal("nowFn should be set")
	}
	if got := s.nowFn(); !got.Equal(fixed) {
		t.Fatalf("nowFn() = %v, want %v", got, fixed)
	}
}

func TestWithNowFunc_Nil(t *testing.T) {
	orig := func() time.Time { return time.Time{} }
	s := &Store{nowFn: orig}
	WithNowFunc(nil)(s)
	if s.nowFn == nil {
		t.Fatal("nil nowFn should be ignored")
	}
}

// --- postgresMetricErrorKind tests ---

func TestPostgresMetricErrorKind(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", err: nil, want: ""},
		{name: "queue_full", err: queue.ErrQueueFull, want: "queue_full"},
		{name: "duplicate", err: queue.ErrEnvelopeExists, want: "duplicate"},
		{name: "lease_not_found", err: queue.ErrLeaseNotFound, want: "lease_not_found"},
		{name: "lease_expired", err: queue.ErrLeaseExpired, want: "lease_expired"},
		{name: "unknown_error", err: fmt.Errorf("something broke"), want: "other"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := postgresMetricErrorKind(tt.err)
			if got != tt.want {
				t.Fatalf("postgresMetricErrorKind(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

// --- Helper function tests ---

func TestClampSliceCap(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		max      int
		want     int
	}{
		{name: "normal", size: 10, max: 100, want: 10},
		{name: "clamped", size: 200, max: 100, want: 100},
		{name: "zero_size", size: 0, max: 100, want: 0},
		{name: "negative_size", size: -1, max: 100, want: 0},
		{name: "zero_max", size: 10, max: 0, want: 0},
		{name: "negative_max", size: 10, max: -1, want: 0},
		{name: "equal", size: 50, max: 50, want: 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := clampSliceCap(tt.size, tt.max)
			if got != tt.want {
				t.Fatalf("clampSliceCap(%d, %d) = %d, want %d", tt.size, tt.max, got, tt.want)
			}
		})
	}
}

func TestNormalizeUniqueIDs(t *testing.T) {
	tests := []struct {
		name string
		ids  []string
		want []string
	}{
		{name: "nil", ids: nil, want: nil},
		{name: "empty", ids: []string{}, want: nil},
		{name: "dedup", ids: []string{"a", "b", "a"}, want: []string{"a", "b"}},
		{name: "trim", ids: []string{"  a  ", "b", "  a"}, want: []string{"a", "b"}},
		{name: "skip_empty", ids: []string{"a", "", "  ", "b"}, want: []string{"a", "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeUniqueIDs(tt.ids)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestNullIfEmpty(t *testing.T) {
	if nullIfEmpty("hello") != "hello" {
		t.Fatal("non-empty string should pass through")
	}
	if nullIfEmpty("") != nil {
		t.Fatal("empty string should return nil")
	}
	if nullIfEmpty("   ") != nil {
		t.Fatal("whitespace-only string should return nil")
	}
}

func TestNullTime(t *testing.T) {
	if nullTime(time.Time{}) != nil {
		t.Fatal("zero time should return nil")
	}
	now := time.Now()
	got := nullTime(now)
	if got == nil {
		t.Fatal("non-zero time should not return nil")
	}
}

func TestNullInt(t *testing.T) {
	if nullInt(0) != nil {
		t.Fatal("zero int should return nil")
	}
	if nullInt(42) != 42 {
		t.Fatal("non-zero int should pass through")
	}
}

func TestEncodeStringMapJSON(t *testing.T) {
	got, err := encodeStringMapJSON(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != "{}" {
		t.Fatalf("nil map should encode to {}, got %q", string(got))
	}

	got, err = encodeStringMapJSON(map[string]string{"key": "val"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != `{"key":"val"}` {
		t.Fatalf("got %q", string(got))
	}
}

func TestDecodeStringMapJSON(t *testing.T) {
	if got := decodeStringMapJSON(nil); got != nil {
		t.Fatalf("nil input should return nil, got %v", got)
	}
	if got := decodeStringMapJSON([]byte("{}")); got != nil {
		t.Fatalf("empty map should return nil, got %v", got)
	}
	if got := decodeStringMapJSON([]byte("invalid")); got != nil {
		t.Fatalf("invalid JSON should return nil, got %v", got)
	}

	got := decodeStringMapJSON([]byte(`{"key":"val"}`))
	if got == nil || got["key"] != "val" {
		t.Fatalf("got %v, want {key:val}", got)
	}
}

func TestMapPostgresInsertError(t *testing.T) {
	if err := mapPostgresInsertError(nil); err != nil {
		t.Fatalf("nil error should return nil, got %v", err)
	}

	generic := fmt.Errorf("some error")
	if err := mapPostgresInsertError(generic); err != generic {
		t.Fatalf("generic error should pass through")
	}
}
```

Add `"fmt"` to the imports at the top of the file (alongside the existing imports).

**Step 2: Run test to verify it passes**

Run: `go test ./modules/postgres/ -v -count=1`
Expected: All subtests PASS (no Postgres needed -- all new tests work without a database).

**Step 3: Commit**

```
git add modules/postgres/postgres_test.go
git commit -m "test(postgres): add unit tests for options, helpers, and error mapping"
```

---

### Task 8: Postgres Docker + DSN-Gated Integration Tests

**Files:**
- Create: `docker-compose.test.yml`
- Modify: `modules/postgres/postgres_test.go` (add DSN-gated tests)

**Step 1: Create docker-compose.test.yml**

```yaml
# Postgres test instance for integration tests.
# Usage:
#   docker compose -f docker-compose.test.yml up -d
#   HOOKAIDO_TEST_POSTGRES_DSN="postgres://hookaido_test:hookaido_test@localhost:5433/hookaido_test?sslmode=disable" make test
#   docker compose -f docker-compose.test.yml down

services:
  postgres-test:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: hookaido_test
      POSTGRES_PASSWORD: hookaido_test
      POSTGRES_DB: hookaido_test
    ports:
      - "5433:5432"
    tmpfs:
      - /var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U hookaido_test"]
      interval: 2s
      timeout: 5s
      retries: 10
```

**Step 2: Append DSN-gated tests to postgres_test.go**

```go
// --- DSN-gated integration tests ---

func requirePostgresDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("HOOKAIDO_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("HOOKAIDO_TEST_POSTGRES_DSN not set")
	}
	return dsn
}

func TestIntegration_SchemaInit(t *testing.T) {
	dsn := requirePostgresDSN(t)
	s, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()

	// Verify all expected tables exist by querying pg_tables.
	tables := []string{"queue_items", "delivery_attempts", "backlog_trend_samples"}
	for _, table := range tables {
		var exists bool
		err := s.db.QueryRow(`
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_name = $1
			)
		`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("query table %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("table %s should exist after init", table)
		}
	}
}

func TestIntegration_EnqueueDequeueRoundTrip(t *testing.T) {
	dsn := requirePostgresDSN(t)
	now := time.Date(2026, 3, 13, 12, 0, 0, 0, time.UTC)
	s, err := NewStore(dsn, WithNowFunc(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()

	env := queue.Envelope{
		ID:      "integration_evt_1",
		Route:   "/test",
		Target:  "default",
		Payload: []byte(`{"test":true}`),
		Headers: map[string]string{"X-Test": "yes"},
	}
	if err := s.Enqueue(env); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	resp, err := s.Dequeue(queue.DequeueRequest{
		Route:    "/test",
		Target:   "default",
		Batch:    1,
		LeaseTTL: 30 * time.Second,
		Now:      now,
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(resp.Items))
	}

	item := resp.Items[0]
	if item.ID != "integration_evt_1" {
		t.Fatalf("id = %q, want integration_evt_1", item.ID)
	}
	if string(item.Payload) != `{"test":true}` {
		t.Fatalf("payload = %q", string(item.Payload))
	}
	if item.Headers["X-Test"] != "yes" {
		t.Fatalf("headers = %v", item.Headers)
	}
	if item.State != queue.StateLeased {
		t.Fatalf("state = %q, want leased", item.State)
	}
	if item.LeaseID == "" {
		t.Fatal("lease_id should not be empty")
	}
}

func TestIntegration_QueueLimits(t *testing.T) {
	dsn := requirePostgresDSN(t)
	s, err := NewStore(dsn, WithQueueLimits(2, "reject"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()

	for i := 0; i < 2; i++ {
		env := queue.Envelope{
			ID:     fmt.Sprintf("limit_evt_%d", i),
			Route:  "/limit",
			Target: "default",
		}
		if err := s.Enqueue(env); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	err = s.Enqueue(queue.Envelope{ID: "limit_evt_2", Route: "/limit", Target: "default"})
	if err != queue.ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestIntegration_BacklogTrendCapture(t *testing.T) {
	dsn := requirePostgresDSN(t)
	now := time.Date(2026, 3, 13, 12, 0, 0, 0, time.UTC)
	s, err := NewStore(dsn, WithNowFunc(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()

	if err := s.Enqueue(queue.Envelope{ID: "trend_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := s.CaptureBacklogTrendSample(now); err != nil {
		t.Fatalf("capture: %v", err)
	}

	resp, err := s.ListBacklogTrend(queue.BacklogTrendListRequest{
		Since: now.Add(-time.Minute),
		Until: now.Add(time.Minute),
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(resp.Items))
	}
	if resp.Items[0].Queued != 1 {
		t.Fatalf("queued = %d, want 1", resp.Items[0].Queued)
	}
}

func TestIntegration_ConcurrentDequeue(t *testing.T) {
	dsn := requirePostgresDSN(t)
	now := time.Date(2026, 3, 13, 12, 0, 0, 0, time.UTC)
	s, err := NewStore(dsn, WithNowFunc(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()

	// Enqueue one item.
	if err := s.Enqueue(queue.Envelope{ID: "concurrent_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// 10 concurrent dequeue attempts -- only one should succeed.
	results := make(chan int, 10)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := s.Dequeue(queue.DequeueRequest{
				Route:    "/r",
				Target:   "pull",
				Batch:    1,
				LeaseTTL: 30 * time.Second,
				Now:      now,
			})
			if err != nil {
				return
			}
			results <- len(resp.Items)
		}()
	}
	wg.Wait()
	close(results)

	total := 0
	for n := range results {
		total += n
	}
	if total != 1 {
		t.Fatalf("total dequeued = %d, want exactly 1 (FOR UPDATE SKIP LOCKED)", total)
	}
}
```

Add `"os"`, `"sync"`, and `"fmt"` to the imports if not already present.

**Step 2: Start Postgres and run integration tests**

```bash
docker compose -f docker-compose.test.yml up -d --wait
HOOKAIDO_TEST_POSTGRES_DSN="postgres://hookaido_test:hookaido_test@localhost:5433/hookaido_test?sslmode=disable" go test ./modules/postgres/ -v -count=1
docker compose -f docker-compose.test.yml down
```

Expected: All tests PASS (both unit and integration).

**Step 3: Commit**

```
git add docker-compose.test.yml modules/postgres/postgres_test.go
git commit -m "test(postgres): add DSN-gated integration tests and docker-compose.test.yml"
```

---

## Final Verification

After all tasks are complete, run the full test suite:

```bash
go test ./... -count=1 -timeout 300s
```

Expected: All tests PASS. No regressions in existing tests.

Then verify the new test count:

```bash
go test ./... -v -count=1 2>&1 | grep -c "^--- PASS\|^=== RUN"
```

Expected: Approximately 90+ new tests added across the project.
