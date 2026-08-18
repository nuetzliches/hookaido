package app

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucketLimiterBurstAllowsN(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	lim := newTokenBucketLimiter(10, 5, now)

	for i := 0; i < 5; i++ {
		if !lim.AllowAt(now) {
			t.Fatalf("request %d should be allowed within burst of 5", i+1)
		}
	}
}

func TestTokenBucketLimiterBurstExhausted(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	lim := newTokenBucketLimiter(10, 5, now)

	// Drain all 5 burst tokens.
	for i := 0; i < 5; i++ {
		lim.AllowAt(now)
	}

	if lim.AllowAt(now) {
		t.Fatal("request 6 should be rejected after burst is exhausted")
	}
}

func TestTokenBucketLimiterReplenishment(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rps := 10.0
	lim := newTokenBucketLimiter(rps, 5, now)

	// Drain all tokens at t=0.
	for i := 0; i < 5; i++ {
		lim.AllowAt(now)
	}
	if lim.AllowAt(now) {
		t.Fatal("should be exhausted")
	}

	// Advance 100ms → 10 rps * 0.1s = 1 token replenished.
	later := now.Add(100 * time.Millisecond)
	if !lim.AllowAt(later) {
		t.Fatal("should have 1 token after 100ms at 10 rps")
	}
	// That single token should now be consumed.
	if lim.AllowAt(later) {
		t.Fatal("should be exhausted again after consuming replenished token")
	}
}

func TestTokenBucketLimiterReplenishmentCappedAtBurst(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	burst := 3
	lim := newTokenBucketLimiter(10, burst, now)

	// Drain all tokens.
	for i := 0; i < burst; i++ {
		lim.AllowAt(now)
	}

	// Advance 10 seconds → 10 rps * 10s = 100 tokens, but capped at burst=3.
	later := now.Add(10 * time.Second)

	allowed := 0
	for i := 0; i < burst+2; i++ {
		if lim.AllowAt(later) {
			allowed++
		}
	}
	if allowed != burst {
		t.Fatalf("expected exactly %d allowed (burst cap), got %d", burst, allowed)
	}
}

func TestTokenBucketLimiterNilReceiver(t *testing.T) {
	var lim *tokenBucketLimiter
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if !lim.AllowAt(now) {
		t.Fatal("nil receiver should always return true")
	}
}

func TestTokenBucketLimiterZeroTimeFallback(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	lim := newTokenBucketLimiter(10, 5, now)

	// Passing zero time should not panic and should use time.Now() internally.
	if !lim.AllowAt(time.Time{}) {
		t.Fatal("AllowAt with zero time should allow (burst still available)")
	}
}

func TestTokenBucketLimiterNegativeZeroRPS(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, rps := range []float64{0, -1, -100} {
		lim := newTokenBucketLimiter(rps, 5, now)
		if lim.rate != 1 {
			t.Errorf("rps=%v: expected rate normalized to 1, got %v", rps, lim.rate)
		}
	}
}

func TestTokenBucketLimiterNegativeZeroBurst(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, burst := range []int{0, -1, -100} {
		lim := newTokenBucketLimiter(10, burst, now)
		if lim.burst != 1 {
			t.Errorf("burst=%d: expected burst normalized to 1, got %v", burst, lim.burst)
		}
		if lim.tokens != 1 {
			t.Errorf("burst=%d: expected tokens initialized to 1, got %v", burst, lim.tokens)
		}
	}
}

func TestTokenBucketLimiterConcurrency(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	burst := 100
	lim := newTokenBucketLimiter(1000, burst, now)

	var (
		wg      sync.WaitGroup
		allowed atomic.Int64
	)

	goroutines := 200
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			// All goroutines use the same timestamp so no replenishment occurs.
			if lim.AllowAt(now) {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	got := allowed.Load()
	if got != int64(burst) {
		t.Fatalf("expected exactly %d allowed out of %d goroutines, got %d", burst, goroutines, got)
	}
}

func TestTokenBucketLimiter_Matches(t *testing.T) {
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	lim := newTokenBucketLimiter(10, 5, now)

	if !lim.matches(10, 5) {
		t.Fatalf("expected identical limits to match")
	}
	if lim.matches(11, 5) {
		t.Fatalf("expected a changed rps not to match")
	}
	if lim.matches(10, 6) {
		t.Fatalf("expected a changed burst not to match")
	}
	// newTokenBucketLimiter clamps both values, so the comparison has to clamp
	// too or a config of 0 would look like a change on every reload.
	clamped := newTokenBucketLimiter(0, 0, now)
	if !clamped.matches(0, 0) {
		t.Fatalf("expected clamped limits to match their own config")
	}
	if !clamped.matches(1, 1) {
		t.Fatalf("expected clamped limits to match the values they were clamped to")
	}

	var nilLim *tokenBucketLimiter
	if nilLim.matches(10, 5) {
		t.Fatalf("expected a nil limiter never to match")
	}
}

func TestConfigureIngressRateLimits_PreservesBucketsAcrossReload(t *testing.T) {
	// Every new limiter starts with tokens = burst, and this ran on every
	// successful reload -- including each applied Admin API managed-endpoint
	// mutation and each --watch write -- so a reload refilled every bucket and
	// the effective rate limit was unbounded at reload frequency.
	src := `
ingress {
  listen ":8080"
  rate_limit { rps 10 burst 2 }
}

"/hooks" {
  rate_limit { rps 10 burst 2 }
  deliver "https://a.example.com/x" { }
}
`
	compiled := compileForReloadTest(t, src)

	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	state := newRuntimeState(compiled)
	state.now = func() time.Time { return now }
	state.configureIngressRateLimits(compiled)

	globalBefore := state.ingressGlobalLimit
	routeBefore := state.ingressRouteLimits["/hooks"]
	if globalBefore == nil || routeBefore == nil {
		t.Fatalf("expected both limiters to be configured")
	}

	// Drain both buckets at a fixed instant so no refill can occur.
	for i := 0; i < 2; i++ {
		if !globalBefore.AllowAt(now) || !routeBefore.AllowAt(now) {
			t.Fatalf("expected the first %d requests to be allowed", i+1)
		}
	}
	if globalBefore.AllowAt(now) || routeBefore.AllowAt(now) {
		t.Fatalf("expected both buckets to be empty")
	}

	// A reload that does not touch the rate limits.
	state.updateAll(compileForReloadTest(t, src))

	if state.ingressGlobalLimit != globalBefore {
		t.Fatalf("expected the global limiter to be carried over")
	}
	if state.ingressRouteLimits["/hooks"] != routeBefore {
		t.Fatalf("expected the route limiter to be carried over")
	}
	if state.ingressGlobalLimit.AllowAt(now) {
		t.Fatalf("global bucket refilled across the reload")
	}
	if state.ingressRouteLimits["/hooks"].AllowAt(now) {
		t.Fatalf("route bucket refilled across the reload")
	}
}

func TestConfigureIngressRateLimits_ReplacesBucketsWhenLimitsChange(t *testing.T) {
	before := compileForReloadTest(t, `
ingress {
  listen ":8080"
  rate_limit { rps 10 burst 2 }
}

"/hooks" {
  rate_limit { rps 10 burst 2 }
  deliver "https://a.example.com/x" { }
}
`)
	after := compileForReloadTest(t, `
ingress {
  listen ":8080"
  rate_limit { rps 50 burst 2 }
}

"/hooks" {
  rate_limit { rps 10 burst 9 }
  deliver "https://a.example.com/x" { }
}
`)

	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	state := newRuntimeState(before)
	state.now = func() time.Time { return now }
	state.configureIngressRateLimits(before)

	globalBefore := state.ingressGlobalLimit
	routeBefore := state.ingressRouteLimits["/hooks"]

	state.updateAll(after)

	if state.ingressGlobalLimit == globalBefore {
		t.Fatalf("expected a changed rps to produce a fresh global limiter")
	}
	if state.ingressRouteLimits["/hooks"] == routeBefore {
		t.Fatalf("expected a changed burst to produce a fresh route limiter")
	}
	// The replacement must actually carry the new limits.
	if !state.ingressGlobalLimit.matches(50, 2) {
		t.Fatalf("global limiter did not adopt the new rps")
	}
	if !state.ingressRouteLimits["/hooks"].matches(10, 9) {
		t.Fatalf("route limiter did not adopt the new burst")
	}
}

func TestConfigureIngressRateLimits_DropsLimitersForRemovedRoutes(t *testing.T) {
	before := compileForReloadTest(t, `
"/hooks" {
  rate_limit { rps 10 burst 2 }
  deliver "https://a.example.com/x" { }
}

"/other" {
  rate_limit { rps 10 burst 2 }
  deliver "https://b.example.com/x" { }
}
`)
	after := compileForReloadTest(t, `
"/hooks" {
  rate_limit { rps 10 burst 2 }
  deliver "https://a.example.com/x" { }
}
`)

	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	state := newRuntimeState(before)
	state.now = func() time.Time { return now }
	state.configureIngressRateLimits(before)
	if _, ok := state.ingressRouteLimits["/other"]; !ok {
		t.Fatalf("expected /other to start with a limiter")
	}

	state.updateAll(after)

	if _, ok := state.ingressRouteLimits["/other"]; ok {
		t.Fatalf("expected the removed route's limiter to be dropped")
	}
	if _, ok := state.ingressRouteLimits["/hooks"]; !ok {
		t.Fatalf("expected /hooks to keep its limiter")
	}
}

func TestConfigureIngressRateLimits_ClearsGlobalLimiterWhenDisabled(t *testing.T) {
	before := compileForReloadTest(t, `
ingress {
  listen ":8080"
  rate_limit { rps 10 burst 2 }
}

"/hooks" {
  deliver "https://a.example.com/x" { }
}
`)
	after := compileForReloadTest(t, `
ingress { listen ":8080" }

"/hooks" {
  deliver "https://a.example.com/x" { }
}
`)

	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	state := newRuntimeState(before)
	state.now = func() time.Time { return now }
	state.configureIngressRateLimits(before)
	if state.ingressGlobalLimit == nil {
		t.Fatalf("expected a global limiter to start configured")
	}

	state.updateAll(after)

	if state.ingressGlobalLimit != nil {
		t.Fatalf("expected the global limiter to be cleared when disabled")
	}
}
