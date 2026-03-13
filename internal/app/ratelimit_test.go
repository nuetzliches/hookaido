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
