package app

import (
	"sync"
	"time"
)

type tokenBucketLimiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

// normalizeBucketLimits clamps the configured values the way a live limiter
// holds them, so a comparison against one sees the same numbers.
func normalizeBucketLimits(rps float64, burst int) (float64, float64) {
	if rps <= 0 {
		rps = 1
	}
	if burst <= 0 {
		burst = 1
	}
	return rps, float64(burst)
}

func newTokenBucketLimiter(rps float64, burst int, now time.Time) *tokenBucketLimiter {
	rate, burstF := normalizeBucketLimits(rps, burst)
	t := now
	if t.IsZero() {
		t = time.Now()
	}
	return &tokenBucketLimiter{
		rate:   rate,
		burst:  burstF,
		tokens: burstF,
		last:   t,
	}
}

// matches reports whether l is already configured for exactly these limits, so
// a caller can keep it instead of replacing it with a fresh, full bucket. A nil
// limiter never matches: there is nothing to keep.
func (l *tokenBucketLimiter) matches(rps float64, burst int) bool {
	if l == nil {
		return false
	}
	wantRate, wantBurst := normalizeBucketLimits(rps, burst)
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rate == wantRate && l.burst == wantBurst
}

func (l *tokenBucketLimiter) AllowAt(now time.Time) bool {
	if l == nil {
		return true
	}
	t := now
	if t.IsZero() {
		t = time.Now()
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.last.IsZero() {
		l.last = t
	}
	if dt := t.Sub(l.last).Seconds(); dt > 0 {
		l.tokens += dt * l.rate
		if l.tokens > l.burst {
			l.tokens = l.burst
		}
		l.last = t
	}

	if l.tokens < 1 {
		return false
	}
	l.tokens -= 1
	return true
}
