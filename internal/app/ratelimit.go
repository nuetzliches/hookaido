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

func newTokenBucketLimiter(rps float64, burst int, now time.Time) *tokenBucketLimiter {
	if rps <= 0 {
		rps = 1
	}
	if burst <= 0 {
		burst = 1
	}
	t := now
	if t.IsZero() {
		t = time.Now()
	}
	return &tokenBucketLimiter{
		rate:   rps,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   t,
	}
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
