package dispatcher

import (
	"context"
	"net/http"
	"time"
)

type CustomHeader struct {
	Name  string
	Value string
}

type Delivery struct {
	ID            string
	Route         string
	Target        string
	Method        string
	URL           string
	Header        http.Header
	Body          []byte
	CustomHeaders []CustomHeader
	Sign          *HMACSigningConfig

	// Exec delivery fields.
	IsExec  bool
	ExecEnv map[string]string
	Attempt int
}

type Result struct {
	StatusCode int
	Err        error

	// RetryAfter carries the target's Retry-After hint, already resolved to a
	// duration (delta-seconds and HTTP-date both land here). Zero means the
	// header was absent, unparseable, or already in the past — the retry
	// schedule then applies unchanged. Only the HTTP deliverer sets it; exec
	// delivery has no equivalent.
	RetryAfter time.Duration
}

type Deliverer interface {
	Deliver(ctx context.Context, d Delivery) Result
}
