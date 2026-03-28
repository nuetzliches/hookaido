package dispatcher

import (
	"context"
	"net/http"
)

type Delivery struct {
	ID     string
	Route  string
	Target string
	Method string
	URL    string
	Header http.Header
	Body   []byte
	Sign   *HMACSigningConfig

	// Exec delivery fields.
	IsExec  bool
	ExecEnv map[string]string
	Attempt int
}

type Result struct {
	StatusCode int
	Err        error
}

type Deliverer interface {
	Deliver(ctx context.Context, d Delivery) Result
}
