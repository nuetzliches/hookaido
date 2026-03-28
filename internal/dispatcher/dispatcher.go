package dispatcher

import (
	"context"
	"net/http"
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
}

type Deliverer interface {
	Deliver(ctx context.Context, d Delivery) Result
}
