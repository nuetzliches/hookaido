package dispatcher

import (
	"context"
	"net/http"
)

type Delivery struct {
	ID     string
	Target string
	Method string
	URL    string
	Header http.Header
	Body   []byte
	Sign   *HMACSigningConfig
}

type Result struct {
	StatusCode int
	Err        error
}

type Deliverer interface {
	Deliver(ctx context.Context, d Delivery) Result
}
