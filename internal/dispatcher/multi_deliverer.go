package dispatcher

import "context"

// MultiDeliverer delegates to either an ExecDeliverer or an HTTPDeliverer
// based on the Delivery.IsExec flag.
type MultiDeliverer struct {
	HTTP *HTTPDeliverer
	Exec *ExecDeliverer
}

func (m *MultiDeliverer) Deliver(ctx context.Context, d Delivery) Result {
	if d.IsExec {
		return m.Exec.Deliver(ctx, d)
	}
	return m.HTTP.Deliver(ctx, d)
}
