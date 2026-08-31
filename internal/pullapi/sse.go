package pullapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/queue"
)

const (
	defaultSSEKeepalive = 15 * time.Second
	sseFallbackPoll     = time.Second
)

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request, q Queue) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, pullErrInternal, "streaming not supported")
		return
	}

	batch := 1
	if v := r.URL.Query().Get("batch"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, pullErrInvalidBody, "batch must be a positive integer")
			return
		}
		batch = n
		if s.MaxBatch > 0 && batch > s.MaxBatch {
			batch = s.MaxBatch
		}
	}

	leaseTTL := s.DefaultLeaseTTL
	if v := r.URL.Query().Get("lease_ttl"); v != "" {
		d, ok := parseDuration(v)
		if !ok || d <= 0 {
			writeError(w, http.StatusBadRequest, pullErrInvalidBody, "lease_ttl must be a valid positive duration")
			return
		}
		leaseTTL = d
		if s.MaxLeaseTTL > 0 && leaseTTL > s.MaxLeaseTTL {
			leaseTTL = s.MaxLeaseTTL
		}
	}

	ctx := r.Context()
	if s.SSEMaxConnection > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.SSEMaxConnection)
		defer cancel()
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	keepalive := s.SSEKeepalive
	if keepalive <= 0 {
		keepalive = defaultSSEKeepalive
	}

	// Determine notification source.
	var notifyCh func() <-chan struct{}
	if sn, ok := s.Store.(queue.StoreNotifier); ok {
		notifyCh = sn.NotifyCh
	}

	// The stream enters the registry only once the response head is out, so
	// what /admin/pull/consumers lists is exactly what
	// hookaido_pull_sse_connection_active counts: streams a consumer is
	// attached to, not requests that failed validation above and never became
	// one.
	//
	// Teardown is a single defer rather than a call on each return path. There
	// are seven of those, each previously repeating the same observe line, and
	// one of them was missed for long enough that the gauge only ever counted
	// up (see the cancellation comment below).
	consumer := s.registerConsumer(r, q)
	status := http.StatusOK
	defer func() { s.unregisterConsumer(consumer, status) }()

	keepaliveTicker := time.NewTicker(keepalive)
	defer keepaliveTicker.Stop()

	for {
		// Check for cancellation before doing more work.
		//
		// The busy path below ends in `continue`, so a stream with items always
		// available never reaches the select at the bottom of the loop -- the
		// only place ctx.Done() was handled. Two consequences: a client that
		// disconnected mid-stream kept having messages dequeued and leased on
		// its behalf, which nobody would ever ack, and SSEMaxConnection never
		// fired on exactly the busy connections it exists to bound.
		// observeSSEDisconnect never ran either, so
		// hookaido_pull_sse_connection_active only ever counted up.
		if ctx.Err() != nil {
			return
		}

		// Capture the readiness channel *before* the dequeue.
		//
		// The dequeue below is non-blocking (MaxWait 0), so it registers no
		// waiter. Taking the channel afterwards left a window: MemoryStore
		// closes and replaces its notify channel on every enqueue, so a message
		// published between the empty dequeue and the call handed this stream
		// the fresh channel -- which fires only on the *next* enqueue. A single
		// message landing in that window sat queued and ready while the
		// connected consumer blocked until the keepalive tick (15s by default,
		// and sse_keepalive has no upper bound).
		//
		// Captured first, an enqueue is either visible to the dequeue or closes
		// the channel already held. The store's own long-poll Dequeue avoids
		// the same race the same way, capturing the channel under the lock that
		// checks emptiness.
		var ready <-chan struct{}
		if notifyCh != nil {
			ready = notifyCh()
		}

		// Non-blocking dequeue.
		outcome, opErr := s.Dequeue(ctx, q, DequeueParams{
			Batch:       batch,
			MaxWait:     0,
			HasMaxWait:  true,
			LeaseTTL:    leaseTTL,
			HasLeaseTTL: true,
		})
		if opErr != nil {
			// Store unavailable — write an SSE error event and close.
			fmt.Fprintf(w, "event: error\ndata: %s\n\n", opErr.Detail)
			flusher.Flush()
			status = opErr.StatusCode
			return
		}

		if len(outcome.Items) > 0 {
			for _, it := range outcome.Items {
				item := dequeueItem{
					ID:         it.ID,
					LeaseID:    it.LeaseID,
					ReceivedAt: it.ReceivedAt,
					Attempt:    it.Attempt,
					NextRunAt:  it.NextRunAt,
					Route:      it.Route,
					PayloadB64: base64.StdEncoding.EncodeToString(it.Payload),
					Headers:    it.Headers,
					Trace:      it.Trace,
				}
				data, _ := json.Marshal(item)
				// A failed write means the peer is gone. The keepalive paths
				// below already checked this; the per-item write discarded the
				// error, so on a busy stream a disconnect went unnoticed until
				// the next idle moment -- which may never come.
				if _, err := fmt.Fprintf(w, "id: %s\nevent: message\ndata: %s\n\n", it.LeaseID, data); err != nil {
					return
				}
				s.recordMessagesSent(consumer, 1)
			}
			flusher.Flush()
			// Reset keepalive timer after activity.
			keepaliveTicker.Reset(keepalive)
			continue
		}

		// No items available — wait for notification, keepalive, or cancellation.
		if ready != nil {
			select {
			case <-ready:
				continue
			case <-keepaliveTicker.C:
				if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
					return
				}
				flusher.Flush()
			case <-ctx.Done():
				return
			}
		} else {
			// Fallback: short poll.
			fallback := time.NewTimer(sseFallbackPoll)
			select {
			case <-fallback.C:
				continue
			case <-keepaliveTicker.C:
				if !fallback.Stop() {
					<-fallback.C
				}
				if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
					return
				}
				flusher.Flush()
			case <-ctx.Done():
				if !fallback.Stop() {
					<-fallback.C
				}
				return
			}
		}
	}
}

func (s *Server) observeSSEConnect(q Queue) {
	if s.ObserveSSEConnect != nil {
		s.ObserveSSEConnect(q)
	}
}

func (s *Server) observeSSEDisconnect(q Queue, statusCode int, messagesSent int, duration time.Duration) {
	if s.ObserveSSEDisconnect != nil {
		s.ObserveSSEDisconnect(q, statusCode, messagesSent, duration)
	}
}
