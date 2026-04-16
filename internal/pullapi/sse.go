package pullapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

const (
	defaultSSEKeepalive = 15 * time.Second
	sseFallbackPoll     = time.Second
)

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request, route string) {
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

	s.observeSSEConnect(route)
	start := time.Now()
	messagesSent := 0
	keepaliveTicker := time.NewTicker(keepalive)
	defer keepaliveTicker.Stop()

	for {
		// Non-blocking dequeue.
		outcome, opErr := s.Dequeue(route, DequeueParams{
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
			s.observeSSEDisconnect(route, opErr.StatusCode, messagesSent, time.Since(start))
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
				fmt.Fprintf(w, "id: %s\nevent: message\ndata: %s\n\n", it.LeaseID, data)
				messagesSent++
			}
			flusher.Flush()
			// Reset keepalive timer after activity.
			keepaliveTicker.Reset(keepalive)
			continue
		}

		// No items available — wait for notification, keepalive, or cancellation.
		if notifyCh != nil {
			ch := notifyCh()
			select {
			case <-ch:
				continue
			case <-keepaliveTicker.C:
				if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
					s.observeSSEDisconnect(route, http.StatusOK, messagesSent, time.Since(start))
					return
				}
				flusher.Flush()
			case <-ctx.Done():
				s.observeSSEDisconnect(route, http.StatusOK, messagesSent, time.Since(start))
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
					s.observeSSEDisconnect(route, http.StatusOK, messagesSent, time.Since(start))
					return
				}
				flusher.Flush()
			case <-ctx.Done():
				if !fallback.Stop() {
					<-fallback.C
				}
				s.observeSSEDisconnect(route, http.StatusOK, messagesSent, time.Since(start))
				return
			}
		}
	}
}

func (s *Server) observeSSEConnect(route string) {
	if s.ObserveSSEConnect != nil {
		s.ObserveSSEConnect(route)
	}
}

func (s *Server) observeSSEDisconnect(route string, statusCode int, messagesSent int, duration time.Duration) {
	if s.ObserveSSEDisconnect != nil {
		s.ObserveSSEDisconnect(route, statusCode, messagesSent, duration)
	}
}
