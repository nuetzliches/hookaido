package admin

import (
	"encoding/json"
	"net/http"
	"time"
)

// PullConsumer is one pull consumer with an open SSE stream, as reported by
// GET /pull/consumers.
//
// TokenRef is the configured secret reference the consumer authenticated with
// (`env.PULL_TOKEN`), never the token value.
type PullConsumer struct {
	ID            string    `json:"id"`
	Route         string    `json:"route"`
	ConsumerGroup string    `json:"consumer_group,omitempty"`
	Endpoint      string    `json:"endpoint"`
	RemoteAddr    string    `json:"remote_addr"`
	UserAgent     string    `json:"user_agent,omitempty"`
	TokenRef      string    `json:"token_ref,omitempty"`
	ConnectedAt   time.Time `json:"connected_at"`
	MessagesSent  int64     `json:"messages_sent"`
	LastMessageAt time.Time `json:"last_message_at,omitzero"`
}

type pullConsumerView struct {
	PullConsumer
	ConnectedForSeconds float64 `json:"connected_for_seconds"`
}

type pullConsumersResponse struct {
	Consumers []pullConsumerView `json:"consumers"`
	Count     int                `json:"count"`
}

// handlePullConsumers answers which pull consumers are attached right now.
//
// `hookaido_pull_sse_connection_active` already says how many, and that is the
// decisive signal — but an unexpected second consumer on a competing-consumer
// queue looks, from inside either consumer, exactly like delivery loss: the
// ingress answers 202 for every event and each side sees a fluctuating
// fraction arrive. Without this the only way to name the second consumer was to
// correlate raw container logs by remote address on the host, which needs shell
// access and only works while it is still connected.
//
// Only SSE streams are listed. A consumer polling `POST .../dequeue` holds no
// connection between calls, so there is nothing to report — and nothing the
// gauge counts either, which keeps the two surfaces consistent.
func (s *Server) handlePullConsumers(w http.ResponseWriter, r *http.Request) {
	if s.PullConsumers == nil {
		writeManagementError(w, http.StatusServiceUnavailable, readCodePullUnavailable, "pull consumer registry is unavailable")
		return
	}

	routeFilter, ok := parseOptionalRoutePath(r.URL.Query().Get("route"))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}

	now := time.Now()
	consumers := s.PullConsumers()
	out := pullConsumersResponse{Consumers: make([]pullConsumerView, 0, len(consumers))}
	for _, c := range consumers {
		if routeFilter != "" && c.Route != routeFilter {
			continue
		}
		out.Consumers = append(out.Consumers, pullConsumerView{
			PullConsumer:        c,
			ConnectedForSeconds: now.Sub(c.ConnectedAt).Seconds(),
		})
	}
	out.Count = len(out.Consumers)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
