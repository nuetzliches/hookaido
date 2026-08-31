package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/queue"
)

func decodePullConsumers(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal response: %v (%s)", err, body)
	}
	return out
}

func TestServer_PullConsumers(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	connectedAt := time.Now().Add(-90 * time.Second)
	srv.PullConsumers = func() []PullConsumer {
		return []PullConsumer{
			{
				ID:            "con_a",
				Route:         "/webhooks/appliance",
				Endpoint:      "/appliance",
				RemoteAddr:    "10.0.0.5:41234",
				UserAgent:     "hookaido-worker/1.0",
				TokenRef:      "env.PULL_TOKEN",
				ConnectedAt:   connectedAt,
				MessagesSent:  81,
				LastMessageAt: connectedAt.Add(time.Minute),
			},
			{
				ID:          "con_b",
				Route:       "/webhooks/other",
				Endpoint:    "/other",
				RemoteAddr:  "10.0.0.9:52001",
				ConnectedAt: connectedAt,
			},
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/pull/consumers", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", rr.Code, rr.Body.String())
	}
	out := decodePullConsumers(t, rr.Body.Bytes())
	if count, _ := out["count"].(float64); count != 2 {
		t.Fatalf("expected count 2, got %v", out["count"])
	}
	consumers, ok := out["consumers"].([]any)
	if !ok || len(consumers) != 2 {
		t.Fatalf("expected two consumers, got %#v", out["consumers"])
	}

	first, _ := consumers[0].(map[string]any)
	if first["id"] != "con_a" {
		t.Fatalf("expected con_a first, got %v", first["id"])
	}
	if first["token_ref"] != "env.PULL_TOKEN" {
		t.Fatalf("expected the token reference, got %v", first["token_ref"])
	}
	if sent, _ := first["messages_sent"].(float64); sent != 81 {
		t.Fatalf("expected messages_sent 81, got %v", first["messages_sent"])
	}
	if secs, _ := first["connected_for_seconds"].(float64); secs < 60 {
		t.Fatalf("expected connected_for_seconds to reflect a 90s connection, got %v", secs)
	}

	// A consumer that has received nothing yet must not report a zero-value
	// timestamp as if it had.
	second, _ := consumers[1].(map[string]any)
	if _, present := second["last_message_at"]; present {
		t.Fatalf("expected last_message_at to be omitted when nothing was sent, got %v", second["last_message_at"])
	}
	if _, present := second["token_ref"]; present {
		t.Fatalf("expected token_ref to be omitted without a matched token, got %v", second["token_ref"])
	}
}

func TestServer_PullConsumersRouteFilter(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.PullConsumers = func() []PullConsumer {
		return []PullConsumer{
			{ID: "con_a", Route: "/webhooks/appliance", ConnectedAt: time.Now()},
			{ID: "con_b", Route: "/webhooks/other", ConnectedAt: time.Now()},
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/pull/consumers?route=/webhooks/appliance", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", rr.Code, rr.Body.String())
	}
	out := decodePullConsumers(t, rr.Body.Bytes())
	consumers, _ := out["consumers"].([]any)
	if len(consumers) != 1 {
		t.Fatalf("expected one consumer after filtering, got %#v", consumers)
	}
	first, _ := consumers[0].(map[string]any)
	if first["id"] != "con_a" {
		t.Fatalf("expected con_a, got %v", first["id"])
	}
}

func TestServer_PullConsumersRejectsRelativeRouteFilter(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.PullConsumers = func() []PullConsumer { return nil }

	req := httptest.NewRequest(http.MethodGet, "http://example/pull/consumers?route=webhooks", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (%s)", rr.Code, rr.Body.String())
	}
}

func TestServer_PullConsumersEmptyList(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.PullConsumers = func() []PullConsumer { return nil }

	req := httptest.NewRequest(http.MethodGet, "http://example/pull/consumers", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", rr.Code, rr.Body.String())
	}
	// An empty registry must serialize as [], not null: a client iterating the
	// list should not have to special-case "nobody is attached".
	if body := rr.Body.String(); !jsonHasEmptyConsumers(body) {
		t.Fatalf("expected an empty consumers array, got %s", body)
	}
}

func jsonHasEmptyConsumers(body string) bool {
	var out struct {
		Consumers []PullConsumer `json:"consumers"`
		Count     int            `json:"count"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		return false
	}
	return out.Consumers != nil && len(out.Consumers) == 0 && out.Count == 0
}

func TestServer_PullConsumersUnavailable(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/pull/consumers", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d (%s)", rr.Code, rr.Body.String())
	}
	out := decodePullConsumers(t, rr.Body.Bytes())
	if out["code"] != "pull_consumers_unavailable" {
		t.Fatalf("expected pull_consumers_unavailable, got %v", out["code"])
	}
}

func TestServer_PullConsumersMethodNotAllowed(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.PullConsumers = func() []PullConsumer { return nil }

	req := httptest.NewRequest(http.MethodPost, "http://example/pull/consumers", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d (%s)", rr.Code, rr.Body.String())
	}
}

func TestServer_PullConsumersRequiresAuth(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.Authorize = BearerTokenAuthorizer([][]byte{[]byte("admin-token")})
	srv.PullConsumers = func() []PullConsumer { return nil }

	req := httptest.NewRequest(http.MethodGet, "http://example/pull/consumers", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d (%s)", rr.Code, rr.Body.String())
	}
}
