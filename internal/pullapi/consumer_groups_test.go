package pullapi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/queue"
)

// newGroupTestServer serves one route through two consumer-group endpoints.
func newGroupTestServer(t *testing.T) (*Server, *queue.MemoryStore) {
	t.Helper()
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.SSEKeepalive = 100 * time.Millisecond
	srv.ResolveQueue = func(endpoint string) (Queue, bool) {
		switch endpoint {
		case "/pull/appliance/integration":
			return Queue{Route: "/webhooks/appliance", Target: "pull:integration", ConsumerGroup: "integration"}, true
		case "/pull/appliance/workstation":
			return Queue{Route: "/webhooks/appliance", Target: "pull:workstation", ConsumerGroup: "workstation"}, true
		default:
			return Queue{}, false
		}
	}
	return srv, store
}

// enqueueForGroups mimics the ingress fan-out: one inbound event becomes one
// envelope per group.
func enqueueForGroups(t *testing.T, store *queue.MemoryStore, id string, groups ...string) {
	t.Helper()
	now := time.Now()
	for _, g := range groups {
		if err := store.Enqueue(queue.Envelope{
			ID:         id + "_" + g,
			Route:      "/webhooks/appliance",
			Target:     "pull:" + g,
			ReceivedAt: now,
			NextRunAt:  now,
			Payload:    []byte(fmt.Sprintf(`{"id":%q}`, id)),
		}); err != nil {
			t.Fatalf("enqueue %s for %s: %v", id, g, err)
		}
	}
}

func dequeueOne(t *testing.T, srv *Server, endpoint string) (dequeueResponse, int) {
	t.Helper()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull"+endpoint+"/dequeue", strings.NewReader(`{"batch":10}`))
	srv.ServeHTTP(rr, req)
	var out dequeueResponse
	if rr.Code == http.StatusOK {
		if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
			t.Fatalf("unmarshal dequeue response: %v (%s)", err, rr.Body.String())
		}
	}
	return out, rr.Code
}

// The whole point: both groups receive the same event, rather than one of them
// winning it.
func TestConsumerGroups_BothGroupsReceiveEveryMessage(t *testing.T) {
	srv, store := newGroupTestServer(t)
	enqueueForGroups(t, store, "evt_1", "integration", "workstation")

	integration, code := dequeueOne(t, srv, "/appliance/integration")
	if code != http.StatusOK {
		t.Fatalf("integration dequeue: expected 200, got %d", code)
	}
	workstation, code := dequeueOne(t, srv, "/appliance/workstation")
	if code != http.StatusOK {
		t.Fatalf("workstation dequeue: expected 200, got %d", code)
	}

	if len(integration.Items) != 1 || len(workstation.Items) != 1 {
		t.Fatalf("expected each group to receive the event, got %d and %d", len(integration.Items), len(workstation.Items))
	}
	// Distinct leases: settling one must not settle the other's copy.
	if integration.Items[0].LeaseID == workstation.Items[0].LeaseID {
		t.Fatal("expected each group to hold its own lease")
	}
}

// Two consumers on the *same* group still compete, which is what makes scaling
// workers within a group work.
func TestConsumerGroups_ConsumersWithinOneGroupStillCompete(t *testing.T) {
	srv, store := newGroupTestServer(t)
	enqueueForGroups(t, store, "evt_1", "integration")

	first, code := dequeueOne(t, srv, "/appliance/integration")
	if code != http.StatusOK || len(first.Items) != 1 {
		t.Fatalf("expected the first consumer to get the event, got code=%d items=%d", code, len(first.Items))
	}
	second, code := dequeueOne(t, srv, "/appliance/integration")
	if code != http.StatusOK {
		t.Fatalf("second dequeue: expected 200, got %d", code)
	}
	if len(second.Items) != 0 {
		t.Fatalf("expected the message to be leased to exactly one consumer, got %d", len(second.Items))
	}
}

// The bare path of a grouped route is not configured, so it answers 404 rather
// than quietly serving one group.
func TestConsumerGroups_BarePathIsNotFound(t *testing.T) {
	srv, _ := newGroupTestServer(t)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/appliance/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for the ungrouped path, got %d (%s)", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "route_not_found") {
		t.Fatalf("expected route_not_found, got %s", rr.Body.String())
	}
}

// A stream registers under its own group, so an operator can tell one group's
// consumers from another's.
func TestConsumerGroups_SSEStreamsAreLabelledByGroup(t *testing.T) {
	srv, store := newGroupTestServer(t)
	enqueueForGroups(t, store, "evt_1", "integration", "workstation")

	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, group := range []string{"integration", "workstation"} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/pull/appliance/"+group+"/stream", nil)
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("stream %s: %v", group, err)
		}
		defer resp.Body.Close()
		// Read the group's copy so the stream is definitely established.
		scanner := bufio.NewScanner(resp.Body)
		if _, isComment, eof := readSSEEvent(scanner); eof || isComment {
			t.Fatalf("expected a message event for %s, got eof=%v comment=%v", group, eof, isComment)
		}
	}

	got := waitForConsumers(t, srv, 2)
	if got[0].ConsumerGroup != "integration" || got[1].ConsumerGroup != "workstation" {
		t.Fatalf("expected consumers labelled and ordered by group, got %q and %q", got[0].ConsumerGroup, got[1].ConsumerGroup)
	}
	for _, c := range got {
		if c.Route != "/webhooks/appliance" {
			t.Fatalf("expected both on the same route, got %q", c.Route)
		}
	}
}
