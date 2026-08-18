package pullapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/queue"
)

// scopeTestServer wires a server for two routes backed by one store, with
// route-scoped pull credentials in use.
func scopeTestServer(t *testing.T, store queue.Store) *Server {
	t.Helper()
	srv := NewServer(store)
	srv.Target = "pull"
	srv.LeaseRouteScoped = func() bool { return true }
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		switch endpoint {
		case "/pull/a":
			return "/a", true
		case "/pull/b":
			return "/b", true
		}
		return "", false
	}
	return srv
}

func leaseFor(t *testing.T, store queue.Store, route string) queue.Envelope {
	t.Helper()
	if err := store.Enqueue(queue.Envelope{Route: route, Target: "pull", Payload: []byte("{}")}); err != nil {
		t.Fatalf("enqueue %s: %v", route, err)
	}
	resp, err := store.Dequeue(queue.DequeueRequest{Route: route, Target: "pull", Batch: 1, LeaseTTL: time.Minute})
	if err != nil {
		t.Fatalf("dequeue %s: %v", route, err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("dequeue %s items=%d, want 1", route, len(resp.Items))
	}
	return resp.Items[0]
}

// A client authorized only for route /a must not be able to settle route /b's
// in-flight message by presenting its lease ID. The handlers passed only the
// lease ID to the store; the resolved route was used for metrics and nothing
// else, so per-route pull tokens bought nothing at the operation layer.
func TestLeaseScope_ForeignLeaseRejected(t *testing.T) {
	ops := []struct {
		name string
		run  func(srv *Server, route string, lease queue.Envelope) *OpError
	}{
		{
			name: "ack",
			run: func(srv *Server, route string, lease queue.Envelope) *OpError {
				return srv.AckSingle(route, lease.LeaseID)
			},
		},
		{
			name: "nack",
			run: func(srv *Server, route string, lease queue.Envelope) *OpError {
				return srv.NackSingle(route, lease.LeaseID, false, "", 0)
			},
		},
		{
			name: "mark_dead",
			run: func(srv *Server, route string, lease queue.Envelope) *OpError {
				return srv.NackSingle(route, lease.LeaseID, true, "manual", 0)
			},
		},
		{
			name: "extend",
			run: func(srv *Server, route string, lease queue.Envelope) *OpError {
				return srv.Extend(route, lease.LeaseID, time.Minute)
			},
		},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			srv := scopeTestServer(t, store)
			lease := leaseFor(t, store, "/b")

			// The attacker calls route /a's endpoint with route /b's lease.
			opErr := op.run(srv, "/a", lease)
			if opErr == nil {
				t.Fatalf("%s on a foreign lease was accepted", op.name)
			}
			if opErr.StatusCode != 409 || opErr.Code != pullErrLeaseConflict {
				t.Fatalf("status=%d code=%q, want 409/%s", opErr.StatusCode, opErr.Code, pullErrLeaseConflict)
			}

			// The message is untouched: still leased to its own route.
			stats, err := store.Stats()
			if err != nil {
				t.Fatalf("stats: %v", err)
			}
			if stats.ByState[queue.StateLeased] != 1 {
				t.Fatalf("message state changed: %v", stats.ByState)
			}

			// And the legitimate owner can still settle it.
			if opErr := op.run(srv, "/b", lease); opErr != nil {
				t.Fatalf("%s by the owning route failed: %v", op.name, opErr)
			}
		})
	}
}

func TestLeaseScope_BatchSplitsForeignLeases(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := scopeTestServer(t, store)

	mine := leaseFor(t, store, "/a")
	theirs := leaseFor(t, store, "/b")

	res, opErr := srv.AckBatch("/a", []string{mine.LeaseID, theirs.LeaseID})
	if opErr != nil {
		t.Fatalf("ack batch: %v", opErr)
	}
	if res.Succeeded != 1 {
		t.Fatalf("succeeded=%d, want 1 (only the in-route lease)", res.Succeeded)
	}
	if len(res.Conflicts) != 1 || res.Conflicts[0].LeaseID != theirs.LeaseID {
		t.Fatalf("conflicts=%+v, want the foreign lease", res.Conflicts)
	}

	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ByState[queue.StateLeased] != 1 {
		t.Fatalf("the foreign lease was settled: %v", stats.ByState)
	}
}

// Without route-scoped credentials every client is authorized for every route,
// so the lookup is skipped entirely -- including its cost on the ack hot path.
func TestLeaseScope_SkippedWithoutRouteScopedCredentials(t *testing.T) {
	store := &countingLeaseRouteStore{MemoryStore: queue.NewMemoryStore()}
	srv := scopeTestServer(t, store)
	srv.LeaseRouteScoped = func() bool { return false }

	lease := leaseFor(t, store, "/b")
	if opErr := srv.AckSingle("/a", lease.LeaseID); opErr != nil {
		t.Fatalf("ack across routes without scoped credentials: %v", opErr)
	}
	if store.calls != 0 {
		t.Fatalf("LeaseRoutes was called %d times, want 0", store.calls)
	}
}

// A resolver failure must deny rather than wave the operation through.
func TestLeaseScope_ResolverFailureDenies(t *testing.T) {
	store := &failingLeaseRouteStore{MemoryStore: queue.NewMemoryStore()}
	srv := scopeTestServer(t, store)
	lease := leaseFor(t, store, "/a")

	opErr := srv.AckSingle("/a", lease.LeaseID)
	if opErr == nil {
		t.Fatal("expected the ack to fail when the scope check cannot run")
	}
	if opErr.StatusCode != 500 {
		t.Fatalf("status=%d, want 500", opErr.StatusCode)
	}
}

// A gRPC worker that gives up must not have items leased to it afterwards: they
// would be invisible for the whole lease TTL before the expiry sweep.
func TestDequeue_CancelledContextDoesNotLease(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.Target = "pull"

	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/a", Target: "pull", Payload: []byte("{}")}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	outcome, opErr := srv.Dequeue(ctx, "/a", DequeueParams{Batch: 1, MaxWait: time.Second, HasMaxWait: true})
	if opErr != nil {
		t.Fatalf("a cancelled dequeue must not be reported as a store failure: %v", opErr)
	}
	if len(outcome.Items) != 0 {
		t.Fatalf("items=%d, want 0: the caller is gone", len(outcome.Items))
	}

	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ByState[queue.StateLeased] != 0 {
		t.Fatalf("the message was leased to a cancelled caller: %v", stats.ByState)
	}
	if stats.ByState[queue.StateQueued] != 1 {
		t.Fatalf("the message should still be queued: %v", stats.ByState)
	}
}

// The long poll must end when the caller does, rather than parking a goroutine
// for the rest of max_wait.
func TestDequeue_ContextCancelledMidWaitReturns(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.Target = "pull"

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, opErr := srv.Dequeue(ctx, "/a", DequeueParams{Batch: 1, MaxWait: 30 * time.Second, HasMaxWait: true}); opErr != nil {
			t.Errorf("dequeue: %v", opErr)
		}
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the long poll outlived its caller")
	}
}

type countingLeaseRouteStore struct {
	*queue.MemoryStore
	calls int
}

func (s *countingLeaseRouteStore) LeaseRoutes(leaseIDs []string) (map[string]string, error) {
	s.calls++
	return s.MemoryStore.LeaseRoutes(leaseIDs)
}

type failingLeaseRouteStore struct {
	*queue.MemoryStore
}

func (s *failingLeaseRouteStore) LeaseRoutes([]string) (map[string]string, error) {
	return nil, errors.New("synthetic resolver failure")
}
