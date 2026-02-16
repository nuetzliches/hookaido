package queue

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

type storeFactory struct {
	name string
	new  func(t *testing.T, now *time.Time) Store
}

func contractStoreFactories() []storeFactory {
	out := []storeFactory{
		{
			name: "memory",
			new: func(t *testing.T, now *time.Time) Store {
				t.Helper()
				return NewMemoryStore(
					WithNowFunc(func() time.Time { return now.UTC() }),
				)
			},
		},
		{
			name: "sqlite",
			new: func(t *testing.T, now *time.Time) Store {
				t.Helper()
				dbPath := filepath.Join(t.TempDir(), "hookaido.db")
				s, err := NewSQLiteStore(
					dbPath,
					WithSQLiteNowFunc(func() time.Time { return now.UTC() }),
					WithSQLitePollInterval(5*time.Millisecond),
					WithSQLiteCheckpointInterval(0),
				)
				if err != nil {
					t.Fatalf("new sqlite store: %v", err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		},
	}

	dsn := strings.TrimSpace(os.Getenv("HOOKAIDO_TEST_POSTGRES_DSN"))
	if dsn != "" {
		out = append(out, storeFactory{
			name: "postgres",
			new: func(t *testing.T, now *time.Time) Store {
				t.Helper()
				s, err := NewPostgresStore(
					dsn,
					WithPostgresNowFunc(func() time.Time { return now.UTC() }),
					WithPostgresPollInterval(5*time.Millisecond),
				)
				if err != nil {
					t.Fatalf("new postgres store: %v", err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		})
	}

	return out
}

func TestStoreContract_DequeueAck(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 2, 14, 21, 0, 0, 0, time.UTC)
			store := factory.new(t, &now)

			for _, id := range []string{"evt_1", "evt_2"} {
				if err := store.Enqueue(Envelope{ID: id, Route: "/r", Target: "pull"}); err != nil {
					t.Fatalf("enqueue %s: %v", id, err)
				}
			}

			got := make([]string, 0, 2)
			for i := 0; i < 2; i++ {
				resp, err := store.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 30 * time.Second})
				if err != nil {
					t.Fatalf("dequeue %d: %v", i+1, err)
				}
				if len(resp.Items) != 1 {
					t.Fatalf("dequeue %d items=%d, want 1", i+1, len(resp.Items))
				}
				item := resp.Items[0]
				got = append(got, item.ID)
				if err := store.Ack(item.LeaseID); err != nil {
					t.Fatalf("ack %d: %v", i+1, err)
				}
			}

			sort.Strings(got)
			if got[0] != "evt_1" || got[1] != "evt_2" {
				t.Fatalf("acked ids=%v, want [evt_1 evt_2]", got)
			}
		})
	}
}

func TestStoreContract_NackDelayRequeue(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 2, 14, 21, 5, 0, 0, time.UTC)
			store := factory.new(t, &now)

			if err := store.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			resp, err := store.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
			if err != nil {
				t.Fatalf("dequeue: %v", err)
			}
			if len(resp.Items) != 1 {
				t.Fatalf("dequeue items=%d, want 1", len(resp.Items))
			}
			if err := store.Nack(resp.Items[0].LeaseID, 2*time.Second); err != nil {
				t.Fatalf("nack: %v", err)
			}

			resp, err = store.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
			if err != nil {
				t.Fatalf("dequeue before delay: %v", err)
			}
			if len(resp.Items) != 0 {
				t.Fatalf("dequeue before delay items=%d, want 0", len(resp.Items))
			}

			now = now.Add(3 * time.Second)
			resp, err = store.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
			if err != nil {
				t.Fatalf("dequeue after delay: %v", err)
			}
			if len(resp.Items) != 1 {
				t.Fatalf("dequeue after delay items=%d, want 1", len(resp.Items))
			}
			if resp.Items[0].ID != "evt_1" {
				t.Fatalf("dequeue after delay id=%q, want evt_1", resp.Items[0].ID)
			}
			if err := store.Ack(resp.Items[0].LeaseID); err != nil {
				t.Fatalf("ack: %v", err)
			}
		})
	}
}

func TestStoreContract_BacklogTrendCaptureList(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 2, 16, 10, 0, 0, 0, time.UTC)
			store := factory.new(t, &now)

			trendStore, ok := store.(BacklogTrendStore)
			if !ok {
				t.Skip("store does not implement BacklogTrendStore")
			}

			seed := []Envelope{
				{ID: "evt_1", Route: "/r1", Target: "pull"},
				{ID: "evt_2", Route: "/r1", Target: "pull"},
				{ID: "evt_3", Route: "/r2", Target: "deliver"},
			}
			for _, env := range seed {
				if err := store.Enqueue(env); err != nil {
					t.Fatalf("enqueue %q: %v", env.ID, err)
				}
			}

			dequeued, err := store.Dequeue(DequeueRequest{
				Route:    "/r1",
				Target:   "pull",
				Batch:    1,
				LeaseTTL: 30 * time.Second,
			})
			if err != nil {
				t.Fatalf("dequeue: %v", err)
			}
			if len(dequeued.Items) != 1 {
				t.Fatalf("dequeue items=%d, want 1", len(dequeued.Items))
			}
			if err := store.MarkDead(dequeued.Items[0].LeaseID, "test_dead"); err != nil {
				t.Fatalf("mark dead: %v", err)
			}

			if err := trendStore.CaptureBacklogTrendSample(now); err != nil {
				t.Fatalf("capture backlog trend sample: %v", err)
			}

			global, err := trendStore.ListBacklogTrend(BacklogTrendListRequest{
				Since: now.Add(-time.Minute),
				Until: now.Add(time.Minute),
				Limit: 10,
			})
			if err != nil {
				t.Fatalf("list global backlog trend: %v", err)
			}
			if global.Truncated {
				t.Fatalf("expected global trend not truncated")
			}
			if len(global.Items) != 1 {
				t.Fatalf("expected 1 global trend sample, got %d", len(global.Items))
			}
			if got := global.Items[0].Queued; got != 2 {
				t.Fatalf("global queued=%d, want 2", got)
			}
			if got := global.Items[0].Leased; got != 0 {
				t.Fatalf("global leased=%d, want 0", got)
			}
			if got := global.Items[0].Dead; got != 1 {
				t.Fatalf("global dead=%d, want 1", got)
			}

			routeOnly, err := trendStore.ListBacklogTrend(BacklogTrendListRequest{
				Route: "/r1",
				Since: now.Add(-time.Minute),
				Until: now.Add(time.Minute),
				Limit: 10,
			})
			if err != nil {
				t.Fatalf("list route backlog trend: %v", err)
			}
			if len(routeOnly.Items) != 1 {
				t.Fatalf("expected 1 route trend sample, got %d", len(routeOnly.Items))
			}
			if got := routeOnly.Items[0].Queued; got != 1 {
				t.Fatalf("route queued=%d, want 1", got)
			}
			if got := routeOnly.Items[0].Leased; got != 0 {
				t.Fatalf("route leased=%d, want 0", got)
			}
			if got := routeOnly.Items[0].Dead; got != 1 {
				t.Fatalf("route dead=%d, want 1", got)
			}
		})
	}
}

func TestStoreContract_ExtendLease(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 2, 14, 21, 10, 0, 0, time.UTC)
			store := factory.new(t, &now)

			if err := store.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			resp, err := store.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 2 * time.Second})
			if err != nil {
				t.Fatalf("dequeue: %v", err)
			}
			if len(resp.Items) != 1 {
				t.Fatalf("dequeue items=%d, want 1", len(resp.Items))
			}
			leaseID := resp.Items[0].LeaseID

			now = now.Add(1 * time.Second)
			if err := store.Extend(leaseID, 5*time.Second); err != nil {
				t.Fatalf("extend: %v", err)
			}

			// Ensure the lease is still valid after the original ttl.
			now = now.Add(2 * time.Second)
			if err := store.Ack(leaseID); err != nil {
				t.Fatalf("ack after extend: %v", err)
			}
		})
	}
}

func TestStoreContract_MarkDeadAndRequeue(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 2, 14, 21, 15, 0, 0, time.UTC)
			store := factory.new(t, &now)

			if err := store.Enqueue(Envelope{ID: "evt_dead", Route: "/r", Target: "pull"}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			resp, err := store.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
			if err != nil {
				t.Fatalf("dequeue: %v", err)
			}
			if len(resp.Items) != 1 {
				t.Fatalf("dequeue items=%d, want 1", len(resp.Items))
			}
			if err := store.MarkDead(resp.Items[0].LeaseID, "test_failure"); err != nil {
				t.Fatalf("mark dead: %v", err)
			}

			dead, err := store.ListDead(DeadListRequest{Route: "/r", Limit: 10})
			if err != nil {
				t.Fatalf("list dead: %v", err)
			}
			if len(dead.Items) != 1 {
				t.Fatalf("dead items=%d, want 1", len(dead.Items))
			}
			if dead.Items[0].ID != "evt_dead" {
				t.Fatalf("dead id=%q, want evt_dead", dead.Items[0].ID)
			}

			requeueResp, err := store.RequeueDead(DeadRequeueRequest{IDs: []string{"evt_dead"}})
			if err != nil {
				t.Fatalf("requeue dead: %v", err)
			}
			if requeueResp.Requeued != 1 {
				t.Fatalf("requeued=%d, want 1", requeueResp.Requeued)
			}

			resp, err = store.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
			if err != nil {
				t.Fatalf("dequeue requeued: %v", err)
			}
			if len(resp.Items) != 1 {
				t.Fatalf("requeued dequeue items=%d, want 1", len(resp.Items))
			}
			if resp.Items[0].ID != "evt_dead" {
				t.Fatalf("requeued dequeue id=%q, want evt_dead", resp.Items[0].ID)
			}
		})
	}
}
