package queue_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/queue"
	"github.com/nuetzliches/hookaido/v2/modules/postgres"
	"github.com/nuetzliches/hookaido/v2/modules/sqlite"
)

type storeFactory struct {
	name string
	new  func(t *testing.T, now *time.Time) queue.Store
}

func contractStoreFactories() []storeFactory {
	out := []storeFactory{
		{
			name: "memory",
			new: func(t *testing.T, now *time.Time) queue.Store {
				t.Helper()
				return queue.NewMemoryStore(
					queue.WithNowFunc(func() time.Time { return now.UTC() }),
				)
			},
		},
		{
			name: "sqlite",
			new: func(t *testing.T, now *time.Time) queue.Store {
				t.Helper()
				dbPath := filepath.Join(t.TempDir(), "hookaido.db")
				s, err := sqlite.NewStore(
					dbPath,
					sqlite.WithNowFunc(func() time.Time { return now.UTC() }),
					sqlite.WithPollInterval(5*time.Millisecond),
					sqlite.WithCheckpointInterval(0),
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
			new: func(t *testing.T, now *time.Time) queue.Store {
				t.Helper()
				s, err := postgres.NewStore(
					dsn,
					postgres.WithNowFunc(func() time.Time { return now.UTC() }),
					postgres.WithPollInterval(5*time.Millisecond),
				)
				if err != nil {
					t.Fatalf("new postgres store: %v", err)
				}
				postgres.TruncateForTest(t, s)
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		})
	}

	return out
}

// contractRetentionStoreFactories mirrors contractStoreFactories but configures
// delivered-item retention, so retention behaviour can be exercised across every
// backend. Queue retention stays at zero -- only the prune interval is set --
// so pruning runs on every pass without the queued probe items being collected.
func contractRetentionStoreFactories(deliveredMaxAge time.Duration) []storeFactory {
	out := []storeFactory{
		{
			name: "memory",
			new: func(t *testing.T, now *time.Time) queue.Store {
				t.Helper()
				return queue.NewMemoryStore(
					queue.WithNowFunc(func() time.Time { return now.UTC() }),
					queue.WithQueueRetention(0, time.Millisecond),
					queue.WithDeliveredRetention(deliveredMaxAge),
				)
			},
		},
		{
			name: "sqlite",
			new: func(t *testing.T, now *time.Time) queue.Store {
				t.Helper()
				dbPath := filepath.Join(t.TempDir(), "hookaido.db")
				s, err := sqlite.NewStore(
					dbPath,
					sqlite.WithNowFunc(func() time.Time { return now.UTC() }),
					sqlite.WithPollInterval(5*time.Millisecond),
					sqlite.WithCheckpointInterval(0),
					sqlite.WithRetention(0, time.Millisecond),
					sqlite.WithDeliveredRetention(deliveredMaxAge),
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
			new: func(t *testing.T, now *time.Time) queue.Store {
				t.Helper()
				s, err := postgres.NewStore(
					dsn,
					postgres.WithNowFunc(func() time.Time { return now.UTC() }),
					postgres.WithPollInterval(5*time.Millisecond),
					postgres.WithRetention(0, time.Millisecond),
					postgres.WithDeliveredRetention(deliveredMaxAge),
				)
				if err != nil {
					t.Fatalf("new postgres store: %v", err)
				}
				postgres.TruncateForTest(t, s)
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		})
	}

	return out
}

// deliveredCount reports how many items sit in the delivered state.
func deliveredCount(t *testing.T, store queue.Store) int {
	t.Helper()
	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	return stats.ByState[queue.StateDelivered]
}

// triggerPrune forces a retention pass. Every backend prunes from Enqueue, so a
// throwaway queued item is the portable way to reach it.
func triggerPrune(t *testing.T, store queue.Store, probeID string) {
	t.Helper()
	if err := store.Enqueue(queue.Envelope{ID: probeID, Route: "/probe", Target: "pull"}); err != nil {
		t.Fatalf("enqueue prune probe %s: %v", probeID, err)
	}
}

// Delivered retention must be measured from when an item was delivered, not from
// when it arrived. Ack stamps NextRunAt with the delivery time on all three
// backends, so that -- not ReceivedAt -- is the field retention has to key on.
//
// Postgres pruned on received_at, so an item that had sat queued for longer than
// delivered_retention_max_age was deleted on the first prune after delivery: a
// slow route lost its delivery history immediately while a fast route kept the
// configured window.
func TestStoreContract_DeliveredRetentionAgesFromDeliveryTime(t *testing.T) {
	const deliveredWindow = time.Hour

	for _, factory := range contractRetentionStoreFactories(deliveredWindow) {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
			store := factory.new(t, &now)

			if err := store.Enqueue(queue.Envelope{ID: "evt_slow", Route: "/r", Target: "pull"}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			// Let it sit queued for far longer than the delivered window, so
			// ReceivedAt and NextRunAt disagree by more than the retention age.
			now = now.Add(3 * deliveredWindow)

			resp, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: time.Minute})
			if err != nil {
				t.Fatalf("dequeue: %v", err)
			}
			if len(resp.Items) != 1 {
				t.Fatalf("dequeue items=%d, want 1", len(resp.Items))
			}
			if err := store.Ack(resp.Items[0].LeaseID); err != nil {
				t.Fatalf("ack: %v", err)
			}
			if got := deliveredCount(t, store); got != 1 {
				t.Fatalf("delivered=%d immediately after ack, want 1", got)
			}

			// One minute after delivery the window has not elapsed.
			now = now.Add(time.Minute)
			triggerPrune(t, store, "evt_probe_1")
			if got := deliveredCount(t, store); got != 1 {
				t.Fatalf("delivered=%d one minute after delivery, want 1 -- retention is keyed on arrival time, not delivery time", got)
			}

			// Well past the window measured from delivery, it must be collected.
			now = now.Add(2 * deliveredWindow)
			triggerPrune(t, store, "evt_probe_2")
			if got := deliveredCount(t, store); got != 0 {
				t.Fatalf("delivered=%d past the retention window, want 0", got)
			}
		})
	}
}

// Stats must report per-bucket backlog age and ready lag, not just a count.
// These are what an operator reads to spot a stalled target; Postgres left both
// at zero, so a route stuck for hours reported an age of 0s.
func TestStoreContract_StatsTopBacklogReportsAgeAndLag(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
			store := factory.new(t, &now)

			if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			const stalled = 90 * time.Minute
			now = now.Add(stalled)

			stats, err := store.Stats()
			if err != nil {
				t.Fatalf("stats: %v", err)
			}
			if len(stats.TopQueued) != 1 {
				t.Fatalf("TopQueued buckets=%d, want 1", len(stats.TopQueued))
			}

			b := stats.TopQueued[0]
			if b.Route != "/r" || b.Target != "pull" {
				t.Fatalf("bucket route/target=%q/%q, want /r/pull", b.Route, b.Target)
			}
			if b.Queued != 1 {
				t.Fatalf("bucket queued=%d, want 1", b.Queued)
			}
			if b.OldestQueuedReceivedAt.IsZero() {
				t.Fatal("bucket OldestQueuedReceivedAt is zero, want the enqueue time")
			}
			if b.OldestQueuedAge != stalled {
				t.Fatalf("bucket OldestQueuedAge=%s, want %s", b.OldestQueuedAge, stalled)
			}
			if b.EarliestQueuedNextRun.IsZero() {
				t.Fatal("bucket EarliestQueuedNextRun is zero, want the next-run time")
			}
			if b.ReadyLag != stalled {
				t.Fatalf("bucket ReadyLag=%s, want %s", b.ReadyLag, stalled)
			}
		})
	}
}

func TestStoreContract_DequeueAck(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 2, 14, 21, 0, 0, 0, time.UTC)
			store := factory.new(t, &now)

			for _, id := range []string{"evt_1", "evt_2"} {
				if err := store.Enqueue(queue.Envelope{ID: id, Route: "/r", Target: "pull"}); err != nil {
					t.Fatalf("enqueue %s: %v", id, err)
				}
			}

			got := make([]string, 0, 2)
			for i := 0; i < 2; i++ {
				resp, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 30 * time.Second})
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

// An empty Route or Target in a DequeueRequest means "any". The push
// dispatcher relies on this: it dequeues per route without naming a target
// and resolves the target per item afterwards. Every other contract test
// names both fields explicitly, so this is the only coverage of the wildcard.
func TestStoreContract_DequeueEmptyRouteOrTargetIsWildcard(t *testing.T) {
	cases := []struct {
		name string
		req  queue.DequeueRequest
	}{
		{name: "target_wildcard", req: queue.DequeueRequest{Route: "/r"}},
		{name: "route_wildcard", req: queue.DequeueRequest{Target: "https://example.com/hook"}},
		{name: "both_wildcard", req: queue.DequeueRequest{}},
	}

	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					now := time.Date(2026, 2, 14, 21, 0, 0, 0, time.UTC)
					store := factory.new(t, &now)

					if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "https://example.com/hook"}); err != nil {
						t.Fatalf("enqueue: %v", err)
					}

					req := tc.req
					req.Batch = 1
					req.LeaseTTL = 30 * time.Second
					resp, err := store.Dequeue(req)
					if err != nil {
						t.Fatalf("dequeue: %v", err)
					}
					if len(resp.Items) != 1 {
						t.Fatalf("dequeue with route=%q target=%q returned %d items, want 1",
							tc.req.Route, tc.req.Target, len(resp.Items))
					}
					if got := resp.Items[0].ID; got != "evt_1" {
						t.Fatalf("dequeued id=%q, want evt_1", got)
					}
					if got := resp.Items[0].Target; got != "https://example.com/hook" {
						t.Fatalf("dequeued target=%q, want https://example.com/hook", got)
					}
				})
			}
		})
	}
}

func TestStoreContract_NackDelayRequeue(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 2, 14, 21, 5, 0, 0, time.UTC)
			store := factory.new(t, &now)

			if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			resp, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
			if err != nil {
				t.Fatalf("dequeue: %v", err)
			}
			if len(resp.Items) != 1 {
				t.Fatalf("dequeue items=%d, want 1", len(resp.Items))
			}
			if err := store.Nack(resp.Items[0].LeaseID, 2*time.Second); err != nil {
				t.Fatalf("nack: %v", err)
			}

			resp, err = store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
			if err != nil {
				t.Fatalf("dequeue before delay: %v", err)
			}
			if len(resp.Items) != 0 {
				t.Fatalf("dequeue before delay items=%d, want 0", len(resp.Items))
			}

			now = now.Add(3 * time.Second)
			resp, err = store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
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

			trendStore, ok := store.(queue.BacklogTrendStore)
			if !ok {
				t.Skip("store does not implement BacklogTrendStore")
			}

			seed := []queue.Envelope{
				{ID: "evt_1", Route: "/r1", Target: "pull"},
				{ID: "evt_2", Route: "/r1", Target: "pull"},
				{ID: "evt_3", Route: "/r2", Target: "deliver"},
			}
			for _, env := range seed {
				if err := store.Enqueue(env); err != nil {
					t.Fatalf("enqueue %q: %v", env.ID, err)
				}
			}

			dequeued, err := store.Dequeue(queue.DequeueRequest{
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

			global, err := trendStore.ListBacklogTrend(queue.BacklogTrendListRequest{
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

			routeOnly, err := trendStore.ListBacklogTrend(queue.BacklogTrendListRequest{
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

			if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			resp, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 2 * time.Second})
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

			if err := store.Enqueue(queue.Envelope{ID: "evt_dead", Route: "/r", Target: "pull"}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			resp, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
			if err != nil {
				t.Fatalf("dequeue: %v", err)
			}
			if len(resp.Items) != 1 {
				t.Fatalf("dequeue items=%d, want 1", len(resp.Items))
			}
			if err := store.MarkDead(resp.Items[0].LeaseID, "test_failure"); err != nil {
				t.Fatalf("mark dead: %v", err)
			}

			dead, err := store.ListDead(queue.DeadListRequest{Route: "/r", Limit: 10})
			if err != nil {
				t.Fatalf("list dead: %v", err)
			}
			if len(dead.Items) != 1 {
				t.Fatalf("dead items=%d, want 1", len(dead.Items))
			}
			if dead.Items[0].ID != "evt_dead" {
				t.Fatalf("dead id=%q, want evt_dead", dead.Items[0].ID)
			}

			requeueResp, err := store.RequeueDead(queue.DeadRequeueRequest{IDs: []string{"evt_dead"}})
			if err != nil {
				t.Fatalf("requeue dead: %v", err)
			}
			if requeueResp.Requeued != 1 {
				t.Fatalf("requeued=%d, want 1", requeueResp.Requeued)
			}

			resp, err = store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
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
