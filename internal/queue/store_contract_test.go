package queue_test

import (
	"fmt"
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

// Stats.Backlog is what the route-labeled queue gauges are rendered from, so
// it has to be complete: TopQueued is a top-N cut, and a low-volume route that
// falls below it is exactly the stalled route an operator is looking for.
func TestStoreContract_StatsBacklogCoversEveryRouteAndState(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 9, 1, 9, 0, 0, 0, time.UTC)
			store := factory.new(t, &now)

			// One busy route plus more quiet routes than the top-N cut admits.
			const quietRoutes = 12
			for i := 0; i < 3; i++ {
				if err := store.Enqueue(queue.Envelope{
					ID:     fmt.Sprintf("evt_busy_%d", i),
					Route:  "/busy",
					Target: "pull",
				}); err != nil {
					t.Fatalf("enqueue busy: %v", err)
				}
			}
			for i := 0; i < quietRoutes; i++ {
				if err := store.Enqueue(queue.Envelope{
					ID:     fmt.Sprintf("evt_quiet_%d", i),
					Route:  fmt.Sprintf("/quiet-%02d", i),
					Target: "pull",
				}); err != nil {
					t.Fatalf("enqueue quiet: %v", err)
				}
			}

			// A leased and a dead item on one route, so the breakdown covers
			// every state the depth gauge reports.
			if err := store.Enqueue(queue.Envelope{ID: "evt_lease", Route: "/settled", Target: "pull"}); err != nil {
				t.Fatalf("enqueue lease: %v", err)
			}
			if err := store.Enqueue(queue.Envelope{ID: "evt_dead", Route: "/settled", Target: "pull"}); err != nil {
				t.Fatalf("enqueue dead: %v", err)
			}
			resp, err := store.Dequeue(queue.DequeueRequest{Route: "/settled", Target: "pull", Batch: 2, LeaseTTL: time.Hour})
			if err != nil {
				t.Fatalf("dequeue: %v", err)
			}
			if len(resp.Items) != 2 {
				t.Fatalf("dequeue items=%d, want 2", len(resp.Items))
			}
			if err := store.MarkDead(resp.Items[1].LeaseID, "test_failure"); err != nil {
				t.Fatalf("mark dead: %v", err)
			}

			stats, err := store.Stats()
			if err != nil {
				t.Fatalf("stats: %v", err)
			}

			if len(stats.TopQueued) != 10 {
				t.Fatalf("TopQueued buckets=%d, want the top-N cut of 10", len(stats.TopQueued))
			}
			if want := quietRoutes + 2; len(stats.Backlog) != want {
				t.Fatalf("Backlog buckets=%d, want %d", len(stats.Backlog), want)
			}
			if !sort.SliceIsSorted(stats.Backlog, func(i, j int) bool {
				if stats.Backlog[i].Route != stats.Backlog[j].Route {
					return stats.Backlog[i].Route < stats.Backlog[j].Route
				}
				return stats.Backlog[i].Target < stats.Backlog[j].Target
			}) {
				t.Fatalf("Backlog is not ordered by route then target: %+v", stats.Backlog)
			}

			byRoute := make(map[string]queue.RouteBacklogBucket, len(stats.Backlog))
			for _, b := range stats.Backlog {
				if b.Target != "pull" {
					t.Fatalf("bucket target=%q, want pull", b.Target)
				}
				byRoute[b.Route] = b
			}

			if got := byRoute["/busy"]; got.Queued != 3 || got.Leased != 0 || got.Dead != 0 {
				t.Fatalf("busy bucket=%+v, want queued 3", got)
			}
			// The last quiet route sits below the top-N cut; before the
			// breakdown existed it had no per-route figures at all.
			last := fmt.Sprintf("/quiet-%02d", quietRoutes-1)
			if got := byRoute[last]; got.Queued != 1 {
				t.Fatalf("%s bucket=%+v, want queued 1", last, got)
			}
			if got := byRoute["/settled"]; got.Queued != 0 || got.Leased != 1 || got.Dead != 1 {
				t.Fatalf("settled bucket=%+v, want leased 1 and dead 1", got)
			}

			// A queued-free bucket carries no age or lag, and a stalled one
			// carries both.
			const stalled = 30 * time.Minute
			now = now.Add(stalled)
			stats, err = store.Stats()
			if err != nil {
				t.Fatalf("stats after stall: %v", err)
			}
			for _, b := range stats.Backlog {
				switch b.Route {
				case "/settled":
					if b.OldestQueuedAge != 0 || b.ReadyLag != 0 {
						t.Fatalf("settled bucket age/lag=%s/%s, want 0 with nothing queued", b.OldestQueuedAge, b.ReadyLag)
					}
				default:
					if b.OldestQueuedAge != stalled {
						t.Fatalf("%s bucket OldestQueuedAge=%s, want %s", b.Route, b.OldestQueuedAge, stalled)
					}
					if b.ReadyLag != stalled {
						t.Fatalf("%s bucket ReadyLag=%s, want %s", b.Route, b.ReadyLag, stalled)
					}
				}
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

// queue.LeaseBatchStore asks implementations to settle a whole batch in one
// store transaction and to report per-lease conflicts. There was no contract
// coverage for it at all, which is how Postgres came to loop over its
// single-lease methods — settling each lease in its own transaction — while
// memory and SQLite settled the batch as a unit. These cases pin the shared
// semantics across every backend.
func TestStoreContract_LeaseBatchSettlesValidLeasesAndReportsConflicts(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
			store := factory.new(t, &now)

			batchStore, ok := store.(queue.LeaseBatchStore)
			if !ok {
				t.Fatalf("%T does not implement queue.LeaseBatchStore", store)
			}

			for i := 0; i < 3; i++ {
				if err := store.Enqueue(queue.Envelope{ID: leaseBatchID(i), Route: "/r", Target: "pull"}); err != nil {
					t.Fatalf("enqueue %d: %v", i, err)
				}
			}
			resp, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 3, LeaseTTL: 30 * time.Second})
			if err != nil {
				t.Fatalf("dequeue: %v", err)
			}
			if len(resp.Items) != 3 {
				t.Fatalf("dequeued %d, want 3", len(resp.Items))
			}

			leaseIDs := []string{
				resp.Items[0].LeaseID,
				"",                     // blank
				resp.Items[1].LeaseID,  // valid
				"lease_does_not_exist", // unknown
				resp.Items[1].LeaseID,  // repeat of one already in this batch
				resp.Items[2].LeaseID,  // valid
			}

			res, err := batchStore.MarkDeadBatch(leaseIDs, "contract-test")
			if err != nil {
				t.Fatalf("MarkDeadBatch: %v", err)
			}
			if res.Succeeded != 3 {
				t.Fatalf("Succeeded=%d, want 3", res.Succeeded)
			}
			// Blank, unknown, and the repeat — a lease named twice in one batch
			// may only succeed once.
			if len(res.Conflicts) != 3 {
				t.Fatalf("Conflicts=%d (%#v), want 3", len(res.Conflicts), res.Conflicts)
			}
			for _, c := range res.Conflicts {
				if c.Expired {
					t.Fatalf("unexpected expired conflict: %#v", c)
				}
			}

			dead, err := store.ListDead(queue.DeadListRequest{Limit: 10})
			if err != nil {
				t.Fatalf("list dead: %v", err)
			}
			if len(dead.Items) != 3 {
				t.Fatalf("dead items=%d, want 3", len(dead.Items))
			}
		})
	}
}

// An expired lease is requeued rather than settled, and reported as an expired
// conflict — in the same batch, without taking the valid leases down with it.
func TestStoreContract_LeaseBatchRequeuesExpiredLeases(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
			store := factory.new(t, &now)

			batchStore, ok := store.(queue.LeaseBatchStore)
			if !ok {
				t.Fatalf("%T does not implement queue.LeaseBatchStore", store)
			}

			for _, id := range []string{"evt_short", "evt_long"} {
				if err := store.Enqueue(queue.Envelope{ID: id, Route: "/r", Target: "pull"}); err != nil {
					t.Fatalf("enqueue %s: %v", id, err)
				}
			}
			// Both leases are taken before the clock moves, with different TTLs,
			// so advancing time expires exactly one of them. Taking the second
			// lease after the advance instead would let the dequeue reclaim the
			// first expired item, and the batch would then see an unknown lease
			// rather than an expired one.
			short, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: time.Second})
			if err != nil || len(short.Items) != 1 {
				t.Fatalf("dequeue short: err=%v items=%d", err, len(short.Items))
			}
			expiringLease := short.Items[0].LeaseID

			long, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: time.Hour})
			if err != nil || len(long.Items) != 1 {
				t.Fatalf("dequeue long: err=%v items=%d", err, len(long.Items))
			}
			validLease := long.Items[0].LeaseID

			now = now.Add(5 * time.Second)

			res, err := batchStore.AckBatch([]string{expiringLease, validLease})
			if err != nil {
				t.Fatalf("AckBatch: %v", err)
			}
			if res.Succeeded != 1 {
				t.Fatalf("Succeeded=%d, want 1 -- only the unexpired lease settles", res.Succeeded)
			}
			if len(res.Conflicts) != 1 || !res.Conflicts[0].Expired {
				t.Fatalf("Conflicts=%#v, want one expired conflict", res.Conflicts)
			}
		})
	}
}

func leaseBatchID(i int) string {
	return "evt_lb_" + string(rune('a'+i))
}

// A requeued message must come back with a fresh retry budget. Requeue reset
// state, lease and reason but kept Attempt, so a message that dead-lettered at
// attempt 9 (retry.max 8) returned at 9: the next dequeue made it 10, the
// dispatcher's `env.Attempt <= target.Retry.Max` gate was false for every
// retryable outcome, and a single 503 or connection-refused sent it straight
// back to the DLQ with reason max_retries. The configured exponential schedule
// never applied to requeued messages at all, so an operator requeueing a cohort
// during a brief target blip re-dead-lettered all of it.
func TestStoreContract_RequeueResetsAttempt(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			requeues := map[string]func(store queue.Store) error{
				"RequeueDead": func(store queue.Store) error {
					_, err := store.RequeueDead(queue.DeadRequeueRequest{IDs: []string{"evt_dead"}})
					return err
				},
				"RequeueMessages": func(store queue.Store) error {
					_, err := store.RequeueMessages(queue.MessageRequeueRequest{IDs: []string{"evt_dead"}})
					return err
				},
				"RequeueMessagesByFilter": func(store queue.Store) error {
					_, err := store.RequeueMessagesByFilter(queue.MessageManageFilterRequest{Route: "/r"})
					return err
				},
			}

			for name, requeue := range requeues {
				t.Run(name, func(t *testing.T) {
					now := time.Date(2026, 2, 14, 21, 15, 0, 0, time.UTC)
					store := factory.new(t, &now)

					if err := store.Enqueue(queue.Envelope{ID: "evt_dead", Route: "/r", Target: "pull"}); err != nil {
						t.Fatalf("enqueue: %v", err)
					}

					// Burn a few attempts the way repeated delivery failures do.
					const failures = 3
					for i := 0; i < failures; i++ {
						resp, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
						if err != nil {
							t.Fatalf("dequeue %d: %v", i, err)
						}
						if len(resp.Items) != 1 {
							t.Fatalf("dequeue %d items=%d, want 1", i, len(resp.Items))
						}
						if got, want := resp.Items[0].Attempt, i+1; got != want {
							t.Fatalf("attempt after dequeue %d = %d, want %d", i, got, want)
						}
						if i == failures-1 {
							if err := store.MarkDead(resp.Items[0].LeaseID, "max_retries"); err != nil {
								t.Fatalf("mark dead: %v", err)
							}
							break
						}
						if err := store.Nack(resp.Items[0].LeaseID, 0); err != nil {
							t.Fatalf("nack %d: %v", i, err)
						}
					}

					if err := requeue(store); err != nil {
						t.Fatalf("requeue: %v", err)
					}

					resp, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 10 * time.Second})
					if err != nil {
						t.Fatalf("dequeue after requeue: %v", err)
					}
					if len(resp.Items) != 1 {
						t.Fatalf("dequeue after requeue items=%d, want 1", len(resp.Items))
					}
					if got := resp.Items[0].Attempt; got != 1 {
						t.Fatalf("attempt after requeue = %d, want 1: the message keeps its old retry count and dead-letters again after one delivery", got)
					}
				})
			}
		})
	}
}

// Waiters -- the store's own long-poll Dequeue and any NotifyCh observer, such
// as an SSE stream -- learn about ready items only through the notify channel.
// Several transitions never fired it: the memory backend signalled on enqueue
// and requeue but not on Nack or the expiry sweep, and Postgres signalled on
// enqueue only, so a nacked or operator-requeued message sat ready for up to
// the SSE keepalive (15s) or a poll interval with a consumer connected and
// idle. No message was lost; the cost was latency, and documented backend
// parity was broken.
func TestStoreContract_ReadinessTransitionsNotifyWaiters(t *testing.T) {
	for _, factory := range contractStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			transitions := []struct {
				name string
				// run performs the transition on a store holding one message
				// with ID "evt_1", already dequeued and leased.
				run func(t *testing.T, store queue.Store, lease queue.Envelope, now *time.Time)
			}{
				{
					name: "nack",
					run: func(t *testing.T, store queue.Store, lease queue.Envelope, _ *time.Time) {
						if err := store.Nack(lease.LeaseID, 0); err != nil {
							t.Fatalf("nack: %v", err)
						}
					},
				},
				{
					name: "requeue_dead",
					run: func(t *testing.T, store queue.Store, lease queue.Envelope, _ *time.Time) {
						if err := store.MarkDead(lease.LeaseID, "test"); err != nil {
							t.Fatalf("mark dead: %v", err)
						}
						if _, err := store.RequeueDead(queue.DeadRequeueRequest{IDs: []string{"evt_1"}}); err != nil {
							t.Fatalf("requeue dead: %v", err)
						}
					},
				},
				{
					name: "resume",
					run: func(t *testing.T, store queue.Store, lease queue.Envelope, _ *time.Time) {
						if err := store.Nack(lease.LeaseID, 0); err != nil {
							t.Fatalf("nack: %v", err)
						}
						if _, err := store.CancelMessages(queue.MessageCancelRequest{IDs: []string{"evt_1"}}); err != nil {
							t.Fatalf("cancel: %v", err)
						}
						if _, err := store.ResumeMessages(queue.MessageResumeRequest{IDs: []string{"evt_1"}}); err != nil {
							t.Fatalf("resume: %v", err)
						}
					},
				},
				{
					name: "lease_expiry",
					run: func(t *testing.T, store queue.Store, _ queue.Envelope, now *time.Time) {
						// The sweep runs inside a dequeue. Another consumer's
						// poll is what reclaims the lease, and the item it does
						// not take has to wake everyone else.
						*now = now.Add(time.Hour)
						if _, err := store.Dequeue(queue.DequeueRequest{Route: "/other", Target: "pull", Batch: 1}); err != nil {
							t.Fatalf("sweeping dequeue: %v", err)
						}
					},
				},
			}

			for _, tc := range transitions {
				t.Run(tc.name, func(t *testing.T) {
					now := time.Date(2026, 2, 14, 21, 15, 0, 0, time.UTC)
					store := factory.new(t, &now)

					notifier, ok := store.(queue.StoreNotifier)
					if !ok {
						t.Skipf("%s does not implement StoreNotifier", factory.name)
					}

					if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
						t.Fatalf("enqueue: %v", err)
					}
					resp, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: time.Minute})
					if err != nil {
						t.Fatalf("dequeue: %v", err)
					}
					if len(resp.Items) != 1 {
						t.Fatalf("dequeue items=%d, want 1", len(resp.Items))
					}

					// Captured after the setup, so only the transition itself
					// can close it.
					ch := notifier.NotifyCh()
					tc.run(t, store, resp.Items[0], &now)

					select {
					case <-ch:
					default:
						t.Fatalf("%s made the message ready without waking waiters", tc.name)
					}
				})
			}
		})
	}
}

// contractAttemptsRetentionFactories configures attempt-history retention on
// every backend so the shared behaviour can be exercised.
func contractAttemptsRetentionFactories(maxAge time.Duration, maxRows int) []storeFactory {
	out := []storeFactory{
		{
			name: "memory",
			new: func(t *testing.T, now *time.Time) queue.Store {
				t.Helper()
				return queue.NewMemoryStore(
					queue.WithNowFunc(func() time.Time { return now.UTC() }),
					queue.WithQueueRetention(0, time.Nanosecond),
					queue.WithAttemptsRetention(maxAge, maxRows),
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
					sqlite.WithRetention(0, time.Nanosecond),
					sqlite.WithAttemptsRetention(maxAge, maxRows),
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
					postgres.WithRetention(0, time.Nanosecond),
					postgres.WithAttemptsRetention(maxAge, maxRows),
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

func recordAttempts(t *testing.T, store queue.Store, n int, at time.Time) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := store.RecordAttempt(queue.DeliveryAttempt{
			ID:        fmt.Sprintf("att_%d_%d", at.UnixNano(), i),
			EventID:   fmt.Sprintf("evt_%d", i),
			Route:     "/r",
			Target:    "https://example.org/hook",
			Attempt:   1,
			Outcome:   queue.AttemptOutcomeAcked,
			CreatedAt: at,
		}); err != nil {
			t.Fatalf("record attempt %d: %v", i, err)
		}
	}
}

func attemptCount(t *testing.T, store queue.Store) int {
	t.Helper()
	resp, err := store.ListAttempts(queue.AttemptListRequest{Limit: 1000})
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	return len(resp.Items)
}

// Delivery-attempt history was append-only in every backend: nothing anywhere
// deleted or capped it. The memory store grew until it OOMed the process --
// invisible to the memory-pressure guard, which counts only envelope bytes --
// and the SQLite/Postgres tables grew until the disk filled and enqueue started
// failing.
func TestStoreContract_AttemptsRetentionPrunesByAge(t *testing.T) {
	for _, factory := range contractAttemptsRetentionFactories(time.Hour, 0) {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 2, 14, 21, 15, 0, 0, time.UTC)
			store := factory.new(t, &now)

			recordAttempts(t, store, 3, now)
			if got := attemptCount(t, store); got != 3 {
				t.Fatalf("attempts before prune = %d, want 3", got)
			}

			now = now.Add(2 * time.Hour)
			recordAttempts(t, store, 1, now)

			// Retention runs with the other prunes, on enqueue or dequeue.
			if _, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1}); err != nil {
				t.Fatalf("dequeue: %v", err)
			}

			if got := attemptCount(t, store); got != 1 {
				t.Fatalf("attempts after prune = %d, want 1 (only the recent one survives)", got)
			}
		})
	}
}

func TestStoreContract_AttemptsRetentionCapsRowCount(t *testing.T) {
	for _, factory := range contractAttemptsRetentionFactories(0, 5) {
		t.Run(factory.name, func(t *testing.T) {
			now := time.Date(2026, 2, 14, 21, 15, 0, 0, time.UTC)
			store := factory.new(t, &now)

			for i := 0; i < 12; i++ {
				now = now.Add(time.Second)
				recordAttempts(t, store, 1, now)
			}
			if _, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1}); err != nil {
				t.Fatalf("dequeue: %v", err)
			}

			if got := attemptCount(t, store); got > 5 {
				t.Fatalf("attempts = %d, want at most the configured 5", got)
			}
		})
	}
}
