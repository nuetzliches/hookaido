package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/hookaido"
	"github.com/nuetzliches/hookaido/v2/internal/queue"
	"github.com/nuetzliches/hookaido/v2/internal/secrets"
)

// freshIntegrationStore returns a *Store backed by the live test Postgres,
// truncating all tables so each test starts clean. The caller is responsible
// for closing the store via t.Cleanup.
func freshIntegrationStore(t *testing.T, opts ...Option) *Store {
	t.Helper()
	dsn := requirePostgresDSN(t)
	store, err := NewStore(dsn, opts...)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, table := range []string{"queue_items", "delivery_attempts", "backlog_trend_samples", "runtime_secrets"} {
		if _, err := store.db.Exec("DELETE FROM " + table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
	return store
}

func enqueueTest(t *testing.T, s *Store, id, route, target string) {
	t.Helper()
	if err := s.Enqueue(queue.Envelope{
		ID: id, Route: route, Target: target, Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("Enqueue %s: %v", id, err)
	}
}

func dequeueOne(t *testing.T, s *Store, route, target string, ttl time.Duration) queue.Envelope {
	t.Helper()
	resp, err := s.Dequeue(queue.DequeueRequest{
		Route: route, Target: target, Batch: 1, LeaseTTL: ttl,
	})
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("dequeue items=%d, want 1", len(resp.Items))
	}
	return resp.Items[0]
}

// --- AckBatch / NackBatch / MarkDeadBatch ---

func TestIntegration_AckBatch_SuccessAndConflicts(t *testing.T) {
	store := freshIntegrationStore(t)
	enqueueTest(t, store, "ab_1", "/r", "pull")
	enqueueTest(t, store, "ab_2", "/r", "pull")

	first := dequeueOne(t, store, "/r", "pull", 30*time.Second)
	second := dequeueOne(t, store, "/r", "pull", 30*time.Second)

	res, err := store.AckBatch([]string{first.LeaseID, "  ", "lease_unknown", second.LeaseID})
	if err != nil {
		t.Fatalf("AckBatch: %v", err)
	}
	if res.Succeeded != 2 {
		t.Fatalf("Succeeded=%d, want 2", res.Succeeded)
	}
	if len(res.Conflicts) != 2 {
		t.Fatalf("Conflicts=%d, want 2", len(res.Conflicts))
	}
	// Empty/whitespace lease => not-found-style conflict (Expired=false).
	// Bogus lease => not-found conflict.
	for _, c := range res.Conflicts {
		if c.Expired {
			t.Fatalf("unexpected Expired=true for lease %q", c.LeaseID)
		}
	}
}

func TestIntegration_NackBatch_DelayAndConflicts(t *testing.T) {
	now := time.Now().UTC()
	store := freshIntegrationStore(t, WithNowFunc(func() time.Time { return now }))
	enqueueTest(t, store, "nb_1", "/r", "pull")
	enqueueTest(t, store, "nb_2", "/r", "pull")

	a := dequeueOne(t, store, "/r", "pull", 30*time.Second)
	b := dequeueOne(t, store, "/r", "pull", 30*time.Second)

	res, err := store.NackBatch([]string{a.LeaseID, b.LeaseID, "lease_unknown"}, 5*time.Second)
	if err != nil {
		t.Fatalf("NackBatch: %v", err)
	}
	if res.Succeeded != 2 {
		t.Fatalf("Succeeded=%d, want 2", res.Succeeded)
	}
	if len(res.Conflicts) != 1 {
		t.Fatalf("Conflicts=%d, want 1", len(res.Conflicts))
	}
}

func TestIntegration_MarkDeadBatch_SuccessAndConflicts(t *testing.T) {
	store := freshIntegrationStore(t)
	enqueueTest(t, store, "md_1", "/r", "pull")
	enqueueTest(t, store, "md_2", "/r", "pull")

	a := dequeueOne(t, store, "/r", "pull", 30*time.Second)
	b := dequeueOne(t, store, "/r", "pull", 30*time.Second)

	res, err := store.MarkDeadBatch([]string{a.LeaseID, b.LeaseID, "", "lease_unknown"}, "test-failure")
	if err != nil {
		t.Fatalf("MarkDeadBatch: %v", err)
	}
	if res.Succeeded != 2 {
		t.Fatalf("Succeeded=%d, want 2", res.Succeeded)
	}
	if len(res.Conflicts) != 2 {
		t.Fatalf("Conflicts=%d, want 2", len(res.Conflicts))
	}

	dead, err := store.ListDead(queue.DeadListRequest{Limit: 100})
	if err != nil {
		t.Fatalf("ListDead: %v", err)
	}
	if len(dead.Items) != 2 {
		t.Fatalf("dead items=%d, want 2", len(dead.Items))
	}
	for _, item := range dead.Items {
		if item.DeadReason != "test-failure" {
			t.Fatalf("DeadReason=%q, want %q", item.DeadReason, "test-failure")
		}
	}
}

func TestIntegration_AckBatch_ExpiredLease(t *testing.T) {
	now := time.Now().UTC()
	store := freshIntegrationStore(t, WithNowFunc(func() time.Time { return now }))
	enqueueTest(t, store, "exp_1", "/r", "pull")

	item := dequeueOne(t, store, "/r", "pull", 1*time.Second)

	// Advance virtual clock past lease expiry.
	now = now.Add(10 * time.Second)
	res, err := store.AckBatch([]string{item.LeaseID})
	if err != nil {
		t.Fatalf("AckBatch: %v", err)
	}
	if res.Succeeded != 0 {
		t.Fatalf("Succeeded=%d, want 0", res.Succeeded)
	}
	if len(res.Conflicts) != 1 || !res.Conflicts[0].Expired {
		t.Fatalf("Conflicts=%v, want 1 expired", res.Conflicts)
	}
}

// --- DLQ list / requeue / delete ---

func TestIntegration_DeadDelete_RemovesOnlyMatching(t *testing.T) {
	store := freshIntegrationStore(t)
	for _, id := range []string{"dd_1", "dd_2", "dd_3"} {
		enqueueTest(t, store, id, "/r", "pull")
		item := dequeueOne(t, store, "/r", "pull", 30*time.Second)
		if err := store.MarkDead(item.LeaseID, "boom"); err != nil {
			t.Fatalf("MarkDead %s: %v", id, err)
		}
	}

	resp, err := store.DeleteDead(queue.DeadDeleteRequest{IDs: []string{"dd_1", "missing", "dd_2"}})
	if err != nil {
		t.Fatalf("DeleteDead: %v", err)
	}
	if resp.Deleted != 2 {
		t.Fatalf("Deleted=%d, want 2", resp.Deleted)
	}

	// Empty-IDs is a no-op.
	respEmpty, err := store.DeleteDead(queue.DeadDeleteRequest{})
	if err != nil {
		t.Fatalf("DeleteDead empty: %v", err)
	}
	if respEmpty.Deleted != 0 {
		t.Fatalf("Deleted empty=%d, want 0", respEmpty.Deleted)
	}

	// Only dd_3 remains in the DLQ.
	dead, err := store.ListDead(queue.DeadListRequest{Limit: 100})
	if err != nil {
		t.Fatalf("ListDead: %v", err)
	}
	if len(dead.Items) != 1 || dead.Items[0].ID != "dd_3" {
		t.Fatalf("remaining dead=%v, want [dd_3]", deadIDs(dead.Items))
	}
}

func TestIntegration_RequeueDead_EmptyAndUnknown(t *testing.T) {
	store := freshIntegrationStore(t)
	// Empty IDs is a no-op.
	respEmpty, err := store.RequeueDead(queue.DeadRequeueRequest{})
	if err != nil {
		t.Fatalf("RequeueDead empty: %v", err)
	}
	if respEmpty.Requeued != 0 {
		t.Fatalf("Requeued empty=%d, want 0", respEmpty.Requeued)
	}

	// Unknown ID => 0 requeued, no error.
	resp, err := store.RequeueDead(queue.DeadRequeueRequest{IDs: []string{"missing"}})
	if err != nil {
		t.Fatalf("RequeueDead missing: %v", err)
	}
	if resp.Requeued != 0 {
		t.Fatalf("Requeued missing=%d, want 0", resp.Requeued)
	}
}

func TestIntegration_ListDead_FilterAndExclusions(t *testing.T) {
	store := freshIntegrationStore(t)
	enqueueTest(t, store, "ld_1", "/route_a", "pull")
	enqueueTest(t, store, "ld_2", "/route_b", "pull")

	a := dequeueOne(t, store, "/route_a", "pull", 30*time.Second)
	b := dequeueOne(t, store, "/route_b", "pull", 30*time.Second)
	if err := store.MarkDead(a.LeaseID, "fail-a"); err != nil {
		t.Fatalf("mark dead a: %v", err)
	}
	if err := store.MarkDead(b.LeaseID, "fail-b"); err != nil {
		t.Fatalf("mark dead b: %v", err)
	}

	resp, err := store.ListDead(queue.DeadListRequest{
		Route:          "/route_a",
		Limit:          50,
		IncludePayload: false,
		IncludeHeaders: false,
		IncludeTrace:   false,
	})
	if err != nil {
		t.Fatalf("ListDead: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "ld_1" {
		t.Fatalf("ListDead route filter: items=%v", deadIDs(resp.Items))
	}
	if resp.Items[0].Payload != nil {
		t.Fatalf("Payload should be excluded")
	}
	if resp.Items[0].Headers != nil {
		t.Fatalf("Headers should be excluded")
	}

	// Limit=0 picks the default.
	respDefault, err := store.ListDead(queue.DeadListRequest{Limit: 0})
	if err != nil {
		t.Fatalf("ListDead default limit: %v", err)
	}
	if len(respDefault.Items) != 2 {
		t.Fatalf("default-limit dead items=%d, want 2", len(respDefault.Items))
	}

	// Before filter excludes recent items (received_at < now-1h => empty).
	respBefore, err := store.ListDead(queue.DeadListRequest{
		Before: time.Now().Add(-1 * time.Hour),
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListDead before: %v", err)
	}
	if len(respBefore.Items) != 0 {
		t.Fatalf("ListDead before items=%d, want 0", len(respBefore.Items))
	}
}

// --- ListMessages / LookupMessages ---

func TestIntegration_ListMessages_FiltersAndOrder(t *testing.T) {
	store := freshIntegrationStore(t)
	for _, id := range []string{"lm_1", "lm_2", "lm_3"} {
		enqueueTest(t, store, id, "/route_x", "pull")
	}
	enqueueTest(t, store, "lm_other", "/route_y", "pull")

	// Default order is desc.
	respDesc, err := store.ListMessages(queue.MessageListRequest{
		Route:          "/route_x",
		State:          queue.StateQueued,
		Limit:          10,
		IncludePayload: true,
		IncludeHeaders: true,
		IncludeTrace:   true,
	})
	if err != nil {
		t.Fatalf("ListMessages desc: %v", err)
	}
	if len(respDesc.Items) != 3 {
		t.Fatalf("ListMessages desc items=%d, want 3", len(respDesc.Items))
	}

	respAsc, err := store.ListMessages(queue.MessageListRequest{
		Route: "/route_x",
		Order: queue.MessageOrderAsc,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListMessages asc: %v", err)
	}
	if len(respAsc.Items) != 3 {
		t.Fatalf("ListMessages asc items=%d, want 3", len(respAsc.Items))
	}
	// Without IncludePayload, payload should be nil.
	if respAsc.Items[0].Payload != nil {
		t.Fatalf("payload should be excluded by default")
	}

	// Target filter.
	respTarget, err := store.ListMessages(queue.MessageListRequest{
		Target: "pull",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListMessages target: %v", err)
	}
	if len(respTarget.Items) != 4 {
		t.Fatalf("ListMessages target items=%d, want 4", len(respTarget.Items))
	}

	// Invalid order => error.
	if _, err := store.ListMessages(queue.MessageListRequest{Order: "sideways"}); err == nil {
		t.Fatalf("expected error for invalid order")
	}

	// Default limit clamp (Limit <= 0 picks 100).
	respDefault, err := store.ListMessages(queue.MessageListRequest{Route: "/route_x"})
	if err != nil {
		t.Fatalf("ListMessages default limit: %v", err)
	}
	if len(respDefault.Items) != 3 {
		t.Fatalf("default-limit items=%d, want 3", len(respDefault.Items))
	}

	// Before filter — set Before in the past so no rows match.
	respBefore, err := store.ListMessages(queue.MessageListRequest{
		Route:  "/route_x",
		Before: time.Now().Add(-1 * time.Hour),
	})
	if err != nil {
		t.Fatalf("ListMessages before: %v", err)
	}
	if len(respBefore.Items) != 0 {
		t.Fatalf("ListMessages before items=%d, want 0", len(respBefore.Items))
	}
}

func TestIntegration_LookupMessages_DedupAndMissing(t *testing.T) {
	store := freshIntegrationStore(t)
	enqueueTest(t, store, "lk_1", "/r", "pull")
	enqueueTest(t, store, "lk_2", "/r", "pull")

	// Empty IDs list returns empty response with no error.
	respEmpty, err := store.LookupMessages(queue.MessageLookupRequest{})
	if err != nil {
		t.Fatalf("LookupMessages empty: %v", err)
	}
	if len(respEmpty.Items) != 0 {
		t.Fatalf("LookupMessages empty items=%d, want 0", len(respEmpty.Items))
	}

	// Whitespace + dup + unknown.
	resp, err := store.LookupMessages(queue.MessageLookupRequest{
		IDs: []string{"lk_1", "lk_1", " ", "missing", "lk_2"},
	})
	if err != nil {
		t.Fatalf("LookupMessages: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("LookupMessages items=%d, want 2", len(resp.Items))
	}
	gotIDs := make(map[string]bool)
	for _, it := range resp.Items {
		gotIDs[it.ID] = true
		if it.State != queue.StateQueued {
			t.Fatalf("State[%s]=%q, want queued", it.ID, it.State)
		}
	}
	if !gotIDs["lk_1"] || !gotIDs["lk_2"] {
		t.Fatalf("missing IDs in lookup: %v", gotIDs)
	}
}

// --- Cancel/Requeue/Resume by IDs ---

func TestIntegration_CancelRequeueResume_ByIDs(t *testing.T) {
	store := freshIntegrationStore(t)
	enqueueTest(t, store, "crr_1", "/r", "pull")
	enqueueTest(t, store, "crr_2", "/r", "pull")

	// Empty IDs is a no-op for all three.
	if r, err := store.CancelMessages(queue.MessageCancelRequest{}); err != nil || r.Canceled != 0 {
		t.Fatalf("CancelMessages empty: r=%+v err=%v", r, err)
	}
	if r, err := store.RequeueMessages(queue.MessageRequeueRequest{}); err != nil || r.Requeued != 0 {
		t.Fatalf("RequeueMessages empty: r=%+v err=%v", r, err)
	}
	if r, err := store.ResumeMessages(queue.MessageResumeRequest{}); err != nil || r.Resumed != 0 {
		t.Fatalf("ResumeMessages empty: r=%+v err=%v", r, err)
	}

	// Cancel both.
	cancelResp, err := store.CancelMessages(queue.MessageCancelRequest{IDs: []string{"crr_1", "crr_2"}})
	if err != nil {
		t.Fatalf("CancelMessages: %v", err)
	}
	if cancelResp.Canceled != 2 {
		t.Fatalf("Canceled=%d, want 2", cancelResp.Canceled)
	}

	// Resume one.
	resumeResp, err := store.ResumeMessages(queue.MessageResumeRequest{IDs: []string{"crr_1"}})
	if err != nil {
		t.Fatalf("ResumeMessages: %v", err)
	}
	if resumeResp.Resumed != 1 {
		t.Fatalf("Resumed=%d, want 1", resumeResp.Resumed)
	}

	// Requeue the other (it's still canceled).
	requeueResp, err := store.RequeueMessages(queue.MessageRequeueRequest{IDs: []string{"crr_2"}})
	if err != nil {
		t.Fatalf("RequeueMessages: %v", err)
	}
	if requeueResp.Requeued != 1 {
		t.Fatalf("Requeued=%d, want 1", requeueResp.Requeued)
	}

	// Both should now be queued again.
	resp, err := store.ListMessages(queue.MessageListRequest{Route: "/r", State: queue.StateQueued, Limit: 10})
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("queued items=%d, want 2", len(resp.Items))
	}
}

// --- ByFilter variants ---

func TestIntegration_CancelMessagesByFilter_PreviewAndExecute(t *testing.T) {
	store := freshIntegrationStore(t)
	for _, id := range []string{"f_1", "f_2", "f_3"} {
		enqueueTest(t, store, id, "/r", "pull")
	}

	preview, err := store.CancelMessagesByFilter(queue.MessageManageFilterRequest{
		Route:       "/r",
		State:       queue.StateQueued,
		Limit:       10,
		PreviewOnly: true,
	})
	if err != nil {
		t.Fatalf("CancelMessagesByFilter preview: %v", err)
	}
	if !preview.PreviewOnly {
		t.Fatalf("PreviewOnly=false, want true")
	}
	if preview.Matched != 3 || preview.Canceled != 0 {
		t.Fatalf("preview matched=%d canceled=%d, want 3/0", preview.Matched, preview.Canceled)
	}

	// Execute.
	exec, err := store.CancelMessagesByFilter(queue.MessageManageFilterRequest{
		Route: "/r",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("CancelMessagesByFilter exec: %v", err)
	}
	if exec.Canceled != 3 || exec.Matched != 3 {
		t.Fatalf("exec canceled=%d matched=%d, want 3/3", exec.Canceled, exec.Matched)
	}

	// Filter that matches nothing — preview branch.
	emptyPreview, err := store.CancelMessagesByFilter(queue.MessageManageFilterRequest{
		Route:       "/no-such-route",
		PreviewOnly: true,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("empty preview: %v", err)
	}
	if !emptyPreview.PreviewOnly || emptyPreview.Matched != 0 {
		t.Fatalf("empty preview = %+v", emptyPreview)
	}

	// Filter that matches nothing — execute branch.
	emptyExec, err := store.CancelMessagesByFilter(queue.MessageManageFilterRequest{
		Route: "/no-such-route",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("empty exec: %v", err)
	}
	if emptyExec.Matched != 0 || emptyExec.Canceled != 0 {
		t.Fatalf("empty exec = %+v", emptyExec)
	}

	// Disallowed state — selectMessageIDsByFilter returns nil.
	disallowed, err := store.CancelMessagesByFilter(queue.MessageManageFilterRequest{
		Route: "/r",
		State: queue.StateDelivered,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("disallowed state: %v", err)
	}
	if disallowed.Canceled != 0 || disallowed.Matched != 0 {
		t.Fatalf("disallowed state = %+v", disallowed)
	}
}

func TestIntegration_RequeueMessagesByFilter_PreviewAndExecute(t *testing.T) {
	store := freshIntegrationStore(t)
	enqueueTest(t, store, "rq_1", "/r", "pull")
	enqueueTest(t, store, "rq_2", "/r", "pull")
	if _, err := store.CancelMessages(queue.MessageCancelRequest{IDs: []string{"rq_1", "rq_2"}}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	preview, err := store.RequeueMessagesByFilter(queue.MessageManageFilterRequest{
		Route:       "/r",
		PreviewOnly: true,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("RequeueMessagesByFilter preview: %v", err)
	}
	if !preview.PreviewOnly || preview.Matched != 2 || preview.Requeued != 0 {
		t.Fatalf("preview = %+v", preview)
	}

	exec, err := store.RequeueMessagesByFilter(queue.MessageManageFilterRequest{
		Route: "/r",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("RequeueMessagesByFilter exec: %v", err)
	}
	if exec.Requeued != 2 || exec.Matched != 2 {
		t.Fatalf("exec = %+v", exec)
	}
}

func TestIntegration_ResumeMessagesByFilter_PreviewAndExecute(t *testing.T) {
	store := freshIntegrationStore(t)
	enqueueTest(t, store, "rs_1", "/r", "pull")
	enqueueTest(t, store, "rs_2", "/r", "pull")
	if _, err := store.CancelMessages(queue.MessageCancelRequest{IDs: []string{"rs_1", "rs_2"}}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	preview, err := store.ResumeMessagesByFilter(queue.MessageManageFilterRequest{
		Route:       "/r",
		PreviewOnly: true,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ResumeMessagesByFilter preview: %v", err)
	}
	if !preview.PreviewOnly || preview.Matched != 2 || preview.Resumed != 0 {
		t.Fatalf("preview = %+v", preview)
	}

	exec, err := store.ResumeMessagesByFilter(queue.MessageManageFilterRequest{
		Route: "/r",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ResumeMessagesByFilter exec: %v", err)
	}
	if exec.Resumed != 2 || exec.Matched != 2 {
		t.Fatalf("exec = %+v", exec)
	}
}

// --- Stats ---

func TestIntegration_Stats_AggregatesByState(t *testing.T) {
	store := freshIntegrationStore(t)
	enqueueTest(t, store, "st_1", "/r", "pull")
	enqueueTest(t, store, "st_2", "/r", "pull")
	enqueueTest(t, store, "st_3", "/r2", "deliver")

	leased := dequeueOne(t, store, "/r", "pull", 30*time.Second)

	dead := dequeueOne(t, store, "/r2", "deliver", 30*time.Second)
	if err := store.MarkDead(dead.LeaseID, "boom"); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Total != 3 {
		t.Fatalf("Total=%d, want 3", stats.Total)
	}
	if stats.ByState[queue.StateQueued] != 1 {
		t.Fatalf("queued=%d, want 1", stats.ByState[queue.StateQueued])
	}
	if stats.ByState[queue.StateLeased] != 1 {
		t.Fatalf("leased=%d, want 1", stats.ByState[queue.StateLeased])
	}
	if stats.ByState[queue.StateDead] != 1 {
		t.Fatalf("dead=%d, want 1", stats.ByState[queue.StateDead])
	}
	if stats.OldestQueuedReceivedAt.IsZero() {
		t.Fatalf("OldestQueuedReceivedAt should be set")
	}
	if stats.OldestQueuedAge < 0 {
		t.Fatalf("OldestQueuedAge should be >= 0")
	}
	if len(stats.TopQueued) == 0 {
		t.Logf("note: TopQueued may be 0 if backlog buckets aren't populated for small datasets")
	}

	// Suppress unused warnings.
	_ = leased
}

// --- RecordAttempt / ListAttempts ---

func TestIntegration_RecordAndListAttempts(t *testing.T) {
	store := freshIntegrationStore(t)

	if err := store.RecordAttempt(queue.DeliveryAttempt{
		EventID:    "evt_a",
		Route:      "/r",
		Target:     "https://example.com",
		Attempt:    1,
		StatusCode: 500,
		Error:      "boom",
		Outcome:    queue.AttemptOutcomeRetry,
	}); err != nil {
		t.Fatalf("RecordAttempt 1: %v", err)
	}
	if err := store.RecordAttempt(queue.DeliveryAttempt{
		EventID: "evt_a",
		Route:   "/r",
		Target:  "https://example.com",
		Attempt: 2,
		Outcome: queue.AttemptOutcomeAcked,
	}); err != nil {
		t.Fatalf("RecordAttempt 2: %v", err)
	}
	if err := store.RecordAttempt(queue.DeliveryAttempt{
		EventID:    "evt_b",
		Route:      "/r",
		Target:     "https://example.com",
		Attempt:    1,
		DeadReason: "max_retries",
		Outcome:    queue.AttemptOutcomeDead,
	}); err != nil {
		t.Fatalf("RecordAttempt 3: %v", err)
	}

	all, err := store.ListAttempts(queue.AttemptListRequest{Limit: 100})
	if err != nil {
		t.Fatalf("ListAttempts all: %v", err)
	}
	if len(all.Items) != 3 {
		t.Fatalf("ListAttempts all items=%d, want 3", len(all.Items))
	}

	// Default limit (Limit=0 -> 100).
	allDefault, err := store.ListAttempts(queue.AttemptListRequest{})
	if err != nil {
		t.Fatalf("ListAttempts default: %v", err)
	}
	if len(allDefault.Items) != 3 {
		t.Fatalf("default-limit items=%d, want 3", len(allDefault.Items))
	}

	// Filter by event ID.
	byEvent, err := store.ListAttempts(queue.AttemptListRequest{EventID: "evt_a", Limit: 10})
	if err != nil {
		t.Fatalf("ListAttempts evt_a: %v", err)
	}
	if len(byEvent.Items) != 2 {
		t.Fatalf("by-event items=%d, want 2", len(byEvent.Items))
	}

	// Filter by outcome.
	byOutcome, err := store.ListAttempts(queue.AttemptListRequest{
		Outcome: queue.AttemptOutcomeDead,
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("ListAttempts dead: %v", err)
	}
	if len(byOutcome.Items) != 1 || byOutcome.Items[0].DeadReason != "max_retries" {
		t.Fatalf("by-outcome items=%v", byOutcome.Items)
	}

	// Filter by route + target.
	byRoute, err := store.ListAttempts(queue.AttemptListRequest{
		Route:  "/r",
		Target: "https://example.com",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListAttempts route: %v", err)
	}
	if len(byRoute.Items) != 3 {
		t.Fatalf("by-route items=%d, want 3", len(byRoute.Items))
	}

	// Before in the past => no items.
	byBefore, err := store.ListAttempts(queue.AttemptListRequest{
		Before: time.Now().Add(-1 * time.Hour),
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListAttempts before: %v", err)
	}
	if len(byBefore.Items) != 0 {
		t.Fatalf("by-before items=%d, want 0", len(byBefore.Items))
	}
}

func TestIntegration_RecordAttempt_DefaultsAndDuplicate(t *testing.T) {
	store := freshIntegrationStore(t)

	att := queue.DeliveryAttempt{
		ID:      "att_dupe",
		EventID: "evt",
		Route:   "/r",
		Target:  "pull",
		Attempt: 1,
		// Outcome empty => defaulted to retry.
		// CreatedAt zero => defaulted to now.
	}
	if err := store.RecordAttempt(att); err != nil {
		t.Fatalf("RecordAttempt: %v", err)
	}

	// Re-using the same ID should hit the unique-constraint mapping.
	if err := store.RecordAttempt(att); err == nil {
		t.Fatalf("expected duplicate-ID error")
	}

	resp, err := store.ListAttempts(queue.AttemptListRequest{Limit: 10})
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("items=%d, want 1", len(resp.Items))
	}
	if resp.Items[0].Outcome != queue.AttemptOutcomeRetry {
		t.Fatalf("default outcome=%q, want retry", resp.Items[0].Outcome)
	}
	if resp.Items[0].CreatedAt.IsZero() {
		t.Fatalf("default CreatedAt should be set")
	}
}

// --- Drop-oldest queue limit ---

func TestIntegration_QueueLimitsDropOldest(t *testing.T) {
	store := freshIntegrationStore(t, WithQueueLimits(2, "drop_oldest"))

	enqueueTest(t, store, "do_1", "/r", "pull")
	time.Sleep(10 * time.Millisecond) // ensure received_at ordering
	enqueueTest(t, store, "do_2", "/r", "pull")
	time.Sleep(10 * time.Millisecond)
	enqueueTest(t, store, "do_3", "/r", "pull") // should evict do_1

	resp, err := store.ListMessages(queue.MessageListRequest{
		Route: "/r",
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	gotIDs := make(map[string]bool, len(resp.Items))
	for _, item := range resp.Items {
		gotIDs[item.ID] = true
	}
	if gotIDs["do_1"] {
		t.Fatalf("do_1 should have been evicted, got items=%v", gotIDs)
	}
	if !gotIDs["do_2"] || !gotIDs["do_3"] {
		t.Fatalf("do_2 and do_3 should remain, got items=%v", gotIDs)
	}
}

// --- Lease expiry path (requeueLeaseTx via Dequeue) ---

func TestIntegration_DequeueRequeuesExpiredLease(t *testing.T) {
	now := time.Now().UTC()
	store := freshIntegrationStore(t, WithNowFunc(func() time.Time { return now }))
	enqueueTest(t, store, "exp_lease", "/r", "pull")

	first := dequeueOne(t, store, "/r", "pull", 1*time.Second)

	// Advance virtual clock so the lease expires; the next Dequeue should
	// requeue the expired lease and hand it back as a fresh lease.
	now = now.Add(10 * time.Second)
	second := dequeueOne(t, store, "/r", "pull", 30*time.Second)
	if second.ID != first.ID {
		t.Fatalf("second.ID=%q, want %q", second.ID, first.ID)
	}
	if second.LeaseID == first.LeaseID {
		t.Fatalf("LeaseID should have been refreshed after expiry")
	}
	if second.Attempt < first.Attempt {
		t.Fatalf("Attempt should not regress: first=%d second=%d", first.Attempt, second.Attempt)
	}
}

// --- runtime_secrets persistence ---

func TestIntegration_RuntimeSecrets_RoundTrip(t *testing.T) {
	store := freshIntegrationStore(t)

	now := time.Now().UTC().Truncate(time.Microsecond)
	rec := secrets.Record{
		PoolName:  "stripe-webhooks",
		ID:        "v1",
		Sealed:    []byte("sealed-bytes-1"),
		NotBefore: now,
		NotAfter:  now.Add(24 * time.Hour),
		CreatedAt: now,
	}

	if err := store.Upsert(rec); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Idempotent upsert (same key) — should overwrite Sealed.
	rec2 := rec
	rec2.Sealed = []byte("sealed-bytes-rotated")
	if err := store.Upsert(rec2); err != nil {
		t.Fatalf("Upsert overwrite: %v", err)
	}

	listed, err := store.ListByPool("stripe-webhooks")
	if err != nil {
		t.Fatalf("ListByPool: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("ListByPool items=%d, want 1", len(listed))
	}
	if string(listed[0].Sealed) != "sealed-bytes-rotated" {
		t.Fatalf("Sealed=%q, want overwritten", listed[0].Sealed)
	}
	if listed[0].NotBefore.UTC() != now {
		t.Fatalf("NotBefore=%v, want %v", listed[0].NotBefore, now)
	}
	if listed[0].NotAfter.IsZero() {
		t.Fatalf("NotAfter should be set")
	}

	// Add a record to a second pool to verify ListAll spans pools.
	other := secrets.Record{
		PoolName:  "cituro-webhooks",
		ID:        "v1",
		Sealed:    []byte("other"),
		NotBefore: now,
		// NotAfter zero => unbounded.
	}
	if err := store.Upsert(other); err != nil {
		t.Fatalf("Upsert other: %v", err)
	}

	all, err := store.ListAll()
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ListAll items=%d, want 2", len(all))
	}
	for _, r := range all {
		if r.PoolName == "cituro-webhooks" && !r.NotAfter.IsZero() {
			t.Fatalf("cituro NotAfter=%v, want zero/unbounded", r.NotAfter)
		}
	}

	// Delete the cituro record.
	removed, err := store.Delete("cituro-webhooks", "v1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !removed {
		t.Fatalf("Delete should report removed=true")
	}

	// Deleting a missing record returns false, no error.
	removedAgain, err := store.Delete("cituro-webhooks", "v1")
	if err != nil {
		t.Fatalf("Delete missing: %v", err)
	}
	if removedAgain {
		t.Fatalf("Delete missing should report removed=false")
	}

	allAfter, err := store.ListAll()
	if err != nil {
		t.Fatalf("ListAll after delete: %v", err)
	}
	if len(allAfter) != 1 || allAfter[0].PoolName != "stripe-webhooks" {
		t.Fatalf("ListAll after delete=%v", poolNames(allAfter))
	}
}

func TestIntegration_RuntimeSecrets_Validation(t *testing.T) {
	store := freshIntegrationStore(t)

	// Empty pool name.
	if err := store.Upsert(secrets.Record{ID: "v1", Sealed: []byte("x"), NotBefore: time.Now()}); err == nil ||
		!strings.Contains(err.Error(), "empty pool_name") {
		t.Fatalf("Upsert empty pool: err=%v", err)
	}

	// Empty ID.
	if err := store.Upsert(secrets.Record{PoolName: "p", Sealed: []byte("x"), NotBefore: time.Now()}); err == nil ||
		!strings.Contains(err.Error(), "empty id") {
		t.Fatalf("Upsert empty id: err=%v", err)
	}

	// Empty Sealed.
	if err := store.Upsert(secrets.Record{PoolName: "p", ID: "v1", NotBefore: time.Now()}); err == nil ||
		!strings.Contains(err.Error(), "empty sealed payload") {
		t.Fatalf("Upsert empty sealed: err=%v", err)
	}

	// Zero NotBefore.
	if err := store.Upsert(secrets.Record{PoolName: "p", ID: "v1", Sealed: []byte("x")}); err == nil ||
		!strings.Contains(err.Error(), "not_before is zero") {
		t.Fatalf("Upsert zero not_before: err=%v", err)
	}

	// CreatedAt is auto-defaulted from now() when zero.
	if err := store.Upsert(secrets.Record{
		PoolName: "p", ID: "v1", Sealed: []byte("x"), NotBefore: time.Now(),
	}); err != nil {
		t.Fatalf("Upsert with default CreatedAt: %v", err)
	}
}

// --- NotifyCh ---

func TestIntegration_NotifyCh_FiresOnEnqueue(t *testing.T) {
	store := freshIntegrationStore(t)

	ch := store.NotifyCh()
	if ch == nil {
		t.Fatalf("NotifyCh returned nil")
	}

	enqueueTest(t, store, "n_1", "/r", "pull")

	select {
	case <-ch:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatalf("NotifyCh did not fire within 2s after Enqueue")
	}
}

// --- OpenStore (registry) ---

func TestIntegration_OpenStore(t *testing.T) {
	dsn := requirePostgresDSN(t)

	be := backend{}
	if be.Name() != "postgres" {
		t.Fatalf("Name=%q, want postgres", be.Name())
	}

	store, closer, err := be.OpenStore(hookaido.QueueBackendConfig{DSN: dsn})
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	if store == nil {
		t.Fatalf("OpenStore returned nil store")
	}
	if closer == nil {
		t.Fatalf("OpenStore returned nil closer")
	}
	if err := closer(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Empty DSN should fail.
	if _, _, err := be.OpenStore(hookaido.QueueBackendConfig{}); err == nil {
		t.Fatalf("expected error for empty DSN")
	}
}

// --- helpers ---

func deadIDs(items []queue.Envelope) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.ID)
	}
	return out
}

func poolNames(records []secrets.Record) []string {
	out := make([]string, 0, len(records))
	for _, r := range records {
		out = append(out, r.PoolName)
	}
	return out
}
