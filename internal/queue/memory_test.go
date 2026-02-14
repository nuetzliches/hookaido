package queue

import (
	"errors"
	"testing"
	"time"
)

func TestMemoryStore_EnqueueDequeueAck(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	err := s.Enqueue(Envelope{
		ID:         "evt_1",
		Route:      "/pull/github",
		Target:     "pull",
		State:      StateQueued,
		ReceivedAt: nowVar,
		NextRunAt:  nowVar,
		Payload:    []byte("hello"),
		Headers:    map[string]string{"Content-Type": "text/plain"},
		Trace:      map[string]string{"trace_id": "t1"},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	resp, err := s.Dequeue(DequeueRequest{
		Route:    "/pull/github",
		Target:   "pull",
		Batch:    1,
		LeaseTTL: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	item := resp.Items[0]
	if item.Attempt != 1 {
		t.Fatalf("expected attempt=1, got %d", item.Attempt)
	}
	if item.State != StateLeased {
		t.Fatalf("expected leased state, got %q", item.State)
	}
	if item.LeaseID == "" {
		t.Fatalf("expected lease id")
	}
	if got, want := item.LeaseUntil, nowVar.Add(30*time.Second); !got.Equal(want) {
		t.Fatalf("lease_until: got %s want %s", got, want)
	}
	if got, want := item.NextRunAt, item.LeaseUntil; !got.Equal(want) {
		t.Fatalf("next_run_at should match lease_until: got %s want %s", got, want)
	}

	if err := s.Ack(item.LeaseID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	resp2, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull"})
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if len(resp2.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(resp2.Items))
	}
}

func TestMemoryStore_LeaseExpiryRequeues(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/pull/github", Target: "pull", Payload: []byte("x")}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	resp, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", LeaseTTL: 10 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	leaseID := resp.Items[0].LeaseID
	if leaseID == "" {
		t.Fatalf("expected lease id")
	}
	if resp.Items[0].Attempt != 1 {
		t.Fatalf("expected attempt=1, got %d", resp.Items[0].Attempt)
	}

	nowVar = nowVar.Add(11 * time.Second)
	if err := s.Ack(leaseID); err != ErrLeaseExpired {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}

	resp2, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", LeaseTTL: 10 * time.Second})
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if len(resp2.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp2.Items))
	}
	if resp2.Items[0].Attempt != 2 {
		t.Fatalf("expected attempt=2, got %d", resp2.Items[0].Attempt)
	}
}

func TestMemoryStore_NackDelay(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/pull/github", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	resp, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}

	if err := s.Nack(resp.Items[0].LeaseID, 5*time.Second); err != nil {
		t.Fatalf("nack: %v", err)
	}

	resp2, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull"})
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if len(resp2.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(resp2.Items))
	}

	nowVar = nowVar.Add(5 * time.Second)
	resp3, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull"})
	if err != nil {
		t.Fatalf("dequeue3: %v", err)
	}
	if len(resp3.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp3.Items))
	}
	if resp3.Items[0].Attempt != 2 {
		t.Fatalf("expected attempt=2, got %d", resp3.Items[0].Attempt)
	}
}

func TestMemoryStore_MarkDead(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item")
	}
	leaseID := resp.Items[0].LeaseID

	if err := s.MarkDead(leaseID, "no_retry"); err != nil {
		t.Fatalf("mark dead: %v", err)
	}
	env := s.items["evt_1"]
	if env == nil {
		t.Fatalf("expected item to remain")
	}
	if env.State != StateDead {
		t.Fatalf("expected dead state, got %s", env.State)
	}
	if env.DeadReason != "no_retry" {
		t.Fatalf("expected dead_reason %q, got %q", "no_retry", env.DeadReason)
	}
}

func TestMemoryStore_LeaseBatchMethods(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	for _, id := range []string{"evt_1", "evt_2", "evt_3"} {
		if err := s.Enqueue(Envelope{ID: id, Route: "/r", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	deq, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 2, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(deq.Items))
	}

	ackRes, err := s.AckBatch([]string{deq.Items[0].LeaseID, "missing", deq.Items[1].LeaseID})
	if err != nil {
		t.Fatalf("ack batch: %v", err)
	}
	if ackRes.Succeeded != 2 {
		t.Fatalf("expected 2 successful acks, got %d", ackRes.Succeeded)
	}
	if len(ackRes.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %#v", ackRes.Conflicts)
	}
	if ackRes.Conflicts[0].LeaseID != "missing" || ackRes.Conflicts[0].Expired {
		t.Fatalf("unexpected conflict: %#v", ackRes.Conflicts[0])
	}

	deq2, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if len(deq2.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq2.Items))
	}

	nackRes, err := s.NackBatch([]string{deq2.Items[0].LeaseID}, 5*time.Second)
	if err != nil {
		t.Fatalf("nack batch: %v", err)
	}
	if nackRes.Succeeded != 1 || len(nackRes.Conflicts) != 0 {
		t.Fatalf("unexpected nack batch result: %#v", nackRes)
	}

	deq3, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue3: %v", err)
	}
	if len(deq3.Items) != 0 {
		t.Fatalf("expected no items before nack delay elapsed, got %d", len(deq3.Items))
	}

	nowVar = nowVar.Add(5 * time.Second)
	deq4, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue4: %v", err)
	}
	if len(deq4.Items) != 1 {
		t.Fatalf("expected 1 item after nack delay, got %d", len(deq4.Items))
	}

	deadRes, err := s.MarkDeadBatch([]string{deq4.Items[0].LeaseID, "missing_dead"}, "no_retry")
	if err != nil {
		t.Fatalf("mark dead batch: %v", err)
	}
	if deadRes.Succeeded != 1 {
		t.Fatalf("expected 1 successful mark dead, got %d", deadRes.Succeeded)
	}
	if len(deadRes.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %#v", deadRes.Conflicts)
	}
	if deadRes.Conflicts[0].LeaseID != "missing_dead" || deadRes.Conflicts[0].Expired {
		t.Fatalf("unexpected conflict: %#v", deadRes.Conflicts[0])
	}

	env := s.items[deq4.Items[0].ID]
	if env == nil {
		t.Fatalf("expected dead item to remain in store")
	}
	if env.State != StateDead {
		t.Fatalf("expected dead state, got %q", env.State)
	}
	if env.DeadReason != "no_retry" {
		t.Fatalf("expected dead reason %q, got %q", "no_retry", env.DeadReason)
	}
}

func TestMemoryStore_AckBatchExpiredRequeues(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deq, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq.Items))
	}
	leaseID := deq.Items[0].LeaseID

	nowVar = nowVar.Add(2 * time.Second)
	res, err := s.AckBatch([]string{leaseID})
	if err != nil {
		t.Fatalf("ack batch: %v", err)
	}
	if res.Succeeded != 0 {
		t.Fatalf("expected 0 successful acks, got %d", res.Succeeded)
	}
	if len(res.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %#v", res.Conflicts)
	}
	if res.Conflicts[0].LeaseID != leaseID || !res.Conflicts[0].Expired {
		t.Fatalf("unexpected conflict: %#v", res.Conflicts[0])
	}

	deq2, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: time.Second})
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if len(deq2.Items) != 1 {
		t.Fatalf("expected requeued item, got %d", len(deq2.Items))
	}
	if deq2.Items[0].Attempt != 2 {
		t.Fatalf("expected attempt=2 after expired batch ack, got %d", deq2.Items[0].Attempt)
	}
}

func TestMemoryStore_ListDead(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: now.Add(-2 * time.Minute), Payload: []byte("a")}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "dead_2", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: now.Add(-1 * time.Minute), Payload: []byte("b")}); err != nil {
		t.Fatalf("enqueue dead_2: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "dead_3", Route: "/other", Target: "pull", State: StateDead, ReceivedAt: now.Add(-30 * time.Second), Payload: []byte("c")}); err != nil {
		t.Fatalf("enqueue dead_3: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: now}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}

	resp, err := s.ListDead(DeadListRequest{Route: "/r", Limit: 10})
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "dead_2" || resp.Items[1].ID != "dead_1" {
		t.Fatalf("unexpected order: %#v", []string{resp.Items[0].ID, resp.Items[1].ID})
	}
	if resp.Items[0].Payload != nil {
		t.Fatalf("expected payload omitted by default")
	}

	resp2, err := s.ListDead(DeadListRequest{
		Route:          "/r",
		Limit:          10,
		Before:         now.Add(-90 * time.Second),
		IncludePayload: true,
	})
	if err != nil {
		t.Fatalf("list dead before: %v", err)
	}
	if len(resp2.Items) != 1 || resp2.Items[0].ID != "dead_1" {
		t.Fatalf("expected dead_1 only, got %#v", resp2.Items)
	}
	if string(resp2.Items[0].Payload) != "a" {
		t.Fatalf("expected payload included")
	}
}

func TestMemoryStore_RecordAndListAttempts(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.RecordAttempt(DeliveryAttempt{
		ID:         "att_1",
		EventID:    "evt_1",
		Route:      "/r",
		Target:     "https://example.internal/a",
		Attempt:    1,
		StatusCode: 503,
		Error:      "upstream timeout",
		Outcome:    AttemptOutcomeRetry,
		CreatedAt:  now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("record att_1: %v", err)
	}
	if err := s.RecordAttempt(DeliveryAttempt{
		ID:         "att_2",
		EventID:    "evt_1",
		Route:      "/r",
		Target:     "https://example.internal/a",
		Attempt:    2,
		StatusCode: 502,
		Outcome:    AttemptOutcomeDead,
		DeadReason: "max_retries",
		CreatedAt:  now.Add(-1 * time.Minute),
	}); err != nil {
		t.Fatalf("record att_2: %v", err)
	}
	if err := s.RecordAttempt(DeliveryAttempt{
		ID:        "att_3",
		EventID:   "evt_2",
		Route:     "/other",
		Target:    "https://example.internal/b",
		Attempt:   1,
		Outcome:   AttemptOutcomeAcked,
		CreatedAt: now.Add(-30 * time.Second),
	}); err != nil {
		t.Fatalf("record att_3: %v", err)
	}

	resp, err := s.ListAttempts(AttemptListRequest{
		Route:   "/r",
		EventID: "evt_1",
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "att_2" || resp.Items[1].ID != "att_1" {
		t.Fatalf("unexpected order: %#v", []string{resp.Items[0].ID, resp.Items[1].ID})
	}
	if resp.Items[0].Outcome != AttemptOutcomeDead {
		t.Fatalf("expected dead outcome, got %q", resp.Items[0].Outcome)
	}
	if resp.Items[0].DeadReason != "max_retries" {
		t.Fatalf("expected dead reason max_retries, got %q", resp.Items[0].DeadReason)
	}

	resp2, err := s.ListAttempts(AttemptListRequest{
		Outcome: AttemptOutcomeRetry,
		Before:  now.Add(-1 * time.Minute),
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("list attempts retry: %v", err)
	}
	if len(resp2.Items) != 1 || resp2.Items[0].ID != "att_1" {
		t.Fatalf("expected att_1 only, got %#v", resp2.Items)
	}
	if resp2.Items[0].StatusCode != 503 {
		t.Fatalf("expected status_code 503, got %d", resp2.Items[0].StatusCode)
	}
}

func TestMemoryStore_RequeueDead(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: now.Add(-time.Minute), DeadReason: "max_retries"}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "dead_2", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: now.Add(-2 * time.Minute), DeadReason: "no_retry"}); err != nil {
		t.Fatalf("enqueue dead_2: %v", err)
	}

	resp, err := s.RequeueDead(DeadRequeueRequest{IDs: []string{"dead_1", "missing", "dead_1"}})
	if err != nil {
		t.Fatalf("requeue dead: %v", err)
	}
	if resp.Requeued != 1 {
		t.Fatalf("expected requeued=1, got %d", resp.Requeued)
	}

	deadResp, err := s.ListDead(DeadListRequest{Route: "/r", Limit: 10})
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(deadResp.Items) != 1 || deadResp.Items[0].ID != "dead_2" {
		t.Fatalf("expected only dead_2 in DLQ, got %#v", deadResp.Items)
	}

	deq, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 10})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	found := false
	for _, it := range deq.Items {
		if it.ID == "dead_1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected dead_1 requeued")
	}
}

func TestMemoryStore_DeleteDead(t *testing.T) {
	s := NewMemoryStore()

	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: StateDead, DeadReason: "max_retries"}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "dead_2", Route: "/r", Target: "pull", State: StateDead, DeadReason: "no_retry"}); err != nil {
		t.Fatalf("enqueue dead_2: %v", err)
	}

	resp, err := s.DeleteDead(DeadDeleteRequest{IDs: []string{"dead_1", "dead_1", "missing"}})
	if err != nil {
		t.Fatalf("delete dead: %v", err)
	}
	if resp.Deleted != 1 {
		t.Fatalf("expected deleted=1, got %d", resp.Deleted)
	}

	deadResp, err := s.ListDead(DeadListRequest{Route: "/r", Limit: 10})
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(deadResp.Items) != 1 || deadResp.Items[0].ID != "dead_2" {
		t.Fatalf("expected only dead_2 in DLQ, got %#v", deadResp.Items)
	}
}

func TestMemoryStore_ListMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: now.Add(-2 * time.Minute), Payload: []byte("a")}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "queued_2", Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: now.Add(-1 * time.Minute), Payload: []byte("b")}); err != nil {
		t.Fatalf("enqueue queued_2: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: now.Add(-30 * time.Second), Payload: []byte("dead")}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}

	resp, err := s.ListMessages(MessageListRequest{
		Route:          "/r",
		State:          StateQueued,
		Limit:          1,
		IncludePayload: true,
	})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "queued_2" {
		t.Fatalf("expected queued_2, got %q", resp.Items[0].ID)
	}
	if string(resp.Items[0].Payload) != "b" {
		t.Fatalf("expected payload for queued_2")
	}
}

func TestMemoryStore_LookupMessages(t *testing.T) {
	s := NewMemoryStore()

	if err := s.Enqueue(Envelope{ID: "queued_1", Route: "/r/queued", Target: "pull", State: StateQueued}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r/dead", Target: "pull", State: StateDead}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}

	resp, err := s.LookupMessages(MessageLookupRequest{IDs: []string{"dead_1", "missing", "queued_1", "dead_1", " "}})
	if err != nil {
		t.Fatalf("lookup messages: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 lookup items, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "dead_1" || resp.Items[0].Route != "/r/dead" || resp.Items[0].State != StateDead {
		t.Fatalf("unexpected first lookup item: %#v", resp.Items[0])
	}
	if resp.Items[1].ID != "queued_1" || resp.Items[1].Route != "/r/queued" || resp.Items[1].State != StateQueued {
		t.Fatalf("unexpected second lookup item: %#v", resp.Items[1])
	}
}

func TestIDMutationTouchesManagedRoute(t *testing.T) {
	s := NewMemoryStore()
	if err := s.Enqueue(Envelope{ID: "managed_queued", Route: "/managed", Target: "pull", State: StateQueued}); err != nil {
		t.Fatalf("enqueue managed_queued: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "managed_dead", Route: "/managed", Target: "pull", State: StateDead}); err != nil {
		t.Fatalf("enqueue managed_dead: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "direct_queued", Route: "/direct", Target: "pull", State: StateQueued}); err != nil {
		t.Fatalf("enqueue direct_queued: %v", err)
	}

	touches, err := IDMutationTouchesManagedRoute(
		s,
		[]string{"direct_queued", "managed_queued"},
		map[State]struct{}{StateQueued: {}},
		map[string]struct{}{"/managed": {}},
	)
	if err != nil {
		t.Fatalf("touches managed route: %v", err)
	}
	if !touches {
		t.Fatalf("expected managed route touch for queued state")
	}

	touches, err = IDMutationTouchesManagedRoute(
		s,
		[]string{"managed_dead"},
		map[State]struct{}{StateQueued: {}},
		map[string]struct{}{"/managed": {}},
	)
	if err != nil {
		t.Fatalf("touches managed route dead-state-filtered: %v", err)
	}
	if touches {
		t.Fatalf("expected no managed route touch when state filter excludes matching item")
	}
}

func TestMemoryStore_ListMessagesOrderAsc(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "queued_2", Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_2: %v", err)
	}

	resp, err := s.ListMessages(MessageListRequest{
		Route: "/r",
		State: StateQueued,
		Order: MessageOrderAsc,
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "queued_1" || resp.Items[1].ID != "queued_2" {
		t.Fatalf("unexpected order: %#v", resp.Items)
	}
}

func TestMemoryStore_ListMessagesOrderInvalid(t *testing.T) {
	s := NewMemoryStore()
	_, err := s.ListMessages(MessageListRequest{Order: "sideways"})
	if err == nil {
		t.Fatalf("expected error for invalid order")
	}
}

func TestMemoryStore_CancelMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: StateQueued}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: StateDead}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "delivered_1", Route: "/r", Target: "pull", State: StateDelivered}); err != nil {
		t.Fatalf("enqueue delivered_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "leased_1", Route: "/r", Target: "pull", State: StateQueued}); err != nil {
		t.Fatalf("enqueue leased_1: %v", err)
	}

	deq, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 1 || deq.Items[0].ID != "queued_1" {
		t.Fatalf("expected queued_1 leased first, got %#v", deq.Items)
	}
	leaseID := deq.Items[0].LeaseID

	resp, err := s.CancelMessages(MessageCancelRequest{
		IDs: []string{"queued_1", "dead_1", "leased_1", "delivered_1", "missing", "queued_1"},
	})
	if err != nil {
		t.Fatalf("cancel messages: %v", err)
	}
	if resp.Canceled != 3 {
		t.Fatalf("expected canceled=3, got %d", resp.Canceled)
	}
	if err := s.Ack(leaseID); err != ErrLeaseNotFound {
		t.Fatalf("expected lease not found after cancel, got %v", err)
	}

	canceledResp, err := s.ListMessages(MessageListRequest{Route: "/r", State: StateCanceled, Limit: 10})
	if err != nil {
		t.Fatalf("list canceled messages: %v", err)
	}
	if len(canceledResp.Items) != 3 {
		t.Fatalf("expected 3 canceled items, got %d", len(canceledResp.Items))
	}

	deq2, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 10})
	if err != nil {
		t.Fatalf("dequeue after cancel: %v", err)
	}
	if len(deq2.Items) != 0 {
		t.Fatalf("expected no queued items after cancel, got %d", len(deq2.Items))
	}
}

func TestMemoryStore_RequeueMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: StateDead, DeadReason: "max_retries"}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "canceled_1", Route: "/r", Target: "pull", State: StateCanceled}); err != nil {
		t.Fatalf("enqueue canceled_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "delivered_1", Route: "/r", Target: "pull", State: StateDelivered}); err != nil {
		t.Fatalf("enqueue delivered_1: %v", err)
	}

	resp, err := s.RequeueMessages(MessageRequeueRequest{
		IDs: []string{"dead_1", "canceled_1", "delivered_1", "missing", "dead_1"},
	})
	if err != nil {
		t.Fatalf("requeue messages: %v", err)
	}
	if resp.Requeued != 2 {
		t.Fatalf("expected requeued=2, got %d", resp.Requeued)
	}

	queuedResp, err := s.ListMessages(MessageListRequest{Route: "/r", State: StateQueued, Limit: 10})
	if err != nil {
		t.Fatalf("list queued messages: %v", err)
	}
	if len(queuedResp.Items) != 2 {
		t.Fatalf("expected 2 queued items, got %d", len(queuedResp.Items))
	}

	for _, item := range queuedResp.Items {
		if item.ID == "dead_1" && item.DeadReason != "" {
			t.Fatalf("expected dead_reason cleared for dead_1, got %q", item.DeadReason)
		}
	}
}

func TestMemoryStore_ResumeMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "canceled_1", Route: "/r", Target: "pull", State: StateCanceled}); err != nil {
		t.Fatalf("enqueue canceled_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: StateDead}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}

	resp, err := s.ResumeMessages(MessageResumeRequest{
		IDs: []string{"canceled_1", "dead_1", "missing"},
	})
	if err != nil {
		t.Fatalf("resume messages: %v", err)
	}
	if resp.Resumed != 1 {
		t.Fatalf("expected resumed=1, got %d", resp.Resumed)
	}

	queuedResp, err := s.ListMessages(MessageListRequest{Route: "/r", State: StateQueued, Limit: 10})
	if err != nil {
		t.Fatalf("list queued messages: %v", err)
	}
	if len(queuedResp.Items) != 1 || queuedResp.Items[0].ID != "canceled_1" {
		t.Fatalf("expected canceled_1 queued, got %#v", queuedResp.Items)
	}
}

func TestMemoryStore_CancelMessagesByFilter(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "q_1", Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: now.Add(-4 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "q_2", Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: now.Add(-3 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_2: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "d_1", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "x_1", Route: "/x", Target: "pull", State: StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue x_1: %v", err)
	}

	resp, err := s.CancelMessagesByFilter(MessageManageFilterRequest{
		Route: "/r",
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("cancel by filter: %v", err)
	}
	if resp.Canceled != 2 {
		t.Fatalf("expected canceled=2, got %d", resp.Canceled)
	}

	canceled, err := s.ListMessages(MessageListRequest{Route: "/r", State: StateCanceled, Limit: 10})
	if err != nil {
		t.Fatalf("list canceled: %v", err)
	}
	if len(canceled.Items) != 2 {
		t.Fatalf("expected 2 canceled items, got %d", len(canceled.Items))
	}
}

func TestMemoryStore_RequeueMessagesByFilter(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "d_1", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: now.Add(-4 * time.Minute), DeadReason: "max_retries"}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "c_1", Route: "/r", Target: "pull", State: StateCanceled, ReceivedAt: now.Add(-3 * time.Minute)}); err != nil {
		t.Fatalf("enqueue c_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "c_2", Route: "/r", Target: "pull", State: StateCanceled, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue c_2: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "q_1", Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}

	resp, err := s.RequeueMessagesByFilter(MessageManageFilterRequest{
		Route:  "/r",
		State:  StateCanceled,
		Before: now.Add(-90 * time.Second),
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("requeue by filter: %v", err)
	}
	if resp.Requeued != 2 {
		t.Fatalf("expected requeued=2, got %d", resp.Requeued)
	}

	queued, err := s.ListMessages(MessageListRequest{Route: "/r", State: StateQueued, Limit: 10})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	if len(queued.Items) != 3 {
		t.Fatalf("expected 3 queued items, got %d", len(queued.Items))
	}
}

func TestMemoryStore_CancelMessagesByFilterPreviewOnly(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "q_1", Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "q_2", Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_2: %v", err)
	}

	resp, err := s.CancelMessagesByFilter(MessageManageFilterRequest{
		Route:       "/r",
		State:       StateQueued,
		Limit:       10,
		PreviewOnly: true,
	})
	if err != nil {
		t.Fatalf("cancel by filter preview: %v", err)
	}
	if resp.PreviewOnly != true || resp.Matched != 2 || resp.Canceled != 0 {
		t.Fatalf("unexpected preview response: %#v", resp)
	}

	queued, err := s.ListMessages(MessageListRequest{Route: "/r", State: StateQueued, Limit: 10})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	if len(queued.Items) != 2 {
		t.Fatalf("expected queue unchanged with 2 items, got %d", len(queued.Items))
	}
}

func TestMemoryStore_RequeueMessagesByFilterPreviewOnly(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "d_1", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "c_1", Route: "/r", Target: "pull", State: StateCanceled, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue c_1: %v", err)
	}

	resp, err := s.RequeueMessagesByFilter(MessageManageFilterRequest{
		Route:       "/r",
		Limit:       10,
		PreviewOnly: true,
	})
	if err != nil {
		t.Fatalf("requeue by filter preview: %v", err)
	}
	if resp.PreviewOnly != true || resp.Matched != 2 || resp.Requeued != 0 {
		t.Fatalf("unexpected preview response: %#v", resp)
	}

	queued, err := s.ListMessages(MessageListRequest{Route: "/r", State: StateQueued, Limit: 10})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	if len(queued.Items) != 0 {
		t.Fatalf("expected queue unchanged with 0 queued items, got %d", len(queued.Items))
	}
}

func TestMemoryStore_ResumeMessagesByFilterPreviewOnly(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "c_1", Route: "/r", Target: "pull", State: StateCanceled, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue c_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "c_2", Route: "/r", Target: "pull", State: StateCanceled, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue c_2: %v", err)
	}

	resp, err := s.ResumeMessagesByFilter(MessageManageFilterRequest{
		Route:       "/r",
		Limit:       10,
		PreviewOnly: true,
	})
	if err != nil {
		t.Fatalf("resume by filter preview: %v", err)
	}
	if resp.PreviewOnly != true || resp.Matched != 2 || resp.Resumed != 0 {
		t.Fatalf("unexpected preview response: %#v", resp)
	}

	queued, err := s.ListMessages(MessageListRequest{Route: "/r", State: StateQueued, Limit: 10})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	if len(queued.Items) != 0 {
		t.Fatalf("expected queue unchanged with 0 queued items, got %d", len(queued.Items))
	}
}

func TestMemoryStore_Stats(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{
		ID:         "q_1",
		Route:      "/r",
		Target:     "pull",
		State:      StateQueued,
		ReceivedAt: now.Add(-2 * time.Minute),
		NextRunAt:  now.Add(-30 * time.Second),
	}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "d_1", Route: "/r", Target: "pull", State: StateDead}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "c_1", Route: "/r", Target: "pull", State: StateCanceled}); err != nil {
		t.Fatalf("enqueue c_1: %v", err)
	}

	stats, err := s.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Total != 3 {
		t.Fatalf("expected total=3, got %d", stats.Total)
	}
	if stats.ByState[StateQueued] != 1 || stats.ByState[StateDead] != 1 || stats.ByState[StateCanceled] != 1 {
		t.Fatalf("unexpected by_state: %#v", stats.ByState)
	}
	if got, want := stats.OldestQueuedReceivedAt, now.Add(-2*time.Minute); !got.Equal(want) {
		t.Fatalf("oldest queued received_at: got %s want %s", got, want)
	}
	if got, want := stats.OldestQueuedAge, 2*time.Minute; got != want {
		t.Fatalf("oldest queued age: got %s want %s", got, want)
	}
	if got, want := stats.EarliestQueuedNextRun, now.Add(-30*time.Second); !got.Equal(want) {
		t.Fatalf("earliest queued next_run_at: got %s want %s", got, want)
	}
	if got, want := stats.ReadyLag, 30*time.Second; got != want {
		t.Fatalf("ready lag: got %s want %s", got, want)
	}
	if len(stats.TopQueued) != 1 {
		t.Fatalf("expected 1 top_queued bucket, got %d", len(stats.TopQueued))
	}
	if got := stats.TopQueued[0]; got.Route != "/r" || got.Target != "pull" || got.Queued != 1 {
		t.Fatalf("unexpected top_queued bucket: %#v", got)
	}
}

func TestMemoryStore_BacklogTrendCaptureAndList(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "q1", Route: "/r1", Target: "pull", State: StateQueued}); err != nil {
		t.Fatalf("enqueue q1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "l1", Route: "/r1", Target: "pull", State: StateLeased}); err != nil {
		t.Fatalf("enqueue l1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "d1", Route: "/r2", Target: "pull", State: StateDead}); err != nil {
		t.Fatalf("enqueue d1: %v", err)
	}

	if err := s.CaptureBacklogTrendSample(nowVar); err != nil {
		t.Fatalf("capture trend #1: %v", err)
	}

	if err := s.Enqueue(Envelope{ID: "q2", Route: "/r1", Target: "https://example.org/hook", State: StateQueued}); err != nil {
		t.Fatalf("enqueue q2: %v", err)
	}
	nowVar = nowVar.Add(1 * time.Minute)
	if err := s.CaptureBacklogTrendSample(nowVar); err != nil {
		t.Fatalf("capture trend #2: %v", err)
	}

	global, err := s.ListBacklogTrend(BacklogTrendListRequest{
		Since: now.Add(-1 * time.Minute),
		Until: now.Add(2 * time.Minute),
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list global trend: %v", err)
	}
	if global.Truncated {
		t.Fatalf("expected global trend not truncated")
	}
	if len(global.Items) != 2 {
		t.Fatalf("expected 2 global trend samples, got %d", len(global.Items))
	}
	if global.Items[0].Queued != 1 || global.Items[0].Leased != 1 || global.Items[0].Dead != 1 {
		t.Fatalf("unexpected global sample #1: %#v", global.Items[0])
	}
	if global.Items[1].Queued != 2 || global.Items[1].Leased != 1 || global.Items[1].Dead != 1 {
		t.Fatalf("unexpected global sample #2: %#v", global.Items[1])
	}

	routeOnly, err := s.ListBacklogTrend(BacklogTrendListRequest{
		Route: "/r1",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list route trend: %v", err)
	}
	if len(routeOnly.Items) != 2 {
		t.Fatalf("expected 2 route trend samples, got %d", len(routeOnly.Items))
	}
	if routeOnly.Items[0].Queued != 1 || routeOnly.Items[0].Leased != 1 || routeOnly.Items[0].Dead != 0 {
		t.Fatalf("unexpected route sample #1: %#v", routeOnly.Items[0])
	}
	if routeOnly.Items[1].Queued != 2 || routeOnly.Items[1].Leased != 1 || routeOnly.Items[1].Dead != 0 {
		t.Fatalf("unexpected route sample #2: %#v", routeOnly.Items[1])
	}

	truncated, err := s.ListBacklogTrend(BacklogTrendListRequest{
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("list truncated trend: %v", err)
	}
	if !truncated.Truncated {
		t.Fatalf("expected truncated trend response")
	}
	if len(truncated.Items) != 1 {
		t.Fatalf("expected 1 truncated trend item, got %d", len(truncated.Items))
	}
	if got, want := truncated.Items[0].CapturedAt, now.Add(1*time.Minute); !got.Equal(want) {
		t.Fatalf("unexpected truncated sample timestamp: got %s want %s", got, want)
	}
}

func TestMemoryStore_DeliveredRetentionPrunesDelivered(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(
		WithNowFunc(func() time.Time { return nowVar }),
		WithQueueRetention(0, 1*time.Second),
		WithDeliveredRetention(2*time.Second),
	)

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item")
	}
	if err := s.Ack(resp.Items[0].LeaseID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	nowVar = nowVar.Add(3 * time.Second)
	_, _ = s.Dequeue(DequeueRequest{Route: "/r", Target: "pull"})

	if _, ok := s.items["evt_1"]; ok {
		t.Fatalf("expected delivered item pruned")
	}
}

func TestMemoryStore_Extend(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/pull/github", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	resp, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", LeaseTTL: 10 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	leaseID := resp.Items[0].LeaseID

	if err := s.Extend(leaseID, 5*time.Second); err != nil {
		t.Fatalf("extend: %v", err)
	}

	nowVar = nowVar.Add(12 * time.Second)
	if err := s.Ack(leaseID); err != nil {
		t.Fatalf("ack after extend: %v", err)
	}
}

func TestMemoryStore_ExtendUnknownLease(t *testing.T) {
	s := NewMemoryStore()
	err := s.Extend("nonexistent-lease", 5*time.Second)
	if !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("expected ErrLeaseNotFound, got %v", err)
	}
}

func TestMemoryStore_ExtendExpiredLease(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(WithNowFunc(func() time.Time { return nowVar }))

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", LeaseTTL: 5 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	leaseID := resp.Items[0].LeaseID

	// Advance past lease expiry
	nowVar = nowVar.Add(10 * time.Second)
	err = s.Extend(leaseID, 5*time.Second)
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
}

func TestMemoryStore_ExtendZeroIsNoop(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	s := NewMemoryStore(WithNowFunc(func() time.Time { return now }))

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", LeaseTTL: 10 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	leaseID := resp.Items[0].LeaseID

	// Extend by zero should be a no-op (no error, lease unchanged)
	if err := s.Extend(leaseID, 0); err != nil {
		t.Fatalf("extend by zero: %v", err)
	}
}

func TestMemoryStore_DequeueLongPollWakesOnEnqueue(t *testing.T) {
	s := NewMemoryStore()

	done := make(chan DequeueResponse, 1)
	go func() {
		resp, _ := s.Dequeue(DequeueRequest{
			Route:   "/pull/github",
			Target:  "pull",
			Batch:   1,
			MaxWait: 200 * time.Millisecond,
		})
		done <- resp
	}()

	time.Sleep(20 * time.Millisecond)
	_ = s.Enqueue(Envelope{ID: "evt_1", Route: "/pull/github", Target: "pull"})

	resp := <-done
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
}

func TestMemoryStore_RetentionPrunesQueued(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(
		WithNowFunc(func() time.Time { return nowVar }),
		WithQueueRetention(2*time.Second, 1*time.Second),
	)

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}

	nowVar = nowVar.Add(3 * time.Second)
	if err := s.Enqueue(Envelope{ID: "evt_2", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue2: %v", err)
	}

	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull"})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "evt_2" {
		t.Fatalf("expected evt_2, got %#v", resp.Items)
	}
}

func TestMemoryStore_DLQRetentionPrunesDead(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(
		WithNowFunc(func() time.Time { return nowVar }),
		WithQueueRetention(0, 1*time.Second),
		WithDLQRetention(2*time.Second, 1),
	)

	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: nowVar}); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}

	nowVar = nowVar.Add(3 * time.Second)
	if err := s.Enqueue(Envelope{ID: "dead_2", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: nowVar}); err != nil {
		t.Fatalf("enqueue2: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items["dead_1"]; ok {
		t.Fatalf("expected dead_1 pruned")
	}
	if _, ok := s.items["dead_2"]; !ok {
		t.Fatalf("expected dead_2 to remain")
	}
}

func TestMemoryStore_QueueLimitsReject(t *testing.T) {
	s := NewMemoryStore(WithQueueLimits(1, "reject"))
	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "evt_2", Route: "/r", Target: "pull"}); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestMemoryStore_EnqueueDuplicateID(t *testing.T) {
	s := NewMemoryStore()
	if err := s.Enqueue(Envelope{ID: "evt_dup", Route: "/r", Target: "pull", Payload: []byte("first")}); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "evt_dup", Route: "/r", Target: "pull", Payload: []byte("second")}); err != ErrEnvelopeExists {
		t.Fatalf("expected ErrEnvelopeExists, got %v", err)
	}

	resp, err := s.ListMessages(MessageListRequest{
		Route:          "/r",
		Target:         "pull",
		State:          StateQueued,
		Limit:          10,
		IncludePayload: true,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	if got := string(resp.Items[0].Payload); got != "first" {
		t.Fatalf("expected payload to remain first, got %q", got)
	}
}

func TestMemoryStore_QueueLimitsDropOldest(t *testing.T) {
	s := NewMemoryStore(WithQueueLimits(1, "drop_oldest"))
	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "evt_2", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue2: %v", err)
	}
	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull"})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "evt_2" {
		t.Fatalf("expected evt_2, got %#v", resp.Items)
	}
}

func TestMemoryStore_RuntimeMetrics(t *testing.T) {
	now := time.Date(2026, 2, 13, 12, 0, 0, 0, time.UTC)
	s := NewMemoryStore(
		WithNowFunc(func() time.Time { return now }),
		WithQueueLimits(1, "drop_oldest"),
	)

	if err := s.Enqueue(Envelope{
		ID:      "evt_1",
		Route:   "/r",
		Target:  "pull",
		Payload: []byte("alpha"),
		Headers: map[string]string{"X-Test": "1"},
	}); err != nil {
		t.Fatalf("enqueue evt_1: %v", err)
	}
	if err := s.Enqueue(Envelope{
		ID:     "dead_1",
		Route:  "/r",
		Target: "pull",
		State:  StateDead,
	}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := s.Enqueue(Envelope{
		ID:      "evt_2",
		Route:   "/r",
		Target:  "pull",
		Payload: []byte("beta"),
	}); err != nil {
		t.Fatalf("enqueue evt_2: %v", err)
	}

	rm := s.RuntimeMetrics()
	if rm.Backend != "memory" {
		t.Fatalf("backend: got %q, want memory", rm.Backend)
	}
	if rm.Memory == nil {
		t.Fatalf("expected memory runtime metrics")
	}
	if rm.Memory.ItemsByState[StateQueued] != 1 {
		t.Fatalf("expected queued item count=1, got %d", rm.Memory.ItemsByState[StateQueued])
	}
	if rm.Memory.ItemsByState[StateDead] != 1 {
		t.Fatalf("expected dead item count=1, got %d", rm.Memory.ItemsByState[StateDead])
	}
	if rm.Memory.RetainedBytesTotal <= 0 {
		t.Fatalf("expected retained bytes total > 0")
	}
	if rm.Memory.EvictionsTotalByReason[memoryEvictionReasonDropOldest] != 1 {
		t.Fatalf("expected drop_oldest eviction count=1, got %d", rm.Memory.EvictionsTotalByReason[memoryEvictionReasonDropOldest])
	}
}

func TestMemoryStore_MemoryPressureRejectsEnqueue(t *testing.T) {
	s := NewMemoryStore(
		WithQueueLimits(10, "reject"),
		WithMemoryPressureLimits(1, 1<<30),
	)
	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: StateDead}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); !errors.Is(err, ErrMemoryPressure) {
		t.Fatalf("expected ErrMemoryPressure, got %v", err)
	}

	rm := s.RuntimeMetrics()
	if rm.Memory == nil {
		t.Fatalf("expected memory runtime metrics")
	}
	if !rm.Memory.Pressure.Active {
		t.Fatalf("expected memory pressure active=true")
	}
	if rm.Memory.Pressure.Reason != "retained_items" {
		t.Fatalf("expected memory pressure reason retained_items, got %q", rm.Memory.Pressure.Reason)
	}
	if rm.Memory.Pressure.RejectTotal != 1 {
		t.Fatalf("expected reject_total=1, got %d", rm.Memory.Pressure.RejectTotal)
	}
}

// TestMemoryStore_MaxDepthCountsOnlyActive verifies that max_depth counts only
// queued+leased items, matching SQLite semantics. Dead/delivered/canceled items
// must not consume depth budget.
func TestMemoryStore_MaxDepthCountsOnlyActive(t *testing.T) {
	s := NewMemoryStore(WithQueueLimits(2, "reject"))

	// Fill to max_depth.
	if err := s.Enqueue(Envelope{ID: "a", Route: "/r", Target: "t"}); err != nil {
		t.Fatalf("enqueue a: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "b", Route: "/r", Target: "t"}); err != nil {
		t.Fatalf("enqueue b: %v", err)
	}

	// Third enqueue should be rejected (queue full).
	if err := s.Enqueue(Envelope{ID: "c", Route: "/r", Target: "t"}); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	// Dequeue item "a" to get a lease, then mark it dead — frees one active slot.
	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "t", Batch: 1, LeaseTTL: time.Minute})
	if err != nil || len(resp.Items) != 1 {
		t.Fatalf("dequeue: err=%v items=%d", err, len(resp.Items))
	}
	if err := s.MarkDead(resp.Items[0].LeaseID, "test-failure"); err != nil {
		t.Fatalf("mark dead: %v", err)
	}

	// Now enqueue should succeed because only "b" is active (queued).
	if err := s.Enqueue(Envelope{ID: "c", Route: "/r", Target: "t"}); err != nil {
		t.Fatalf("enqueue c after dead: %v", err)
	}

	// Verify stats: 2 active (queued), 1 dead.
	stats, _ := s.Stats()
	if stats.ByState[StateQueued] != 2 {
		t.Fatalf("expected 2 queued, got %d", stats.ByState[StateQueued])
	}
	if stats.ByState[StateDead] != 1 {
		t.Fatalf("expected 1 dead, got %d", stats.ByState[StateDead])
	}
}

func TestMemoryStore_DeliveredRetentionRespectsMaxDepth(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(
		WithNowFunc(func() time.Time { return nowVar }),
		WithQueueLimits(2, "reject"),
		WithQueueRetention(0, 1*time.Second),
		WithDeliveredRetention(time.Hour),
	)

	if err := s.Enqueue(Envelope{ID: "a", Route: "/r", Target: "t"}); err != nil {
		t.Fatalf("enqueue a: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "b", Route: "/r", Target: "t"}); err != nil {
		t.Fatalf("enqueue b: %v", err)
	}

	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "t", Batch: 2, LeaseTTL: time.Minute})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 dequeued items, got %d", len(resp.Items))
	}
	for _, item := range resp.Items {
		if err := s.Ack(item.LeaseID); err != nil {
			t.Fatalf("ack %s: %v", item.ID, err)
		}
	}

	if err := s.Enqueue(Envelope{ID: "c", Route: "/r", Target: "t"}); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull while delivered retention is at max_depth, got %v", err)
	}

	stats, err := s.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ByState[StateDelivered] != 2 {
		t.Fatalf("expected 2 delivered, got %d", stats.ByState[StateDelivered])
	}
	if stats.ByState[StateQueued] != 0 {
		t.Fatalf("expected 0 queued, got %d", stats.ByState[StateQueued])
	}

	nowVar = nowVar.Add(2 * time.Hour)
	_, _ = s.Dequeue(DequeueRequest{Route: "/r", Target: "t"})

	if err := s.Enqueue(Envelope{ID: "c", Route: "/r", Target: "t"}); err != nil {
		t.Fatalf("enqueue c after retention prune: %v", err)
	}
}

func TestMemoryStore_EnqueueBatch_DeliveredRetentionRespectsMaxDepth(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := NewMemoryStore(
		WithNowFunc(func() time.Time { return nowVar }),
		WithQueueLimits(3, "reject"),
		WithQueueRetention(0, 1*time.Second),
		WithDeliveredRetention(time.Hour),
	)

	for _, id := range []string{"a", "b", "c"} {
		if err := s.Enqueue(Envelope{ID: id, Route: "/r", Target: "t"}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "t", Batch: 3, LeaseTTL: time.Minute})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 3 {
		t.Fatalf("expected 3 dequeued items, got %d", len(resp.Items))
	}
	for _, item := range resp.Items {
		if err := s.Ack(item.LeaseID); err != nil {
			t.Fatalf("ack %s: %v", item.ID, err)
		}
	}

	n, err := s.EnqueueBatch([]Envelope{
		{ID: "d", Route: "/r", Target: "t"},
		{ID: "e", Route: "/r", Target: "t"},
	})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got n=%d err=%v", n, err)
	}
	if n != 0 {
		t.Fatalf("expected 0 committed, got %d", n)
	}
}

func TestMemoryStore_EnqueueBatch_AllOrNothing(t *testing.T) {
	s := NewMemoryStore(WithQueueLimits(2, "reject"))

	// Batch of 3 items with depth limit 2 → all rejected (none committed).
	items := []Envelope{
		{ID: "a", Route: "/r", Target: "t"},
		{ID: "b", Route: "/r", Target: "t"},
		{ID: "c", Route: "/r", Target: "t"},
	}
	n, err := s.EnqueueBatch(items)
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got n=%d err=%v", n, err)
	}
	if n != 0 {
		t.Fatalf("expected 0 committed, got %d", n)
	}

	// Queue should be empty — nothing was committed.
	stats, _ := s.Stats()
	if stats.Total != 0 {
		t.Fatalf("expected empty queue after failed batch, got total=%d", stats.Total)
	}

	// Batch of 2 items that fits → all committed.
	items2 := []Envelope{
		{ID: "a", Route: "/r", Target: "t"},
		{ID: "b", Route: "/r", Target: "t"},
	}
	n, err = s.EnqueueBatch(items2)
	if err != nil {
		t.Fatalf("batch enqueue: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 committed, got %d", n)
	}

	stats, _ = s.Stats()
	if stats.ByState[StateQueued] != 2 {
		t.Fatalf("expected 2 queued, got %d", stats.ByState[StateQueued])
	}
}

func TestMemoryStore_EnqueueBatch_DuplicateID(t *testing.T) {
	s := NewMemoryStore()

	// Pre-existing item.
	if err := s.Enqueue(Envelope{ID: "existing", Route: "/r", Target: "t"}); err != nil {
		t.Fatalf("pre-enqueue: %v", err)
	}

	// Batch with duplicate → no items committed.
	items := []Envelope{
		{ID: "new1", Route: "/r", Target: "t"},
		{ID: "existing", Route: "/r", Target: "t"},
	}
	n, err := s.EnqueueBatch(items)
	if err != ErrEnvelopeExists {
		t.Fatalf("expected ErrEnvelopeExists, got n=%d err=%v", n, err)
	}

	// Only the pre-existing item should be in the queue.
	stats, _ := s.Stats()
	if stats.Total != 1 {
		t.Fatalf("expected 1 item (pre-existing only), got %d", stats.Total)
	}
}
