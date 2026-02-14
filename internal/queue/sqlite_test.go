package queue

import (
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSQLiteStore_JournalModeIsWAL(t *testing.T) {
	s := newSQLiteStoreForTest(t, time.Now)

	var mode string
	if err := s.db.QueryRow(`PRAGMA journal_mode;`).Scan(&mode); err != nil {
		t.Fatalf("pragma journal_mode: %v", err)
	}
	if strings.ToLower(mode) != "wal" {
		t.Fatalf("journal_mode=%q, want wal", mode)
	}
}

func TestSQLiteStore_SchemaVersionRecorded(t *testing.T) {
	s := newSQLiteStoreForTest(t, time.Now)

	var v int
	if err := s.db.QueryRow(`SELECT version FROM schema_migrations LIMIT 1;`).Scan(&v); err != nil {
		t.Fatalf("schema_version: %v", err)
	}
	if v != schemaVersion {
		t.Fatalf("schema_version=%d, want %d", v, schemaVersion)
	}
}

func TestSQLiteStore_EnqueueDequeueAck(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_LeaseExpiryRequeues(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_NackDelay(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_MarkDead(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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
	leaseID := resp.Items[0].LeaseID

	if err := s.MarkDead(leaseID, "no_retry"); err != nil {
		t.Fatalf("mark dead: %v", err)
	}

	var state string
	var leaseIDDB *string
	var deadReason sql.NullString
	if err := s.db.QueryRow(`SELECT state, lease_id, dead_reason FROM queue_items WHERE id = ?;`, "evt_1").Scan(&state, &leaseIDDB, &deadReason); err != nil {
		t.Fatalf("query: %v", err)
	}
	if state != string(StateDead) {
		t.Fatalf("state=%q, want %q", state, StateDead)
	}
	if leaseIDDB != nil {
		t.Fatalf("expected lease_id NULL, got %q", *leaseIDDB)
	}
	if !deadReason.Valid || deadReason.String != "no_retry" {
		t.Fatalf("dead_reason=%q, want %q", deadReason.String, "no_retry")
	}
}

func TestSQLiteStore_LeaseBatchMethods(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

	for _, id := range []string{"evt_1", "evt_2", "evt_3"} {
		if err := s.Enqueue(Envelope{ID: id, Route: "/pull/github", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	deq, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", Batch: 2, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 2 {
		t.Fatalf("expected 2 dequeued items, got %d", len(deq.Items))
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

	deq2, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", Batch: 1, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if len(deq2.Items) != 1 {
		t.Fatalf("expected 1 dequeued item, got %d", len(deq2.Items))
	}

	nackRes, err := s.NackBatch([]string{deq2.Items[0].LeaseID}, 5*time.Second)
	if err != nil {
		t.Fatalf("nack batch: %v", err)
	}
	if nackRes.Succeeded != 1 || len(nackRes.Conflicts) != 0 {
		t.Fatalf("unexpected nack batch result: %#v", nackRes)
	}

	deq3, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue3: %v", err)
	}
	if len(deq3.Items) != 0 {
		t.Fatalf("expected no items before nack delay elapsed, got %d", len(deq3.Items))
	}

	nowVar = nowVar.Add(5 * time.Second)
	deq4, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", Batch: 1, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue4: %v", err)
	}
	if len(deq4.Items) != 1 {
		t.Fatalf("expected 1 dequeued item after delay, got %d", len(deq4.Items))
	}

	deadRes, err := s.MarkDeadBatch([]string{deq4.Items[0].LeaseID, "missing_dead"}, "no_retry")
	if err != nil {
		t.Fatalf("mark dead batch: %v", err)
	}
	if deadRes.Succeeded != 1 {
		t.Fatalf("expected 1 successful dead mark, got %d", deadRes.Succeeded)
	}
	if len(deadRes.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %#v", deadRes.Conflicts)
	}
	if deadRes.Conflicts[0].LeaseID != "missing_dead" || deadRes.Conflicts[0].Expired {
		t.Fatalf("unexpected conflict: %#v", deadRes.Conflicts[0])
	}

	var state string
	var leaseIDDB *string
	var deadReason sql.NullString
	if err := s.db.QueryRow(`SELECT state, lease_id, dead_reason FROM queue_items WHERE id = ?;`, deq4.Items[0].ID).Scan(&state, &leaseIDDB, &deadReason); err != nil {
		t.Fatalf("query dead item: %v", err)
	}
	if state != string(StateDead) {
		t.Fatalf("state=%q, want %q", state, StateDead)
	}
	if leaseIDDB != nil {
		t.Fatalf("expected lease_id NULL, got %q", *leaseIDDB)
	}
	if !deadReason.Valid || deadReason.String != "no_retry" {
		t.Fatalf("dead_reason=%q, want %q", deadReason.String, "no_retry")
	}
}

func TestSQLiteStore_AckBatchExpiredRequeues(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/pull/github", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deq, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", Batch: 1, LeaseTTL: time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("expected 1 dequeued item, got %d", len(deq.Items))
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

	deq2, err := s.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", Batch: 1, LeaseTTL: time.Second})
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

func TestSQLiteStore_ListDead(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: now.Add(-2 * time.Minute), Payload: []byte("a")}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "dead_2", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: now.Add(-1 * time.Minute), Payload: []byte("b")}); err != nil {
		t.Fatalf("enqueue dead_2: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "dead_3", Route: "/other", Target: "pull", State: StateDead, ReceivedAt: now.Add(-30 * time.Second), Payload: []byte("c")}); err != nil {
		t.Fatalf("enqueue dead_3: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: now, Payload: []byte("q")}); err != nil {
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

func TestSQLiteStore_RecordAndListAttempts(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_RequeueDead(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_DeleteDead(t *testing.T) {
	s := newSQLiteStoreForTest(t, time.Now)

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

func TestSQLiteStore_ListMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_LookupMessages(t *testing.T) {
	s := newSQLiteStoreForTest(t, time.Now)

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

func TestSQLiteStore_ListMessagesOrderAsc(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_ListMessagesOrderInvalid(t *testing.T) {
	s := newSQLiteStoreForTest(t, time.Now)
	_, err := s.ListMessages(MessageListRequest{Order: "sideways"})
	if err == nil {
		t.Fatalf("expected error for invalid order")
	}
}

func TestSQLiteStore_CancelMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_RequeueMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_ResumeMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_CancelMessagesByFilter(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_RequeueMessagesByFilter(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_CancelMessagesByFilterPreviewOnly(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_RequeueMessagesByFilterPreviewOnly(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_ResumeMessagesByFilterPreviewOnly(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_Stats(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_BacklogTrendCaptureAndList(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

	truncated, err := s.ListBacklogTrend(BacklogTrendListRequest{Limit: 1})
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

func TestSQLiteStore_DeliveredRetentionPrunesDelivered(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(
		dbPath,
		WithSQLiteNowFunc(func() time.Time { return nowVar }),
		WithSQLiteRetention(0, 1*time.Second),
		WithSQLiteDeliveredRetention(2*time.Second),
	)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	if err := s.Ack(resp.Items[0].LeaseID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	nowVar = nowVar.Add(3 * time.Second)
	_, _ = s.Dequeue(DequeueRequest{Route: "/r", Target: "pull"})

	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM queue_items WHERE state = ?;`, string(StateDelivered)).Scan(&count); err != nil {
		t.Fatalf("count delivered: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected delivered item pruned, got %d", count)
	}
}

func TestSQLiteStore_Extend(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

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

func TestSQLiteStore_ExtendZeroIsNoop(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	s := newSQLiteStoreForTest(t, func() time.Time { return now })

	if err := s.Enqueue(Envelope{ID: "evt_z", Route: "/pull/zero", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	resp, err := s.Dequeue(DequeueRequest{Route: "/pull/zero", Target: "pull", LeaseTTL: 10 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	leaseID := resp.Items[0].LeaseID

	if err := s.Extend(leaseID, 0); err != nil {
		t.Fatalf("extend(0) should noop but got: %v", err)
	}
	if err := s.Extend(leaseID, -5*time.Second); err != nil {
		t.Fatalf("extend(-5s) should noop but got: %v", err)
	}
	// Ack still works — lease was not modified.
	if err := s.Ack(leaseID); err != nil {
		t.Fatalf("ack after zero extend: %v", err)
	}
}

func TestSQLiteStore_DequeueLongPollWakesOnEnqueue(t *testing.T) {
	s := newSQLiteStoreForTest(t, time.Now)

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

func TestSQLiteStore_RetentionPrunesQueued(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(
		dbPath,
		WithSQLiteNowFunc(func() time.Time { return nowVar }),
		WithSQLiteRetention(2*time.Second, 1*time.Second),
	)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

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

func TestSQLiteStore_RecoveryAfterRestart(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")

	s1, err := NewSQLiteStore(dbPath, WithSQLiteNowFunc(func() time.Time { return nowVar }))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := s1.Enqueue(Envelope{ID: "evt_1", Route: "/pull/github", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	resp, err := s1.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", LeaseTTL: 10 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	nowVar = nowVar.Add(11 * time.Second)
	s2, err := NewSQLiteStore(dbPath, WithSQLiteNowFunc(func() time.Time { return nowVar }))
	if err != nil {
		t.Fatalf("new store2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	resp2, err := s2.Dequeue(DequeueRequest{Route: "/pull/github", Target: "pull", LeaseTTL: 10 * time.Second})
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if len(resp2.Items) != 1 {
		t.Fatalf("expected 1 item after restart, got %d", len(resp2.Items))
	}
	if resp2.Items[0].Attempt != 2 {
		t.Fatalf("expected attempt=2 after re-lease, got %d", resp2.Items[0].Attempt)
	}
}

func TestSQLiteStore_DLQRetentionPrunesDead(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(
		dbPath,
		WithSQLiteNowFunc(func() time.Time { return nowVar }),
		WithSQLiteRetention(0, 1*time.Second),
		WithSQLiteDLQRetention(2*time.Second, 1),
	)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Enqueue(Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: nowVar}); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}

	nowVar = nowVar.Add(3 * time.Second)
	if err := s.Enqueue(Envelope{ID: "dead_2", Route: "/r", Target: "pull", State: StateDead, ReceivedAt: nowVar}); err != nil {
		t.Fatalf("enqueue2: %v", err)
	}

	_, _ = s.Dequeue(DequeueRequest{Route: "/r", Target: "pull"})

	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM queue_items WHERE state = ?;`, string(StateDead)).Scan(&count); err != nil {
		t.Fatalf("count dead: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 dead item, got %d", count)
	}
}

func TestSQLiteStore_QueueLimitsReject(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(dbPath, WithSQLiteQueueLimits(1, "reject"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "evt_2", Route: "/r", Target: "pull"}); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestSQLiteStore_EnqueueDuplicateID(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

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

func TestSQLiteStore_QueueLimitsDropOldest(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(dbPath, WithSQLiteQueueLimits(1, "drop_oldest"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

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

func TestSQLiteStore_QueueCountersTrackActiveDepth(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	s := newSQLiteStoreForTest(t, func() time.Time { return nowVar })

	mustCounters := func(wantQueued, wantLeased int) {
		t.Helper()
		var queued int
		var leased int
		if err := s.db.QueryRow(`SELECT queued, leased FROM queue_counters WHERE id = 1;`).Scan(&queued, &leased); err != nil {
			t.Fatalf("queue_counters query: %v", err)
		}
		if queued != wantQueued || leased != wantLeased {
			t.Fatalf("queue_counters: got queued=%d leased=%d, want queued=%d leased=%d", queued, leased, wantQueued, wantLeased)
		}
	}

	mustCounters(0, 0)

	if err := s.Enqueue(Envelope{ID: "q1", Route: "/r", Target: "pull", State: StateQueued}); err != nil {
		t.Fatalf("enqueue q1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "d1", Route: "/r", Target: "pull", State: StateDead}); err != nil {
		t.Fatalf("enqueue d1: %v", err)
	}
	if err := s.Enqueue(Envelope{ID: "q2", Route: "/r", Target: "pull", State: StateQueued}); err != nil {
		t.Fatalf("enqueue q2: %v", err)
	}
	mustCounters(2, 0)

	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 dequeued item, got %d", len(resp.Items))
	}
	leaseID := resp.Items[0].LeaseID
	mustCounters(1, 1)

	if err := s.Nack(leaseID, 0); err != nil {
		t.Fatalf("nack: %v", err)
	}
	mustCounters(2, 0)

	resp2, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 2, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if len(resp2.Items) != 2 {
		t.Fatalf("expected 2 dequeued items, got %d", len(resp2.Items))
	}
	mustCounters(0, 2)

	if err := s.Ack(resp2.Items[0].LeaseID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	mustCounters(0, 1)

	if err := s.MarkDead(resp2.Items[1].LeaseID, "no_retry"); err != nil {
		t.Fatalf("mark dead: %v", err)
	}
	mustCounters(0, 0)
}

func TestSQLiteStore_RuntimeMetrics(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(
		dbPath,
		WithSQLiteNowFunc(func() time.Time { return nowVar }),
		WithSQLiteQueueLimits(10000, "reject"),
		WithSQLiteCheckpointInterval(0),
	)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Enqueue(Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 1, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 dequeued item, got %d", len(resp.Items))
	}
	if err := s.Ack(resp.Items[0].LeaseID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := s.checkpointPassive(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	rm := s.RuntimeMetrics()
	if rm.Backend != "sqlite" {
		t.Fatalf("backend: got %q, want sqlite", rm.Backend)
	}
	if rm.SQLite == nil {
		t.Fatalf("expected sqlite runtime metrics")
	}
	if rm.SQLite.WriteDurationSeconds.Count == 0 {
		t.Fatalf("expected write histogram count > 0")
	}
	if rm.SQLite.DequeueDurationSeconds.Count == 0 {
		t.Fatalf("expected dequeue histogram count > 0")
	}
	if rm.SQLite.CheckpointDurationSeconds.Count == 0 {
		t.Fatalf("expected checkpoint histogram count > 0")
	}
	if rm.SQLite.TxCommitTotal == 0 {
		t.Fatalf("expected tx commit counter > 0")
	}
	if rm.SQLite.CheckpointTotal != 1 {
		t.Fatalf("expected checkpoint_total=1, got %d", rm.SQLite.CheckpointTotal)
	}
	if len(rm.SQLite.WriteDurationSeconds.Buckets) == 0 {
		t.Fatalf("expected write histogram buckets")
	}
	last := rm.SQLite.WriteDurationSeconds.Buckets[len(rm.SQLite.WriteDurationSeconds.Buckets)-1]
	if !math.IsInf(last.Le, 1) {
		t.Fatalf("expected +Inf as last bucket upper bound, got %v", last.Le)
	}
}

func newSQLiteStoreForTest(t *testing.T, nowFn func() time.Time) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(dbPath, WithSQLiteNowFunc(nowFn), WithSQLitePollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// --- WAL recovery and resilience tests ---

func TestSQLiteStore_CrashRecovery_NoClose(t *testing.T) {
	// Simulate crash: open DB, enqueue items, abandon store (no Close), reopen.
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")

	s1, err := NewSQLiteStore(dbPath, WithSQLiteNowFunc(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	for i := 0; i < 10; i++ {
		err := s1.Enqueue(Envelope{
			ID: fmt.Sprintf("evt_%d", i), Route: "/r", Target: "pull",
			State: StateQueued, ReceivedAt: now, Payload: []byte("data"),
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	// Intentionally NOT calling s1.Close() — simulates process crash.
	// Deferred close releases the file handle for TempDir cleanup on Windows.
	t.Cleanup(func() { _ = s1.Close() })

	// Reopen — WAL recovery should replay committed data.
	s2, err := NewSQLiteStore(dbPath, WithSQLiteNowFunc(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	resp, err := s2.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 20, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue after crash: %v", err)
	}
	if len(resp.Items) != 10 {
		t.Fatalf("expected 10 items after crash recovery, got %d", len(resp.Items))
	}
}

func TestSQLiteStore_CrashRecovery_LeasedItemsRequeued(t *testing.T) {
	// Items leased when the process crashed should become re-dequeueable after lease expiry.
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")

	s1, err := NewSQLiteStore(dbPath, WithSQLiteNowFunc(func() time.Time { return nowVar }))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	for i := 0; i < 5; i++ {
		_ = s1.Enqueue(Envelope{ID: fmt.Sprintf("evt_%d", i), Route: "/r", Target: "pull", State: StateQueued, ReceivedAt: nowVar})
	}
	_, _ = s1.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 5, LeaseTTL: 10 * time.Second})
	// All 5 items are now leased. Crash (no Close).
	// Deferred close releases the file handle for TempDir cleanup on Windows.
	t.Cleanup(func() { _ = s1.Close() })

	// Advance past lease expiry and reopen.
	nowVar = nowVar.Add(15 * time.Second)
	s2, err := NewSQLiteStore(dbPath, WithSQLiteNowFunc(func() time.Time { return nowVar }))
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	resp, err := s2.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 10, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue after crash: %v", err)
	}
	if len(resp.Items) != 5 {
		t.Fatalf("expected 5 re-leased items after crash, got %d", len(resp.Items))
	}
	for _, it := range resp.Items {
		if it.Attempt != 2 {
			t.Fatalf("expected attempt=2 for re-leased item %s, got %d", it.ID, it.Attempt)
		}
	}
}

func TestSQLiteStore_ConcurrentEnqueueDequeue(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(dbPath, WithSQLitePollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const N = 100
	var wg sync.WaitGroup

	// N producers enqueue concurrently.
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := s.Enqueue(Envelope{
				ID: fmt.Sprintf("evt_%d", id), Route: "/r", Target: "pull",
				State: StateQueued, Payload: []byte("p"),
			})
			if err != nil {
				t.Errorf("enqueue %d: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	// Drain all items via dequeue.
	seen := make(map[string]bool)
	for {
		resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 20, LeaseTTL: 30 * time.Second})
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		for _, it := range resp.Items {
			if seen[it.ID] {
				t.Fatalf("duplicate item dequeued: %s", it.ID)
			}
			seen[it.ID] = true
		}
		if len(resp.Items) == 0 {
			break
		}
	}
	if len(seen) != N {
		t.Fatalf("expected %d unique items, got %d", N, len(seen))
	}
}

func TestSQLiteStore_ConcurrentEnqueueDequeueStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(dbPath, WithSQLitePollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const producers = 10
	const itemsPerProducer = 50
	const consumers = 5

	var prodWg sync.WaitGroup
	for p := 0; p < producers; p++ {
		prodWg.Add(1)
		go func(pid int) {
			defer prodWg.Done()
			for i := 0; i < itemsPerProducer; i++ {
				_ = s.Enqueue(Envelope{
					ID: fmt.Sprintf("evt_p%d_%d", pid, i), Route: "/r", Target: "pull",
					State: StateQueued, Payload: []byte("stress"),
				})
			}
		}(p)
	}

	var consWg sync.WaitGroup
	acked := make(chan string, producers*itemsPerProducer)
	stop := make(chan struct{})
	for c := 0; c < consumers; c++ {
		consWg.Add(1)
		go func() {
			defer consWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				resp, err := s.Dequeue(DequeueRequest{Route: "/r", Target: "pull", Batch: 5, LeaseTTL: 5 * time.Second})
				if err != nil {
					continue
				}
				for _, it := range resp.Items {
					// Count only successful acks; expired/missing leases can be redelivered.
					if err := s.Ack(it.LeaseID); err != nil {
						continue
					}
					select {
					case acked <- it.ID:
					case <-stop:
						return
					}
				}
				if len(resp.Items) == 0 {
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()
	}

	prodWg.Wait()

	// Wait for consumers to drain all items (poll-based instead of fixed sleep).
	deadline := time.After(30 * time.Second)
	for len(acked) < producers*itemsPerProducer {
		select {
		case <-deadline:
			close(stop)
			consWg.Wait()
			close(acked)
			t.Fatalf("timeout: only %d of %d items acked", len(acked), producers*itemsPerProducer)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	close(stop)
	consWg.Wait()
	close(acked)

	ackedSet := make(map[string]bool)
	for id := range acked {
		ackedSet[id] = true
	}

	if len(ackedSet) != producers*itemsPerProducer {
		t.Fatalf("expected %d acked items, got %d", producers*itemsPerProducer, len(ackedSet))
	}
}

func TestSQLiteStore_IntegrityCheckAfterStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(dbPath, WithSQLitePollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	// Write a burst of items concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = s.Enqueue(Envelope{
				ID: fmt.Sprintf("evt_%d", id), Route: "/r", Target: "pull",
				State: StateQueued, Payload: []byte("integrity"),
			})
		}(i)
	}
	wg.Wait()

	// Run PRAGMA integrity_check.
	var result string
	if err := s.db.QueryRow("PRAGMA integrity_check;").Scan(&result); err != nil {
		t.Fatalf("integrity_check: %v", err)
	}
	if result != "ok" {
		t.Fatalf("integrity_check failed: %s", result)
	}
	_ = s.Close()

	// Reopen and verify integrity after WAL replay.
	s2, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	if err := s2.db.QueryRow("PRAGMA integrity_check;").Scan(&result); err != nil {
		t.Fatalf("integrity_check after reopen: %v", err)
	}
	if result != "ok" {
		t.Fatalf("integrity_check failed after reopen: %s", result)
	}
}

func TestSQLiteStore_EnqueueBatch_AllOrNothing(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(dbPath, WithSQLiteQueueLimits(2, "reject"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

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

func TestSQLiteStore_EnqueueBatch_DuplicateID(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Pre-existing item.
	if err := s.Enqueue(Envelope{ID: "existing", Route: "/r", Target: "t"}); err != nil {
		t.Fatalf("pre-enqueue: %v", err)
	}

	// Batch with duplicate → no items committed (transaction rollback).
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
