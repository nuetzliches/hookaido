package pullapi

import (
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

func TestPullOpsDequeueClampsAndDefaults(t *testing.T) {
	store := &stubStore{
		dequeueResp: queue.DequeueResponse{Items: nil},
	}
	srv := NewServer(store)
	srv.MaxBatch = 10
	srv.DefaultMaxWait = 3 * time.Second
	srv.DefaultLeaseTTL = 30 * time.Second
	srv.MaxLeaseTTL = 20 * time.Second

	_, opErr := srv.Dequeue("/hooks/github", DequeueParams{
		Batch:       99,
		HasMaxWait:  false,
		HasLeaseTTL: false,
	})
	if opErr != nil {
		t.Fatalf("unexpected opErr: %#v", opErr)
	}
	if store.lastDequeueReq.Batch != 10 {
		t.Fatalf("expected clamped batch=10, got %d", store.lastDequeueReq.Batch)
	}
	if store.lastDequeueReq.MaxWait != 3*time.Second {
		t.Fatalf("expected default max_wait=3s, got %s", store.lastDequeueReq.MaxWait)
	}
	if store.lastDequeueReq.LeaseTTL != 20*time.Second {
		t.Fatalf("expected clamped lease_ttl=20s, got %s", store.lastDequeueReq.LeaseTTL)
	}
}

func TestPullOpsAckBatchIncludesRecentlyCompleted(t *testing.T) {
	now := time.Date(2026, 2, 14, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar}); err != nil {
		t.Fatalf("enqueue evt_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_2", Route: "/r", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar}); err != nil {
		t.Fatalf("enqueue evt_2: %v", err)
	}

	deq, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 2, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(deq.Items))
	}

	srv := NewServer(store)
	if opErr := srv.AckSingle("/r", deq.Items[0].LeaseID); opErr != nil {
		t.Fatalf("ack single opErr: %#v", opErr)
	}

	out, opErr := srv.AckBatch("/r", []string{deq.Items[0].LeaseID, deq.Items[1].LeaseID})
	if opErr != nil {
		t.Fatalf("ack batch opErr: %#v", opErr)
	}
	if out.Succeeded != 2 {
		t.Fatalf("expected succeeded=2, got %d", out.Succeeded)
	}
	if len(out.Conflicts) != 0 {
		t.Fatalf("expected no conflicts, got %#v", out.Conflicts)
	}
}

func TestPullOpsNackDeadUsesMarkDead(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)

	opErr := srv.NackSingle("/r", "lease_1", true, "no_retry", 0)
	if opErr != nil {
		t.Fatalf("unexpected opErr: %#v", opErr)
	}
	if !store.markDeadCalled {
		t.Fatalf("expected mark dead call")
	}
	if store.markDeadLease != "lease_1" {
		t.Fatalf("expected mark dead lease_1, got %q", store.markDeadLease)
	}
	if store.markDeadReason != "no_retry" {
		t.Fatalf("expected mark dead reason no_retry, got %q", store.markDeadReason)
	}
}

func TestPullOpsExtendLeaseConflict(t *testing.T) {
	store := &stubStore{extendErr: queue.ErrLeaseExpired}
	srv := NewServer(store)

	opErr := srv.Extend("/r", "lease_1", 5*time.Second)
	if opErr == nil {
		t.Fatalf("expected opErr")
	}
	if opErr.StatusCode != 409 {
		t.Fatalf("expected status 409, got %d", opErr.StatusCode)
	}
	if opErr.Code != pullErrLeaseConflict {
		t.Fatalf("expected code %q, got %q", pullErrLeaseConflict, opErr.Code)
	}
}
