package workerapi

import (
	"context"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/pullapi"
	"github.com/nuetzliches/hookaido/internal/queue"
	workerapipb "github.com/nuetzliches/hookaido/internal/workerapi/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestWorkerAPIDequeueAckFlow(t *testing.T) {
	now := time.Date(2026, 2, 14, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{
		ID:         "evt_1",
		Route:      "/webhooks/github",
		Target:     "pull",
		ReceivedAt: nowVar,
		NextRunAt:  nowVar,
		Payload:    []byte(`{"ok":true}`),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	deq, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
		Endpoint: "/pull/github",
		Batch:    1,
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.GetItems()) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq.GetItems()))
	}
	leaseID := deq.GetItems()[0].GetLeaseId()
	if leaseID == "" {
		t.Fatalf("expected lease id")
	}

	ack, err := ws.Ack(context.Background(), &workerapipb.AckRequest{
		Endpoint: "/pull/github",
		LeaseId:  leaseID,
	})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if ack.GetAcked() != 1 {
		t.Fatalf("expected acked=1, got %d", ack.GetAcked())
	}

	deq2, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
		Endpoint: "/pull/github",
		Batch:    1,
	})
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if len(deq2.GetItems()) != 0 {
		t.Fatalf("expected 0 items, got %d", len(deq2.GetItems()))
	}
}

func TestWorkerAPIBearerAuth(t *testing.T) {
	store := queue.NewMemoryStore()
	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) {
		return "/r", true
	}
	ws.Authorize = BearerTokenAuthorizer([][]byte{[]byte("t1")})

	_, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
		Endpoint: "/pull/r",
		Batch:    1,
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer t1"))
	if _, err := ws.Dequeue(ctx, &workerapipb.DequeueRequest{
		Endpoint: "/pull/r",
		Batch:    1,
	}); err != nil {
		t.Fatalf("expected authorized dequeue, got %v", err)
	}
}

func TestWorkerAPIAckBatchConflictPayload(t *testing.T) {
	now := time.Date(2026, 2, 14, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{
		ID:         "evt_1",
		Route:      "/r",
		Target:     "pull",
		ReceivedAt: nowVar,
		NextRunAt:  nowVar,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	deq, err := store.Dequeue(queue.DequeueRequest{
		Route:    "/r",
		Target:   "pull",
		Batch:    1,
		LeaseTTL: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("seed dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq.Items))
	}

	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) {
		return "/r", true
	}

	resp, err := ws.Ack(context.Background(), &workerapipb.AckRequest{
		Endpoint: "/pull/r",
		LeaseIds: []string{deq.Items[0].LeaseID, "lease_missing"},
	})
	if err != nil {
		t.Fatalf("ack batch: %v", err)
	}
	if resp.GetAcked() != 1 {
		t.Fatalf("expected acked=1, got %d", resp.GetAcked())
	}
	if len(resp.GetConflicts()) != 1 {
		t.Fatalf("expected 1 conflict, got %d", len(resp.GetConflicts()))
	}
	if resp.GetConflicts()[0].GetLeaseId() != "lease_missing" {
		t.Fatalf("unexpected conflict lease id: %q", resp.GetConflicts()[0].GetLeaseId())
	}
}

func TestWorkerAPIExtendRequiresDuration(t *testing.T) {
	store := queue.NewMemoryStore()
	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) {
		return "/r", true
	}

	_, err := ws.Extend(context.Background(), &workerapipb.ExtendRequest{
		Endpoint: "/pull/r",
		LeaseId:  "lease_1",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}

	_, err = ws.Extend(context.Background(), &workerapipb.ExtendRequest{
		Endpoint: "/pull/r",
		LeaseId:  "lease_1",
		ExtendBy: durationpb.New(5 * time.Second),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected failed precondition for unknown lease, got %v", err)
	}
}
