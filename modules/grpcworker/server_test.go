package grpcworker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/pullapi"
	"github.com/nuetzliches/hookaido/internal/queue"
	"github.com/nuetzliches/hookaido/internal/workerapi"
	workerapipb "github.com/nuetzliches/hookaido/modules/grpcworker/proto"
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
	ws.Authorize = workerapi.BearerTokenAuthorizer([][]byte{[]byte("t1")})

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

// ---------------------------------------------------------------------------
// Input Validation Tests
// ---------------------------------------------------------------------------

func TestWorkerAPINilRequest(t *testing.T) {
	store := queue.NewMemoryStore()
	ws := NewServer(pullapi.NewServer(store))

	tests := []struct {
		name string
		call func() error
	}{
		{"Dequeue", func() error { _, err := ws.Dequeue(context.Background(), nil); return err }},
		{"Ack", func() error { _, err := ws.Ack(context.Background(), nil); return err }},
		{"Nack", func() error { _, err := ws.Nack(context.Background(), nil); return err }},
		{"Extend", func() error { _, err := ws.Extend(context.Background(), nil); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %v", err)
			}
		})
	}
}

func TestWorkerAPIBlankEndpoint(t *testing.T) {
	store := queue.NewMemoryStore()
	ws := NewServer(pullapi.NewServer(store))

	endpoints := []string{"", "   ", "\t"}
	for _, ep := range endpoints {
		t.Run("Dequeue/"+ep, func(t *testing.T) {
			_, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{Endpoint: ep})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for endpoint %q, got %v", ep, err)
			}
		})
		t.Run("Ack/"+ep, func(t *testing.T) {
			_, err := ws.Ack(context.Background(), &workerapipb.AckRequest{Endpoint: ep})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for endpoint %q, got %v", ep, err)
			}
		})
		t.Run("Nack/"+ep, func(t *testing.T) {
			_, err := ws.Nack(context.Background(), &workerapipb.NackRequest{Endpoint: ep})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for endpoint %q, got %v", ep, err)
			}
		})
		t.Run("Extend/"+ep, func(t *testing.T) {
			_, err := ws.Extend(context.Background(), &workerapipb.ExtendRequest{Endpoint: ep})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for endpoint %q, got %v", ep, err)
			}
		})
	}
}

func TestWorkerAPIPullNilGuard(t *testing.T) {
	ws := &Server{
		ResolveRoute: func(endpoint string) (string, bool) { return "/r", true },
	}

	tests := []struct {
		name string
		call func() error
	}{
		{"Dequeue", func() error {
			_, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{Endpoint: "/pull/r"})
			return err
		}},
		{"Ack", func() error {
			_, err := ws.Ack(context.Background(), &workerapipb.AckRequest{Endpoint: "/pull/r", LeaseId: "l1"})
			return err
		}},
		{"Nack", func() error {
			_, err := ws.Nack(context.Background(), &workerapipb.NackRequest{Endpoint: "/pull/r", LeaseId: "l1"})
			return err
		}},
		{"Extend", func() error {
			_, err := ws.Extend(context.Background(), &workerapipb.ExtendRequest{
				Endpoint: "/pull/r",
				LeaseId:  "l1",
				ExtendBy: durationpb.New(5 * time.Second),
			})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if status.Code(err) != codes.Internal {
				t.Fatalf("expected Internal, got %v", err)
			}
		})
	}
}

func TestWorkerAPIInvalidDuration(t *testing.T) {
	store := queue.NewMemoryStore()
	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) { return "/r", true }

	// Negative seconds with positive nanos is invalid per protobuf spec.
	badDuration := &durationpb.Duration{Seconds: -1, Nanos: 1}

	t.Run("max_wait", func(t *testing.T) {
		_, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
			Endpoint: "/pull/r",
			MaxWait:  badDuration,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument for bad max_wait, got %v", err)
		}
	})

	t.Run("lease_ttl", func(t *testing.T) {
		_, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
			Endpoint: "/pull/r",
			LeaseTtl: badDuration,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument for bad lease_ttl, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Lease ID Normalization Tests
// ---------------------------------------------------------------------------

func TestWorkerAPILeaseIDAndLeaseIDsBothSet(t *testing.T) {
	store := queue.NewMemoryStore()
	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) { return "/r", true }

	t.Run("Ack", func(t *testing.T) {
		_, err := ws.Ack(context.Background(), &workerapipb.AckRequest{
			Endpoint: "/pull/r",
			LeaseId:  "l1",
			LeaseIds: []string{"l2"},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument, got %v", err)
		}
	})

	t.Run("Nack", func(t *testing.T) {
		_, err := ws.Nack(context.Background(), &workerapipb.NackRequest{
			Endpoint: "/pull/r",
			LeaseId:  "l1",
			LeaseIds: []string{"l2"},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument, got %v", err)
		}
	})
}

func TestWorkerAPILeaseIDsAllEmpty(t *testing.T) {
	store := queue.NewMemoryStore()
	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) { return "/r", true }

	t.Run("Ack", func(t *testing.T) {
		_, err := ws.Ack(context.Background(), &workerapipb.AckRequest{
			Endpoint: "/pull/r",
			LeaseIds: []string{"", "  ", "\t"},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument, got %v", err)
		}
	})

	t.Run("Nack", func(t *testing.T) {
		_, err := ws.Nack(context.Background(), &workerapipb.NackRequest{
			Endpoint: "/pull/r",
			LeaseIds: []string{"", "  "},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument, got %v", err)
		}
	})
}

func TestWorkerAPILeaseIDsExceedMaxBatch(t *testing.T) {
	store := queue.NewMemoryStore()
	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) { return "/r", true }

	// Default max is 100; generate 101 unique lease IDs.
	ids := make([]string, 101)
	for i := range ids {
		ids[i] = fmt.Sprintf("lease_%d", i)
	}

	_, err := ws.Ack(context.Background(), &workerapipb.AckRequest{
		Endpoint: "/pull/r",
		LeaseIds: ids,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for exceeding max batch, got %v", err)
	}
}

func TestWorkerAPILeaseIDsDedup(t *testing.T) {
	now := time.Date(2026, 2, 14, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	// Enqueue a single item and dequeue it to get a valid lease.
	if err := store.Enqueue(queue.Envelope{
		ID: "evt_dup", Route: "/r", Target: "pull",
		ReceivedAt: nowVar, NextRunAt: nowVar,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) { return "/r", true }

	deq, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
		Endpoint: "/pull/r",
		Batch:    1,
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	leaseID := deq.GetItems()[0].GetLeaseId()

	// Ack with duplicate lease IDs — should dedup and succeed.
	resp, err := ws.Ack(context.Background(), &workerapipb.AckRequest{
		Endpoint: "/pull/r",
		LeaseIds: []string{leaseID, leaseID, leaseID},
	})
	if err != nil {
		t.Fatalf("ack with dups: %v", err)
	}
	if resp.GetAcked() != 1 {
		t.Fatalf("expected acked=1 after dedup, got %d", resp.GetAcked())
	}
}

// ---------------------------------------------------------------------------
// Route Resolution Tests
// ---------------------------------------------------------------------------

func TestWorkerAPIResolveRouteFallbackChain(t *testing.T) {
	now := time.Date(2026, 2, 14, 12, 0, 0, 0, time.UTC)
	nowVar := now

	t.Run("server_ResolveRoute_takes_priority", func(t *testing.T) {
		store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
		if err := store.Enqueue(queue.Envelope{
			ID: "evt_s", Route: "/server", Target: "pull",
			ReceivedAt: nowVar, NextRunAt: nowVar,
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		pullSrv := pullapi.NewServer(store)
		pullSrv.ResolveRoute = func(endpoint string) (string, bool) {
			return "/pull_fallback", true
		}
		ws := NewServer(pullSrv)
		ws.ResolveRoute = func(endpoint string) (string, bool) {
			return "/server", true
		}

		deq, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
			Endpoint: "/ep", Batch: 1,
		})
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		// Should use the server-level resolver (route "/server"), which has an item.
		if len(deq.GetItems()) != 1 {
			t.Fatalf("expected 1 item from server resolver, got %d", len(deq.GetItems()))
		}
	})

	t.Run("falls_back_to_Pull.ResolveRoute", func(t *testing.T) {
		store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
		if err := store.Enqueue(queue.Envelope{
			ID: "evt_p", Route: "/pull_resolved", Target: "pull",
			ReceivedAt: nowVar, NextRunAt: nowVar,
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		pullSrv := pullapi.NewServer(store)
		pullSrv.ResolveRoute = func(endpoint string) (string, bool) {
			return "/pull_resolved", true
		}
		ws := NewServer(pullSrv)
		// No server-level ResolveRoute — should fall back to Pull.ResolveRoute.

		deq, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
			Endpoint: "/ep", Batch: 1,
		})
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if len(deq.GetItems()) != 1 {
			t.Fatalf("expected 1 item from Pull.ResolveRoute fallback, got %d", len(deq.GetItems()))
		}
	})

	t.Run("identity_fallback", func(t *testing.T) {
		store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
		if err := store.Enqueue(queue.Envelope{
			ID: "evt_id", Route: "/identity", Target: "pull",
			ReceivedAt: nowVar, NextRunAt: nowVar,
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		pullSrv := pullapi.NewServer(store)
		// Neither Pull.ResolveRoute nor Server.ResolveRoute set — identity.
		ws := NewServer(pullSrv)

		deq, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
			Endpoint: "/identity", Batch: 1,
		})
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if len(deq.GetItems()) != 1 {
			t.Fatalf("expected 1 item from identity fallback, got %d", len(deq.GetItems()))
		}
	})
}

func TestWorkerAPICustomMaxLeaseBatch(t *testing.T) {
	store := queue.NewMemoryStore()
	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) { return "/r", true }
	ws.MaxLeaseBatch = 3

	// Generate 4 unique lease IDs — exceeds custom max of 3.
	_, err := ws.Ack(context.Background(), &workerapipb.AckRequest{
		Endpoint: "/pull/r",
		LeaseIds: []string{"l1", "l2", "l3", "l4"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for exceeding custom max batch, got %v", err)
	}

	// 3 entries should be accepted (within limit), though leases won't exist.
	resp, err := ws.Ack(context.Background(), &workerapipb.AckRequest{
		Endpoint: "/pull/r",
		LeaseIds: []string{"l1", "l2", "l3"},
	})
	if err != nil {
		t.Fatalf("ack within limit: %v", err)
	}
	// All 3 leases are unknown, so expect 0 acked and 3 conflicts.
	if resp.GetAcked() != 0 {
		t.Fatalf("expected acked=0, got %d", resp.GetAcked())
	}
	if len(resp.GetConflicts()) != 3 {
		t.Fatalf("expected 3 conflicts, got %d", len(resp.GetConflicts()))
	}
}

// ---------------------------------------------------------------------------
// Error Mapping Tests
// ---------------------------------------------------------------------------

func TestWorkerAPIMapOpErrorCoverage(t *testing.T) {
	tests := []struct {
		httpCode int
		grpcCode codes.Code
	}{
		{400, codes.InvalidArgument},
		{401, codes.Unauthenticated},
		{404, codes.NotFound},
		{409, codes.FailedPrecondition},
		{503, codes.Unavailable},
		{500, codes.Internal},
		{418, codes.Internal}, // unknown code maps to Internal
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("http_%d", tt.httpCode), func(t *testing.T) {
			opErr := &pullapi.OpError{
				StatusCode: tt.httpCode,
				Detail:     "test error",
			}
			grpcErr := mapOpError(opErr)
			if status.Code(grpcErr) != tt.grpcCode {
				t.Fatalf("http %d: expected gRPC code %v, got %v", tt.httpCode, tt.grpcCode, status.Code(grpcErr))
			}
		})
	}

	t.Run("nil_OpError", func(t *testing.T) {
		if err := mapOpError(nil); err != nil {
			t.Fatalf("expected nil error for nil OpError, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// gRPC-Specific Edge Cases
// ---------------------------------------------------------------------------

func TestWorkerAPINackDeadViaGRPC(t *testing.T) {
	now := time.Date(2026, 2, 14, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{
		ID: "evt_dead", Route: "/r", Target: "pull",
		ReceivedAt: nowVar, NextRunAt: nowVar, Payload: []byte("payload"),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) { return "/r", true }

	deq, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
		Endpoint: "/pull/r", Batch: 1,
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.GetItems()) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq.GetItems()))
	}
	leaseID := deq.GetItems()[0].GetLeaseId()

	resp, err := ws.Nack(context.Background(), &workerapipb.NackRequest{
		Endpoint: "/pull/r",
		LeaseId:  leaseID,
		Dead:     true,
		Reason:   "permanent_failure",
	})
	if err != nil {
		t.Fatalf("nack dead: %v", err)
	}
	if resp.GetSucceeded() != 1 {
		t.Fatalf("expected succeeded=1, got %d", resp.GetSucceeded())
	}

	// Verify item landed in DLQ.
	dlq, err := store.ListDead(queue.DeadListRequest{Route: "/r", Limit: 10})
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(dlq.Items) != 1 {
		t.Fatalf("expected 1 dead item, got %d", len(dlq.Items))
	}
	if dlq.Items[0].ID != "evt_dead" {
		t.Fatalf("expected dead item evt_dead, got %s", dlq.Items[0].ID)
	}
	if dlq.Items[0].DeadReason != "permanent_failure" {
		t.Fatalf("expected dead reason %q, got %q", "permanent_failure", dlq.Items[0].DeadReason)
	}
}

func TestWorkerAPINackBatch(t *testing.T) {
	now := time.Date(2026, 2, 14, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	for i := 0; i < 3; i++ {
		if err := store.Enqueue(queue.Envelope{
			ID: fmt.Sprintf("evt_%d", i), Route: "/r", Target: "pull",
			ReceivedAt: nowVar, NextRunAt: nowVar,
		}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) { return "/r", true }

	deq, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
		Endpoint: "/pull/r", Batch: 3,
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.GetItems()) != 3 {
		t.Fatalf("expected 3 items, got %d", len(deq.GetItems()))
	}

	leaseIDs := make([]string, 3)
	for i, it := range deq.GetItems() {
		leaseIDs[i] = it.GetLeaseId()
	}

	// Batch nack with a mix of valid and invalid lease IDs.
	resp, err := ws.Nack(context.Background(), &workerapipb.NackRequest{
		Endpoint: "/pull/r",
		LeaseIds: append(leaseIDs, "missing_lease"),
	})
	if err != nil {
		t.Fatalf("nack batch: %v", err)
	}
	if resp.GetSucceeded() != 3 {
		t.Fatalf("expected succeeded=3, got %d", resp.GetSucceeded())
	}
	if len(resp.GetConflicts()) != 1 {
		t.Fatalf("expected 1 conflict, got %d", len(resp.GetConflicts()))
	}
	if resp.GetConflicts()[0].GetLeaseId() != "missing_lease" {
		t.Fatalf("expected conflict lease_id %q, got %q", "missing_lease", resp.GetConflicts()[0].GetLeaseId())
	}
}

func TestWorkerAPILargeBatchDequeue(t *testing.T) {
	now := time.Date(2026, 2, 14, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	const batchSize = 50
	for i := 0; i < batchSize; i++ {
		if err := store.Enqueue(queue.Envelope{
			ID:         fmt.Sprintf("evt_%03d", i),
			Route:      "/r",
			Target:     "pull",
			ReceivedAt: nowVar,
			NextRunAt:  nowVar,
			Payload:    []byte(fmt.Sprintf(`{"index":%d}`, i)),
			Headers:    map[string]string{"X-Index": fmt.Sprintf("%d", i)},
		}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	ws := NewServer(pullapi.NewServer(store))
	ws.ResolveRoute = func(endpoint string) (string, bool) { return "/r", true }

	deq, err := ws.Dequeue(context.Background(), &workerapipb.DequeueRequest{
		Endpoint: "/pull/r",
		Batch:    batchSize,
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.GetItems()) != batchSize {
		t.Fatalf("expected %d items, got %d", batchSize, len(deq.GetItems()))
	}

	// Verify every item has populated fields.
	seen := make(map[string]bool)
	for _, it := range deq.GetItems() {
		if it.GetLeaseId() == "" {
			t.Fatalf("item %s missing lease_id", it.GetId())
		}
		if len(it.GetPayload()) == 0 {
			t.Fatalf("item %s missing payload", it.GetId())
		}
		if it.GetHeaders()["X-Index"] == "" {
			t.Fatalf("item %s missing X-Index header", it.GetId())
		}
		if it.GetRoute() != "/r" {
			t.Fatalf("item %s: expected route /r, got %s", it.GetId(), it.GetRoute())
		}
		if seen[it.GetId()] {
			t.Fatalf("duplicate item %s", it.GetId())
		}
		seen[it.GetId()] = true
	}
}
