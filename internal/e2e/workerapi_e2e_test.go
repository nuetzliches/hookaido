package e2e

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/pullapi"
	"github.com/nuetzliches/hookaido/internal/queue"
	"github.com/nuetzliches/hookaido/internal/workerapi"
	workerapipb "github.com/nuetzliches/hookaido/internal/workerapi/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func startWorkerClient(
	t *testing.T,
	pull *pullapi.Server,
	authorize workerapi.Authorizer,
) (workerapipb.WorkerServiceClient, func()) {
	t.Helper()

	ws := workerapi.NewServer(pull)
	ws.ResolveRoute = pull.ResolveRoute
	ws.Authorize = authorize

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcSrv := grpc.NewServer()
	workerapipb.RegisterWorkerServiceServer(grpcSrv, ws)
	go func() {
		_ = grpcSrv.Serve(ln)
	}()

	dialCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		dialCtx,
		ln.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		grpcSrv.Stop()
		_ = ln.Close()
	}
	return workerapipb.NewWorkerServiceClient(conn), cleanup
}

func workerAuthContext(token string) context.Context {
	if token == "" {
		return context.Background()
	}
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}

func TestE2E_WorkerAPIDequeueExtendAckRoundTrip(t *testing.T) {
	store := newTestStore()
	ing := httptest.NewServer(ingressServer(store, "/hooks/grpc"))
	defer ing.Close()

	pull := pullServer(store, "/hooks/grpc")
	client, cleanup := startWorkerClient(t, pull, nil)
	defer cleanup()

	payload := `{"source":"grpc"}`
	resp, err := http.Post(ing.URL+"/hooks/grpc/events", "application/json", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatalf("ingress POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingress status: got %d, want 202", resp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	deq, err := client.Dequeue(ctx, &workerapipb.DequeueRequest{
		Endpoint: "/hooks/grpc",
		Batch:    1,
		LeaseTtl: durationpb.New(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.GetItems()) != 1 {
		t.Fatalf("dequeue items: got %d, want 1", len(deq.GetItems()))
	}
	item := deq.GetItems()[0]
	if string(item.GetPayload()) != payload {
		t.Fatalf("payload mismatch: got %q, want %q", string(item.GetPayload()), payload)
	}
	if item.GetRoute() != "/hooks/grpc" {
		t.Fatalf("route: got %q, want /hooks/grpc", item.GetRoute())
	}

	_, err = client.Extend(ctx, &workerapipb.ExtendRequest{
		Endpoint: "/hooks/grpc",
		LeaseId:  item.GetLeaseId(),
		ExtendBy: durationpb.New(15 * time.Second),
	})
	if err != nil {
		t.Fatalf("extend: %v", err)
	}

	ack, err := client.Ack(ctx, &workerapipb.AckRequest{
		Endpoint: "/hooks/grpc",
		LeaseId:  item.GetLeaseId(),
	})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if ack.GetAcked() != 1 {
		t.Fatalf("ack acked: got %d, want 1", ack.GetAcked())
	}

	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ByState[queue.StateQueued] != 0 {
		t.Fatalf("queued items remaining: %d", stats.ByState[queue.StateQueued])
	}
}

func TestE2E_WorkerAPINackRequeueAndIdempotentRetry(t *testing.T) {
	store := newTestStore()
	ing := httptest.NewServer(ingressServer(store, "/hooks/grpc-nack"))
	defer ing.Close()

	pull := pullServer(store, "/hooks/grpc-nack")
	client, cleanup := startWorkerClient(t, pull, nil)
	defer cleanup()

	resp, err := http.Post(ing.URL+"/hooks/grpc-nack", "application/json", bytes.NewReader([]byte(`{"retry":"yes"}`)))
	if err != nil {
		t.Fatalf("ingress POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingress status: got %d, want 202", resp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	deq, err := client.Dequeue(ctx, &workerapipb.DequeueRequest{
		Endpoint: "/hooks/grpc-nack",
		Batch:    1,
		LeaseTtl: durationpb.New(30 * time.Second),
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.GetItems()) != 1 {
		t.Fatalf("dequeue items: got %d, want 1", len(deq.GetItems()))
	}
	leaseID := deq.GetItems()[0].GetLeaseId()

	nack, err := client.Nack(ctx, &workerapipb.NackRequest{
		Endpoint: "/hooks/grpc-nack",
		LeaseId:  leaseID,
		Delay:    durationpb.New(0),
	})
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if nack.GetSucceeded() != 1 {
		t.Fatalf("nack succeeded: got %d, want 1", nack.GetSucceeded())
	}

	// Duplicate retries after successful nack should be idempotent success.
	nackDup, err := client.Nack(ctx, &workerapipb.NackRequest{
		Endpoint: "/hooks/grpc-nack",
		LeaseId:  leaseID,
		Delay:    durationpb.New(0),
	})
	if err != nil {
		t.Fatalf("nack duplicate: %v", err)
	}
	if nackDup.GetSucceeded() != 1 {
		t.Fatalf("duplicate nack succeeded: got %d, want 1", nackDup.GetSucceeded())
	}

	deq2, err := client.Dequeue(ctx, &workerapipb.DequeueRequest{
		Endpoint: "/hooks/grpc-nack",
		Batch:    1,
		LeaseTtl: durationpb.New(30 * time.Second),
	})
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if len(deq2.GetItems()) != 1 {
		t.Fatalf("dequeue2 items: got %d, want 1", len(deq2.GetItems()))
	}
	if deq2.GetItems()[0].GetAttempt() != 2 {
		t.Fatalf("attempt: got %d, want 2", deq2.GetItems()[0].GetAttempt())
	}
}

func TestE2E_WorkerAPIAckBatchConflictAndIdempotentRetry(t *testing.T) {
	store := newTestStore()
	if err := store.Enqueue(queue.Envelope{
		ID:      "evt_1",
		Route:   "/hooks/grpc-ack",
		Target:  "pull",
		Payload: []byte(`{"x":1}`),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	pull := pullServer(store, "/hooks/grpc-ack")
	client, cleanup := startWorkerClient(t, pull, nil)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	deq, err := client.Dequeue(ctx, &workerapipb.DequeueRequest{
		Endpoint: "/hooks/grpc-ack",
		Batch:    1,
		LeaseTtl: durationpb.New(30 * time.Second),
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.GetItems()) != 1 {
		t.Fatalf("dequeue items: got %d, want 1", len(deq.GetItems()))
	}
	leaseID := deq.GetItems()[0].GetLeaseId()

	ack, err := client.Ack(ctx, &workerapipb.AckRequest{
		Endpoint: "/hooks/grpc-ack",
		LeaseIds: []string{leaseID, "lease_missing"},
	})
	if err != nil {
		t.Fatalf("ack batch: %v", err)
	}
	if ack.GetAcked() != 1 {
		t.Fatalf("ack batch acked: got %d, want 1", ack.GetAcked())
	}
	if len(ack.GetConflicts()) != 1 || ack.GetConflicts()[0].GetLeaseId() != "lease_missing" {
		t.Fatalf("ack conflicts: got %#v", ack.GetConflicts())
	}

	// Duplicate retries after successful ack should be idempotent success.
	ackDup, err := client.Ack(ctx, &workerapipb.AckRequest{
		Endpoint: "/hooks/grpc-ack",
		LeaseId:  leaseID,
	})
	if err != nil {
		t.Fatalf("ack duplicate: %v", err)
	}
	if ackDup.GetAcked() != 1 {
		t.Fatalf("duplicate acked: got %d, want 1", ackDup.GetAcked())
	}
}

func TestE2E_WorkerAPIBearerAuthRouteOverride(t *testing.T) {
	store := newTestStore()
	pull := pullServer(store, "/hooks/auth", "/hooks/other")

	globalAuth := workerapi.BearerTokenAuthorizer([][]byte{[]byte("global-token")})
	routeAuth := workerapi.BearerTokenAuthorizer([][]byte{[]byte("route-token")})
	authorize := func(ctx context.Context, endpoint string) bool {
		if endpoint == "/hooks/auth" {
			return routeAuth(ctx, endpoint)
		}
		return globalAuth(ctx, endpoint)
	}

	client, cleanup := startWorkerClient(t, pull, authorize)
	defer cleanup()

	reqCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := client.Dequeue(workerAuthContext("global-token"), &workerapipb.DequeueRequest{
		Endpoint: "/hooks/auth",
		Batch:    1,
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated with global token on route override, got %v", err)
	}

	_, err = client.Dequeue(workerAuthContext("route-token"), &workerapipb.DequeueRequest{
		Endpoint: "/hooks/auth",
		Batch:    1,
	})
	if err != nil {
		t.Fatalf("expected route token authorization, got %v", err)
	}

	_, err = client.Dequeue(workerAuthContext("route-token"), &workerapipb.DequeueRequest{
		Endpoint: "/hooks/other",
		Batch:    1,
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated with route token on global endpoint, got %v", err)
	}

	_, err = client.Dequeue(workerAuthContext("global-token"), &workerapipb.DequeueRequest{
		Endpoint: "/hooks/other",
		Batch:    1,
	})
	if err != nil {
		t.Fatalf("expected global token authorization, got %v", err)
	}

	_, err = client.Dequeue(reqCtx, &workerapipb.DequeueRequest{
		Endpoint: "/hooks/auth",
		Batch:    1,
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated without token, got %v", err)
	}
}

func TestE2E_WorkerAPIAuthorizationHeaderFormat(t *testing.T) {
	store := newTestStore()
	pull := pullServer(store, "/hooks/auth")
	authorize := workerapi.BearerTokenAuthorizer([][]byte{[]byte("token")})

	client, cleanup := startWorkerClient(t, pull, authorize)
	defer cleanup()

	badCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Basic token"))
	_, err := client.Dequeue(badCtx, &workerapipb.DequeueRequest{
		Endpoint: "/hooks/auth",
		Batch:    1,
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated for non-bearer auth format, got %v", err)
	}
}
