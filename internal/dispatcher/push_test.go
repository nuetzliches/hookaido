package dispatcher

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hookaido/internal/queue"
)

func TestShouldRetry(t *testing.T) {
	cases := []struct {
		name string
		in   Result
		want bool
	}{
		{name: "network error", in: Result{Err: errors.New("dial timeout")}, want: true},
		{name: "policy denied", in: Result{Err: ErrPolicyDenied}, want: false},
		{name: "request timeout", in: Result{StatusCode: http.StatusRequestTimeout}, want: true},
		{name: "too many requests", in: Result{StatusCode: http.StatusTooManyRequests}, want: true},
		{name: "server error", in: Result{StatusCode: http.StatusBadGateway}, want: true},
		{name: "client error no retry", in: Result{StatusCode: http.StatusBadRequest}, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldRetry(tc.in)
			if got != tc.want {
				t.Fatalf("shouldRetry()=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestRetryDelay(t *testing.T) {
	retry := RetryConfig{
		Type:   "exponential",
		Max:    8,
		Base:   2 * time.Second,
		Cap:    30 * time.Second,
		Jitter: 0,
	}

	if got := retryDelay(1, retry); got != 2*time.Second {
		t.Fatalf("attempt1 delay=%s, want 2s", got)
	}
	if got := retryDelay(2, retry); got != 4*time.Second {
		t.Fatalf("attempt2 delay=%s, want 4s", got)
	}
	if got := retryDelay(5, retry); got != 30*time.Second {
		t.Fatalf("attempt5 delay=%s, want 30s cap", got)
	}
}

func TestHandleDelivery_SuccessAcks(t *testing.T) {
	store := &stubPushStore{}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: http.StatusNoContent}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}

	env := queue.Envelope{Route: "/r", Target: "https://x", LeaseID: "lease_1", Attempt: 1}
	target := TargetConfig{
		URL:     "https://x",
		Timeout: time.Second,
		Retry:   RetryConfig{Max: 3, Base: time.Second, Cap: 10 * time.Second},
	}
	d.handleDelivery(d.Logger, env, target)

	if store.ackLeaseID != "lease_1" {
		t.Fatalf("ack lease=%q, want lease_1", store.ackLeaseID)
	}
	if store.nackLeaseID != "" || store.markDeadLeaseID != "" {
		t.Fatalf("unexpected nack/dead calls: %#v", store)
	}
	if len(store.attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(store.attempts))
	}
	if got := store.attempts[0]; got.Outcome != queue.AttemptOutcomeAcked || got.Attempt != 1 || got.EventID != env.ID {
		t.Fatalf("unexpected attempt record: %#v", got)
	}
}

func TestHandleDelivery_RetryNacks(t *testing.T) {
	store := &stubPushStore{}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: http.StatusBadGateway}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}

	env := queue.Envelope{Route: "/r", Target: "https://x", LeaseID: "lease_1", Attempt: 1}
	target := TargetConfig{
		URL:     "https://x",
		Timeout: time.Second,
		Retry:   RetryConfig{Max: 3, Base: 2 * time.Second, Cap: 10 * time.Second, Jitter: 0},
	}
	d.handleDelivery(d.Logger, env, target)

	if store.nackLeaseID != "lease_1" {
		t.Fatalf("nack lease=%q, want lease_1", store.nackLeaseID)
	}
	if store.nackDelay != 2*time.Second {
		t.Fatalf("nack delay=%s, want 2s", store.nackDelay)
	}
	if store.ackLeaseID != "" || store.markDeadLeaseID != "" {
		t.Fatalf("unexpected ack/dead calls: %#v", store)
	}
	if len(store.attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(store.attempts))
	}
	if got := store.attempts[0]; got.Outcome != queue.AttemptOutcomeRetry || got.StatusCode != http.StatusBadGateway {
		t.Fatalf("unexpected attempt record: %#v", got)
	}
}

func TestHandleDelivery_MaxRetryMarksDead(t *testing.T) {
	store := &stubPushStore{}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: http.StatusServiceUnavailable}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}

	env := queue.Envelope{Route: "/r", Target: "https://x", LeaseID: "lease_1", Attempt: 3}
	target := TargetConfig{
		URL:     "https://x",
		Timeout: time.Second,
		Retry:   RetryConfig{Max: 3, Base: time.Second, Cap: 10 * time.Second},
	}
	d.handleDelivery(d.Logger, env, target)

	if store.markDeadLeaseID != "lease_1" {
		t.Fatalf("mark dead lease=%q, want lease_1", store.markDeadLeaseID)
	}
	if store.markDeadReason != "max_retries" {
		t.Fatalf("mark dead reason=%q, want max_retries", store.markDeadReason)
	}
	if store.ackLeaseID != "" || store.nackLeaseID != "" {
		t.Fatalf("unexpected ack/nack calls: %#v", store)
	}
	if len(store.attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(store.attempts))
	}
	if got := store.attempts[0]; got.Outcome != queue.AttemptOutcomeDead || got.DeadReason != "max_retries" {
		t.Fatalf("unexpected attempt record: %#v", got)
	}
}

func TestHandleDelivery_PolicyDeniedMarksDeadNoRetry(t *testing.T) {
	store := &stubPushStore{}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{Err: ErrPolicyDenied}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}

	env := queue.Envelope{Route: "/r", Target: "https://x", LeaseID: "lease_1", Attempt: 1}
	target := TargetConfig{
		URL:     "https://x",
		Timeout: time.Second,
		Retry:   RetryConfig{Max: 8, Base: time.Second, Cap: 10 * time.Second},
	}
	d.handleDelivery(d.Logger, env, target)

	if store.markDeadLeaseID != "lease_1" {
		t.Fatalf("mark dead lease=%q, want lease_1", store.markDeadLeaseID)
	}
	if store.markDeadReason != "policy_denied" {
		t.Fatalf("mark dead reason=%q, want policy_denied", store.markDeadReason)
	}
	if store.ackLeaseID != "" || store.nackLeaseID != "" {
		t.Fatalf("unexpected ack/nack calls: %#v", store)
	}
	if len(store.attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(store.attempts))
	}
	if got := store.attempts[0]; got.Outcome != queue.AttemptOutcomeDead || got.Error == "" || got.DeadReason != "policy_denied" {
		t.Fatalf("unexpected attempt record: %#v", got)
	}
}

type staticDeliverer struct {
	res Result
}

func (d staticDeliverer) Deliver(_ context.Context, _ Delivery) Result {
	return d.res
}

type stubPushStore struct {
	ackLeaseID      string
	nackLeaseID     string
	nackDelay       time.Duration
	markDeadLeaseID string
	markDeadReason  string
	attempts        []queue.DeliveryAttempt

	ackErr           error
	nackErr          error
	markDeadErr      error
	recordAttemptErr error
}

func (s *stubPushStore) Enqueue(_ queue.Envelope) error {
	return nil
}

func (s *stubPushStore) Dequeue(_ queue.DequeueRequest) (queue.DequeueResponse, error) {
	return queue.DequeueResponse{}, nil
}

func (s *stubPushStore) Ack(leaseID string) error {
	s.ackLeaseID = leaseID
	return s.ackErr
}

func (s *stubPushStore) Nack(leaseID string, delay time.Duration) error {
	s.nackLeaseID = leaseID
	s.nackDelay = delay
	return s.nackErr
}

func (s *stubPushStore) Extend(_ string, _ time.Duration) error {
	return nil
}

func (s *stubPushStore) MarkDead(leaseID string, reason string) error {
	s.markDeadLeaseID = leaseID
	s.markDeadReason = reason
	return s.markDeadErr
}

func (s *stubPushStore) ListDead(_ queue.DeadListRequest) (queue.DeadListResponse, error) {
	return queue.DeadListResponse{}, nil
}

func (s *stubPushStore) RequeueDead(_ queue.DeadRequeueRequest) (queue.DeadRequeueResponse, error) {
	return queue.DeadRequeueResponse{}, nil
}

func (s *stubPushStore) DeleteDead(_ queue.DeadDeleteRequest) (queue.DeadDeleteResponse, error) {
	return queue.DeadDeleteResponse{}, nil
}

func (s *stubPushStore) ListMessages(_ queue.MessageListRequest) (queue.MessageListResponse, error) {
	return queue.MessageListResponse{}, nil
}

func (s *stubPushStore) LookupMessages(_ queue.MessageLookupRequest) (queue.MessageLookupResponse, error) {
	return queue.MessageLookupResponse{}, nil
}

func (s *stubPushStore) CancelMessages(_ queue.MessageCancelRequest) (queue.MessageCancelResponse, error) {
	return queue.MessageCancelResponse{}, nil
}

func (s *stubPushStore) RequeueMessages(_ queue.MessageRequeueRequest) (queue.MessageRequeueResponse, error) {
	return queue.MessageRequeueResponse{}, nil
}

func (s *stubPushStore) ResumeMessages(_ queue.MessageResumeRequest) (queue.MessageResumeResponse, error) {
	return queue.MessageResumeResponse{}, nil
}

func (s *stubPushStore) CancelMessagesByFilter(_ queue.MessageManageFilterRequest) (queue.MessageCancelResponse, error) {
	return queue.MessageCancelResponse{}, nil
}

func (s *stubPushStore) RequeueMessagesByFilter(_ queue.MessageManageFilterRequest) (queue.MessageRequeueResponse, error) {
	return queue.MessageRequeueResponse{}, nil
}

func (s *stubPushStore) ResumeMessagesByFilter(_ queue.MessageManageFilterRequest) (queue.MessageResumeResponse, error) {
	return queue.MessageResumeResponse{}, nil
}

func (s *stubPushStore) Stats() (queue.Stats, error) {
	return queue.Stats{}, nil
}

func (s *stubPushStore) RecordAttempt(attempt queue.DeliveryAttempt) error {
	s.attempts = append(s.attempts, attempt)
	return s.recordAttemptErr
}

func (s *stubPushStore) ListAttempts(_ queue.AttemptListRequest) (queue.AttemptListResponse, error) {
	return queue.AttemptListResponse{}, nil
}

// ---------- lease-expired tolerance ----------

func TestHandleDelivery_LeaseExpiredToleratedOnAck(t *testing.T) {
	store := &stubPushStore{ackErr: queue.ErrLeaseExpired}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: 200}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	env := queue.Envelope{ID: "e1", Route: "/test", Target: "https://t", LeaseID: "l1", Attempt: 1, Payload: []byte(`{}`)}
	target := TargetConfig{URL: "https://t", Timeout: 5 * time.Second}
	d.handleDelivery(d.Logger, env, target)
	if store.ackLeaseID != "l1" {
		t.Fatalf("expected ack, got %q", store.ackLeaseID)
	}
	if len(store.attempts) != 1 || store.attempts[0].Outcome != queue.AttemptOutcomeAcked {
		t.Fatalf("expected 1 acked attempt, got %v", store.attempts)
	}
}

func TestHandleDelivery_LeaseExpiredToleratedOnNack(t *testing.T) {
	store := &stubPushStore{nackErr: queue.ErrLeaseExpired}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: 502}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	env := queue.Envelope{ID: "e1", Route: "/test", Target: "https://t", LeaseID: "l1", Attempt: 1, Payload: []byte(`{}`)}
	target := TargetConfig{URL: "https://t", Timeout: 5 * time.Second, Retry: RetryConfig{Max: 3, Base: time.Second}}
	d.handleDelivery(d.Logger, env, target)
	if store.nackLeaseID != "l1" {
		t.Fatalf("expected nack, got %q", store.nackLeaseID)
	}
	if len(store.attempts) != 1 || store.attempts[0].Outcome != queue.AttemptOutcomeRetry {
		t.Fatalf("expected 1 retry attempt, got %v", store.attempts)
	}
}

func TestHandleDelivery_LeaseExpiredToleratedOnMarkDead(t *testing.T) {
	store := &stubPushStore{markDeadErr: queue.ErrLeaseExpired}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: 502}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	env := queue.Envelope{ID: "e1", Route: "/test", Target: "https://t", LeaseID: "l1", Attempt: 3, Payload: []byte(`{}`)}
	target := TargetConfig{URL: "https://t", Timeout: 5 * time.Second, Retry: RetryConfig{Max: 3}}
	d.handleDelivery(d.Logger, env, target)
	if store.markDeadLeaseID != "l1" {
		t.Fatalf("expected mark dead, got %q", store.markDeadLeaseID)
	}
	if len(store.attempts) != 1 || store.attempts[0].Outcome != queue.AttemptOutcomeDead {
		t.Fatalf("expected 1 dead attempt, got %v", store.attempts)
	}
}

// ---------- RecordAttempt error tolerance ----------

func TestHandleDelivery_RecordAttemptErrorDoesNotBlockAck(t *testing.T) {
	store := &stubPushStore{recordAttemptErr: errors.New("attempt store failure")}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: 200}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	env := queue.Envelope{ID: "e1", Route: "/test", Target: "https://t", LeaseID: "l1", Attempt: 1, Payload: []byte(`{}`)}
	target := TargetConfig{URL: "https://t", Timeout: 5 * time.Second}
	d.handleDelivery(d.Logger, env, target)
	if store.ackLeaseID != "l1" {
		t.Fatalf("expected ack despite RecordAttempt error, got %q", store.ackLeaseID)
	}
}

// drainableStore returns one envelope per Dequeue call, with configurable delivery delay.
type drainableStore struct {
	stubPushStore
	mu       sync.Mutex
	dequeued int32
}

func (s *drainableStore) Dequeue(req queue.DequeueRequest) (queue.DequeueResponse, error) {
	// Only return one item total to keep tests fast.
	if atomic.AddInt32(&s.dequeued, 1) > 1 {
		// Simulate MaxWait poll with a short sleep.
		time.Sleep(req.MaxWait)
		return queue.DequeueResponse{}, nil
	}
	return queue.DequeueResponse{
		Items: []queue.Envelope{
			{ID: "evt-1", Route: req.Route, Target: req.Target, LeaseID: "lease-1", Attempt: 1, Payload: []byte(`{}`)},
		},
	}, nil
}

// slowDeliverer adds a configurable delay before returning.
type slowDeliverer struct {
	delay time.Duration
	res   Result
}

func (d slowDeliverer) Deliver(_ context.Context, _ Delivery) Result {
	time.Sleep(d.delay)
	return d.res
}

func TestDrain_CompletesInFlight(t *testing.T) {
	store := &drainableStore{}
	d := PushDispatcher{
		Store:     store,
		Deliverer: slowDeliverer{delay: 200 * time.Millisecond, res: Result{StatusCode: 200}},
		Routes: []RouteConfig{
			{Route: "/test", Targets: []TargetConfig{{URL: "https://target", Timeout: 5 * time.Second}}, Concurrency: 1},
		},
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxWait: 50 * time.Millisecond,
	}

	d.Start()

	// Give the goroutine time to dequeue and start delivery
	time.Sleep(50 * time.Millisecond)

	// Drain should wait for the in-flight 200ms delivery
	ok := d.Drain(5 * time.Second)
	if !ok {
		t.Fatal("Drain timed out, expected it to complete")
	}
	if store.ackLeaseID != "lease-1" {
		t.Fatalf("expected ack lease-1, got %q", store.ackLeaseID)
	}
}

func TestDrain_TimeoutReturns(t *testing.T) {
	// slowDeliverer takes 5s, but drain timeout is only 100ms
	store := &drainableStore{}
	d := PushDispatcher{
		Store:     store,
		Deliverer: slowDeliverer{delay: 5 * time.Second, res: Result{StatusCode: 200}},
		Routes: []RouteConfig{
			{Route: "/test", Targets: []TargetConfig{{URL: "https://target", Timeout: 10 * time.Second}}, Concurrency: 1},
		},
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxWait: 50 * time.Millisecond,
	}

	d.Start()
	time.Sleep(50 * time.Millisecond)

	ok := d.Drain(100 * time.Millisecond)
	if ok {
		t.Fatal("Drain should have timed out")
	}
}

func TestDrain_NilStopCh(t *testing.T) {
	// Drain on a dispatcher that was never started should return immediately.
	d := PushDispatcher{}
	ok := d.Drain(time.Second)
	if !ok {
		t.Fatal("expected Drain to return true for unstarted dispatcher")
	}
}
