package dispatcher

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
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

func TestHandleDelivery_ObserveDeadReason(t *testing.T) {
	store := &stubPushStore{}
	var observedOutcome queue.AttemptOutcome
	var observedReason string
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: http.StatusServiceUnavailable}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ObserveAttempt: func(outcome queue.AttemptOutcome) {
			observedOutcome = outcome
		},
		ObserveDead: func(reason string) {
			observedReason = reason
		},
	}

	env := queue.Envelope{Route: "/r", Target: "https://x", LeaseID: "lease_1", Attempt: 3}
	target := TargetConfig{
		URL:     "https://x",
		Timeout: time.Second,
		Retry:   RetryConfig{Max: 3, Base: time.Second, Cap: 10 * time.Second},
	}
	d.handleDelivery(d.Logger, env, target)

	if observedOutcome != queue.AttemptOutcomeDead {
		t.Fatalf("observed outcome=%q, want dead", observedOutcome)
	}
	if observedReason != "max_retries" {
		t.Fatalf("observed dead reason=%q, want max_retries", observedReason)
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
	target := req.Target
	if target == "" {
		target = "https://target"
	}
	return queue.DequeueResponse{
		Items: []queue.Envelope{
			{ID: "evt-1", Route: req.Route, Target: target, LeaseID: "lease-1", Attempt: 1, Payload: []byte(`{}`)},
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

type stopRequeueStore struct {
	stubPushStore
	mu        sync.Mutex
	dequeued  bool
	onDequeue func()
}

func (s *stopRequeueStore) Dequeue(req queue.DequeueRequest) (queue.DequeueResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dequeued {
		time.Sleep(req.MaxWait)
		return queue.DequeueResponse{}, nil
	}
	s.dequeued = true
	if s.onDequeue != nil {
		s.onDequeue()
	}
	target := req.Target
	if target == "" {
		target = "https://known-target"
	}
	return queue.DequeueResponse{
		Items: []queue.Envelope{
			{
				ID:      "evt-stop",
				Route:   req.Route,
				Target:  target,
				LeaseID: "lease-stop",
				Attempt: 1,
				Payload: []byte(`{}`),
			},
		},
	}, nil
}

type missingTargetStore struct {
	stubPushStore
	mu       sync.Mutex
	dequeued bool
}

func (s *missingTargetStore) Dequeue(req queue.DequeueRequest) (queue.DequeueResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dequeued {
		time.Sleep(req.MaxWait)
		return queue.DequeueResponse{}, nil
	}
	s.dequeued = true
	return queue.DequeueResponse{
		Items: []queue.Envelope{
			{
				ID:      "evt-missing-target",
				Route:   req.Route,
				Target:  "https://unknown-target",
				LeaseID: "lease-missing-target",
				Attempt: 1,
				Payload: []byte(`{}`),
			},
		},
	}, nil
}

func TestRunRoute_StopBeforeDeliveryRequeuesLeasedItem(t *testing.T) {
	d := PushDispatcher{
		Deliverer: staticDeliverer{res: Result{StatusCode: 200}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		stopCh:    make(chan struct{}),
	}
	store := &stopRequeueStore{
		onDequeue: func() {
			close(d.stopCh)
		},
	}
	d.Store = store
	d.wg.Add(1)

	done := make(chan struct{})
	go func() {
		d.runRoute(
			d.Logger,
			"/test",
			map[string]TargetConfig{
				"https://known-target": {URL: "https://known-target", Timeout: 5 * time.Second},
			},
			20*time.Millisecond,
			30*time.Second,
			1,
		)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runRoute did not stop after stop signal")
	}

	if store.nackLeaseID != "lease-stop" {
		t.Fatalf("expected stop-time requeue via nack for lease-stop, got %q", store.nackLeaseID)
	}
	if store.nackDelay != 0 {
		t.Fatalf("expected stop-time nack delay=0, got %s", store.nackDelay)
	}
}

type batchLeaseMutationStore struct {
	stubPushStore
	mu            sync.Mutex
	items         []queue.Envelope
	ackCalls      int
	ackBatchCalls int
	ackBatchIDs   []string
	ackBatchErr   error
}

func (s *batchLeaseMutationStore) Dequeue(req queue.DequeueRequest) (queue.DequeueResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.items) == 0 {
		time.Sleep(req.MaxWait)
		return queue.DequeueResponse{}, nil
	}
	n := req.Batch
	if n <= 0 || n > len(s.items) {
		n = len(s.items)
	}
	out := make([]queue.Envelope, n)
	copy(out, s.items[:n])
	s.items = s.items[n:]
	return queue.DequeueResponse{Items: out}, nil
}

func (s *batchLeaseMutationStore) Ack(leaseID string) error {
	s.mu.Lock()
	s.ackCalls++
	s.mu.Unlock()
	return s.stubPushStore.Ack(leaseID)
}

func (s *batchLeaseMutationStore) AckBatch(leaseIDs []string) (queue.LeaseBatchResult, error) {
	s.mu.Lock()
	s.ackBatchCalls++
	s.ackBatchIDs = append(s.ackBatchIDs, leaseIDs...)
	err := s.ackBatchErr
	s.mu.Unlock()
	if err != nil {
		return queue.LeaseBatchResult{}, err
	}
	return queue.LeaseBatchResult{Succeeded: len(leaseIDs)}, nil
}

func (s *batchLeaseMutationStore) NackBatch(_ []string, _ time.Duration) (queue.LeaseBatchResult, error) {
	return queue.LeaseBatchResult{}, nil
}

func (s *batchLeaseMutationStore) MarkDeadBatch(_ []string, _ string) (queue.LeaseBatchResult, error) {
	return queue.LeaseBatchResult{}, nil
}

func (s *batchLeaseMutationStore) snapshot() (ackCalls int, ackBatchCalls int, ackBatchIDs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ackBatchIDs = append(ackBatchIDs, s.ackBatchIDs...)
	return s.ackCalls, s.ackBatchCalls, ackBatchIDs
}

func TestRunRoute_UsesAckBatchWhenStoreSupportsLeaseBatch(t *testing.T) {
	store := &batchLeaseMutationStore{
		items: []queue.Envelope{
			{
				ID:      "evt-1",
				Route:   "/test",
				Target:  "https://known-target",
				LeaseID: "lease-1",
				Attempt: 1,
				Payload: []byte(`{}`),
			},
			{
				ID:      "evt-2",
				Route:   "/test",
				Target:  "https://known-target",
				LeaseID: "lease-2",
				Attempt: 1,
				Payload: []byte(`{}`),
			},
		},
	}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: 200}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		stopCh:    make(chan struct{}),
	}
	d.wg.Add(1)

	done := make(chan struct{})
	go func() {
		d.runRoute(
			d.Logger,
			"/test",
			map[string]TargetConfig{
				"https://known-target": {URL: "https://known-target", Timeout: 5 * time.Second},
			},
			20*time.Millisecond,
			30*time.Second,
			2,
		)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, ackBatchCalls, _ := store.snapshot()
		if ackBatchCalls > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	ackCalls, ackBatchCalls, ackBatchIDs := store.snapshot()
	if ackBatchCalls != 1 {
		close(d.stopCh)
		<-done
		t.Fatalf("expected one AckBatch call, got %d", ackBatchCalls)
	}
	if ackCalls != 0 {
		close(d.stopCh)
		<-done
		t.Fatalf("expected zero single Ack calls, got %d", ackCalls)
	}
	if len(ackBatchIDs) != 2 || ackBatchIDs[0] != "lease-1" || ackBatchIDs[1] != "lease-2" {
		close(d.stopCh)
		<-done
		t.Fatalf("unexpected AckBatch lease IDs: %#v", ackBatchIDs)
	}

	close(d.stopCh)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runRoute did not stop")
	}
}

func TestRunRoute_AckBatchFallbackUsesSingleAckOnBatchError(t *testing.T) {
	store := &batchLeaseMutationStore{
		ackBatchErr: errors.New("ack batch failed"),
		items: []queue.Envelope{
			{
				ID:      "evt-1",
				Route:   "/test",
				Target:  "https://known-target",
				LeaseID: "lease-1",
				Attempt: 1,
				Payload: []byte(`{}`),
			},
		},
	}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: 200}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		stopCh:    make(chan struct{}),
	}
	d.wg.Add(1)

	done := make(chan struct{})
	go func() {
		d.runRoute(
			d.Logger,
			"/test",
			map[string]TargetConfig{
				"https://known-target": {URL: "https://known-target", Timeout: 5 * time.Second},
			},
			20*time.Millisecond,
			30*time.Second,
			2,
		)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ackCalls, _, _ := store.snapshot()
		if ackCalls > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	ackCalls, ackBatchCalls, _ := store.snapshot()
	if ackBatchCalls != 1 {
		close(d.stopCh)
		<-done
		t.Fatalf("expected one AckBatch call, got %d", ackBatchCalls)
	}
	if ackCalls != 1 {
		close(d.stopCh)
		<-done
		t.Fatalf("expected fallback single Ack call count=1, got %d", ackCalls)
	}

	close(d.stopCh)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runRoute did not stop")
	}
}

func TestRunRoute_MultiTargetSkipsAckBatchMutations(t *testing.T) {
	store := &batchLeaseMutationStore{
		items: []queue.Envelope{
			{
				ID:      "evt-1",
				Route:   "/test",
				Target:  "https://target-a",
				LeaseID: "lease-1",
				Attempt: 1,
				Payload: []byte(`{}`),
			},
			{
				ID:      "evt-2",
				Route:   "/test",
				Target:  "https://target-b",
				LeaseID: "lease-2",
				Attempt: 1,
				Payload: []byte(`{}`),
			},
		},
	}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: 200}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		stopCh:    make(chan struct{}),
	}
	d.wg.Add(1)

	done := make(chan struct{})
	go func() {
		d.runRoute(
			d.Logger,
			"/test",
			map[string]TargetConfig{
				"https://target-a": {URL: "https://target-a", Timeout: 5 * time.Second},
				"https://target-b": {URL: "https://target-b", Timeout: 5 * time.Second},
			},
			20*time.Millisecond,
			30*time.Second,
			2,
		)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ackCalls, _, _ := store.snapshot()
		if ackCalls >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	ackCalls, ackBatchCalls, _ := store.snapshot()
	if ackBatchCalls != 0 {
		close(d.stopCh)
		<-done
		t.Fatalf("expected no AckBatch calls for multi-target route, got %d", ackBatchCalls)
	}
	if ackCalls != 2 {
		close(d.stopCh)
		<-done
		t.Fatalf("expected two single Ack calls, got %d", ackCalls)
	}

	close(d.stopCh)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runRoute did not stop")
	}
}

func TestRunRoute_MissingTargetNacksWithBackoff(t *testing.T) {
	store := &missingTargetStore{}
	d := PushDispatcher{
		Store:     store,
		Deliverer: staticDeliverer{res: Result{StatusCode: 200}},
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		stopCh:    make(chan struct{}),
	}
	d.wg.Add(1)

	done := make(chan struct{})
	go func() {
		d.runRoute(
			d.Logger,
			"/test",
			map[string]TargetConfig{
				"https://known-target": {URL: "https://known-target", Timeout: 5 * time.Second},
			},
			20*time.Millisecond,
			30*time.Second,
			1,
		)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if store.nackLeaseID == "lease-missing-target" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if store.nackLeaseID != "lease-missing-target" {
		close(d.stopCh)
		<-done
		t.Fatal("expected missing target lease to be nacked")
	}
	if store.nackDelay != time.Second {
		close(d.stopCh)
		<-done
		t.Fatalf("expected missing target nack delay=1s, got %s", store.nackDelay)
	}

	close(d.stopCh)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runRoute did not stop")
	}
}

func TestTargetConfigByURL(t *testing.T) {
	targets := []TargetConfig{
		{URL: "https://a", Timeout: 5 * time.Second},
		{URL: "https://b", Timeout: 7 * time.Second},
	}
	byURL := targetConfigByURL(targets)
	if len(byURL) != 2 {
		t.Fatalf("target map len=%d, want 2", len(byURL))
	}
	if byURL["https://a"].Timeout != 5*time.Second {
		t.Fatalf("target a timeout=%s, want 5s", byURL["https://a"].Timeout)
	}
	if byURL["https://b"].Timeout != 7*time.Second {
		t.Fatalf("target b timeout=%s, want 7s", byURL["https://b"].Timeout)
	}
}

func TestRouteLeaseTTL(t *testing.T) {
	targets := []TargetConfig{
		{URL: "https://fast", Timeout: 2 * time.Second},
		{URL: "https://slow", Timeout: 9 * time.Second},
	}
	if got := routeLeaseTTL(targets, 30*time.Second); got != 39*time.Second {
		t.Fatalf("routeLeaseTTL=%s, want 39s", got)
	}

	if got := routeLeaseTTL([]TargetConfig{{URL: "https://default"}}, 5*time.Second); got != 30*time.Second {
		t.Fatalf("routeLeaseTTL default=%s, want 30s floor", got)
	}
}

func TestRouteDequeueBatch(t *testing.T) {
	cases := []struct {
		name        string
		concurrency int
		targetCount int
		want        int
	}{
		{name: "zero defaults to one", concurrency: 0, targetCount: 1, want: 1},
		{name: "single worker", concurrency: 1, targetCount: 1, want: 1},
		{name: "two workers", concurrency: 2, targetCount: 1, want: 2},
		{name: "three workers", concurrency: 3, targetCount: 1, want: 3},
		{name: "capped at four", concurrency: 8, targetCount: 1, want: 4},
		{name: "multi target caps dequeue at two", concurrency: 8, targetCount: 2, want: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := routeDequeueBatch(tc.concurrency, tc.targetCount); got != tc.want {
				t.Fatalf("routeDequeueBatch(%d,%d)=%d, want %d", tc.concurrency, tc.targetCount, got, tc.want)
			}
		})
	}
}

func TestRouteMutationBatch(t *testing.T) {
	cases := []struct {
		name         string
		dequeueBatch int
		want         int
	}{
		{name: "single", dequeueBatch: 1, want: 1},
		{name: "two", dequeueBatch: 2, want: 2},
		{name: "three maps to two", dequeueBatch: 3, want: 2},
		{name: "four maps to two", dequeueBatch: 4, want: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := routeMutationBatch(tc.dequeueBatch); got != tc.want {
				t.Fatalf("routeMutationBatch(%d)=%d, want %d", tc.dequeueBatch, got, tc.want)
			}
		})
	}
}

type concurrencyProbeStore struct {
	stubPushStore
	mu    sync.Mutex
	next  int
	total int
}

func (s *concurrencyProbeStore) Dequeue(req queue.DequeueRequest) (queue.DequeueResponse, error) {
	s.mu.Lock()
	if s.next >= s.total {
		s.mu.Unlock()
		time.Sleep(req.MaxWait)
		return queue.DequeueResponse{}, nil
	}
	id := s.next
	s.next++
	s.mu.Unlock()

	leaseID := "lease-" + strconv.Itoa(id)
	eventID := "evt-" + strconv.Itoa(id)
	target := req.Target
	if target == "" {
		target = "https://target"
	}
	return queue.DequeueResponse{
		Items: []queue.Envelope{
			{
				ID:      eventID,
				Route:   req.Route,
				Target:  target,
				LeaseID: leaseID,
				Attempt: 1,
				Payload: []byte(`{}`),
			},
		},
	}, nil
}

type gatingDeliverer struct {
	gate        <-chan struct{}
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (d *gatingDeliverer) Deliver(ctx context.Context, _ Delivery) Result {
	inFlight := d.inFlight.Add(1)
	for {
		prev := d.maxInFlight.Load()
		if inFlight <= prev {
			break
		}
		if d.maxInFlight.CompareAndSwap(prev, inFlight) {
			break
		}
	}
	defer d.inFlight.Add(-1)

	select {
	case <-d.gate:
		return Result{StatusCode: http.StatusNoContent}
	case <-ctx.Done():
		return Result{Err: ctx.Err()}
	}
}

func TestDispatcher_StartSingleTargetUsesConfiguredConcurrency(t *testing.T) {
	const routeConcurrency = 4

	store := &concurrencyProbeStore{total: routeConcurrency * 2}
	gate := make(chan struct{})
	var gateClose sync.Once
	releaseGate := func() {
		gateClose.Do(func() { close(gate) })
	}
	deliverer := &gatingDeliverer{gate: gate}

	d := PushDispatcher{
		Store:     store,
		Deliverer: deliverer,
		Routes: []RouteConfig{
			{
				Route: "/test",
				Targets: []TargetConfig{
					{
						URL:     "https://target",
						Timeout: 5 * time.Second,
						Retry: RetryConfig{
							Max: 1,
						},
					},
				},
				Concurrency: routeConcurrency,
			},
		},
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxWait: 10 * time.Millisecond,
	}

	d.Start()
	defer releaseGate()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if int(deliverer.maxInFlight.Load()) >= routeConcurrency {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	gotMax := int(deliverer.maxInFlight.Load())
	if gotMax < routeConcurrency {
		releaseGate()
		_ = d.Drain(100 * time.Millisecond)
		t.Fatalf("max in-flight deliveries=%d, want at least %d", gotMax, routeConcurrency)
	}

	// Unblock all in-flight deliveries and drain workers.
	releaseGate()
	if ok := d.Drain(2 * time.Second); !ok {
		t.Fatal("expected dispatcher drain to complete")
	}
}

type multiTargetProbeStore struct {
	stubPushStore
	mu        sync.Mutex
	hotTarget string
	next      int
	total     int
}

func (s *multiTargetProbeStore) Dequeue(req queue.DequeueRequest) (queue.DequeueResponse, error) {
	s.mu.Lock()
	if s.next >= s.total {
		s.mu.Unlock()
		time.Sleep(req.MaxWait)
		return queue.DequeueResponse{}, nil
	}

	// Optional target filtering is supported by the stub, but runRoute dequeues route-wide.
	if req.Target != "" && req.Target != s.hotTarget {
		s.mu.Unlock()
		time.Sleep(req.MaxWait)
		return queue.DequeueResponse{}, nil
	}

	id := s.next
	s.next++
	s.mu.Unlock()

	leaseID := "lease-hot-" + strconv.Itoa(id)
	eventID := "evt-hot-" + strconv.Itoa(id)
	return queue.DequeueResponse{
		Items: []queue.Envelope{
			{
				ID:      eventID,
				Route:   req.Route,
				Target:  s.hotTarget,
				LeaseID: leaseID,
				Attempt: 1,
				Payload: []byte(`{}`),
			},
		},
	}, nil
}

func TestDispatcher_StartMultiTargetSharesWorkersAcrossTargets(t *testing.T) {
	const routeConcurrency = 4

	hotTarget := "https://hot-target"
	coldTarget := "https://cold-target"

	store := &multiTargetProbeStore{
		hotTarget: hotTarget,
		total:     routeConcurrency * 3,
	}
	gate := make(chan struct{})
	var gateClose sync.Once
	releaseGate := func() {
		gateClose.Do(func() { close(gate) })
	}
	deliverer := &gatingDeliverer{gate: gate}

	d := PushDispatcher{
		Store:     store,
		Deliverer: deliverer,
		Routes: []RouteConfig{
			{
				Route: "/test",
				Targets: []TargetConfig{
					{
						URL:     hotTarget,
						Timeout: 5 * time.Second,
						Retry: RetryConfig{
							Max: 1,
						},
					},
					{
						URL:     coldTarget,
						Timeout: 5 * time.Second,
						Retry: RetryConfig{
							Max: 1,
						},
					},
				},
				Concurrency: routeConcurrency,
			},
		},
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxWait: 10 * time.Millisecond,
	}

	d.Start()
	defer releaseGate()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if int(deliverer.maxInFlight.Load()) >= routeConcurrency {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	gotMax := int(deliverer.maxInFlight.Load())
	if gotMax < routeConcurrency {
		releaseGate()
		_ = d.Drain(100 * time.Millisecond)
		t.Fatalf("max in-flight deliveries=%d, want at least %d", gotMax, routeConcurrency)
	}

	releaseGate()
	if ok := d.Drain(2 * time.Second); !ok {
		t.Fatal("expected dispatcher drain to complete")
	}
}
