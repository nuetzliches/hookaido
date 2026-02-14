package pullapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

func TestPullAPI_DequeueAck(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{
		ID:         "evt_1",
		Route:      "/webhooks/github",
		Target:     "pull",
		ReceivedAt: nowVar,
		NextRunAt:  nowVar,
		Payload:    []byte(`{"x":1}`),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rr.Code)
	}

	var got struct {
		Items []struct {
			ID         string `json:"id"`
			LeaseID    string `json:"lease_id"`
			PayloadB64 string `json:"payload_b64"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(got.Items))
	}
	if got.Items[0].ID != "evt_1" {
		t.Fatalf("unexpected id: %q", got.Items[0].ID)
	}
	if got.Items[0].LeaseID == "" {
		t.Fatalf("expected lease_id")
	}
	if got.Items[0].PayloadB64 != base64.StdEncoding.EncodeToString([]byte(`{"x":1}`)) {
		t.Fatalf("unexpected payload_b64")
	}

	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(`{"lease_id":"`+got.Items[0].LeaseID+`"}`))
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusNoContent {
		t.Fatalf("ack status: got %d", rr2.Code)
	}

	rr3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusOK {
		t.Fatalf("dequeue2 status: got %d", rr3.Code)
	}
	var got2 struct {
		Items []any `json:"items"`
	}
	if err := json.Unmarshal(rr3.Body.Bytes(), &got2); err != nil {
		t.Fatalf("decode response2: %v", err)
	}
	if len(got2.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(got2.Items))
	}
}

func TestPullAPI_AckRetryIdempotent(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{
		ID:         "evt_1",
		Route:      "/webhooks/github",
		Target:     "pull",
		ReceivedAt: nowVar,
		NextRunAt:  nowVar,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rrDeq := httptest.NewRecorder()
	reqDeq := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rrDeq, reqDeq)
	if rrDeq.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rrDeq.Code)
	}

	var deq struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rrDeq.Body.Bytes(), &deq); err != nil {
		t.Fatalf("decode dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq.Items))
	}

	body := `{"lease_id":"` + deq.Items[0].LeaseID + `"}`
	rrAck1 := httptest.NewRecorder()
	srv.ServeHTTP(rrAck1, httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(body)))
	if rrAck1.Code != http.StatusNoContent {
		t.Fatalf("ack1 status: got %d body=%s", rrAck1.Code, rrAck1.Body.String())
	}

	rrAck2 := httptest.NewRecorder()
	srv.ServeHTTP(rrAck2, httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(body)))
	if rrAck2.Code != http.StatusNoContent {
		t.Fatalf("ack retry status: got %d body=%s", rrAck2.Code, rrAck2.Body.String())
	}
}

func TestPullAPI_AckBatchIncludesRecentlyCompletedLease(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/webhooks/github", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar}); err != nil {
		t.Fatalf("enqueue evt_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_2", Route: "/webhooks/github", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar}); err != nil {
		t.Fatalf("enqueue evt_2: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rrDeq := httptest.NewRecorder()
	reqDeq := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":2}`))
	srv.ServeHTTP(rrDeq, reqDeq)
	if rrDeq.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rrDeq.Code)
	}

	var deq struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rrDeq.Body.Bytes(), &deq); err != nil {
		t.Fatalf("decode dequeue: %v", err)
	}
	if len(deq.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(deq.Items))
	}

	rrAckSingle := httptest.NewRecorder()
	reqAckSingle := httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(`{"lease_id":"`+deq.Items[0].LeaseID+`"}`))
	srv.ServeHTTP(rrAckSingle, reqAckSingle)
	if rrAckSingle.Code != http.StatusNoContent {
		t.Fatalf("single ack status: got %d body=%s", rrAckSingle.Code, rrAckSingle.Body.String())
	}

	body := `{"lease_ids":["` + deq.Items[0].LeaseID + `","` + deq.Items[1].LeaseID + `"]}`
	rrAckBatch := httptest.NewRecorder()
	reqAckBatch := httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(body))
	srv.ServeHTTP(rrAckBatch, reqAckBatch)
	if rrAckBatch.Code != http.StatusOK {
		t.Fatalf("batch ack status: got %d body=%s", rrAckBatch.Code, rrAckBatch.Body.String())
	}

	var out ackBatchResponse
	if err := json.Unmarshal(rrAckBatch.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode ack batch: %v", err)
	}
	if out.Acked != 2 {
		t.Fatalf("expected acked=2, got %d", out.Acked)
	}
	if len(out.Conflicts) != 0 {
		t.Fatalf("expected no conflicts, got %#v", out.Conflicts)
	}
}

func TestPullAPI_NackRetryIdempotentButAckThenNackStillConflicts(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{
		ID:         "evt_1",
		Route:      "/webhooks/github",
		Target:     "pull",
		ReceivedAt: nowVar,
		NextRunAt:  nowVar,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rrDeq := httptest.NewRecorder()
	srv.ServeHTTP(rrDeq, httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`)))
	if rrDeq.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rrDeq.Code)
	}

	var deq struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rrDeq.Body.Bytes(), &deq); err != nil {
		t.Fatalf("decode dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq.Items))
	}

	leaseID := deq.Items[0].LeaseID
	rrNack1 := httptest.NewRecorder()
	srv.ServeHTTP(rrNack1, httptest.NewRequest(http.MethodPost, "http://example/pull/github/nack", strings.NewReader(`{"lease_id":"`+leaseID+`","delay":"0s"}`)))
	if rrNack1.Code != http.StatusNoContent {
		t.Fatalf("nack1 status: got %d body=%s", rrNack1.Code, rrNack1.Body.String())
	}

	rrNack2 := httptest.NewRecorder()
	srv.ServeHTTP(rrNack2, httptest.NewRequest(http.MethodPost, "http://example/pull/github/nack", strings.NewReader(`{"lease_id":"`+leaseID+`","delay":"0s"}`)))
	if rrNack2.Code != http.StatusNoContent {
		t.Fatalf("nack retry status: got %d body=%s", rrNack2.Code, rrNack2.Body.String())
	}

	rrAck := httptest.NewRecorder()
	srv.ServeHTTP(rrAck, httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(`{"lease_id":"`+leaseID+`"}`)))
	if rrAck.Code != http.StatusConflict {
		t.Fatalf("ack after nack retries should conflict, got %d body=%s", rrAck.Code, rrAck.Body.String())
	}
}

func TestPullAPI_DequeueAckBatch(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/webhooks/github", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar, Payload: []byte(`{"x":1}`)}); err != nil {
		t.Fatalf("enqueue evt_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_2", Route: "/webhooks/github", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar, Payload: []byte(`{"x":2}`)}); err != nil {
		t.Fatalf("enqueue evt_2: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rrDeq := httptest.NewRecorder()
	reqDeq := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":2}`))
	srv.ServeHTTP(rrDeq, reqDeq)
	if rrDeq.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rrDeq.Code)
	}

	var deq struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rrDeq.Body.Bytes(), &deq); err != nil {
		t.Fatalf("decode dequeue: %v", err)
	}
	if len(deq.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(deq.Items))
	}

	body := `{"lease_ids":["` + deq.Items[0].LeaseID + `","` + deq.Items[1].LeaseID + `"]}`
	rrAck := httptest.NewRecorder()
	reqAck := httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(body))
	srv.ServeHTTP(rrAck, reqAck)
	if rrAck.Code != http.StatusOK {
		t.Fatalf("ack batch status: got %d body=%s", rrAck.Code, rrAck.Body.String())
	}

	var ackOut ackBatchResponse
	if err := json.Unmarshal(rrAck.Body.Bytes(), &ackOut); err != nil {
		t.Fatalf("decode ack batch: %v", err)
	}
	if ackOut.Acked != 2 {
		t.Fatalf("expected acked=2, got %d", ackOut.Acked)
	}
	if len(ackOut.Conflicts) != 0 {
		t.Fatalf("expected no conflicts, got %#v", ackOut.Conflicts)
	}

	rrDeq2 := httptest.NewRecorder()
	reqDeq2 := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rrDeq2, reqDeq2)
	if rrDeq2.Code != http.StatusOK {
		t.Fatalf("dequeue2 status: got %d", rrDeq2.Code)
	}
	var deq2 struct {
		Items []any `json:"items"`
	}
	if err := json.Unmarshal(rrDeq2.Body.Bytes(), &deq2); err != nil {
		t.Fatalf("decode dequeue2: %v", err)
	}
	if len(deq2.Items) != 0 {
		t.Fatalf("expected empty queue after batch ack, got %d items", len(deq2.Items))
	}
}

func TestPullAPI_AckBatchPartialConflict(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/webhooks/github", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rrDeq := httptest.NewRecorder()
	reqDeq := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rrDeq, reqDeq)
	if rrDeq.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rrDeq.Code)
	}

	var deq struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rrDeq.Body.Bytes(), &deq); err != nil {
		t.Fatalf("decode dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq.Items))
	}

	body := `{"lease_ids":["` + deq.Items[0].LeaseID + `","missing"]}`
	rrAck := httptest.NewRecorder()
	reqAck := httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(body))
	srv.ServeHTTP(rrAck, reqAck)
	if rrAck.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d body=%s", rrAck.Code, rrAck.Body.String())
	}

	var ackOut ackBatchResponse
	if err := json.Unmarshal(rrAck.Body.Bytes(), &ackOut); err != nil {
		t.Fatalf("decode ack batch: %v", err)
	}
	if ackOut.Acked != 1 {
		t.Fatalf("expected acked=1, got %d", ackOut.Acked)
	}
	if ackOut.Code != pullErrLeaseConflict {
		t.Fatalf("expected code=%q, got %q", pullErrLeaseConflict, ackOut.Code)
	}
	if strings.TrimSpace(ackOut.Detail) == "" {
		t.Fatalf("expected non-empty detail")
	}
	if len(ackOut.Conflicts) != 1 {
		t.Fatalf("expected one conflict, got %#v", ackOut.Conflicts)
	}
	if ackOut.Conflicts[0].LeaseID != "missing" || ackOut.Conflicts[0].Reason != "lease_not_found" {
		t.Fatalf("unexpected conflict: %#v", ackOut.Conflicts[0])
	}
}

func TestPullAPI_ObserverCallbacks(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{
		ID:         "evt_1",
		Route:      "/webhooks/github",
		Target:     "pull",
		ReceivedAt: nowVar,
		NextRunAt:  nowVar,
		Payload:    []byte(`{"x":1}`),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	var dequeueObserved struct {
		route  string
		status int
		items  []queue.Envelope
	}
	var ackObserved struct {
		route        string
		status       int
		leaseID      string
		leaseExpired bool
	}
	srv.ObserveDequeue = func(route string, statusCode int, items []queue.Envelope) {
		dequeueObserved.route = route
		dequeueObserved.status = statusCode
		dequeueObserved.items = append([]queue.Envelope(nil), items...)
	}
	srv.ObserveAck = func(route string, statusCode int, leaseID string, leaseExpired bool) {
		ackObserved.route = route
		ackObserved.status = statusCode
		ackObserved.leaseID = leaseID
		ackObserved.leaseExpired = leaseExpired
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rr.Code)
	}

	if dequeueObserved.route != "/webhooks/github" {
		t.Fatalf("observe dequeue route: got %q", dequeueObserved.route)
	}
	if dequeueObserved.status != http.StatusOK {
		t.Fatalf("observe dequeue status: got %d", dequeueObserved.status)
	}
	if len(dequeueObserved.items) != 1 {
		t.Fatalf("observe dequeue item count: got %d", len(dequeueObserved.items))
	}
	leaseID := dequeueObserved.items[0].LeaseID
	if leaseID == "" {
		t.Fatalf("observe dequeue should include lease_id")
	}

	rrAck := httptest.NewRecorder()
	reqAck := httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(`{"lease_id":"`+leaseID+`"}`))
	srv.ServeHTTP(rrAck, reqAck)
	if rrAck.Code != http.StatusNoContent {
		t.Fatalf("ack status: got %d", rrAck.Code)
	}
	if ackObserved.route != "/webhooks/github" {
		t.Fatalf("observe ack route: got %q", ackObserved.route)
	}
	if ackObserved.status != http.StatusNoContent {
		t.Fatalf("observe ack status: got %d", ackObserved.status)
	}
	if ackObserved.leaseID != leaseID {
		t.Fatalf("observe ack lease_id: got %q, want %q", ackObserved.leaseID, leaseID)
	}
	if ackObserved.leaseExpired {
		t.Fatalf("observe ack leaseExpired should be false")
	}
}

func TestPullAPI_ObserverAckExpiredConflict(t *testing.T) {
	store := &stubStore{ackErr: queue.ErrLeaseExpired}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	var observed struct {
		route        string
		status       int
		leaseID      string
		leaseExpired bool
	}
	srv.ObserveAck = func(route string, statusCode int, leaseID string, leaseExpired bool) {
		observed.route = route
		observed.status = statusCode
		observed.leaseID = leaseID
		observed.leaseExpired = leaseExpired
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/ack", strings.NewReader(`{"lease_id":"lease_1"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
	if observed.route != "/x" {
		t.Fatalf("observe ack route: got %q", observed.route)
	}
	if observed.status != http.StatusConflict {
		t.Fatalf("observe ack status: got %d", observed.status)
	}
	if observed.leaseID != "lease_1" {
		t.Fatalf("observe ack lease_id: got %q", observed.leaseID)
	}
	if !observed.leaseExpired {
		t.Fatalf("observe ack should mark leaseExpired=true")
	}
}

func TestPullAPI_AckBatchRejectsMixedLeaseIDForms(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/ack", strings.NewReader(`{"lease_id":"l1","lease_ids":["l2"]}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrInvalidBody {
		t.Fatalf("expected error code %q, got %q", pullErrInvalidBody, errResp.Code)
	}
}

func TestPullAPI_AckBatchRejectsTooLarge(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.MaxLeaseBatch = 2
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/ack", strings.NewReader(`{"lease_ids":["l1","l2","l3"]}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrInvalidBody {
		t.Fatalf("expected error code %q, got %q", pullErrInvalidBody, errResp.Code)
	}
}

func TestPullAPI_NackBatchDelay(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/webhooks/github", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar}); err != nil {
		t.Fatalf("enqueue evt_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_2", Route: "/webhooks/github", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar}); err != nil {
		t.Fatalf("enqueue evt_2: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rrDeq := httptest.NewRecorder()
	reqDeq := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":2,"lease_ttl":"30s"}`))
	srv.ServeHTTP(rrDeq, reqDeq)
	if rrDeq.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rrDeq.Code)
	}

	var deq struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rrDeq.Body.Bytes(), &deq); err != nil {
		t.Fatalf("decode dequeue: %v", err)
	}
	if len(deq.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(deq.Items))
	}

	rrNack := httptest.NewRecorder()
	reqNack := httptest.NewRequest(http.MethodPost, "http://example/pull/github/nack", strings.NewReader(`{"lease_ids":["`+deq.Items[0].LeaseID+`","`+deq.Items[1].LeaseID+`"],"delay":"5s"}`))
	srv.ServeHTTP(rrNack, reqNack)
	if rrNack.Code != http.StatusOK {
		t.Fatalf("nack batch status: got %d body=%s", rrNack.Code, rrNack.Body.String())
	}
	var nackOut nackBatchResponse
	if err := json.Unmarshal(rrNack.Body.Bytes(), &nackOut); err != nil {
		t.Fatalf("decode nack batch: %v", err)
	}
	if nackOut.Succeeded != 2 {
		t.Fatalf("expected succeeded=2, got %d", nackOut.Succeeded)
	}
	if len(nackOut.Conflicts) != 0 {
		t.Fatalf("expected no conflicts, got %#v", nackOut.Conflicts)
	}

	rrDeq2 := httptest.NewRecorder()
	reqDeq2 := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rrDeq2, reqDeq2)
	if rrDeq2.Code != http.StatusOK {
		t.Fatalf("dequeue2 status: got %d", rrDeq2.Code)
	}
	var deq2 struct {
		Items []any `json:"items"`
	}
	if err := json.Unmarshal(rrDeq2.Body.Bytes(), &deq2); err != nil {
		t.Fatalf("decode dequeue2: %v", err)
	}
	if len(deq2.Items) != 0 {
		t.Fatalf("expected no ready items before delay elapsed, got %d", len(deq2.Items))
	}

	nowVar = nowVar.Add(5 * time.Second)
	rrDeq3 := httptest.NewRecorder()
	reqDeq3 := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":2,"lease_ttl":"30s"}`))
	srv.ServeHTTP(rrDeq3, reqDeq3)
	if rrDeq3.Code != http.StatusOK {
		t.Fatalf("dequeue3 status: got %d", rrDeq3.Code)
	}
	var deq3 struct {
		Items []any `json:"items"`
	}
	if err := json.Unmarshal(rrDeq3.Body.Bytes(), &deq3); err != nil {
		t.Fatalf("decode dequeue3: %v", err)
	}
	if len(deq3.Items) != 2 {
		t.Fatalf("expected 2 items after delay, got %d", len(deq3.Items))
	}
}

func TestPullAPI_NackBatchDeadPartialConflict(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/webhooks/github", Target: "pull", ReceivedAt: nowVar, NextRunAt: nowVar}); err != nil {
		t.Fatalf("enqueue evt_1: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rrDeq := httptest.NewRecorder()
	reqDeq := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1,"lease_ttl":"30s"}`))
	srv.ServeHTTP(rrDeq, reqDeq)
	if rrDeq.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rrDeq.Code)
	}
	var deq struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rrDeq.Body.Bytes(), &deq); err != nil {
		t.Fatalf("decode dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq.Items))
	}

	rrNack := httptest.NewRecorder()
	reqNack := httptest.NewRequest(http.MethodPost, "http://example/pull/github/nack", strings.NewReader(`{"lease_ids":["`+deq.Items[0].LeaseID+`","missing"],"dead":true,"reason":"no_retry"}`))
	srv.ServeHTTP(rrNack, reqNack)
	if rrNack.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d body=%s", rrNack.Code, rrNack.Body.String())
	}
	var nackOut nackBatchResponse
	if err := json.Unmarshal(rrNack.Body.Bytes(), &nackOut); err != nil {
		t.Fatalf("decode nack batch: %v", err)
	}
	if nackOut.Code != pullErrLeaseConflict {
		t.Fatalf("expected code=%q, got %q", pullErrLeaseConflict, nackOut.Code)
	}
	if nackOut.Succeeded != 1 {
		t.Fatalf("expected succeeded=1, got %d", nackOut.Succeeded)
	}
	if len(nackOut.Conflicts) != 1 || nackOut.Conflicts[0].LeaseID != "missing" {
		t.Fatalf("unexpected conflicts: %#v", nackOut.Conflicts)
	}

	dead, err := store.ListDead(queue.DeadListRequest{Route: "/webhooks/github", Limit: 10})
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(dead.Items) != 1 {
		t.Fatalf("expected one dead item, got %d", len(dead.Items))
	}
}

func TestPullAPI_ObserverDequeueBadBody(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	var observed struct {
		route  string
		status int
	}
	srv.ObserveDequeue = func(route string, statusCode int, _ []queue.Envelope) {
		observed.route = route
		observed.status = statusCode
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1,"unknown":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	if observed.route != "/webhooks/github" {
		t.Fatalf("observe dequeue route: got %q", observed.route)
	}
	if observed.status != http.StatusBadRequest {
		t.Fatalf("observe dequeue status: got %d", observed.status)
	}
}

func TestPullAPI_ExpiredLeaseIs409(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/webhooks/github", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1,"lease_ttl":"1s"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rr.Code)
	}

	var got struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got.Items) != 1 || got.Items[0].LeaseID == "" {
		t.Fatalf("expected lease_id")
	}

	nowVar = nowVar.Add(2 * time.Second)

	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(`{"lease_id":"`+got.Items[0].LeaseID+`"}`))
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr2.Code)
	}
	errResp := decodePullError(t, rr2)
	if errResp.Code != pullErrLeaseConflict {
		t.Fatalf("expected error code %q, got %q", pullErrLeaseConflict, errResp.Code)
	}

	rr3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusOK {
		t.Fatalf("dequeue2 status: got %d", rr3.Code)
	}

	var got3 struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr3.Body.Bytes(), &got3); err != nil {
		t.Fatalf("decode response2: %v", err)
	}
	if len(got3.Items) != 1 || got3.Items[0].ID != "evt_1" {
		t.Fatalf("expected requeued item")
	}
}

func TestPullAPI_NackDelay(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/webhooks/github", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1,"lease_ttl":"10s"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rr.Code)
	}
	var got struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	leaseID := got.Items[0].LeaseID

	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "http://example/pull/github/nack", strings.NewReader(`{"lease_id":"`+leaseID+`","delay":"5s"}`))
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusNoContent {
		t.Fatalf("nack status: got %d", rr2.Code)
	}

	rr3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusOK {
		t.Fatalf("dequeue2 status: got %d", rr3.Code)
	}
	var got3 struct {
		Items []any `json:"items"`
	}
	if err := json.Unmarshal(rr3.Body.Bytes(), &got3); err != nil {
		t.Fatalf("decode response2: %v", err)
	}
	if len(got3.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(got3.Items))
	}

	nowVar = nowVar.Add(5 * time.Second)
	rr4 := httptest.NewRecorder()
	req4 := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rr4, req4)
	if rr4.Code != http.StatusOK {
		t.Fatalf("dequeue3 status: got %d", rr4.Code)
	}
	var got4 struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr4.Body.Bytes(), &got4); err != nil {
		t.Fatalf("decode response3: %v", err)
	}
	if len(got4.Items) != 1 || got4.Items[0].ID != "evt_1" {
		t.Fatalf("expected item after delay")
	}
}

func TestPullAPI_NackDead(t *testing.T) {
	store := &stubStore{
		dequeueResp: queue.DequeueResponse{
			Items: []queue.Envelope{
				{LeaseID: "lease_1"},
			},
		},
	}

	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/nack", strings.NewReader(`{"lease_id":"lease_1","dead":true,"reason":"no_retry"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("nack dead status: got %d", rr.Code)
	}
	if !store.markDeadCalled {
		t.Fatalf("expected MarkDead to be called")
	}
	if store.markDeadLease != "lease_1" {
		t.Fatalf("expected lease_1, got %q", store.markDeadLease)
	}
	if store.markDeadReason != "no_retry" {
		t.Fatalf("expected reason, got %q", store.markDeadReason)
	}
	if store.nackCalled {
		t.Fatalf("expected Nack not to be called")
	}
}

func TestPullAPI_UnknownEndpointIs404(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/unknown/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrRouteNotFound {
		t.Fatalf("expected error code %q, got %q", pullErrRouteNotFound, errResp.Code)
	}
}

func TestPullAPI_DequeueRejectsUnknownFields(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1,"unknown":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrInvalidBody {
		t.Fatalf("expected error code %q, got %q", pullErrInvalidBody, errResp.Code)
	}
}

func TestPullAPI_DequeueRejectsTrailingJSON(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}{"batch":2}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrInvalidBody {
		t.Fatalf("expected error code %q, got %q", pullErrInvalidBody, errResp.Code)
	}
}

func TestPullAPI_DequeueAppliesServerLimits(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}
	srv.MaxBatch = 2
	srv.DefaultLeaseTTL = 45 * time.Second
	srv.MaxLeaseTTL = 60 * time.Second
	srv.DefaultMaxWait = 3 * time.Second
	srv.MaxWait = 5 * time.Second

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":10,"max_wait":"30s"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rr.Code)
	}
	if store.lastDequeueReq.Batch != 2 {
		t.Fatalf("expected batch clamp to 2, got %d", store.lastDequeueReq.Batch)
	}
	if store.lastDequeueReq.LeaseTTL != 45*time.Second {
		t.Fatalf("expected default lease ttl 45s, got %s", store.lastDequeueReq.LeaseTTL)
	}
	if store.lastDequeueReq.MaxWait != 5*time.Second {
		t.Fatalf("expected max_wait cap 5s, got %s", store.lastDequeueReq.MaxWait)
	}
	if store.lastDequeueReq.Route != "/webhooks/github" {
		t.Fatalf("expected resolved route, got %q", store.lastDequeueReq.Route)
	}
	if store.lastDequeueReq.Target != "pull" {
		t.Fatalf("expected target pull, got %q", store.lastDequeueReq.Target)
	}
}

func TestPullAPI_DequeueAppliesMaxLeaseTTL(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}
	srv.MaxLeaseTTL = 60 * time.Second

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1,"lease_ttl":"90s"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rr.Code)
	}
	if store.lastDequeueReq.LeaseTTL != 60*time.Second {
		t.Fatalf("expected lease_ttl cap 60s, got %s", store.lastDequeueReq.LeaseTTL)
	}
}

func TestPullAPI_DequeueAppliesDefaultMaxWait(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}
	srv.DefaultMaxWait = 4 * time.Second

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dequeue status: got %d", rr.Code)
	}
	if store.lastDequeueReq.MaxWait != 4*time.Second {
		t.Fatalf("expected default max_wait 4s, got %s", store.lastDequeueReq.MaxWait)
	}
}

func TestPullAPI_AckRejectsUnknownFields(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(`{"lease_id":"lease_1","unknown":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrInvalidBody {
		t.Fatalf("expected error code %q, got %q", pullErrInvalidBody, errResp.Code)
	}
}

func TestPullAPI_AckRejectsTrailingJSON(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/pull/github/ack", strings.NewReader(`{"lease_id":"lease_1"}{"lease_id":"lease_2"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrInvalidBody {
		t.Fatalf("expected error code %q, got %q", pullErrInvalidBody, errResp.Code)
	}
}

type stubStore struct {
	dequeueResp    queue.DequeueResponse
	dequeueErr     error
	lastDequeueReq queue.DequeueRequest
	ackErr         error
	nackCalled     bool
	nackErr        error
	markDeadCalled bool
	markDeadLease  string
	markDeadReason string
	markDeadErr    error
	extendErr      error
	extendCalled   bool
}

func (s *stubStore) Enqueue(env queue.Envelope) error {
	return nil
}

func (s *stubStore) Dequeue(req queue.DequeueRequest) (queue.DequeueResponse, error) {
	s.lastDequeueReq = req
	return s.dequeueResp, s.dequeueErr
}

func (s *stubStore) Ack(leaseID string) error {
	return s.ackErr
}

func (s *stubStore) Nack(leaseID string, delay time.Duration) error {
	s.nackCalled = true
	return s.nackErr
}

func (s *stubStore) Extend(leaseID string, extendBy time.Duration) error {
	s.extendCalled = true
	return s.extendErr
}

func (s *stubStore) MarkDead(leaseID string, reason string) error {
	s.markDeadCalled = true
	s.markDeadLease = leaseID
	s.markDeadReason = reason
	return s.markDeadErr
}

func (s *stubStore) ListDead(req queue.DeadListRequest) (queue.DeadListResponse, error) {
	return queue.DeadListResponse{}, nil
}

func (s *stubStore) RequeueDead(req queue.DeadRequeueRequest) (queue.DeadRequeueResponse, error) {
	return queue.DeadRequeueResponse{}, nil
}

func (s *stubStore) DeleteDead(req queue.DeadDeleteRequest) (queue.DeadDeleteResponse, error) {
	return queue.DeadDeleteResponse{}, nil
}

func (s *stubStore) ListMessages(req queue.MessageListRequest) (queue.MessageListResponse, error) {
	return queue.MessageListResponse{}, nil
}

func (s *stubStore) LookupMessages(req queue.MessageLookupRequest) (queue.MessageLookupResponse, error) {
	return queue.MessageLookupResponse{}, nil
}

func (s *stubStore) CancelMessages(req queue.MessageCancelRequest) (queue.MessageCancelResponse, error) {
	return queue.MessageCancelResponse{}, nil
}

func (s *stubStore) RequeueMessages(req queue.MessageRequeueRequest) (queue.MessageRequeueResponse, error) {
	return queue.MessageRequeueResponse{}, nil
}

func (s *stubStore) ResumeMessages(req queue.MessageResumeRequest) (queue.MessageResumeResponse, error) {
	return queue.MessageResumeResponse{}, nil
}

func (s *stubStore) CancelMessagesByFilter(req queue.MessageManageFilterRequest) (queue.MessageCancelResponse, error) {
	return queue.MessageCancelResponse{}, nil
}

func (s *stubStore) RequeueMessagesByFilter(req queue.MessageManageFilterRequest) (queue.MessageRequeueResponse, error) {
	return queue.MessageRequeueResponse{}, nil
}

func (s *stubStore) ResumeMessagesByFilter(req queue.MessageManageFilterRequest) (queue.MessageResumeResponse, error) {
	return queue.MessageResumeResponse{}, nil
}

func (s *stubStore) Stats() (queue.Stats, error) {
	return queue.Stats{}, nil
}

func (s *stubStore) RecordAttempt(attempt queue.DeliveryAttempt) error {
	return nil
}

func (s *stubStore) ListAttempts(req queue.AttemptListRequest) (queue.AttemptListResponse, error) {
	return queue.AttemptListResponse{}, nil
}

func TestPullAPI_BearerAuth(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }
	srv.Authorize = BearerTokenAuthorizer([][]byte{[]byte("t1")})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/dequeue", strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrUnauthorized {
		t.Fatalf("expected error code %q, got %q", pullErrUnauthorized, errResp.Code)
	}

	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "http://example/x/dequeue", strings.NewReader(`{"batch":1}`))
	req2.Header.Set("Authorization", "Bearer t1")
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr2.Code)
	}
}

func TestPullAPI_MethodNotAllowedStructuredError(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example/x/dequeue", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrMethodNotAllowed {
		t.Fatalf("expected error code %q, got %q", pullErrMethodNotAllowed, errResp.Code)
	}
}

// ---------- store / operation error paths ----------

func TestPullAPI_DequeueStoreErrorReturns503(t *testing.T) {
	store := &stubStore{dequeueErr: errors.New("disk full")}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/dequeue",
		strings.NewReader(`{"batch":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrStoreUnavailable {
		t.Fatalf("expected code %q, got %q", pullErrStoreUnavailable, errResp.Code)
	}
}

func TestPullAPI_UnknownOperationReturns404(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/bogus", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrOperationNotFound {
		t.Fatalf("expected code %q, got %q", pullErrOperationNotFound, errResp.Code)
	}
}

// ---------- Ack/Nack/MarkDead store error paths ----------

func TestPullAPI_AckStoreErrorReturns500(t *testing.T) {
	store := &stubStore{ackErr: errors.New("disk I/O")}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/ack",
		strings.NewReader(`{"lease_id":"l1"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrInternal {
		t.Fatalf("expected code %q, got %q", pullErrInternal, errResp.Code)
	}
}

func TestPullAPI_AckExpiredLeaseIs409(t *testing.T) {
	store := &stubStore{ackErr: queue.ErrLeaseExpired}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/ack",
		strings.NewReader(`{"lease_id":"l1"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
}

func TestPullAPI_NackExpiredLeaseIs409(t *testing.T) {
	store := &stubStore{nackErr: queue.ErrLeaseExpired}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/nack",
		strings.NewReader(`{"lease_id":"l1","delay":"1s"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
}

func TestPullAPI_NackStoreErrorReturns500(t *testing.T) {
	store := &stubStore{nackErr: errors.New("disk I/O")}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/nack",
		strings.NewReader(`{"lease_id":"l1","delay":"1s"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

func TestPullAPI_MarkDeadExpiredLeaseIs409(t *testing.T) {
	store := &stubStore{markDeadErr: queue.ErrLeaseExpired}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/nack",
		strings.NewReader(`{"lease_id":"l1","dead":true,"reason":"bad"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
}

func TestPullAPI_MarkDeadStoreErrorReturns500(t *testing.T) {
	store := &stubStore{markDeadErr: errors.New("disk I/O")}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/nack",
		strings.NewReader(`{"lease_id":"l1","dead":true,"reason":"bad"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

func decodePullError(t *testing.T, rr *httptest.ResponseRecorder) errorResponse {
	t.Helper()
	if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected JSON content-type, got %q", ct)
	}
	var out errorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode error response: %v (body=%q)", err, rr.Body.String())
	}
	if strings.TrimSpace(out.Code) == "" || strings.TrimSpace(out.Detail) == "" {
		t.Fatalf("expected non-empty error code/detail, got %#v", out)
	}
	return out
}

// ---------- handleExtend tests ----------

func TestPullAPI_ExtendHappyPath(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/extend",
		strings.NewReader(`{"lease_id":"lease_1","extend_by":"30s"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	if !store.extendCalled {
		t.Fatal("expected Extend to be called")
	}
}

func TestPullAPI_ExtendMissingExtendBy(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/extend",
		strings.NewReader(`{"lease_id":"lease_1"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrInvalidBody {
		t.Fatalf("expected code %q, got %q", pullErrInvalidBody, errResp.Code)
	}
}

func TestPullAPI_ExtendInvalidDuration(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/extend",
		strings.NewReader(`{"lease_id":"lease_1","extend_by":"not-a-duration"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrInvalidBody {
		t.Fatalf("expected code %q, got %q", pullErrInvalidBody, errResp.Code)
	}
}

func TestPullAPI_ExtendExpiredLease(t *testing.T) {
	store := &stubStore{extendErr: queue.ErrLeaseExpired}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/extend",
		strings.NewReader(`{"lease_id":"lease_1","extend_by":"30s"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
	errResp := decodePullError(t, rr)
	if errResp.Code != pullErrLeaseConflict {
		t.Fatalf("expected code %q, got %q", pullErrLeaseConflict, errResp.Code)
	}
}

func TestPullAPI_ExtendRejectsUnknownFields(t *testing.T) {
	store := &stubStore{}
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) { return "/x", true }

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/x/extend",
		strings.NewReader(`{"lease_id":"lease_1","extend_by":"30s","unknown":true}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}
