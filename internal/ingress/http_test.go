package ingress

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"hookaido/internal/queue"
	"hookaido/internal/router"
)

func TestIngress_ResolvedRouteIsEnqueued(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		if router.MatchPath(requestPath, "/webhooks/github") {
			return "/webhooks/github", true
		}
		return "", false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/webhooks/github/events", strings.NewReader(`{"x":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: got %d", rr.Code)
	}

	deq, err := store.Dequeue(queue.DequeueRequest{Route: "/webhooks/github", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq.Items))
	}
	if string(deq.Items[0].Payload) != `{"x":1}` {
		t.Fatalf("unexpected payload")
	}
}

func TestIngress_DeliverTargetsFanout(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		if router.MatchPath(requestPath, "/webhooks/github") {
			return "/webhooks/github", true
		}
		return "", false
	}
	srv.TargetsFor = func(route string) []string {
		if route == "/webhooks/github" {
			return []string{"t1", "t2"}
		}
		return nil
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/webhooks/github", strings.NewReader(`{"x":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: got %d", rr.Code)
	}

	deq1, err := store.Dequeue(queue.DequeueRequest{Route: "/webhooks/github", Target: "t1", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue t1: %v", err)
	}
	if len(deq1.Items) != 1 {
		t.Fatalf("expected 1 item for t1, got %d", len(deq1.Items))
	}

	deq2, err := store.Dequeue(queue.DequeueRequest{Route: "/webhooks/github", Target: "t2", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue t2: %v", err)
	}
	if len(deq2.Items) != 1 {
		t.Fatalf("expected 1 item for t2, got %d", len(deq2.Items))
	}
}

func TestIngress_UnknownRouteIs404(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		return "", false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/nope", strings.NewReader(`x`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}

	deq, err := store.Dequeue(queue.DequeueRequest{Target: "pull", Batch: 10})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(deq.Items))
	}
}

func TestIngress_MethodNotAllowed(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(r *http.Request, requestPath string) (string, bool) {
		if r.Method != http.MethodPost {
			return "", false
		}
		if router.MatchPath(requestPath, "/webhooks/github") {
			return "/webhooks/github", true
		}
		return "", false
	}
	srv.AllowedMethodsFor = func(r *http.Request, requestPath string) []string {
		if router.MatchPath(requestPath, "/webhooks/github") {
			return []string{http.MethodPost}
		}
		return nil
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example/webhooks/github", strings.NewReader(`x`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
	if allow := rr.Header().Get("Allow"); allow != "POST" {
		t.Fatalf("expected Allow POST, got %q", allow)
	}
}

func TestIngress_RateLimited(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		if router.MatchPath(requestPath, "/webhooks/github") {
			return "/webhooks/github", true
		}
		return "", false
	}
	srv.AllowRequestFor = func(route string) bool {
		return false
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/webhooks/github", strings.NewReader(`{"x":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rr.Code)
	}
}

func TestIngress_HMACAuth_ReplayRejected(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now

	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		if router.MatchPath(requestPath, "/webhooks/github") {
			return "/webhooks/github", true
		}
		return "", false
	}

	secret := []byte("s1")
	auth := NewHMACAuth([][]byte{secret})
	auth.Now = func() time.Time { return nowVar }
	srv.HMACAuthFor = func(route string) *HMACAuth {
		if route == "/webhooks/github" {
			return auth
		}
		return nil
	}

	body := `{"x":1}`
	path := "/webhooks/github"
	ts := strconv.FormatInt(nowVar.Unix(), 10)
	nonce := "n1"
	sig := sign(ts, http.MethodPost, path, body, secret)

	req1 := httptest.NewRequest(http.MethodPost, "http://example"+path, strings.NewReader(body))
	req1.Header.Set("X-Timestamp", ts)
	req1.Header.Set("X-Nonce", nonce)
	req1.Header.Set("X-Signature", sig)
	rr1 := httptest.NewRecorder()
	srv.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rr1.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "http://example"+path, strings.NewReader(body))
	req2.Header.Set("X-Timestamp", ts)
	req2.Header.Set("X-Nonce", nonce)
	req2.Header.Set("X-Signature", sig)
	rr2 := httptest.NewRecorder()
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on replay, got %d", rr2.Code)
	}
}

func TestIngress_BasicAuth(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		if router.MatchPath(requestPath, "/webhooks/github") {
			return "/webhooks/github", true
		}
		return "", false
	}
	srv.BasicAuthFor = func(route string) *BasicAuth {
		if route != "/webhooks/github" {
			return nil
		}
		return NewBasicAuth(map[string]string{"ci": "s3cret"})
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/webhooks/github", strings.NewReader(`{"x":1}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "http://example/webhooks/github", strings.NewReader(`{"x":1}`))
	req2.SetBasicAuth("ci", "s3cret")
	rr2 := httptest.NewRecorder()
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rr2.Code)
	}
}

func TestIngress_ForwardAuth_Denied(t *testing.T) {
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer authServer.Close()

	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		if router.MatchPath(requestPath, "/webhooks/github") {
			return "/webhooks/github", true
		}
		return "", false
	}
	srv.ForwardAuthFor = func(route string) *ForwardAuth {
		if route != "/webhooks/github" {
			return nil
		}
		auth := NewForwardAuth(authServer.URL)
		auth.Timeout = time.Second
		return auth
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/webhooks/github", strings.NewReader(`{"x":1}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr.Code)
	}

	deq, err := store.Dequeue(queue.DequeueRequest{Route: "/webhooks/github", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(deq.Items))
	}
}

func TestIngress_ForwardAuth_CopiesHeadersToEnqueuedEnvelope(t *testing.T) {
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Auth-Subject", "user-123")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer authServer.Close()

	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		if router.MatchPath(requestPath, "/webhooks/github") {
			return "/webhooks/github", true
		}
		return "", false
	}
	srv.ForwardAuthFor = func(route string) *ForwardAuth {
		if route != "/webhooks/github" {
			return nil
		}
		auth := NewForwardAuth(authServer.URL)
		auth.Timeout = time.Second
		auth.CopyHeaders = []string{"x-auth-subject"}
		return auth
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/webhooks/github", strings.NewReader(`{"x":1}`))
	req.Header.Set("X-Event", "push")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rr.Code)
	}

	deq, err := store.Dequeue(queue.DequeueRequest{Route: "/webhooks/github", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq.Items))
	}
	if deq.Items[0].Headers["X-Auth-Subject"] != "user-123" {
		t.Fatalf("expected copied header, got %#v", deq.Items[0].Headers)
	}
	if deq.Items[0].Headers["X-Event"] != "push" {
		t.Fatalf("expected original header, got %#v", deq.Items[0].Headers)
	}
}

func TestIngress_HeadersTooLargeRejected(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		if router.MatchPath(requestPath, "/webhooks/github") {
			return "/webhooks/github", true
		}
		return "", false
	}
	srv.MaxHeaderBytes = 32

	req := httptest.NewRequest(http.MethodPost, "http://example/webhooks/github", strings.NewReader(`{"x":1}`))
	req.Header.Set("X-Event", strings.Repeat("a", 64))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}

	deq, err := store.Dequeue(queue.DequeueRequest{Route: "/webhooks/github", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(deq.Items))
	}
}

func TestIngress_ForwardAuthCopiedHeadersTooLargeRejected(t *testing.T) {
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Auth-Subject", strings.Repeat("u", 64))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer authServer.Close()

	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		if router.MatchPath(requestPath, "/webhooks/github") {
			return "/webhooks/github", true
		}
		return "", false
	}
	srv.MaxHeaderBytes = 48
	srv.ForwardAuthFor = func(route string) *ForwardAuth {
		if route != "/webhooks/github" {
			return nil
		}
		auth := NewForwardAuth(authServer.URL)
		auth.Timeout = time.Second
		auth.CopyHeaders = []string{"x-auth-subject"}
		return auth
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/webhooks/github", strings.NewReader(`{"x":1}`))
	req.Header.Set("X-Event", "push")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}

	deq, err := store.Dequeue(queue.DequeueRequest{Route: "/webhooks/github", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(deq.Items))
	}
}

// ---------- enqueue failure ----------

type failingEnqueueStore struct {
	queue.Store
}

func (f *failingEnqueueStore) Enqueue(env queue.Envelope) error {
	return errors.New("synthetic enqueue failure")
}

func TestIngress_EnqueueFailureReturns503(t *testing.T) {
	store := &failingEnqueueStore{Store: queue.NewMemoryStore()}
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		return "/hooks/test", true
	}

	var observedAccepted bool
	var observedEnqueued int
	srv.ObserveResult = func(accepted bool, enqueued int) {
		observedAccepted = accepted
		observedEnqueued = enqueued
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/hooks/test", strings.NewReader(`{"a":1}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	if observedAccepted {
		t.Fatal("expected ObserveResult accepted=false")
	}
	if observedEnqueued != 0 {
		t.Fatalf("expected enqueued=0, got %d", observedEnqueued)
	}
}

// ---------- body size / read errors ----------

func TestIngress_BodyTooLargeReturns413(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.MaxBodyBytes = 10
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		return "/hooks/test", true
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/hooks/test", strings.NewReader(strings.Repeat("x", 100)))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, errors.New("connection reset")
}

func (errReader) Close() error { return nil }

func TestIngress_BodyReadErrorReturns400(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		return "/hooks/test", true
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/hooks/test", nil)
	req.Body = errReader{}
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func sign(ts, method, path, body string, secret []byte) string {
	h := sha256.Sum256([]byte(body))
	msg := ts + "\n" + method + "\n" + path + "\n" + hex.EncodeToString(h[:])
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(msg))
	return hex.EncodeToString(mac.Sum(nil))
}
