package ingress

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/queue"
)

// nonceTestServer builds an ingress server for one HMAC-protected route,
// returning the server and a request factory for signed requests.
func nonceTestServer(t *testing.T, store queue.Store, targets []string) (*Server, func(nonce string) *http.Request, time.Time) {
	t.Helper()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	const path = "/webhooks/github"
	const body = `{"x":1}`
	secret := []byte("s1")

	srv := NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		if matchPath(requestPath, path) {
			return path, true
		}
		return "", false
	}
	if len(targets) > 0 {
		srv.TargetsFor = func(route string) []string { return targets }
	}

	auth := NewHMACAuth([][]byte{secret})
	auth.Now = func() time.Time { return now }
	srv.HMACAuthFor = func(route string) *HMACAuth {
		if route == path {
			return auth
		}
		return nil
	}

	ts := strconv.FormatInt(now.Unix(), 10)
	sig := sign(ts, http.MethodPost, path, body, secret)
	newReq := func(nonce string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "http://example"+path, strings.NewReader(body))
		req.Header.Set("X-Timestamp", ts)
		req.Header.Set("X-Nonce", nonce)
		req.Header.Set("X-Signature", sig)
		return req
	}
	return srv, newReq, now
}

// A 503 explicitly invites a retry, and webhook senders retry by replaying the
// identical signed request. Burning the nonce before the enqueue succeeded
// turned that retry into a 401 for the rest of the tolerance window, so
// transient backpressure became permanent webhook loss.
func TestIngress_NonceSurvivesFailedEnqueue(t *testing.T) {
	memory := queue.NewMemoryStore()
	failing := &failingEnqueueStore{Store: memory}
	toggle := &toggleEnqueueStore{fail: failing, ok: memory, failing: true}

	srv, newReq, _ := nonceTestServer(t, toggle, nil)

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, newReq("n1"))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 while the store rejects, got %d", rr.Code)
	}

	// The sender retries the identical signed request once the queue recovers.
	toggle.failing = false
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, newReq("n1"))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected the retry after a 503 to be accepted, got %d", rr.Code)
	}

	// The claim is permanent again once the message is durably enqueued.
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, newReq("n1"))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on replay of the accepted request, got %d", rr.Code)
	}
}

// The multi-target case is the worse one: target 1 enqueued, target 2 failed,
// so an identical retry could never deliver to target 2 at all.
func TestIngress_NonceSurvivesPartialMultiTargetEnqueue(t *testing.T) {
	memory := queue.NewMemoryStore()
	store := &secondTargetFailingStore{Store: memory, failTarget: "t2"}

	srv, newReq, _ := nonceTestServer(t, store, []string{"t1", "t2"})

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, newReq("n1"))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when the second target fails, got %d", rr.Code)
	}

	store.failTarget = ""
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, newReq("n1"))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected the retry to be accepted, got %d", rr.Code)
	}

	for _, target := range []string{"t1", "t2"} {
		deq, err := memory.Dequeue(queue.DequeueRequest{Route: "/webhooks/github", Target: target, Batch: 10})
		if err != nil {
			t.Fatalf("dequeue %s: %v", target, err)
		}
		if len(deq.Items) == 0 {
			t.Fatalf("target %s received nothing; the retry could not deliver to it", target)
		}
	}
}

// The header-size rejection sits after verification, so it burned the nonce too.
func TestIngress_NonceSurvivesHeaderTooLarge(t *testing.T) {
	store := queue.NewMemoryStore()
	srv, newReq, _ := nonceTestServer(t, store, nil)
	srv.MaxHeaderBytes = 8

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, newReq("n1"))
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}

	srv.MaxHeaderBytes = 64 << 10
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, newReq("n1"))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected the corrected retry to be accepted, got %d", rr.Code)
	}
}

// Releasing must not open a window for a concurrent replay: while the first
// request is still in flight, a second one carrying the same nonce is rejected.
func TestNonceClaim_BlocksConcurrentReplayBeforeCommit(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	c := newNonceCache(func() time.Time { return now })

	seq, ok := c.claim("n1", now.Add(time.Minute))
	if !ok {
		t.Fatal("first claim must succeed")
	}
	if _, ok := c.claim("n1", now.Add(time.Minute)); ok {
		t.Fatal("a second claim on an in-flight nonce must be rejected")
	}

	claim := &NonceClaim{cache: c, nonce: "n1", seq: seq}
	claim.Release()
	if _, ok := c.claim("n1", now.Add(time.Minute)); !ok {
		t.Fatal("after release the nonce must be claimable again")
	}
}

// A late Release must not delete a newer claim on the same nonce.
func TestNonceClaim_ReleaseIsScopedToItsOwnClaim(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	c := newNonceCache(func() time.Time { return now })

	first, _ := c.claim("n1", now.Add(time.Minute))
	stale := &NonceClaim{cache: c, nonce: "n1", seq: first}
	stale.Release()

	second, ok := c.claim("n1", now.Add(time.Minute))
	if !ok {
		t.Fatal("second claim must succeed after the first was released")
	}
	stale.seq = first
	stale.done = false
	stale.Release() // late release of the first claim

	if _, ok := c.claim("n1", now.Add(time.Minute)); ok {
		t.Fatalf("the live claim (seq %d) was deleted by a stale release", second)
	}
}

func TestNonceClaim_CommitMakesReleaseANoop(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	c := newNonceCache(func() time.Time { return now })

	seq, _ := c.claim("n1", now.Add(time.Minute))
	claim := &NonceClaim{cache: c, nonce: "n1", seq: seq}
	claim.Commit()
	claim.Release()

	if _, ok := c.claim("n1", now.Add(time.Minute)); ok {
		t.Fatal("a committed claim must survive Release")
	}
}

func TestNonceClaim_NilIsSafe(t *testing.T) {
	var claim *NonceClaim
	claim.Commit()
	claim.Release()
}

// ---------- stores ----------

// toggleEnqueueStore fails or succeeds depending on a flag, so one test can
// model a queue that rejects and then recovers.
type toggleEnqueueStore struct {
	queue.Store
	fail    queue.Store
	ok      queue.Store
	failing bool
}

func (s *toggleEnqueueStore) Enqueue(env queue.Envelope) error {
	if s.failing {
		return s.fail.Enqueue(env)
	}
	return s.ok.Enqueue(env)
}

func (s *toggleEnqueueStore) Dequeue(req queue.DequeueRequest) (queue.DequeueResponse, error) {
	return s.ok.Dequeue(req)
}

// secondTargetFailingStore rejects one specific target, leaving the other
// enqueued -- the partial multi-target failure.
type secondTargetFailingStore struct {
	queue.Store
	failTarget string
}

func (s *secondTargetFailingStore) Enqueue(env queue.Envelope) error {
	if s.failTarget != "" && env.Target == s.failTarget {
		return queue.ErrQueueFull
	}
	return s.Store.Enqueue(env)
}
