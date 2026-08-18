package pullapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/queue"
)

func newSSETestServer(t *testing.T) (*Server, *queue.MemoryStore) {
	t.Helper()
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	srv := NewServer(store)
	srv.SSEKeepalive = 100 * time.Millisecond
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}
	return srv, store
}

func enqueueTestMsg(t *testing.T, store *queue.MemoryStore, id string) {
	t.Helper()
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(queue.Envelope{
		ID:         id,
		Route:      "/webhooks/github",
		Target:     "pull",
		ReceivedAt: now,
		NextRunAt:  now,
		Payload:    []byte(fmt.Sprintf(`{"id":%q}`, id)),
	}); err != nil {
		t.Fatalf("enqueue %s: %v", id, err)
	}
}

// readSSEEvent reads one SSE event (terminated by blank line) or a comment line.
// Returns event lines joined by newline and whether it's a comment.
func readSSEEvent(scanner *bufio.Scanner) (lines []string, isComment bool, eof bool) {
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			// End of event block.
			if len(lines) > 0 {
				return lines, false, false
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			return []string{line}, true, false
		}
		lines = append(lines, line)
	}
	return lines, false, true
}

func TestSSE_BasicStream(t *testing.T) {
	srv, store := newSSETestServer(t)
	enqueueTestMsg(t, store, "evt_1")

	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/pull/github/stream", nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %q", ct)
	}

	scanner := bufio.NewScanner(resp.Body)
	lines, isComment, eof := readSSEEvent(scanner)
	if eof || isComment {
		t.Fatalf("expected SSE event, got eof=%v comment=%v", eof, isComment)
	}

	// Parse SSE fields.
	var eventID, eventType, eventData string
	for _, l := range lines {
		if strings.HasPrefix(l, "id: ") {
			eventID = strings.TrimPrefix(l, "id: ")
		}
		if strings.HasPrefix(l, "event: ") {
			eventType = strings.TrimPrefix(l, "event: ")
		}
		if strings.HasPrefix(l, "data: ") {
			eventData = strings.TrimPrefix(l, "data: ")
		}
	}

	if eventType != "message" {
		t.Fatalf("expected event type 'message', got %q", eventType)
	}
	if eventID == "" {
		t.Fatal("expected non-empty event id (lease_id)")
	}

	var item dequeueItem
	if err := json.Unmarshal([]byte(eventData), &item); err != nil {
		t.Fatalf("unmarshal SSE data: %v", err)
	}
	if item.ID != "evt_1" {
		t.Fatalf("expected item ID 'evt_1', got %q", item.ID)
	}
	if item.LeaseID != eventID {
		t.Fatalf("SSE id (%q) != data lease_id (%q)", eventID, item.LeaseID)
	}

	cancel()
}

func TestSSE_Keepalive(t *testing.T) {
	srv, _ := newSSETestServer(t)

	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/pull/github/stream", nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	lines, isComment, eof := readSSEEvent(scanner)
	if eof {
		t.Fatal("unexpected eof before keepalive")
	}
	if !isComment {
		t.Fatalf("expected keepalive comment, got event: %v", lines)
	}
	if len(lines) != 1 || !strings.Contains(lines[0], "keepalive") {
		t.Fatalf("expected keepalive comment, got %v", lines)
	}

	cancel()
}

func TestSSE_Auth(t *testing.T) {
	srv, _ := newSSETestServer(t)
	srv.Authorize = BearerTokenAuthorizer([][]byte{[]byte("secret")})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Without token.
	resp, err := ts.Client().Get(ts.URL + "/pull/github/stream")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", resp.StatusCode)
	}

	// With token.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/pull/github/stream", nil)
	req.Header.Set("Authorization", "Bearer secret")
	resp2, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request with token: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with token, got %d", resp2.StatusCode)
	}
	cancel()
}

func TestSSE_MaxConnection(t *testing.T) {
	srv, _ := newSSETestServer(t)
	srv.SSEMaxConnection = 200 * time.Millisecond

	ts := httptest.NewServer(srv)
	defer ts.Close()

	start := time.Now()
	resp, err := ts.Client().Get(ts.URL + "/pull/github/stream")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	// Read until EOF (server closes connection).
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		// drain
	}

	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("connection should have closed within ~200ms, took %v", elapsed)
	}
}

func TestSSE_MethodRouting(t *testing.T) {
	srv, _ := newSSETestServer(t)

	// GET on /dequeue should return 405.
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example/pull/github/dequeue", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /dequeue: expected 405, got %d", rr.Code)
	}

	// POST on /stream should return 405.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "http://example/pull/github/stream", strings.NewReader("{}"))
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /stream: expected 405, got %d", rr2.Code)
	}
}

func TestSSE_CompetingConsumers(t *testing.T) {
	srv, store := newSSETestServer(t)

	ts := httptest.NewServer(srv)
	defer ts.Close()

	enqueueTestMsg(t, store, "evt_a")
	enqueueTestMsg(t, store, "evt_b")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	received := make(chan string, 4)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/pull/github/stream", nil)
			resp, err := ts.Client().Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			scanner := bufio.NewScanner(resp.Body)
			for {
				lines, isComment, eof := readSSEEvent(scanner)
				if eof {
					return
				}
				if isComment {
					continue
				}
				for _, l := range lines {
					if strings.HasPrefix(l, "data: ") {
						var item dequeueItem
						if err := json.Unmarshal([]byte(strings.TrimPrefix(l, "data: ")), &item); err == nil {
							received <- item.ID
						}
					}
				}
			}
		}()
	}

	// Wait a bit for both consumers to get their messages.
	timeout := time.After(2 * time.Second)
	ids := make(map[string]bool)
	for len(ids) < 2 {
		select {
		case id := <-received:
			ids[id] = true
		case <-timeout:
			cancel()
			wg.Wait()
			t.Fatalf("timed out, only received %d messages: %v", len(ids), ids)
		}
	}

	cancel()
	wg.Wait()

	if !ids["evt_a"] || !ids["evt_b"] {
		t.Fatalf("expected both evt_a and evt_b, got %v", ids)
	}
}

func TestSSE_BatchParam(t *testing.T) {
	srv, store := newSSETestServer(t)
	enqueueTestMsg(t, store, "evt_1")
	enqueueTestMsg(t, store, "evt_2")

	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/pull/github/stream?batch=5", nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}
	defer resp.Body.Close()

	var items []string
	scanner := bufio.NewScanner(resp.Body)
	// Collect events until keepalive or timeout.
	for {
		lines, isComment, eof := readSSEEvent(scanner)
		if eof || isComment {
			break
		}
		for _, l := range lines {
			if strings.HasPrefix(l, "data: ") {
				var item dequeueItem
				if err := json.Unmarshal([]byte(strings.TrimPrefix(l, "data: ")), &item); err == nil {
					items = append(items, item.ID)
				}
			}
		}
	}
	cancel()

	if len(items) != 2 {
		t.Fatalf("expected 2 items with batch=5, got %d: %v", len(items), items)
	}
}

func TestSSE_InvalidBatchParam(t *testing.T) {
	srv, _ := newSSETestServer(t)

	rr := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodGet, "http://example/pull/github/stream?batch=0", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for batch=0, got %d", rr.Code)
	}

	rr2 := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}
	req2 := httptest.NewRequest(http.MethodGet, "http://example/pull/github/stream?batch=abc", nil)
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for batch=abc, got %d", rr2.Code)
	}
}

func TestSSE_RouteNotFound(t *testing.T) {
	srv, _ := newSSETestServer(t)

	rr := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodGet, "http://example/pull/unknown/stream", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

// flusherRecorder wraps httptest.ResponseRecorder to implement http.Flusher.
type flusherRecorder struct {
	*httptest.ResponseRecorder
}

func (f *flusherRecorder) Flush() {}

// busyStore always has an item ready, so the SSE loop never idles and therefore
// never reaches the select that used to be the only place cancellation was
// handled. The small delay keeps the test from spinning hot.
type busyStore struct {
	queue.Store
	route string
}

func (b busyStore) Dequeue(req queue.DequeueRequest) (queue.DequeueResponse, error) {
	time.Sleep(time.Millisecond)
	return queue.DequeueResponse{Items: []queue.Envelope{{
		ID:      "evt_busy",
		LeaseID: "lease_busy",
		Route:   b.route,
	}}}, nil
}

func newBusySSEServer(t *testing.T) (*Server, *flusherRecorder, *http.Request) {
	t.Helper()
	srv := NewServer(busyStore{Store: queue.NewMemoryStore(), route: "/webhooks/github"})
	srv.SSEKeepalive = time.Hour // must not be what ends the stream
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}
	rr := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodGet, "http://example/pull/github/stream", nil)
	return srv, rr, req
}

// sse_max_connection must bound a busy stream, not just an idle one.
func TestSSE_MaxConnectionEndsABusyStream(t *testing.T) {
	srv, rr, req := newBusySSEServer(t)
	srv.SSEMaxConnection = 50 * time.Millisecond

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeHTTP(rr, req)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never returned: with items always available the loop never checked ctx.Done(), so sse_max_connection did not bound the connection")
	}
}

// A client that goes away mid-stream must end the stream. Without this the
// handler keeps dequeuing and leasing messages that nobody will ever ack.
func TestSSE_ClientDisconnectEndsABusyStream(t *testing.T) {
	srv, rr, req := newBusySSEServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeHTTP(rr, req)
	}()

	// Let it stream a little, then drop the peer.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never returned after the request context was canceled")
	}
}

// The disconnect must also be observed, so hookaido_pull_sse_connection_active
// comes back down. It previously only ever counted up on a busy stream.
func TestSSE_BusyStreamObservesDisconnect(t *testing.T) {
	srv, rr, req := newBusySSEServer(t)
	srv.SSEMaxConnection = 50 * time.Millisecond

	var mu sync.Mutex
	disconnects := 0
	srv.ObserveSSEDisconnect = func(route string, statusCode int, messagesSent int, duration time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		disconnects++
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeHTTP(rr, req)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never returned")
	}

	mu.Lock()
	defer mu.Unlock()
	if disconnects != 1 {
		t.Fatalf("ObserveSSEDisconnect called %d times, want 1", disconnects)
	}
}

// failingWriter accepts the SSE preamble and then fails, as a dropped TCP peer
// would. Errors from the per-item write used to be discarded.
type failingWriter struct {
	*httptest.ResponseRecorder
	mu     sync.Mutex
	writes int
}

func (f *failingWriter) Write(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes++
	if f.writes > 2 {
		return 0, errors.New("connection reset by peer")
	}
	return f.ResponseRecorder.Write(b)
}

func (f *failingWriter) Flush() {}

func TestSSE_WriteFailureEndsTheStream(t *testing.T) {
	srv, _, req := newBusySSEServer(t)
	w := &failingWriter{ResponseRecorder: httptest.NewRecorder()}

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeHTTP(w, req)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never returned: the per-item write error was discarded, so a dropped peer went unnoticed")
	}
}

// lateEnqueueStore publishes a message while the SSE handler is inside its
// non-blocking dequeue, which is the window the lost-wakeup race lives in: the
// notify channel used to be taken *after* this call returned, so the handler
// received the channel the enqueue had already replaced and slept until the
// keepalive tick instead of delivering the ready message.
type lateEnqueueStore struct {
	*queue.MemoryStore
	once sync.Once
	t    *testing.T
}

func (s *lateEnqueueStore) Dequeue(req queue.DequeueRequest) (queue.DequeueResponse, error) {
	return s.DequeueContext(context.Background(), req)
}

// DequeueContext has to be overridden too: pullapi prefers it when the store
// implements queue.ContextDequeuer, and the embedded MemoryStore provides it,
// so without this the wrapper would be bypassed entirely.
func (s *lateEnqueueStore) DequeueContext(ctx context.Context, req queue.DequeueRequest) (queue.DequeueResponse, error) {
	resp, err := s.MemoryStore.DequeueContext(ctx, req)
	if err != nil || len(resp.Items) > 0 {
		return resp, err
	}
	s.once.Do(func() {
		enqueueTestMsg(s.t, s.MemoryStore, "evt_late")
	})
	return resp, err
}

func TestSSE_MessageEnqueuedDuringDequeueIsNotMissed(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	inner := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	store := &lateEnqueueStore{MemoryStore: inner, t: t}

	srv := NewServer(store)
	// Long enough that a missed wakeup cannot be rescued by a keepalive within
	// the test's own deadline.
	srv.SSEKeepalive = 30 * time.Second
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == "/pull/github" {
			return "/webhooks/github", true
		}
		return "", false
	}

	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/pull/github/stream", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer resp.Body.Close()

	received := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for {
			lines, isComment, eof := readSSEEvent(scanner)
			if eof {
				return
			}
			if isComment {
				continue
			}
			received <- strings.Join(lines, "\n")
			return
		}
	}()

	select {
	case event := <-received:
		if !strings.Contains(event, "evt_late") {
			t.Fatalf("unexpected event: %s", event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the message published during the dequeue was never delivered: the stream is waiting on a notify channel that already fired")
	}
}
