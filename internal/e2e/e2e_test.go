package e2e

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hookaido/internal/dispatcher"
	"hookaido/internal/ingress"
	"hookaido/internal/pullapi"
	"hookaido/internal/queue"
)

// ---------- helpers ----------

func newTestStore(opts ...queue.MemoryOption) *queue.MemoryStore {
	return queue.NewMemoryStore(opts...)
}

func ingressServer(store queue.Store, routes ...string) *ingress.Server {
	srv := ingress.NewServer(store)
	srv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
		for _, r := range routes {
			if matchPrefix(requestPath, r) {
				return r, true
			}
		}
		return "", false
	}
	return srv
}

func pullServer(store queue.Store, routes ...string) *pullapi.Server {
	srv := pullapi.NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		for _, r := range routes {
			if endpoint == r {
				return r, true
			}
		}
		return "", false
	}
	return srv
}

func matchPrefix(path, prefix string) bool {
	if prefix == "/" {
		return true
	}
	if len(path) < len(prefix) {
		return false
	}
	return path[:len(prefix)] == prefix && (len(path) == len(prefix) || path[len(prefix)] == '/')
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func readJSON(t *testing.T, resp *http.Response, dst any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

type dequeueResp struct {
	Items []dequeueItem `json:"items"`
}

type dequeueItem struct {
	ID         string            `json:"id"`
	LeaseID    string            `json:"lease_id"`
	Attempt    int               `json:"attempt"`
	Route      string            `json:"route"`
	PayloadB64 string            `json:"payload_b64"`
	Headers    map[string]string `json:"headers,omitempty"`
	Trace      map[string]string `json:"trace,omitempty"`
}

// ---------- E2E tests ----------

func TestE2E_IngressPullRoundTrip(t *testing.T) {
	store := newTestStore()
	ing := httptest.NewServer(ingressServer(store, "/hooks/github"))
	defer ing.Close()
	pull := httptest.NewServer(pullServer(store, "/hooks/github"))
	defer pull.Close()

	// 1. POST a webhook to ingress
	payload := `{"action":"opened","number":42}`
	resp, err := http.Post(ing.URL+"/hooks/github/events", "application/json", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatalf("ingress POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingress status: got %d, want 202", resp.StatusCode)
	}

	// 2. Dequeue from pull API
	deqResp := postJSON(t, pull.URL+"/hooks/github/dequeue", map[string]int{"batch": 1})
	if deqResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(deqResp.Body)
		deqResp.Body.Close()
		t.Fatalf("dequeue status: got %d, body: %s", deqResp.StatusCode, body)
	}

	var deq dequeueResp
	readJSON(t, deqResp, &deq)
	if len(deq.Items) != 1 {
		t.Fatalf("dequeue items: got %d, want 1", len(deq.Items))
	}

	decoded, err := base64.StdEncoding.DecodeString(deq.Items[0].PayloadB64)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	if string(decoded) != payload {
		t.Fatalf("payload mismatch: got %q, want %q", decoded, payload)
	}
	if deq.Items[0].Route != "/hooks/github" {
		t.Fatalf("route: got %q, want /hooks/github", deq.Items[0].Route)
	}

	// 3. ACK the message
	ackResp := postJSON(t, pull.URL+"/hooks/github/ack", map[string]string{"lease_id": deq.Items[0].LeaseID})
	ackResp.Body.Close()
	if ackResp.StatusCode != http.StatusNoContent {
		t.Fatalf("ack status: got %d, want 204", ackResp.StatusCode)
	}

	// 4. Queue should be empty now
	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ByState[queue.StateQueued] != 0 {
		t.Fatalf("queued items remainig: %d", stats.ByState[queue.StateQueued])
	}
}

func TestE2E_IngressPushRoundTrip(t *testing.T) {
	var delivered atomic.Int32
	var deliveredPayload atomic.Value

	targetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		deliveredPayload.Store(body)
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer targetSrv.Close()

	store := newTestStore()
	ing := httptest.NewServer(ingressServer(store, "/hooks/stripe"))
	defer ing.Close()

	// Start push dispatcher
	pd := &dispatcher.PushDispatcher{
		Store:     store,
		Deliverer: dispatcher.NewHTTPDeliverer(targetSrv.Client(), dispatcher.EgressPolicy{}),
		Routes: []dispatcher.RouteConfig{
			{
				Route:       "/hooks/stripe",
				Targets:     []dispatcher.TargetConfig{{URL: targetSrv.URL, Timeout: 5 * time.Second, Retry: dispatcher.RetryConfig{Max: 1, Base: 100 * time.Millisecond}}},
				Concurrency: 1,
			},
		},
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxWait: 200 * time.Millisecond,
	}
	pd.Start()
	defer pd.Drain(5 * time.Second)

	// POST to ingress, target should be the push URL
	ingSrv := ingressServer(store, "/hooks/stripe")
	ingSrv.TargetsFor = func(route string) []string {
		return []string{targetSrv.URL}
	}
	ingPush := httptest.NewServer(ingSrv)
	defer ingPush.Close()

	payload := `{"type":"invoice.paid"}`
	resp, err := http.Post(ingPush.URL+"/hooks/stripe", "application/json", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatalf("ingress POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingress status: got %d, want 202", resp.StatusCode)
	}

	// Wait for the push dispatcher to deliver
	deadline := time.After(5 * time.Second)
	for delivered.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("push delivery did not occur within 5s")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	got := deliveredPayload.Load().([]byte)
	if string(got) != payload {
		t.Fatalf("delivered payload mismatch: got %q, want %q", got, payload)
	}
}

func TestE2E_PushDLQLifecycle(t *testing.T) {
	// Target fails for the first call → Retry.Max=1 means immediate DLQ →
	// requeue → new dispatcher with high Max → target succeeds → delivered.
	var deliveryCount atomic.Int32

	targetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := deliveryCount.Add(1)
		if n <= 1 { // only first delivery fails
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer targetSrv.Close()

	store := newTestStore()

	// Dispatcher with Retry.Max=1: first attempt (Attempt=1) fails,
	// 1 < 1 is false → immediately marks dead.
	pd := &dispatcher.PushDispatcher{
		Store:     store,
		Deliverer: dispatcher.NewHTTPDeliverer(targetSrv.Client(), dispatcher.EgressPolicy{}),
		Routes: []dispatcher.RouteConfig{
			{
				Route: "/hooks/dlq",
				Targets: []dispatcher.TargetConfig{{
					URL:     targetSrv.URL,
					Timeout: 2 * time.Second,
					Retry:   dispatcher.RetryConfig{Max: 1, Base: 50 * time.Millisecond, Cap: 100 * time.Millisecond, Jitter: 0},
				}},
				Concurrency: 1,
			},
		},
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxWait: 100 * time.Millisecond,
	}
	pd.Start()
	defer pd.Drain(5 * time.Second)

	// Enqueue with target matching the dispatcher target URL
	ingSrv := ingressServer(store, "/hooks/dlq")
	ingSrv.TargetsFor = func(route string) []string { return []string{targetSrv.URL} }
	ingPush := httptest.NewServer(ingSrv)
	defer ingPush.Close()

	payload := `{"event":"failed"}`
	resp, err := http.Post(ingPush.URL+"/hooks/dlq", "application/json", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatalf("ingress POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingress status: got %d, want 202", resp.StatusCode)
	}

	// Wait for message to land in DLQ
	deadline := time.After(10 * time.Second)
	for {
		dead, _ := store.ListDead(queue.DeadListRequest{Route: "/hooks/dlq", Limit: 10})
		if len(dead.Items) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("message did not reach DLQ within 10s")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Stop first dispatcher and let its goroutines drain.
	// Drain waits for in-flight deliveries, then returns.
	pd.Drain(5 * time.Second)
	time.Sleep(100 * time.Millisecond)

	// Confirm it's in DLQ
	dead, err := store.ListDead(queue.DeadListRequest{Route: "/hooks/dlq", Limit: 10, IncludePayload: true})
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(dead.Items) != 1 {
		t.Fatalf("dead items: got %d, want 1", len(dead.Items))
	}
	if string(dead.Items[0].Payload) != payload {
		t.Fatalf("dead payload: got %q", dead.Items[0].Payload)
	}

	// Requeue from DLQ, then start new dispatcher with high Retry.Max
	// so the accumulated Attempt count doesn't immediately re-DLQ.
	requeued, err := store.RequeueDead(queue.DeadRequeueRequest{IDs: []string{dead.Items[0].ID}})
	if err != nil {
		t.Fatalf("requeue dead: %v", err)
	}
	if requeued.Requeued != 1 {
		t.Fatalf("requeued: got %d, want 1", requeued.Requeued)
	}

	pd2 := &dispatcher.PushDispatcher{
		Store:     store,
		Deliverer: dispatcher.NewHTTPDeliverer(targetSrv.Client(), dispatcher.EgressPolicy{}),
		Routes: []dispatcher.RouteConfig{
			{
				Route: "/hooks/dlq",
				Targets: []dispatcher.TargetConfig{{
					URL:     targetSrv.URL,
					Timeout: 2 * time.Second,
					Retry:   dispatcher.RetryConfig{Max: 20, Base: 50 * time.Millisecond, Cap: 100 * time.Millisecond, Jitter: 0},
				}},
				Concurrency: 1,
			},
		},
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxWait: 100 * time.Millisecond,
	}
	pd2.Start()
	defer pd2.Drain(5 * time.Second)

	// Target returns 200 for n >= 2 → next delivery should succeed
	deadline = time.After(10 * time.Second)
	for deliveryCount.Load() <= 1 {
		select {
		case <-deadline:
			t.Fatalf("requeued message not re-delivered within 10s (deliveryCount=%d)", deliveryCount.Load())
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Queue should become empty (no queued, leased, or dead)
	deadline = time.After(5 * time.Second)
	for {
		stats, _ := store.Stats()
		if stats.ByState[queue.StateQueued] == 0 && stats.ByState[queue.StateLeased] == 0 && stats.ByState[queue.StateDead] == 0 {
			break
		}
		select {
		case <-deadline:
			stats, _ := store.Stats()
			t.Fatalf("queue not empty after requeue+deliver: %v", stats.ByState)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestE2E_FanoutDelivery(t *testing.T) {
	var mu sync.Mutex
	delivered := map[string][]byte{}

	makeTarget := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			delivered[name] = body
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
	}

	t1 := makeTarget("target-a")
	defer t1.Close()
	t2 := makeTarget("target-b")
	defer t2.Close()

	store := newTestStore()

	// Ingress with fanout to both targets
	ingSrv := ingressServer(store, "/hooks/fanout")
	ingSrv.TargetsFor = func(route string) []string {
		return []string{t1.URL, t2.URL}
	}
	ing := httptest.NewServer(ingSrv)
	defer ing.Close()

	// Start push dispatcher for both targets
	pd := &dispatcher.PushDispatcher{
		Store:     store,
		Deliverer: dispatcher.NewHTTPDeliverer(http.DefaultClient, dispatcher.EgressPolicy{}),
		Routes: []dispatcher.RouteConfig{
			{
				Route: "/hooks/fanout",
				Targets: []dispatcher.TargetConfig{
					{URL: t1.URL, Timeout: 5 * time.Second, Retry: dispatcher.RetryConfig{Max: 1, Base: 100 * time.Millisecond}},
					{URL: t2.URL, Timeout: 5 * time.Second, Retry: dispatcher.RetryConfig{Max: 1, Base: 100 * time.Millisecond}},
				},
				Concurrency: 2,
			},
		},
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxWait: 200 * time.Millisecond,
	}
	pd.Start()
	defer pd.Drain(5 * time.Second)

	payload := `{"event":"deploy"}`
	resp, err := http.Post(ing.URL+"/hooks/fanout", "application/json", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatalf("ingress POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingress status: got %d, want 202", resp.StatusCode)
	}

	// Wait for both targets to receive
	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		gotA := delivered["target-a"]
		gotB := delivered["target-b"]
		mu.Unlock()
		if gotA != nil && gotB != nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("not all targets received: a=%v b=%v", gotA != nil, gotB != nil)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if string(delivered["target-a"]) != payload {
		t.Fatalf("target-a payload: got %q", delivered["target-a"])
	}
	if string(delivered["target-b"]) != payload {
		t.Fatalf("target-b payload: got %q", delivered["target-b"])
	}
}

func TestE2E_QueueBackpressure(t *testing.T) {
	store := newTestStore(queue.WithQueueLimits(1, "reject"))
	ing := httptest.NewServer(ingressServer(store, "/hooks/bp"))
	defer ing.Close()

	// First POST should enqueue successfully
	resp, err := http.Post(ing.URL+"/hooks/bp", "application/json", bytes.NewReader([]byte(`{"n":1}`)))
	if err != nil {
		t.Fatalf("first POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("first status: got %d, want 202", resp.StatusCode)
	}

	// Second POST should be rejected (queue full, max_depth=1)
	resp, err = http.Post(ing.URL+"/hooks/bp", "application/json", bytes.NewReader([]byte(`{"n":2}`)))
	if err != nil {
		t.Fatalf("second POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second status: got %d, want 503", resp.StatusCode)
	}

	// Drain the first message
	deq, err := store.Dequeue(queue.DequeueRequest{Route: "/hooks/bp", Target: "pull", Batch: 1, LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("dequeue items: got %d, want 1", len(deq.Items))
	}
	if err := store.Ack(deq.Items[0].LeaseID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Third POST should succeed now that there is capacity
	resp, err = http.Post(ing.URL+"/hooks/bp", "application/json", bytes.NewReader([]byte(`{"n":3}`)))
	if err != nil {
		t.Fatalf("third POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("third status: got %d, want 202", resp.StatusCode)
	}
}

func TestE2E_PullNackRequeue(t *testing.T) {
	store := newTestStore()
	ing := httptest.NewServer(ingressServer(store, "/hooks/nack"))
	defer ing.Close()
	pull := httptest.NewServer(pullServer(store, "/hooks/nack"))
	defer pull.Close()

	// Enqueue
	resp, err := http.Post(ing.URL+"/hooks/nack", "application/json", bytes.NewReader([]byte(`{"retry":"me"}`)))
	if err != nil {
		t.Fatalf("ingress POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingress status: got %d, want 202", resp.StatusCode)
	}

	// Dequeue
	deqResp := postJSON(t, pull.URL+"/hooks/nack/dequeue", map[string]any{"batch": 1, "lease_ttl": "30s"})
	var deq dequeueResp
	readJSON(t, deqResp, &deq)
	if len(deq.Items) != 1 {
		t.Fatalf("dequeue: got %d items, want 1", len(deq.Items))
	}
	if deq.Items[0].Attempt != 1 {
		t.Fatalf("first attempt: got %d, want 1", deq.Items[0].Attempt)
	}
	leaseID := deq.Items[0].LeaseID

	// NACK with no delay
	nackResp := postJSON(t, pull.URL+"/hooks/nack/nack", map[string]string{"lease_id": leaseID, "delay": "0s"})
	nackResp.Body.Close()
	if nackResp.StatusCode != http.StatusNoContent {
		t.Fatalf("nack status: got %d, want 204", nackResp.StatusCode)
	}

	// Dequeue again — attempt should be incremented
	deqResp2 := postJSON(t, pull.URL+"/hooks/nack/dequeue", map[string]any{"batch": 1, "lease_ttl": "30s"})
	var deq2 dequeueResp
	readJSON(t, deqResp2, &deq2)
	if len(deq2.Items) != 1 {
		t.Fatalf("re-dequeue: got %d items, want 1", len(deq2.Items))
	}
	if deq2.Items[0].Attempt != 2 {
		t.Fatalf("second attempt: got %d, want 2", deq2.Items[0].Attempt)
	}

	// ACK
	ackResp := postJSON(t, pull.URL+"/hooks/nack/ack", map[string]string{"lease_id": deq2.Items[0].LeaseID})
	ackResp.Body.Close()
	if ackResp.StatusCode != http.StatusNoContent {
		t.Fatalf("ack status: got %d, want 204", ackResp.StatusCode)
	}
}

func TestE2E_PullLeaseExtend(t *testing.T) {
	store := newTestStore()
	ing := httptest.NewServer(ingressServer(store, "/hooks/ext"))
	defer ing.Close()
	pull := httptest.NewServer(pullServer(store, "/hooks/ext"))
	defer pull.Close()

	// Enqueue
	resp, err := http.Post(ing.URL+"/hooks/ext", "application/json", bytes.NewReader([]byte(`{"x":1}`)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Dequeue with short lease
	deqResp := postJSON(t, pull.URL+"/hooks/ext/dequeue", map[string]any{"batch": 1, "lease_ttl": "2s"})
	var deq dequeueResp
	readJSON(t, deqResp, &deq)
	if len(deq.Items) != 1 {
		t.Fatalf("dequeue: got %d, want 1", len(deq.Items))
	}

	// Extend lease
	extResp := postJSON(t, pull.URL+"/hooks/ext/extend", map[string]string{
		"lease_id":  deq.Items[0].LeaseID,
		"extend_by": "30s",
	})
	extResp.Body.Close()
	if extResp.StatusCode != http.StatusNoContent {
		t.Fatalf("extend status: got %d, want 204", extResp.StatusCode)
	}

	// ACK should still work after extending
	ackResp := postJSON(t, pull.URL+"/hooks/ext/ack", map[string]string{"lease_id": deq.Items[0].LeaseID})
	ackResp.Body.Close()
	if ackResp.StatusCode != http.StatusNoContent {
		t.Fatalf("ack status: got %d, want 204", ackResp.StatusCode)
	}
}

func TestE2E_IngressNotFoundRoute(t *testing.T) {
	store := newTestStore()
	ing := httptest.NewServer(ingressServer(store, "/hooks/known"))
	defer ing.Close()

	// POST to an unknown route → 404
	resp, err := http.Post(ing.URL+"/hooks/unknown", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}

	// Nothing should be enqueued
	stats, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 0 {
		t.Fatalf("total: got %d, want 0", stats.Total)
	}
}

func TestE2E_ConcurrentIngress(t *testing.T) {
	store := newTestStore()
	ing := httptest.NewServer(ingressServer(store, "/hooks/concurrent"))
	defer ing.Close()
	pull := httptest.NewServer(pullServer(store, "/hooks/concurrent"))
	defer pull.Close()

	const N = 50
	var wg sync.WaitGroup
	wg.Add(N)
	for i := range N {
		go func(i int) {
			defer wg.Done()
			body := []byte(`{"i":` + itoa(i) + `}`)
			resp, err := http.Post(ing.URL+"/hooks/concurrent", "application/json", bytes.NewReader(body))
			if err != nil {
				t.Errorf("POST %d: %v", i, err)
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted {
				t.Errorf("POST %d status: %d", i, resp.StatusCode)
			}
		}(i)
	}
	wg.Wait()

	// All messages should be enqueued
	stats, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != N {
		t.Fatalf("total: got %d, want %d", stats.Total, N)
	}

	// Drain all via pull API
	acked := 0
	for acked < N {
		deqResp := postJSON(t, pull.URL+"/hooks/concurrent/dequeue", map[string]any{"batch": 10, "lease_ttl": "30s"})
		var deq dequeueResp
		readJSON(t, deqResp, &deq)
		for _, item := range deq.Items {
			ack := postJSON(t, pull.URL+"/hooks/concurrent/ack", map[string]string{"lease_id": item.LeaseID})
			ack.Body.Close()
			acked++
		}
		if len(deq.Items) == 0 {
			break
		}
	}
	if acked != N {
		t.Fatalf("acked: got %d, want %d", acked, N)
	}
}

func itoa(i int) string {
	buf := [20]byte{}
	pos := len(buf)
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func TestE2E_IngressObserveResultAndQueueStats(t *testing.T) {
	store := newTestStore()
	ing := ingressServer(store, "/hooks/obs")

	var acceptedTotal atomic.Int64
	var rejectedTotal atomic.Int64
	var enqueuedTotal atomic.Int64
	ing.ObserveResult = func(accepted bool, enqueued int) {
		if accepted {
			acceptedTotal.Add(1)
		} else {
			rejectedTotal.Add(1)
		}
		enqueuedTotal.Add(int64(enqueued))
	}

	srv := httptest.NewServer(ing)
	defer srv.Close()

	// 1. POST 3 accepted webhooks.
	for i := range 3 {
		body := []byte(`{"i":` + itoa(i) + `}`)
		resp, err := http.Post(srv.URL+"/hooks/obs", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("POST %d status: got %d, want 202", i, resp.StatusCode)
		}
	}

	// 2. POST to unknown route → rejected.
	resp, err := http.Post(srv.URL+"/unknown", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatalf("POST unknown: %v", err)
	}
	resp.Body.Close()

	// 3. Verify callback counters.
	if got := acceptedTotal.Load(); got != 3 {
		t.Fatalf("accepted: got %d, want 3", got)
	}
	if got := rejectedTotal.Load(); got != 1 {
		t.Fatalf("rejected: got %d, want 1", got)
	}
	if got := enqueuedTotal.Load(); got != 3 {
		t.Fatalf("enqueued: got %d, want 3", got)
	}

	// 4. Verify queue stats.
	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ByState[queue.StateQueued] != 3 {
		t.Fatalf("queued: got %d, want 3", stats.ByState[queue.StateQueued])
	}
}

func TestE2E_PushDeliveryAttemptObservation(t *testing.T) {
	// Target server: fail first attempt, succeed on retries.
	var attempts atomic.Int32
	targetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n <= 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer targetSrv.Close()

	store := newTestStore()

	var acked atomic.Int64
	var retried atomic.Int64

	pd := &dispatcher.PushDispatcher{
		Store:     store,
		Deliverer: dispatcher.NewHTTPDeliverer(targetSrv.Client(), dispatcher.EgressPolicy{}),
		Routes: []dispatcher.RouteConfig{
			{
				Route: "/hooks/push",
				Targets: []dispatcher.TargetConfig{{
					URL:     targetSrv.URL,
					Timeout: 2 * time.Second,
					Retry:   dispatcher.RetryConfig{Max: 3, Base: 50 * time.Millisecond},
				}},
				Concurrency: 1,
			},
		},
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxWait: 200 * time.Millisecond,
		ObserveAttempt: func(outcome queue.AttemptOutcome) {
			switch outcome {
			case queue.AttemptOutcomeAcked:
				acked.Add(1)
			case queue.AttemptOutcomeRetry:
				retried.Add(1)
			}
		},
	}

	// Enqueue a message.
	if err := store.Enqueue(queue.Envelope{
		ID:     "evt_obs",
		Route:  "/hooks/push",
		State:  queue.StateQueued,
		Target: targetSrv.URL,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	pd.Start()
	defer pd.Drain(5 * time.Second)

	// Wait for delivery to complete (delivered or dead).
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for delivery")
		default:
		}
		s, _ := store.Stats()
		if s.ByState[queue.StateQueued] == 0 && s.ByState[queue.StateLeased] == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if got := retried.Load(); got < 1 {
		t.Fatalf("retried: got %d, want >= 1", got)
	}
	if got := acked.Load(); got != 1 {
		t.Fatalf("acked: got %d, want 1", got)
	}
}
