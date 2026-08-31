package pullapi

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// waitForConsumers blocks until the registry reports want entries.
//
// Registration happens on the server goroutine after the response head is
// flushed, so a test that has read the head has not necessarily observed the
// registration yet.
func waitForConsumers(t *testing.T, srv *Server, want int) []ConsumerConnection {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := srv.Consumers()
		if len(got) == want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d consumers, got %d after 2s", want, len(got))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestConsumers_ListsAttachedSSEStreams(t *testing.T) {
	srv, store := newSSETestServer(t)
	srv.IdentifyToken = func(r *http.Request) string {
		if r.Header.Get("Authorization") == "Bearer integration-token" {
			return "env.INTEGRATION_TOKEN"
		}
		return "env.WORKSTATION_TOKEN"
	}
	enqueueTestMsg(t, store, "evt_1")

	ts := httptest.NewServer(srv)
	defer ts.Close()

	if got := srv.Consumers(); len(got) != 0 {
		t.Fatalf("expected no consumers before any stream, got %d", len(got))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/pull/github/stream", nil)
	req.Header.Set("Authorization", "Bearer integration-token")
	req.Header.Set("User-Agent", "hookaido-worker/1.0")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}
	defer resp.Body.Close()

	// Read the queued message so messages_sent is non-zero by the time the
	// registry is inspected.
	scanner := bufio.NewScanner(resp.Body)
	if _, isComment, eof := readSSEEvent(scanner); eof || isComment {
		t.Fatalf("expected SSE message event, got eof=%v comment=%v", eof, isComment)
	}

	got := waitForConsumers(t, srv, 1)
	c := got[0]
	if c.Route != "/webhooks/github" {
		t.Fatalf("expected route /webhooks/github, got %q", c.Route)
	}
	if c.Endpoint != "/pull/github" {
		t.Fatalf("expected endpoint /pull/github, got %q", c.Endpoint)
	}
	if c.ID == "" {
		t.Fatal("expected a consumer id")
	}
	if c.RemoteAddr == "" {
		t.Fatal("expected a remote address")
	}
	if c.UserAgent != "hookaido-worker/1.0" {
		t.Fatalf("expected the request User-Agent, got %q", c.UserAgent)
	}
	if c.TokenRef != "env.INTEGRATION_TOKEN" {
		t.Fatalf("expected the matched token reference, got %q", c.TokenRef)
	}
	if c.ConnectedAt.IsZero() {
		t.Fatal("expected a connect timestamp")
	}

	// messages_sent is written by the stream goroutine; give it the same grace
	// as registration itself.
	deadline := time.Now().Add(2 * time.Second)
	for {
		snap := srv.Consumers()
		if len(snap) == 1 && snap[0].MessagesSent == 1 {
			if snap[0].LastMessageAt.IsZero() {
				t.Fatal("expected last_message_at once a message was sent")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected messages_sent 1, got %#v", snap)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestConsumers_TwoConsumersOnOneRouteAreDistinguishable is the case from the
// issue: two consumers attach to the same competing-consumer queue, split the
// traffic, and each of them sees only a fraction arrive. The connection gauge
// says "two"; this is what says which two.
func TestConsumers_TwoConsumersOnOneRouteAreDistinguishable(t *testing.T) {
	srv, _ := newSSETestServer(t)
	srv.IdentifyToken = func(r *http.Request) string {
		return "env." + r.Header.Get("X-Test-Token-Name")
	}

	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for _, name := range []string{"INTEGRATION", "WORKSTATION"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/pull/github/stream", nil)
			req.Header.Set("X-Test-Token-Name", name)
			resp, err := ts.Client().Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			<-ctx.Done()
		}(name)
	}

	got := waitForConsumers(t, srv, 2)
	refs := map[string]bool{}
	ids := map[string]bool{}
	for _, c := range got {
		if c.Route != "/webhooks/github" {
			t.Fatalf("expected both on /webhooks/github, got %q", c.Route)
		}
		refs[c.TokenRef] = true
		ids[c.ID] = true
	}
	if !refs["env.INTEGRATION"] || !refs["env.WORKSTATION"] {
		t.Fatalf("expected both token references to be distinguishable, got %v", refs)
	}
	if len(ids) != 2 {
		t.Fatalf("expected two distinct consumer ids, got %v", ids)
	}

	cancel()
	wg.Wait()
	waitForConsumers(t, srv, 0)
}

func TestConsumers_DisconnectRemovesTheEntryAndReportsIt(t *testing.T) {
	srv, _ := newSSETestServer(t)

	var mu sync.Mutex
	var connected, disconnected []ConsumerConnection
	var disconnectStatus int
	srv.ObserveConsumerConnect = func(c ConsumerConnection) {
		mu.Lock()
		defer mu.Unlock()
		connected = append(connected, c)
	}
	srv.ObserveConsumerDisconnect = func(c ConsumerConnection, statusCode int, _ time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		disconnected = append(disconnected, c)
		disconnectStatus = statusCode
	}

	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/pull/github/stream", nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}

	waitForConsumers(t, srv, 1)

	cancel()
	_ = resp.Body.Close()

	waitForConsumers(t, srv, 0)

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		nConnected, nDisconnected, status := len(connected), len(disconnected), disconnectStatus
		mu.Unlock()
		if nConnected == 1 && nDisconnected == 1 {
			if status != http.StatusOK {
				t.Fatalf("expected status 200 for a clean teardown, got %d", status)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected one connect and one disconnect, got %d/%d", nConnected, nDisconnected)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestConsumers_RejectedStreamIsNotRegistered pins that the registry lists the
// same thing the connection gauge counts: a request that never became a stream
// must not appear as one.
func TestConsumers_RejectedStreamIsNotRegistered(t *testing.T) {
	srv, _ := newSSETestServer(t)
	srv.Authorize = func(r *http.Request) bool { return false }

	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/pull/github/stream")
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	if got := srv.Consumers(); len(got) != 0 {
		t.Fatalf("expected no registered consumer for a rejected request, got %#v", got)
	}

	// Same for an unknown endpoint.
	resp2, err := ts.Client().Get(ts.URL + "/pull/unknown/stream")
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}
	defer resp2.Body.Close()
	if got := srv.Consumers(); len(got) != 0 {
		t.Fatalf("expected no registered consumer for an unknown endpoint, got %#v", got)
	}
}

func TestConsumers_NilServerReturnsNothing(t *testing.T) {
	var srv *Server
	if got := srv.Consumers(); got != nil {
		t.Fatalf("expected nil from a nil server, got %#v", got)
	}
}

func TestBearerTokenIdentifier(t *testing.T) {
	refs := []string{"env.A", "env.B"}
	tokens := [][]byte{[]byte("token-a"), []byte("token-b")}
	identify := BearerTokenIdentifier(refs, tokens)

	cases := []struct {
		name   string
		header string
		want   string
	}{
		{name: "first token", header: "Bearer token-a", want: "env.A"},
		{name: "second token", header: "Bearer token-b", want: "env.B"},
		{name: "unknown token", header: "Bearer nope", want: ""},
		{name: "no header", header: "", want: ""},
		{name: "not bearer", header: "Basic token-a", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/pull/github/stream", nil)
			if tc.header != "" {
				r.Header.Set("Authorization", tc.header)
			}
			if got := identify(r); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}

	t.Run("no tokens configured", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/pull/github/stream", nil)
		r.Header.Set("Authorization", "Bearer token-a")
		if got := BearerTokenIdentifier(nil, nil)(r); got != "" {
			t.Fatalf("expected empty identity without tokens, got %q", got)
		}
	})

	// A token without a matching ref must not be reported under a neighbour's
	// name. Index alignment is the whole contract here.
	t.Run("token beyond the ref list", func(t *testing.T) {
		identify := BearerTokenIdentifier([]string{"env.A"}, [][]byte{[]byte("token-a"), []byte("token-b")})
		r := httptest.NewRequest(http.MethodGet, "/pull/github/stream", nil)
		r.Header.Set("Authorization", "Bearer token-b")
		if got := identify(r); got != "" {
			t.Fatalf("expected no identity for an unlabelled token, got %q", got)
		}
	})
}
