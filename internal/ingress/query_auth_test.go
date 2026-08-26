package ingress

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/queue"
)

func queryAuthRequest(t *testing.T, rawQuery string) *http.Request {
	t.Helper()
	url := "http://example/webhooks/source"
	if rawQuery != "" {
		url += "?" + rawQuery
	}
	return httptest.NewRequest(http.MethodPost, url, strings.NewReader(`{}`))
}

func queryAuthSnapshot(param string, tokens ...string) func(*http.Request, string) (RouteSnapshot, bool) {
	secrets := make([][]byte, 0, len(tokens))
	for _, tok := range tokens {
		secrets = append(secrets, []byte(tok))
	}
	return func(_ *http.Request, requestPath string) (RouteSnapshot, bool) {
		if !matchPath(requestPath, "/webhooks/source") {
			return RouteSnapshot{}, false
		}
		return RouteSnapshot{
			Route:     "/webhooks/source",
			QueryAuth: NewQueryAuth(param, secrets),
		}, true
	}
}

func TestQueryAuth_Verify(t *testing.T) {
	auth := NewQueryAuth("t", [][]byte{[]byte("s3cr3t")})

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "correct token", query: "t=s3cr3t", want: true},
		{name: "wrong token", query: "t=nope", want: false},
		{name: "empty value", query: "t=", want: false},
		{name: "parameter absent", query: "other=s3cr3t", want: false},
		{name: "no query at all", query: "", want: false},
		{name: "prefix of the token", query: "t=s3cr3", want: false},
		{name: "token with trailing space", query: "t=s3cr3t%20", want: false},
		{name: "case sensitive", query: "t=S3CR3T", want: false},
		{name: "correct among several values", query: "t=nope&t=s3cr3t", want: true},
		{name: "url-encoded token decodes", query: "t=s3cr%33t", want: true},
		{name: "parameter name is case sensitive", query: "T=s3cr3t", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := auth.Verify(queryAuthRequest(t, tt.query)); got != tt.want {
				t.Fatalf("Verify(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func TestQueryAuth_MultipleStaticTokensAllAccepted(t *testing.T) {
	auth := NewQueryAuth("t", [][]byte{[]byte("old"), []byte("new")})

	for _, tok := range []string{"old", "new"} {
		if !auth.Verify(queryAuthRequest(t, "t="+tok)) {
			t.Fatalf("expected %q to be accepted", tok)
		}
	}
	if auth.Verify(queryAuthRequest(t, "t=other")) {
		t.Fatal("expected an unconfigured token to be rejected")
	}
}

// A pool supplies the accepted tokens at verification time, which is what gives
// a rotation its overlap window: two live versions are valid at once.
func TestQueryAuth_SelectSecretsGivesRotationOverlap(t *testing.T) {
	cutover := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	now := cutover.Add(-time.Hour)

	auth := NewQueryAuth("t", nil)
	auth.Now = func() time.Time { return now }
	auth.SelectSecrets = func(at time.Time) [][]byte {
		if at.Before(cutover) {
			return [][]byte{[]byte("v1"), []byte("v2")}
		}
		return [][]byte{[]byte("v2")}
	}

	if !auth.Verify(queryAuthRequest(t, "t=v1")) {
		t.Fatal("expected the outgoing version to be accepted during overlap")
	}
	if !auth.Verify(queryAuthRequest(t, "t=v2")) {
		t.Fatal("expected the incoming version to be accepted during overlap")
	}

	now = cutover.Add(time.Hour)
	if auth.Verify(queryAuthRequest(t, "t=v1")) {
		t.Fatal("expected the retired version to be rejected after cutover")
	}
	if !auth.Verify(queryAuthRequest(t, "t=v2")) {
		t.Fatal("expected the live version to stay accepted")
	}
}

// A static token and a pool can be combined; both are candidates.
func TestQueryAuth_StaticTokenSurvivesAlongsideAPool(t *testing.T) {
	auth := NewQueryAuth("t", [][]byte{[]byte("static")})
	auth.SelectSecrets = func(time.Time) [][]byte { return [][]byte{[]byte("pooled")} }

	for _, tok := range []string{"static", "pooled"} {
		if !auth.Verify(queryAuthRequest(t, "t="+tok)) {
			t.Fatalf("expected %q to be accepted", tok)
		}
	}
	if auth.Verify(queryAuthRequest(t, "t=other")) {
		t.Fatal("expected an unconfigured token to be rejected")
	}
}

// Compilation rejects an `auth query` with no secret, so this state is
// unreachable through the config path. It must still fail closed rather than
// waving everything through the way a nil authenticator would.
func TestQueryAuth_NoCandidatesFailsClosed(t *testing.T) {
	auth := NewQueryAuth("t", nil)
	auth.SelectSecrets = func(time.Time) [][]byte { return nil }

	if auth.Verify(queryAuthRequest(t, "t=anything")) {
		t.Fatal("expected a route with no live token to reject every request")
	}
	if auth.Verify(queryAuthRequest(t, "")) {
		t.Fatal("expected a route with no live token to reject a request with no token")
	}
}

func TestQueryAuth_NilReceiverIsNoAuth(t *testing.T) {
	var auth *QueryAuth
	if !auth.Verify(queryAuthRequest(t, "")) {
		t.Fatal("a nil authenticator means the route has no query auth configured")
	}
}

func TestQueryAuth_NilRequestIsRejected(t *testing.T) {
	auth := NewQueryAuth("t", [][]byte{[]byte("s3cr3t")})
	if auth.Verify(nil) {
		t.Fatal("expected a nil request to be rejected")
	}
}

func TestIngress_QueryAuthRejectsWith404AndEnqueuesNothing(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRequest = queryAuthSnapshot("t", "s3cr3t")

	type reject struct {
		route  string
		status int
		reason string
	}
	var rejects []reject
	srv.ObserveReject = func(route string, status int, reason string) {
		rejects = append(rejects, reject{route, status, reason})
	}

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, queryAuthRequest(t, "t=nope"))

	// 404, not 401: for a URL-only source there is no client that benefits from
	// a distinguishable auth error, and 404 does not confirm the path exists.
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", rr.Code)
	}
	if len(rejects) != 1 {
		t.Fatalf("expected exactly one reject observation, got %#v", rejects)
	}
	// The reason distinguishes a wrong token from a genuinely unknown path; the
	// label is the route path, never the token.
	if rejects[0] != (reject{"/webhooks/source", http.StatusNotFound, "auth"}) {
		t.Fatalf("reject observation: got %#v", rejects[0])
	}

	deq, err := store.Dequeue(queue.DequeueRequest{Route: "/webhooks/source", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 0 {
		t.Fatalf("expected nothing enqueued, got %d items", len(deq.Items))
	}
}

func TestIngress_QueryAuthAcceptsAndKeepsTheTokenOutOfTheEnvelope(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRequest = queryAuthSnapshot("t", "s3cr3t")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, queryAuthRequest(t, "t=s3cr3t"))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", rr.Code)
	}

	deq, err := store.Dequeue(queue.DequeueRequest{Route: "/webhooks/source", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(deq.Items))
	}

	// Keeping the token out of the envelope is the property that makes a query
	// token preferable to one in the path, which is simultaneously queue key,
	// access-log field, envelope trace and Prometheus label.
	item := deq.Items[0]
	for k, v := range item.Trace {
		if strings.Contains(v, "s3cr3t") {
			t.Fatalf("token leaked into envelope trace %q = %q", k, v)
		}
	}
	if strings.Contains(item.Route, "s3cr3t") {
		t.Fatalf("token leaked into the route key %q", item.Route)
	}
	if strings.Contains(string(item.Payload), "s3cr3t") {
		t.Fatal("token leaked into the payload")
	}
	if got := item.Trace["path"]; got != "/webhooks/source" {
		t.Fatalf("envelope path: got %q, want the clean path", got)
	}
	for name, value := range item.Headers {
		if strings.Contains(value, "s3cr3t") {
			t.Fatalf("token leaked into forwarded header %q = %q", name, value)
		}
	}
}

// The `match query` workaround this variant replaces rejected before the rate
// limiter, so a wrong token consumed no token budget. That has to survive.
func TestIngress_QueryAuthRunsBeforeTheRateLimiter(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRequest = queryAuthSnapshot("t", "s3cr3t")

	limiterCalls := 0
	srv.AllowRequestFor = func(string) bool {
		limiterCalls++
		return true
	}
	enqueueGate := 0
	srv.AllowEnqueueFor = func(string) (bool, int, string) {
		enqueueGate++
		return true, 0, ""
	}

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, queryAuthRequest(t, "t=nope"))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", rr.Code)
	}
	if limiterCalls != 0 {
		t.Fatalf("rate limiter consulted %d times for a rejected token, want 0", limiterCalls)
	}
	if enqueueGate != 0 {
		t.Fatalf("backpressure gate consulted %d times for a rejected token, want 0", enqueueGate)
	}

	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, queryAuthRequest(t, "t=s3cr3t"))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", rr.Code)
	}
	if limiterCalls != 1 {
		t.Fatalf("rate limiter consulted %d times for an accepted token, want 1", limiterCalls)
	}
}

func TestIngress_QueryAuthRejectsBeforeReadingTheBody(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveRequest = queryAuthSnapshot("t", "s3cr3t")

	body := &countingReader{data: []byte(`{"x":1}`)}
	req := httptest.NewRequest(http.MethodPost, "http://example/webhooks/source?t=nope", body)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", rr.Code)
	}
	if body.reads != 0 {
		t.Fatalf("body was read %d times for a rejected token, want 0", body.reads)
	}
}

type countingReader struct {
	data  []byte
	off   int
	reads int
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.reads++
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}
