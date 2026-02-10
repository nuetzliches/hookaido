package pullapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func FuzzPullAPIServer(f *testing.F) {
	f.Add(http.MethodPost, "/pull/github/dequeue", `{"batch":1}`, "Bearer fuzz-token")
	f.Add(http.MethodPost, "/pull/github/ack", `{"lease_id":"lease_1"}`, "")
	f.Add(http.MethodGet, "/pull/github/dequeue", "", "")

	f.Fuzz(func(t *testing.T, method string, requestPath string, body string, authHeader string) {
		if requestPath == "" {
			requestPath = "/pull/github/dequeue"
		}
		if !strings.HasPrefix(requestPath, "/") {
			requestPath = "/" + requestPath
		}
		if len(requestPath) > 256 {
			requestPath = requestPath[:256]
		}
		if len(body) > 1<<20 {
			body = body[:1<<20]
		}
		if strings.ContainsAny(authHeader, "\r\n") {
			authHeader = ""
		}

		srv := NewServer(&stubStore{})
		srv.MaxBatch = 10
		srv.DefaultLeaseTTL = 30 * time.Second
		srv.ResolveRoute = func(endpoint string) (string, bool) {
			if strings.HasPrefix(endpoint, "/pull/") {
				return "/webhooks/fuzz", true
			}
			return "", false
		}
		srv.Authorize = BearerTokenAuthorizer([][]byte{[]byte("fuzz-token")})

		req := &http.Request{
			Method: method,
			URL: &url.URL{
				Scheme: "http",
				Host:   "example.test",
				Path:   requestPath,
			},
			Header: make(http.Header),
			Body:   io.NopCloser(strings.NewReader(body)),
		}
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}

		rr := httptest.NewRecorder()
		srv.ServeHTTP(rr, req)

		if rr.Code < 100 || rr.Code > 599 {
			t.Fatalf("invalid status code: %d", rr.Code)
		}
	})
}

func FuzzBearerTokenAuthorizer(f *testing.F) {
	f.Add("Bearer token-1", "token-1")
	f.Add("", "")
	f.Add("Basic xyz", "token-1")

	f.Fuzz(func(t *testing.T, authHeader string, token string) {
		if len(authHeader) > 2048 {
			authHeader = authHeader[:2048]
		}
		if len(token) > 256 {
			token = token[:256]
		}
		if strings.ContainsAny(authHeader, "\r\n") {
			authHeader = ""
		}

		authorize := BearerTokenAuthorizer([][]byte{
			[]byte(token),
			nil,
			[]byte(""),
		})

		req := httptest.NewRequest(http.MethodPost, "http://example.test/pull/github/dequeue", nil)
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}

		_ = authorize(req)
	})
}
