package ingress

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestForwardAuthAuthorize_AllowAndCopyHeaders(t *testing.T) {
	var gotMethod string
	var gotOriginalPath string
	var gotOriginalMethod string
	var gotBody []byte

	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotOriginalPath = r.Header.Get("X-Hookaido-Original-Path")
		gotOriginalMethod = r.Header.Get("X-Hookaido-Original-Method")
		body, _ := io.ReadAll(r.Body)
		gotBody = append([]byte(nil), body...)
		w.Header().Add("X-User-Id", "u-123")
		w.Header().Add("X-Role", "ops")
		w.Header().Add("X-Role", "admin")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer authServer.Close()

	auth := NewForwardAuth(authServer.URL)
	auth.Timeout = time.Second
	auth.CopyHeaders = []string{"x-user-id", "x-role", "x-missing"}

	req := httptest.NewRequest(http.MethodPut, "http://example.test/webhooks/github", bytes.NewReader([]byte("abc")))
	copied, status := auth.Authorize(req, "/webhooks/github", []byte("abc"))
	if status != 0 {
		t.Fatalf("authorize status: got %d", status)
	}
	if gotMethod != http.MethodPut {
		t.Fatalf("auth method: got %q", gotMethod)
	}
	if gotOriginalPath != "/webhooks/github" {
		t.Fatalf("original path: got %q", gotOriginalPath)
	}
	if gotOriginalMethod != http.MethodPut {
		t.Fatalf("original method: got %q", gotOriginalMethod)
	}
	if !bytes.Equal(gotBody, []byte("abc")) {
		t.Fatalf("auth body: got %q", string(gotBody))
	}
	if len(copied) != 2 {
		t.Fatalf("copied headers: got %#v", copied)
	}
	if copied["X-User-Id"] != "u-123" {
		t.Fatalf("copied X-User-Id: got %q", copied["X-User-Id"])
	}
	if copied["X-Role"] != "ops,admin" {
		t.Fatalf("copied X-Role: got %q", copied["X-Role"])
	}
}

func TestForwardAuthAuthorize_DenyAndFailClosed(t *testing.T) {
	tests := []struct {
		name       string
		authStatus int
		wantStatus int
	}{
		{name: "unauthorized", authStatus: http.StatusUnauthorized, wantStatus: http.StatusUnauthorized},
		{name: "forbidden", authStatus: http.StatusForbidden, wantStatus: http.StatusForbidden},
		{name: "internal_error", authStatus: http.StatusInternalServerError, wantStatus: http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.authStatus)
			}))
			defer authServer.Close()

			auth := NewForwardAuth(authServer.URL)
			auth.Timeout = time.Second

			req := httptest.NewRequest(http.MethodPost, "http://example.test/webhooks/github", nil)
			_, status := auth.Authorize(req, "/webhooks/github", []byte(`{"x":1}`))
			if status != tt.wantStatus {
				t.Fatalf("status: got %d want %d", status, tt.wantStatus)
			}
		})
	}
}

func TestForwardAuthAuthorize_BodyLimitTruncatesRequestBody(t *testing.T) {
	var gotBody []byte
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = append([]byte(nil), body...)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer authServer.Close()

	auth := NewForwardAuth(authServer.URL)
	auth.Timeout = time.Second
	auth.BodyLimitBytes = 2

	body := []byte{0x00, 0xff, 0x7f}
	req := httptest.NewRequest(http.MethodPost, "http://example.test/webhooks/github", nil)
	_, status := auth.Authorize(req, "/webhooks/github", body)
	if status != 0 {
		t.Fatalf("authorize status: got %d", status)
	}
	if !bytes.Equal(gotBody, body[:2]) {
		t.Fatalf("truncated body: got %#v want %#v", gotBody, body[:2])
	}
}

func TestForwardAuthAuthorize_TimeoutReturnsServiceUnavailable(t *testing.T) {
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer authServer.Close()

	auth := NewForwardAuth(authServer.URL)
	auth.Timeout = 20 * time.Millisecond

	req := httptest.NewRequest(http.MethodPost, "http://example.test/webhooks/github", nil)
	_, status := auth.Authorize(req, "/webhooks/github", []byte("x"))
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d", status)
	}
}
