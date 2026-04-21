package mcp

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type rotateSecretMockAdmin struct {
	server *http.Server
	done   chan struct{}
	calls  []rotateSecretMockCall
}

type rotateSecretMockCall struct {
	method string
	path   string
	body   []byte
	reason string
	actor  string
}

func newRotateSecretMockAdmin(t *testing.T, token string, response func(w http.ResponseWriter, r *http.Request, calls *[]rotateSecretMockCall)) (*rotateSecretMockAdmin, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mock := &rotateSecretMockAdmin{done: make(chan struct{})}
	mock.server = &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			response(w, r, &mock.calls)
		}),
	}
	go func() {
		_ = mock.server.Serve(ln)
		close(mock.done)
	}()
	t.Cleanup(func() { _ = mock.server.Close() })
	return mock, ln.Addr().String()
}

func writeRotateSecretConfig(t *testing.T, addr, token string) string {
	t.Helper()
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
secrets {
  secret "cituro" {
    runtime true
  }
}
pull_api { auth token "raw:puller" }
"/r" {
  auth hmac secret_ref "cituro"
  pull { path "/e" }
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return cfgPath
}

func newRotateSecretServer(t *testing.T, cfgPath string) *Server {
	t.Helper()
	return NewServer(nil, nil, cfgPath, "",
		WithMutationsEnabled(true),
		WithRole(RoleAdmin),
		WithPrincipal("test-admin"),
	)
}

func TestRotateSecret_AddSuccess(t *testing.T) {
	const token = "admintoken"
	mock, addr := newRotateSecretMockAdmin(t, token, func(w http.ResponseWriter, r *http.Request, calls *[]rotateSecretMockCall) {
		if r.Method != http.MethodPost || r.URL.Path != "/admin/secrets/cituro" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, _ := io.ReadAll(r.Body)
		*calls = append(*calls, rotateSecretMockCall{
			method: r.Method,
			path:   r.URL.Path,
			body:   body,
			reason: r.Header.Get("X-Hookaido-Audit-Reason"),
			actor:  r.Header.Get("X-Hookaido-Audit-Actor"),
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":         "sec_abc123",
			"not_before": "2026-04-21T10:00:00Z",
			"created_at": "2026-04-21T10:00:00Z",
		})
	})

	cfgPath := writeRotateSecretConfig(t, addr, token)
	s := newRotateSecretServer(t, cfgPath)

	resp := callTool(t, s, "rotate_secret", map[string]any{
		"operation": "add",
		"name":      "cituro",
		"value":     "whs_new",
		"reason":    "rotate for soapNEO",
	})
	if resp.IsError {
		t.Fatalf("unexpected rotate_secret error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if op, _ := out["operation"].(string); op != "add" {
		t.Fatalf("operation=%q want add", op)
	}
	nested, ok := out["response"].(map[string]any)
	if !ok {
		t.Fatalf("nested response type: %T", out["response"])
	}
	if id, _ := nested["id"].(string); id != "sec_abc123" {
		t.Fatalf("id=%q", id)
	}

	if len(mock.calls) != 1 {
		t.Fatalf("expected 1 admin call, got %d", len(mock.calls))
	}
	call := mock.calls[0]
	if call.reason != "rotate for soapNEO" {
		t.Fatalf("reason header=%q", call.reason)
	}
	if call.actor != "test-admin" {
		t.Fatalf("actor header=%q (principal should propagate)", call.actor)
	}
	var sent map[string]any
	if err := json.Unmarshal(call.body, &sent); err != nil {
		t.Fatalf("decode sent body: %v", err)
	}
	if sent["value"] != "whs_new" {
		t.Fatalf("body.value=%v", sent["value"])
	}
}

func TestRotateSecret_ListSuccess(t *testing.T) {
	const token = "admintoken"
	_, addr := newRotateSecretMockAdmin(t, token, func(w http.ResponseWriter, r *http.Request, calls *[]rotateSecretMockCall) {
		if r.Method != http.MethodGet || r.URL.Path != "/admin/secrets/cituro" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name": "cituro",
			"versions": []map[string]any{
				{"id": "sec_a", "not_before": "2026-04-21T10:00:00Z"},
			},
		})
	})

	cfgPath := writeRotateSecretConfig(t, addr, token)
	s := newRotateSecretServer(t, cfgPath)

	resp := callTool(t, s, "rotate_secret", map[string]any{
		"operation": "list",
		"name":      "cituro",
	})
	if resp.IsError {
		t.Fatalf("unexpected rotate_secret list error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	nested, ok := out["response"].(map[string]any)
	if !ok {
		t.Fatalf("nested response: %T", out["response"])
	}
	if nested["name"] != "cituro" {
		t.Fatalf("name=%v", nested["name"])
	}
}

func TestRotateSecret_DeleteSuccess(t *testing.T) {
	const token = "admintoken"
	mock, addr := newRotateSecretMockAdmin(t, token, func(w http.ResponseWriter, r *http.Request, calls *[]rotateSecretMockCall) {
		if r.Method != http.MethodDelete || r.URL.Path != "/admin/secrets/cituro/sec_abc123" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		*calls = append(*calls, rotateSecretMockCall{
			method: r.Method,
			path:   r.URL.Path,
			reason: r.Header.Get("X-Hookaido-Audit-Reason"),
		})
		w.WriteHeader(http.StatusNoContent)
	})

	cfgPath := writeRotateSecretConfig(t, addr, token)
	s := newRotateSecretServer(t, cfgPath)

	resp := callTool(t, s, "rotate_secret", map[string]any{
		"operation": "delete",
		"name":      "cituro",
		"id":        "sec_abc123",
		"reason":    "revoke rotated-out secret",
	})
	if resp.IsError {
		t.Fatalf("unexpected rotate_secret delete error: %s", resp.Content[0].Text)
	}
	if len(mock.calls) != 1 {
		t.Fatalf("expected 1 admin call, got %d", len(mock.calls))
	}
	if mock.calls[0].reason != "revoke rotated-out secret" {
		t.Fatalf("reason=%q", mock.calls[0].reason)
	}
}

func TestRotateSecret_AddRequiresReason(t *testing.T) {
	const token = "admintoken"
	_, addr := newRotateSecretMockAdmin(t, token, func(w http.ResponseWriter, r *http.Request, calls *[]rotateSecretMockCall) {
		// Should never be called — reason validation fails before HTTP.
		t.Fatalf("admin server should not be hit")
	})
	cfgPath := writeRotateSecretConfig(t, addr, token)
	s := newRotateSecretServer(t, cfgPath)

	resp := callTool(t, s, "rotate_secret", map[string]any{
		"operation": "add",
		"name":      "cituro",
		"value":     "x",
		// no reason
	})
	if !resp.IsError {
		t.Fatalf("expected error for missing reason")
	}
	if !strings.Contains(resp.Content[0].Text, "reason") {
		t.Fatalf("error should mention reason: %q", resp.Content[0].Text)
	}
}

func TestRotateSecret_AddRejectsInvalidWindow(t *testing.T) {
	const token = "admintoken"
	_, addr := newRotateSecretMockAdmin(t, token, func(w http.ResponseWriter, r *http.Request, calls *[]rotateSecretMockCall) {
		t.Fatalf("admin server should not be hit")
	})
	cfgPath := writeRotateSecretConfig(t, addr, token)
	s := newRotateSecretServer(t, cfgPath)

	resp := callTool(t, s, "rotate_secret", map[string]any{
		"operation":  "add",
		"name":       "cituro",
		"value":      "x",
		"not_before": "2026-04-21T10:00:00Z",
		"not_after":  "2026-04-20T09:00:00Z",
		"reason":     "r",
	})
	if !resp.IsError {
		t.Fatalf("expected error for invalid window")
	}
	if !strings.Contains(resp.Content[0].Text, "not_after") {
		t.Fatalf("error should mention not_after: %q", resp.Content[0].Text)
	}
}

func TestRotateSecret_UnknownOperation(t *testing.T) {
	const token = "admintoken"
	_, addr := newRotateSecretMockAdmin(t, token, func(w http.ResponseWriter, r *http.Request, calls *[]rotateSecretMockCall) {
		t.Fatalf("admin server should not be hit")
	})
	cfgPath := writeRotateSecretConfig(t, addr, token)
	s := newRotateSecretServer(t, cfgPath)

	resp := callTool(t, s, "rotate_secret", map[string]any{
		"operation": "nuke",
		"name":      "cituro",
	})
	if !resp.IsError {
		t.Fatalf("expected error for unknown operation")
	}
	if !strings.Contains(resp.Content[0].Text, "add|list|delete") {
		t.Fatalf("error should mention valid operations: %q", resp.Content[0].Text)
	}
}

func TestRotateSecret_ListRejectsMutationArgs(t *testing.T) {
	const token = "admintoken"
	_, addr := newRotateSecretMockAdmin(t, token, func(w http.ResponseWriter, r *http.Request, calls *[]rotateSecretMockCall) {
		t.Fatalf("admin server should not be hit")
	})
	cfgPath := writeRotateSecretConfig(t, addr, token)
	s := newRotateSecretServer(t, cfgPath)

	resp := callTool(t, s, "rotate_secret", map[string]any{
		"operation": "list",
		"name":      "cituro",
		"reason":    "should not be accepted",
	})
	if !resp.IsError {
		t.Fatalf("expected error for list with reason")
	}
	if !strings.Contains(resp.Content[0].Text, "list") {
		t.Fatalf("error should mention list: %q", resp.Content[0].Text)
	}
}

func TestRotateSecret_InvalidName(t *testing.T) {
	const token = "admintoken"
	_, addr := newRotateSecretMockAdmin(t, token, func(w http.ResponseWriter, r *http.Request, calls *[]rotateSecretMockCall) {
		t.Fatalf("admin server should not be hit")
	})
	cfgPath := writeRotateSecretConfig(t, addr, token)
	s := newRotateSecretServer(t, cfgPath)

	resp := callTool(t, s, "rotate_secret", map[string]any{
		"operation": "list",
		"name":      "bad name!",
	})
	if !resp.IsError {
		t.Fatalf("expected error for invalid name")
	}
	if !strings.Contains(resp.Content[0].Text, "label pattern") {
		t.Fatalf("error should mention label pattern: %q", resp.Content[0].Text)
	}
}

func TestRotateSecret_RequiresMutationsFlag(t *testing.T) {
	// Without WithMutationsEnabled(true), the tool should be gated.
	cfgPath := writeRotateSecretConfig(t, "127.0.0.1:0", "x")
	s := NewServer(nil, nil, cfgPath, "",
		WithRole(RoleAdmin),
		WithPrincipal("test-admin"),
	)
	resp := callTool(t, s, "rotate_secret", map[string]any{
		"operation": "list",
		"name":      "cituro",
	})
	if !resp.IsError {
		t.Fatalf("expected rotate_secret to be gated by mutations flag")
	}
	if !strings.Contains(resp.Content[0].Text, "enable-mutations") {
		t.Fatalf("error should mention enable-mutations: %q", resp.Content[0].Text)
	}
}

func TestRotateSecret_RequiresAdminRole(t *testing.T) {
	cfgPath := writeRotateSecretConfig(t, "127.0.0.1:0", "x")
	s := NewServer(nil, nil, cfgPath, "",
		WithMutationsEnabled(true),
		WithRole(RoleOperate),
		WithPrincipal("test-op"),
	)
	resp := callTool(t, s, "rotate_secret", map[string]any{
		"operation": "list",
		"name":      "cituro",
	})
	if !resp.IsError {
		t.Fatalf("expected rotate_secret to require RoleAdmin")
	}
	if !strings.Contains(resp.Content[0].Text, "admin") {
		t.Fatalf("error should mention admin role: %q", resp.Content[0].Text)
	}
}

// Static assertion that httptest is imported so linters don't complain if
// future refactors remove its direct usage.
var _ = httptest.NewRecorder
