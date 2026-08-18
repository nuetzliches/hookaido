package mcp

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeStubHealthz answers like a running instance: a plain 200 for the
// liveness probe, and detailed diagnostics carrying the fingerprint of the
// config the instance is running. Pass an empty configPath to model an instance
// that reports no config identity at all.
func writeStubHealthz(t *testing.T, w http.ResponseWriter, r *http.Request, configPath string) {
	t.Helper()
	if r.URL.Query().Get("details") != "true" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}

	diagnostics := map[string]any{}
	if strings.TrimSpace(configPath) != "" {
		content, err := os.ReadFile(configPath)
		if err != nil {
			t.Errorf("stub healthz: read config: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		diagnostics["config"] = map[string]any{
			"fingerprint": configFingerprint(content),
			"generation":  1,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":          true,
		"diagnostics": diagnostics,
	})
}

// startStubAdmin runs a stub admin API whose healthz reports the fingerprint of
// runningPath, and returns its address. runningPath is what the *instance* is
// running, which is not necessarily what was just written to disk.
func startStubAdmin(t *testing.T, token string, runningPath func() string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/admin/healthz" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			writeStubHealthz(t, w, r, runningPath())
		}),
	}
	done := make(chan struct{})
	go func() {
		_ = srv.Serve(ln)
		close(done)
	}()
	t.Cleanup(func() {
		_ = srv.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for the stub admin server to stop")
		}
	})
	return ln.Addr().String()
}

// The reload check used to be a liveness probe: /healthz answers 200 whichever
// config is in force, so config_apply reported ok/applied/reloaded while the
// instance still served the old config -- and rollbackConfigFile could never
// run. This models exactly that: the instance keeps running the old file.
func TestToolConfigApply_ReloadNotAdoptedRollsBack(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "Hookaidofile")
	runningPath := filepath.Join(dir, "running-config")

	const token = "admintoken"
	addr := startStubAdmin(t, token, func() string { return runningPath })

	original := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  deliver "https://example.com" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}
	// The instance is running the original and never picks the new one up.
	if err := os.WriteFile(runningPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write running config: %v", err)
	}

	candidate := strings.Replace(original, "https://example.com", "https://example.org", 1)

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "config_apply", map[string]any{
		"path":           cfgPath,
		"content":        candidate,
		"mode":           "write_and_reload",
		"reload_timeout": "1s",
		"reason":         "apply test",
	})
	if resp.IsError {
		t.Fatalf("unexpected config_apply error: %s", resp.Content[0].Text)
	}

	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); okVal {
		t.Fatal("config_apply reported success while the instance kept the old config")
	}
	if reloaded, _ := out["reloaded"].(bool); reloaded {
		t.Fatal("reloaded=true without the instance adopting the config")
	}
	if rolledBack, _ := out["rolled_back"].(bool); !rolledBack {
		t.Fatal("expected the file to be rolled back")
	}

	got, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if string(got) != original {
		t.Fatalf("config was not restored:\n%s", got)
	}
}

// An instance that cannot report its config identity must not be claimed as a
// verified reload -- but must not be treated as a failure either, since the
// file was written and the old liveness check is all that is available.
func TestToolConfigApply_UnverifiableInstanceReportsHonestly(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "Hookaidofile")

	const token = "admintoken"
	addr := startStubAdmin(t, token, func() string { return "" })

	original := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  deliver "https://example.com" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}
	candidate := strings.Replace(original, "https://example.com", "https://example.org", 1)

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "config_apply", map[string]any{
		"path":           cfgPath,
		"content":        candidate,
		"mode":           "write_and_reload",
		"reload_timeout": "1s",
		"reason":         "apply test",
	})
	if resp.IsError {
		t.Fatalf("unexpected config_apply error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if applied, _ := out["applied"].(bool); !applied {
		t.Fatal("expected applied=true: the file was written")
	}
	if reloaded, _ := out["reloaded"].(bool); reloaded {
		t.Fatal("expected reloaded=false: nothing confirmed the instance adopted it")
	}
	if rolledBack, _ := out["rolled_back"].(bool); rolledBack {
		t.Fatal("expected no rollback")
	}
	if note, _ := out["reload_verification"].(string); note == "" {
		t.Fatal("expected reload_verification to explain why the reload is unverified")
	}

	got, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if string(got) != candidate {
		t.Fatal("the written config should stay in place")
	}
}
