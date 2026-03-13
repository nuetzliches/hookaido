package mcp

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
	"github.com/nuetzliches/hookaido/modules/sqlite"
	_ "github.com/nuetzliches/hookaido/modules/postgres"
)

func TestQueueBackendUsesAdminProxy(t *testing.T) {
	tests := []struct {
		backend string
		want    bool
	}{
		{backend: "", want: false},
		{backend: "sqlite", want: false},
		{backend: "memory", want: true},
		{backend: "postgres", want: true},
		{backend: "mixed", want: false},
	}
	for _, tt := range tests {
		if got := queueBackendUsesAdminProxy(tt.backend); got != tt.want {
			t.Fatalf("queueBackendUsesAdminProxy(%q)=%v, want %v", tt.backend, got, tt.want)
		}
	}
}

func TestToolAdminHealthIncludesAdminDiagnostics(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:     "evt_1",
		Route:  "/r",
		Target: "pull",
		State:  queue.StateQueued,
	}); err != nil {
		t.Fatalf("enqueue evt_1: %v", err)
	}
	_ = store.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
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
			if r.URL.Query().Get("details") == "1" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ok": true,
					"diagnostics": map[string]any{
						"tracing": map[string]any{
							"enabled":             true,
							"init_failures_total": 1,
							"export_errors_total": 2,
						},
						"publish": map[string]any{
							"accepted_total":                          3,
							"rejected_total":                          5,
							"rejected_managed_target_mismatch_total":  2,
							"rejected_managed_resolver_missing_total": 1,
						},
						"ingress": map[string]any{
							"accepted_total": 7,
							"rejected_total": 11,
							"rejected_by_reason": map[string]any{
								"adaptive_backpressure": 4,
								"memory_pressure":       2,
							},
							"adaptive_backpressure_applied_total": 4,
							"adaptive_backpressure_by_reason": map[string]any{
								"queued_pressure": 3,
								"ready_lag":       1,
							},
						},
						"store": map[string]any{
							"backend":              "memory",
							"retained_bytes_total": 1234,
							"memory_pressure": map[string]any{
								"active":         true,
								"reason":         "retained_items",
								"rejected_total": 2,
							},
						},
					},
				})
				return
			}
			w.WriteHeader(http.StatusOK)
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
			t.Fatalf("timed out waiting for admin diagnostics server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  deliver "https://example.org" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, dbPath)
	resp := callTool(t, s, "admin_health", map[string]any{})
	if resp.IsError {
		t.Fatalf("unexpected admin_health error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); !okVal {
		t.Fatalf("expected ok=true, got %#v", out["ok"])
	}
	queueDiag, ok := out["queue"].(map[string]any)
	if !ok {
		t.Fatalf("expected queue object, got %T", out["queue"])
	}
	if checked, _ := queueDiag["checked"].(bool); !checked {
		t.Fatalf("expected queue.checked=true")
	}
	if okVal, _ := queueDiag["ok"].(bool); !okVal {
		t.Fatalf("expected queue.ok=true")
	}
	if total := intFromAny(queueDiag["total"]); total != 1 {
		t.Fatalf("expected queue.total=1, got %#v", queueDiag["total"])
	}
	if _, ok := queueDiag["oldest_queued_age_seconds"]; !ok {
		t.Fatalf("expected oldest_queued_age_seconds in queue diagnostics")
	}
	if _, ok := queueDiag["ready_lag_seconds"]; !ok {
		t.Fatalf("expected ready_lag_seconds in queue diagnostics")
	}
	if top, ok := queueDiag["top_queued"].([]map[string]any); !ok || len(top) == 0 {
		t.Fatalf("expected top_queued buckets in queue diagnostics, got %#v", queueDiag["top_queued"])
	}
	trendSignals, ok := queueDiag["trend_signals"].(map[string]any)
	if !ok {
		t.Fatalf("expected queue.trend_signals object, got %T", queueDiag["trend_signals"])
	}
	if _, ok := trendSignals["sampling_stale"]; !ok {
		t.Fatalf("expected queue.trend_signals.sampling_stale")
	}
	if _, ok := trendSignals["operator_actions"]; !ok {
		t.Fatalf("expected queue.trend_signals.operator_actions")
	}

	adminAPI, ok := out["admin_api"].(map[string]any)
	if !ok {
		t.Fatalf("expected admin_api object, got %T", out["admin_api"])
	}
	if checked, _ := adminAPI["checked"].(bool); !checked {
		t.Fatalf("expected admin_api.checked=true")
	}
	if okVal, _ := adminAPI["ok"].(bool); !okVal {
		t.Fatalf("expected admin_api.ok=true")
	}

	details, ok := adminAPI["details"].(map[string]any)
	if !ok {
		t.Fatalf("expected admin_api.details object, got %T", adminAPI["details"])
	}
	diagnostics, ok := details["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected diagnostics object, got %T", details["diagnostics"])
	}
	if _, ok := diagnostics["tracing"]; !ok {
		t.Fatalf("expected tracing diagnostics in admin health details")
	}
	publishDiag, ok := diagnostics["publish"].(map[string]any)
	if !ok {
		t.Fatalf("expected publish diagnostics in admin health details, got %T", diagnostics["publish"])
	}
	if got := intFromAny(publishDiag["rejected_managed_target_mismatch_total"]); got != 2 {
		t.Fatalf("expected rejected_managed_target_mismatch_total=2, got %#v", publishDiag["rejected_managed_target_mismatch_total"])
	}
	if got := intFromAny(publishDiag["rejected_managed_resolver_missing_total"]); got != 1 {
		t.Fatalf("expected rejected_managed_resolver_missing_total=1, got %#v", publishDiag["rejected_managed_resolver_missing_total"])
	}
	ingressDiag, ok := diagnostics["ingress"].(map[string]any)
	if !ok {
		t.Fatalf("expected ingress diagnostics in admin health details, got %T", diagnostics["ingress"])
	}
	if got := intFromAny(ingressDiag["adaptive_backpressure_applied_total"]); got != 4 {
		t.Fatalf("expected adaptive_backpressure_applied_total=4, got %#v", ingressDiag["adaptive_backpressure_applied_total"])
	}
	rejectedByReason, ok := ingressDiag["rejected_by_reason"].(map[string]any)
	if !ok {
		t.Fatalf("expected rejected_by_reason diagnostics map, got %T", ingressDiag["rejected_by_reason"])
	}
	if got := intFromAny(rejectedByReason["memory_pressure"]); got != 2 {
		t.Fatalf("expected rejected_by_reason.memory_pressure=2, got %#v", rejectedByReason["memory_pressure"])
	}
	reasonDiag, ok := ingressDiag["adaptive_backpressure_by_reason"].(map[string]any)
	if !ok {
		t.Fatalf("expected adaptive_backpressure_by_reason diagnostics map, got %T", ingressDiag["adaptive_backpressure_by_reason"])
	}
	if got := intFromAny(reasonDiag["queued_pressure"]); got != 3 {
		t.Fatalf("expected adaptive_backpressure_by_reason.queued_pressure=3, got %#v", reasonDiag["queued_pressure"])
	}
	storeDiag, ok := diagnostics["store"].(map[string]any)
	if !ok {
		t.Fatalf("expected store diagnostics in admin health details, got %T", diagnostics["store"])
	}
	pressure, ok := storeDiag["memory_pressure"].(map[string]any)
	if !ok {
		t.Fatalf("expected store.memory_pressure diagnostics, got %T", storeDiag["memory_pressure"])
	}
	if active, _ := pressure["active"].(bool); !active {
		t.Fatalf("expected memory pressure active=true, got %#v", pressure["active"])
	}
}

func TestToolAdminHealthMemoryBackendUsesAdminQueueDiagnostics(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
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
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": true,
				"diagnostics": map[string]any{
					"queue": map[string]any{
						"ok":    true,
						"total": 3,
						"by_state": map[string]any{
							"queued": 3,
						},
					},
				},
			})
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
			t.Fatalf("timed out waiting for admin health server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "admin_health", map[string]any{})
	if resp.IsError {
		t.Fatalf("unexpected admin_health error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); !okVal {
		t.Fatalf("expected ok=true, got %#v", out["ok"])
	}
	if backend, _ := out["queue_backend"].(string); backend != "memory" {
		t.Fatalf("expected queue_backend=memory, got %#v", out["queue_backend"])
	}
	queueDiag, ok := out["queue"].(map[string]any)
	if !ok {
		t.Fatalf("expected queue object, got %T", out["queue"])
	}
	if source, _ := queueDiag["source"].(string); source != "admin_api" {
		t.Fatalf("expected queue.source=admin_api, got %#v", queueDiag["source"])
	}
	if checked, _ := queueDiag["checked"].(bool); !checked {
		t.Fatalf("expected queue.checked=true")
	}
	if okVal, _ := queueDiag["ok"].(bool); !okVal {
		t.Fatalf("expected queue.ok=true")
	}
	if total := intFromAny(queueDiag["total"]); total != 3 {
		t.Fatalf("expected queue.total=3, got %#v", queueDiag["total"])
	}
}

func TestToolAdminHealthPostgresBackendUsesAdminQueueDiagnostics(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
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
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": true,
				"diagnostics": map[string]any{
					"queue": map[string]any{
						"ok":    true,
						"total": 5,
						"by_state": map[string]any{
							"queued": 4,
							"leased": 1,
						},
					},
				},
			})
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
			t.Fatalf("timed out waiting for admin health server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "postgres" }
  deliver "https://example.org" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "admin_health", map[string]any{})
	if resp.IsError {
		t.Fatalf("unexpected admin_health error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); !okVal {
		t.Fatalf("expected ok=true, got %#v", out["ok"])
	}
	if backend, _ := out["queue_backend"].(string); backend != "postgres" {
		t.Fatalf("expected queue_backend=postgres, got %#v", out["queue_backend"])
	}
	queueDiag, ok := out["queue"].(map[string]any)
	if !ok {
		t.Fatalf("expected queue object, got %T", out["queue"])
	}
	if source, _ := queueDiag["source"].(string); source != "admin_api" {
		t.Fatalf("expected queue.source=admin_api, got %#v", queueDiag["source"])
	}
	if checked, _ := queueDiag["checked"].(bool); !checked {
		t.Fatalf("expected queue.checked=true")
	}
	if okVal, _ := queueDiag["ok"].(bool); !okVal {
		t.Fatalf("expected queue.ok=true")
	}
	if total := intFromAny(queueDiag["total"]); total != 5 {
		t.Fatalf("expected queue.total=5, got %#v", queueDiag["total"])
	}
}

func TestToolAdminHealthMemoryBackendBlockedByAllowlist(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
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
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": true,
				"diagnostics": map[string]any{
					"queue": map[string]any{
						"ok": true,
					},
				},
			})
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
			t.Fatalf("timed out waiting for admin health server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithAdminProxyEndpointAllowlist([]string{"127.0.0.1:1"}))
	resp := callTool(t, s, "admin_health", map[string]any{})
	if resp.IsError {
		t.Fatalf("unexpected admin_health error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	adminAPI, ok := out["admin_api"].(map[string]any)
	if !ok {
		t.Fatalf("expected admin_api object, got %T", out["admin_api"])
	}
	if okVal, _ := adminAPI["ok"].(bool); okVal {
		t.Fatalf("expected admin_api.ok=false")
	}
	if msg, _ := adminAPI["error"].(string); !strings.Contains(msg, "not allowed by admin endpoint allowlist") {
		t.Fatalf("expected allowlist error, got %#v", adminAPI["error"])
	}
}

func TestToolAdminHealthMemoryBackendProbeNonOKStatus(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
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
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "store_unavailable",
			})
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
			t.Fatalf("timed out waiting for admin health server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "admin_health", map[string]any{})
	if resp.IsError {
		t.Fatalf("unexpected admin_health error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	adminAPI, ok := out["admin_api"].(map[string]any)
	if !ok {
		t.Fatalf("expected admin_api object, got %T", out["admin_api"])
	}
	if checked, _ := adminAPI["checked"].(bool); !checked {
		t.Fatalf("expected admin_api.checked=true")
	}
	if okVal, _ := adminAPI["ok"].(bool); okVal {
		t.Fatalf("expected admin_api.ok=false")
	}
	if status := intFromAny(adminAPI["status_code"]); status != http.StatusServiceUnavailable {
		t.Fatalf("expected admin_api.status_code=%d, got %#v", http.StatusServiceUnavailable, adminAPI["status_code"])
	}
	if msg, _ := adminAPI["error"].(string); !strings.Contains(msg, "health check returned status 503") {
		t.Fatalf("expected status error text, got %#v", adminAPI["error"])
	}
	queueDiag, ok := out["queue"].(map[string]any)
	if !ok {
		t.Fatalf("expected queue object, got %T", out["queue"])
	}
	if source, _ := queueDiag["source"].(string); source != "admin_api" {
		t.Fatalf("expected queue.source=admin_api, got %#v", queueDiag["source"])
	}
	if msg, _ := queueDiag["error"].(string); !strings.Contains(msg, "health check returned status 503") {
		t.Fatalf("expected queue error to include probe status text, got %#v", queueDiag["error"])
	}
}

func TestToolAdminHealthIncludesAdminProxyPublishRollbackCounters(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	cancelCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			switch r.URL.Path {
			case "/admin/healthz":
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ok": true,
					"diagnostics": map[string]any{
						"queue": map[string]any{
							"ok": true,
						},
					},
				})
				return
			case "/admin/applications/billing/endpoints/invoice.created/messages/publish":
				if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if r.Header.Get("X-Hookaido-Audit-Actor") != "test-admin" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"published": 1,
				})
				return
			case "/admin/applications/erp/endpoints/order.created/messages/publish":
				if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if r.Header.Get("X-Hookaido-Audit-Actor") != "test-admin" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":       "duplicate_id",
					"detail":     "item.id already exists",
					"item_index": 0,
				})
				return
			case "/admin/messages/cancel":
				if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if r.Header.Get("X-Hookaido-Audit-Actor") != "test-admin" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				cancelCalls++
				if cancelCalls == 1 {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{
						"canceled": 1,
					})
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "store_unavailable",
				})
				return
			default:
				w.WriteHeader(http.StatusNotFound)
				return
			}
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
			t.Fatalf("timed out waiting for admin health server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/managed_a" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
"/managed_b" {
  queue { backend "memory" }
  application "erp"
  endpoint_name "order.created"
  deliver "https://example.org/b" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	for i := 0; i < 2; i++ {
		resp := callTool(t, s, "messages_publish", map[string]any{
			"reason": "operator_publish",
			"items": []any{
				map[string]any{
					"id":            fmt.Sprintf("evt_ok_%d", i),
					"application":   "billing",
					"endpoint_name": "invoice.created",
				},
				map[string]any{
					"id":            fmt.Sprintf("evt_conflict_%d", i),
					"application":   "erp",
					"endpoint_name": "order.created",
				},
			},
		})
		if !resp.IsError {
			t.Fatalf("expected publish error for rollback counter test")
		}
	}

	healthResp := callTool(t, s, "admin_health", map[string]any{})
	if healthResp.IsError {
		t.Fatalf("unexpected admin_health error: %s", healthResp.Content[0].Text)
	}
	out, ok := healthResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", healthResp.StructuredContent)
	}
	mcpOut, ok := out["mcp"].(map[string]any)
	if !ok {
		t.Fatalf("expected mcp object, got %T", out["mcp"])
	}
	adminProxyPublish, ok := mcpOut["admin_proxy_publish"].(map[string]any)
	if !ok {
		t.Fatalf("expected mcp.admin_proxy_publish object, got %T", mcpOut["admin_proxy_publish"])
	}
	if got := intFromAny(adminProxyPublish["rollback_attempts_total"]); got != 2 {
		t.Fatalf("expected rollback_attempts_total=2, got %#v", adminProxyPublish["rollback_attempts_total"])
	}
	if got := intFromAny(adminProxyPublish["rollback_succeeded_total"]); got != 1 {
		t.Fatalf("expected rollback_succeeded_total=1, got %#v", adminProxyPublish["rollback_succeeded_total"])
	}
	if got := intFromAny(adminProxyPublish["rollback_failed_total"]); got != 1 {
		t.Fatalf("expected rollback_failed_total=1, got %#v", adminProxyPublish["rollback_failed_total"])
	}
	if got := intFromAny(adminProxyPublish["rollback_ids_total"]); got != 2 {
		t.Fatalf("expected rollback_ids_total=2, got %#v", adminProxyPublish["rollback_ids_total"])
	}
}

func TestToolAdminHealthTracingConfigSurface(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	cfg := `observability {
  tracing {
    enabled on
    collector "https://otel.example.com:4318"
  }
}
"/r" {
  deliver "https://example.com" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, dbPath)
	resp := callTool(t, s, "admin_health", map[string]any{})
	if resp.IsError {
		t.Fatalf("unexpected admin_health error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}

	tracingOut, ok := out["tracing"].(map[string]any)
	if !ok {
		t.Fatalf("expected tracing object in health output, got %T", out["tracing"])
	}
	if enabled, _ := tracingOut["enabled"].(bool); !enabled {
		t.Fatalf("expected tracing.enabled=true, got %#v", tracingOut["enabled"])
	}
	if collector, _ := tracingOut["collector"].(string); collector != "https://otel.example.com:4318" {
		t.Fatalf("expected tracing.collector=https://otel.example.com:4318, got %q", collector)
	}
}

func TestToolAdminHealthTracingPropagatesRuntimeDiagnostics(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.URL.Path == "/admin/healthz" && r.URL.Query().Get("details") == "1" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ok": true,
					"diagnostics": map[string]any{
						"tracing": map[string]any{
							"enabled":             true,
							"init_failures_total": 3,
							"export_errors_total": 7,
						},
					},
				})
				return
			}
			w.WriteHeader(http.StatusOK)
		}),
	}
	done := make(chan struct{})
	go func() {
		_ = srv.Serve(ln)
		close(done)
	}()
	t.Cleanup(func() {
		_ = srv.Close()
		<-done
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
observability {
  tracing {
    enabled on
    collector "https://otel.example.com:4318"
  }
}
"/r" {
  deliver "https://example.com" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, dbPath)
	resp := callTool(t, s, "admin_health", map[string]any{})
	if resp.IsError {
		t.Fatalf("unexpected admin_health error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}

	tracingOut, ok := out["tracing"].(map[string]any)
	if !ok {
		t.Fatalf("expected tracing object, got %T", out["tracing"])
	}
	// Config-level surface:
	if enabled, _ := tracingOut["enabled"].(bool); !enabled {
		t.Fatalf("expected tracing.enabled=true")
	}
	if collector, _ := tracingOut["collector"].(string); collector != "https://otel.example.com:4318" {
		t.Fatalf("expected tracing.collector from config, got %q", collector)
	}
	// Runtime counters propagated from admin probe:
	if got := intFromAny(tracingOut["init_failures_total"]); got != 3 {
		t.Fatalf("expected tracing.init_failures_total=3, got %d", got)
	}
	if got := intFromAny(tracingOut["export_errors_total"]); got != 7 {
		t.Fatalf("expected tracing.export_errors_total=7, got %d", got)
	}
}

func TestAdminProxyErrorDetailFallbackKnownCodes(t *testing.T) {
	cases := adminProxyFallbackDetailCases()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.code, func(t *testing.T) {
			if got := adminProxyErrorDetailFallback(tc.code); got != tc.detail {
				t.Fatalf("expected fallback %q for code %q, got %q", tc.detail, tc.code, got)
			}
			if got := adminProxyErrorDetailFallback("  " + tc.code + "  "); got != tc.detail {
				t.Fatalf("expected trimmed fallback %q for code %q, got %q", tc.detail, tc.code, got)
			}
		})
	}
	if got := adminProxyErrorDetailFallback("unknown_code"); got != "" {
		t.Fatalf("expected empty fallback for unknown code, got %q", got)
	}
}

func TestAdminProxyStatusErrorUsesFallbackDetailForCodeOnlyPayloads(t *testing.T) {
	cases := adminProxyFallbackDetailCases()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.code, func(t *testing.T) {
			payload, err := json.Marshal(map[string]any{
				"code": tc.code,
			})
			if err != nil {
				t.Fatalf("marshal payload: %v", err)
			}
			err = adminProxyStatusError(http.MethodPost, "/admin/test", http.StatusBadRequest, payload)
			if err == nil {
				t.Fatalf("expected status error")
			}
			msg := err.Error()
			if !strings.Contains(msg, "code="+tc.code) {
				t.Fatalf("expected code in error text, got %q", msg)
			}
			if !strings.Contains(msg, "detail="+tc.detail) {
				t.Fatalf("expected fallback detail in error text, got %q", msg)
			}
		})
	}
}

func TestAdminProxyStatusErrorPrefersExplicitDetailOverFallback(t *testing.T) {
	payload, err := json.Marshal(map[string]any{
		"code":   "invalid_query",
		"detail": "limit must be positive",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	err = adminProxyStatusError(http.MethodGet, "/admin/messages", http.StatusBadRequest, payload)
	if err == nil {
		t.Fatalf("expected status error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "code=invalid_query") {
		t.Fatalf("expected code in error text, got %q", msg)
	}
	if !strings.Contains(msg, "detail=limit must be positive") {
		t.Fatalf("expected explicit detail in error text, got %q", msg)
	}
	if strings.Contains(msg, "detail=request query is invalid") {
		t.Fatalf("expected explicit detail to override fallback detail, got %q", msg)
	}
}

func adminProxyFallbackDetailCases() []struct {
	code   string
	detail string
} {
	return []struct {
		code   string
		detail string
	}{
		{code: "invalid_body", detail: "request body is invalid"},
		{code: "invalid_query", detail: "request query is invalid"},
		{code: "audit_reason_required", detail: "X-Hookaido-Audit-Reason is required"},
		{code: "audit_actor_required", detail: "X-Hookaido-Audit-Actor is required by defaults.publish_policy.require_actor"},
		{code: "audit_actor_not_allowed", detail: "X-Hookaido-Audit-Actor is not allowed for endpoint-scoped managed mutation"},
		{code: "audit_request_id_required", detail: "X-Request-ID is required by defaults.publish_policy.require_request_id"},
		{code: "store_unavailable", detail: "queue store is unavailable"},
		{code: "queue_full", detail: "queue is full"},
		{code: "duplicate_id", detail: "item.id already exists"},
		{code: "route_resolver_missing", detail: "route target resolution is not configured for global publish path"},
		{code: "route_not_found", detail: "route has no publishable targets or is not configured"},
		{code: "target_unresolvable", detail: "item.target is required or not allowed for route"},
		{code: "invalid_received_at", detail: "item.received_at must be RFC3339"},
		{code: "invalid_next_run_at", detail: "item.next_run_at must be RFC3339"},
		{code: "invalid_payload_b64", detail: "item.payload_b64 must be valid base64"},
		{code: "invalid_header", detail: "item.headers contains invalid HTTP header name/value"},
		{code: "payload_too_large", detail: "decoded payload exceeds max_body for route"},
		{code: "headers_too_large", detail: "headers exceed max_headers for route"},
		{code: "managed_selector_required", detail: "route is managed by application/endpoint; use application+endpoint_name"},
		{code: "scoped_publish_required", detail: "managed publish must use endpoint-scoped publish path"},
		{code: "managed_resolver_missing", detail: "managed endpoint resolution is not configured"},
		{code: "managed_endpoint_not_found", detail: "managed endpoint not found"},
		{code: "managed_target_mismatch", detail: "managed endpoint target/ownership mapping is out of sync with route policy resolution"},
		{code: "managed_endpoint_no_targets", detail: "managed endpoint has no publishable targets"},
		{code: "selector_scope_mismatch", detail: "route/managed selector is invalid or mismatched"},
		{code: "selector_scope_forbidden", detail: "selector hints are not allowed on scoped path"},
		{code: "global_publish_disabled", detail: "global direct publish path is disabled by defaults.publish_policy.direct"},
		{code: "scoped_publish_disabled", detail: "endpoint-scoped publish path is disabled by defaults.publish_policy.managed"},
		{code: "route_publish_disabled", detail: "publish is disabled for route by route-level publish policy"},
		{code: "pull_route_publish_disabled", detail: "publish to pull routes is disabled by defaults.publish_policy.allow_pull_routes"},
		{code: "deliver_route_publish_disabled", detail: "publish to deliver routes is disabled by defaults.publish_policy.allow_deliver_routes"},
		{code: "management_unavailable", detail: "management endpoint mutation failed"},
		{code: "management_route_not_found", detail: "management route not found"},
		{code: "management_endpoint_not_found", detail: "management endpoint not found"},
		{code: "management_route_already_mapped", detail: "management endpoint target route is already mapped to another endpoint"},
		{code: "management_route_publish_disabled", detail: "management endpoint target route has managed publish disabled"},
		{code: "management_route_target_mismatch", detail: "management endpoint target route has mismatched publish target profile"},
		{code: "management_route_backlog_active", detail: "management endpoint current route has active queued/leased backlog"},
		{code: "management_conflict", detail: "management endpoint mutation conflict"},
		{code: "not_found", detail: "resource not found"},
		{code: "backlog_unavailable", detail: "backlog trend store is not supported by queue backend"},
		{code: "management_model_unavailable", detail: "management model is not configured"},
	}
}

func TestLoadAdminHealthToken_VaultRef(t *testing.T) {
	var seenToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenToken = r.Header.Get("X-Vault-Token")
		if r.URL.Path != "/v1/secret/data/hookaido/admin" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"data":{"token":"admin-vault-token"}}}`))
	}))
	defer srv.Close()

	t.Setenv("HOOKAIDO_VAULT_ADDR", srv.URL)
	t.Setenv("HOOKAIDO_VAULT_TOKEN", "mcp-vault-token")

	token, err := loadAdminHealthToken([]string{"vault:secret/data/hookaido/admin#token"})
	if err != nil {
		t.Fatalf("loadAdminHealthToken(vault): %v", err)
	}
	if token != "admin-vault-token" {
		t.Fatalf("unexpected token %q", token)
	}
	if seenToken != "mcp-vault-token" {
		t.Fatalf("unexpected X-Vault-Token header %q", seenToken)
	}
}

func TestToolManagementModel(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
pull_api { auth token "raw:t" }
"/r_pull" {
  application "billing"
  endpoint_name "invoice.created"
  publish { direct off }
  pull { path "/e" }
}
"/r_deliver" {
  application "billing"
  endpoint_name "invoice.retry"
  publish { managed off }
  deliver "https://example.org/retry" {}
}
"/r_plain" {
  deliver "https://example.org/plain" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "management_model", map[string]any{})
	if resp.IsError {
		t.Fatalf("unexpected management_model error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if appCount := intFromAny(out["application_count"]); appCount != 1 {
		t.Fatalf("expected application_count=1, got %#v", out["application_count"])
	}
	if endpointCount := intFromAny(out["path_count"]); endpointCount != 2 {
		t.Fatalf("expected path_count=2, got %#v", out["path_count"])
	}
	apps, ok := out["applications"].([]map[string]any)
	if !ok || len(apps) != 1 {
		t.Fatalf("expected one application entry, got %#v", out["applications"])
	}
	appRec := apps[0]
	endpoints, ok := appRec["endpoints"].([]map[string]any)
	if !ok || len(endpoints) != 2 {
		t.Fatalf("expected two endpoints, got %#v", appRec["endpoints"])
	}
	ep0 := endpoints[0]
	ep1 := endpoints[1]
	pol0, ok := ep0["publish_policy"].(map[string]any)
	if !ok {
		t.Fatalf("expected publish_policy object for ep0, got %#v", ep0["publish_policy"])
	}
	if v, _ := pol0["direct_enabled"].(bool); v {
		t.Fatalf("expected ep0 direct_enabled=false, got %#v", pol0)
	}
	pol1, ok := ep1["publish_policy"].(map[string]any)
	if !ok {
		t.Fatalf("expected publish_policy object for ep1, got %#v", ep1["publish_policy"])
	}
	if v, _ := pol1["managed_enabled"].(bool); v {
		t.Fatalf("expected ep1 managed_enabled=false, got %#v", pol1)
	}
}

