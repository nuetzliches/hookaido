package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

func TestHandleRequest_ToolsList(t *testing.T) {
	s := NewServer(nil, nil, "", "")
	resp := s.handleRequest(rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/list",
	})
	if resp == nil {
		t.Fatalf("expected response")
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %#v", resp.Error)
	}

	result, ok := resp.Result.(toolsListResult)
	if !ok {
		t.Fatalf("unexpected result type %T", resp.Result)
	}
	if len(result.Tools) == 0 {
		t.Fatalf("expected tools")
	}

	want := map[string]bool{
		"config_parse":          false,
		"config_validate":       false,
		"config_compile":        false,
		"config_fmt_preview":    false,
		"config_diff":           false,
		"admin_health":          false,
		"management_model":      false,
		"backlog_top_queued":    false,
		"backlog_oldest_queued": false,
		"backlog_aging_summary": false,
		"backlog_trends":        false,
		"messages_list":         false,
		"attempts_list":         false,
		"dlq_list":              false,
	}
	for _, tool := range result.Tools {
		if _, ok := want[tool.Name]; ok {
			want[tool.Name] = true
		}
		if tool.Name == "dlq_requeue" || tool.Name == "dlq_delete" || tool.Name == "messages_cancel" || tool.Name == "messages_requeue" || tool.Name == "messages_resume" || tool.Name == "messages_publish" || tool.Name == "messages_cancel_by_filter" || tool.Name == "messages_requeue_by_filter" || tool.Name == "messages_resume_by_filter" || tool.Name == "management_endpoint_upsert" || tool.Name == "management_endpoint_delete" || tool.Name == "config_apply" || tool.Name == "instance_status" || tool.Name == "instance_logs_tail" || tool.Name == "instance_start" || tool.Name == "instance_stop" || tool.Name == "instance_reload" {
			t.Fatalf("unexpected mutation tool %q in read-only mode", tool.Name)
		}
	}
	for name, seen := range want {
		if !seen {
			t.Fatalf("missing tool %q", name)
		}
	}
}

func TestHandleRequest_ToolsListWithMutations(t *testing.T) {
	s := NewServer(nil, nil, "", "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := s.handleRequest(rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/list",
	})
	if resp == nil || resp.Error != nil {
		t.Fatalf("unexpected response: %#v", resp)
	}
	result, ok := resp.Result.(toolsListResult)
	if !ok {
		t.Fatalf("unexpected result type %T", resp.Result)
	}
	seen := make(map[string]bool, len(result.Tools))
	byName := make(map[string]toolDescriptor, len(result.Tools))
	for _, tool := range result.Tools {
		seen[tool.Name] = true
		byName[tool.Name] = tool
	}
	for _, name := range []string{"config_apply", "management_endpoint_upsert", "management_endpoint_delete", "dlq_requeue", "dlq_delete", "messages_cancel", "messages_requeue", "messages_resume", "messages_publish", "messages_cancel_by_filter", "messages_requeue_by_filter", "messages_resume_by_filter"} {
		if !seen[name] {
			t.Fatalf("expected tool %q", name)
		}
	}
	for _, name := range []string{"management_endpoint_upsert", "management_endpoint_delete"} {
		schema := byName[name].InputSchema
		props, ok := schema["properties"].(map[string]any)
		if !ok {
			t.Fatalf("expected properties object in %s schema, got %T", name, schema["properties"])
		}
		if _, ok := props["path"]; !ok {
			t.Fatalf("expected optional path property in %s schema", name)
		}
		if additional, ok := schema["additionalProperties"].(bool); !ok || additional {
			t.Fatalf("expected additionalProperties=false in %s schema, got %#v", name, schema["additionalProperties"])
		}
		if name == "management_endpoint_upsert" {
			routeProp, ok := props["route"].(map[string]any)
			if !ok {
				t.Fatalf("expected route property object in %s schema, got %T", name, props["route"])
			}
			if pattern, _ := routeProp["pattern"].(string); pattern != "^/.*$" {
				t.Fatalf("expected route pattern ^/.*$ in %s schema, got %#v", name, routeProp["pattern"])
			}
		}
	}
	for _, name := range []string{
		"backlog_top_queued",
		"backlog_oldest_queued",
		"backlog_aging_summary",
		"backlog_trends",
		"messages_list",
		"attempts_list",
		"dlq_list",
		"messages_cancel_by_filter",
		"messages_requeue_by_filter",
		"messages_resume_by_filter",
	} {
		schema := byName[name].InputSchema
		props, ok := schema["properties"].(map[string]any)
		if !ok {
			t.Fatalf("expected properties object in %s schema, got %T", name, schema["properties"])
		}
		routeProp, ok := props["route"].(map[string]any)
		if !ok {
			t.Fatalf("expected route property object in %s schema, got %T", name, props["route"])
		}
		if pattern, _ := routeProp["pattern"].(string); pattern != "^/.*$" {
			t.Fatalf("expected route pattern ^/.*$ in %s schema, got %#v", name, routeProp["pattern"])
		}
	}
	publishSchema := byName["messages_publish"].InputSchema
	publishProps, ok := publishSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected properties object in messages_publish schema, got %T", publishSchema["properties"])
	}
	itemsProp, ok := publishProps["items"].(map[string]any)
	if !ok {
		t.Fatalf("expected items property object in messages_publish schema, got %T", publishProps["items"])
	}
	itemSchema, ok := itemsProp["items"].(map[string]any)
	if !ok {
		t.Fatalf("expected items.items object in messages_publish schema, got %T", itemsProp["items"])
	}
	itemProps, ok := itemSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected items.items.properties object in messages_publish schema, got %T", itemSchema["properties"])
	}
	itemRouteProp, ok := itemProps["route"].(map[string]any)
	if !ok {
		t.Fatalf("expected items.items.properties.route object in messages_publish schema, got %T", itemProps["route"])
	}
	if pattern, _ := itemRouteProp["pattern"].(string); pattern != "^/.*$" {
		t.Fatalf("expected items route pattern ^/.*$ in messages_publish schema, got %#v", itemRouteProp["pattern"])
	}
	for _, name := range []string{"instance_status", "instance_logs_tail", "instance_start", "instance_stop", "instance_reload"} {
		if seen[name] {
			t.Fatalf("unexpected runtime tool %q without runtime control flag", name)
		}
	}
}

func TestHandleRequest_ToolsListWithRuntimeControl(t *testing.T) {
	s := NewServer(nil, nil, "", "", WithRuntimeControlEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := s.handleRequest(rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/list",
	})
	if resp == nil || resp.Error != nil {
		t.Fatalf("unexpected response: %#v", resp)
	}
	result, ok := resp.Result.(toolsListResult)
	if !ok {
		t.Fatalf("unexpected result type %T", resp.Result)
	}
	seen := make(map[string]bool, len(result.Tools))
	for _, tool := range result.Tools {
		seen[tool.Name] = true
	}
	for _, name := range []string{"instance_status", "instance_logs_tail", "instance_start", "instance_stop", "instance_reload"} {
		if !seen[name] {
			t.Fatalf("expected runtime control tool %q", name)
		}
	}
}

func TestHandleRequest_ToolsListWithOperateRole(t *testing.T) {
	s := NewServer(
		nil,
		nil,
		"",
		"",
		WithMutationsEnabled(true),
		WithRuntimeControlEnabled(true),
		WithRole(RoleOperate),
		WithPrincipal("test-operator"),
	)
	resp := s.handleRequest(rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/list",
	})
	if resp == nil || resp.Error != nil {
		t.Fatalf("unexpected response: %#v", resp)
	}
	result, ok := resp.Result.(toolsListResult)
	if !ok {
		t.Fatalf("unexpected result type %T", resp.Result)
	}
	seen := make(map[string]bool, len(result.Tools))
	for _, tool := range result.Tools {
		seen[tool.Name] = true
	}
	for _, name := range []string{"messages_publish", "messages_cancel", "messages_requeue", "messages_resume", "dlq_requeue", "dlq_delete", "instance_status", "instance_logs_tail"} {
		if !seen[name] {
			t.Fatalf("expected operate tool %q", name)
		}
	}
	for _, name := range []string{"config_apply", "management_endpoint_upsert", "management_endpoint_delete", "instance_start", "instance_stop", "instance_reload"} {
		if seen[name] {
			t.Fatalf("unexpected admin-only tool %q for operate role", name)
		}
	}
}

func TestHandleRequest_ToolsListReadRoleWithFlags(t *testing.T) {
	s := NewServer(
		nil,
		nil,
		"",
		"",
		WithMutationsEnabled(true),
		WithRuntimeControlEnabled(true),
		WithRole(RoleRead),
	)
	resp := s.handleRequest(rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/list",
	})
	if resp == nil || resp.Error != nil {
		t.Fatalf("unexpected response: %#v", resp)
	}
	result, ok := resp.Result.(toolsListResult)
	if !ok {
		t.Fatalf("unexpected result type %T", resp.Result)
	}
	for _, tool := range result.Tools {
		if tool.Name == "config_apply" || tool.Name == "management_endpoint_upsert" || tool.Name == "management_endpoint_delete" ||
			tool.Name == "dlq_requeue" || tool.Name == "dlq_delete" || tool.Name == "messages_publish" || tool.Name == "messages_cancel" ||
			tool.Name == "messages_requeue" || tool.Name == "messages_resume" || tool.Name == "messages_cancel_by_filter" || tool.Name == "messages_requeue_by_filter" ||
			tool.Name == "messages_resume_by_filter" || tool.Name == "instance_status" || tool.Name == "instance_logs_tail" ||
			tool.Name == "instance_start" || tool.Name == "instance_stop" || tool.Name == "instance_reload" {
			t.Fatalf("unexpected privileged tool %q for read role", tool.Name)
		}
	}
}

func TestToolRoleDenied(t *testing.T) {
	readRole := NewServer(nil, nil, "", "", WithMutationsEnabled(true), WithRole(RoleRead))
	resp := callTool(t, readRole, "messages_cancel", map[string]any{
		"ids":    []any{"evt_1"},
		"reason": "test",
	})
	if !resp.IsError {
		t.Fatalf("expected role denied error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `requires role "operate"`) {
		t.Fatalf("expected operate role requirement, got %#v", resp.Content)
	}

	operateRole := NewServer(nil, nil, "", "", WithMutationsEnabled(true), WithRole(RoleOperate), WithPrincipal("test-operator"))
	resp = callTool(t, operateRole, "config_apply", map[string]any{
		"content": `"/r" { deliver "https://example.com" {} }`,
	})
	if !resp.IsError {
		t.Fatalf("expected role denied error for config_apply")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `requires role "admin"`) {
		t.Fatalf("expected admin role requirement, got %#v", resp.Content)
	}
}

func TestToolMutationRequiresPrincipal(t *testing.T) {
	s := NewServer(nil, nil, "", "", WithMutationsEnabled(true), WithRole(RoleAdmin))
	resp := callTool(t, s, "config_apply", map[string]any{
		"content": `"/r" { deliver "https://example.com" {} }`,
	})
	if !resp.IsError {
		t.Fatalf("expected principal requirement error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "requires configured MCP principal") {
		t.Fatalf("expected principal requirement message, got %#v", resp.Content)
	}
}

func TestToolMutationAuditEventSuccess(t *testing.T) {
	var audit bytes.Buffer
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`"/r" { deliver "https://example.com" {} }`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	s := NewServer(
		nil,
		nil,
		cfgPath,
		"",
		WithMutationsEnabled(true),
		WithRole(RoleAdmin),
		WithPrincipal("ops@example.test"),
		WithAuditWriter(&audit),
	)
	args := map[string]any{
		"content": `"/r" { deliver "https://example.com" {} }`,
		"mode":    "preview_only",
	}
	resp := callTool(t, s, "config_apply", args)
	if resp.IsError {
		t.Fatalf("unexpected error: %s", resp.Content[0].Text)
	}
	line := strings.TrimSpace(audit.String())
	if line == "" {
		t.Fatalf("expected audit event line")
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("unmarshal audit event: %v", err)
	}
	if got, _ := event["principal"].(string); got != "ops@example.test" {
		t.Fatalf("expected principal in audit event, got %#v", event["principal"])
	}
	if got, _ := event["tool"].(string); got != "config_apply" {
		t.Fatalf("expected tool=config_apply, got %#v", event["tool"])
	}
	if got, _ := event["result"].(string); got != "success" {
		t.Fatalf("expected result=success, got %#v", event["result"])
	}
	if got, _ := event["input_hash"].(string); got != toolInputHash(args) {
		t.Fatalf("expected stable input hash, got %#v", event["input_hash"])
	}
	if _, ok := event["duration_ms"]; !ok {
		t.Fatalf("expected duration_ms in audit event")
	}
	metadata, ok := event["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata object, got %T", event["metadata"])
	}
	configMeta, ok := metadata["config_mutation"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata.config_mutation object, got %T", metadata["config_mutation"])
	}
	if got, _ := configMeta["operation"].(string); got != "config_apply" {
		t.Fatalf("expected operation=config_apply, got %#v", configMeta["operation"])
	}
	if got, _ := configMeta["mode"].(string); got != "preview_only" {
		t.Fatalf("expected mode=preview_only, got %#v", configMeta["mode"])
	}
	if okVal, _ := configMeta["ok"].(bool); !okVal {
		t.Fatalf("expected ok=true, got %#v", configMeta["ok"])
	}
	if applied, _ := configMeta["applied"].(bool); applied {
		t.Fatalf("expected applied=false, got %#v", configMeta["applied"])
	}
}

func TestToolMutationAuditEventDenied(t *testing.T) {
	var audit bytes.Buffer
	s := NewServer(
		nil,
		nil,
		"",
		"",
		WithMutationsEnabled(true),
		WithRole(RoleRead),
		WithPrincipal("viewer@example.test"),
		WithAuditWriter(&audit),
	)
	resp := callTool(t, s, "messages_cancel", map[string]any{
		"ids":    []any{"evt_1"},
		"reason": "test",
	})
	if !resp.IsError {
		t.Fatalf("expected denied error")
	}
	line := strings.TrimSpace(audit.String())
	if line == "" {
		t.Fatalf("expected denied audit event line")
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("unmarshal denied audit event: %v", err)
	}
	if got, _ := event["tool"].(string); got != "messages_cancel" {
		t.Fatalf("expected tool=messages_cancel, got %#v", event["tool"])
	}
	if got, _ := event["result"].(string); got != "denied" {
		t.Fatalf("expected result=denied, got %#v", event["result"])
	}
	if _, ok := event["error"].(string); !ok {
		t.Fatalf("expected error text in denied audit event")
	}
	metadata, ok := event["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata object, got %T", event["metadata"])
	}
	idMeta, ok := metadata["id_mutation"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata.id_mutation object, got %T", metadata["id_mutation"])
	}
	if got, _ := idMeta["operation"].(string); got != "messages_cancel" {
		t.Fatalf("expected operation=messages_cancel, got %#v", idMeta["operation"])
	}
	if got, _ := idMeta["changed_field"].(string); got != "canceled" {
		t.Fatalf("expected changed_field=canceled, got %#v", idMeta["changed_field"])
	}
	if got := intFromAny(idMeta["ids_requested"]); got != 1 {
		t.Fatalf("expected ids_requested=1, got %#v", idMeta["ids_requested"])
	}
	if got := intFromAny(idMeta["ids_unique"]); got != 1 {
		t.Fatalf("expected ids_unique=1, got %#v", idMeta["ids_unique"])
	}
}

func TestToolMutationAuditEventMessagesPublishIncludesAdminProxyRollbackMetadata(t *testing.T) {
	var audit bytes.Buffer
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
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Actor") != "test-admin" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch r.URL.Path {
			case "/admin/applications/billing/endpoints/invoice.created/messages/publish":
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"published": 1,
				})
				return
			case "/admin/applications/erp/endpoints/order.created/messages/publish":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":       "duplicate_id",
					"detail":     "item.id already exists",
					"item_index": 0,
				})
				return
			case "/admin/messages/cancel":
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"canceled": 1,
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
			t.Fatalf("timed out waiting for admin server shutdown")
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

	s := NewServer(
		nil,
		nil,
		cfgPath,
		"",
		WithMutationsEnabled(true),
		WithRole(RoleAdmin),
		WithPrincipal("test-admin"),
		WithAuditWriter(&audit),
	)
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_ok",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
			map[string]any{
				"id":            "evt_conflict",
				"application":   "erp",
				"endpoint_name": "order.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error")
	}

	line := strings.TrimSpace(audit.String())
	if line == "" {
		t.Fatalf("expected audit event line")
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("unmarshal audit event: %v", err)
	}
	if got, _ := event["tool"].(string); got != "messages_publish" {
		t.Fatalf("expected tool=messages_publish, got %#v", event["tool"])
	}
	if got, _ := event["result"].(string); got != "error" {
		t.Fatalf("expected result=error, got %#v", event["result"])
	}

	metadata, ok := event["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata object, got %T", event["metadata"])
	}
	publishMeta, ok := metadata["admin_proxy_publish"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata.admin_proxy_publish object, got %T", metadata["admin_proxy_publish"])
	}
	if got := intFromAny(publishMeta["rollback_attempts"]); got != 1 {
		t.Fatalf("expected rollback_attempts=1, got %#v", publishMeta["rollback_attempts"])
	}
	if got := intFromAny(publishMeta["rollback_succeeded"]); got != 1 {
		t.Fatalf("expected rollback_succeeded=1, got %#v", publishMeta["rollback_succeeded"])
	}
	if got := intFromAny(publishMeta["rollback_failed"]); got != 0 {
		t.Fatalf("expected rollback_failed=0, got %#v", publishMeta["rollback_failed"])
	}
	if got := intFromAny(publishMeta["rollback_ids"]); got != 1 {
		t.Fatalf("expected rollback_ids=1, got %#v", publishMeta["rollback_ids"])
	}
	if got := intFromAny(publishMeta["rollback_attempts_total"]); got != 1 {
		t.Fatalf("expected rollback_attempts_total=1, got %#v", publishMeta["rollback_attempts_total"])
	}
	if got := intFromAny(publishMeta["rollback_succeeded_total"]); got != 1 {
		t.Fatalf("expected rollback_succeeded_total=1, got %#v", publishMeta["rollback_succeeded_total"])
	}
	if got := intFromAny(publishMeta["rollback_failed_total"]); got != 0 {
		t.Fatalf("expected rollback_failed_total=0, got %#v", publishMeta["rollback_failed_total"])
	}
	if got := intFromAny(publishMeta["rollback_ids_total"]); got != 1 {
		t.Fatalf("expected rollback_ids_total=1, got %#v", publishMeta["rollback_ids_total"])
	}
}

func TestToolMutationAuditEventMessagesCancelIncludesIDMetadata(t *testing.T) {
	var audit bytes.Buffer
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:     "evt_cancel_id_meta",
		Route:  "/r",
		Target: "pull",
		State:  queue.StateQueued,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_ = store.Close()

	s := NewServer(
		nil,
		nil,
		"",
		dbPath,
		WithMutationsEnabled(true),
		WithRole(RoleAdmin),
		WithPrincipal("ops@example.test"),
		WithAuditWriter(&audit),
	)
	resp := callTool(t, s, "messages_cancel", map[string]any{
		"reason": "operator_cleanup",
		"ids":    []any{"evt_cancel_id_meta", "evt_cancel_id_meta"},
	})
	if resp.IsError {
		t.Fatalf("unexpected error: %s", resp.Content[0].Text)
	}

	line := strings.TrimSpace(audit.String())
	if line == "" {
		t.Fatalf("expected audit event line")
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("unmarshal audit event: %v", err)
	}
	if got, _ := event["tool"].(string); got != "messages_cancel" {
		t.Fatalf("expected tool=messages_cancel, got %#v", event["tool"])
	}
	if got, _ := event["result"].(string); got != "success" {
		t.Fatalf("expected result=success, got %#v", event["result"])
	}

	metadata, ok := event["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata object, got %T", event["metadata"])
	}
	idMeta, ok := metadata["id_mutation"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata.id_mutation object, got %T", metadata["id_mutation"])
	}
	if got, _ := idMeta["operation"].(string); got != "messages_cancel" {
		t.Fatalf("expected operation=messages_cancel, got %#v", idMeta["operation"])
	}
	if got, _ := idMeta["changed_field"].(string); got != "canceled" {
		t.Fatalf("expected changed_field=canceled, got %#v", idMeta["changed_field"])
	}
	if got := intFromAny(idMeta["ids_requested"]); got != 2 {
		t.Fatalf("expected ids_requested=2, got %#v", idMeta["ids_requested"])
	}
	if got := intFromAny(idMeta["ids_unique"]); got != 1 {
		t.Fatalf("expected ids_unique=1, got %#v", idMeta["ids_unique"])
	}
	if got := intFromAny(idMeta["changed"]); got != 1 {
		t.Fatalf("expected changed=1, got %#v", idMeta["changed"])
	}
}

func TestToolMutationAuditEventMessagesCancelByFilterIncludesMetadata(t *testing.T) {
	var audit bytes.Buffer
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:     "evt_cancel_meta",
		Route:  "/r",
		Target: "pull",
		State:  queue.StateQueued,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_ = store.Close()

	s := NewServer(
		nil,
		nil,
		"",
		dbPath,
		WithMutationsEnabled(true),
		WithRole(RoleAdmin),
		WithPrincipal("ops@example.test"),
		WithAuditWriter(&audit),
	)
	resp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"reason": "operator_cleanup",
		"route":  "/r",
		"state":  "queued",
		"limit":  10,
	})
	if resp.IsError {
		t.Fatalf("unexpected error: %s", resp.Content[0].Text)
	}

	line := strings.TrimSpace(audit.String())
	if line == "" {
		t.Fatalf("expected audit event line")
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("unmarshal audit event: %v", err)
	}
	if got, _ := event["tool"].(string); got != "messages_cancel_by_filter" {
		t.Fatalf("expected tool=messages_cancel_by_filter, got %#v", event["tool"])
	}
	if got, _ := event["result"].(string); got != "success" {
		t.Fatalf("expected result=success, got %#v", event["result"])
	}

	metadata, ok := event["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata object, got %T", event["metadata"])
	}
	filterMeta, ok := metadata["filter_mutation"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata.filter_mutation object, got %T", metadata["filter_mutation"])
	}
	if got, _ := filterMeta["operation"].(string); got != "cancel_by_filter" {
		t.Fatalf("expected operation=cancel_by_filter, got %#v", filterMeta["operation"])
	}
	if got, _ := filterMeta["changed_field"].(string); got != "canceled" {
		t.Fatalf("expected changed_field=canceled, got %#v", filterMeta["changed_field"])
	}
	if got := intFromAny(filterMeta["matched"]); got != 1 {
		t.Fatalf("expected matched=1, got %#v", filterMeta["matched"])
	}
	if got := intFromAny(filterMeta["changed"]); got != 1 {
		t.Fatalf("expected changed=1, got %#v", filterMeta["changed"])
	}
	if preview, _ := filterMeta["preview_only"].(bool); preview {
		t.Fatalf("expected preview_only=false, got %#v", filterMeta["preview_only"])
	}
}

func TestToolMutationAuditEventMessagesRequeueByFilterPreviewIncludesMetadata(t *testing.T) {
	var audit bytes.Buffer
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(
		nil,
		nil,
		"",
		dbPath,
		WithMutationsEnabled(true),
		WithRole(RoleAdmin),
		WithPrincipal("ops@example.test"),
		WithAuditWriter(&audit),
	)
	resp := callTool(t, s, "messages_requeue_by_filter", map[string]any{
		"reason":       "operator_cleanup",
		"route":        "/r",
		"state":        "dead",
		"limit":        10,
		"preview_only": true,
	})
	if resp.IsError {
		t.Fatalf("unexpected error: %s", resp.Content[0].Text)
	}

	line := strings.TrimSpace(audit.String())
	if line == "" {
		t.Fatalf("expected audit event line")
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("unmarshal audit event: %v", err)
	}
	if got, _ := event["tool"].(string); got != "messages_requeue_by_filter" {
		t.Fatalf("expected tool=messages_requeue_by_filter, got %#v", event["tool"])
	}
	if got, _ := event["result"].(string); got != "success" {
		t.Fatalf("expected result=success, got %#v", event["result"])
	}

	metadata, ok := event["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata object, got %T", event["metadata"])
	}
	filterMeta, ok := metadata["filter_mutation"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata.filter_mutation object, got %T", metadata["filter_mutation"])
	}
	if got, _ := filterMeta["operation"].(string); got != "requeue_by_filter" {
		t.Fatalf("expected operation=requeue_by_filter, got %#v", filterMeta["operation"])
	}
	if got, _ := filterMeta["changed_field"].(string); got != "requeued" {
		t.Fatalf("expected changed_field=requeued, got %#v", filterMeta["changed_field"])
	}
	if got := intFromAny(filterMeta["matched"]); got != 0 {
		t.Fatalf("expected matched=0, got %#v", filterMeta["matched"])
	}
	if got := intFromAny(filterMeta["changed"]); got != 0 {
		t.Fatalf("expected changed=0, got %#v", filterMeta["changed"])
	}
	if preview, _ := filterMeta["preview_only"].(bool); !preview {
		t.Fatalf("expected preview_only=true, got %#v", filterMeta["preview_only"])
	}
}

func TestToolMutationAuditEventManagementEndpointUpsertIncludesConfigMetadata(t *testing.T) {
	var audit bytes.Buffer
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	cfg := `pull_api {
  auth token "raw:test-token"
}
"/r" {
  pull {
    path "/events"
  }
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(
		nil,
		nil,
		cfgPath,
		"",
		WithMutationsEnabled(true),
		WithRole(RoleAdmin),
		WithPrincipal("ops@example.test"),
		WithAuditWriter(&audit),
	)
	resp := callTool(t, s, "management_endpoint_upsert", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"route":         "/r",
		"reason":        "map_endpoint",
		"mode":          "preview_only",
	})
	if resp.IsError {
		t.Fatalf("unexpected error: %s", resp.Content[0].Text)
	}

	line := strings.TrimSpace(audit.String())
	if line == "" {
		t.Fatalf("expected audit event line")
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("unmarshal audit event: %v", err)
	}
	if got, _ := event["tool"].(string); got != "management_endpoint_upsert" {
		t.Fatalf("expected tool=management_endpoint_upsert, got %#v", event["tool"])
	}
	if got, _ := event["result"].(string); got != "success" {
		t.Fatalf("expected result=success, got %#v", event["result"])
	}

	metadata, ok := event["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata object, got %T", event["metadata"])
	}
	configMeta, ok := metadata["config_mutation"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata.config_mutation object, got %T", metadata["config_mutation"])
	}
	if got, _ := configMeta["operation"].(string); got != "management_endpoint_upsert" {
		t.Fatalf("expected operation=management_endpoint_upsert, got %#v", configMeta["operation"])
	}
	if got, _ := configMeta["application"].(string); got != "billing" {
		t.Fatalf("expected application=billing, got %#v", configMeta["application"])
	}
	if got, _ := configMeta["endpoint_name"].(string); got != "invoice.created" {
		t.Fatalf("expected endpoint_name=invoice.created, got %#v", configMeta["endpoint_name"])
	}
	if got, _ := configMeta["route"].(string); got != "/r" {
		t.Fatalf("expected route=/r, got %#v", configMeta["route"])
	}
	if got, _ := configMeta["mode"].(string); got != "preview_only" {
		t.Fatalf("expected mode=preview_only, got %#v", configMeta["mode"])
	}
	if got, _ := configMeta["action"].(string); got != "created" {
		t.Fatalf("expected action=created, got %#v", configMeta["action"])
	}
	if mutates, _ := configMeta["mutates_config"].(bool); !mutates {
		t.Fatalf("expected mutates_config=true, got %#v", configMeta["mutates_config"])
	}
	if applied, _ := configMeta["applied"].(bool); applied {
		t.Fatalf("expected applied=false, got %#v", configMeta["applied"])
	}
}

func TestToolMutationAuditEventInstanceStopIncludesRuntimeMetadata(t *testing.T) {
	var audit bytes.Buffer
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	pidPath := filepath.Join(tmp, "hookaido.pid")
	cfg := `pull_api {
  auth token "raw:test-token"
}
"/r" {
  pull {
    path "/events"
  }
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(
		nil,
		nil,
		cfgPath,
		dbPath,
		WithRuntimeControlEnabled(true),
		WithRole(RoleAdmin),
		WithPrincipal("ops@example.test"),
		WithRuntimeControlPIDFile(pidPath),
		WithAuditWriter(&audit),
	)
	resp := callTool(t, s, "instance_stop", map[string]any{
		"pid_file": pidPath,
		"timeout":  "3s",
		"force":    true,
	})
	if resp.IsError {
		t.Fatalf("unexpected error: %s", resp.Content[0].Text)
	}

	line := strings.TrimSpace(audit.String())
	if line == "" {
		t.Fatalf("expected audit event line")
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("unmarshal audit event: %v", err)
	}
	if got, _ := event["tool"].(string); got != "instance_stop" {
		t.Fatalf("expected tool=instance_stop, got %#v", event["tool"])
	}
	if got, _ := event["result"].(string); got != "success" {
		t.Fatalf("expected result=success, got %#v", event["result"])
	}

	metadata, ok := event["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata object, got %T", event["metadata"])
	}
	runtimeMeta, ok := metadata["runtime_control"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata.runtime_control object, got %T", metadata["runtime_control"])
	}
	if got, _ := runtimeMeta["operation"].(string); got != "instance_stop" {
		t.Fatalf("expected operation=instance_stop, got %#v", runtimeMeta["operation"])
	}
	if got, _ := runtimeMeta["pid_file"].(string); got != pidPath {
		t.Fatalf("expected pid_file=%q, got %#v", pidPath, runtimeMeta["pid_file"])
	}
	if got, _ := runtimeMeta["timeout"].(string); got != "3s" {
		t.Fatalf("expected timeout=3s, got %#v", runtimeMeta["timeout"])
	}
	if force, _ := runtimeMeta["force"].(bool); !force {
		t.Fatalf("expected force=true, got %#v", runtimeMeta["force"])
	}
	if stopped, _ := runtimeMeta["stopped"].(bool); stopped {
		t.Fatalf("expected stopped=false, got %#v", runtimeMeta["stopped"])
	}
	if alreadyStopped, _ := runtimeMeta["already_stopped"].(bool); !alreadyStopped {
		t.Fatalf("expected already_stopped=true, got %#v", runtimeMeta["already_stopped"])
	}
}

func TestParseRole(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    Role
		wantErr bool
	}{
		{name: "default_empty", raw: "", want: RoleRead},
		{name: "read", raw: "read", want: RoleRead},
		{name: "operate", raw: "operate", want: RoleOperate},
		{name: "admin", raw: "admin", want: RoleAdmin},
		{name: "trim_and_case", raw: "  AdMiN ", want: RoleAdmin},
		{name: "invalid", raw: "owner", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseRole(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("ParseRole(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestParseMutationAuditArgsDerivesActorFromPrincipal(t *testing.T) {
	audit, err := parseMutationAuditArgs(map[string]any{
		"reason": "operator_cleanup",
	}, "ops@example.test")
	if err != nil {
		t.Fatalf("unexpected parseMutationAuditArgs error: %v", err)
	}
	if audit.Reason != "operator_cleanup" {
		t.Fatalf("unexpected reason: %#v", audit.Reason)
	}
	if audit.Actor != "ops@example.test" {
		t.Fatalf("expected derived actor from principal, got %#v", audit.Actor)
	}
}

func TestParseMutationAuditArgsRejectsActorPrincipalMismatch(t *testing.T) {
	_, err := parseMutationAuditArgs(map[string]any{
		"reason": "operator_cleanup",
		"actor":  "dev@example.test",
	}, "ops@example.test")
	if err == nil {
		t.Fatalf("expected actor/principal mismatch error")
	}
	if !strings.Contains(err.Error(), `actor "dev@example.test" must match configured MCP principal "ops@example.test"`) {
		t.Fatalf("unexpected mismatch error: %v", err)
	}
}

func TestToolConfigValidate(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	cfg := `"/webhooks/github" {
  deliver "https://example.com" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_validate", map[string]any{"path": cfgPath})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}

	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	okVal, ok := out["ok"].(bool)
	if !ok || !okVal {
		t.Fatalf("expected ok=true, got %#v", out["ok"])
	}
}

func TestToolConfigParse(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	cfg := `"/webhooks/github" {
  deliver "https://example.com" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_parse", map[string]any{"path": cfgPath})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}

	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if okVal, ok := out["ok"].(bool); !ok || !okVal {
		t.Fatalf("expected ok=true, got %#v", out["ok"])
	}

	ast, ok := out["ast"].(map[string]any)
	if !ok {
		t.Fatalf("expected ast object, got %T", out["ast"])
	}
	routesRaw, ok := ast["Routes"].([]any)
	if !ok || len(routesRaw) != 1 {
		t.Fatalf("expected one route in AST, got %#v", ast["Routes"])
	}
	route, ok := routesRaw[0].(map[string]any)
	if !ok {
		t.Fatalf("expected route object, got %T", routesRaw[0])
	}
	if gotPath, _ := route["Path"].(string); gotPath != "/webhooks/github" {
		t.Fatalf("expected route path /webhooks/github, got %#v", route["Path"])
	}
}

func TestToolConfigParseParseError(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`"/r" {`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_parse", map[string]any{"path": cfgPath})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}

	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); okVal {
		t.Fatalf("expected ok=false")
	}
	if parseOnly, _ := out["parse_only"].(bool); !parseOnly {
		t.Fatalf("expected parse_only=true, got %#v", out["parse_only"])
	}
	if got := anyStringSlice(out["errors"]); len(got) == 0 {
		t.Fatalf("expected parse errors, got %#v", out["errors"])
	}
}

func TestToolConfigParsePathGuardrail(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`"/r" { deliver "https://example.com" {} }`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_parse", map[string]any{"path": "/tmp/other"})
	if !resp.IsError {
		t.Fatalf("expected tool error")
	}
}

func TestToolConfigValidate_PathGuardrail(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`"/r" { deliver "https://example.com" {} }`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_validate", map[string]any{"path": "/tmp/other"})
	if !resp.IsError {
		t.Fatalf("expected tool error")
	}
}

func TestToolAdminHealthIncludesAdminDiagnostics(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
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
	store, err := queue.NewSQLiteStore(dbPath)
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
	store, err := queue.NewSQLiteStore(dbPath)
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

func TestToolConfigDiffChanged(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	current := `"/r" {
  deliver "https://example.com" {}
}
`
	candidate := `"/r" {
  deliver "https://example.org" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(current), 0o600); err != nil {
		t.Fatalf("write current: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_diff", map[string]any{
		"path":    cfgPath,
		"content": candidate,
	})
	if resp.IsError {
		t.Fatalf("unexpected config_diff error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); !okVal {
		t.Fatalf("expected ok=true, got %#v", out["ok"])
	}
	if changed, _ := out["changed"].(bool); !changed {
		t.Fatalf("expected changed=true")
	}
	diff, _ := out["unified_diff"].(string)
	if !strings.Contains(diff, "-  deliver \"https://example.com\"") {
		t.Fatalf("diff missing remove line: %q", diff)
	}
	if !strings.Contains(diff, "+  deliver \"https://example.org\"") {
		t.Fatalf("diff missing add line: %q", diff)
	}
}

func TestToolConfigDiffUnchanged(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	current := `"/r" {
  deliver "https://example.com" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(current), 0o600); err != nil {
		t.Fatalf("write current: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_diff", map[string]any{
		"path":    cfgPath,
		"content": current,
	})
	if resp.IsError {
		t.Fatalf("unexpected config_diff error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if changed, _ := out["changed"].(bool); changed {
		t.Fatalf("expected changed=false")
	}
	if diff, _ := out["unified_diff"].(string); diff != "" {
		t.Fatalf("expected empty diff, got %q", diff)
	}
}

func TestToolConfigDiffInvalidCandidate(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	current := `"/r" {
  deliver "https://example.com" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(current), 0o600); err != nil {
		t.Fatalf("write current: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_diff", map[string]any{
		"path":    cfgPath,
		"content": `"/r" {`,
	})
	if resp.IsError {
		t.Fatalf("unexpected config_diff tool error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); okVal {
		t.Fatalf("expected ok=false")
	}
	if changed, _ := out["changed"].(bool); changed {
		t.Fatalf("expected changed=false")
	}
}

func TestToolMessagesList(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	now := time.Date(2026, 2, 6, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(queue.Envelope{
		ID:         "evt_1",
		Route:      "/r",
		Target:     "pull",
		State:      queue.StateQueued,
		ReceivedAt: now,
		NextRunAt:  now,
		Payload:    []byte("hello"),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath)
	resp := callTool(t, s, "messages_list", map[string]any{
		"route":           "/r",
		"state":           "queued",
		"limit":           10,
		"include_payload": true,
	})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}

	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	var item map[string]any
	switch v := out["items"].(type) {
	case []any:
		if len(v) != 1 {
			t.Fatalf("expected 1 item, got %d", len(v))
		}
		casted, ok := v[0].(map[string]any)
		if !ok {
			t.Fatalf("item type: %T", v[0])
		}
		item = casted
	case []map[string]any:
		if len(v) != 1 {
			t.Fatalf("expected 1 item, got %d", len(v))
		}
		item = v[0]
	default:
		t.Fatalf("items type: %T", out["items"])
	}
	if item["id"] != "evt_1" {
		t.Fatalf("expected evt_1, got %#v", item["id"])
	}
}

func TestToolMessagesListManagedEndpoint(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
pull_api { auth token "raw:t" }
"/r" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e" }
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	now := time.Date(2026, 2, 6, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(queue.Envelope{
		ID:         "evt_m1",
		Route:      "/r",
		Target:     "pull",
		State:      queue.StateQueued,
		ReceivedAt: now,
		NextRunAt:  now,
	}); err != nil {
		t.Fatalf("enqueue evt_m1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:         "evt_m2",
		Route:      "/other",
		Target:     "pull",
		State:      queue.StateQueued,
		ReceivedAt: now,
		NextRunAt:  now,
	}); err != nil {
		t.Fatalf("enqueue evt_m2: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath)
	resp := callTool(t, s, "messages_list", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "queued",
		"limit":         10,
	})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}

	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	items, ok := out["items"].([]map[string]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected 1 managed item, got %#v", out["items"])
	}
	if items[0]["id"] != "evt_m1" {
		t.Fatalf("expected evt_m1, got %#v", items[0]["id"])
	}
}

func TestToolMessagesListManagedEndpointBadRequest(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
pull_api { auth token "raw:t" }
"/r" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e" }
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath)
	partial := callTool(t, s, "messages_list", map[string]any{
		"application": "billing",
		"state":       "queued",
	})
	if !partial.IsError {
		t.Fatalf("expected partial managed selector error")
	}

	forbidden := callTool(t, s, "messages_list", map[string]any{
		"route":         "/other",
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "queued",
	})
	if !forbidden.IsError {
		t.Fatalf("expected route-hint forbidden error")
	}
	if len(forbidden.Content) == 0 || !strings.Contains(forbidden.Content[0].Text, "route is not allowed when application and endpoint_name are set") {
		t.Fatalf("expected route-hint forbidden text, got %#v", forbidden.Content)
	}
}

func TestParseMessageListArgsRejectsInvalidManagedSelectorLabels(t *testing.T) {
	_, _, _, err := parseMessageListArgs(map[string]any{
		"application":   "billing core",
		"endpoint_name": "invoice.created",
	})
	if err == nil {
		t.Fatalf("expected invalid managed selector label error")
	}
	if !strings.Contains(err.Error(), "application must match") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseMessageListArgsRejectsNonAbsoluteRoute(t *testing.T) {
	_, _, _, err := parseMessageListArgs(map[string]any{
		"route": "relative",
	})
	if err == nil {
		t.Fatalf("expected route format error")
	}
	if !strings.Contains(err.Error(), "route must start with '/'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseAttemptListArgsRejectsInvalidManagedSelectorLabels(t *testing.T) {
	_, _, _, err := parseAttemptListArgs(map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice created",
	})
	if err == nil {
		t.Fatalf("expected invalid managed selector label error")
	}
	if !strings.Contains(err.Error(), "endpoint_name must match") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseAttemptListArgsRejectsNonAbsoluteRoute(t *testing.T) {
	_, _, _, err := parseAttemptListArgs(map[string]any{
		"route": "relative",
	})
	if err == nil {
		t.Fatalf("expected route format error")
	}
	if !strings.Contains(err.Error(), "route must start with '/'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseMessageManageFilterArgsRejectsInvalidManagedSelectorLabels(t *testing.T) {
	_, _, _, err := parseMessageManageFilterArgs(map[string]any{
		"application":   "billing core",
		"endpoint_name": "invoice.created",
		"state":         "queued",
		"limit":         float64(10),
	}, map[queue.State]struct{}{
		queue.StateQueued: {},
		queue.StateLeased: {},
		queue.StateDead:   {},
	})
	if err == nil {
		t.Fatalf("expected invalid managed selector label error")
	}
	if !strings.Contains(err.Error(), "application must match") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseMessageManageFilterArgsRejectsNonAbsoluteRoute(t *testing.T) {
	_, _, _, err := parseMessageManageFilterArgs(map[string]any{
		"route": "relative",
		"state": "queued",
		"limit": float64(10),
	}, map[queue.State]struct{}{
		queue.StateQueued: {},
		queue.StateLeased: {},
		queue.StateDead:   {},
	})
	if err == nil {
		t.Fatalf("expected route format error")
	}
	if !strings.Contains(err.Error(), "route must start with '/'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseDeadListArgsRejectsNonAbsoluteRoute(t *testing.T) {
	_, err := parseDeadListArgs(map[string]any{
		"route": "relative",
	})
	if err == nil {
		t.Fatalf("expected route format error")
	}
	if !strings.Contains(err.Error(), "route must start with '/'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestToolMutationDisabled(t *testing.T) {
	s := NewServer(nil, nil, "", "", WithMutationsEnabled(false))
	resp := callTool(t, s, "messages_cancel", map[string]any{
		"ids": []any{"evt_1"},
	})
	if !resp.IsError {
		t.Fatalf("expected error")
	}

	resp = callTool(t, s, "messages_requeue", map[string]any{
		"ids": []any{"evt_1"},
	})
	if !resp.IsError {
		t.Fatalf("expected error")
	}

	resp = callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"route": "/r",
	})
	if !resp.IsError {
		t.Fatalf("expected error")
	}

	resp = callTool(t, s, "messages_requeue_by_filter", map[string]any{
		"route": "/r",
	})
	if !resp.IsError {
		t.Fatalf("expected error")
	}
}

func TestToolConfigCompile_QueueBackendSummary(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	cfg := `pull_api { auth token "raw:t" }
"/r" {
  queue { backend memory }
  pull { path "/pull/r" }
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_compile", map[string]any{"path": cfgPath})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	summary, ok := out["summary"].(map[string]any)
	if !ok {
		t.Fatalf("summary type: %T", out["summary"])
	}
	if summary["queue_backend"] != "memory" {
		t.Fatalf("expected queue_backend=memory, got %#v", summary["queue_backend"])
	}
	if v, ok := summary["publish_policy_direct_enabled"].(bool); !ok || !v {
		t.Fatalf("expected publish_policy_direct_enabled=true, got %#v", summary["publish_policy_direct_enabled"])
	}
	if v, ok := summary["publish_policy_managed_enabled"].(bool); !ok || !v {
		t.Fatalf("expected publish_policy_managed_enabled=true, got %#v", summary["publish_policy_managed_enabled"])
	}
	if v, ok := summary["publish_policy_allow_pull_routes"].(bool); !ok || !v {
		t.Fatalf("expected publish_policy_allow_pull_routes=true, got %#v", summary["publish_policy_allow_pull_routes"])
	}
	if v, ok := summary["publish_policy_allow_deliver_routes"].(bool); !ok || !v {
		t.Fatalf("expected publish_policy_allow_deliver_routes=true, got %#v", summary["publish_policy_allow_deliver_routes"])
	}
	if v, ok := summary["publish_policy_require_actor"].(bool); !ok || v {
		t.Fatalf("expected publish_policy_require_actor=false, got %#v", summary["publish_policy_require_actor"])
	}
	if v, ok := summary["publish_policy_require_request_id"].(bool); !ok || v {
		t.Fatalf("expected publish_policy_require_request_id=false, got %#v", summary["publish_policy_require_request_id"])
	}
	if v, ok := summary["publish_policy_fail_closed"].(bool); !ok || v {
		t.Fatalf("expected publish_policy_fail_closed=false, got %#v", summary["publish_policy_fail_closed"])
	}
	if got := summary["publish_policy_actor_allowlist"]; len(anyStringSlice(got)) != 0 {
		t.Fatalf("expected empty publish_policy_actor_allowlist, got %#v", got)
	}
	if got := summary["publish_policy_actor_prefixes"]; len(anyStringSlice(got)) != 0 {
		t.Fatalf("expected empty publish_policy_actor_prefixes, got %#v", got)
	}
}

func TestToolConfigCompile_AuthHMACHeaderCollisionValidation(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	cfg := `pull_api { auth token "raw:t" }
"/r" {
  auth hmac {
    secret "raw:s1"
    signature_header "X-Timestamp"
  }
  pull { path "/pull/r" }
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_compile", map[string]any{"path": cfgPath})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if okValue, _ := out["ok"].(bool); okValue {
		t.Fatalf("expected compile ok=false for header collision, got %#v", out)
	}
	errors := anyStringSlice(out["errors"])
	found := false
	for _, e := range errors {
		if strings.Contains(e, "auth hmac signature_header and timestamp_header must differ") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected auth hmac header-collision error, got %#v", errors)
	}
}

func TestToolConfigFmtPreview_ObservabilityMetricsShorthand(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	cfg := `observability {
  metrics off
}
pull_api { auth token "raw:t" }
"/r" { pull { path "/pull/r" } }
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_fmt_preview", map[string]any{"path": cfgPath})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	formatted, _ := out["formatted"].(string)
	if !strings.Contains(formatted, "metrics off") {
		t.Fatalf("expected metrics shorthand in formatted output, got:\n%s", formatted)
	}
	if strings.Contains(formatted, "metrics {") {
		t.Fatalf("expected shorthand preservation, got block form:\n%s", formatted)
	}
}

func TestToolConfigFmtPreview_ObservabilityTracingShorthand(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	cfg := `observability {
  tracing off
}
pull_api { auth token "raw:t" }
"/r" { pull { path "/pull/r" } }
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_fmt_preview", map[string]any{"path": cfgPath})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	formatted, _ := out["formatted"].(string)
	if !strings.Contains(formatted, "tracing off") {
		t.Fatalf("expected tracing shorthand in formatted output, got:\n%s", formatted)
	}
	if strings.Contains(formatted, "tracing {") {
		t.Fatalf("expected shorthand preservation, got block form:\n%s", formatted)
	}
}

func TestToolConfigFmtPreview_RouteQueueShorthand(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	cfg := `pull_api { auth token "raw:t" }
"/r" {
  queue memory
  pull { path "/pull/r" }
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_fmt_preview", map[string]any{"path": cfgPath})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	formatted, _ := out["formatted"].(string)
	if !strings.Contains(formatted, "queue memory") {
		t.Fatalf("expected queue shorthand in formatted output, got:\n%s", formatted)
	}
	if strings.Contains(formatted, "queue {") {
		t.Fatalf("expected queue shorthand preservation, got block form:\n%s", formatted)
	}
}

func TestToolConfigFmtPreview_AuthHMACShorthandInlineBlock(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	cfg := `pull_api { auth token "raw:t" }
"/r" {
  auth hmac secret_ref "S1" {
    signature_header "X-Hookaido-Signature"
    timestamp_header "X-Hookaido-Timestamp"
    nonce_header "X-Hookaido-Nonce"
    tolerance "10m"
  }
  pull { path "/pull/r" }
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_fmt_preview", map[string]any{"path": cfgPath})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	formatted, _ := out["formatted"].(string)
	if !strings.Contains(formatted, "auth hmac {") {
		t.Fatalf("expected auth hmac block in formatted output, got:\n%s", formatted)
	}
	if !strings.Contains(formatted, `secret_ref "S1"`) {
		t.Fatalf("expected secret_ref in formatted output, got:\n%s", formatted)
	}
	if strings.Contains(formatted, `auth hmac secret_ref "S1"`) {
		t.Fatalf("expected normalized block output, got shorthand+block form:\n%s", formatted)
	}
}

func TestToolConfigCompile_AuthHMACShorthandInlineBlock(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	cfg := `secrets {
  secret "S1" {
    value "raw:s1"
    valid_from "2025-01-01T00:00:00Z"
  }
}
pull_api { auth token "raw:t" }
"/r" {
  auth hmac secret_ref "S1" {
    signature_header x-hookaido-signature
    timestamp_header x-hookaido-timestamp
    nonce_header x-hookaido-nonce
    tolerance 7m
  }
  pull { path "/pull/r" }
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "config_compile", map[string]any{"path": cfgPath})
	if resp.IsError {
		t.Fatalf("unexpected tool error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if okValue, _ := out["ok"].(bool); !okValue {
		t.Fatalf("expected compile ok=true, got %#v", out)
	}
	summary, ok := out["summary"].(map[string]any)
	if !ok {
		t.Fatalf("summary type: %T", out["summary"])
	}
	if got := intFromAny(summary["route_count"]); got != 1 {
		t.Fatalf("expected route_count=1, got %#v", summary["route_count"])
	}
	if got := intFromAny(summary["pull_route_count"]); got != 1 {
		t.Fatalf("expected pull_route_count=1, got %#v", summary["pull_route_count"])
	}
}

func TestToolRuntimeControlDisabled(t *testing.T) {
	s := NewServer(nil, nil, "", "")
	resp := callTool(t, s, "instance_status", map[string]any{})
	if !resp.IsError {
		t.Fatalf("expected error")
	}
}

func TestToolConfigApplyPreviewOnly(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	original := `"/r" {
  deliver "https://example.com" {}
}
`
	candidate := `"/r" {
  deliver "https://example.org" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "config_apply", map[string]any{
		"path":    cfgPath,
		"content": candidate,
		"mode":    "preview_only",
	})
	if resp.IsError {
		t.Fatalf("unexpected config_apply error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); !okVal {
		t.Fatalf("expected ok=true, got %#v", out["ok"])
	}
	if applied, _ := out["applied"].(bool); applied {
		t.Fatalf("expected applied=false")
	}

	got, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(got) != original {
		t.Fatalf("preview mode must not write file")
	}
}

func TestToolConfigApplyWriteOnly(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	original := `"/r" {
  deliver "https://example.com" {}
}
`
	candidate := `"/r" {
  deliver "https://example.org" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "config_apply", map[string]any{
		"path":    cfgPath,
		"content": candidate,
		"mode":    "write_only",
	})
	if resp.IsError {
		t.Fatalf("unexpected config_apply error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); !okVal {
		t.Fatalf("expected ok=true, got %#v", out["ok"])
	}
	if applied, _ := out["applied"].(bool); !applied {
		t.Fatalf("expected applied=true")
	}

	got, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(got) != candidate {
		t.Fatalf("write mode did not apply candidate")
	}
}

func TestToolConfigApplyInvalidCandidate(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	original := `"/r" {
  deliver "https://example.com" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "config_apply", map[string]any{
		"path":    cfgPath,
		"content": `"/r" {`,
		"mode":    "write_only",
	})
	if resp.IsError {
		t.Fatalf("unexpected config_apply tool error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); okVal {
		t.Fatalf("expected ok=false")
	}
	if applied, _ := out["applied"].(bool); applied {
		t.Fatalf("expected applied=false")
	}

	got, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(got) != original {
		t.Fatalf("invalid candidate must not write file")
	}
}

func TestToolConfigApplyWriteAndReloadSuccess(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	original := `"/r" {
  deliver "https://example.com" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}

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
			t.Fatalf("timed out waiting for health server shutdown")
		}
	})

	candidate := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  deliver "https://example.org" {}
}
`, addr, token)

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "config_apply", map[string]any{
		"path":           cfgPath,
		"content":        candidate,
		"mode":           "write_and_reload",
		"reload_timeout": "2s",
	})
	if resp.IsError {
		t.Fatalf("unexpected config_apply error: %s", resp.Content[0].Text)
	}

	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); !okVal {
		t.Fatalf("expected ok=true, got %#v", out["ok"])
	}
	if applied, _ := out["applied"].(bool); !applied {
		t.Fatalf("expected applied=true")
	}
	if reloaded, _ := out["reloaded"].(bool); !reloaded {
		t.Fatalf("expected reloaded=true")
	}
	if rolledBack, _ := out["rolled_back"].(bool); rolledBack {
		t.Fatalf("expected rolled_back=false")
	}
	if reloadErr, _ := out["reload_error"].(string); reloadErr != "" {
		t.Fatalf("expected empty reload_error, got %q", reloadErr)
	}
	if healthURL, _ := out["health_url"].(string); !strings.Contains(healthURL, "/admin/healthz") {
		t.Fatalf("unexpected health_url %q", healthURL)
	}

	got, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(got) != candidate {
		t.Fatalf("write_and_reload mode did not persist candidate")
	}
}

func TestToolConfigApplyWriteAndReloadRollback(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "Hookaidofile")
	original := `"/r" {
  deliver "https://example.com" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Path == "/admin/healthz" {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for failing health server shutdown")
		}
	})

	candidate := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
}
"/r" {
  deliver "https://example.org" {}
}
`, addr)

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "config_apply", map[string]any{
		"path":           cfgPath,
		"content":        candidate,
		"mode":           "write_and_reload",
		"reload_timeout": "300ms",
	})
	if resp.IsError {
		t.Fatalf("unexpected config_apply error: %s", resp.Content[0].Text)
	}

	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); okVal {
		t.Fatalf("expected ok=false")
	}
	if applied, _ := out["applied"].(bool); applied {
		t.Fatalf("expected applied=false after rollback")
	}
	if reloaded, _ := out["reloaded"].(bool); reloaded {
		t.Fatalf("expected reloaded=false")
	}
	if rolledBack, _ := out["rolled_back"].(bool); !rolledBack {
		t.Fatalf("expected rolled_back=true")
	}
	if reloadErr, _ := out["reload_error"].(string); strings.TrimSpace(reloadErr) == "" {
		t.Fatalf("expected non-empty reload_error")
	}

	got, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(got) != original {
		t.Fatalf("expected rollback to restore original config")
	}
}

func TestToolMessagesCancel(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:     "evt_cancel",
		Route:  "/r",
		Target: "pull",
		State:  queue.StateQueued,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	cancelResp := callTool(t, s, "messages_cancel", map[string]any{
		"ids":    []any{"evt_cancel"},
		"reason": "operator_cleanup",
	})
	if cancelResp.IsError {
		t.Fatalf("unexpected cancel error: %s", cancelResp.Content[0].Text)
	}

	listResp := callTool(t, s, "messages_list", map[string]any{
		"state": "canceled",
		"limit": 10,
	})
	if listResp.IsError {
		t.Fatalf("unexpected list error: %s", listResp.Content[0].Text)
	}
	out, ok := listResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", listResp.StructuredContent)
	}
	items, ok := out["items"].([]map[string]any)
	if !ok {
		t.Fatalf("items type: %T", out["items"])
	}
	if len(items) != 1 || items[0]["id"] != "evt_cancel" {
		t.Fatalf("unexpected canceled items: %#v", items)
	}
}

func TestToolMessagesCancelRequiresReason(t *testing.T) {
	s := NewServer(nil, nil, "", "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel", map[string]any{
		"ids": []any{"evt_cancel"},
	})
	if !resp.IsError {
		t.Fatalf("expected reason validation error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "reason is required") {
		t.Fatalf("unexpected error text: %#v", resp.Content)
	}
}

func TestToolMessagesRequeue(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:     "evt_requeue",
		Route:  "/r",
		Target: "pull",
		State:  queue.StateDead,
	}); err != nil {
		t.Fatalf("enqueue dead: %v", err)
	}
	if _, err := store.CancelMessages(queue.MessageCancelRequest{IDs: []string{"evt_requeue"}}); err != nil {
		t.Fatalf("cancel message: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	requeueResp := callTool(t, s, "messages_requeue", map[string]any{
		"ids":    []any{"evt_requeue"},
		"reason": "operator_cleanup",
	})
	if requeueResp.IsError {
		t.Fatalf("unexpected requeue error: %s", requeueResp.Content[0].Text)
	}

	listResp := callTool(t, s, "messages_list", map[string]any{
		"state": "queued",
		"limit": 10,
	})
	if listResp.IsError {
		t.Fatalf("unexpected list error: %s", listResp.Content[0].Text)
	}
	out, ok := listResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", listResp.StructuredContent)
	}
	items, ok := out["items"].([]map[string]any)
	if !ok {
		t.Fatalf("items type: %T", out["items"])
	}
	if len(items) != 1 || items[0]["id"] != "evt_requeue" {
		t.Fatalf("unexpected queued items: %#v", items)
	}
}

func TestToolMessagesResume(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:     "evt_resume",
		Route:  "/r",
		Target: "pull",
		State:  queue.StateCanceled,
	}); err != nil {
		t.Fatalf("enqueue canceled: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resumeResp := callTool(t, s, "messages_resume", map[string]any{
		"ids":    []any{"evt_resume"},
		"reason": "operator_cleanup",
	})
	if resumeResp.IsError {
		t.Fatalf("unexpected resume error: %s", resumeResp.Content[0].Text)
	}

	listResp := callTool(t, s, "messages_list", map[string]any{
		"state": "queued",
		"limit": 10,
	})
	if listResp.IsError {
		t.Fatalf("unexpected list error: %s", listResp.Content[0].Text)
	}
	out, ok := listResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", listResp.StructuredContent)
	}
	items, ok := out["items"].([]map[string]any)
	if !ok {
		t.Fatalf("items type: %T", out["items"])
	}
	if len(items) != 1 || items[0]["id"] != "evt_resume" {
		t.Fatalf("unexpected queued items: %#v", items)
	}
}

func TestToolIDMutationsRejectsDerivedScopedManagedActorByPolicyInSQLiteMode(t *testing.T) {
	tests := []struct {
		tool  string
		state queue.State
	}{
		{tool: "messages_cancel", state: queue.StateQueued},
		{tool: "messages_requeue", state: queue.StateDead},
		{tool: "messages_resume", state: queue.StateCanceled},
		{tool: "dlq_requeue", state: queue.StateDead},
		{tool: "dlq_delete", state: queue.StateDead},
	}

	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			tmp := t.TempDir()
			cfgPath := filepath.Join(tmp, "Hookaidofile")
			dbPath := filepath.Join(tmp, "hookaido.db")
			if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/managed" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			store, err := queue.NewSQLiteStore(dbPath)
			if err != nil {
				t.Fatalf("new sqlite store: %v", err)
			}
			if err := store.Enqueue(queue.Envelope{
				ID:     "evt_managed_1",
				Route:  "/managed",
				Target: "https://example.org/hook",
				State:  tc.state,
			}); err != nil {
				_ = store.Close()
				t.Fatalf("enqueue: %v", err)
			}
			_ = store.Close()

			s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
			resp := callTool(t, s, tc.tool, map[string]any{
				"ids":    []any{"evt_managed_1"},
				"reason": "operator_cleanup",
			})
			if !resp.IsError {
				t.Fatalf("expected %s error when derived actor is not allowed by scoped managed actor policy", tc.tool)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "is not allowed for endpoint-scoped managed mutation") {
				t.Fatalf("expected scoped managed actor denied text, got %#v", resp.Content)
			}
		})
	}
}

func TestToolMessagesCancelUnmanagedIDIgnoresScopedManagedActorPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/managed" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/managed" {}
}
"/direct" {
  deliver "https://example.org/direct" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:     "evt_direct_1",
		Route:  "/direct",
		Target: "https://example.org/direct",
		State:  queue.StateQueued,
	}); err != nil {
		_ = store.Close()
		t.Fatalf("enqueue: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel", map[string]any{
		"ids":    []any{"evt_direct_1"},
		"reason": "operator_cleanup",
	})
	if resp.IsError {
		t.Fatalf("unexpected messages_cancel error: %s", resp.Content[0].Text)
	}

	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if canceled := intFromAny(out["canceled"]); canceled != 1 {
		t.Fatalf("expected canceled=1, got %#v", out["canceled"])
	}
}

func TestToolIDMutationsFailClosedWhenConfigUnavailableByPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    fail_closed on
  }
}
"/managed" {
  queue { backend memory }
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/pull/managed" }
}
"/other" {
  queue { backend sqlite }
  pull { path "/pull/other" }
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel", map[string]any{
		"ids":    []any{"evt_managed_1"},
		"reason": "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected fail-closed error when config compile is unavailable")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "cannot evaluate scoped managed id mutation policy") {
		t.Fatalf("expected fail-closed policy error text, got %#v", resp.Content)
	}
}

func TestToolIDMutationsConfigUnavailableFailOpenWhenPolicyDisabledInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/managed" {
  queue { backend memory }
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/pull/managed" }
}
"/other" {
  queue { backend sqlite }
  pull { path "/pull/other" }
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:     "evt_managed_1",
		Route:  "/managed",
		Target: "pull",
		State:  queue.StateQueued,
	}); err != nil {
		_ = store.Close()
		t.Fatalf("enqueue: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel", map[string]any{
		"ids":    []any{"evt_managed_1"},
		"reason": "operator_cleanup",
	})
	if resp.IsError {
		t.Fatalf("unexpected messages_cancel error with fail-open policy: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if canceled := intFromAny(out["canceled"]); canceled != 1 {
		t.Fatalf("expected canceled=1, got %#v", out["canceled"])
	}
}

func TestToolMessagesFilterMutationsFailClosedWhenConfigUnavailableByPolicyInSQLiteMode(t *testing.T) {
	tests := []struct {
		tool  string
		state queue.State
	}{
		{tool: "messages_cancel_by_filter", state: queue.StateQueued},
		{tool: "messages_requeue_by_filter", state: queue.StateDead},
		{tool: "messages_resume_by_filter", state: queue.StateCanceled},
	}

	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			tmp := t.TempDir()
			cfgPath := filepath.Join(tmp, "Hookaidofile")
			dbPath := filepath.Join(tmp, "hookaido.db")
			if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    fail_closed on
  }
}
"/managed" {
  queue { backend memory }
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/pull/managed" }
}
"/other" {
  queue { backend sqlite }
  pull { path "/pull/other" }
}
`), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			store, err := queue.NewSQLiteStore(dbPath)
			if err != nil {
				t.Fatalf("new sqlite store: %v", err)
			}
			if err := store.Enqueue(queue.Envelope{
				ID:     "evt_direct_1",
				Route:  "/other",
				Target: "pull",
				State:  tc.state,
			}); err != nil {
				_ = store.Close()
				t.Fatalf("enqueue: %v", err)
			}
			_ = store.Close()

			s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
			resp := callTool(t, s, tc.tool, map[string]any{
				"route":  "/other",
				"state":  string(tc.state),
				"limit":  10,
				"reason": "operator_cleanup",
			})
			if !resp.IsError {
				t.Fatalf("expected fail-closed %s error when config compile is unavailable", tc.tool)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "cannot evaluate scoped managed filter mutation policy") {
				t.Fatalf("expected fail-closed policy error text, got %#v", resp.Content)
			}

			verifyStore, err := queue.NewSQLiteStore(dbPath)
			if err != nil {
				t.Fatalf("reopen sqlite store: %v", err)
			}
			lookup, err := verifyStore.LookupMessages(queue.MessageLookupRequest{IDs: []string{"evt_direct_1"}})
			_ = verifyStore.Close()
			if err != nil {
				t.Fatalf("lookup evt_direct_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("expected state %q to remain unchanged, got %#v", tc.state, lookup.Items)
			}
		})
	}
}

func TestToolMessagesFilterMutationsConfigUnavailableFailOpenWhenPolicyDisabledInSQLiteMode(t *testing.T) {
	tests := []struct {
		tool          string
		state         queue.State
		changedKey    string
		expectedState queue.State
	}{
		{tool: "messages_cancel_by_filter", state: queue.StateQueued, changedKey: "canceled", expectedState: queue.StateCanceled},
		{tool: "messages_requeue_by_filter", state: queue.StateDead, changedKey: "requeued", expectedState: queue.StateQueued},
		{tool: "messages_resume_by_filter", state: queue.StateCanceled, changedKey: "resumed", expectedState: queue.StateQueued},
	}

	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			tmp := t.TempDir()
			cfgPath := filepath.Join(tmp, "Hookaidofile")
			dbPath := filepath.Join(tmp, "hookaido.db")
			if err := os.WriteFile(cfgPath, []byte(`
"/managed" {
  queue { backend memory }
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/pull/managed" }
}
"/other" {
  queue { backend sqlite }
  pull { path "/pull/other" }
}
`), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			store, err := queue.NewSQLiteStore(dbPath)
			if err != nil {
				t.Fatalf("new sqlite store: %v", err)
			}
			if err := store.Enqueue(queue.Envelope{
				ID:     "evt_direct_1",
				Route:  "/other",
				Target: "pull",
				State:  tc.state,
			}); err != nil {
				_ = store.Close()
				t.Fatalf("enqueue: %v", err)
			}
			_ = store.Close()

			s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
			resp := callTool(t, s, tc.tool, map[string]any{
				"route":  "/other",
				"state":  string(tc.state),
				"limit":  10,
				"reason": "operator_cleanup",
			})
			if resp.IsError {
				t.Fatalf("unexpected %s error with fail-open policy: %s", tc.tool, resp.Content[0].Text)
			}
			out, ok := resp.StructuredContent.(map[string]any)
			if !ok {
				t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
			}
			if changed := intFromAny(out[tc.changedKey]); changed != 1 {
				t.Fatalf("expected %s=1, got %#v", tc.changedKey, out[tc.changedKey])
			}

			verifyStore, err := queue.NewSQLiteStore(dbPath)
			if err != nil {
				t.Fatalf("reopen sqlite store: %v", err)
			}
			lookup, err := verifyStore.LookupMessages(queue.MessageLookupRequest{IDs: []string{"evt_direct_1"}})
			_ = verifyStore.Close()
			if err != nil {
				t.Fatalf("lookup evt_direct_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.expectedState {
				t.Fatalf("expected state %q, got %#v", tc.expectedState, lookup.Items)
			}
		})
	}
}

func TestToolMessagesPublish(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	publishResp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":          "evt_pub_1",
				"route":       "/r",
				"target":      "https://example.org/hook",
				"payload_b64": "aGVsbG8=",
			},
			map[string]any{
				"id":     "evt_pub_2",
				"route":  "/r",
				"target": "https://example.org/hook",
			},
		},
	})
	if publishResp.IsError {
		t.Fatalf("unexpected publish error: %s", publishResp.Content[0].Text)
	}
	out, ok := publishResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", publishResp.StructuredContent)
	}
	if published := intFromAny(out["published"]); published != 2 {
		t.Fatalf("expected published=2, got %#v", out["published"])
	}
	audit, ok := out["audit"].(map[string]any)
	if !ok {
		t.Fatalf("expected audit object, got %T", out["audit"])
	}
	if reason, _ := audit["reason"].(string); reason != "operator_publish" {
		t.Fatalf("expected audit.reason=operator_publish, got %#v", audit["reason"])
	}
	if principal, _ := audit["principal"].(string); principal != "test-admin" {
		t.Fatalf("expected audit.principal=test-admin, got %#v", audit["principal"])
	}

	listResp := callTool(t, s, "messages_list", map[string]any{
		"state": "queued",
		"limit": 10,
	})
	if listResp.IsError {
		t.Fatalf("unexpected list error: %s", listResp.Content[0].Text)
	}
	listOut, ok := listResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", listResp.StructuredContent)
	}
	items, ok := listOut["items"].([]map[string]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected 2 queued items, got %#v", listOut["items"])
	}
}

func TestToolAttemptsListManagedSelectorSQLite(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")

	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	if err := store.RecordAttempt(queue.DeliveryAttempt{
		ID:         "att_sql_managed_1",
		EventID:    "evt_sql_managed_1",
		Route:      "/r",
		Target:     "https://example.org/hook",
		Attempt:    1,
		StatusCode: 503,
		Outcome:    queue.AttemptOutcomeRetry,
		CreatedAt:  now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("record attempt 1: %v", err)
	}
	if err := store.RecordAttempt(queue.DeliveryAttempt{
		ID:         "att_sql_managed_2",
		EventID:    "evt_sql_managed_1",
		Route:      "/r",
		Target:     "https://example.org/hook",
		Attempt:    2,
		StatusCode: 502,
		Outcome:    queue.AttemptOutcomeDead,
		DeadReason: "max_retries",
		CreatedAt:  now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("record attempt 2: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath)
	resp := callTool(t, s, "attempts_list", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"outcome":       "dead",
		"limit":         1,
	})
	if resp.IsError {
		t.Fatalf("unexpected attempts_list error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	items, ok := out["items"].([]map[string]any)
	if !ok {
		rawItems, ok := out["items"].([]any)
		if !ok {
			t.Fatalf("unexpected items type %T", out["items"])
		}
		items = make([]map[string]any, 0, len(rawItems))
		for _, raw := range rawItems {
			item, ok := raw.(map[string]any)
			if !ok {
				t.Fatalf("unexpected item type %T", raw)
			}
			items = append(items, item)
		}
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 attempt, got %#v", out["items"])
	}
	if id, _ := items[0]["id"].(string); id != "att_sql_managed_2" {
		t.Fatalf("expected id att_sql_managed_2, got %#v", items[0]["id"])
	}
}

func TestToolMessagesListMemoryBackendViaAdmin(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/messages" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			q := r.URL.Query()
			if q.Get("route") != "/r" || q.Get("state") != "queued" || q.Get("limit") != "10" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"id":          "evt_mem_list_1",
						"route":       "/r",
						"target":      "pull",
						"state":       "queued",
						"received_at": "2026-02-07T12:00:00Z",
						"attempt":     0,
						"next_run_at": "2026-02-07T12:00:00Z",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "messages_list", map[string]any{
		"route": "/r",
		"state": "queued",
		"limit": 10,
	})
	if resp.IsError {
		t.Fatalf("unexpected messages_list error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	items, ok := out["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one item, got %#v", out["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected item type %T", items[0])
	}
	if id, _ := item["id"].(string); id != "evt_mem_list_1" {
		t.Fatalf("expected id evt_mem_list_1, got %#v", item["id"])
	}
}

func TestToolMessagesListMemoryBackendManagedViaAdminScopedPath(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedPath := "/admin/applications/billing/endpoints/invoice.created/messages"
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != scopedPath {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			q := r.URL.Query()
			if q.Get("route") != "" || q.Get("application") != "" || q.Get("endpoint_name") != "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if q.Get("state") != "queued" || q.Get("limit") != "10" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"id":          "evt_mem_list_managed_1",
						"route":       "/r",
						"target":      "https://example.org/hook",
						"state":       "queued",
						"received_at": "2026-02-07T12:00:00Z",
						"attempt":     0,
						"next_run_at": "2026-02-07T12:00:00Z",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "messages_list", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "queued",
		"limit":         10,
	})
	if resp.IsError {
		t.Fatalf("unexpected messages_list error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	items, ok := out["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one item, got %#v", out["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected item type %T", items[0])
	}
	if id, _ := item["id"].(string); id != "evt_mem_list_managed_1" {
		t.Fatalf("expected id evt_mem_list_managed_1, got %#v", item["id"])
	}
}

func TestToolMessagesListMemoryBackendManagedRouteHintForbiddenBeforeAdmin(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	requestCount := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "messages_list", map[string]any{
		"route":         "/other",
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"limit":         10,
	})
	if !resp.IsError {
		t.Fatalf("expected messages_list route-hint forbidden error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "route is not allowed when application and endpoint_name are set") {
		t.Fatalf("expected route-hint forbidden text, got %#v", resp.Content)
	}
	if requestCount != 0 {
		t.Fatalf("expected no admin request on local validation failure, got %d", requestCount)
	}
}

func TestToolAttemptsListMemoryBackendManagedSelectorViaAdmin(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/attempts" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			q := r.URL.Query()
			if q.Get("route") != "" || q.Get("application") != "billing" || q.Get("endpoint_name") != "invoice.created" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if q.Get("limit") != "10" || q.Get("outcome") != "dead" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"id":          "att_mem_managed_1",
						"event_id":    "evt_mem_managed_1",
						"route":       "/r",
						"target":      "https://example.org/hook",
						"attempt":     2,
						"status_code": 502,
						"outcome":     "dead",
						"dead_reason": "max_retries",
						"created_at":  "2026-02-07T12:00:00Z",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "attempts_list", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"outcome":       "dead",
		"limit":         10,
	})
	if resp.IsError {
		t.Fatalf("unexpected attempts_list error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	items, ok := out["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one item, got %#v", out["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected item type %T", items[0])
	}
	if id, _ := item["id"].(string); id != "att_mem_managed_1" {
		t.Fatalf("expected id att_mem_managed_1, got %#v", item["id"])
	}
}

func TestToolAttemptsListMemoryBackendManagedViaAdminTargetMismatchFallbackDetail(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/attempts" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			q := r.URL.Query()
			if q.Get("route") != "" || q.Get("application") != "billing" || q.Get("endpoint_name") != "invoice.created" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if q.Get("limit") != "10" || q.Get("outcome") != "dead" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "managed_target_mismatch",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "attempts_list", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"outcome":       "dead",
		"limit":         10,
	})
	if !resp.IsError {
		t.Fatalf("expected attempts_list error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=managed_target_mismatch") {
		t.Fatalf("expected managed_target_mismatch code, got %q", msg)
	}
	if !strings.Contains(msg, "detail=managed endpoint target/ownership mapping is out of sync with route policy resolution") {
		t.Fatalf("expected managed_target_mismatch fallback detail, got %q", msg)
	}
}

func TestToolAttemptsListMemoryBackendManagedRouteHintForbiddenBeforeAdmin(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	requestCount := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "attempts_list", map[string]any{
		"route":         "/other",
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"limit":         10,
	})
	if !resp.IsError {
		t.Fatalf("expected attempts_list route-hint forbidden error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "route is not allowed when application and endpoint_name are set") {
		t.Fatalf("expected route-hint forbidden text, got %#v", resp.Content)
	}
	if requestCount != 0 {
		t.Fatalf("expected no admin request on local validation failure, got %d", requestCount)
	}
}

func TestToolMessagesListMemoryBackendManagedViaAdminStructuredError(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedPath := "/admin/applications/billing/endpoints/invoice.created/messages"
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != scopedPath {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":   "managed_endpoint_not_found",
				"detail": "managed endpoint not found",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "messages_list", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "queued",
		"limit":         10,
	})
	if !resp.IsError {
		t.Fatalf("expected messages_list error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=managed_endpoint_not_found") || !strings.Contains(msg, "managed endpoint not found") {
		t.Fatalf("expected structured code/detail in mapped error, got %q", msg)
	}
}

func TestToolMessagesListMemoryBackendManagedViaAdminResolverFallbackDetail(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedPath := "/admin/applications/billing/endpoints/invoice.created/messages"
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != scopedPath {
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
				"code": "managed_resolver_missing",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "messages_list", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "queued",
		"limit":         10,
	})
	if !resp.IsError {
		t.Fatalf("expected messages_list error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=managed_resolver_missing") {
		t.Fatalf("expected managed_resolver_missing code, got %q", msg)
	}
	if !strings.Contains(msg, "detail=managed endpoint resolution is not configured") {
		t.Fatalf("expected managed_resolver_missing fallback detail, got %q", msg)
	}
}

func TestToolMessagesListMemoryBackendManagedViaAdminTargetMismatchFallbackDetail(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedPath := "/admin/applications/billing/endpoints/invoice.created/messages"
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != scopedPath {
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
				"code": "managed_target_mismatch",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "messages_list", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "queued",
		"limit":         10,
	})
	if !resp.IsError {
		t.Fatalf("expected messages_list error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=managed_target_mismatch") {
		t.Fatalf("expected managed_target_mismatch code, got %q", msg)
	}
	if !strings.Contains(msg, "detail=managed endpoint target/ownership mapping is out of sync with route policy resolution") {
		t.Fatalf("expected managed_target_mismatch fallback detail, got %q", msg)
	}
}

func TestToolMessagesListMemoryBackendRouteSelectorManagedTargetMismatchFallbackDetail(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/messages" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			q := r.URL.Query()
			if q.Get("route") != "/managed" || q.Get("state") != "queued" || q.Get("limit") != "10" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "managed_target_mismatch",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "messages_list", map[string]any{
		"route": "/managed",
		"state": "queued",
		"limit": 10,
	})
	if !resp.IsError {
		t.Fatalf("expected messages_list error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=managed_target_mismatch") {
		t.Fatalf("expected managed_target_mismatch code, got %q", msg)
	}
	if !strings.Contains(msg, "detail=managed endpoint target/ownership mapping is out of sync with route policy resolution") {
		t.Fatalf("expected managed_target_mismatch fallback detail, got %q", msg)
	}
}

func TestToolAttemptsListMemoryBackendViaAdminResolverFallbackDetail(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/attempts" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			q := r.URL.Query()
			if q.Get("route") != "/r" || q.Get("limit") != "10" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "managed_resolver_missing",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "attempts_list", map[string]any{
		"route": "/r",
		"limit": 10,
	})
	if !resp.IsError {
		t.Fatalf("expected attempts_list error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=managed_resolver_missing") {
		t.Fatalf("expected managed_resolver_missing code, got %q", msg)
	}
	if !strings.Contains(msg, "detail=managed endpoint resolution is not configured") {
		t.Fatalf("expected managed_resolver_missing fallback detail, got %q", msg)
	}
}

func TestToolAttemptsListMemoryBackendRouteSelectorManagedTargetMismatchFallbackDetail(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/attempts" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			q := r.URL.Query()
			if q.Get("route") != "/managed" || q.Get("outcome") != "retry" || q.Get("limit") != "10" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "managed_target_mismatch",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "attempts_list", map[string]any{
		"route":   "/managed",
		"outcome": "retry",
		"limit":   10,
	})
	if !resp.IsError {
		t.Fatalf("expected attempts_list error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=managed_target_mismatch") {
		t.Fatalf("expected managed_target_mismatch code, got %q", msg)
	}
	if !strings.Contains(msg, "detail=managed endpoint target/ownership mapping is out of sync with route policy resolution") {
		t.Fatalf("expected managed_target_mismatch fallback detail, got %q", msg)
	}
}

func TestToolAttemptsListMemoryBackendManagedViaAdminNotFoundFallbackDetail(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/attempts" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "managed_endpoint_not_found",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/hooks" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "attempts_list", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"outcome":       "dead",
		"limit":         10,
	})
	if !resp.IsError {
		t.Fatalf("expected attempts_list error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=managed_endpoint_not_found") {
		t.Fatalf("expected managed_endpoint_not_found code, got %q", msg)
	}
	if !strings.Contains(msg, "detail=managed endpoint not found") {
		t.Fatalf("expected managed_endpoint_not_found fallback detail, got %q", msg)
	}
}

func TestToolMessagesCancelByFilterMemoryBackendManagedViaAdminScopedPath(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedPath := "/admin/applications/billing/endpoints/invoice.created/messages/cancel_by_filter"
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != scopedPath {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_cleanup" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if payload["route"] != nil || payload["application"] != nil || payload["endpoint_name"] != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if payload["state"] != "queued" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if intFromAny(payload["limit"]) != 10 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"matched":      1,
				"canceled":     1,
				"preview_only": false,
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "queued",
		"limit":         10,
		"reason":        "operator_cleanup",
	})
	if resp.IsError {
		t.Fatalf("unexpected messages_cancel_by_filter error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if canceled := intFromAny(out["canceled"]); canceled != 1 {
		t.Fatalf("expected canceled=1, got %#v", out["canceled"])
	}
	if matched := intFromAny(out["matched"]); matched != 1 {
		t.Fatalf("expected matched=1, got %#v", out["matched"])
	}
}

func TestToolMessagesCancelByFilterMemoryBackendManagedRouteHintForbiddenBeforeAdmin(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	requestCount := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"route":         "/other",
		"state":         "queued",
		"limit":         10,
		"reason":        "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected managed route-hint forbidden error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "route is not allowed when application and endpoint_name are set") {
		t.Fatalf("expected managed route-hint forbidden text, got %#v", resp.Content)
	}
	if requestCount != 0 {
		t.Fatalf("expected no admin request on local validation failure, got %d", requestCount)
	}
}

func TestToolMessagesCancelByFilterRejectsDerivedScopedManagedActorByPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/managed" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "queued",
		"limit":         10,
		"reason":        "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected cancel_by_filter error when derived actor is not allowed by scoped managed actor policy")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "is not allowed for endpoint-scoped managed mutation") {
		t.Fatalf("expected scoped managed actor denied text, got %#v", resp.Content)
	}
}

func TestToolMessagesCancelByFilterDirectPathIgnoresScopedManagedActorPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/direct" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:     "evt_direct_1",
		Route:  "/direct",
		Target: "https://example.org/hook",
		State:  queue.StateQueued,
	}); err != nil {
		t.Fatalf("seed enqueue: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"route":  "/direct",
		"state":  "queued",
		"limit":  10,
		"reason": "operator_cleanup",
	})
	if resp.IsError {
		t.Fatalf("unexpected direct cancel_by_filter error with scoped managed actor policy: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if canceled := intFromAny(out["canceled"]); canceled != 1 {
		t.Fatalf("expected canceled=1, got %#v", out["canceled"])
	}
}

func TestToolMessagesFilterMutationsRequireManagedSelectorForManagedRouteInSQLiteMode(t *testing.T) {
	tests := []struct {
		name  string
		tool  string
		state queue.State
		args  map[string]any
	}{
		{
			name:  "cancel_route",
			tool:  "messages_cancel_by_filter",
			state: queue.StateQueued,
			args: map[string]any{
				"route": "/managed",
				"state": "queued",
				"limit": 10,
			},
		},
		{
			name:  "cancel_global",
			tool:  "messages_cancel_by_filter",
			state: queue.StateQueued,
			args: map[string]any{
				"state": "queued",
				"limit": 10,
			},
		},
		{
			name:  "requeue_route",
			tool:  "messages_requeue_by_filter",
			state: queue.StateDead,
			args: map[string]any{
				"route": "/managed",
				"state": "dead",
				"limit": 10,
			},
		},
		{
			name:  "requeue_global",
			tool:  "messages_requeue_by_filter",
			state: queue.StateDead,
			args: map[string]any{
				"state": "dead",
				"limit": 10,
			},
		},
		{
			name:  "resume_route",
			tool:  "messages_resume_by_filter",
			state: queue.StateCanceled,
			args: map[string]any{
				"route": "/managed",
				"state": "canceled",
				"limit": 10,
			},
		},
		{
			name:  "resume_global",
			tool:  "messages_resume_by_filter",
			state: queue.StateCanceled,
			args: map[string]any{
				"state": "canceled",
				"limit": 10,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			cfgPath := filepath.Join(tmp, "Hookaidofile")
			dbPath := filepath.Join(tmp, "hookaido.db")
			if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/managed" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			store, err := queue.NewSQLiteStore(dbPath)
			if err != nil {
				t.Fatalf("new sqlite store: %v", err)
			}
			if err := store.Enqueue(queue.Envelope{
				ID:     "evt_managed_1",
				Route:  "/managed",
				Target: "https://example.org/hook",
				State:  tc.state,
			}); err != nil {
				_ = store.Close()
				t.Fatalf("enqueue: %v", err)
			}
			_ = store.Close()

			s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("dev@example.test"))
			args := make(map[string]any, len(tc.args)+1)
			for k, v := range tc.args {
				args[k] = v
			}
			args["reason"] = "operator_cleanup"
			resp := callTool(t, s, tc.tool, args)
			if !resp.IsError {
				t.Fatalf("expected %s error when managed filter mutation is not scoped via application+endpoint_name", tc.tool)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "use application+endpoint_name") {
				t.Fatalf("expected managed-selector-required text, got %#v", resp.Content)
			}

			verifyStore, err := queue.NewSQLiteStore(dbPath)
			if err != nil {
				t.Fatalf("reopen sqlite store: %v", err)
			}
			lookup, err := verifyStore.LookupMessages(queue.MessageLookupRequest{IDs: []string{"evt_managed_1"}})
			_ = verifyStore.Close()
			if err != nil {
				t.Fatalf("lookup evt_managed_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("expected state %q to remain unchanged, got %#v", tc.state, lookup.Items)
			}
		})
	}
}

func TestToolMessagesCancelByFilterRejectsScopedManagedActorByPolicyInAdminProxyMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			w.WriteHeader(http.StatusInternalServerError)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("dev@example.test"))
	resp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "queued",
		"limit":         10,
		"reason":        "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected cancel_by_filter error when scoped managed actor is not allowed in proxy mode")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "is not allowed for endpoint-scoped managed mutation") {
		t.Fatalf("expected scoped managed actor denied text, got %#v", resp.Content)
	}
	if adminCalls != 0 {
		t.Fatalf("expected zero admin calls when scoped managed actor policy rejects locally, got %d", adminCalls)
	}
}

func TestToolMessagesFilterMutationsRequireManagedSelectorForManagedRouteInAdminProxyMode(t *testing.T) {
	tests := []struct {
		name string
		tool string
		args map[string]any
	}{
		{
			name: "cancel_route",
			tool: "messages_cancel_by_filter",
			args: map[string]any{
				"route": "/managed",
				"state": "queued",
				"limit": 10,
			},
		},
		{
			name: "cancel_global",
			tool: "messages_cancel_by_filter",
			args: map[string]any{
				"state": "queued",
				"limit": 10,
			},
		},
		{
			name: "requeue_route",
			tool: "messages_requeue_by_filter",
			args: map[string]any{
				"route": "/managed",
				"state": "dead",
				"limit": 10,
			},
		},
		{
			name: "requeue_global",
			tool: "messages_requeue_by_filter",
			args: map[string]any{
				"state": "dead",
				"limit": 10,
			},
		},
		{
			name: "resume_route",
			tool: "messages_resume_by_filter",
			args: map[string]any{
				"route": "/managed",
				"state": "canceled",
				"limit": 10,
			},
		},
		{
			name: "resume_global",
			tool: "messages_resume_by_filter",
			args: map[string]any{
				"state": "canceled",
				"limit": 10,
			},
		},
	}

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			w.WriteHeader(http.StatusInternalServerError)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("dev@example.test"))
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := make(map[string]any, len(tc.args)+1)
			for k, v := range tc.args {
				args[k] = v
			}
			args["reason"] = "operator_cleanup"
			resp := callTool(t, s, tc.tool, args)
			if !resp.IsError {
				t.Fatalf("expected %s error when managed filter mutation is not scoped via application+endpoint_name in proxy mode", tc.tool)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "use application+endpoint_name") {
				t.Fatalf("expected managed-selector-required text, got %#v", resp.Content)
			}
		})
	}
	if adminCalls != 0 {
		t.Fatalf("expected zero admin calls when scoped managed actor policy rejects locally, got %d", adminCalls)
	}
}

func TestToolMessagesFilterMutationsRejectsDerivedScopedManagedActorByPolicyInSQLiteMode(t *testing.T) {
	tests := []struct {
		tool  string
		state queue.State
	}{
		{tool: "messages_requeue_by_filter", state: queue.StateDead},
		{tool: "messages_resume_by_filter", state: queue.StateCanceled},
	}

	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			tmp := t.TempDir()
			cfgPath := filepath.Join(tmp, "Hookaidofile")
			dbPath := filepath.Join(tmp, "hookaido.db")
			if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/managed" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			store, err := queue.NewSQLiteStore(dbPath)
			if err != nil {
				t.Fatalf("new sqlite store: %v", err)
			}
			if err := store.Enqueue(queue.Envelope{
				ID:     "evt_managed_1",
				Route:  "/managed",
				Target: "https://example.org/hook",
				State:  tc.state,
			}); err != nil {
				_ = store.Close()
				t.Fatalf("enqueue: %v", err)
			}
			_ = store.Close()

			s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
			resp := callTool(t, s, tc.tool, map[string]any{
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"state":         string(tc.state),
				"limit":         10,
				"reason":        "operator_cleanup",
			})
			if !resp.IsError {
				t.Fatalf("expected %s error when derived actor is not allowed by scoped managed actor policy", tc.tool)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "is not allowed for endpoint-scoped managed mutation") {
				t.Fatalf("expected scoped managed actor denied text, got %#v", resp.Content)
			}

			verifyStore, err := queue.NewSQLiteStore(dbPath)
			if err != nil {
				t.Fatalf("reopen sqlite store: %v", err)
			}
			lookup, err := verifyStore.LookupMessages(queue.MessageLookupRequest{IDs: []string{"evt_managed_1"}})
			_ = verifyStore.Close()
			if err != nil {
				t.Fatalf("lookup evt_managed_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("expected state %q to remain unchanged, got %#v", tc.state, lookup.Items)
			}
		})
	}
}

func TestToolMessagesFilterMutationsDirectPathIgnoreScopedManagedActorPolicyInSQLiteMode(t *testing.T) {
	tests := []struct {
		tool     string
		state    queue.State
		countKey string
	}{
		{tool: "messages_requeue_by_filter", state: queue.StateDead, countKey: "requeued"},
		{tool: "messages_resume_by_filter", state: queue.StateCanceled, countKey: "resumed"},
	}

	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			tmp := t.TempDir()
			cfgPath := filepath.Join(tmp, "Hookaidofile")
			dbPath := filepath.Join(tmp, "hookaido.db")
			if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/direct" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			store, err := queue.NewSQLiteStore(dbPath)
			if err != nil {
				t.Fatalf("new sqlite store: %v", err)
			}
			if err := store.Enqueue(queue.Envelope{
				ID:     "evt_direct_1",
				Route:  "/direct",
				Target: "https://example.org/hook",
				State:  tc.state,
			}); err != nil {
				_ = store.Close()
				t.Fatalf("seed enqueue: %v", err)
			}
			_ = store.Close()

			s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
			resp := callTool(t, s, tc.tool, map[string]any{
				"route":  "/direct",
				"state":  string(tc.state),
				"limit":  10,
				"reason": "operator_cleanup",
			})
			if resp.IsError {
				t.Fatalf("unexpected direct %s error with scoped managed actor policy: %s", tc.tool, resp.Content[0].Text)
			}
			out, ok := resp.StructuredContent.(map[string]any)
			if !ok {
				t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
			}
			if changed := intFromAny(out[tc.countKey]); changed != 1 {
				t.Fatalf("expected %s=1, got %#v", tc.countKey, out[tc.countKey])
			}
		})
	}
}

func TestToolMessagesFilterMutationsRejectsScopedManagedActorByPolicyInAdminProxyMode(t *testing.T) {
	tests := []struct {
		tool  string
		state string
	}{
		{tool: "messages_requeue_by_filter", state: "dead"},
		{tool: "messages_resume_by_filter", state: "canceled"},
	}

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			w.WriteHeader(http.StatusInternalServerError)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("dev@example.test"))
	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			resp := callTool(t, s, tc.tool, map[string]any{
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"state":         tc.state,
				"limit":         10,
				"reason":        "operator_cleanup",
			})
			if !resp.IsError {
				t.Fatalf("expected %s error when scoped managed actor is not allowed in proxy mode", tc.tool)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "is not allowed for endpoint-scoped managed mutation") {
				t.Fatalf("expected scoped managed actor denied text, got %#v", resp.Content)
			}
		})
	}
	if adminCalls != 0 {
		t.Fatalf("expected zero admin calls when scoped managed actor policy rejects locally, got %d", adminCalls)
	}
}

func TestToolMessagesCancelByFilterMemoryBackendViaAdminStructuredError(t *testing.T) {
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
			if r.Method != http.MethodPost || r.URL.Path != "/admin/messages/cancel_by_filter" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":   "selector_scope_mismatch",
				"detail": "route/managed selector is invalid or mismatched",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"route":  "/r",
		"state":  "queued",
		"limit":  10,
		"reason": "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected messages_cancel_by_filter error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=selector_scope_mismatch") || !strings.Contains(msg, "route/managed selector is invalid or mismatched") {
		t.Fatalf("expected structured code/detail in mapped error, got %q", msg)
	}
}

func TestToolMessagesCancelByFilterMemoryBackendManagedViaAdminNotFoundFallbackDetail(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedPath := "/admin/applications/billing/endpoints/invoice.created/messages/cancel_by_filter"
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != scopedPath {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "managed_endpoint_not_found",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "queued",
		"limit":         10,
		"reason":        "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected messages_cancel_by_filter error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=managed_endpoint_not_found") {
		t.Fatalf("expected managed_endpoint_not_found code, got %q", msg)
	}
	if !strings.Contains(msg, "detail=managed endpoint not found") {
		t.Fatalf("expected managed_endpoint_not_found fallback detail, got %q", msg)
	}
}

func TestToolMessagesRequeueByFilterMemoryBackendManagedViaAdminStructuredError(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedPath := "/admin/applications/billing/endpoints/invoice.created/messages/requeue_by_filter"
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != scopedPath {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":   "selector_scope_forbidden",
				"detail": "selector hints are not allowed on endpoint-scoped filter path",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_requeue_by_filter", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "dead",
		"limit":         10,
		"reason":        "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected messages_requeue_by_filter error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=selector_scope_forbidden") || !strings.Contains(msg, "selector hints are not allowed on endpoint-scoped filter path") {
		t.Fatalf("expected structured code/detail in mapped scoped error, got %q", msg)
	}
}

func TestToolMessagesResumeByFilterMemoryBackendManagedViaAdminScopedPath(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedPath := "/admin/applications/billing/endpoints/invoice.created/messages/resume_by_filter"
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != scopedPath {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_cleanup" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if payload["route"] != nil || payload["application"] != nil || payload["endpoint_name"] != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if payload["state"] != "canceled" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if intFromAny(payload["limit"]) != 10 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"matched":      2,
				"resumed":      2,
				"preview_only": false,
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_resume_by_filter", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "canceled",
		"limit":         10,
		"reason":        "operator_cleanup",
	})
	if resp.IsError {
		t.Fatalf("unexpected messages_resume_by_filter error: %s", resp.Content[0].Text)
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected response content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "resumed") || !strings.Contains(msg, "2") {
		t.Fatalf("expected resumed count in response, got %q", msg)
	}
}

func TestToolMessagesResumeByFilterMemoryBackendViaAdminStructuredError(t *testing.T) {
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
			if r.Method != http.MethodPost || r.URL.Path != "/admin/messages/resume_by_filter" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":   "selector_scope_mismatch",
				"detail": "route/managed selector is invalid or mismatched",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_resume_by_filter", map[string]any{
		"route":  "/r",
		"state":  "canceled",
		"limit":  10,
		"reason": "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected messages_resume_by_filter error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=selector_scope_mismatch") || !strings.Contains(msg, "route/managed selector is invalid or mismatched") {
		t.Fatalf("expected structured code/detail in mapped error, got %q", msg)
	}
}

func TestToolMessagesResumeByFilterMemoryBackendManagedViaAdminNotFoundFallbackDetail(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedPath := "/admin/applications/billing/endpoints/invoice.created/messages/resume_by_filter"
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != scopedPath {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "managed_endpoint_not_found",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_resume_by_filter", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "canceled",
		"limit":         10,
		"reason":        "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected messages_resume_by_filter error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=managed_endpoint_not_found") {
		t.Fatalf("expected managed_endpoint_not_found code, got %q", msg)
	}
	if !strings.Contains(msg, "detail=managed endpoint not found") {
		t.Fatalf("expected managed_endpoint_not_found fallback detail, got %q", msg)
	}
}

func TestToolMessagesFilterMutationsMemoryBackendManagedResolverMissingFallbackDetail(t *testing.T) {
	tests := []struct {
		tool string
		path string
		args map[string]any
	}{
		{
			tool: "messages_cancel_by_filter",
			path: "/admin/messages/cancel_by_filter",
			args: map[string]any{
				"route":  "/r",
				"state":  "queued",
				"limit":  10,
				"reason": "operator_cleanup",
			},
		},
		{
			tool: "messages_requeue_by_filter",
			path: "/admin/messages/requeue_by_filter",
			args: map[string]any{
				"route":  "/r",
				"state":  "dead",
				"limit":  10,
				"reason": "operator_cleanup",
			},
		},
		{
			tool: "messages_resume_by_filter",
			path: "/admin/messages/resume_by_filter",
			args: map[string]any{
				"route":  "/r",
				"state":  "canceled",
				"limit":  10,
				"reason": "operator_cleanup",
			},
		},
	}

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	callCount := make(map[string]int)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_cleanup" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch r.URL.Path {
			case "/admin/messages/cancel_by_filter", "/admin/messages/requeue_by_filter", "/admin/messages/resume_by_filter":
				callCount[r.URL.Path]++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "managed_resolver_missing",
				})
			default:
				w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			resp := callTool(t, s, tc.tool, tc.args)
			if !resp.IsError {
				t.Fatalf("expected %s error", tc.tool)
			}
			if len(resp.Content) == 0 {
				t.Fatalf("expected error content")
			}
			msg := resp.Content[0].Text
			if !strings.Contains(msg, "code=managed_resolver_missing") {
				t.Fatalf("expected managed_resolver_missing code, got %q", msg)
			}
			if !strings.Contains(msg, "detail=managed endpoint resolution is not configured") {
				t.Fatalf("expected managed_resolver_missing fallback detail, got %q", msg)
			}
		})
	}
	for _, tc := range tests {
		if callCount[tc.path] != 1 {
			t.Fatalf("expected exactly one admin call for %s, got %d", tc.path, callCount[tc.path])
		}
	}
}

func TestToolMessagesFilterMutationsMemoryBackendManagedTargetMismatchFallbackDetail(t *testing.T) {
	tests := []struct {
		tool string
		path string
		args map[string]any
	}{
		{
			tool: "messages_cancel_by_filter",
			path: "/admin/messages/cancel_by_filter",
			args: map[string]any{
				"route":  "/r",
				"state":  "queued",
				"limit":  10,
				"reason": "operator_cleanup",
			},
		},
		{
			tool: "messages_requeue_by_filter",
			path: "/admin/messages/requeue_by_filter",
			args: map[string]any{
				"route":  "/r",
				"state":  "dead",
				"limit":  10,
				"reason": "operator_cleanup",
			},
		},
		{
			tool: "messages_resume_by_filter",
			path: "/admin/messages/resume_by_filter",
			args: map[string]any{
				"route":  "/r",
				"state":  "canceled",
				"limit":  10,
				"reason": "operator_cleanup",
			},
		},
	}

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	callCount := make(map[string]int)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_cleanup" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch r.URL.Path {
			case "/admin/messages/cancel_by_filter", "/admin/messages/requeue_by_filter", "/admin/messages/resume_by_filter":
				callCount[r.URL.Path]++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "managed_target_mismatch",
				})
			default:
				w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			resp := callTool(t, s, tc.tool, tc.args)
			if !resp.IsError {
				t.Fatalf("expected %s error", tc.tool)
			}
			if len(resp.Content) == 0 {
				t.Fatalf("expected error content")
			}
			msg := resp.Content[0].Text
			if !strings.Contains(msg, "code=managed_target_mismatch") {
				t.Fatalf("expected managed_target_mismatch code, got %q", msg)
			}
			if !strings.Contains(msg, "detail=managed endpoint target/ownership mapping is out of sync with route policy resolution") {
				t.Fatalf("expected managed_target_mismatch fallback detail, got %q", msg)
			}
		})
	}
	for _, tc := range tests {
		if callCount[tc.path] != 1 {
			t.Fatalf("expected exactly one admin call for %s, got %d", tc.path, callCount[tc.path])
		}
	}
}

func TestToolMessagesRequeueByFilterMemoryBackendManagedScopedResolverMissingFallbackDetail(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedPath := "/admin/applications/billing/endpoints/invoice.created/messages/requeue_by_filter"
	calls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != scopedPath {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_cleanup" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			calls++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "managed_resolver_missing",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_requeue_by_filter", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "dead",
		"limit":         10,
		"reason":        "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected messages_requeue_by_filter error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=managed_resolver_missing") {
		t.Fatalf("expected managed_resolver_missing code, got %q", msg)
	}
	if !strings.Contains(msg, "detail=managed endpoint resolution is not configured") {
		t.Fatalf("expected managed_resolver_missing fallback detail, got %q", msg)
	}
	if calls != 1 {
		t.Fatalf("expected exactly one scoped admin call, got %d", calls)
	}
}

func TestToolMessagesPublishMemoryBackendViaAdmin(t *testing.T) {
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
			if r.Method != http.MethodPost || r.URL.Path != "/admin/messages/publish" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			items, ok := payload["items"].([]any)
			if !ok || len(items) != 1 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			item, ok := items[0].(map[string]any)
			if !ok || item["route"] != "/r" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"published": 1,
				"audit": map[string]any{
					"reason": "operator_publish",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":          "evt_mem_pub_1",
				"route":       "/r",
				"target":      "https://example.org/hook",
				"payload_b64": "aGVsbG8=",
			},
		},
	})
	if resp.IsError {
		t.Fatalf("unexpected messages_publish error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if published := intFromAny(out["published"]); published != 1 {
		t.Fatalf("expected published=1, got %#v", out["published"])
	}
	audit, ok := out["audit"].(map[string]any)
	if !ok {
		t.Fatalf("expected audit object, got %T", out["audit"])
	}
	if principal, _ := audit["principal"].(string); principal != "test-admin" {
		t.Fatalf("expected audit.principal=test-admin, got %#v", audit["principal"])
	}
}

func TestToolMessagesPublishMemoryBackendViaAdminRouteNotFoundPreflightNoAdminCall(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	calls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/admin/messages/publish" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			calls++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "route_not_found",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/known" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_mem_missing_route",
				"route":  "/missing",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected messages_publish error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, `items[0]: route "/missing" not found in compiled config`) {
		t.Fatalf("expected local route-not-found preflight detail, got %q", msg)
	}
	if calls != 0 {
		t.Fatalf("expected zero admin calls on local preflight failure, got %d", calls)
	}
}

func TestToolMessagesPublishMemoryBackendManagedViaAdminScopedPath(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedPath := "/admin/applications/billing/endpoints/invoice.created/messages/publish"
	globalCalled := 0
	scopedCalled := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch r.URL.Path {
			case "/admin/messages/publish":
				globalCalled++
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":   "scoped_publish_required",
					"detail": "managed publish must be endpoint-scoped",
				})
				return
			case scopedPath:
				scopedCalled++
				var payload map[string]any
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				items, ok := payload["items"].([]any)
				if !ok || len(items) != 1 {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				item, ok := items[0].(map[string]any)
				if !ok || item["id"] != "evt_managed_1" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"published": 1,
					"scope": map[string]any{
						"application":   "billing",
						"endpoint_name": "invoice.created",
						"route":         "/managed",
					},
					"audit": map[string]any{
						"reason": "operator_publish",
					},
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
pull_api { auth token "raw:t" }
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e" }
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_managed_1",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
		},
	})
	if resp.IsError {
		t.Fatalf("unexpected messages_publish error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if published := intFromAny(out["published"]); published != 1 {
		t.Fatalf("expected published=1, got %#v", out["published"])
	}
	if globalCalled != 0 {
		t.Fatalf("expected managed publish to avoid global path, got globalCalled=%d", globalCalled)
	}
	if scopedCalled != 1 {
		t.Fatalf("expected exactly one scoped publish call, got %d", scopedCalled)
	}
}

func TestToolMessagesPublishRejectsMixedSelectorModesInAdminProxyMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/direct" {
  queue { backend "memory" }
  deliver "https://example.org/direct" {}
}
pull_api { auth token "raw:t" }
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e" }
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_direct_1",
				"route":  "/direct",
				"target": "https://example.org/direct",
			},
			map[string]any{
				"id":            "evt_managed_2",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected messages_publish mixed selector-mode error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "items must use a single selector mode per request") {
		t.Fatalf("expected mixed selector-mode error text, got %#v", resp.Content)
	}
	if adminCalls != 0 {
		t.Fatalf("expected zero admin calls on mixed selector-mode validation failure, got %d", adminCalls)
	}
}

func TestToolMessagesPublishMemoryBackendManagedSelectorRequiredPreflightNoAdminCall(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	calls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/admin/messages/publish" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			calls++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "managed_selector_required",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_mem_managed_route_only",
				"route":  "/managed",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected messages_publish error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, `items[0]: route "/managed" is managed by ("billing", "invoice.created"); provide application and endpoint_name`) {
		t.Fatalf("expected local managed-selector preflight detail, got %q", msg)
	}
	if calls != 0 {
		t.Fatalf("expected zero admin calls on local preflight failure, got %d", calls)
	}
}

func TestToolMessagesPublishMemoryBackendManagedRouteHintForbiddenBeforeAdmin(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			w.WriteHeader(http.StatusInternalServerError)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
pull_api { auth token "raw:t" }
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e" }
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_managed_mismatch",
				"route":         "/other",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected route-hint forbidden error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "route is not allowed when application and endpoint_name are set") {
		t.Fatalf("expected route-hint forbidden error text, got %#v", resp.Content)
	}
	if adminCalls != 0 {
		t.Fatalf("expected no admin calls on local validation failure, got %d", adminCalls)
	}
}

func TestToolMessagesPublishMemoryBackendManagedAdminBatchesItemIndexOffset(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedBillingCalled := 0
	scopedERPCalled := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch r.URL.Path {
			case "/admin/applications/billing/endpoints/invoice.created/messages/publish":
				scopedBillingCalled++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"published": 1,
				})
				return
			case "/admin/applications/erp/endpoints/order.created/messages/publish":
				scopedERPCalled++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":       "duplicate_id",
					"detail":     "item.id \"evt_managed_dup\" already exists",
					"item_index": 0,
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
			t.Fatalf("timed out waiting for admin server shutdown")
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
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_managed_ok",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
			map[string]any{
				"id":            "evt_managed_dup",
				"application":   "erp",
				"endpoint_name": "order.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for managed batch conflict")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=duplicate_id") || !strings.Contains(msg, "item_index=1") {
		t.Fatalf("expected shifted item_index in mapped error, got %q", msg)
	}
	if scopedBillingCalled != 1 || scopedERPCalled != 1 {
		t.Fatalf("expected one call per managed batch, got billing=%d erp=%d", scopedBillingCalled, scopedERPCalled)
	}
}

func TestToolMessagesPublishMemoryBackendManagedAdminBatchesFailureRollsBackEarlierIDs(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedBillingCalled := 0
	scopedERPCalled := 0
	cancelCalled := 0
	var cancelIDs []string
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Actor") != "test-admin" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch r.URL.Path {
			case "/admin/applications/billing/endpoints/invoice.created/messages/publish":
				scopedBillingCalled++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"published": 1,
				})
				return
			case "/admin/applications/erp/endpoints/order.created/messages/publish":
				scopedERPCalled++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":       "duplicate_id",
					"detail":     "item.id \"evt_managed_dup\" already exists",
					"item_index": 0,
				})
				return
			case "/admin/messages/cancel":
				cancelCalled++
				var req struct {
					IDs []string `json:"ids"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				cancelIDs = append([]string(nil), req.IDs...)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"canceled": len(req.IDs),
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
			t.Fatalf("timed out waiting for admin server shutdown")
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
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_managed_ok",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
			map[string]any{
				"id":            "evt_managed_dup",
				"application":   "erp",
				"endpoint_name": "order.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for managed batch conflict")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=duplicate_id") || !strings.Contains(msg, "item_index=1") {
		t.Fatalf("expected shifted item_index in mapped error, got %q", msg)
	}
	if scopedBillingCalled != 1 || scopedERPCalled != 1 {
		t.Fatalf("expected one call per managed batch, got billing=%d erp=%d", scopedBillingCalled, scopedERPCalled)
	}
	if cancelCalled != 1 {
		t.Fatalf("expected one rollback cancel call, got %d", cancelCalled)
	}
	if len(cancelIDs) != 1 || cancelIDs[0] != "evt_managed_ok" {
		t.Fatalf("expected rollback cancel IDs [evt_managed_ok], got %#v", cancelIDs)
	}
}

func TestToolMessagesPublishMemoryBackendManagedAdminBatchesFailureRollbackErrorPreservesPublishError(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedBillingCalled := 0
	scopedERPCalled := 0
	cancelCalled := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Actor") != "test-admin" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch r.URL.Path {
			case "/admin/applications/billing/endpoints/invoice.created/messages/publish":
				scopedBillingCalled++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"published": 1,
				})
				return
			case "/admin/applications/erp/endpoints/order.created/messages/publish":
				scopedERPCalled++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":       "duplicate_id",
					"detail":     "item.id \"evt_managed_dup\" already exists",
					"item_index": 0,
				})
				return
			case "/admin/messages/cancel":
				cancelCalled++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":   "store_unavailable",
					"detail": "queue store is unavailable",
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
			t.Fatalf("timed out waiting for admin server shutdown")
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
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_managed_ok",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
			map[string]any{
				"id":            "evt_managed_dup",
				"application":   "erp",
				"endpoint_name": "order.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for managed batch conflict")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=duplicate_id") || !strings.Contains(msg, "item_index=1") {
		t.Fatalf("expected shifted item_index in mapped error, got %q", msg)
	}
	if strings.Contains(msg, "store_unavailable") {
		t.Fatalf("expected original publish error text without rollback override, got %q", msg)
	}
	if scopedBillingCalled != 1 || scopedERPCalled != 1 {
		t.Fatalf("expected one call per managed batch, got billing=%d erp=%d", scopedBillingCalled, scopedERPCalled)
	}
	if cancelCalled != 1 {
		t.Fatalf("expected one rollback cancel attempt, got %d", cancelCalled)
	}
}

func TestToolMessagesPublishMemoryBackendManagedAdminBatchesFailureRollsBackFailedBatchPrefix(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedBillingCalled := 0
	scopedERPCalled := 0
	cancelCalled := 0
	var cancelIDs []string
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Actor") != "test-admin" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch r.URL.Path {
			case "/admin/applications/billing/endpoints/invoice.created/messages/publish":
				scopedBillingCalled++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"published": 1,
				})
				return
			case "/admin/applications/erp/endpoints/order.created/messages/publish":
				scopedERPCalled++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":       "duplicate_id",
					"detail":     "item.id \"evt_erp_second\" already exists",
					"item_index": 1,
				})
				return
			case "/admin/messages/cancel":
				cancelCalled++
				var req struct {
					IDs []string `json:"ids"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				cancelIDs = append([]string(nil), req.IDs...)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"canceled": len(req.IDs),
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
			t.Fatalf("timed out waiting for admin server shutdown")
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
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_billing_ok",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
			map[string]any{
				"id":            "evt_erp_first",
				"application":   "erp",
				"endpoint_name": "order.created",
			},
			map[string]any{
				"id":            "evt_erp_second",
				"application":   "erp",
				"endpoint_name": "order.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for managed batch conflict")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=duplicate_id") || !strings.Contains(msg, "item_index=2") {
		t.Fatalf("expected shifted item_index in mapped error, got %q", msg)
	}
	if scopedBillingCalled != 1 || scopedERPCalled != 1 {
		t.Fatalf("expected one call per managed batch, got billing=%d erp=%d", scopedBillingCalled, scopedERPCalled)
	}
	if cancelCalled != 1 {
		t.Fatalf("expected one rollback cancel call, got %d", cancelCalled)
	}
	want := []string{"evt_billing_ok", "evt_erp_first"}
	if len(cancelIDs) != len(want) {
		t.Fatalf("expected rollback cancel IDs %#v, got %#v", want, cancelIDs)
	}
	for i := range want {
		if cancelIDs[i] != want[i] {
			t.Fatalf("expected rollback cancel IDs %#v, got %#v", want, cancelIDs)
		}
	}
}

func TestToolMessagesPublishMemoryBackendManagedAdminSingleBatchFailureRollsBackPrefix(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedBillingCalled := 0
	cancelCalled := 0
	var cancelIDs []string
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Actor") != "test-admin" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch r.URL.Path {
			case "/admin/applications/billing/endpoints/invoice.created/messages/publish":
				scopedBillingCalled++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":       "duplicate_id",
					"detail":     "item.id \"evt_billing_second\" already exists",
					"item_index": 1,
				})
				return
			case "/admin/messages/cancel":
				cancelCalled++
				var req struct {
					IDs []string `json:"ids"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				cancelIDs = append([]string(nil), req.IDs...)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"canceled": len(req.IDs),
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
			t.Fatalf("timed out waiting for admin server shutdown")
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
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_billing_first",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
			map[string]any{
				"id":            "evt_billing_second",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for managed batch conflict")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=duplicate_id") || !strings.Contains(msg, "item_index=1") {
		t.Fatalf("expected original item_index in mapped error, got %q", msg)
	}
	if scopedBillingCalled != 1 {
		t.Fatalf("expected one scoped publish call, got %d", scopedBillingCalled)
	}
	if cancelCalled != 1 {
		t.Fatalf("expected one rollback cancel call, got %d", cancelCalled)
	}
	if len(cancelIDs) != 1 || cancelIDs[0] != "evt_billing_first" {
		t.Fatalf("expected rollback cancel IDs [evt_billing_first], got %#v", cancelIDs)
	}
}

func TestToolBacklogTopQueuedMemoryBackendViaAdmin(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/backlog/top_queued" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.URL.Query().Get("limit") != "5" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"queue_total":  3,
				"queued_total": 2,
				"limit":        5,
				"items": []map[string]any{
					{"route": "/r", "target": "pull", "queued": 2},
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "backlog_top_queued", map[string]any{
		"limit": 5,
	})
	if resp.IsError {
		t.Fatalf("unexpected backlog_top_queued error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if queueTotal := intFromAny(out["queue_total"]); queueTotal != 3 {
		t.Fatalf("expected queue_total=3, got %#v", out["queue_total"])
	}
}

func TestToolBacklogTopQueuedMemoryBackendViaAdminStructuredError(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/backlog/top_queued" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":   "invalid_query",
				"detail": "limit must be a positive integer",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "backlog_top_queued", map[string]any{
		"limit": 5,
	})
	if !resp.IsError {
		t.Fatalf("expected backlog_top_queued error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=invalid_query") || !strings.Contains(msg, "limit must be a positive integer") {
		t.Fatalf("expected structured code/detail in mapped error, got %q", msg)
	}
}

func TestToolBacklogReadToolsMemoryBackendFallbackDetail(t *testing.T) {
	tests := []struct {
		tool string
		path string
		args map[string]any
	}{
		{
			tool: "backlog_top_queued",
			path: "/admin/backlog/top_queued",
			args: map[string]any{"limit": 5},
		},
		{
			tool: "backlog_oldest_queued",
			path: "/admin/backlog/oldest_queued",
			args: map[string]any{"limit": 5},
		},
		{
			tool: "backlog_aging_summary",
			path: "/admin/backlog/aging_summary",
			args: map[string]any{"limit": 5},
		},
		{
			tool: "backlog_trends",
			path: "/admin/backlog/trends",
			args: map[string]any{"window": "1h", "step": "5m"},
		},
	}

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	callCount := make(map[string]int)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			switch r.URL.Path {
			case "/admin/backlog/top_queued", "/admin/backlog/oldest_queued", "/admin/backlog/aging_summary", "/admin/backlog/trends":
				callCount[r.URL.Path]++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "invalid_query",
				})
			default:
				w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			resp := callTool(t, s, tc.tool, tc.args)
			if !resp.IsError {
				t.Fatalf("expected %s error", tc.tool)
			}
			if len(resp.Content) == 0 {
				t.Fatalf("expected error content")
			}
			msg := resp.Content[0].Text
			if !strings.Contains(msg, "code=invalid_query") {
				t.Fatalf("expected invalid_query code, got %q", msg)
			}
			if !strings.Contains(msg, "detail=request query is invalid") {
				t.Fatalf("expected invalid_query fallback detail, got %q", msg)
			}
		})
	}
	for _, tc := range tests {
		if callCount[tc.path] != 1 {
			t.Fatalf("expected exactly one admin call for %s, got %d", tc.path, callCount[tc.path])
		}
	}
}

func TestToolBacklogReadToolsRejectNonAbsoluteRouteBeforeAdmin(t *testing.T) {
	tests := []struct {
		tool string
		args map[string]any
	}{
		{
			tool: "backlog_top_queued",
			args: map[string]any{"route": "relative", "limit": 5},
		},
		{
			tool: "backlog_oldest_queued",
			args: map[string]any{"route": "relative", "limit": 5},
		},
		{
			tool: "backlog_aging_summary",
			args: map[string]any{"route": "relative", "limit": 5},
		},
		{
			tool: "backlog_trends",
			args: map[string]any{"route": "relative", "window": "1h", "step": "5m"},
		},
	}

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			resp := callTool(t, s, tc.tool, tc.args)
			if !resp.IsError {
				t.Fatalf("expected %s error", tc.tool)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "route must start with '/'") {
				t.Fatalf("expected route format error text, got %#v", resp.Content)
			}
		})
	}
	if adminCalls != 0 {
		t.Fatalf("expected zero admin calls on local route validation failures, got %d", adminCalls)
	}
}

func TestToolBacklogTopQueuedMemoryBackendBlockedByAllowlist(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/backlog/top_queued" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"queue_total":  1,
				"queued_total": 1,
				"limit":        10,
				"items": []map[string]any{
					{"route": "/r", "target": "pull", "queued": 1},
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithAdminProxyEndpointAllowlist([]string{"127.0.0.1:1"}))
	resp := callTool(t, s, "backlog_top_queued", map[string]any{
		"limit": 10,
	})
	if !resp.IsError {
		t.Fatalf("expected backlog_top_queued allowlist error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "not allowed by admin endpoint allowlist") {
		t.Fatalf("expected allowlist error text, got %#v", resp.Content)
	}
}

func TestToolBacklogTopQueuedMemoryBackendAllowedByURLPrefixAllowlist(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/backlog/top_queued" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"queue_total":  2,
				"queued_total": 2,
				"limit":        10,
				"items": []map[string]any{
					{"route": "/r", "target": "pull", "queued": 2},
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	allowEntry := fmt.Sprintf("http://%s/admin", addr)
	s := NewServer(nil, nil, cfgPath, "", WithAdminProxyEndpointAllowlist([]string{allowEntry}))
	resp := callTool(t, s, "backlog_top_queued", map[string]any{
		"limit": 10,
	})
	if resp.IsError {
		t.Fatalf("unexpected backlog_top_queued error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if queueTotal := intFromAny(out["queue_total"]); queueTotal != 2 {
		t.Fatalf("expected queue_total=2, got %#v", out["queue_total"])
	}
}

func TestToolBacklogTopQueuedMemoryBackendRetriesTransientStatus(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	attempts := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/admin/backlog/top_queued" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			attempts++
			if attempts == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("warming up"))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"queue_total":  2,
				"queued_total": 2,
				"limit":        5,
				"items": []map[string]any{
					{"route": "/r", "target": "pull", "queued": 2},
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "backlog_top_queued", map[string]any{
		"limit": 5,
	})
	if resp.IsError {
		t.Fatalf("unexpected backlog_top_queued error: %s", resp.Content[0].Text)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 admin attempts, got %d", attempts)
	}
}

func TestToolDLQListMemoryBackendViaAdminFallbackDetail(t *testing.T) {
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
			if r.Method != http.MethodGet || r.URL.Path != "/admin/dlq" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.URL.Query().Get("limit") != "10" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "invalid_query",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "")
	resp := callTool(t, s, "dlq_list", map[string]any{
		"limit": 10,
	})
	if !resp.IsError {
		t.Fatalf("expected dlq_list error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=invalid_query") {
		t.Fatalf("expected invalid_query code, got %q", msg)
	}
	if !strings.Contains(msg, "detail=request query is invalid") {
		t.Fatalf("expected invalid_query fallback detail, got %q", msg)
	}
}

func TestToolDLQRequeueMemoryBackendViaAdmin(t *testing.T) {
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
			if r.Method != http.MethodPost || r.URL.Path != "/admin/dlq/requeue" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			ids, ok := payload["ids"].([]any)
			if !ok || len(ids) != 1 || ids[0] != "evt_mem_dead_1" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"requeued": 1})
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "dlq_requeue", map[string]any{
		"ids":    []any{"evt_mem_dead_1"},
		"reason": "operator_cleanup",
	})
	if resp.IsError {
		t.Fatalf("unexpected dlq_requeue error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if requeued := intFromAny(out["requeued"]); requeued != 1 {
		t.Fatalf("expected requeued=1, got %#v", out["requeued"])
	}
}

func TestToolDLQDeleteMemoryBackendViaAdminStructuredError(t *testing.T) {
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
			if r.Method != http.MethodPost || r.URL.Path != "/admin/dlq/delete" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":   "invalid_body",
				"detail": "ids must contain between 1 and 1000 non-empty entries",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "dlq_delete", map[string]any{
		"ids":    []any{"evt_mem_dead_1"},
		"reason": "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected dlq_delete error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=invalid_body") || !strings.Contains(msg, "ids must contain between 1 and 1000 non-empty entries") {
		t.Fatalf("expected structured code/detail in mapped error, got %q", msg)
	}
}

func TestToolMessagesCancelMemoryBackendViaAdminStructuredError(t *testing.T) {
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
			if r.Method != http.MethodPost || r.URL.Path != "/admin/messages/cancel" {
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
				"code":   "store_unavailable",
				"detail": "queue store is unavailable",
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel", map[string]any{
		"ids":    []any{"evt_mem_1"},
		"reason": "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected messages_cancel error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "code=store_unavailable") || !strings.Contains(msg, "queue store is unavailable") {
		t.Fatalf("expected structured code/detail in mapped error, got %q", msg)
	}
}

func TestToolMessagesIDMutationsMemoryBackendViaAdminStructuredError(t *testing.T) {
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
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			switch r.URL.Path {
			case "/admin/messages/requeue":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":   "invalid_body",
					"detail": "ids must contain between 1 and 1000 non-empty entries",
				})
			case "/admin/messages/resume":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":   "store_unavailable",
					"detail": "queue store is unavailable",
				})
			default:
				w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	cases := []struct {
		tool       string
		expectCode string
		expectPart string
	}{
		{
			tool:       "messages_requeue",
			expectCode: "code=invalid_body",
			expectPart: "ids must contain between 1 and 1000 non-empty entries",
		},
		{
			tool:       "messages_resume",
			expectCode: "code=store_unavailable",
			expectPart: "queue store is unavailable",
		},
	}
	for _, tc := range cases {
		t.Run(tc.tool, func(t *testing.T) {
			resp := callTool(t, s, tc.tool, map[string]any{
				"ids":    []any{"evt_mem_1"},
				"reason": "operator_cleanup",
			})
			if !resp.IsError {
				t.Fatalf("expected %s error", tc.tool)
			}
			if len(resp.Content) == 0 {
				t.Fatalf("expected error content")
			}
			msg := resp.Content[0].Text
			if !strings.Contains(msg, tc.expectCode) || !strings.Contains(msg, tc.expectPart) {
				t.Fatalf("expected structured code/detail in mapped error, got %q", msg)
			}
		})
	}
}

func TestToolIDMutationsMemoryBackendViaAdminInvalidBodyFallbackDetail(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	callCount := make(map[string]int)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			switch r.URL.Path {
			case "/admin/dlq/requeue", "/admin/dlq/delete", "/admin/messages/cancel", "/admin/messages/requeue", "/admin/messages/resume":
				callCount[r.URL.Path]++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "invalid_body",
				})
			default:
				w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	tests := []struct {
		tool string
		path string
	}{
		{tool: "dlq_requeue", path: "/admin/dlq/requeue"},
		{tool: "dlq_delete", path: "/admin/dlq/delete"},
		{tool: "messages_cancel", path: "/admin/messages/cancel"},
		{tool: "messages_requeue", path: "/admin/messages/requeue"},
		{tool: "messages_resume", path: "/admin/messages/resume"},
	}
	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			resp := callTool(t, s, tc.tool, map[string]any{
				"ids":    []any{"evt_mem_1"},
				"reason": "operator_cleanup",
			})
			if !resp.IsError {
				t.Fatalf("expected %s error", tc.tool)
			}
			if len(resp.Content) == 0 {
				t.Fatalf("expected error content")
			}
			msg := resp.Content[0].Text
			if !strings.Contains(msg, "code=invalid_body") {
				t.Fatalf("expected invalid_body code, got %q", msg)
			}
			if !strings.Contains(msg, "detail=request body is invalid") {
				t.Fatalf("expected invalid_body fallback detail, got %q", msg)
			}
		})
	}
	for _, tc := range tests {
		if callCount[tc.path] != 1 {
			t.Fatalf("expected exactly one admin call for %s, got %d", tc.path, callCount[tc.path])
		}
	}
}

func TestToolIDMutationsMemoryBackendViaAdminScopedManagedAuditFallback(t *testing.T) {
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
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			switch r.URL.Path {
			case "/admin/dlq/requeue", "/admin/dlq/delete", "/admin/messages/cancel", "/admin/messages/requeue", "/admin/messages/resume":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "audit_actor_not_allowed",
				})
			default:
				w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	tests := []string{"dlq_requeue", "dlq_delete", "messages_cancel", "messages_requeue", "messages_resume"}
	for _, toolName := range tests {
		t.Run(toolName, func(t *testing.T) {
			resp := callTool(t, s, toolName, map[string]any{
				"ids":    []any{"evt_mem_1"},
				"reason": "operator_cleanup",
			})
			if !resp.IsError {
				t.Fatalf("expected %s error", toolName)
			}
			if len(resp.Content) == 0 {
				t.Fatalf("expected error content")
			}
			msg := resp.Content[0].Text
			if !strings.Contains(msg, "code=audit_actor_not_allowed") {
				t.Fatalf("expected audit_actor_not_allowed code, got %q", msg)
			}
			if !strings.Contains(msg, "X-Hookaido-Audit-Actor is not allowed for endpoint-scoped managed mutation") {
				t.Fatalf("expected scoped managed audit fallback detail, got %q", msg)
			}
		})
	}
}

func TestToolIDMutationsMemoryBackendViaAdminManagedTargetMismatchFallback(t *testing.T) {
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
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			switch r.URL.Path {
			case "/admin/dlq/requeue", "/admin/dlq/delete", "/admin/messages/cancel", "/admin/messages/requeue", "/admin/messages/resume":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "managed_target_mismatch",
				})
			default:
				w.WriteHeader(http.StatusNotFound)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	tests := []string{"dlq_requeue", "dlq_delete", "messages_cancel", "messages_requeue", "messages_resume"}
	for _, toolName := range tests {
		t.Run(toolName, func(t *testing.T) {
			resp := callTool(t, s, toolName, map[string]any{
				"ids":    []any{"evt_mem_1"},
				"reason": "operator_cleanup",
			})
			if !resp.IsError {
				t.Fatalf("expected %s error", toolName)
			}
			if len(resp.Content) == 0 {
				t.Fatalf("expected error content")
			}
			msg := resp.Content[0].Text
			if !strings.Contains(msg, "code=managed_target_mismatch") {
				t.Fatalf("expected managed_target_mismatch code, got %q", msg)
			}
			if !strings.Contains(msg, "managed endpoint target/ownership mapping is out of sync with route policy resolution") {
				t.Fatalf("expected managed_target_mismatch fallback detail, got %q", msg)
			}
		})
	}
}

func TestToolMessagesPublishMemoryBackendConflictStatusMapped(t *testing.T) {
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
			if r.Method != http.MethodPost || r.URL.Path != "/admin/messages/publish" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":       "duplicate_id",
				"detail":     "item.id \"evt_mem_conflict\" already exists",
				"item_index": 0,
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r" {
  queue { backend "memory" }
  deliver "https://example.org/hook" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_mem_conflict",
				"route":  "/r",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish conflict error")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, "conflict") || !strings.Contains(msg, "code=duplicate_id") || !strings.Contains(msg, "item_index=0") {
		t.Fatalf("expected mapped conflict with detail, got %q", msg)
	}
}

func TestToolMessagesPublishRequiresReason(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"items": []any{
			map[string]any{
				"id":     "evt_pub_no_reason",
				"route":  "/r",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for missing reason")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "reason") {
		t.Fatalf("expected missing reason error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRejectsOversizedAuditFields(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	cases := []struct {
		name        string
		overrideArg map[string]any
		expectText  string
	}{
		{
			name:        "reason_too_long",
			overrideArg: map[string]any{"reason": strings.Repeat("r", 513)},
			expectText:  "reason must be at most 512 chars",
		},
		{
			name:        "actor_too_long",
			overrideArg: map[string]any{"actor": strings.Repeat("a", 257)},
			expectText:  "actor must be at most 256 chars",
		},
		{
			name:        "request_id_too_long",
			overrideArg: map[string]any{"request_id": strings.Repeat("q", 257)},
			expectText:  "request_id must be at most 256 chars",
		},
		{
			name:        "actor_principal_mismatch",
			overrideArg: map[string]any{"actor": "ops@example.test"},
			expectText:  `actor "ops@example.test" must match configured MCP principal "test-admin"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := map[string]any{
				"reason": "operator_publish",
				"items": []any{
					map[string]any{
						"id":     "evt_pub_audit_limits",
						"route":  "/r",
						"target": "https://example.org/hook",
					},
				},
			}
			for k, v := range tc.overrideArg {
				args[k] = v
			}

			resp := callTool(t, s, "messages_publish", args)
			if !resp.IsError {
				t.Fatalf("expected publish error for %s", tc.name)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, tc.expectText) {
				t.Fatalf("expected error containing %q, got %#v", tc.expectText, resp.Content)
			}
		})
	}
}

func TestToolMessagesPublishRejectsUnknownFields(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))

	unknownTopLevel := callTool(t, s, "messages_publish", map[string]any{
		"reason":  "operator_publish",
		"unknown": true,
		"items": []any{
			map[string]any{
				"id":     "evt_pub_unknown_top",
				"route":  "/r",
				"target": "https://example.org/hook",
			},
		},
	})
	if !unknownTopLevel.IsError {
		t.Fatalf("expected publish error for unknown top-level arg")
	}
	if len(unknownTopLevel.Content) == 0 || !strings.Contains(unknownTopLevel.Content[0].Text, `arguments contains unknown key "unknown"`) {
		t.Fatalf("expected unknown top-level key error text, got %#v", unknownTopLevel.Content)
	}

	unknownItem := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":       "evt_pub_unknown_item",
				"route":    "/r",
				"target":   "https://example.org/hook",
				"unknown":  "value",
				"trace_id": "abc",
			},
		},
	})
	if !unknownItem.IsError {
		t.Fatalf("expected publish error for unknown item field")
	}
	if len(unknownItem.Content) == 0 || !strings.Contains(unknownItem.Content[0].Text, `items[0] contains unknown keys`) {
		t.Fatalf("expected unknown item field error text, got %#v", unknownItem.Content)
	}
}

func TestParseMessagesPublishArgsRejectsInvalidManagedSelectorLabels(t *testing.T) {
	_, err := parseMessagesPublishArgs(map[string]any{
		"items": []any{
			map[string]any{
				"id":            "evt_invalid_managed_selector",
				"application":   "billing core",
				"endpoint_name": "invoice.created",
			},
		},
	})
	if err == nil {
		t.Fatalf("expected invalid managed selector label error")
	}
	if !strings.Contains(err.Error(), "items[0].application must match") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseMessagesPublishArgsRejectsInvalidHeaders(t *testing.T) {
	_, err := parseMessagesPublishArgs(map[string]any{
		"items": []any{
			map[string]any{
				"id":      "evt_invalid_headers",
				"route":   "/r",
				"headers": map[string]any{"Bad Header": "value"},
			},
		},
	})
	if err == nil {
		t.Fatalf("expected invalid headers error")
	}
	if !strings.Contains(err.Error(), "items[0].headers:") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "invalid field name") {
		t.Fatalf("expected invalid field-name detail, got %v", err)
	}
}

func TestToolMessagesPublishRejectsPayloadOverRouteMaxBody(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  max_body 4b
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":          "evt_pub_too_large",
				"route":       "/r",
				"target":      "https://example.org/hook",
				"payload_b64": "aGVsbG8=",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected payload over max_body error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `decoded payload (5 bytes) exceeds max_body (4 bytes) for route "/r"`) {
		t.Fatalf("expected max_body publish error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRejectsHeadersOverRouteMaxHeaders(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  max_headers 4b
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":      "evt_pub_headers_too_large",
				"route":   "/r",
				"target":  "https://example.org/hook",
				"headers": map[string]any{"X": "hello"},
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected headers over max_headers error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `headers (6 bytes) exceed max_headers (4 bytes) for route "/r"`) {
		t.Fatalf("expected max_headers publish error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRejectsMixedSelectorModesInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/direct" {
  deliver "https://example.org/direct" {}
}
"/managed" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/managed" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_direct_mode",
				"route":  "/direct",
				"target": "https://example.org/direct",
			},
			map[string]any{
				"id":            "evt_managed_mode",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected mixed selector-mode error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "items must use a single selector mode per request") {
		t.Fatalf("expected mixed selector-mode error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRejectsMissingTargetWithAllowedTargetsDetail(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  deliver "https://example.org/a" {}
  deliver "https://example.org/b" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":    "evt_pub_missing_target",
				"route": "/r",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected missing target publish error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `item.target is required for route "/r"; allowed targets: "https://example.org/a", "https://example.org/b"`) {
		t.Fatalf("expected allowed targets detail, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRejectsInvalidTargetWithAllowedTargetsDetail(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  deliver "https://example.org/a" {}
  deliver "https://example.org/b" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_pub_bad_target",
				"route":  "/r",
				"target": "https://bad.example/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected invalid target publish error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `item.target "https://bad.example/hook" is not allowed for route "/r"; allowed targets: "https://example.org/a", "https://example.org/b"`) {
		t.Fatalf("expected allowed targets detail, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishMemoryBackendManagedAdminBatchesTargetErrorPreflightNoAdminCalls(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	scopedBillingCalled := 0
	scopedERPCalled := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get("X-Hookaido-Audit-Reason") != "operator_publish" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch r.URL.Path {
			case "/admin/applications/billing/endpoints/invoice.created/messages/publish":
				scopedBillingCalled++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"published": 1,
				})
				return
			case "/admin/applications/erp/endpoints/order.created/messages/publish":
				scopedERPCalled++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":       "target_unresolvable",
					"detail":     `item.target is required for route "/managed_b"; allowed targets: "https://example.org/c", "https://example.org/d"`,
					"item_index": 0,
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
			t.Fatalf("timed out waiting for admin server shutdown")
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
  deliver "https://example.org/c" {}
  deliver "https://example.org/d" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_managed_ok",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
			map[string]any{
				"id":            "evt_managed_missing_target",
				"application":   "erp",
				"endpoint_name": "order.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for managed batch target validation")
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected error content")
	}
	msg := resp.Content[0].Text
	if !strings.Contains(msg, `items[1]: item.target is required for route "/managed_b"; allowed targets: "https://example.org/c", "https://example.org/d"`) {
		t.Fatalf("expected local preflight target detail for second item, got %q", msg)
	}
	if scopedBillingCalled != 0 || scopedERPCalled != 0 {
		t.Fatalf("expected zero admin calls on local preflight failure, got billing=%d erp=%d", scopedBillingCalled, scopedERPCalled)
	}
}

func TestToolMessagesPublishManagedEndpoint(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
pull_api { auth token "raw:t" }
"/r" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e" }
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	publishResp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_pub_m1",
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"payload_b64":   "aGVsbG8=",
			},
		},
	})
	if publishResp.IsError {
		t.Fatalf("unexpected publish managed error: %s", publishResp.Content[0].Text)
	}

	listResp := callTool(t, s, "messages_list", map[string]any{
		"state": "queued",
		"route": "/r",
		"limit": 10,
	})
	if listResp.IsError {
		t.Fatalf("unexpected list error: %s", listResp.Content[0].Text)
	}
	listOut, ok := listResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", listResp.StructuredContent)
	}
	items, ok := listOut["items"].([]map[string]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected 1 queued item, got %#v", listOut["items"])
	}
	if tgt, _ := items[0]["target"].(string); tgt != "pull" {
		t.Fatalf("expected target=pull, got %#v", items[0]["target"])
	}
}

func TestToolMessagesPublishManagedRouteRequiresSelector(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
pull_api { auth token "raw:t" }
"/r" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e" }
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_pub_managed_route",
				"route":  "/r",
				"target": "pull",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for managed route without selector")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "is managed by") {
		t.Fatalf("expected managed-route selector error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishPreflightValidationPreventsPartialEnqueueInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/ok" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_preflight_ok",
				"route":  "/ok",
				"target": "https://example.org/hook",
			},
			map[string]any{
				"id":     "evt_preflight_missing_route",
				"route":  "/missing",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for missing route")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `items[1]: route "/missing" not found in compiled config`) {
		t.Fatalf("expected missing-route error text, got %#v", resp.Content)
	}

	store, err = queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer func() { _ = store.Close() }()

	listResp, err := store.ListMessages(queue.MessageListRequest{
		Route: "/ok",
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list queued messages: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected zero queued items after preflight failure, got %#v", listResp.Items)
	}
}

func TestToolMessagesPublishManagedEndpointRequiresTargetWhenAmbiguous(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
  deliver "https://example.org/b" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_pub_ambiguous",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for ambiguous target")
	}
}

func TestToolMessagesPublishDuplicateID(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_dup", Route: "/r", Target: "https://example.org/hook"}); err != nil {
		t.Fatalf("seed enqueue: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_dup",
				"route":  "/r",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for duplicate id")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "already exists") {
		t.Fatalf("expected duplicate-id error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishDuplicateIDPrecheckPreventsPartialEnqueueInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_dup_existing", Route: "/r", Target: "https://example.org/hook"}); err != nil {
		t.Fatalf("seed enqueue: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_new_before_dup",
				"route":  "/r",
				"target": "https://example.org/hook",
			},
			map[string]any{
				"id":     "evt_dup_existing",
				"route":  "/r",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for duplicate id")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `items[1].id "evt_dup_existing" already exists`) {
		t.Fatalf("expected indexed duplicate-id error text, got %#v", resp.Content)
	}

	store, err = queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer func() { _ = store.Close() }()

	listResp, err := store.ListMessages(queue.MessageListRequest{
		Route: "/r",
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list queued messages: %v", err)
	}
	for _, item := range listResp.Items {
		if item.ID == "evt_new_before_dup" {
			t.Fatalf("expected no partial enqueue for first item, got %#v", listResp.Items)
		}
	}
}

func TestToolMessagesPublishRouteMustStartWithSlash(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/r" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_bad_route",
				"route":  "bad-route",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error for invalid route")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "route must start with '/'") {
		t.Fatalf("expected route-format error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRespectsGlobalDirectPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    direct off
  }
}
"/r" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_policy_global_off",
				"route":  "/r",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when global direct publish is disabled")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "direct publish is disabled by defaults.publish_policy.direct") {
		t.Fatalf("expected global policy error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRespectsScopedManagedPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    managed off
  }
}
"/managed" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_policy_scoped_off",
				"application":   "billing",
				"endpoint_name": "invoice.created",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when scoped managed publish is disabled")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "managed publish is disabled by defaults.publish_policy.managed") {
		t.Fatalf("expected scoped policy error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRespectsPullRoutePolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    allow_pull_routes off
  }
}
pull_api { auth token "raw:t" }
"/pull-route" {
  pull { path "/e" }
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_policy_pull_off",
				"route":  "/pull-route",
				"target": "pull",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when pull-route publish is disabled")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "publish to pull routes is disabled by defaults.publish_policy.allow_pull_routes") {
		t.Fatalf("expected pull-route policy error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRespectsDeliverRoutePolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    allow_deliver_routes off
  }
}
"/deliver-route" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_policy_deliver_off",
				"route":  "/deliver-route",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when deliver-route publish is disabled")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "publish to deliver routes is disabled by defaults.publish_policy.allow_deliver_routes") {
		t.Fatalf("expected deliver-route policy error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRespectsRoutePublishPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/deliver-route" {
  publish off
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_policy_route_publish_off",
				"route":  "/deliver-route",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when route publish is disabled")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `publish is disabled for route "/deliver-route" by route.publish`) {
		t.Fatalf("expected route publish policy error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRespectsRoutePublishDirectPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
"/deliver-route" {
  publish { direct off }
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_policy_route_publish.direct_off",
				"route":  "/deliver-route",
				"target": "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when route publish.direct is disabled")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `direct publish is disabled for route "/deliver-route" by route.publish.direct`) {
		t.Fatalf("expected route publish.direct policy error text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishDerivesAuditActorFromPrincipalWhenRequiredByPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    require_actor on
  }
}
"/deliver-route" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_policy_actor_off",
				"route":  "/deliver-route",
				"target": "https://example.org/hook",
			},
		},
	})
	if resp.IsError {
		t.Fatalf("unexpected publish error when actor is derived from principal: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if published := intFromAny(out["published"]); published != 1 {
		t.Fatalf("expected published=1, got %#v", out["published"])
	}
	audit, ok := out["audit"].(map[string]any)
	if !ok {
		t.Fatalf("expected audit object, got %T", out["audit"])
	}
	if actor, _ := audit["actor"].(string); actor != "test-admin" {
		t.Fatalf("expected derived audit.actor=test-admin, got %#v", audit["actor"])
	}
}

func TestToolMessagesPublishRejectsDerivedScopedManagedActorByPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/managed" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_policy_scoped_actor_missing",
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"target":        "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when derived actor is not allowed by scoped managed actor policy")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "is not allowed for endpoint-scoped managed publish") {
		t.Fatalf("expected scoped managed actor denied text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishRejectsScopedManagedActorByPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test"
    actor_prefix "role:ops/"
  }
}
"/managed" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("dev@example.test"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_policy_scoped_actor_denied",
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"target":        "https://example.org/hook",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when scoped managed actor is not allowed")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "is not allowed for endpoint-scoped managed publish") {
		t.Fatalf("expected scoped managed actor denied text, got %#v", resp.Content)
	}
}

func TestToolMessagesPublishDirectPathIgnoresScopedManagedActorPolicyInSQLiteMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/direct" {
  deliver "https://example.org/hook" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_policy_scoped_actor_direct_ok",
				"route":  "/direct",
				"target": "https://example.org/hook",
			},
		},
	})
	if resp.IsError {
		t.Fatalf("unexpected direct publish error with scoped managed actor policy: %s", resp.Content[0].Text)
	}
}

func TestToolMessagesPublishRespectsGlobalDirectPolicyInAdminProxyMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			w.WriteHeader(http.StatusInternalServerError)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
defaults {
  publish_policy {
    direct off
  }
}
"/direct" {
  queue { backend "memory" }
  deliver "https://example.org/direct" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_mem_global_off",
				"route":  "/direct",
				"target": "https://example.org/direct",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when global direct publish is disabled in proxy mode")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "direct publish is disabled by defaults.publish_policy.direct") {
		t.Fatalf("expected global policy error text, got %#v", resp.Content)
	}
	if adminCalls != 0 {
		t.Fatalf("expected zero admin calls when policy rejects locally, got %d", adminCalls)
	}
}

func TestToolMessagesPublishRespectsDeliverRoutePolicyInAdminProxyMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			w.WriteHeader(http.StatusInternalServerError)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
defaults {
  publish_policy {
    allow_deliver_routes off
  }
}
"/direct" {
  queue { backend "memory" }
  deliver "https://example.org/direct" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_mem_deliver_off",
				"route":  "/direct",
				"target": "https://example.org/direct",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when deliver-route publish is disabled in proxy mode")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "publish to deliver routes is disabled by defaults.publish_policy.allow_deliver_routes") {
		t.Fatalf("expected deliver-route policy error text, got %#v", resp.Content)
	}
	if adminCalls != 0 {
		t.Fatalf("expected zero admin calls when policy rejects locally, got %d", adminCalls)
	}
}

func TestToolMessagesPublishRequiresAuditRequestIDByPolicyInAdminProxyMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			w.WriteHeader(http.StatusInternalServerError)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
defaults {
  publish_policy {
    require_request_id on
  }
}
"/direct" {
  queue { backend "memory" }
  deliver "https://example.org/direct" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_mem_reqid_off",
				"route":  "/direct",
				"target": "https://example.org/direct",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when request id is required in proxy mode")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "X-Request-ID is required by defaults.publish_policy.require_request_id") {
		t.Fatalf("expected request-id policy error text, got %#v", resp.Content)
	}
	if adminCalls != 0 {
		t.Fatalf("expected zero admin calls when policy rejects locally, got %d", adminCalls)
	}
}

func TestToolMessagesPublishRejectsScopedManagedActorByPolicyInAdminProxyMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			w.WriteHeader(http.StatusInternalServerError)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
defaults {
  publish_policy {
    actor_allow "ops@example.test"
  }
}
"/managed" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/managed" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("dev@example.test"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_mem_scoped_actor_denied",
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"target":        "https://example.org/managed",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when scoped managed actor is not allowed in proxy mode")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "is not allowed for endpoint-scoped managed publish") {
		t.Fatalf("expected scoped managed actor denied text, got %#v", resp.Content)
	}
	if adminCalls != 0 {
		t.Fatalf("expected zero admin calls when scoped managed actor policy rejects locally, got %d", adminCalls)
	}
}

func TestToolMessagesPublishRespectsRoutePublishPolicyInAdminProxyMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			w.WriteHeader(http.StatusInternalServerError)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/direct" {
  publish off
  queue { backend "memory" }
  deliver "https://example.org/direct" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":     "evt_mem_route_publish_off",
				"route":  "/direct",
				"target": "https://example.org/direct",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when route publish is disabled in proxy mode")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `publish is disabled for route "/direct" by route.publish`) {
		t.Fatalf("expected route publish policy error text, got %#v", resp.Content)
	}
	if adminCalls != 0 {
		t.Fatalf("expected zero admin calls when policy rejects locally, got %d", adminCalls)
	}
}

func TestToolMessagesPublishRespectsRoutePublishManagedPolicyInAdminProxyMode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	adminCalls := 0
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminCalls++
			w.WriteHeader(http.StatusInternalServerError)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/managed" {
  application "billing"
  endpoint_name "invoice.created"
  publish { managed off }
  queue { backend "memory" }
  deliver "https://example.org/direct" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_publish", map[string]any{
		"reason": "operator_publish",
		"items": []any{
			map[string]any{
				"id":            "evt_mem_route_publish.managed_off",
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"target":        "https://example.org/direct",
			},
		},
	})
	if !resp.IsError {
		t.Fatalf("expected publish error when route publish.managed is disabled in proxy mode")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `managed publish is disabled for route "/managed" by route.publish.managed`) {
		t.Fatalf("expected route publish.managed policy error text, got %#v", resp.Content)
	}
	if adminCalls != 0 {
		t.Fatalf("expected zero admin calls when policy rejects locally, got %d", adminCalls)
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

func TestToolBacklogTopQueued(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(queue.Envelope{ID: "evt_bq_1", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-3 * time.Minute), NextRunAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_bq_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_bq_2", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-2 * time.Minute), NextRunAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_bq_2: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_bq_3", Route: "/r2", Target: "https://example.org/hook", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute), NextRunAt: now.Add(30 * time.Second)}); err != nil {
		t.Fatalf("enqueue evt_bq_3: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_bq_dead", Route: "/r3", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_bq_dead: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath)
	resp := callTool(t, s, "backlog_top_queued", map[string]any{
		"limit": 1,
	})
	if resp.IsError {
		t.Fatalf("unexpected backlog_top_queued error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if queueTotal := intFromAny(out["queue_total"]); queueTotal != 4 {
		t.Fatalf("expected queue_total=4, got %#v", out["queue_total"])
	}
	if queuedTotal := intFromAny(out["queued_total"]); queuedTotal != 3 {
		t.Fatalf("expected queued_total=3, got %#v", out["queued_total"])
	}
	if truncated, _ := out["truncated"].(bool); !truncated {
		t.Fatalf("expected truncated=true")
	}
	items, ok := out["items"].([]map[string]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one item, got %#v", out["items"])
	}
	if route, _ := items[0]["route"].(string); route != "/r1" {
		t.Fatalf("expected top bucket route=/r1, got %#v", items[0]["route"])
	}
	if queued := intFromAny(items[0]["queued"]); queued != 2 {
		t.Fatalf("expected top bucket queued=2, got %#v", items[0]["queued"])
	}

	filterResp := callTool(t, s, "backlog_top_queued", map[string]any{
		"route": "/r2",
		"limit": 10,
	})
	if filterResp.IsError {
		t.Fatalf("unexpected filtered backlog_top_queued error: %s", filterResp.Content[0].Text)
	}
	filtered, ok := filterResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected filtered structured content type %T", filterResp.StructuredContent)
	}
	filterItems, ok := filtered["items"].([]map[string]any)
	if !ok || len(filterItems) != 1 {
		t.Fatalf("expected one filtered item, got %#v", filtered["items"])
	}
	if route, _ := filterItems[0]["route"].(string); route != "/r2" {
		t.Fatalf("expected filtered route=/r2, got %#v", filterItems[0]["route"])
	}
	if truncated, _ := filtered["truncated"].(bool); truncated {
		t.Fatalf("expected truncated=false for filtered response")
	}
}

func TestToolBacklogOldestQueued(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(queue.Envelope{ID: "evt_bo_1", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-4 * time.Minute), NextRunAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_bo_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_bo_2", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-3 * time.Minute), NextRunAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_bo_2: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_bo_3", Route: "/r2", Target: "https://example.org/hook", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute), NextRunAt: now.Add(30 * time.Second)}); err != nil {
		t.Fatalf("enqueue evt_bo_3: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_bo_dead", Route: "/r3", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_bo_dead: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath)
	resp := callTool(t, s, "backlog_oldest_queued", map[string]any{
		"limit": 2,
	})
	if resp.IsError {
		t.Fatalf("unexpected backlog_oldest_queued error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if queueTotal := intFromAny(out["queue_total"]); queueTotal != 4 {
		t.Fatalf("expected queue_total=4, got %#v", out["queue_total"])
	}
	if queuedTotal := intFromAny(out["queued_total"]); queuedTotal != 3 {
		t.Fatalf("expected queued_total=3, got %#v", out["queued_total"])
	}
	if truncated, _ := out["truncated"].(bool); !truncated {
		t.Fatalf("expected truncated=true")
	}
	items, ok := out["items"].([]map[string]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected two items, got %#v", out["items"])
	}
	if id, _ := items[0]["id"].(string); id != "evt_bo_1" {
		t.Fatalf("expected first oldest id evt_bo_1, got %#v", items[0]["id"])
	}
	if id, _ := items[1]["id"].(string); id != "evt_bo_2" {
		t.Fatalf("expected second oldest id evt_bo_2, got %#v", items[1]["id"])
	}

	filterResp := callTool(t, s, "backlog_oldest_queued", map[string]any{
		"route": "/r2",
		"limit": 10,
	})
	if filterResp.IsError {
		t.Fatalf("unexpected filtered backlog_oldest_queued error: %s", filterResp.Content[0].Text)
	}
	filtered, ok := filterResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected filtered structured content type %T", filterResp.StructuredContent)
	}
	filterItems, ok := filtered["items"].([]map[string]any)
	if !ok || len(filterItems) != 1 {
		t.Fatalf("expected one filtered item, got %#v", filtered["items"])
	}
	if id, _ := filterItems[0]["id"].(string); id != "evt_bo_3" {
		t.Fatalf("expected filtered id evt_bo_3, got %#v", filterItems[0]["id"])
	}
	if truncated, _ := filtered["truncated"].(bool); truncated {
		t.Fatalf("expected truncated=false for filtered response")
	}
}

func TestToolBacklogAgingSummary(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	now := time.Now().UTC().Truncate(time.Second)
	store, err := queue.NewSQLiteStore(dbPath, queue.WithSQLiteNowFunc(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_ba_1", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-5 * time.Minute), NextRunAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_ba_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_ba_2", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-4 * time.Minute), NextRunAt: now.Add(48 * time.Hour)}); err != nil {
		t.Fatalf("enqueue evt_ba_2: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_ba_3", Route: "/r1", Target: "pull", State: queue.StateLeased, ReceivedAt: now.Add(-20 * time.Minute), NextRunAt: now.Add(1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_ba_3: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_ba_dead", Route: "/r2", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_ba_dead: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath)
	resp := callTool(t, s, "backlog_aging_summary", map[string]any{
		"limit": 2,
	})
	if resp.IsError {
		t.Fatalf("unexpected backlog_aging_summary error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if queueTotal := intFromAny(out["queue_total"]); queueTotal != 4 {
		t.Fatalf("expected queue_total=4, got %#v", out["queue_total"])
	}
	if queuedTotal := intFromAny(out["queued_total"]); queuedTotal != 2 {
		t.Fatalf("expected queued_total=2, got %#v", out["queued_total"])
	}
	if truncated, _ := out["truncated"].(bool); truncated {
		t.Fatalf("expected truncated=false")
	}
	if scanned := intFromAny(out["scanned"]); scanned != 4 {
		t.Fatalf("expected scanned=4, got %#v", out["scanned"])
	}
	if samples := intFromAny(out["age_sample_count"]); samples != 4 {
		t.Fatalf("expected age_sample_count=4, got %#v", out["age_sample_count"])
	}
	ageWindows, ok := out["age_windows"].(map[string]any)
	if !ok {
		t.Fatalf("expected top-level age_windows, got %T", out["age_windows"])
	}
	if bucketTotal := intFromAny(ageWindows["le_5m"]) + intFromAny(ageWindows["gt_5m_le_15m"]) + intFromAny(ageWindows["gt_15m_le_1h"]) + intFromAny(ageWindows["gt_1h_le_6h"]) + intFromAny(ageWindows["gt_6h"]); bucketTotal != 4 {
		t.Fatalf("expected top-level age windows to sum to 4, got %d (%#v)", bucketTotal, ageWindows)
	}
	topPercentiles, ok := out["age_percentiles_seconds"].(map[string]any)
	if !ok {
		t.Fatalf("expected top-level age_percentiles_seconds, got %T", out["age_percentiles_seconds"])
	}
	p50 := intFromAny(topPercentiles["p50"])
	p90 := intFromAny(topPercentiles["p90"])
	p99 := intFromAny(topPercentiles["p99"])
	if p50 <= 0 || p90 <= 0 || p99 <= 0 || p50 > p90 || p90 > p99 {
		t.Fatalf("expected monotonic positive top-level percentiles, got p50=%d p90=%d p99=%d", p50, p90, p99)
	}
	items, ok := out["items"].([]map[string]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected two summary items, got %#v", out["items"])
	}
	if route, _ := items[0]["route"].(string); route != "/r1" {
		t.Fatalf("expected summary route=/r1, got %#v", items[0]["route"])
	}
	if observed := intFromAny(items[0]["queued_observed"]); observed != 2 {
		t.Fatalf("expected queued_observed=2, got %#v", items[0]["queued_observed"])
	}
	if observed := intFromAny(items[0]["leased_observed"]); observed != 1 {
		t.Fatalf("expected leased_observed=1, got %#v", items[0]["leased_observed"])
	}
	if observed := intFromAny(items[0]["dead_observed"]); observed != 0 {
		t.Fatalf("expected dead_observed=0, got %#v", items[0]["dead_observed"])
	}
	if observed := intFromAny(items[0]["total_observed"]); observed != 3 {
		t.Fatalf("expected total_observed=3, got %#v", items[0]["total_observed"])
	}
	if overdue := intFromAny(items[0]["ready_overdue_count"]); overdue != 1 {
		t.Fatalf("expected ready_overdue_count=1, got %#v", items[0]["ready_overdue_count"])
	}
	if samples := intFromAny(items[0]["age_sample_count"]); samples != 3 {
		t.Fatalf("expected first item age_sample_count=3, got %#v", items[0]["age_sample_count"])
	}
	itemAgeWindows, ok := items[0]["age_windows"].(map[string]any)
	if !ok {
		t.Fatalf("expected first item age_windows map, got %T", items[0]["age_windows"])
	}
	if bucketTotal := intFromAny(itemAgeWindows["le_5m"]) + intFromAny(itemAgeWindows["gt_5m_le_15m"]) + intFromAny(itemAgeWindows["gt_15m_le_1h"]) + intFromAny(itemAgeWindows["gt_1h_le_6h"]) + intFromAny(itemAgeWindows["gt_6h"]); bucketTotal != 3 {
		t.Fatalf("expected first item age windows to sum to 3, got %d (%#v)", bucketTotal, itemAgeWindows)
	}
	itemPercentiles, ok := items[0]["age_percentiles_seconds"].(map[string]any)
	if !ok {
		t.Fatalf("expected first item age_percentiles_seconds map, got %T", items[0]["age_percentiles_seconds"])
	}
	itemP50 := intFromAny(itemPercentiles["p50"])
	itemP90 := intFromAny(itemPercentiles["p90"])
	itemP99 := intFromAny(itemPercentiles["p99"])
	if itemP50 <= 0 || itemP90 <= 0 || itemP99 <= 0 || itemP50 > itemP90 || itemP90 > itemP99 {
		t.Fatalf("expected monotonic positive first-item percentiles, got p50=%d p90=%d p99=%d", itemP50, itemP90, itemP99)
	}

	filterResp := callTool(t, s, "backlog_aging_summary", map[string]any{
		"states": []any{"queued"},
		"limit":  1,
	})
	if filterResp.IsError {
		t.Fatalf("unexpected filtered backlog_aging_summary error: %s", filterResp.Content[0].Text)
	}
	filtered, ok := filterResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected filtered structured content type %T", filterResp.StructuredContent)
	}
	filterItems, ok := filtered["items"].([]map[string]any)
	if !ok || len(filterItems) != 1 {
		t.Fatalf("expected one filtered summary item, got %#v", filtered["items"])
	}
	if route, _ := filterItems[0]["route"].(string); route != "/r1" {
		t.Fatalf("expected filtered route=/r1, got %#v", filterItems[0]["route"])
	}
	if truncated, _ := filtered["truncated"].(bool); !truncated {
		t.Fatalf("expected truncated=true for filtered response")
	}
	if samples := intFromAny(filtered["age_sample_count"]); samples != 1 {
		t.Fatalf("expected filtered age_sample_count=1, got %#v", filtered["age_sample_count"])
	}
	filteredPercentiles, ok := filtered["age_percentiles_seconds"].(map[string]any)
	if !ok {
		t.Fatalf("expected filtered age_percentiles_seconds, got %T", filtered["age_percentiles_seconds"])
	}
	filteredP50 := intFromAny(filteredPercentiles["p50"])
	filteredP90 := intFromAny(filteredPercentiles["p90"])
	filteredP99 := intFromAny(filteredPercentiles["p99"])
	if filteredP50 <= 0 || filteredP50 != filteredP90 || filteredP90 != filteredP99 {
		t.Fatalf("expected equal positive filtered percentiles, got p50=%d p90=%d p99=%d", filteredP50, filteredP90, filteredP99)
	}

	badResp := callTool(t, s, "backlog_aging_summary", map[string]any{
		"states": []any{"delivered"},
	})
	if !badResp.IsError {
		t.Fatalf("expected error for invalid states")
	}
}

func TestToolBacklogTrends(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store, err := queue.NewSQLiteStore(dbPath, queue.WithSQLiteNowFunc(func() time.Time { return nowVar }))
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_bt_1", Route: "/r1", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue evt_bt_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_bt_2", Route: "/r1", Target: "pull", State: queue.StateLeased}); err != nil {
		t.Fatalf("enqueue evt_bt_2: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_bt_3", Route: "/r2", Target: "pull", State: queue.StateDead}); err != nil {
		t.Fatalf("enqueue evt_bt_3: %v", err)
	}
	if err := store.CaptureBacklogTrendSample(nowVar); err != nil {
		t.Fatalf("capture trend #1: %v", err)
	}

	if err := store.Enqueue(queue.Envelope{ID: "evt_bt_4", Route: "/r1", Target: "https://example.org/hook", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue evt_bt_4: %v", err)
	}
	nowVar = nowVar.Add(1 * time.Minute)
	if err := store.CaptureBacklogTrendSample(nowVar); err != nil {
		t.Fatalf("capture trend #2: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath)
	resp := callTool(t, s, "backlog_trends", map[string]any{
		"window": "2m",
		"step":   "1m",
		"until":  "2026-02-07T12:02:00Z",
	})
	if resp.IsError {
		t.Fatalf("unexpected backlog_trends error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if samples := intFromAny(out["sample_count"]); samples != 2 {
		t.Fatalf("expected sample_count=2, got %#v", out["sample_count"])
	}
	if points := intFromAny(out["point_count"]); points != 2 {
		t.Fatalf("expected point_count=2, got %#v", out["point_count"])
	}
	if latest := intFromAny(out["latest_total"]); latest != 4 {
		t.Fatalf("expected latest_total=4, got %#v", out["latest_total"])
	}
	if maxTotal := intFromAny(out["max_total"]); maxTotal != 4 {
		t.Fatalf("expected max_total=4, got %#v", out["max_total"])
	}
	signals, ok := out["signals"].(map[string]any)
	if !ok {
		t.Fatalf("expected signals object, got %T", out["signals"])
	}
	if sampleCount := intFromAny(signals["sample_count"]); sampleCount != 2 {
		t.Fatalf("expected signals.sample_count=2, got %#v", signals["sample_count"])
	}
	if _, ok := signals["operator_actions"]; !ok {
		t.Fatalf("expected signals.operator_actions")
	}
	items, ok := out["items"].([]map[string]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected 2 trend items, got %#v", out["items"])
	}
	if intFromAny(items[0]["total_last"]) != 3 || intFromAny(items[1]["total_last"]) != 4 {
		t.Fatalf("unexpected total_last values: %#v", items)
	}

	filterResp := callTool(t, s, "backlog_trends", map[string]any{
		"window": "2m",
		"step":   "1m",
		"until":  "2026-02-07T12:02:00Z",
		"route":  "/r1",
	})
	if filterResp.IsError {
		t.Fatalf("unexpected filtered backlog_trends error: %s", filterResp.Content[0].Text)
	}
	filtered, ok := filterResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected filtered structured content type %T", filterResp.StructuredContent)
	}
	if latest := intFromAny(filtered["latest_total"]); latest != 3 {
		t.Fatalf("expected filtered latest_total=3, got %#v", filtered["latest_total"])
	}

	badResp := callTool(t, s, "backlog_trends", map[string]any{
		"window": "2h",
		"step":   "30s",
	})
	if !badResp.IsError {
		t.Fatalf("expected error for invalid backlog_trends step")
	}
}

func TestToolBacklogTrendsUsesConfiguredTrendSignals(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "hookaido.db")
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	cfg := `
defaults {
  trend_signals {
    recent_surge_min_total 100
    recent_surge_min_delta 100
    recent_surge_percent 300
    queued_pressure_min_total 100
  }
}
"/r1" {
  deliver "https://example.org/hook" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store, err := queue.NewSQLiteStore(dbPath, queue.WithSQLiteNowFunc(func() time.Time { return nowVar }))
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := store.Enqueue(queue.Envelope{
			ID:     fmt.Sprintf("evt_cfg_%02d", i),
			Route:  "/r1",
			Target: "pull",
			State:  queue.StateQueued,
		}); err != nil {
			t.Fatalf("enqueue baseline event %d: %v", i, err)
		}
	}
	if err := store.CaptureBacklogTrendSample(nowVar); err != nil {
		t.Fatalf("capture trend #1: %v", err)
	}
	for i := 10; i < 40; i++ {
		if err := store.Enqueue(queue.Envelope{
			ID:     fmt.Sprintf("evt_cfg_%02d", i),
			Route:  "/r1",
			Target: "pull",
			State:  queue.StateQueued,
		}); err != nil {
			t.Fatalf("enqueue surge event %d: %v", i, err)
		}
	}
	nowVar = nowVar.Add(1 * time.Minute)
	if err := store.CaptureBacklogTrendSample(nowVar); err != nil {
		t.Fatalf("capture trend #2: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath)
	resp := callTool(t, s, "backlog_trends", map[string]any{
		"window": "2m",
		"step":   "1m",
		"until":  "2026-02-07T12:02:00Z",
	})
	if resp.IsError {
		t.Fatalf("unexpected backlog_trends error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	signals, ok := out["signals"].(map[string]any)
	if !ok {
		t.Fatalf("expected signals object, got %T", out["signals"])
	}
	if surge, _ := signals["recent_surge"].(bool); surge {
		t.Fatalf("expected recent_surge=false with configured trend_signals policy, got %#v", signals)
	}
	if signalAlertsContain(signals["active_alerts"], "recent_surge") {
		t.Fatalf("did not expect recent_surge in active_alerts, got %#v", signals["active_alerts"])
	}
	if signalOperatorActionsContain(signals["operator_actions"], "backlog_growth_triage") {
		t.Fatalf("did not expect backlog_growth_triage in operator_actions, got %#v", signals["operator_actions"])
	}
}

func TestToolManagementEndpointUpsert(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
"/r_a" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
"/r_b" {
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	upsertResp := callTool(t, s, "management_endpoint_upsert", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"route":         "/r_b",
		"reason":        "oncall_move",
		"request_id":    "req-mcp-upsert-1",
	})
	if upsertResp.IsError {
		t.Fatalf("unexpected management_endpoint_upsert error: %s", upsertResp.Content[0].Text)
	}
	out, ok := upsertResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", upsertResp.StructuredContent)
	}
	if action, _ := out["action"].(string); action != "updated" {
		t.Fatalf("expected action=updated, got %#v", out["action"])
	}
	if route, _ := out["route"].(string); route != "/r_b" {
		t.Fatalf("expected route=/r_b, got %#v", out["route"])
	}
	if mutates, _ := out["mutates_config"].(bool); !mutates {
		t.Fatalf("expected mutates_config=true")
	}
	audit, ok := out["audit"].(map[string]any)
	if !ok {
		t.Fatalf("expected audit object, got %T", out["audit"])
	}
	if reason, _ := audit["reason"].(string); reason != "oncall_move" {
		t.Fatalf("expected audit.reason=oncall_move, got %#v", audit["reason"])
	}
	if actor, _ := audit["actor"].(string); actor != "test-admin" {
		t.Fatalf("expected audit.actor=test-admin, got %#v", audit["actor"])
	}
	if principal, _ := audit["principal"].(string); principal != "test-admin" {
		t.Fatalf("expected audit.principal=test-admin, got %#v", audit["principal"])
	}

	modelResp := callTool(t, s, "management_model", map[string]any{})
	if modelResp.IsError {
		t.Fatalf("unexpected management_model error: %s", modelResp.Content[0].Text)
	}
	modelOut, ok := modelResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected model structured content type %T", modelResp.StructuredContent)
	}
	apps, ok := modelOut["applications"].([]map[string]any)
	if !ok || len(apps) != 1 {
		t.Fatalf("expected one application, got %#v", modelOut["applications"])
	}
	endpoints, ok := apps[0]["endpoints"].([]map[string]any)
	if !ok || len(endpoints) != 1 {
		t.Fatalf("expected one endpoint, got %#v", apps[0]["endpoints"])
	}
	if route, _ := endpoints[0]["route"].(string); route != "/r_b" {
		t.Fatalf("expected endpoint route=/r_b after upsert, got %#v", endpoints[0]["route"])
	}
}

func TestToolManagementEndpointDelete(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
"/r_a" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	deleteResp := callTool(t, s, "management_endpoint_delete", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"reason":        "cleanup",
		"request_id":    "req-mcp-delete-1",
	})
	if deleteResp.IsError {
		t.Fatalf("unexpected management_endpoint_delete error: %s", deleteResp.Content[0].Text)
	}
	out, ok := deleteResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", deleteResp.StructuredContent)
	}
	if action, _ := out["action"].(string); action != "deleted" {
		t.Fatalf("expected action=deleted, got %#v", out["action"])
	}
	if mutates, _ := out["mutates_config"].(bool); !mutates {
		t.Fatalf("expected mutates_config=true")
	}
	audit, ok := out["audit"].(map[string]any)
	if !ok {
		t.Fatalf("expected audit object, got %T", out["audit"])
	}
	if actor, _ := audit["actor"].(string); actor != "test-admin" {
		t.Fatalf("expected audit.actor=test-admin, got %#v", audit["actor"])
	}

	modelResp := callTool(t, s, "management_model", map[string]any{})
	if modelResp.IsError {
		t.Fatalf("unexpected management_model error: %s", modelResp.Content[0].Text)
	}
	modelOut, ok := modelResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected model structured content type %T", modelResp.StructuredContent)
	}
	if appCount := intFromAny(modelOut["application_count"]); appCount != 0 {
		t.Fatalf("expected application_count=0 after delete, got %#v", modelOut["application_count"])
	}
	if endpointCount := intFromAny(modelOut["path_count"]); endpointCount != 0 {
		t.Fatalf("expected path_count=0 after delete, got %#v", modelOut["path_count"])
	}
}

func TestToolManagementEndpointMutationRequiresReason(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
"/r_a" {
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))

	upsertResp := callTool(t, s, "management_endpoint_upsert", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"route":         "/r_a",
	})
	if !upsertResp.IsError {
		t.Fatalf("expected reason required error for upsert")
	}

	deleteResp := callTool(t, s, "management_endpoint_delete", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
	})
	if !deleteResp.IsError {
		t.Fatalf("expected reason required error for delete")
	}
}

func TestToolManagementEndpointMutationRequiresAuditRequestIDByPolicy(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    require_request_id on
  }
}
"/r_a" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	cases := []struct {
		name string
		tool string
		args map[string]any
	}{
		{
			name: "upsert",
			tool: "management_endpoint_upsert",
			args: map[string]any{
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"route":         "/r_a",
				"reason":        "oncall_move",
			},
		},
		{
			name: "delete",
			tool: "management_endpoint_delete",
			args: map[string]any{
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"reason":        "cleanup",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := callTool(t, s, tc.tool, tc.args)
			if !resp.IsError {
				t.Fatalf("expected %s policy error when request_id is required", tc.tool)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "X-Request-ID is required by defaults.publish_policy.require_request_id") {
				t.Fatalf("expected request-id policy error text, got %#v", resp.Content)
			}
		})
	}
}

func TestToolManagementEndpointMutationRejectsScopedManagedActorByPolicy(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test"
    actor_prefix "role:ops/"
  }
}
"/r_a" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("dev@example.test"))
	cases := []struct {
		name string
		tool string
		args map[string]any
	}{
		{
			name: "upsert",
			tool: "management_endpoint_upsert",
			args: map[string]any{
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"route":         "/r_a",
				"reason":        "oncall_move",
				"request_id":    "req-mcp-upsert-policy-1",
			},
		},
		{
			name: "delete",
			tool: "management_endpoint_delete",
			args: map[string]any{
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"reason":        "cleanup",
				"request_id":    "req-mcp-delete-policy-1",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := callTool(t, s, tc.tool, tc.args)
			if !resp.IsError {
				t.Fatalf("expected %s policy error when scoped managed actor is denied", tc.tool)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "is not allowed for endpoint-scoped managed mutation") {
				t.Fatalf("expected scoped managed actor denied text, got %#v", resp.Content)
			}
		})
	}
}

func TestParseManagementEndpointMutationArgsRejectsInvalidLabels(t *testing.T) {
	_, err := parseManagementEndpointMutationArgs(map[string]any{
		"application":   "billing core",
		"endpoint_name": "invoice.created",
		"route":         "/r_a",
		"reason":        "oncall_move",
	}, true, "test-admin")
	if err == nil {
		t.Fatalf("expected invalid application label error")
	}
	if !strings.Contains(err.Error(), "application must match") {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = parseManagementEndpointMutationArgs(map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice created",
		"reason":        "oncall_move",
	}, false, "test-admin")
	if err == nil {
		t.Fatalf("expected invalid endpoint_name label error")
	}
	if !strings.Contains(err.Error(), "endpoint_name must match") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestToolManagementEndpointMutationRejectsOversizedAuditFields(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
"/r_a" {
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	cases := []struct {
		name        string
		overrideArg map[string]any
		expectText  string
	}{
		{
			name:        "reason_too_long",
			overrideArg: map[string]any{"reason": strings.Repeat("r", 513)},
			expectText:  "reason must be at most 512 chars",
		},
		{
			name:        "actor_too_long",
			overrideArg: map[string]any{"actor": strings.Repeat("a", 257)},
			expectText:  "actor must be at most 256 chars",
		},
		{
			name:        "request_id_too_long",
			overrideArg: map[string]any{"request_id": strings.Repeat("q", 257)},
			expectText:  "request_id must be at most 256 chars",
		},
		{
			name:        "actor_principal_mismatch",
			overrideArg: map[string]any{"actor": "ops@example.test"},
			expectText:  `actor "ops@example.test" must match configured MCP principal "test-admin"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := map[string]any{
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"route":         "/r_a",
				"reason":        "oncall_move",
			}
			for k, v := range tc.overrideArg {
				args[k] = v
			}

			resp := callTool(t, s, "management_endpoint_upsert", args)
			if !resp.IsError {
				t.Fatalf("expected upsert audit validation error for %s", tc.name)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, tc.expectText) {
				t.Fatalf("expected error containing %q, got %#v", tc.expectText, resp.Content)
			}
		})
	}
}

func TestToolManagementEndpointUpsertConflict(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
"/r_a" {
  application "erp"
  endpoint_name "stock.updated"
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "management_endpoint_upsert", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"route":         "/r_a",
		"reason":        "conflict_test",
	})
	if !resp.IsError {
		t.Fatalf("expected conflict error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "code=management_route_already_mapped") {
		t.Fatalf("expected management_route_already_mapped code, got %#v", resp.Content)
	}
	if !strings.Contains(resp.Content[0].Text, `detail=route "/r_a" is already mapped to ("erp", "stock.updated")`) {
		t.Fatalf("expected conflict detail text, got %q", resp.Content[0].Text)
	}
}

func TestToolManagementEndpointUpsertRejectsRouteWithManagedPublishDisabled(t *testing.T) {
	tests := []struct {
		name       string
		policyLine string
		expectText string
	}{
		{
			name:       "publish_off",
			policyLine: "publish off",
			expectText: `managed publish is disabled for route "/r_b" by route.publish`,
		},
		{
			name:       "publish_managed_off",
			policyLine: "publish { managed off }",
			expectText: `managed publish is disabled for route "/r_b" by route.publish.managed`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			cfgPath := filepath.Join(tmp, "Hookaidofile")
			cfg := fmt.Sprintf(`
"/r_a" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
"/r_b" {
  %s
  deliver "https://example.org/a" {}
}
`, tc.policyLine)
			if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
			resp := callTool(t, s, "management_endpoint_upsert", map[string]any{
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"route":         "/r_b",
				"reason":        "conflict_test",
			})
			if !resp.IsError {
				t.Fatalf("expected conflict error")
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "code=management_route_publish_disabled") {
				t.Fatalf("expected management_route_publish_disabled code, got %#v", resp.Content)
			}
			if !strings.Contains(resp.Content[0].Text, "detail="+tc.expectText) {
				t.Fatalf("expected conflict error containing %q, got %q", tc.expectText, resp.Content[0].Text)
			}
		})
	}
}

func TestToolManagementEndpointUpsertRejectsRouteWithTargetProfileMismatch(t *testing.T) {
	tests := []struct {
		name       string
		configBody string
		detailHint string
	}{
		{
			name: "mode_mismatch_pull_to_deliver",
			configBody: `
pull_api { auth token "raw:t" }
"/r_a" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e1" }
}
"/r_b" {
  deliver "https://example.org/b" {}
}
`,
			detailHint: "mode=pull",
		},
		{
			name: "deliver_target_mismatch",
			configBody: `
"/r_a" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
"/r_b" {
  deliver "https://example.org/b" {}
}
`,
			detailHint: `targets="https://example.org/a"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			cfgPath := filepath.Join(tmp, "Hookaidofile")
			if err := os.WriteFile(cfgPath, []byte(tc.configBody), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
			resp := callTool(t, s, "management_endpoint_upsert", map[string]any{
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"route":         "/r_b",
				"reason":        "conflict_test",
			})
			if !resp.IsError {
				t.Fatalf("expected conflict error")
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "code=management_route_target_mismatch") {
				t.Fatalf("expected management_route_target_mismatch code, got %#v", resp.Content)
			}
			if !strings.Contains(resp.Content[0].Text, "detail=managed endpoint target profile mismatch") {
				t.Fatalf("expected target profile mismatch detail, got %q", resp.Content[0].Text)
			}
			if !strings.Contains(resp.Content[0].Text, tc.detailHint) {
				t.Fatalf("expected detail containing %q, got %q", tc.detailHint, resp.Content[0].Text)
			}
		})
	}
}

func TestToolManagementEndpointUpsertRouteNotFoundCode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
"/r_a" {
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "management_endpoint_upsert", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"route":         "/missing",
		"reason":        "conflict_test",
	})
	if !resp.IsError {
		t.Fatalf("expected route not found error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "code=management_route_not_found") {
		t.Fatalf("expected management_route_not_found code, got %#v", resp.Content)
	}
	if !strings.Contains(resp.Content[0].Text, `detail=route "/missing" not found in compiled config`) {
		t.Fatalf("expected route not found detail, got %q", resp.Content[0].Text)
	}
}

func TestToolManagementEndpointDeleteNotFoundCode(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
"/r_a" {
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "management_endpoint_delete", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"reason":        "cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected endpoint not found error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "code=management_endpoint_not_found") {
		t.Fatalf("expected management_endpoint_not_found code, got %#v", resp.Content)
	}
	if !strings.Contains(resp.Content[0].Text, `detail=management endpoint ("billing", "invoice.created") not found`) {
		t.Fatalf("expected endpoint not found detail, got %q", resp.Content[0].Text)
	}
}

func TestToolManagementEndpointUpsertRejectsNonAbsoluteRoute(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`
"/r_a" {
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "management_endpoint_upsert", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"route":         "r_a",
		"reason":        "invalid_route",
	})
	if !resp.IsError {
		t.Fatalf("expected invalid route error")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "route must start with '/'") {
		t.Fatalf("expected route format error text, got %#v", resp.Content)
	}
}

func TestToolManagementEndpointUpsertRejectsActiveBacklogWhenSQLiteContextAvailable(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")

	if err := os.WriteFile(cfgPath, []byte(`
"/r_a" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
"/r_b" {
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:         "evt_backlog_upsert_queued",
		Route:      "/r_a",
		Target:     "https://example.org/a",
		State:      queue.StateQueued,
		ReceivedAt: time.Date(2026, 2, 9, 9, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "management_endpoint_upsert", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"route":         "/r_b",
		"reason":        "oncall_move",
	})
	if !resp.IsError {
		t.Fatalf("expected active backlog conflict")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "code=management_route_backlog_active") {
		t.Fatalf("expected management_route_backlog_active code, got %#v", resp.Content)
	}
	if !strings.Contains(resp.Content[0].Text, `detail=cannot move management endpoint while route "/r_a" has active backlog (queued)`) {
		t.Fatalf("expected active backlog detail, got %q", resp.Content[0].Text)
	}
}

func TestToolManagementEndpointDeleteRejectsActiveBacklogWhenSQLiteContextAvailable(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")

	if err := os.WriteFile(cfgPath, []byte(`
"/r_a" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:         "evt_backlog_delete_leased",
		Route:      "/r_a",
		Target:     "https://example.org/a",
		State:      queue.StateLeased,
		ReceivedAt: time.Date(2026, 2, 9, 9, 5, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "management_endpoint_delete", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"reason":        "cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected active backlog conflict")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "code=management_route_backlog_active") {
		t.Fatalf("expected management_route_backlog_active code, got %#v", resp.Content)
	}
	if !strings.Contains(resp.Content[0].Text, `detail=cannot delete management endpoint while route "/r_a" has active backlog (leased)`) {
		t.Fatalf("expected active backlog detail, got %q", resp.Content[0].Text)
	}
}

func TestToolManagementEndpointUpsertRejectsActiveBacklogWhenMemoryContextAvailable(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	var queuedCalls int
	var leasedCalls int
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/admin/messages" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			q := r.URL.Query()
			if q.Get("route") != "/r_a" || q.Get("limit") != "1" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch q.Get("state") {
			case "queued":
				queuedCalls++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items": []map[string]any{
						{"id": "evt_mem_backlog"},
					},
				})
			case "leased":
				leasedCalls++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items": []map[string]any{},
				})
			default:
				w.WriteHeader(http.StatusBadRequest)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r_a" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
"/r_b" {
  queue { backend "memory" }
  deliver "https://example.org/a" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "management_endpoint_upsert", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"route":         "/r_b",
		"reason":        "oncall_move",
	})
	if !resp.IsError {
		t.Fatalf("expected active backlog conflict")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "code=management_route_backlog_active") {
		t.Fatalf("expected management_route_backlog_active code, got %#v", resp.Content)
	}
	if queuedCalls == 0 || leasedCalls == 0 {
		t.Fatalf("expected queued+leased admin backlog checks, got queued=%d leased=%d", queuedCalls, leasedCalls)
	}
}

func TestToolManagementEndpointDeleteRejectsActiveBacklogWhenMemoryContextAvailable(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	const token = "admintoken"
	var queuedCalls int
	var leasedCalls int
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/admin/messages" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			q := r.URL.Query()
			if q.Get("route") != "/r_a" || q.Get("limit") != "1" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch q.Get("state") {
			case "queued":
				queuedCalls++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items": []map[string]any{},
				})
			case "leased":
				leasedCalls++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items": []map[string]any{
						{"id": "evt_mem_backlog"},
					},
				})
			default:
				w.WriteHeader(http.StatusBadRequest)
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
			t.Fatalf("timed out waiting for admin server shutdown")
		}
	})

	cfg := fmt.Sprintf(`admin_api {
  listen %q
  prefix "/admin"
  auth token "raw:%s"
}
"/r_a" {
  queue { backend "memory" }
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
`, addr, token)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(nil, nil, cfgPath, "", WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "management_endpoint_delete", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"reason":        "cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected active backlog conflict")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, "code=management_route_backlog_active") {
		t.Fatalf("expected management_route_backlog_active code, got %#v", resp.Content)
	}
	if queuedCalls == 0 || leasedCalls == 0 {
		t.Fatalf("expected queued+leased admin backlog checks, got queued=%d leased=%d", queuedCalls, leasedCalls)
	}
}

func TestToolMessagesCancelByFilter(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_2", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_2: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	cancelResp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"route":  "/r",
		"state":  "queued",
		"limit":  1,
		"reason": "operator_cleanup",
	})
	if cancelResp.IsError {
		t.Fatalf("unexpected cancel by filter error: %s", cancelResp.Content[0].Text)
	}
	out, ok := cancelResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", cancelResp.StructuredContent)
	}
	if canceled := intFromAny(out["canceled"]); canceled != 1 {
		t.Fatalf("expected canceled=1, got %#v", out["canceled"])
	}
	if matched := intFromAny(out["matched"]); matched != 1 {
		t.Fatalf("expected matched=1, got %#v", out["matched"])
	}
	if preview, _ := out["preview_only"].(bool); preview {
		t.Fatalf("expected preview_only=false")
	}
}

func TestToolMessagesCancelByFilterRejectsUnknownFields(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"route":   "/r",
		"state":   "queued",
		"limit":   1,
		"reason":  "operator_cleanup",
		"unknown": true,
	})
	if !resp.IsError {
		t.Fatalf("expected cancel_by_filter error for unknown argument")
	}
	if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `arguments contains unknown key "unknown"`) {
		t.Fatalf("expected unknown key error text, got %#v", resp.Content)
	}
}

func TestToolMutationsRejectUnknownTopLevelFields(t *testing.T) {
	s := NewServer(
		nil,
		nil,
		"",
		"",
		WithMutationsEnabled(true),
		WithRuntimeControlEnabled(true),
		WithRole(RoleAdmin),
		WithPrincipal("test-admin"),
	)

	cases := []struct {
		name string
		tool string
		args map[string]any
	}{
		{
			name: "config_apply",
			tool: "config_apply",
			args: map[string]any{
				"content": `"/r" { deliver "https://example.org/hook" {} }`,
				"unknown": true,
			},
		},
		{
			name: "management_endpoint_upsert",
			tool: "management_endpoint_upsert",
			args: map[string]any{
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"route":         "/r",
				"reason":        "test",
				"unknown":       true,
			},
		},
		{
			name: "management_endpoint_delete",
			tool: "management_endpoint_delete",
			args: map[string]any{
				"application":   "billing",
				"endpoint_name": "invoice.created",
				"reason":        "test",
				"unknown":       true,
			},
		},
		{
			name: "dlq_requeue",
			tool: "dlq_requeue",
			args: map[string]any{
				"reason":  "test",
				"ids":     []any{"evt_1"},
				"unknown": true,
			},
		},
		{
			name: "dlq_delete",
			tool: "dlq_delete",
			args: map[string]any{
				"reason":  "test",
				"ids":     []any{"evt_1"},
				"unknown": true,
			},
		},
		{
			name: "messages_cancel",
			tool: "messages_cancel",
			args: map[string]any{
				"reason":  "test",
				"ids":     []any{"evt_1"},
				"unknown": true,
			},
		},
		{
			name: "messages_requeue",
			tool: "messages_requeue",
			args: map[string]any{
				"reason":  "test",
				"ids":     []any{"evt_1"},
				"unknown": true,
			},
		},
		{
			name: "messages_resume",
			tool: "messages_resume",
			args: map[string]any{
				"reason":  "test",
				"ids":     []any{"evt_1"},
				"unknown": true,
			},
		},
		{
			name: "messages_publish",
			tool: "messages_publish",
			args: map[string]any{
				"reason": "test",
				"items": []any{
					map[string]any{
						"id":     "evt_1",
						"route":  "/r",
						"target": "pull",
					},
				},
				"unknown": true,
			},
		},
		{
			name: "messages_cancel_by_filter",
			tool: "messages_cancel_by_filter",
			args: map[string]any{
				"reason":  "test",
				"unknown": true,
			},
		},
		{
			name: "messages_requeue_by_filter",
			tool: "messages_requeue_by_filter",
			args: map[string]any{
				"reason":  "test",
				"unknown": true,
			},
		},
		{
			name: "messages_resume_by_filter",
			tool: "messages_resume_by_filter",
			args: map[string]any{
				"reason":  "test",
				"unknown": true,
			},
		},
		{
			name: "instance_start",
			tool: "instance_start",
			args: map[string]any{
				"timeout": "1s",
				"unknown": true,
			},
		},
		{
			name: "instance_stop",
			tool: "instance_stop",
			args: map[string]any{
				"timeout": "1s",
				"unknown": true,
			},
		},
		{
			name: "instance_reload",
			tool: "instance_reload",
			args: map[string]any{
				"timeout": "1s",
				"unknown": true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := callTool(t, s, tc.tool, tc.args)
			if !resp.IsError {
				t.Fatalf("expected unknown-key error for %s", tc.tool)
			}
			if len(resp.Content) == 0 || !strings.Contains(resp.Content[0].Text, `arguments contains unknown key "unknown"`) {
				t.Fatalf("expected unknown key error text, got %#v", resp.Content)
			}
		})
	}
}

func TestToolMessagesCancelByFilterManagedEndpoint(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	if err := os.WriteFile(cfgPath, []byte(`
pull_api { auth token "raw:t" }
"/r" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e" }
}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(queue.Envelope{ID: "evt_mf_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_mf_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_mf_2", Route: "/other", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_mf_2: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, cfgPath, dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	cancelResp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"application":   "billing",
		"endpoint_name": "invoice.created",
		"state":         "queued",
		"limit":         10,
		"reason":        "operator_cleanup",
	})
	if cancelResp.IsError {
		t.Fatalf("unexpected cancel by filter error: %s", cancelResp.Content[0].Text)
	}
	out, ok := cancelResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", cancelResp.StructuredContent)
	}
	if canceled := intFromAny(out["canceled"]); canceled != 1 {
		t.Fatalf("expected canceled=1, got %#v", out["canceled"])
	}

	listResp := callTool(t, s, "messages_list", map[string]any{
		"route": "/other",
		"state": "queued",
		"limit": 10,
	})
	if listResp.IsError {
		t.Fatalf("unexpected list error: %s", listResp.Content[0].Text)
	}
	listOut, ok := listResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", listResp.StructuredContent)
	}
	items, ok := listOut["items"].([]map[string]any)
	if !ok || len(items) != 1 || items[0]["id"] != "evt_mf_2" {
		t.Fatalf("expected only evt_mf_2 queued, got %#v", listOut["items"])
	}
}

func TestToolMessagesFilterManagedSelectorBadRequest(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"application": "billing",
		"state":       "queued",
		"reason":      "operator_cleanup",
	})
	if !resp.IsError {
		t.Fatalf("expected partial managed selector error")
	}
}

func TestToolMessagesRequeueByFilter(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(queue.Envelope{ID: "evt_d", Route: "/r", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_d: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	requeueResp := callTool(t, s, "messages_requeue_by_filter", map[string]any{
		"route":  "/r",
		"state":  "dead",
		"limit":  10,
		"reason": "operator_cleanup",
	})
	if requeueResp.IsError {
		t.Fatalf("unexpected requeue by filter error: %s", requeueResp.Content[0].Text)
	}
	out, ok := requeueResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", requeueResp.StructuredContent)
	}
	if requeued := intFromAny(out["requeued"]); requeued != 1 {
		t.Fatalf("expected requeued=1, got %#v", out["requeued"])
	}
	if matched := intFromAny(out["matched"]); matched != 1 {
		t.Fatalf("expected matched=1, got %#v", out["matched"])
	}
	if preview, _ := out["preview_only"].(bool); preview {
		t.Fatalf("expected preview_only=false")
	}
}

func TestToolMessagesCancelByFilterPreviewOnly(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_2", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_2: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_cancel_by_filter", map[string]any{
		"route":        "/r",
		"state":        "queued",
		"limit":        10,
		"preview_only": true,
		"reason":       "operator_cleanup",
	})
	if resp.IsError {
		t.Fatalf("unexpected cancel by filter preview error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if canceled := intFromAny(out["canceled"]); canceled != 0 {
		t.Fatalf("expected canceled=0, got %#v", out["canceled"])
	}
	if matched := intFromAny(out["matched"]); matched != 2 {
		t.Fatalf("expected matched=2, got %#v", out["matched"])
	}
	if preview, _ := out["preview_only"].(bool); !preview {
		t.Fatalf("expected preview_only=true")
	}

	listResp := callTool(t, s, "messages_list", map[string]any{
		"route": "/r",
		"state": "queued",
		"limit": 10,
	})
	if listResp.IsError {
		t.Fatalf("unexpected list error: %s", listResp.Content[0].Text)
	}
	listOut, ok := listResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", listResp.StructuredContent)
	}
	items, ok := listOut["items"].([]map[string]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected 2 queued items after preview, got %#v", listOut["items"])
	}
}

func TestToolMessagesRequeueByFilterPreviewOnly(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(queue.Envelope{ID: "evt_d1", Route: "/r", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_d1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_d2", Route: "/r", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_d2: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_requeue_by_filter", map[string]any{
		"route":        "/r",
		"state":        "dead",
		"limit":        10,
		"preview_only": true,
		"reason":       "operator_cleanup",
	})
	if resp.IsError {
		t.Fatalf("unexpected requeue by filter preview error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if requeued := intFromAny(out["requeued"]); requeued != 0 {
		t.Fatalf("expected requeued=0, got %#v", out["requeued"])
	}
	if matched := intFromAny(out["matched"]); matched != 2 {
		t.Fatalf("expected matched=2, got %#v", out["matched"])
	}
	if preview, _ := out["preview_only"].(bool); !preview {
		t.Fatalf("expected preview_only=true")
	}

	listResp := callTool(t, s, "messages_list", map[string]any{
		"route": "/r",
		"state": "queued",
		"limit": 10,
	})
	if listResp.IsError {
		t.Fatalf("unexpected list error: %s", listResp.Content[0].Text)
	}
	listOut, ok := listResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", listResp.StructuredContent)
	}
	items, ok := listOut["items"].([]map[string]any)
	if !ok || len(items) != 0 {
		t.Fatalf("expected 0 queued items after preview, got %#v", listOut["items"])
	}
}

func TestToolMessagesResumeByFilterPreviewOnly(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(queue.Envelope{ID: "evt_c1", Route: "/r", Target: "pull", State: queue.StateCanceled, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_c1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_c2", Route: "/r", Target: "pull", State: queue.StateCanceled, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue evt_c2: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	resp := callTool(t, s, "messages_resume_by_filter", map[string]any{
		"route":        "/r",
		"limit":        10,
		"preview_only": true,
		"reason":       "operator_cleanup",
	})
	if resp.IsError {
		t.Fatalf("unexpected resume by filter preview error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", resp.StructuredContent)
	}
	if resumed := intFromAny(out["resumed"]); resumed != 0 {
		t.Fatalf("expected resumed=0, got %#v", out["resumed"])
	}
	if matched := intFromAny(out["matched"]); matched != 2 {
		t.Fatalf("expected matched=2, got %#v", out["matched"])
	}
	if preview, _ := out["preview_only"].(bool); !preview {
		t.Fatalf("expected preview_only=true")
	}
}

func TestToolDLQRequeue(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:     "dead_1",
		Route:  "/r",
		Target: "pull",
		State:  queue.StateDead,
	}); err != nil {
		t.Fatalf("enqueue dead: %v", err)
	}
	_ = store.Close()

	s := NewServer(nil, nil, "", dbPath, WithMutationsEnabled(true), WithRole(RoleAdmin), WithPrincipal("test-admin"))
	requeueResp := callTool(t, s, "dlq_requeue", map[string]any{
		"ids":    []any{"dead_1"},
		"reason": "operator_cleanup",
	})
	if requeueResp.IsError {
		t.Fatalf("unexpected requeue error: %s", requeueResp.Content[0].Text)
	}

	listResp := callTool(t, s, "dlq_list", map[string]any{
		"limit": 10,
	})
	if listResp.IsError {
		t.Fatalf("unexpected dlq list error: %s", listResp.Content[0].Text)
	}
	out, ok := listResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content type %T", listResp.StructuredContent)
	}
	items, ok := out["items"].([]map[string]any)
	if !ok {
		t.Fatalf("items type: %T", out["items"])
	}
	if len(items) != 0 {
		t.Fatalf("expected empty DLQ, got %#v", items)
	}
}

func TestToolInstanceRuntimeControlLifecycle(t *testing.T) {
	bin := buildHookaidoBinary(t)
	tmp := t.TempDir()

	ingressListen := reserveLocalAddr(t)
	pullListen := reserveLocalAddr(t)
	adminListen := reserveLocalAddr(t)
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	dbPath := filepath.Join(tmp, "hookaido.db")
	pidFile := filepath.Join(tmp, "hookaido.pid")

	cfg := fmt.Sprintf(`ingress { listen %q }
pull_api {
  listen %q
  auth token "raw:pulltoken"
}
admin_api { listen %q }
"/r" {
  pull { path "/pull/r" }
}
`, ingressListen, pullListen, adminListen)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(
		nil,
		nil,
		cfgPath,
		dbPath,
		WithRuntimeControlEnabled(true),
		WithRole(RoleAdmin),
		WithPrincipal("test-admin"),
		WithRuntimeControlPIDFile(pidFile),
		WithRuntimeControlRunBinary(bin),
		WithRuntimeControlRunWatch(true),
		WithRuntimeControlRunLogLevel("info"),
	)

	stopInstance := func() {
		_ = callTool(t, s, "instance_stop", map[string]any{
			"timeout": "4s",
			"force":   true,
		})
	}
	t.Cleanup(stopInstance)

	startResp := callTool(t, s, "instance_start", map[string]any{
		"timeout": "8s",
	})
	if startResp.IsError {
		t.Fatalf("unexpected instance_start error: %s", startResp.Content[0].Text)
	}
	startOut, ok := startResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", startResp.StructuredContent)
	}
	if okVal, _ := startOut["ok"].(bool); !okVal {
		t.Fatalf("expected start ok=true, got %#v", startOut["ok"])
	}
	if started, _ := startOut["started"].(bool); !started {
		t.Fatalf("expected started=true")
	}
	pid := intFromAny(startOut["pid"])
	if pid <= 0 {
		t.Fatalf("expected valid pid, got %#v", startOut["pid"])
	}

	reloadResp := callTool(t, s, "instance_reload", map[string]any{
		"timeout": "5s",
	})
	if runtime.GOOS == "windows" {
		if !reloadResp.IsError {
			t.Fatalf("expected instance_reload to report unsupported signal on windows")
		}
		if len(reloadResp.Content) == 0 || !strings.Contains(reloadResp.Content[0].Text, "not supported on Windows") {
			t.Fatalf("expected windows unsupported reload signal error, got %#v", reloadResp.Content)
		}
	} else {
		if reloadResp.IsError {
			t.Fatalf("unexpected instance_reload error: %s", reloadResp.Content[0].Text)
		}
		reloadOut, ok := reloadResp.StructuredContent.(map[string]any)
		if !ok {
			t.Fatalf("structured content type: %T", reloadResp.StructuredContent)
		}
		if okVal, _ := reloadOut["ok"].(bool); !okVal {
			t.Fatalf("expected reload ok=true, got %#v", reloadOut["ok"])
		}
		if reloaded, _ := reloadOut["reloaded"].(bool); !reloaded {
			t.Fatalf("expected reloaded=true")
		}
	}

	stopResp := callTool(t, s, "instance_stop", map[string]any{
		"timeout": "8s",
	})
	if stopResp.IsError {
		t.Fatalf("unexpected instance_stop error: %s", stopResp.Content[0].Text)
	}
	stopOut, ok := stopResp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", stopResp.StructuredContent)
	}
	if okVal, _ := stopOut["ok"].(bool); !okVal {
		t.Fatalf("expected stop ok=true, got %#v", stopOut["ok"])
	}
	if stopped, _ := stopOut["stopped"].(bool); !stopped {
		t.Fatalf("expected stopped=true")
	}
}

func TestToolInstanceStatusNotRunning(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	pidFile := filepath.Join(tmp, "hookaido.pid")
	cfg := `"/r" {
  deliver "https://example.com" {}
}
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := NewServer(
		nil,
		nil,
		cfgPath,
		filepath.Join(tmp, "hookaido.db"),
		WithRuntimeControlEnabled(true),
		WithRole(RoleOperate),
		WithRuntimeControlPIDFile(pidFile),
		WithRuntimeControlRunBinary("/bin/true"),
	)
	resp := callTool(t, s, "instance_status", map[string]any{
		"timeout": "500ms",
	})
	if resp.IsError {
		t.Fatalf("unexpected instance_status error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if running, _ := out["running"].(bool); running {
		t.Fatalf("expected running=false")
	}
	if configOK, _ := out["config_ok"].(bool); !configOK {
		t.Fatalf("expected config_ok=true")
	}
	summary, ok := out["summary"].(map[string]any)
	if !ok {
		t.Fatalf("summary type: %T", out["summary"])
	}
	if summary["queue_backend"] != "sqlite" {
		t.Fatalf("expected queue_backend=sqlite, got %#v", summary["queue_backend"])
	}
	if v, ok := summary["publish_policy_direct_enabled"].(bool); !ok || !v {
		t.Fatalf("expected publish_policy_direct_enabled=true, got %#v", summary["publish_policy_direct_enabled"])
	}
	if v, ok := summary["publish_policy_managed_enabled"].(bool); !ok || !v {
		t.Fatalf("expected publish_policy_managed_enabled=true, got %#v", summary["publish_policy_managed_enabled"])
	}
	if v, ok := summary["publish_policy_allow_pull_routes"].(bool); !ok || !v {
		t.Fatalf("expected publish_policy_allow_pull_routes=true, got %#v", summary["publish_policy_allow_pull_routes"])
	}
	if v, ok := summary["publish_policy_allow_deliver_routes"].(bool); !ok || !v {
		t.Fatalf("expected publish_policy_allow_deliver_routes=true, got %#v", summary["publish_policy_allow_deliver_routes"])
	}
	if v, ok := summary["publish_policy_require_actor"].(bool); !ok || v {
		t.Fatalf("expected publish_policy_require_actor=false, got %#v", summary["publish_policy_require_actor"])
	}
	if v, ok := summary["publish_policy_require_request_id"].(bool); !ok || v {
		t.Fatalf("expected publish_policy_require_request_id=false, got %#v", summary["publish_policy_require_request_id"])
	}
	if v, ok := summary["publish_policy_fail_closed"].(bool); !ok || v {
		t.Fatalf("expected publish_policy_fail_closed=false, got %#v", summary["publish_policy_fail_closed"])
	}
	if got := summary["publish_policy_actor_allowlist"]; len(anyStringSlice(got)) != 0 {
		t.Fatalf("expected empty publish_policy_actor_allowlist, got %#v", got)
	}
	if got := summary["publish_policy_actor_prefixes"]; len(anyStringSlice(got)) != 0 {
		t.Fatalf("expected empty publish_policy_actor_prefixes, got %#v", got)
	}
}

func TestToolInstanceLogsTail(t *testing.T) {
	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "runtime.log")
	cfgPath := filepath.Join(tmp, "Hookaidofile")
	pidFile := filepath.Join(tmp, "hookaido.pid")

	cfg := fmt.Sprintf(`observability {
  runtime_log {
    level info
    output file
    path %q
  }
}
"/r" {
  deliver "https://example.com" {}
}
`, logPath)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.WriteFile(logPath, []byte("l1\nl2\nl3\nl4\n"), 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}

	s := NewServer(
		nil,
		nil,
		cfgPath,
		filepath.Join(tmp, "hookaido.db"),
		WithRuntimeControlEnabled(true),
		WithRole(RoleOperate),
		WithRuntimeControlPIDFile(pidFile),
		WithRuntimeControlRunBinary("/bin/true"),
	)
	resp := callTool(t, s, "instance_logs_tail", map[string]any{
		"max_lines": 2,
		"max_bytes": 4096,
	})
	if resp.IsError {
		t.Fatalf("unexpected instance_logs_tail error: %s", resp.Content[0].Text)
	}
	out, ok := resp.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content type: %T", resp.StructuredContent)
	}
	if okVal, _ := out["ok"].(bool); !okVal {
		t.Fatalf("expected ok=true, got %#v", out["ok"])
	}
	linesAny, ok := out["lines"].([]string)
	if !ok {
		linesRaw, ok := out["lines"].([]any)
		if !ok {
			t.Fatalf("unexpected lines type: %T", out["lines"])
		}
		linesAny = make([]string, 0, len(linesRaw))
		for _, v := range linesRaw {
			s, _ := v.(string)
			linesAny = append(linesAny, s)
		}
	}
	if len(linesAny) != 2 || linesAny[0] != "l3" || linesAny[1] != "l4" {
		t.Fatalf("unexpected lines: %#v", linesAny)
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

func buildHookaidoBinary(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "hookaido")
	if runtime.GOOS == "windows" {
		out += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", out, "./cmd/hookaido")
	cmd.Dir = filepath.Clean(filepath.Join("..", ".."))
	b, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build failed: %v\n%s", err, string(b))
	}
	return out
}

func reserveLocalAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve addr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func intFromAny(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(x))
		return n
	default:
		return 0
	}
}

func signalAlertsContain(v any, want string) bool {
	switch alerts := v.(type) {
	case []string:
		for _, alert := range alerts {
			if alert == want {
				return true
			}
		}
	case []any:
		for _, item := range alerts {
			s, _ := item.(string)
			if s == want {
				return true
			}
		}
	}
	return false
}

func signalOperatorActionsContain(v any, want string) bool {
	switch actions := v.(type) {
	case []any:
		for _, item := range actions {
			action, _ := item.(map[string]any)
			if id, _ := action["id"].(string); id == want {
				return true
			}
		}
	case []map[string]any:
		for _, action := range actions {
			if action["id"] == want {
				return true
			}
		}
	}
	return false
}

func anyStringSlice(v any) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			s, _ := item.(string)
			out = append(out, s)
		}
		return out
	default:
		return nil
	}
}

func callTool(t *testing.T, s *Server, name string, args map[string]any) toolsCallResult {
	t.Helper()

	params, err := json.Marshal(map[string]any{
		"name":      name,
		"arguments": args,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	resp := s.handleRequest(rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  params,
	})
	if resp == nil {
		t.Fatalf("expected response")
	}
	if resp.Error != nil {
		t.Fatalf("unexpected rpc error: %#v", resp.Error)
	}
	result, ok := resp.Result.(toolsCallResult)
	if !ok {
		t.Fatalf("unexpected result type %T", resp.Result)
	}
	return result
}
