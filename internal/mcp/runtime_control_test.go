package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

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

func TestToolRuntimeControlDisabled(t *testing.T) {
	s := NewServer(nil, nil, "", "")
	resp := callTool(t, s, "instance_status", map[string]any{})
	if !resp.IsError {
		t.Fatalf("expected error")
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
