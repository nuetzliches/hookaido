package dispatcher

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func writeScript(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestExecDeliverer_Success(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell scripts not supported on Windows")
	}
	dir := t.TempDir()
	script := writeScript(t, dir, "ok.sh", "#!/bin/sh\nexit 0\n")

	d := &ExecDeliverer{}
	res := d.Deliver(context.Background(), Delivery{
		ID:     "evt-1",
		URL:    script,
		Body:   []byte(`{"test":true}`),
		Header: http.Header{"Content-Type": {"application/json"}},
		IsExec: true,
	})

	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", res.StatusCode)
	}
}

func TestExecDeliverer_GeneralFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell scripts not supported on Windows")
	}
	dir := t.TempDir()
	script := writeScript(t, dir, "fail.sh", "#!/bin/sh\nexit 1\n")

	d := &ExecDeliverer{}
	res := d.Deliver(context.Background(), Delivery{
		URL:    script,
		IsExec: true,
	})

	if res.StatusCode != 500 {
		t.Fatalf("expected status 500, got %d", res.StatusCode)
	}
	if res.Err == nil {
		t.Fatal("expected error")
	}
}

func TestExecDeliverer_TempFail(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell scripts not supported on Windows")
	}
	dir := t.TempDir()
	script := writeScript(t, dir, "tempfail.sh", "#!/bin/sh\nexit 75\n")

	d := &ExecDeliverer{}
	res := d.Deliver(context.Background(), Delivery{
		URL:    script,
		IsExec: true,
	})

	if res.StatusCode != 503 {
		t.Fatalf("expected status 503 (EX_TEMPFAIL), got %d", res.StatusCode)
	}
}

func TestExecDeliverer_NotFound(t *testing.T) {
	d := &ExecDeliverer{}
	res := d.Deliver(context.Background(), Delivery{
		URL:    "/nonexistent/command/path",
		IsExec: true,
	})

	if !errors.Is(res.Err, ErrPolicyDenied) {
		t.Fatalf("expected ErrPolicyDenied, got %v", res.Err)
	}
}

func TestExecDeliverer_NotExecutable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission model differs on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "noexec.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	d := &ExecDeliverer{}
	res := d.Deliver(context.Background(), Delivery{
		URL:    path,
		IsExec: true,
	})

	if !errors.Is(res.Err, ErrPolicyDenied) {
		t.Fatalf("expected ErrPolicyDenied for non-executable, got %v (status=%d)", res.Err, res.StatusCode)
	}
}

func TestExecDeliverer_Timeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell scripts not supported on Windows")
	}
	dir := t.TempDir()
	// Use a script that traps signals and exits on TERM for clean timeout handling.
	script := writeScript(t, dir, "slow.sh", "#!/bin/sh\ntrap 'exit 1' TERM\nwhile true; do sleep 0.01; done\n")

	d := &ExecDeliverer{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	res := d.Deliver(ctx, Delivery{
		URL:    script,
		IsExec: true,
	})

	// Context-cancelled exec returns 408 (retriable) or 502 (signal kill).
	if res.StatusCode != 408 && res.StatusCode != 502 {
		t.Fatalf("expected status 408 or 502 (timeout/signal), got %d (err=%v)", res.StatusCode, res.Err)
	}
	if res.Err == nil {
		t.Fatal("expected error on timeout")
	}
}

func TestExecDeliverer_StdinPayload(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell scripts not supported on Windows")
	}
	dir := t.TempDir()
	outFile := filepath.Join(dir, "payload.out")
	script := writeScript(t, dir, "capture.sh", "#!/bin/sh\ncat > "+outFile+"\n")

	d := &ExecDeliverer{}
	payload := `{"event":"push","ref":"refs/heads/main"}`
	res := d.Deliver(context.Background(), Delivery{
		URL:    script,
		Body:   []byte(payload),
		IsExec: true,
	})

	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read payload output: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("payload mismatch: got %q, want %q", got, payload)
	}
}

func TestExecDeliverer_EnvVars(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell scripts not supported on Windows")
	}
	dir := t.TempDir()
	outFile := filepath.Join(dir, "env.out")
	script := writeScript(t, dir, "env.sh", "#!/bin/sh\nenv > "+outFile+"\n")

	d := &ExecDeliverer{}
	res := d.Deliver(context.Background(), Delivery{
		ID:      "evt-42",
		Route:   "/hooks/deploy",
		Target:  script,
		URL:     script,
		Body:    []byte("{}"),
		Header:  http.Header{"Content-Type": {"application/json"}, "X-GitHub-Event": {"push"}},
		IsExec:  true,
		Attempt: 3,
		ExecEnv: map[string]string{"MY_SECRET": "s3cret"},
	})

	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read env output: %v", err)
	}
	envStr := string(got)

	checks := []string{
		"HOOKAIDO_EVENT_ID=evt-42",
		"HOOKAIDO_ROUTE=/hooks/deploy",
		"HOOKAIDO_ATTEMPT=3",
		"HOOKAIDO_CONTENT_TYPE=application/json",
		"HOOKAIDO_HEADER_X_GITHUB_EVENT=push",
		"MY_SECRET=s3cret",
	}
	for _, check := range checks {
		if !strings.Contains(envStr, check) {
			t.Errorf("expected %q in env output, got:\n%s", check, envStr)
		}
	}
}

func TestExecDeliverer_StderrCapture(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell scripts not supported on Windows")
	}
	dir := t.TempDir()
	script := writeScript(t, dir, "stderr.sh", "#!/bin/sh\necho 'error details' >&2\nexit 1\n")

	d := &ExecDeliverer{MaxStderrBytes: 64}
	res := d.Deliver(context.Background(), Delivery{
		URL:    script,
		IsExec: true,
	})

	if res.StatusCode != 500 {
		t.Fatalf("expected status 500, got %d", res.StatusCode)
	}
}

func TestExecDeliverer_EmptyCommand(t *testing.T) {
	d := &ExecDeliverer{}
	res := d.Deliver(context.Background(), Delivery{
		URL:    "",
		IsExec: true,
	})

	if !errors.Is(res.Err, ErrPolicyDenied) {
		t.Fatalf("expected ErrPolicyDenied for empty command, got %v", res.Err)
	}
}

func TestExecDeliverer_SignalKill(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals not supported on Windows")
	}
	dir := t.TempDir()
	// Script kills itself with SIGTERM → Go reports ExitCode() == -1.
	script := writeScript(t, dir, "killself.sh", "#!/bin/sh\nkill -TERM $$\n")

	d := &ExecDeliverer{}
	res := d.Deliver(context.Background(), Delivery{
		URL:    script,
		IsExec: true,
	})

	// Signal kill → status 502 (retriable).
	if res.StatusCode != 502 {
		t.Fatalf("expected status 502 for signal kill, got %d (err=%v)", res.StatusCode, res.Err)
	}
}
