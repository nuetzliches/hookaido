package e2e

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------- binary build + helpers ----------

const binaryTestPullToken = "test-e2e-pull-token"

var (
	binaryOnce sync.Once
	binaryPath string
	binaryErr  error
)

func ensureBinary(t *testing.T) string {
	t.Helper()
	binaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "hookaido-e2e-bin-*")
		if err != nil {
			binaryErr = fmt.Errorf("mkdirTemp: %w", err)
			return
		}
		out := filepath.Join(dir, "hookaido")
		if runtime.GOOS == "windows" {
			out += ".exe"
		}
		cmd := exec.Command("go", "build", "-o", out, "./cmd/hookaido")
		cmd.Dir = filepath.Clean(filepath.Join("..", ".."))
		b, err := cmd.CombinedOutput()
		if err != nil {
			binaryErr = fmt.Errorf("go build failed: %v\n%s", err, string(b))
			return
		}
		binaryPath = out
	})
	if binaryErr != nil {
		t.Fatalf("build binary: %v", binaryErr)
	}
	t.Cleanup(func() {
		// Intentionally not removing dir here because it's shared across tests
		// via sync.Once. The OS will clean up temp dirs.
	})
	return binaryPath
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func startHookaido(t *testing.T, ingressPort, pullPort, adminPort int) *exec.Cmd {
	t.Helper()
	bin := ensureBinary(t)

	cfgContent := fmt.Sprintf(`ingress {
  listen "127.0.0.1:%d"
}

"/webhooks" {
  queue "memory"
  pull {
    path "/webhooks"
  }
}

pull_api {
  listen "127.0.0.1:%d"
  prefix "/pull"
  auth token "raw:%s"
}

admin_api {
  listen "127.0.0.1:%d"
}
`, ingressPort, pullPort, binaryTestPullToken, adminPort)

	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cmd := exec.Command(bin, "run", "--config", cfgPath)
	cmd.Stdout = os.Stderr // pipe subprocess stdout to test stderr for debugging
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start hookaido: %v", err)
	}

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	// Wait for admin health endpoint to be ready.
	adminURL := fmt.Sprintf("http://127.0.0.1:%d/healthz", adminPort)
	waitForHealth(t, adminURL, 10*time.Second)

	return cmd
}

func waitForHealth(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("health check %s did not become ready within %v", url, timeout)
}

// ---------- Binary E2E tests ----------

func TestBinaryE2E_AdminHealth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary E2E in short mode")
	}

	adminPort := freePort(t)
	ingressPort := freePort(t)
	pullPort := freePort(t)
	startHookaido(t, ingressPort, pullPort, adminPort)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz?details=1", adminPort))
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status: got %d, want 200", resp.StatusCode)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode health JSON: %v", err)
	}
	ok, _ := body["ok"].(bool)
	if !ok {
		t.Fatalf("health ok: got %v, want true", body["ok"])
	}
}

func TestBinaryE2E_IngressToPull(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary E2E in short mode")
	}

	adminPort := freePort(t)
	ingressPort := freePort(t)
	pullPort := freePort(t)
	startHookaido(t, ingressPort, pullPort, adminPort)

	// 1. POST a webhook to ingress.
	payload := `{"action":"opened","number":42}`
	resp, err := http.Post(
		fmt.Sprintf("http://127.0.0.1:%d/webhooks/events", ingressPort),
		"application/json",
		strings.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("ingress POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingress status: got %d, want 202", resp.StatusCode)
	}

	// 2. Dequeue from pull API (with auth token).
	deqBody, _ := json.Marshal(map[string]any{"batch": 1, "lease_ttl": "30s"})
	deqReq, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/pull/webhooks/dequeue", pullPort),
		bytes.NewReader(deqBody))
	deqReq.Header.Set("Content-Type", "application/json")
	deqReq.Header.Set("Authorization", "Bearer "+binaryTestPullToken)
	deqResp, err := http.DefaultClient.Do(deqReq)
	if err != nil {
		t.Fatalf("dequeue POST: %v", err)
	}
	defer deqResp.Body.Close()

	if deqResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(deqResp.Body)
		t.Fatalf("dequeue status: got %d, body: %s", deqResp.StatusCode, body)
	}

	var deq dequeueResp
	if err := json.NewDecoder(deqResp.Body).Decode(&deq); err != nil {
		t.Fatalf("decode dequeue: %v", err)
	}
	if len(deq.Items) != 1 {
		t.Fatalf("dequeue items: got %d, want 1", len(deq.Items))
	}

	decoded, err := base64.StdEncoding.DecodeString(deq.Items[0].PayloadB64)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	if string(decoded) != payload {
		t.Fatalf("payload mismatch: got %q, want %q", decoded, payload)
	}

	// 3. ACK the message (with auth token).
	ackBody, _ := json.Marshal(map[string]string{"lease_id": deq.Items[0].LeaseID})
	ackReq, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/pull/webhooks/ack", pullPort),
		bytes.NewReader(ackBody))
	ackReq.Header.Set("Content-Type", "application/json")
	ackReq.Header.Set("Authorization", "Bearer "+binaryTestPullToken)
	ackResp, err := http.DefaultClient.Do(ackReq)
	if err != nil {
		t.Fatalf("ack POST: %v", err)
	}
	ackResp.Body.Close()
	if ackResp.StatusCode != http.StatusNoContent {
		t.Fatalf("ack status: got %d, want 204", ackResp.StatusCode)
	}
}

func TestBinaryE2E_ConfigValidate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary E2E in short mode")
	}

	bin := ensureBinary(t)

	cfgContent := `ingress {
  listen "127.0.0.1:8080"
}

"/hooks" {
  queue "memory"
  pull {
    path "/hooks"
  }
}

pull_api {
  listen "127.0.0.1:9443"
  prefix "/pull"
  auth token "raw:validate-token"
}

admin_api {
  listen "127.0.0.1:2019"
}
`
	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cmd := exec.Command(bin, "config", "validate", "--config", cfgPath, "--format", "text")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("config validate failed (exit code %v): %s", err, string(out))
	}
}

func TestBinaryE2E_ConfigFmtIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary E2E in short mode")
	}

	bin := ensureBinary(t)

	cfgContent := `ingress {
  listen "127.0.0.1:8080"
}

"/hooks" {
  queue "memory"
  pull {
    path "/hooks"
  }
}

pull_api {
  listen "127.0.0.1:9443"
  prefix "/pull"
  auth token "raw:fmt-token"
}

admin_api {
  listen "127.0.0.1:2019"
}
`
	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// First fmt pass.
	cmd1 := exec.Command(bin, "config", "fmt", "--config", cfgPath)
	out1, err := cmd1.Output()
	if err != nil {
		t.Fatalf("first config fmt failed: %v", err)
	}

	// Write the formatted output back as a new config and fmt again.
	cfgPath2 := filepath.Join(cfgDir, "Hookaidofile2")
	if err := os.WriteFile(cfgPath2, out1, 0o644); err != nil {
		t.Fatalf("write formatted config: %v", err)
	}

	cmd2 := exec.Command(bin, "config", "fmt", "--config", cfgPath2)
	out2, err := cmd2.Output()
	if err != nil {
		t.Fatalf("second config fmt failed: %v", err)
	}

	if !bytes.Equal(out1, out2) {
		t.Fatalf("config fmt not idempotent:\n--- first ---\n%s\n--- second ---\n%s", out1, out2)
	}
}

func TestBinaryE2E_RejectInvalidConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary E2E in short mode")
	}

	bin := ensureBinary(t)

	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(`this is not valid config {{{`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cmd := exec.Command(bin, "run", "--config", cfgPath)
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected non-zero exit code for invalid config, got success")
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected *exec.ExitError, got %T: %v", err, err)
	}
	if exitErr.ExitCode() == 0 {
		t.Fatal("expected non-zero exit code for invalid config")
	}
}
