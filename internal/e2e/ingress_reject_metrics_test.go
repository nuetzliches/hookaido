package e2e

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The route on an ingress reject has to survive the whole way from the handler
// through the runtime wiring into the scrape output. It did not: the handler
// always passed it, and the metrics hook in run.go discarded it with a `_`, so
// every reject was attributed to the instance rather than to a route. Only an
// end-to-end scrape catches that -- a unit test on the observer cannot see the
// wiring that drops the argument.
func TestBinaryE2E_IngressRejectMetricsCarryRoute(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary E2E in short mode")
	}

	ingressPort := freePort(t)
	pullPort := freePort(t)
	adminPort := freePort(t)
	metricsPort := freePort(t)
	startHookaidoWithRateLimitedRoute(t, ingressPort, pullPort, adminPort, metricsPort)

	// `rate_limit { rps 1 burst 1 }` on /webhooks/throttled: the first request is
	// admitted, the rest are rejected inside the same second.
	var rejected int
	for i := 0; i < 5; i++ {
		resp, err := http.Post(
			fmt.Sprintf("http://127.0.0.1:%d/webhooks/throttled", ingressPort),
			"application/json",
			strings.NewReader(`{"n":1}`),
		)
		if err != nil {
			t.Fatalf("ingress POST %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			rejected++
		}
	}
	if rejected == 0 {
		t.Fatalf("expected at least one 429 from the rate-limited route")
	}

	// An unmatched path: rejected before any route resolved, so there is no
	// route to attribute it to.
	resp, err := http.Post(
		fmt.Sprintf("http://127.0.0.1:%d/nope", ingressPort),
		"application/json",
		strings.NewReader(`{}`),
	)
	if err != nil {
		t.Fatalf("ingress POST /nope: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unmatched path status: got %d, want 404", resp.StatusCode)
	}

	body := scrapeMetrics(t, metricsPort)
	want := fmt.Sprintf(
		`hookaido_ingress_rejected_by_reason_total{route="/webhooks/throttled",reason="rate_limit",status="429"} %d`,
		rejected,
	)
	if !strings.Contains(body, want) {
		t.Fatalf("missing %q in scrape:\n%s", want, ingressRejectLines(body))
	}
	if !strings.Contains(body, `hookaido_ingress_rejected_by_reason_total{route="",reason="not_found",status="404"} 1`) {
		t.Fatalf("expected the unmatched path to be counted with an empty route:\n%s", ingressRejectLines(body))
	}
}

func startHookaidoWithRateLimitedRoute(t *testing.T, ingressPort, pullPort, adminPort, metricsPort int) *exec.Cmd {
	t.Helper()
	bin := ensureBinary(t)

	cfgContent := fmt.Sprintf(`ingress {
  listen "127.0.0.1:%d"
}

observability {
  metrics {
    listen "127.0.0.1:%d"
    prefix "/metrics"
  }
}

"/webhooks/throttled" {
  queue "memory"
  rate_limit { rps 1 burst 1 }
  pull {
    path "/throttled"
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
`, ingressPort, metricsPort, pullPort, binaryTestPullToken, adminPort)

	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cmd := exec.Command(bin, "run", "--config", cfgPath)
	cmd.Stdout = os.Stderr
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

	waitForHealth(t, fmt.Sprintf("http://127.0.0.1:%d/healthz", adminPort), 10*time.Second)
	return cmd
}

func scrapeMetrics(t *testing.T, port int) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scrape status: got %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	return string(body)
}

// ingressRejectLines trims a failure message down to the family under test.
func ingressRejectLines(body string) string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "hookaido_ingress_rejected_by_reason_total{") && !strings.HasSuffix(line, " 0") {
			out = append(out, line)
		}
	}
	if len(out) == 0 {
		return "(no non-zero hookaido_ingress_rejected_by_reason_total series)"
	}
	return strings.Join(out, "\n")
}
