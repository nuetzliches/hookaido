package app

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A rejected route can now be named. Before this, `rate_limit` said only that
// *something* on the instance was being throttled -- on a multi-route ingress
// that is the same blind spot the per-queue backlog gauges fixed for the queue
// side, and the same one #295 fixed for auth.
func TestMetricsHandler_IngressRejectCarriesRoute(t *testing.T) {
	m := newRuntimeMetrics()
	m.observeIngressReject("/webhooks/github", http.StatusTooManyRequests, "rate_limit")
	m.observeIngressReject("/webhooks/github", http.StatusTooManyRequests, "rate_limit")
	m.observeIngressReject("/webhooks/stripe", http.StatusServiceUnavailable, "queue_full")
	m.observeIngressReject("/webhooks/stripe", http.StatusServiceUnavailable, "adaptive_backpressure")
	m.observeIngressReject("/webhooks/stripe", http.StatusRequestEntityTooLarge, "policy")
	// Pre-resolution: no route exists to attribute an unmatched path to.
	m.observeIngressReject("", http.StatusNotFound, "not_found")

	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_ingress_rejected_by_reason_total{route="/webhooks/github",reason="rate_limit",status="429"} 2`,
		`hookaido_ingress_rejected_by_reason_total{route="/webhooks/stripe",reason="queue_full",status="503"} 1`,
		`hookaido_ingress_rejected_by_reason_total{route="/webhooks/stripe",reason="adaptive_backpressure",status="503"} 1`,
		`hookaido_ingress_rejected_by_reason_total{route="/webhooks/stripe",reason="policy",status="413"} 1`,
		`hookaido_ingress_rejected_by_reason_total{route="",reason="not_found",status="404"} 1`,
		// The zero-filled reason/status baseline keeps the identity it had
		// before the label existed: Prometheus does not distinguish an empty
		// label value from an absent one, so existing rules still select it.
		`hookaido_ingress_rejected_by_reason_total{route="",reason="memory_pressure",status="503"} 0`,
		`hookaido_ingress_rejected_by_reason_total{route="",reason="rate_limit",status="429"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}

	// No route may be zero-filled: routes come and go with the config, and
	// pre-seeding every route against every reason would emit series for
	// combinations that cannot occur.
	for _, unwanted := range []string{
		`hookaido_ingress_rejected_by_reason_total{route="/webhooks/github",reason="queue_full"`,
		`hookaido_ingress_rejected_by_reason_total{route="/webhooks/stripe",reason="rate_limit"`,
	} {
		if strings.Contains(body, unwanted) {
			t.Fatalf("unexpected zero-filled series %q in metrics output:\n%s", unwanted, body)
		}
	}

	// And the arithmetic has to close: every reject is counted under exactly
	// one (route, reason, status) triple, so summing the family reproduces
	// hookaido_ingress_rejected_total.
	var sum int64
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "hookaido_ingress_rejected_by_reason_total{") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("malformed series line %q", line)
		}
		var n int64
		if _, err := fmt.Sscanf(fields[1], "%d", &n); err != nil {
			t.Fatalf("series line %q has a non-integer value: %v", line, err)
		}
		sum += n
	}
	if sum != 6 {
		t.Fatalf("family sums to %d, want 6 (= hookaido_ingress_rejected_total)", sum)
	}
	if !strings.Contains(body, "hookaido_ingress_rejected_total 6") {
		t.Fatalf("expected hookaido_ingress_rejected_total 6 in metrics output:\n%s", body)
	}
}

// The health payload keeps the route dimension out and stays exactly the shape
// it was: a JSON document polled on a short interval is the wrong place to pay
// for a label, and existing consumers (including the MCP admin proxy) read it.
func TestRuntimeMetrics_HealthDiagnosticsRejectsFoldAcrossRoutes(t *testing.T) {
	m := newRuntimeMetrics()
	m.observeIngressReject("/a", http.StatusTooManyRequests, "rate_limit")
	m.observeIngressReject("/b", http.StatusTooManyRequests, "rate_limit")
	m.observeIngressReject("", http.StatusTooManyRequests, "rate_limit")
	m.observeIngressReject("/a", http.StatusServiceUnavailable, "queue_full")

	diag := m.healthDiagnostics()
	ingressDiag, ok := diag["ingress"].(map[string]any)
	if !ok {
		t.Fatalf("expected ingress diagnostics object, got %T", diag["ingress"])
	}
	byReason, ok := ingressDiag["rejected_by_reason"].(map[string]any)
	if !ok {
		t.Fatalf("expected rejected_by_reason map, got %T", ingressDiag["rejected_by_reason"])
	}
	// Three routes' worth of rate_limit rejects, one number.
	if got := intFromAny(byReason["rate_limit"]); got != 3 {
		t.Fatalf("rejected_by_reason.rate_limit = %v, want 3 folded across routes", byReason["rate_limit"])
	}
	if got := intFromAny(byReason["queue_full"]); got != 1 {
		t.Fatalf("rejected_by_reason.queue_full = %v, want 1", byReason["queue_full"])
	}
	if got := intFromAny(byReason["not_found"]); got != 0 {
		t.Fatalf("rejected_by_reason.not_found = %v, want a zero-filled 0", byReason["not_found"])
	}
	for _, key := range []string{"route", "by_route", "rejected_by_route"} {
		if _, present := ingressDiag[key]; present {
			t.Fatalf("health payload gained a route dimension under %q: %#v", key, ingressDiag)
		}
	}
}
