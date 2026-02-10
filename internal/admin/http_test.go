package admin

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

func TestServer_Healthz(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/healthz", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if rr.Body.String() != "ok\n" {
		t.Fatalf("unexpected body: %q", rr.Body.String())
	}
}

func TestServer_HealthzInvalidDetailsStructuredError(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/healthz?details=maybe", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != readCodeInvalidQuery {
		t.Fatalf("expected code=%q, got %q", readCodeInvalidQuery, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "details must be true|false") {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_MethodNotAllowedStructuredError(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "http://example/dlq", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != readCodeMethodNotAllowed {
		t.Fatalf("expected code=%q, got %q", readCodeMethodNotAllowed, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, http.MethodGet) {
		t.Fatalf("expected detail to mention expected method GET, got %q", errResp.Detail)
	}
}

func TestServer_NotFoundStructuredError(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/does-not-exist", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != readCodeNotFound {
		t.Fatalf("expected code=%q, got %q", readCodeNotFound, errResp.Code)
	}
}

func TestServer_ApplicationResourceNotFoundStructuredError(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/unknown", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != readCodeNotFound {
		t.Fatalf("expected code=%q, got %q", readCodeNotFound, errResp.Code)
	}
}

func TestServer_HealthzDetails(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{
		ID:         "q_1",
		Route:      "/r",
		Target:     "pull",
		State:      queue.StateQueued,
		ReceivedAt: now.Add(-2 * time.Minute),
		NextRunAt:  now.Add(-30 * time.Second),
	}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := store.CaptureBacklogTrendSample(nowVar); err != nil {
		t.Fatalf("capture trend sample: %v", err)
	}

	srv := NewServer(store)
	srv.HealthDiagnostics = func() map[string]any {
		return map[string]any{
			"tracing": map[string]any{
				"enabled":             true,
				"init_failures_total": 1,
				"export_errors_total": 2,
			},
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/healthz?details=1", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var out map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if ok, _ := out["ok"].(bool); !ok {
		t.Fatalf("expected ok=true")
	}
	diagnostics, ok := out["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected diagnostics object, got %T", out["diagnostics"])
	}
	if _, ok := diagnostics["tracing"]; !ok {
		t.Fatalf("expected tracing diagnostics")
	}
	queueDiag, ok := diagnostics["queue"].(map[string]any)
	if !ok {
		t.Fatalf("expected queue diagnostics, got %T", diagnostics["queue"])
	}
	if okVal, _ := queueDiag["ok"].(bool); !okVal {
		t.Fatalf("expected queue diagnostics ok=true, got %#v", queueDiag)
	}
	if total, _ := queueDiag["total"].(float64); int(total) != 1 {
		t.Fatalf("expected queue total=1, got %#v", queueDiag["total"])
	}
	if age, _ := queueDiag["oldest_queued_age_seconds"].(float64); int(age) != 120 {
		t.Fatalf("expected oldest_queued_age_seconds=120, got %#v", queueDiag["oldest_queued_age_seconds"])
	}
	if lag, _ := queueDiag["ready_lag_seconds"].(float64); int(lag) != 30 {
		t.Fatalf("expected ready_lag_seconds=30, got %#v", queueDiag["ready_lag_seconds"])
	}
	if ts, _ := queueDiag["oldest_queued_received_at"].(string); ts == "" {
		t.Fatalf("expected oldest_queued_received_at")
	}
	if ts, _ := queueDiag["earliest_queued_next_run_at"].(string); ts == "" {
		t.Fatalf("expected earliest_queued_next_run_at")
	}
	topQueued, ok := queueDiag["top_queued"].([]any)
	if !ok || len(topQueued) != 1 {
		t.Fatalf("expected top_queued with 1 bucket, got %#v", queueDiag["top_queued"])
	}
	first, ok := topQueued[0].(map[string]any)
	if !ok {
		t.Fatalf("expected top_queued item object, got %T", topQueued[0])
	}
	if route, _ := first["route"].(string); route != "/r" {
		t.Fatalf("expected top_queued route=/r, got %#v", first["route"])
	}
	trendSignals, ok := queueDiag["trend_signals"].(map[string]any)
	if !ok {
		t.Fatalf("expected trend_signals object, got %T", queueDiag["trend_signals"])
	}
	if sampleCount, _ := trendSignals["sample_count"].(float64); int(sampleCount) < 1 {
		t.Fatalf("expected trend_signals.sample_count >= 1, got %#v", trendSignals["sample_count"])
	}
	if _, ok := trendSignals["active_alerts"]; !ok {
		t.Fatalf("expected trend_signals.active_alerts")
	}
	if _, ok := trendSignals["operator_actions"]; !ok {
		t.Fatalf("expected trend_signals.operator_actions")
	}
}

func TestBacklogTrendsFromSamples_UsesTrendSignalConfig(t *testing.T) {
	until := time.Date(2026, 2, 7, 12, 2, 0, 0, time.UTC)
	window := 2 * time.Minute
	since := until.Add(-window)
	step := time.Minute
	samples := []queue.BacklogTrendSample{
		{CapturedAt: since, Queued: 10, Leased: 0, Dead: 0},
		{CapturedAt: since.Add(step), Queued: 40, Leased: 0, Dead: 0},
	}

	defaultOut := backlogTrendsFromSamples(since, until, window, step, "", "", false, samples, queue.DefaultBacklogTrendSignalConfig())
	if surge, _ := defaultOut.Signals["recent_surge"].(bool); !surge {
		t.Fatalf("expected recent_surge with default thresholds, got %#v", defaultOut.Signals)
	}
	if !signalAlertsContain(defaultOut.Signals["active_alerts"], "recent_surge") {
		t.Fatalf("expected active_alerts to contain recent_surge, got %#v", defaultOut.Signals["active_alerts"])
	}
	if !signalOperatorActionsContain(defaultOut.Signals["operator_actions"], "backlog_growth_triage") {
		t.Fatalf("expected operator_actions to contain backlog_growth_triage, got %#v", defaultOut.Signals["operator_actions"])
	}

	customCfg := queue.DefaultBacklogTrendSignalConfig()
	customCfg.RecentSurgeMinTotal = 100
	customCfg.RecentSurgeMinDelta = 100
	customCfg.RecentSurgePercent = 300
	customCfg.QueuedPressureMinTotal = 100
	customOut := backlogTrendsFromSamples(since, until, window, step, "", "", false, samples, customCfg)
	if surge, _ := customOut.Signals["recent_surge"].(bool); surge {
		t.Fatalf("expected recent_surge=false with custom thresholds, got %#v", customOut.Signals)
	}
	if signalAlertsContain(customOut.Signals["active_alerts"], "recent_surge") {
		t.Fatalf("did not expect recent_surge in active_alerts, got %#v", customOut.Signals["active_alerts"])
	}
	if signalOperatorActionsContain(customOut.Signals["operator_actions"], "backlog_growth_triage") {
		t.Fatalf("did not expect backlog_growth_triage in operator_actions, got %#v", customOut.Signals["operator_actions"])
	}
}

func TestServer_ManagementModel(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       3,
			ApplicationCount: 1,
			EndpointCount:    2,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 2,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/billing/invoice",
							Mode:    "pull",
							Targets: []string{"pull"},
							PublishPolicy: ManagementEndpointPublishPolicy{
								Enabled:        true,
								DirectEnabled:  false,
								ManagedEnabled: true,
							},
						},
						{
							Name:    "invoice.sent",
							Route:   "/billing/invoice/sent",
							Mode:    "deliver",
							Targets: []string{"https://example.org/hook"},
							PublishPolicy: ManagementEndpointPublishPolicy{
								Enabled:        true,
								DirectEnabled:  true,
								ManagedEnabled: false,
							},
						},
					},
				},
			},
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/management/model", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var out ManagementModel
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.RouteCount != 3 || out.ApplicationCount != 1 || out.EndpointCount != 2 {
		t.Fatalf("unexpected top-level counts: %#v", out)
	}
	if len(out.Applications) != 1 || out.Applications[0].Name != "billing" {
		t.Fatalf("unexpected applications: %#v", out.Applications)
	}
	if len(out.Applications[0].Endpoints) != 2 {
		t.Fatalf("unexpected endpoints: %#v", out.Applications[0].Endpoints)
	}
	if out.Applications[0].Endpoints[0].PublishPolicy.DirectEnabled {
		t.Fatalf("expected first endpoint direct publish disabled, got %#v", out.Applications[0].Endpoints[0].PublishPolicy)
	}
	if out.Applications[0].Endpoints[1].PublishPolicy.ManagedEnabled {
		t.Fatalf("expected second endpoint managed publish disabled, got %#v", out.Applications[0].Endpoints[1].PublishPolicy)
	}
}

func TestServer_ManagementModelUnavailable(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	req := httptest.NewRequest(http.MethodGet, "http://example/management/model", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != readCodeManagementUnavailable {
		t.Fatalf("expected code=%q, got %q", readCodeManagementUnavailable, errResp.Code)
	}
}

func TestServer_ListApplications(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 2,
					Endpoints: []ManagementEndpoint{
						{Name: "invoice.created", Route: "/a", Mode: "pull", Targets: []string{"pull"}},
						{Name: "invoice.sent", Route: "/b", Mode: "deliver", Targets: []string{"https://example.org/a"}},
					},
				},
				{
					Name:          "erp",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{Name: "stock.updated", Route: "/c", Mode: "deliver", Targets: []string{"https://example.org/b"}},
					},
				},
			},
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/applications", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var out applicationsResponse
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.ApplicationCount != 2 || out.EndpointCount != 3 {
		t.Fatalf("unexpected counts: %#v", out)
	}
	if len(out.Items) != 2 {
		t.Fatalf("expected 2 applications, got %#v", out.Items)
	}
}

func TestServer_ListApplicationEndpoints(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			Applications: []ManagementApplication{
				{
					Name:          "billing-core",
					EndpointCount: 2,
					Endpoints: []ManagementEndpoint{
						{Name: "invoice.created", Route: "/a", Mode: "pull", Targets: []string{"pull"}},
						{Name: "invoice.sent", Route: "/b", Mode: "deliver", Targets: []string{"https://example.org/a"}},
					},
				},
			},
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/applications/billing-core/endpoints", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var out applicationEndpointsResponse
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Application != "billing-core" {
		t.Fatalf("expected application billing-core, got %#v", out.Application)
	}
	if out.EndpointCount != 2 || len(out.Items) != 2 {
		t.Fatalf("unexpected endpoint payload: %#v", out)
	}
}

func TestServer_GetApplicationEndpoint(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			Applications: []ManagementApplication{
				{
					Name: "billing-core",
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/a",
							Mode:    "pull",
							Targets: []string{"pull"},
							PublishPolicy: ManagementEndpointPublishPolicy{
								Enabled:        true,
								DirectEnabled:  false,
								ManagedEnabled: true,
							},
						},
					},
				},
			},
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/applications/billing-core/endpoints/invoice.created", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var out applicationEndpointResponse
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Application != "billing-core" || out.EndpointName != "invoice.created" || out.Route != "/a" {
		t.Fatalf("unexpected endpoint response: %#v", out)
	}
	if out.PublishPolicy.DirectEnabled {
		t.Fatalf("expected direct publish disabled in endpoint response, got %#v", out.PublishPolicy)
	}
}

func TestServer_ApplicationResourceRejectsInvalidApplicationPathSegment(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	req := httptest.NewRequest(http.MethodGet, "http://example/applications/billing%20core/endpoints", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "application path segment must match") {
		t.Fatalf("expected application path-segment validation detail, got %q", errResp.Detail)
	}
}

func TestServer_ApplicationResourceRejectsInvalidEndpointPathSegment(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	req := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/endpoints/invoice%20created", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "endpoint_name path segment must match") {
		t.Fatalf("expected endpoint_name path-segment validation detail, got %q", errResp.Detail)
	}
}

func TestServer_ApplicationEndpointUpsert(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			Applications: []ManagementApplication{
				{
					Name: "billing",
					Endpoints: []ManagementEndpoint{
						{Name: "invoice.created", Route: "/billing/invoice", Mode: "pull", Targets: []string{"pull"}},
					},
				},
			},
		}
	}

	var gotReq ManagementEndpointUpsertRequest
	srv.UpsertManagedEndpoint = func(req ManagementEndpointUpsertRequest) (ManagementEndpointMutationResult, error) {
		gotReq = req
		return ManagementEndpointMutationResult{
			Applied: true,
			Action:  "created",
			Route:   "/billing/invoice",
		}, nil
	}

	req := httptest.NewRequest(http.MethodPut, "http://example/applications/billing/endpoints/invoice.created", strings.NewReader(`{
  "route": "/billing/invoice"
}`))
	req.Header.Set(auditReasonHeader, "oncall_fix")
	req.Header.Set(auditActorHeader, "ops@example.test")
	req.Header.Set(auditRequestIDHeader, "req-upsert-1")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var out applicationEndpointMutationResponse
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !out.Applied || out.Action != "created" {
		t.Fatalf("unexpected mutation response: %#v", out)
	}
	if out.Endpoint == nil || out.Endpoint.Route != "/billing/invoice" || out.Endpoint.Mode != "pull" {
		t.Fatalf("unexpected endpoint projection: %#v", out.Endpoint)
	}
	if out.Scope == nil || out.Scope.Application != "billing" || out.Scope.EndpointName != "invoice.created" || out.Scope.Route != "/billing/invoice" {
		t.Fatalf("unexpected scope: %#v", out.Scope)
	}
	if out.Audit == nil || out.Audit.Reason != "oncall_fix" || out.Audit.Actor != "ops@example.test" || out.Audit.RequestID != "req-upsert-1" {
		t.Fatalf("unexpected audit payload: %#v", out.Audit)
	}

	if gotReq.Application != "billing" || gotReq.EndpointName != "invoice.created" || gotReq.Route != "/billing/invoice" {
		t.Fatalf("unexpected callback request: %#v", gotReq)
	}
	if gotReq.Reason != "oncall_fix" || gotReq.Actor != "ops@example.test" || gotReq.RequestID != "req-upsert-1" {
		t.Fatalf("unexpected callback audit: %#v", gotReq)
	}
}

func TestServer_ApplicationEndpointUpsertAuditHook(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.UpsertManagedEndpoint = func(req ManagementEndpointUpsertRequest) (ManagementEndpointMutationResult, error) {
		return ManagementEndpointMutationResult{
			Applied: true,
			Action:  "created",
			Route:   "/billing/invoice",
		}, nil
	}

	var got ManagementMutationAuditEvent
	srv.AuditManagementMutation = func(event ManagementMutationAuditEvent) {
		got = event
	}

	req := httptest.NewRequest(http.MethodPut, "http://example/applications/billing/endpoints/invoice.created", strings.NewReader(`{"route":"/billing/invoice"}`))
	req.Header.Set(auditReasonHeader, "oncall_fix")
	req.Header.Set(auditActorHeader, "ops@example.test")
	req.Header.Set(auditRequestIDHeader, "req-upsert-2")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if got.Operation != "upsert_endpoint" || got.Application != "billing" || got.EndpointName != "invoice.created" || got.Route != "/billing/invoice" {
		t.Fatalf("unexpected audit event scope: %#v", got)
	}
	if got.Reason != "oncall_fix" || got.Actor != "ops@example.test" || got.RequestID != "req-upsert-2" {
		t.Fatalf("unexpected audit metadata: %#v", got)
	}
	if got.Changed != 1 || got.Matched != 1 {
		t.Fatalf("unexpected audit counters: %#v", got)
	}
}

func TestServer_ApplicationEndpointUpsertBadRequest(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.UpsertManagedEndpoint = func(req ManagementEndpointUpsertRequest) (ManagementEndpointMutationResult, error) {
		t.Fatalf("upsert callback should not be called on bad request")
		return ManagementEndpointMutationResult{}, nil
	}

	req := httptest.NewRequest(http.MethodPut, "http://example/applications/billing/endpoints/invoice.created", strings.NewReader(`{}`))
	req.Header.Set(auditReasonHeader, "oncall_fix")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
	}
}

func TestServer_ApplicationEndpointUpsertRejectsNonAbsoluteRoute(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.UpsertManagedEndpoint = func(req ManagementEndpointUpsertRequest) (ManagementEndpointMutationResult, error) {
		t.Fatalf("upsert callback should not be called on invalid route")
		return ManagementEndpointMutationResult{}, nil
	}

	req := httptest.NewRequest(http.MethodPut, "http://example/applications/billing/endpoints/invoice.created", strings.NewReader(`{"route":"billing/invoice"}`))
	req.Header.Set(auditReasonHeader, "oncall_fix")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "route must start with '/'") {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_ApplicationEndpointUpsertRejectsTrailingJSONBody(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.UpsertManagedEndpoint = func(req ManagementEndpointUpsertRequest) (ManagementEndpointMutationResult, error) {
		t.Fatalf("upsert callback should not be called on bad request")
		return ManagementEndpointMutationResult{}, nil
	}

	req := httptest.NewRequest(http.MethodPut, "http://example/applications/billing/endpoints/invoice.created", strings.NewReader("{\"route\":\"/billing/invoice\"}\n{}"))
	req.Header.Set(auditReasonHeader, "oncall_fix")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
	}
}

func TestServer_ApplicationEndpointUpsertRequiresAuditReason(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.UpsertManagedEndpoint = func(req ManagementEndpointUpsertRequest) (ManagementEndpointMutationResult, error) {
		t.Fatalf("upsert callback should not be called when audit reason is missing")
		return ManagementEndpointMutationResult{}, nil
	}

	req := httptest.NewRequest(http.MethodPut, "http://example/applications/billing/endpoints/invoice.created", strings.NewReader(`{"route":"/billing/invoice"}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditReasonRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditReasonRequired, errResp.Code)
	}
}

func TestServer_ApplicationEndpointUpsertEnforcesScopedAuditIdentityPolicy(t *testing.T) {
	tests := []struct {
		name          string
		configure     func(*Server)
		headers       map[string]string
		wantCode      string
		wantDetailSub string
	}{
		{
			name: "require_actor",
			configure: func(s *Server) {
				s.PublishRequireAuditActor = true
			},
			headers: map[string]string{
				auditReasonHeader: "oncall_fix",
			},
			wantCode:      publishCodeAuditActorRequired,
			wantDetailSub: "defaults.publish_policy.require_actor",
		},
		{
			name: "require_request_id",
			configure: func(s *Server) {
				s.PublishRequireAuditRequestID = true
			},
			headers: map[string]string{
				auditReasonHeader: "oncall_fix",
				auditActorHeader:  "ops@example.test",
			},
			wantCode:      publishCodeAuditRequestIDRequired,
			wantDetailSub: "defaults.publish_policy.require_request_id",
		},
		{
			name: "scoped_actor_not_allowed",
			configure: func(s *Server) {
				s.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
				s.PublishScopedManagedActorPrefixes = []string{"role:ops/"}
			},
			headers: map[string]string{
				auditReasonHeader: "oncall_fix",
				auditActorHeader:  "dev@example.test",
			},
			wantCode:      publishCodeAuditActorNotAllowed,
			wantDetailSub: "allowed actors:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(queue.NewMemoryStore())
			srv.UpsertManagedEndpoint = func(req ManagementEndpointUpsertRequest) (ManagementEndpointMutationResult, error) {
				t.Fatalf("upsert callback should not be called when audit identity policy blocks request")
				return ManagementEndpointMutationResult{}, nil
			}
			tc.configure(srv)

			req := httptest.NewRequest(http.MethodPut, "http://example/applications/billing/endpoints/invoice.created", strings.NewReader(`{"route":"/billing/invoice"}`))
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != tc.wantCode {
				t.Fatalf("expected code=%q, got %q", tc.wantCode, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, tc.wantDetailSub) {
				t.Fatalf("expected detail containing %q, got %q", tc.wantDetailSub, errResp.Detail)
			}
		})
	}
}

func TestServer_ApplicationEndpointUpsertErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		statusCode int
		code       string
		detailSub  string
	}{
		{
			name:       "route_not_found",
			err:        ErrManagementRouteNotFound,
			statusCode: http.StatusNotFound,
			code:       managementCodeRouteNotFound,
			detailSub:  "management route not found",
		},
		{
			name:       "conflict_route_mapped",
			err:        fmt.Errorf("%w: route \"/billing/invoice\" is already mapped to (\"erp\", \"stock.updated\")", ErrManagementConflict),
			statusCode: http.StatusConflict,
			code:       managementCodeRouteMapped,
			detailSub:  `route "/billing/invoice" is already mapped to ("erp", "stock.updated")`,
		},
		{
			name:       "conflict_route_mapped_typed",
			err:        NewManagementMutationError(ErrManagementConflict, managementCodeRouteMapped, `route "/billing/invoice" is owned by endpoint "erp.stock.updated"`),
			statusCode: http.StatusConflict,
			code:       managementCodeRouteMapped,
			detailSub:  `route "/billing/invoice" is owned by endpoint "erp.stock.updated"`,
		},
		{
			name:       "conflict_publish_disabled",
			err:        fmt.Errorf("%w: managed publish is disabled for route \"/billing/invoice\" by route.publish.managed", ErrManagementConflict),
			statusCode: http.StatusConflict,
			code:       managementCodeRoutePublishOff,
			detailSub:  `route.publish.managed`,
		},
		{
			name:       "conflict_publish_disabled_typed",
			err:        NewManagementMutationError(ErrManagementConflict, managementCodeRoutePublishOff, `managed publish is blocked by route policy for "/billing/invoice"`),
			statusCode: http.StatusConflict,
			code:       managementCodeRoutePublishOff,
			detailSub:  `managed publish is blocked by route policy`,
		},
		{
			name:       "conflict_target_profile_mismatch",
			err:        fmt.Errorf("%w: managed endpoint target profile mismatch between current route \"/billing/invoice\" (mode=pull, targets=\"pull\") and target route \"/billing/invoice_v2\" (mode=deliver, targets=\"https://example.org/hook\")", ErrManagementConflict),
			statusCode: http.StatusConflict,
			code:       managementCodeRouteTargetDrift,
			detailSub:  `managed endpoint target profile mismatch`,
		},
		{
			name:       "conflict_target_profile_mismatch_typed",
			err:        NewManagementMutationError(ErrManagementConflict, managementCodeRouteTargetDrift, `managed endpoint target profile mismatch between current route "/billing/invoice" (mode=pull, targets="pull") and target route "/billing/invoice_v2" (mode=deliver, targets="https://example.org/hook")`),
			statusCode: http.StatusConflict,
			code:       managementCodeRouteTargetDrift,
			detailSub:  `mode=pull`,
		},
		{
			name:       "conflict_route_backlog_active_typed",
			err:        NewManagementMutationError(ErrManagementConflict, managementCodeRouteBacklog, `cannot move management endpoint while route "/billing/invoice" has active backlog (queued); drain queued/leased messages first`),
			statusCode: http.StatusConflict,
			code:       managementCodeRouteBacklog,
			detailSub:  `has active backlog`,
		},
		{
			name:       "conflict_typed_unknown_code_falls_back",
			err:        NewManagementMutationError(ErrManagementConflict, "management_weird_conflict", "custom conflict detail"),
			statusCode: http.StatusConflict,
			code:       managementCodeConflict,
			detailSub:  "custom conflict detail",
		},
		{
			name:       "conflict_generic",
			err:        ErrManagementConflict,
			statusCode: http.StatusConflict,
			code:       managementCodeConflict,
			detailSub:  "management endpoint mutation conflict",
		},
		{
			name:       "internal_error",
			err:        errors.New("boom"),
			statusCode: http.StatusServiceUnavailable,
			code:       managementCodeUnavailable,
			detailSub:  "management endpoint mutation failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(queue.NewMemoryStore())
			srv.UpsertManagedEndpoint = func(req ManagementEndpointUpsertRequest) (ManagementEndpointMutationResult, error) {
				return ManagementEndpointMutationResult{}, tc.err
			}

			req := httptest.NewRequest(http.MethodPut, "http://example/applications/billing/endpoints/invoice.created", strings.NewReader(`{"route":"/billing/invoice"}`))
			req.Header.Set(auditReasonHeader, "oncall_fix")

			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != tc.statusCode {
				t.Fatalf("expected %d, got %d", tc.statusCode, rr.Code)
			}
			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != tc.code {
				t.Fatalf("expected code=%q, got %q", tc.code, errResp.Code)
			}
			if tc.detailSub != "" && !strings.Contains(errResp.Detail, tc.detailSub) {
				t.Fatalf("expected detail containing %q, got %q", tc.detailSub, errResp.Detail)
			}
		})
	}
}

func TestServer_ApplicationEndpointDelete(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	var gotReq ManagementEndpointDeleteRequest
	srv.DeleteManagedEndpoint = func(req ManagementEndpointDeleteRequest) (ManagementEndpointMutationResult, error) {
		gotReq = req
		return ManagementEndpointMutationResult{
			Applied: true,
			Action:  "deleted",
			Route:   "/billing/invoice",
		}, nil
	}

	req := httptest.NewRequest(http.MethodDelete, "http://example/applications/billing/endpoints/invoice.created", nil)
	req.Header.Set(auditReasonHeader, "cleanup")
	req.Header.Set(auditActorHeader, "ops@example.test")
	req.Header.Set(auditRequestIDHeader, "req-delete-1")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var out applicationEndpointMutationResponse
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !out.Applied || out.Action != "deleted" {
		t.Fatalf("unexpected mutation response: %#v", out)
	}
	if out.Endpoint == nil || out.Endpoint.Route != "/billing/invoice" {
		t.Fatalf("unexpected endpoint response: %#v", out.Endpoint)
	}
	if out.Scope == nil || out.Scope.Route != "/billing/invoice" {
		t.Fatalf("unexpected scope: %#v", out.Scope)
	}
	if out.Audit == nil || out.Audit.Reason != "cleanup" || out.Audit.Actor != "ops@example.test" || out.Audit.RequestID != "req-delete-1" {
		t.Fatalf("unexpected audit payload: %#v", out.Audit)
	}

	if gotReq.Application != "billing" || gotReq.EndpointName != "invoice.created" {
		t.Fatalf("unexpected callback request: %#v", gotReq)
	}
	if gotReq.Reason != "cleanup" || gotReq.Actor != "ops@example.test" || gotReq.RequestID != "req-delete-1" {
		t.Fatalf("unexpected callback audit: %#v", gotReq)
	}
}

func TestServer_ApplicationEndpointDeleteAuditHook(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.DeleteManagedEndpoint = func(req ManagementEndpointDeleteRequest) (ManagementEndpointMutationResult, error) {
		return ManagementEndpointMutationResult{
			Applied: true,
			Action:  "deleted",
			Route:   "/billing/invoice",
		}, nil
	}

	var got ManagementMutationAuditEvent
	srv.AuditManagementMutation = func(event ManagementMutationAuditEvent) {
		got = event
	}

	req := httptest.NewRequest(http.MethodDelete, "http://example/applications/billing/endpoints/invoice.created", nil)
	req.Header.Set(auditReasonHeader, "cleanup")
	req.Header.Set(auditActorHeader, "ops@example.test")
	req.Header.Set(auditRequestIDHeader, "req-delete-2")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if got.Operation != "delete_endpoint" || got.Application != "billing" || got.EndpointName != "invoice.created" || got.Route != "/billing/invoice" {
		t.Fatalf("unexpected audit event scope: %#v", got)
	}
	if got.Reason != "cleanup" || got.Actor != "ops@example.test" || got.RequestID != "req-delete-2" {
		t.Fatalf("unexpected audit metadata: %#v", got)
	}
	if got.Changed != 1 || got.Matched != 1 {
		t.Fatalf("unexpected audit counters: %#v", got)
	}
}

func TestServer_ApplicationEndpointDeleteErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		statusCode int
		code       string
	}{
		{
			name:       "endpoint_not_found",
			err:        ErrManagementEndpointNotFound,
			statusCode: http.StatusNotFound,
			code:       managementCodeEndpointNotFound,
		},
		{
			name:       "conflict",
			err:        ErrManagementConflict,
			statusCode: http.StatusConflict,
			code:       managementCodeConflict,
		},
		{
			name:       "conflict_route_backlog_active_typed",
			err:        NewManagementMutationError(ErrManagementConflict, managementCodeRouteBacklog, `cannot delete management endpoint while route "/billing/invoice" has active backlog (leased); drain queued/leased messages first`),
			statusCode: http.StatusConflict,
			code:       managementCodeRouteBacklog,
		},
		{
			name:       "internal_error",
			err:        errors.New("boom"),
			statusCode: http.StatusServiceUnavailable,
			code:       managementCodeUnavailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(queue.NewMemoryStore())
			srv.DeleteManagedEndpoint = func(req ManagementEndpointDeleteRequest) (ManagementEndpointMutationResult, error) {
				return ManagementEndpointMutationResult{}, tc.err
			}

			req := httptest.NewRequest(http.MethodDelete, "http://example/applications/billing/endpoints/invoice.created", nil)
			req.Header.Set(auditReasonHeader, "cleanup")

			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != tc.statusCode {
				t.Fatalf("expected %d, got %d", tc.statusCode, rr.Code)
			}
			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != tc.code {
				t.Fatalf("expected code=%q, got %q", tc.code, errResp.Code)
			}
		})
	}
}

func TestServer_ApplicationEndpointDeleteRequiresAuditReason(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.DeleteManagedEndpoint = func(req ManagementEndpointDeleteRequest) (ManagementEndpointMutationResult, error) {
		t.Fatalf("delete callback should not be called when audit reason is missing")
		return ManagementEndpointMutationResult{}, nil
	}

	req := httptest.NewRequest(http.MethodDelete, "http://example/applications/billing/endpoints/invoice.created", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditReasonRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditReasonRequired, errResp.Code)
	}
}

func TestServer_ApplicationEndpointDeleteEnforcesScopedAuditIdentityPolicy(t *testing.T) {
	tests := []struct {
		name          string
		configure     func(*Server)
		headers       map[string]string
		wantCode      string
		wantDetailSub string
	}{
		{
			name: "require_actor",
			configure: func(s *Server) {
				s.PublishRequireAuditActor = true
			},
			headers: map[string]string{
				auditReasonHeader: "cleanup",
			},
			wantCode:      publishCodeAuditActorRequired,
			wantDetailSub: "defaults.publish_policy.require_actor",
		},
		{
			name: "require_request_id",
			configure: func(s *Server) {
				s.PublishRequireAuditRequestID = true
			},
			headers: map[string]string{
				auditReasonHeader: "cleanup",
				auditActorHeader:  "ops@example.test",
			},
			wantCode:      publishCodeAuditRequestIDRequired,
			wantDetailSub: "defaults.publish_policy.require_request_id",
		},
		{
			name: "scoped_actor_not_allowed",
			configure: func(s *Server) {
				s.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
				s.PublishScopedManagedActorPrefixes = []string{"role:ops/"}
			},
			headers: map[string]string{
				auditReasonHeader: "cleanup",
				auditActorHeader:  "dev@example.test",
			},
			wantCode:      publishCodeAuditActorNotAllowed,
			wantDetailSub: "allowed actors:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(queue.NewMemoryStore())
			srv.DeleteManagedEndpoint = func(req ManagementEndpointDeleteRequest) (ManagementEndpointMutationResult, error) {
				t.Fatalf("delete callback should not be called when audit identity policy blocks request")
				return ManagementEndpointMutationResult{}, nil
			}
			tc.configure(srv)

			req := httptest.NewRequest(http.MethodDelete, "http://example/applications/billing/endpoints/invoice.created", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != tc.wantCode {
				t.Fatalf("expected code=%q, got %q", tc.wantCode, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, tc.wantDetailSub) {
				t.Fatalf("expected detail containing %q, got %q", tc.wantDetailSub, errResp.Detail)
			}
		})
	}
}

func TestServer_ListApplicationEndpointsNotFound(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			Applications: []ManagementApplication{
				{Name: "billing"},
			},
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/applications/missing/endpoints", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != readCodeNotFound {
		t.Fatalf("expected code=%q, got %q", readCodeNotFound, errResp.Code)
	}
}

func TestServer_BacklogTopQueued(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-3 * time.Minute), NextRunAt: now.Add(-90 * time.Second)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "q_2", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-2 * time.Minute), NextRunAt: now.Add(-30 * time.Second)}); err != nil {
		t.Fatalf("enqueue q_2: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "q_3", Route: "/r2", Target: "https://example.org/hook", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute), NextRunAt: now.Add(30 * time.Second)}); err != nil {
		t.Fatalf("enqueue q_3: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "d_1", Route: "/r3", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}

	srv := NewServer(store)

	req := httptest.NewRequest(http.MethodGet, "http://example/backlog/top_queued?limit=1", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var out backlogTopQueuedResponse
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.QueueTotal != 4 || out.QueuedTotal != 3 {
		t.Fatalf("unexpected totals: %#v", out)
	}
	if !out.Truncated {
		t.Fatalf("expected truncated=true")
	}
	if len(out.Items) != 1 {
		t.Fatalf("expected 1 item, got %#v", out.Items)
	}
	if out.Items[0].Route != "/r1" || out.Items[0].Target != "pull" || out.Items[0].Queued != 2 {
		t.Fatalf("unexpected top bucket: %#v", out.Items[0])
	}

	filterReq := httptest.NewRequest(http.MethodGet, "http://example/backlog/top_queued?route=/r2&limit=10", nil)
	filterRR := httptest.NewRecorder()
	srv.ServeHTTP(filterRR, filterReq)
	if filterRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for filtered request, got %d", filterRR.Code)
	}
	var filtered backlogTopQueuedResponse
	if err := json.NewDecoder(filterRR.Body).Decode(&filtered); err != nil {
		t.Fatalf("decode filtered response: %v", err)
	}
	if filtered.Truncated {
		t.Fatalf("expected truncated=false for filtered request")
	}
	if len(filtered.Items) != 1 || filtered.Items[0].Route != "/r2" || filtered.Items[0].Queued != 1 {
		t.Fatalf("unexpected filtered items: %#v", filtered.Items)
	}
}

func TestServer_BacklogTopQueuedBadRequest(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/backlog/top_queued?limit=0", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != readCodeInvalidQuery {
		t.Fatalf("expected code=%q, got %q", readCodeInvalidQuery, errResp.Code)
	}
}

func TestServer_BacklogOldestQueued(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-4 * time.Minute), NextRunAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "q_2", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-3 * time.Minute), NextRunAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_2: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "q_3", Route: "/r2", Target: "https://example.org/hook", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute), NextRunAt: now.Add(30 * time.Second)}); err != nil {
		t.Fatalf("enqueue q_3: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "d_1", Route: "/r3", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-30 * time.Second)}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}

	srv := NewServer(store)

	req := httptest.NewRequest(http.MethodGet, "http://example/backlog/oldest_queued?limit=2", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var out backlogOldestQueuedResponse
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.QueueTotal != 4 || out.QueuedTotal != 3 {
		t.Fatalf("unexpected totals: %#v", out)
	}
	if !out.Truncated {
		t.Fatalf("expected truncated=true")
	}
	if len(out.Items) != 2 {
		t.Fatalf("expected 2 items, got %#v", out.Items)
	}
	if out.Items[0].ID != "q_1" || out.Items[1].ID != "q_2" {
		t.Fatalf("unexpected oldest order: %#v", out.Items)
	}

	filterReq := httptest.NewRequest(http.MethodGet, "http://example/backlog/oldest_queued?route=/r2&limit=10", nil)
	filterRR := httptest.NewRecorder()
	srv.ServeHTTP(filterRR, filterReq)
	if filterRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for filtered request, got %d", filterRR.Code)
	}

	var filtered backlogOldestQueuedResponse
	if err := json.NewDecoder(filterRR.Body).Decode(&filtered); err != nil {
		t.Fatalf("decode filtered response: %v", err)
	}
	if filtered.Truncated {
		t.Fatalf("expected truncated=false for filtered request")
	}
	if len(filtered.Items) != 1 || filtered.Items[0].ID != "q_3" {
		t.Fatalf("unexpected filtered items: %#v", filtered.Items)
	}
}

func TestServer_BacklogOldestQueuedBadRequest(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/backlog/oldest_queued?limit=0", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != readCodeInvalidQuery {
		t.Fatalf("expected code=%q, got %q", readCodeInvalidQuery, errResp.Code)
	}
}

func TestServer_BacklogAgingSummary(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-5 * time.Minute), NextRunAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "q_2", Route: "/r1", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-4 * time.Minute), NextRunAt: now.Add(48 * time.Hour)}); err != nil {
		t.Fatalf("enqueue q_2: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "l_1", Route: "/r1", Target: "pull", State: queue.StateLeased, ReceivedAt: now.Add(-20 * time.Minute), NextRunAt: now.Add(1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue l_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "d_1", Route: "/r2", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}

	srv := NewServer(store)

	req := httptest.NewRequest(http.MethodGet, "http://example/backlog/aging_summary?limit=2", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var out backlogAgingSummaryResponse
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.QueueTotal != 4 || out.QueuedTotal != 2 {
		t.Fatalf("unexpected totals: %#v", out)
	}
	if out.Truncated {
		t.Fatalf("expected truncated=false")
	}
	if out.Scanned != 4 || out.BucketCount != 2 {
		t.Fatalf("unexpected scan summary: %#v", out)
	}
	if out.AgeSampleCount != 4 {
		t.Fatalf("expected age_sample_count=4, got %#v", out.AgeSampleCount)
	}
	if out.AgeWindows.LE5M != 3 || out.AgeWindows.GT15MLE1H != 1 {
		t.Fatalf("unexpected age windows: %#v", out.AgeWindows)
	}
	if out.AgePercentilesSeconds == nil || out.AgePercentilesSeconds.P50 != 240 || out.AgePercentilesSeconds.P90 != 1200 || out.AgePercentilesSeconds.P99 != 1200 {
		t.Fatalf("unexpected age percentiles: %#v", out.AgePercentilesSeconds)
	}
	if len(out.Items) != 2 {
		t.Fatalf("expected 2 summary buckets, got %#v", out.Items)
	}
	if out.Items[0].Route != "/r1" || out.Items[0].Target != "pull" || out.Items[0].QueuedObserved != 2 || out.Items[0].LeasedObserved != 1 || out.Items[0].DeadObserved != 0 {
		t.Fatalf("unexpected summary bucket: %#v", out.Items[0])
	}
	if out.Items[0].TotalObserved != 3 {
		t.Fatalf("expected total_observed=3, got %#v", out.Items[0].TotalObserved)
	}
	if out.Items[0].ReadyOverdueCount != 1 {
		t.Fatalf("expected ready_overdue_count=1, got %#v", out.Items[0].ReadyOverdueCount)
	}
	if out.Items[0].AgeSampleCount != 3 || out.Items[0].AgeWindows.LE5M != 2 || out.Items[0].AgeWindows.GT15MLE1H != 1 {
		t.Fatalf("unexpected item age windows: %#v", out.Items[0])
	}
	if out.Items[0].AgePercentilesSeconds == nil || out.Items[0].AgePercentilesSeconds.P50 != 300 || out.Items[0].AgePercentilesSeconds.P90 != 1200 || out.Items[0].AgePercentilesSeconds.P99 != 1200 {
		t.Fatalf("unexpected item age percentiles: %#v", out.Items[0].AgePercentilesSeconds)
	}

	filterReq := httptest.NewRequest(http.MethodGet, "http://example/backlog/aging_summary?states=queued&limit=1", nil)
	filterRR := httptest.NewRecorder()
	srv.ServeHTTP(filterRR, filterReq)
	if filterRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for filtered request, got %d", filterRR.Code)
	}
	var filtered backlogAgingSummaryResponse
	if err := json.NewDecoder(filterRR.Body).Decode(&filtered); err != nil {
		t.Fatalf("decode filtered response: %v", err)
	}
	if !filtered.Truncated {
		t.Fatalf("expected truncated=true for queued-only summary")
	}
	if len(filtered.Items) != 1 || filtered.Items[0].Route != "/r1" || filtered.Items[0].QueuedObserved != 1 || filtered.Items[0].LeasedObserved != 0 || filtered.Items[0].DeadObserved != 0 {
		t.Fatalf("unexpected filtered summary: %#v", filtered.Items)
	}
	if filtered.AgeSampleCount != 1 || filtered.AgeWindows.LE5M != 1 {
		t.Fatalf("unexpected filtered age windows: %#v", filtered)
	}
	if filtered.AgePercentilesSeconds == nil || filtered.AgePercentilesSeconds.P50 != 300 || filtered.AgePercentilesSeconds.P90 != 300 || filtered.AgePercentilesSeconds.P99 != 300 {
		t.Fatalf("unexpected filtered age percentiles: %#v", filtered.AgePercentilesSeconds)
	}
}

func TestServer_BacklogAgingSummaryBadRequest(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/backlog/aging_summary?limit=0", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != readCodeInvalidQuery {
		t.Fatalf("expected code=%q, got %q", readCodeInvalidQuery, errResp.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example/backlog/aging_summary?states=delivered", nil)
	rr2 := httptest.NewRecorder()
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid states, got %d", rr2.Code)
	}
	var errResp2 publishErrorResponse
	if err := json.NewDecoder(rr2.Body).Decode(&errResp2); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp2.Code != readCodeInvalidQuery {
		t.Fatalf("expected code=%q, got %q", readCodeInvalidQuery, errResp2.Code)
	}
}

func TestServer_BacklogTrends(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r1", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "l_1", Route: "/r1", Target: "pull", State: queue.StateLeased}); err != nil {
		t.Fatalf("enqueue l_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "d_1", Route: "/r2", Target: "pull", State: queue.StateDead}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}
	if err := store.CaptureBacklogTrendSample(nowVar); err != nil {
		t.Fatalf("capture trend #1: %v", err)
	}

	if err := store.Enqueue(queue.Envelope{ID: "q_2", Route: "/r1", Target: "https://example.org/hook", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue q_2: %v", err)
	}
	nowVar = nowVar.Add(1 * time.Minute)
	if err := store.CaptureBacklogTrendSample(nowVar); err != nil {
		t.Fatalf("capture trend #2: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodGet, "http://example/backlog/trends?window=2m&step=1m&until=2026-02-07T12:02:00Z", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var out backlogTrendsResponse
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.SampleCount != 2 || out.PointCount != 2 || out.NonEmptyPointCount != 2 {
		t.Fatalf("unexpected trend counts: %#v", out)
	}
	if out.LatestTotal != 4 || out.MaxTotal != 4 {
		t.Fatalf("unexpected trend totals: %#v", out)
	}
	if len(out.Items) != 2 {
		t.Fatalf("expected 2 trend buckets, got %#v", out.Items)
	}
	if out.Signals == nil {
		t.Fatalf("expected trend signals")
	}
	if sampleCount, _ := out.Signals["sample_count"].(float64); int(sampleCount) != 2 {
		t.Fatalf("expected trend signals sample_count=2, got %#v", out.Signals["sample_count"])
	}
	if out.Items[0].SampleCount != 1 || out.Items[0].TotalLast != 3 || out.Items[0].TotalMax != 3 {
		t.Fatalf("unexpected first trend bucket: %#v", out.Items[0])
	}
	if out.Items[1].SampleCount != 1 || out.Items[1].TotalLast != 4 || out.Items[1].TotalMax != 4 {
		t.Fatalf("unexpected second trend bucket: %#v", out.Items[1])
	}

	filterReq := httptest.NewRequest(http.MethodGet, "http://example/backlog/trends?window=2m&step=1m&until=2026-02-07T12:02:00Z&route=/r1", nil)
	filterRR := httptest.NewRecorder()
	srv.ServeHTTP(filterRR, filterReq)
	if filterRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for filtered trends, got %d", filterRR.Code)
	}

	var filtered backlogTrendsResponse
	if err := json.NewDecoder(filterRR.Body).Decode(&filtered); err != nil {
		t.Fatalf("decode filtered response: %v", err)
	}
	if filtered.LatestTotal != 3 || filtered.MaxTotal != 3 {
		t.Fatalf("unexpected filtered totals: %#v", filtered)
	}
	if len(filtered.Items) != 2 || filtered.Items[1].TotalLast != 3 {
		t.Fatalf("unexpected filtered buckets: %#v", filtered.Items)
	}
}

func TestServer_BacklogTrendsBadRequest(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/backlog/trends?window=0s", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid window, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != readCodeInvalidQuery {
		t.Fatalf("expected code=%q, got %q", readCodeInvalidQuery, errResp.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example/backlog/trends?window=2h&step=30s", nil)
	rr2 := httptest.NewRecorder()
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid step, got %d", rr2.Code)
	}
	var errResp2 publishErrorResponse
	if err := json.NewDecoder(rr2.Body).Decode(&errResp2); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp2.Code != readCodeInvalidQuery {
		t.Fatalf("expected code=%q, got %q", readCodeInvalidQuery, errResp2.Code)
	}
}

func TestServer_QueryRouteFiltersRejectNonAbsolutePath(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	tests := []string{
		"/backlog/top_queued?route=relative",
		"/backlog/oldest_queued?route=relative",
		"/backlog/aging_summary?route=relative",
		"/backlog/trends?route=relative",
		"/dlq?route=relative",
		"/messages?route=relative",
		"/attempts?route=relative",
	}

	for _, rawPath := range tests {
		t.Run(rawPath, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "http://example"+rawPath, nil)
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != readCodeInvalidQuery {
				t.Fatalf("expected code=%q, got %q", readCodeInvalidQuery, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "route must start with '/'") {
				t.Fatalf("unexpected detail: %q", errResp.Detail)
			}
		})
	}
}

func TestServer_ApplicationsUnavailable(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/applications", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != readCodeManagementUnavailable {
		t.Fatalf("expected code=%q, got %q", readCodeManagementUnavailable, errResp.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/endpoints", nil)
	rr2 := httptest.NewRecorder()
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr2.Code)
	}
	var errResp2 publishErrorResponse
	if err := json.NewDecoder(rr2.Body).Decode(&errResp2); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp2.Code != readCodeManagementUnavailable {
		t.Fatalf("expected code=%q, got %q", readCodeManagementUnavailable, errResp2.Code)
	}
}

func TestServer_DLQAuth(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.Authorize = BearerTokenAuthorizer([][]byte{[]byte("secret")})

	req := httptest.NewRequest(http.MethodGet, "http://example/dlq", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != readCodeUnauthorized {
		t.Fatalf("expected code=%q, got %q", readCodeUnauthorized, errResp.Code)
	}
}

func TestServer_ListDLQ(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-2 * time.Minute), Payload: []byte("a")}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "dead_2", Route: "/r", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-1 * time.Minute), Payload: []byte("b")}); err != nil {
		t.Fatalf("enqueue dead_2: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodGet, "http://example/dlq?limit=1&include_payload=1", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp dlqResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "dead_2" {
		t.Fatalf("expected dead_2, got %q", resp.Items[0].ID)
	}
	if resp.Items[0].PayloadB64 != base64.StdEncoding.EncodeToString([]byte("b")) {
		t.Fatalf("expected payload_b64 for dead_2")
	}
}

func TestServer_ListMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-2 * time.Minute), Payload: []byte("a")}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "queued_2", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute), Payload: []byte("b")}); err != nil {
		t.Fatalf("enqueue queued_2: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-30 * time.Second), Payload: []byte("dead")}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/r&state=queued&limit=1&include_payload=1", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "queued_2" {
		t.Fatalf("expected queued_2, got %q", resp.Items[0].ID)
	}
	if resp.Items[0].State != queue.StateQueued {
		t.Fatalf("expected queued state, got %q", resp.Items[0].State)
	}
	if resp.Items[0].PayloadB64 != base64.StdEncoding.EncodeToString([]byte("b")) {
		t.Fatalf("expected payload_b64 for queued_2")
	}
}

func TestServer_ListMessagesManagedEndpoint(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "queued_2", Route: "/other", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-30 * time.Second)}); err != nil {
		t.Fatalf("enqueue queued_2: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/messages?application=billing&endpoint_name=invoice.created&state=queued&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "queued_1" {
		t.Fatalf("unexpected managed list result: %#v", resp.Items)
	}
}

func TestServer_ListMessagesManagedEndpointOwnershipMismatch(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/managed", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/managed", []string{"pull"}, true
		}
		return "", nil, false
	}
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if route == "/managed" {
			return "billing", "invoice.updated", true, true
		}
		return "", "", false, true
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/messages?application=billing&endpoint_name=invoice.created&state=queued&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `owned by ("billing", "invoice.updated")`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_ListMessagesManagedEndpointTargetMismatch(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/managed", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/managed", []string{"pull"}, true
		}
		return "", nil, false
	}
	srv.TargetsForRoute = func(route string) []string {
		if route == "/managed" {
			return []string{"https://example.internal/a"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/messages?application=billing&endpoint_name=invoice.created&state=queued&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "targets for route") {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_ListMessagesRouteSelectorOwnershipSourceMismatch(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/managed", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}

	srv := NewServer(store)
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/managed",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if route == "/managed" {
			return "billing", "invoice.updated", true, true
		}
		return "", "", false, true
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/managed&state=queued&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "ownership sources are out of sync") {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_ListMessagesManagedEndpointOwnershipRequiresManagedRoute(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/managed", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/managed", []string{"pull"}, true
		}
		return "", nil, false
	}
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if route == "/managed" {
			return "", "", false, true
		}
		return "", "", false, true
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/messages?application=billing&endpoint_name=invoice.created&state=queued&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `is not managed by route ownership policy`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_ListMessagesManagedEndpointBadRequest(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	reqMissing := httptest.NewRequest(http.MethodGet, "http://example/messages?application=billing&state=queued", nil)
	rrMissing := httptest.NewRecorder()
	srv.ServeHTTP(rrMissing, reqMissing)
	if rrMissing.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing endpoint_name, got %d", rrMissing.Code)
	}

	reqInvalidApplication := httptest.NewRequest(http.MethodGet, "http://example/messages?application=billing%20core&endpoint_name=invoice.created&state=queued", nil)
	rrInvalidApplication := httptest.NewRecorder()
	srv.ServeHTTP(rrInvalidApplication, reqInvalidApplication)
	if rrInvalidApplication.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid application selector, got %d", rrInvalidApplication.Code)
	}
	var invalidApplicationErr publishErrorResponse
	if err := json.NewDecoder(rrInvalidApplication.Body).Decode(&invalidApplicationErr); err != nil {
		t.Fatalf("decode invalid application error response: %v", err)
	}
	if invalidApplicationErr.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, invalidApplicationErr.Code)
	}
	if !strings.Contains(invalidApplicationErr.Detail, "application must match") {
		t.Fatalf("unexpected invalid application detail: %#v", invalidApplicationErr.Detail)
	}

	reqInvalidEndpoint := httptest.NewRequest(http.MethodGet, "http://example/messages?application=billing&endpoint_name=invoice%20created&state=queued", nil)
	rrInvalidEndpoint := httptest.NewRecorder()
	srv.ServeHTTP(rrInvalidEndpoint, reqInvalidEndpoint)
	if rrInvalidEndpoint.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid endpoint selector, got %d", rrInvalidEndpoint.Code)
	}
	var invalidEndpointErr publishErrorResponse
	if err := json.NewDecoder(rrInvalidEndpoint.Body).Decode(&invalidEndpointErr); err != nil {
		t.Fatalf("decode invalid endpoint error response: %v", err)
	}
	if invalidEndpointErr.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, invalidEndpointErr.Code)
	}
	if !strings.Contains(invalidEndpointErr.Detail, "endpoint_name must match") {
		t.Fatalf("unexpected invalid endpoint detail: %#v", invalidEndpointErr.Detail)
	}

	reqUnresolved := httptest.NewRequest(http.MethodGet, "http://example/messages?application=missing&endpoint_name=x&state=queued", nil)
	rrUnresolved := httptest.NewRecorder()
	srv.ServeHTTP(rrUnresolved, reqUnresolved)
	if rrUnresolved.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unresolved selector, got %d", rrUnresolved.Code)
	}
	var unresolvedErr publishErrorResponse
	if err := json.NewDecoder(rrUnresolved.Body).Decode(&unresolvedErr); err != nil {
		t.Fatalf("decode unresolved selector error response: %v", err)
	}
	if unresolvedErr.Code != publishCodeManagedEndpointNotFound {
		t.Fatalf("expected unresolved selector code=%q, got %q", publishCodeManagedEndpointNotFound, unresolvedErr.Code)
	}
	if unresolvedErr.Detail != "managed endpoint not found" {
		t.Fatalf("unexpected unresolved selector detail: %#v", unresolvedErr.Detail)
	}

	reqMismatch := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/r&application=billing&endpoint_name=invoice.created&state=queued", nil)
	rrMismatch := httptest.NewRecorder()
	srv.ServeHTTP(rrMismatch, reqMismatch)
	if rrMismatch.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for route hint with managed selector, got %d", rrMismatch.Code)
	}
	var mismatchErr publishErrorResponse
	if err := json.NewDecoder(rrMismatch.Body).Decode(&mismatchErr); err != nil {
		t.Fatalf("decode mismatch error response: %v", err)
	}
	if mismatchErr.Code != publishCodeScopedSelectorMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeScopedSelectorMismatch, mismatchErr.Code)
	}
	if mismatchErr.Detail != "route is not allowed when application and endpoint_name are set" {
		t.Fatalf("unexpected mismatch detail: %#v", mismatchErr.Detail)
	}
}

func TestServer_ListMessagesManagedEndpointResolverUnavailable(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/messages?application=billing&endpoint_name=invoice.created&state=queued", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when managed resolver is unavailable, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedResolverMissing {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedResolverMissing, errResp.Code)
	}
	if errResp.Detail != "managed endpoint resolution is not configured" {
		t.Fatalf("unexpected detail: %#v", errResp.Detail)
	}
}

func TestServer_ListMessagesManagedEndpointUsesManagementModel(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "queued_2", Route: "/other", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-30 * time.Second)}); err != nil {
		t.Fatalf("enqueue queued_2: %v", err)
	}

	srv := NewServer(store)
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       2,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/r",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/messages?application=billing&endpoint_name=invoice.created&state=queued&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "queued_1" {
		t.Fatalf("unexpected managed list result: %#v", resp.Items)
	}
}

func TestServer_ApplicationEndpointMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "queued_2", Route: "/other", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-30 * time.Second)}); err != nil {
		t.Fatalf("enqueue queued_2: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/endpoints/invoice.created/messages?state=queued&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "queued_1" {
		t.Fatalf("unexpected endpoint messages response: %#v", resp.Items)
	}
}

func TestServer_ApplicationEndpointMessagesUsesManagementModel(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "queued_2", Route: "/other", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-30 * time.Second)}); err != nil {
		t.Fatalf("enqueue queued_2: %v", err)
	}

	srv := NewServer(store)
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       2,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/r",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/endpoints/invoice.created/messages?state=queued&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "queued_1" {
		t.Fatalf("unexpected endpoint messages response: %#v", resp.Items)
	}
}

func TestServer_ApplicationEndpointMessagesManagedOwnershipMismatch(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if route == "/r" {
			return "billing", "invoice.updated", true, true
		}
		return "", "", false, true
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/endpoints/invoice.created/messages?state=queued&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `owned by ("billing", "invoice.updated")`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_ApplicationEndpointMessagesTargetMismatch(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/managed", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/managed", []string{"pull"}, true
		}
		return "", nil, false
	}
	srv.TargetsForRoute = func(route string) []string {
		if route == "/managed" {
			return []string{"https://example.internal/a"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/endpoints/invoice.created/messages?state=queued&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "targets for route") {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_ApplicationEndpointMessagesManagedOwnershipRequiresManagedRoute(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if route == "/r" {
			return "", "", false, true
		}
		return "", "", false, true
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/endpoints/invoice.created/messages?state=queued&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `is not managed by route ownership policy`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_CancelMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "queued_1", Route: "/r", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue queued_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "delivered_1", Route: "/r", Target: "pull", State: queue.StateDelivered}); err != nil {
		t.Fatalf("enqueue delivered_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "leased_1", Route: "/r", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue leased_1: %v", err)
	}

	deq, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", Batch: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deq.Items) != 1 || deq.Items[0].ID != "queued_1" {
		t.Fatalf("expected queued_1 leased first, got %#v", deq.Items)
	}
	leaseID := deq.Items[0].LeaseID

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel", strings.NewReader(`{"ids":["queued_1","dead_1","leased_1","delivered_1","missing","queued_1"]}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesCancelResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Canceled != 3 {
		t.Fatalf("expected canceled=3, got %d", resp.Canceled)
	}
	if err := store.Ack(leaseID); err != queue.ErrLeaseNotFound {
		t.Fatalf("expected lease not found after cancel, got %v", err)
	}

	listReq := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/r&state=canceled&limit=10", nil)
	listRR := httptest.NewRecorder()
	srv.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for canceled list, got %d", listRR.Code)
	}
	var listResp messagesResponse
	if err := json.NewDecoder(listRR.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Items) != 3 {
		t.Fatalf("expected 3 canceled items, got %d", len(listResp.Items))
	}
}

func TestServer_CancelMessagesRequiresAuditReason(t *testing.T) {
	store := queue.NewMemoryStore()
	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel", strings.NewReader(`{"ids":["q_1"]}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when audit reason header missing, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditReasonRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditReasonRequired, errResp.Code)
	}
}

func TestServer_RequeueMessages(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead, DeadReason: "max_retries"}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "canceled_1", Route: "/r", Target: "pull", State: queue.StateCanceled}); err != nil {
		t.Fatalf("enqueue canceled_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "delivered_1", Route: "/r", Target: "pull", State: queue.StateDelivered}); err != nil {
		t.Fatalf("enqueue delivered_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/requeue", strings.NewReader(`{"ids":["dead_1","canceled_1","delivered_1","missing","dead_1"]}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesRequeueResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Requeued != 2 {
		t.Fatalf("expected requeued=2, got %d", resp.Requeued)
	}

	listReq := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/r&state=queued&limit=10", nil)
	listRR := httptest.NewRecorder()
	srv.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for queued list, got %d", listRR.Code)
	}
	var listResp messagesResponse
	if err := json.NewDecoder(listRR.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Items) != 2 {
		t.Fatalf("expected 2 queued items, got %d", len(listResp.Items))
	}
}

func TestServer_RequeueMessagesRequiresAuditReason(t *testing.T) {
	store := queue.NewMemoryStore()
	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead, DeadReason: "max_retries"}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/requeue", strings.NewReader(`{"ids":["dead_1"]}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when audit reason header missing, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditReasonRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditReasonRequired, errResp.Code)
	}
}

func TestServer_PublishMessages(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_1",
      "route": "/r",
      "target": "pull",
      "payload_b64": "aGVsbG8=",
      "received_at": "2026-02-07T11:58:00Z",
      "next_run_at": "2026-02-07T11:59:30Z"
    },
    {
      "id": "pub_2",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	req.Header.Set(auditActorHeader, "ops@example.test")
	req.Header.Set(auditRequestIDHeader, "req-publish-1")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var publishResp messagesPublishResponse
	if err := json.NewDecoder(rr.Body).Decode(&publishResp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if publishResp.Published != 2 {
		t.Fatalf("expected published=2, got %d", publishResp.Published)
	}
	if publishResp.Audit == nil {
		t.Fatalf("expected publish audit in response")
	}
	if publishResp.Audit.Reason != "operator_publish" || publishResp.Audit.Actor != "ops@example.test" || publishResp.Audit.RequestID != "req-publish-1" {
		t.Fatalf("unexpected publish audit payload: %#v", publishResp.Audit)
	}

	listReq := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/r&state=queued&limit=10&include_payload=1", nil)
	listRR := httptest.NewRecorder()
	srv.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for queued list, got %d", listRR.Code)
	}
	var listResp messagesResponse
	if err := json.NewDecoder(listRR.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Items) != 2 {
		t.Fatalf("expected 2 queued items, got %d", len(listResp.Items))
	}
}

func TestServer_PublishMessagesManagedEndpointRequiresScopedPath(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	srv := NewServer(store)

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_m1",
      "application": "billing",
      "endpoint_name": "invoice.created",
      "payload_b64": "aGVsbG8="
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeScopedPublishRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeScopedPublishRequired, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesRejectsInvalidManagedSelectorLabel(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_invalid_selector",
      "application": "billing core",
      "endpoint_name": "invoice.created"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "item.application must match") {
		t.Fatalf("unexpected detail: %#v", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesRejectsTrailingJSONBody(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_trailing",
      "route": "/r",
      "target": "pull"
    }
  ]
}
{}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item_index, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected no enqueued messages, got %#v", listResp.Items)
	}
}

func TestServer_PublishMessagesRouteSelectorFailClosedWhenManagementModelUnavailable(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.PublishScopedManagedFailClosed = true

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_fail_closed",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedResolverMissing {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedResolverMissing, errResp.Code)
	}
	if errResp.Detail != "management model is unavailable for managed-route publish policy evaluation" {
		t.Fatalf("unexpected detail: %#v", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{State: queue.StateQueued, Limit: 10})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected no enqueued messages, got %#v", listResp.Items)
	}
}

func TestServer_PublishMessagesManagedRouteRequiresSelector(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/r",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_mroute_1",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for managed route without selector, got %d", rr.Code)
	}
}

func TestServer_PublishMessagesManagedRouteOwnershipSourceMismatch(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/r",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if strings.TrimSpace(route) == "/r" {
			return "billing", "invoice.updated", true, true
		}
		return "", "", false, true
	}
	srv.TargetsForRoute = func(route string) []string {
		if strings.TrimSpace(route) == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_mroute_source_drift_1",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for ownership source drift, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `ownership sources are out of sync`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{State: queue.StateQueued, Limit: 10})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected no enqueued messages, got %#v", listResp.Items)
	}
}

func TestServer_PublishMessagesManagedRouteRequiresSelector_WithManagedRouteInfoFallback(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if strings.TrimSpace(route) == "/r" {
			return "billing", "invoice.created", true, true
		}
		return "", "", false, true
	}
	srv.TargetsForRoute = func(route string) []string {
		if strings.TrimSpace(route) == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_mroute_fallback",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for managed route without selector, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedSelectorRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedSelectorRequired, errResp.Code)
	}
}

func TestServer_PublishMessagesManagedRouteRequiresSelector_WithManagedRouteSetFallback(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.PublishScopedManagedFailClosed = true
	srv.ManagedRouteSet = func() (map[string]struct{}, bool) {
		return map[string]struct{}{
			"/r": {},
		}, true
	}
	srv.TargetsForRoute = func(route string) []string {
		if strings.TrimSpace(route) == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_mroute_set_fallback",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for managed route without selector, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedSelectorRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedSelectorRequired, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{State: queue.StateQueued, Limit: 10})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected no enqueued messages, got %#v", listResp.Items)
	}
}

func TestServer_PublishMessagesManagedRouteRequiresSelector_WithManagedRouteSetFallbackWhenModelSaysUnmanaged(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.PublishScopedManagedFailClosed = true
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       0,
			ApplicationCount: 0,
			EndpointCount:    0,
			Applications:     nil,
		}
	}
	srv.ManagedRouteSet = func() (map[string]struct{}, bool) {
		return map[string]struct{}{
			"/r": {},
		}, true
	}
	srv.TargetsForRoute = func(route string) []string {
		if strings.TrimSpace(route) == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_mroute_set_with_model_unmanaged",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for managed route without selector, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedSelectorRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedSelectorRequired, errResp.Code)
	}
}

func TestServer_PublishMessagesRouteSelectorAllowedWhenManagedRouteSetAvailableAndRouteUnmanaged(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.PublishScopedManagedFailClosed = true
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       0,
			ApplicationCount: 0,
			EndpointCount:    0,
			Applications:     nil,
		}
	}
	srv.ManagedRouteSet = func() (map[string]struct{}, bool) {
		return map[string]struct{}{
			"/managed": {},
		}, true
	}
	srv.TargetsForRoute = func(route string) []string {
		if strings.TrimSpace(route) == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_unmanaged_with_set",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for unmanaged route publish, got %d", rr.Code)
	}

	var out messagesPublishResponse
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode publish response: %v", err)
	}
	if out.Published != 1 {
		t.Fatalf("expected published=1, got %d", out.Published)
	}
}

func TestServer_PublishMessagesRequiresAuditReason(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_missing_audit",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when audit reason header missing, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditReasonRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditReasonRequired, errResp.Code)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item index, got %v", *errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesRequiresAuditActorByPolicy(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.PublishRequireAuditActor = true

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_missing_actor",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when audit actor header missing, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditActorRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditActorRequired, errResp.Code)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item index, got %v", *errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesDirectPathIgnoresScopedManagedActorPolicy(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_direct_scoped_actor_policy",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 on direct publish without actor, got %d", rr.Code)
	}
}

func TestServer_PublishMessagesRouteResolverMissing(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_missing_route_resolver",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeRouteResolverMissing {
		t.Fatalf("expected code=%q, got %q", publishCodeRouteResolverMissing, errResp.Code)
	}
	if errResp.Detail != "route target resolution is not configured for global publish path" {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected no enqueued messages, got %#v", listResp.Items)
	}
}

func TestServer_PublishMessagesGlobalPathDisabledByPolicy(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.PublishGlobalDirectEnabled = false

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_disabled_global",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeGlobalPublishDisabled {
		t.Fatalf("expected code=%q, got %q", publishCodeGlobalPublishDisabled, errResp.Code)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item index, got %#v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesRoutePublishDisabledByPolicy(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}
	srv.PublishEnabledForRoute = func(route string) bool {
		return strings.TrimSpace(route) != "/r"
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_route_disabled",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeRoutePublishDisabled {
		t.Fatalf("expected code=%q, got %q", publishCodeRoutePublishDisabled, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item index 0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesRoutePublishDirectDisabledByPolicy(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}
	srv.PublishDirectEnabledForRoute = func(route string) bool {
		return strings.TrimSpace(route) != "/r"
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_route_direct_disabled",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeRoutePublishDisabled {
		t.Fatalf("expected code=%q, got %q", publishCodeRoutePublishDisabled, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item index 0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesPullRouteDisabledByPolicy(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.PublishAllowPullRoutes = false
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_pull_disabled",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodePullRoutePublishDisabled {
		t.Fatalf("expected code=%q, got %q", publishCodePullRoutePublishDisabled, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesAuditHook(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}
	var got ManagementMutationAuditEvent
	srv.AuditManagementMutation = func(event ManagementMutationAuditEvent) {
		got = event
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_audit_1",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	req.Header.Set(auditActorHeader, "ops@example.test")
	req.Header.Set(auditRequestIDHeader, "req-publish-audit")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if got.Operation != "publish_messages" || got.Route != "/r" || got.Target != "pull" || got.State != string(queue.StateQueued) {
		t.Fatalf("unexpected publish audit event scope: %#v", got)
	}
	if got.Reason != "operator_publish" || got.Actor != "ops@example.test" || got.RequestID != "req-publish-audit" {
		t.Fatalf("unexpected publish audit metadata: %#v", got)
	}
	if got.Changed != 1 || got.Matched != 1 || got.Limit != 1 {
		t.Fatalf("unexpected publish audit counters: %#v", got)
	}
}

func TestServer_PublishMessagesManagedEndpointRequiresScopedPathWithResolver(t *testing.T) {
	store := queue.NewMemoryStore()

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"https://example.org/a", "https://example.org/b"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_m2",
      "application": "billing",
      "endpoint_name": "invoice.created"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeScopedPublishRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeScopedPublishRequired, errResp.Code)
	}
}

func TestServer_PublishMessagesRejectsManagedSelectorsBeforeEnqueue(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/direct" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_direct_should_not_enqueue",
      "route": "/direct"
    },
    {
      "id": "pub_managed_rejected",
      "application": "billing",
      "endpoint_name": "invoice.created"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeScopedPublishRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeScopedPublishRequired, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 1 {
		t.Fatalf("expected item_index=1, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{
		Route: "/direct",
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected zero queued items after pre-validation failure, got %#v", listResp.Items)
	}
}

func TestServer_PublishMessagesPreflightRouteValidationPreventsPartialEnqueue(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/known" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_preflight_ok",
      "route": "/known"
    },
    {
      "id": "pub_preflight_missing_route",
      "route": "/missing",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeRouteNotFound {
		t.Fatalf("expected code=%q, got %q", publishCodeRouteNotFound, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 1 {
		t.Fatalf("expected item_index=1, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{
		Route: "/known",
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected zero queued items after route preflight failure, got %#v", listResp.Items)
	}
}

func TestServer_PublishMessagesRouteAutoTarget(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_rt1",
      "route": "/r"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	listReq := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/r&target=pull&state=queued&limit=10", nil)
	listRR := httptest.NewRecorder()
	srv.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for queued list, got %d", listRR.Code)
	}
	var listResp messagesResponse
	if err := json.NewDecoder(listRR.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Items) != 1 || listResp.Items[0].ID != "pub_rt1" {
		t.Fatalf("unexpected queued items: %#v", listResp.Items)
	}
}

func TestServer_PublishMessagesTargetMissingListsAllowedTargets(t *testing.T) {
	store := queue.NewMemoryStore()

	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull", "https://example.org/hook"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_missing_target",
      "route": "/r"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeTargetUnresolvable {
		t.Fatalf("expected code=%q, got %q", publishCodeTargetUnresolvable, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `item.target is required for route "/r"; allowed targets: "pull", "https://example.org/hook"`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesTargetMismatchListsAllowedTargets(t *testing.T) {
	store := queue.NewMemoryStore()

	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull", "https://example.org/hook"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_bad_target",
      "route": "/r",
      "target": "https://bad.example/hook"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeTargetUnresolvable {
		t.Fatalf("expected code=%q, got %q", publishCodeTargetUnresolvable, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `item.target "https://bad.example/hook" is not allowed for route "/r"; allowed targets: "pull", "https://example.org/hook"`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesRejectsUnknownRoute(t *testing.T) {
	store := queue.NewMemoryStore()

	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/known" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_missing_route",
      "route": "/missing",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestServer_PublishMessagesDuplicateIDConflict(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{ID: "pub_dup_1", Route: "/r", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("seed enqueue: %v", err)
	}

	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_dup_1",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeDuplicateID {
		t.Fatalf("expected code=%q, got %q", publishCodeDuplicateID, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesDuplicateIDPrecheckPreventsPartialEnqueue(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{ID: "pub_dup_existing", Route: "/r", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("seed enqueue: %v", err)
	}

	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_new_before_dup",
      "route": "/r",
      "target": "pull"
    },
    {
      "id": "pub_dup_existing",
      "route": "/r",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeDuplicateID {
		t.Fatalf("expected code=%q, got %q", publishCodeDuplicateID, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 1 {
		t.Fatalf("expected item_index=1, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{
		Route: "/r",
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	for _, item := range listResp.Items {
		if item.ID == "pub_new_before_dup" {
			t.Fatalf("expected no partial enqueue for first item, got %#v", listResp.Items)
		}
	}
}

func TestServer_PublishMessagesRejectsInvalidRoutePath(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_invalid_route",
      "route": "not-a-path",
      "target": "pull"
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestServer_PublishMessagesPayloadTooLarge(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.MaxBodyBytes = 32
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}
	srv.LimitsForRoute = func(route string) (int64, int) {
		if route == "/r" {
			return 4, 0
		}
		return 0, 0
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_too_large",
      "route": "/r",
      "target": "pull",
      "payload_b64": "aGVsbG8="
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodePayloadTooLarge {
		t.Fatalf("expected code=%q, got %q", publishCodePayloadTooLarge, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `decoded payload (5 bytes) exceeds max_body (4 bytes) for route "/r"`) {
		t.Fatalf("expected detailed max_body error, got %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesHeadersTooLarge(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.MaxHeaderBytes = 32
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}
	srv.LimitsForRoute = func(route string) (int64, int) {
		if route == "/r" {
			return 0, 4
		}
		return 0, 0
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_headers_too_large",
      "route": "/r",
      "target": "pull",
      "headers": {"X":"hello"}
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeHeadersTooLarge {
		t.Fatalf("expected code=%q, got %q", publishCodeHeadersTooLarge, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `headers (6 bytes) exceed max_headers (4 bytes) for route "/r"`) {
		t.Fatalf("expected detailed max_headers error, got %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesRejectsInvalidHeaders(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish", strings.NewReader(`{
  "items": [
    {
      "id": "pub_invalid_header",
      "route": "/r",
      "target": "pull",
      "headers": {"Bad Header":"value"}
    }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidHeader {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidHeader, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "invalid field name") {
		t.Fatalf("expected invalid header detail, got %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_ResumeMessages(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "c_1", Route: "/r", Target: "pull", State: queue.StateCanceled}); err != nil {
		t.Fatalf("enqueue c_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "d_1", Route: "/r", Target: "pull", State: queue.StateDead}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/resume", strings.NewReader(`{"ids":["c_1","d_1"]}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resumeResp messagesResumeResponse
	if err := json.NewDecoder(rr.Body).Decode(&resumeResp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resumeResp.Resumed != 1 {
		t.Fatalf("expected resumed=1, got %d", resumeResp.Resumed)
	}
}

func TestServer_ResumeMessagesRequiresAuditReason(t *testing.T) {
	store := queue.NewMemoryStore()
	if err := store.Enqueue(queue.Envelope{ID: "c_1", Route: "/r", Target: "pull", State: queue.StateCanceled}); err != nil {
		t.Fatalf("enqueue c_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/resume", strings.NewReader(`{"ids":["c_1"]}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when audit reason header missing, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditReasonRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditReasonRequired, errResp.Code)
	}
}

func TestServer_ResumeMessagesByFilterPreviewOnly(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "c_1", Route: "/r", Target: "pull", State: queue.StateCanceled, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue c_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "c_2", Route: "/r", Target: "pull", State: queue.StateCanceled, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue c_2: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/resume_by_filter", strings.NewReader(`{"route":"/r","limit":10,"preview_only":true}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesManageFilterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.PreviewOnly || resp.Matched != 2 || resp.Resumed != 0 {
		t.Fatalf("unexpected preview response: %#v", resp)
	}
}

func TestServer_CancelMessagesByFilter(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-4 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "q_2", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-3 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_2: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "d_1", Route: "/r", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel_by_filter", strings.NewReader(`{"route":"/r","state":"queued","limit":1}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesManageFilterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Canceled != 1 {
		t.Fatalf("expected canceled=1, got %d", resp.Canceled)
	}
	if resp.Matched != 1 || resp.PreviewOnly {
		t.Fatalf("unexpected manage response: %#v", resp)
	}
}

func TestServer_CancelMessagesByFilterManagedEndpoint(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-4 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "q_2", Route: "/other", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-3 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_2: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel_by_filter", strings.NewReader(`{
  "application":"billing",
  "endpoint_name":"invoice.created",
  "state":"queued",
  "limit":10
}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesManageFilterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Canceled != 1 || resp.Matched != 1 {
		t.Fatalf("unexpected manage response: %#v", resp)
	}

	listReq := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/other&state=queued&limit=10", nil)
	listRR := httptest.NewRecorder()
	srv.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for queued list, got %d", listRR.Code)
	}
	var listResp messagesResponse
	if err := json.NewDecoder(listRR.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Items) != 1 || listResp.Items[0].ID != "q_2" {
		t.Fatalf("unexpected remaining queued items: %#v", listResp.Items)
	}
}

func TestServer_ApplicationEndpointCancelByFilter(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-4 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "q_2", Route: "/other", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-3 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_2: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}
	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/cancel_by_filter", strings.NewReader(`{
  "state":"queued",
  "limit":10
}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	req.Header.Set(auditActorHeader, "ops@example.test")
	req.Header.Set(auditRequestIDHeader, "req-123")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesManageFilterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Canceled != 1 || resp.Matched != 1 {
		t.Fatalf("unexpected manage response: %#v", resp)
	}

	listReq := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/other&state=queued&limit=10", nil)
	listRR := httptest.NewRecorder()
	srv.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for queued list, got %d", listRR.Code)
	}
	var listResp messagesResponse
	if err := json.NewDecoder(listRR.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Items) != 1 || listResp.Items[0].ID != "q_2" {
		t.Fatalf("unexpected remaining queued items: %#v", listResp.Items)
	}
}

func TestServer_ApplicationEndpointFilterMutationsManagedOwnershipMismatch(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel",
			path:  "/applications/billing/endpoints/invoice.created/messages/cancel_by_filter",
			body:  `{"state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue",
			path:  "/applications/billing/endpoints/invoice.created/messages/requeue_by_filter",
			body:  `{"state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume",
			path:  "/applications/billing/endpoints/invoice.created/messages/resume_by_filter",
			body:  `{"state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/r", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
				if application == "billing" && endpointName == "invoice.created" {
					return "/r", []string{"pull"}, true
				}
				return "", nil, false
			}
			srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
				if route == "/r" {
					return "billing", "invoice.updated", true, true
				}
				return "", "", false, true
			}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedTargetMismatch {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, `owned by ("billing", "invoice.updated")`) {
				t.Fatalf("unexpected detail: %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_ApplicationEndpointPublish(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}
	var got ManagementMutationAuditEvent
	srv.AuditManagementMutation = func(event ManagementMutationAuditEvent) {
		got = event
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [
    { "id": "evt_ap_1", "payload_b64": "e30=" },
    { "id": "evt_ap_2", "target": "pull" }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	req.Header.Set(auditActorHeader, "ops@example.test")
	req.Header.Set(auditRequestIDHeader, "req-publish-scoped")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var publishResp messagesPublishResponse
	if err := json.NewDecoder(rr.Body).Decode(&publishResp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if publishResp.Published != 2 {
		t.Fatalf("expected published=2, got %d", publishResp.Published)
	}
	if publishResp.Scope == nil || publishResp.Scope.Application != "billing" || publishResp.Scope.EndpointName != "invoice.created" || publishResp.Scope.Route != "/r" {
		t.Fatalf("unexpected scope: %#v", publishResp.Scope)
	}
	if publishResp.Audit == nil || publishResp.Audit.Reason != "operator_publish" {
		t.Fatalf("unexpected audit payload: %#v", publishResp.Audit)
	}

	listReq := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/endpoints/invoice.created/messages?state=queued&limit=10", nil)
	listRR := httptest.NewRecorder()
	srv.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for queued list, got %d", listRR.Code)
	}
	var listResp messagesResponse
	if err := json.NewDecoder(listRR.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Items) != 2 {
		t.Fatalf("expected 2 queued items, got %d", len(listResp.Items))
	}
	if got.Operation != "publish_messages" || got.Application != "billing" || got.EndpointName != "invoice.created" || got.Route != "/r" {
		t.Fatalf("unexpected publish audit scope: %#v", got)
	}
	if got.Changed != 2 || got.Matched != 2 {
		t.Fatalf("unexpected publish audit counters: %#v", got)
	}
	if got.Reason != "operator_publish" || got.Actor != "ops@example.test" || got.RequestID != "req-publish-scoped" {
		t.Fatalf("unexpected publish audit metadata: %#v", got)
	}
}

func TestServer_ApplicationEndpointPublishUsesManagementModel(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	srv := NewServer(store)
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/r",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull", " pull "}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_model_1" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var publishResp messagesPublishResponse
	if err := json.NewDecoder(rr.Body).Decode(&publishResp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if publishResp.Published != 1 {
		t.Fatalf("expected published=1, got %d", publishResp.Published)
	}
	if publishResp.Scope == nil || publishResp.Scope.Route != "/r" {
		t.Fatalf("unexpected scope: %#v", publishResp.Scope)
	}

	listReq := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/r&target=pull&state=queued&limit=10", nil)
	listRR := httptest.NewRecorder()
	srv.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for queued list, got %d", listRR.Code)
	}
	var listResp messagesResponse
	if err := json.NewDecoder(listRR.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Items) != 1 || listResp.Items[0].ID != "evt_ap_model_1" {
		t.Fatalf("unexpected queued items: %#v", listResp.Items)
	}
}

func TestServer_ApplicationEndpointPublishManagedOwnershipSourceMismatch(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/r",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if route == "/r" {
			return "billing", "invoice.updated", true, true
		}
		return "", "", false, true
	}
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_owner_source_drift_1" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `ownership sources are out of sync`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item index, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{
		Route: "/r",
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected no queued items, got %#v", listResp.Items)
	}
}

func TestServer_ApplicationEndpointPublishManagedTargetMismatch(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/r",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"https://example.org/hook"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_model_mismatch" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `managed endpoint ("billing", "invoice.created") targets for route "/r" are out of sync`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item index, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{
		Route: "/r",
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected no queued items, got %#v", listResp.Items)
	}
}

func TestServer_ApplicationEndpointPublishManagedOwnershipMismatch(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if route == "/r" {
			return "billing", "invoice.updated", true, true
		}
		return "", "", false, true
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_owner_mismatch_1" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `owned by ("billing", "invoice.updated")`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item index, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{
		Route: "/r",
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected no queued items, got %#v", listResp.Items)
	}
}

func TestServer_ApplicationEndpointPublishManagedOwnershipRequiresManagedRoute(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}
	srv.TargetsForRoute = func(route string) []string {
		if route == "/r" {
			return []string{"pull"}
		}
		return nil
	}
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if route == "/r" {
			return "", "", false, true
		}
		return "", "", false, true
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_owner_unmanaged_1" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `is not managed by route ownership policy`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_ApplicationEndpointPublishRequiresAuditReason(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_1" }]
}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when audit reason header missing, got %d", rr.Code)
	}
}

func TestServer_ApplicationEndpointPublishRequiresAuditRequestIDByPolicy(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.PublishRequireAuditRequestID = true
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_missing_reqid" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	req.Header.Set(auditActorHeader, "ops@example.test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when request id header missing, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditRequestIDRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditRequestIDRequired, errResp.Code)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item index, got %#v", errResp.ItemIndex)
	}
}

func TestServer_ApplicationEndpointPublishRequiresActorByScopedManagedActorPolicy(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_missing_actor_scoped_policy" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when scoped managed actor policy requires actor, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditActorRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditActorRequired, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "actor_allow/actor_prefix") {
		t.Fatalf("expected scoped managed actor policy detail, got %q", errResp.Detail)
	}
}

func TestServer_ApplicationEndpointPublishRejectsActorByScopedManagedActorPolicy(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
	srv.PublishScopedManagedActorPrefixes = []string{"role:ops/"}
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_actor_denied_scoped_policy" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	req.Header.Set(auditActorHeader, "dev@example.test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when scoped actor is not allowed, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditActorNotAllowed {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditActorNotAllowed, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "allowed actors:") || !strings.Contains(errResp.Detail, "allowed actor prefixes:") {
		t.Fatalf("expected allowed actor details, got %q", errResp.Detail)
	}
}

func TestServer_ApplicationEndpointPublishScopedPathDisabledByPolicy(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.PublishScopedManagedEnabled = false
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_disabled" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeScopedPublishDisabled {
		t.Fatalf("expected code=%q, got %q", publishCodeScopedPublishDisabled, errResp.Code)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item index, got %#v", errResp.ItemIndex)
	}
}

func TestServer_ApplicationEndpointPublishDeliverRouteDisabledByPolicy(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.PublishAllowDeliverRoutes = false
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"https://example.org/hook"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_deliver_disabled" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeDeliverRoutePublishDisabled {
		t.Fatalf("expected code=%q, got %q", publishCodeDeliverRoutePublishDisabled, errResp.Code)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item index, got %#v", errResp.ItemIndex)
	}
}

func TestServer_ApplicationEndpointPublishRoutePublishDisabledByPolicy(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.PublishEnabledForRoute = func(route string) bool {
		return strings.TrimSpace(route) != "/r"
	}
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_route_disabled" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeRoutePublishDisabled {
		t.Fatalf("expected code=%q, got %q", publishCodeRoutePublishDisabled, errResp.Code)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item index, got %#v", errResp.ItemIndex)
	}
}

func TestServer_ApplicationEndpointPublishRoutePublishManagedDisabledByPolicy(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.PublishManagedEnabledForRoute = func(route string) bool {
		return strings.TrimSpace(route) != "/r"
	}
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_route_managed_disabled" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeRoutePublishDisabled {
		t.Fatalf("expected code=%q, got %q", publishCodeRoutePublishDisabled, errResp.Code)
	}
	if errResp.ItemIndex != nil {
		t.Fatalf("expected no item index, got %#v", errResp.ItemIndex)
	}
}

func TestServer_ApplicationEndpointPublishSelectorForbidden(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_1", "route": "/other" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for forbidden selector hint, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeScopedSelectorForbidden {
		t.Fatalf("expected code=%q, got %q", publishCodeScopedSelectorForbidden, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_ApplicationEndpointPublishPreflightValidationPreventsPartialEnqueue(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [
    { "id": "evt_ap_preflight_ok" },
    { "id": "evt_ap_preflight_bad_target", "target": "https://bad.example/hook" }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeTargetUnresolvable {
		t.Fatalf("expected code=%q, got %q", publishCodeTargetUnresolvable, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 1 {
		t.Fatalf("expected item_index=1, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{
		Route: "/r",
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected zero queued items after scoped preflight failure, got %#v", listResp.Items)
	}
}

func TestServer_ApplicationEndpointPublishTargetMismatchListsAllowedTargets(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull", "https://example.org/hook"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_bad_target", "target": "https://bad.example/hook" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeTargetUnresolvable {
		t.Fatalf("expected code=%q, got %q", publishCodeTargetUnresolvable, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `item.target "https://bad.example/hook" is not allowed for route "/r"; allowed targets: "pull", "https://example.org/hook"`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_ApplicationEndpointPublishDuplicateIDPrecheckPreventsPartialEnqueue(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "evt_ap_dup_existing", Route: "/r", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("seed enqueue: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [
    { "id": "evt_ap_new_before_dup" },
    { "id": "evt_ap_dup_existing" }
  ]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeDuplicateID {
		t.Fatalf("expected code=%q, got %q", publishCodeDuplicateID, errResp.Code)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 1 {
		t.Fatalf("expected item_index=1, got %#v", errResp.ItemIndex)
	}

	listResp, err := store.ListMessages(queue.MessageListRequest{
		Route: "/r",
		State: queue.StateQueued,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list queued: %v", err)
	}
	for _, item := range listResp.Items {
		if item.ID == "evt_ap_new_before_dup" {
			t.Fatalf("expected no partial scoped enqueue for first item, got %#v", listResp.Items)
		}
	}
}

func TestServer_ApplicationEndpointPublishPayloadTooLarge(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.MaxBodyBytes = 32
	srv.LimitsForRoute = func(route string) (int64, int) {
		if route == "/r" {
			return 4, 0
		}
		return 0, 0
	}
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_oversize", "payload_b64": "aGVsbG8=" }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodePayloadTooLarge {
		t.Fatalf("expected code=%q, got %q", publishCodePayloadTooLarge, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `decoded payload (5 bytes) exceeds max_body (4 bytes) for route "/r"`) {
		t.Fatalf("expected detailed max_body error, got %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_ApplicationEndpointPublishHeadersTooLarge(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.MaxHeaderBytes = 32
	srv.LimitsForRoute = func(route string) (int64, int) {
		if route == "/r" {
			return 0, 4
		}
		return 0, 0
	}
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_headers_oversize", "headers": {"X":"hello"} }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeHeadersTooLarge {
		t.Fatalf("expected code=%q, got %q", publishCodeHeadersTooLarge, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `headers (6 bytes) exceed max_headers (4 bytes) for route "/r"`) {
		t.Fatalf("expected detailed max_headers error, got %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_ApplicationEndpointPublishRejectsInvalidHeaders(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/publish", strings.NewReader(`{
  "items": [{ "id": "evt_ap_invalid_header", "headers": {"X-Test":"ok\nbad"} }]
}`))
	req.Header.Set(auditReasonHeader, "operator_publish")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidHeader {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidHeader, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "invalid field value") {
		t.Fatalf("expected invalid header detail, got %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %#v", errResp.ItemIndex)
	}
}

func TestServer_ApplicationEndpointCancelByFilterRequiresAuditReason(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/cancel_by_filter", strings.NewReader(`{
  "state":"queued",
  "limit":10
}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when audit reason header missing, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditReasonRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditReasonRequired, errResp.Code)
	}
}

func TestServer_ApplicationEndpointCancelByFilterAuditHook(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	var got ManagementMutationAuditEvent
	srv.AuditManagementMutation = func(event ManagementMutationAuditEvent) {
		got = event
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/cancel_by_filter", strings.NewReader(`{
  "state":"queued",
  "limit":10
}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	req.Header.Set(auditActorHeader, "ops@example.test")
	req.Header.Set(auditRequestIDHeader, "req-321")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if got.Operation != "cancel_by_filter" || got.Application != "billing" || got.EndpointName != "invoice.created" || got.Route != "/r" {
		t.Fatalf("unexpected audit event scope: %#v", got)
	}
	if got.Reason != "operator_cleanup" || got.Actor != "ops@example.test" || got.RequestID != "req-321" {
		t.Fatalf("unexpected audit metadata: %#v", got)
	}
	if got.Changed != 1 || got.Matched != 1 {
		t.Fatalf("unexpected audit counts: %#v", got)
	}
}

func TestServer_ApplicationEndpointCancelByFilterRequiresActorByScopedManagedActorPolicy(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}

	srv := NewServer(store)
	srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/cancel_by_filter", strings.NewReader(`{
  "state":"queued",
  "limit":10
}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when scoped managed actor policy requires actor, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditActorRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditActorRequired, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "actor_allow/actor_prefix") {
		t.Fatalf("expected scoped managed actor policy detail, got %q", errResp.Detail)
	}
}

func TestServer_ApplicationEndpointCancelByFilterRejectsActorByScopedManagedActorPolicy(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}

	srv := NewServer(store)
	srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
	srv.PublishScopedManagedActorPrefixes = []string{"role:ops/"}
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/cancel_by_filter", strings.NewReader(`{
  "state":"queued",
  "limit":10
}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	req.Header.Set(auditActorHeader, "dev@example.test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when scoped managed actor is not allowed, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditActorNotAllowed {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditActorNotAllowed, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "endpoint-scoped managed mutation") {
		t.Fatalf("expected endpoint-scoped managed mutation detail, got %q", errResp.Detail)
	}
	if !strings.Contains(errResp.Detail, "allowed actors:") || !strings.Contains(errResp.Detail, "allowed actor prefixes:") {
		t.Fatalf("expected allowed actor details, got %q", errResp.Detail)
	}
}

func TestServer_CancelMessagesByFilterDirectPathIgnoresScopedManagedActorPolicy(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}

	srv := NewServer(store)
	srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel_by_filter", strings.NewReader(`{"route":"/r","state":"queued","limit":10}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 on direct filter mutation without actor, got %d", rr.Code)
	}

	var resp messagesManageFilterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Canceled != 1 || resp.Matched != 1 {
		t.Fatalf("unexpected manage response: %#v", resp)
	}
}

func TestServer_FilterMutationsManagedSelectorRequireActorByScopedManagedActorPolicy(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel",
			path:  "/messages/cancel_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue",
			path:  "/messages/requeue_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume",
			path:  "/messages/resume_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
			srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
				if application == "billing" && endpointName == "invoice.created" {
					return "/managed", []string{"pull"}, true
				}
				return "", nil, false
			}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 when actor header missing, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeAuditActorRequired {
				t.Fatalf("expected code=%q, got %q", publishCodeAuditActorRequired, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "actor_allow/actor_prefix") {
				t.Fatalf("expected scoped managed actor policy detail, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsManagedSelectorRequireRequestIDByPolicy(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel",
			path:  "/messages/cancel_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue",
			path:  "/messages/requeue_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume",
			path:  "/messages/resume_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.PublishRequireAuditRequestID = true
			srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
				if application == "billing" && endpointName == "invoice.created" {
					return "/managed", []string{"pull"}, true
				}
				return "", nil, false
			}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			req.Header.Set(auditActorHeader, "ops@example.test")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 when request id header missing, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeAuditRequestIDRequired {
				t.Fatalf("expected code=%q, got %q", publishCodeAuditRequestIDRequired, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "defaults.publish_policy.require_request_id") {
				t.Fatalf("expected request id policy detail, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsManagedSelectorRejectActorByScopedManagedActorPolicy(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel",
			path:  "/messages/cancel_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue",
			path:  "/messages/requeue_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume",
			path:  "/messages/resume_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
			srv.PublishScopedManagedActorPrefixes = []string{"role:ops/"}
			srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
				if application == "billing" && endpointName == "invoice.created" {
					return "/managed", []string{"pull"}, true
				}
				return "", nil, false
			}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			req.Header.Set(auditActorHeader, "dev@example.test")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 when actor header is denied, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeAuditActorNotAllowed {
				t.Fatalf("expected code=%q, got %q", publishCodeAuditActorNotAllowed, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "endpoint-scoped managed mutation") {
				t.Fatalf("expected endpoint-scoped managed mutation detail, got %q", errResp.Detail)
			}
			if !strings.Contains(errResp.Detail, "allowed actors:") || !strings.Contains(errResp.Detail, "allowed actor prefixes:") {
				t.Fatalf("expected allowed actor details, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_IDMutationsRequireActorByScopedManagedActorPolicy(t *testing.T) {
	model := func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/managed",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name  string
		path  string
		state queue.State
	}{
		{name: "messages_cancel", path: "/messages/cancel", state: queue.StateQueued},
		{name: "messages_requeue", path: "/messages/requeue", state: queue.StateDead},
		{name: "messages_resume", path: "/messages/resume", state: queue.StateCanceled},
		{name: "dlq_requeue", path: "/dlq/requeue", state: queue.StateDead},
		{name: "dlq_delete", path: "/dlq/delete", state: queue.StateDead},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.ManagementModel = model
			srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(`{"ids":["msg_1"]}`))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 when actor header missing, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeAuditActorRequired {
				t.Fatalf("expected code=%q, got %q", publishCodeAuditActorRequired, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "actor_allow/actor_prefix") {
				t.Fatalf("expected scoped managed actor policy detail, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_IDMutationsRejectActorByScopedManagedActorPolicy(t *testing.T) {
	model := func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/managed",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name  string
		path  string
		state queue.State
	}{
		{name: "messages_cancel", path: "/messages/cancel", state: queue.StateQueued},
		{name: "messages_requeue", path: "/messages/requeue", state: queue.StateDead},
		{name: "messages_resume", path: "/messages/resume", state: queue.StateCanceled},
		{name: "dlq_requeue", path: "/dlq/requeue", state: queue.StateDead},
		{name: "dlq_delete", path: "/dlq/delete", state: queue.StateDead},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.ManagementModel = model
			srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
			srv.PublishScopedManagedActorPrefixes = []string{"role:ops/"}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(`{"ids":["msg_1"]}`))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			req.Header.Set(auditActorHeader, "dev@example.test")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 when actor is not allowed, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeAuditActorNotAllowed {
				t.Fatalf("expected code=%q, got %q", publishCodeAuditActorNotAllowed, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "endpoint-scoped managed mutation") {
				t.Fatalf("expected endpoint-scoped managed mutation detail, got %q", errResp.Detail)
			}
			if !strings.Contains(errResp.Detail, "allowed actors:") || !strings.Contains(errResp.Detail, "allowed actor prefixes:") {
				t.Fatalf("expected allowed actor details, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_IDMutationsUnmanagedIDsIgnoreScopedManagedActorPolicy(t *testing.T) {
	store := queue.NewMemoryStore()
	if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/direct", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue msg_1: %v", err)
	}

	srv := NewServer(store)
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       2,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/managed",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}
	srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel", strings.NewReader(`{"ids":["msg_1"]}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for unmanaged ID mutation without actor, got %d", rr.Code)
	}

	var resp messagesCancelResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Canceled != 1 {
		t.Fatalf("expected canceled=1, got %d", resp.Canceled)
	}
}

func TestServer_IDMutationsFailClosedWhenManagementModelUnavailable(t *testing.T) {
	store := queue.NewMemoryStore()
	if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue msg_1: %v", err)
	}

	srv := NewServer(store)
	srv.PublishScopedManagedFailClosed = true
	srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel", strings.NewReader(`{"ids":["msg_1"]}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when management model is unavailable in fail-closed mode, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedResolverMissing {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedResolverMissing, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "scoped id mutation policy") {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}

	lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
	if err != nil {
		t.Fatalf("lookup msg_1: %v", err)
	}
	if len(lookup.Items) != 1 || lookup.Items[0].State != queue.StateQueued {
		t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
	}
}

func TestServer_IDMutationsUseRouteOwnershipWhenManagedRouteSetUnavailableOrEmpty(t *testing.T) {
	cases := []struct {
		name            string
		configureServer func(*Server)
	}{
		{
			name: "managed_route_set_unavailable",
			configureServer: func(srv *Server) {
				// Keep ManagedRouteSet unset to simulate unavailable set context.
			},
		},
		{
			name: "managed_route_set_empty",
			configureServer: func(srv *Server) {
				srv.ManagedRouteSet = func() (map[string]struct{}, bool) {
					// Simulate stale managed-route set data that drops a managed route.
					return map[string]struct{}{}, true
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: queue.StateQueued}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.PublishScopedManagedFailClosed = true
			srv.PublishScopedManagedActorAllowlist = []string{"ops@example.test"}
			srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
				if route == "/managed" {
					return "billing", "invoice.created", true, true
				}
				return "", "", false, true
			}
			tc.configureServer(srv)

			req := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel", strings.NewReader(`{"ids":["msg_1"]}`))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 when scoped managed actor is missing, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeAuditActorRequired {
				t.Fatalf("expected code=%q, got %q", publishCodeAuditActorRequired, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "actor_allow/actor_prefix") {
				t.Fatalf("expected scoped managed actor policy detail, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != queue.StateQueued {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_IDMutationsFailClosedOnOwnershipSourceDrift(t *testing.T) {
	model := func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/managed",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name  string
		path  string
		state queue.State
	}{
		{name: "messages_cancel", path: "/messages/cancel", state: queue.StateQueued},
		{name: "messages_requeue", path: "/messages/requeue", state: queue.StateDead},
		{name: "messages_resume", path: "/messages/resume", state: queue.StateCanceled},
		{name: "dlq_requeue", path: "/dlq/requeue", state: queue.StateDead},
		{name: "dlq_delete", path: "/dlq/delete", state: queue.StateDead},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.ManagementModel = model
			srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
				if route != "/managed" {
					return "", "", false, true
				}
				// Drift from management model ownership on endpoint name.
				return "billing", "invoice.updated", true, true
			}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(`{"ids":["msg_1"]}`))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503 for ownership source drift, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedTargetMismatch {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "ownership sources are out of sync") {
				t.Fatalf("expected ownership-source drift detail, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_ApplicationEndpointMessagesBadRequest(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	routeHintReq := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/endpoints/invoice.created/messages?route=/r", nil)
	routeHintRR := httptest.NewRecorder()
	srv.ServeHTTP(routeHintRR, routeHintReq)
	if routeHintRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for forbidden route hint, got %d", routeHintRR.Code)
	}
	var routeHintErr publishErrorResponse
	if err := json.NewDecoder(routeHintRR.Body).Decode(&routeHintErr); err != nil {
		t.Fatalf("decode route hint error response: %v", err)
	}
	if routeHintErr.Code != publishCodeScopedSelectorForbidden {
		t.Fatalf("expected route hint code=%q, got %q", publishCodeScopedSelectorForbidden, routeHintErr.Code)
	}

	selectorHintReq := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/endpoints/invoice.created/messages?application=billing&endpoint_name=invoice.created", nil)
	selectorHintRR := httptest.NewRecorder()
	srv.ServeHTTP(selectorHintRR, selectorHintReq)
	if selectorHintRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for forbidden selector hints, got %d", selectorHintRR.Code)
	}
	var selectorHintErr publishErrorResponse
	if err := json.NewDecoder(selectorHintRR.Body).Decode(&selectorHintErr); err != nil {
		t.Fatalf("decode selector hint error response: %v", err)
	}
	if selectorHintErr.Code != publishCodeScopedSelectorForbidden {
		t.Fatalf("expected selector hint code=%q, got %q", publishCodeScopedSelectorForbidden, selectorHintErr.Code)
	}

	notFoundReq := httptest.NewRequest(http.MethodGet, "http://example/applications/billing/endpoints/missing/messages", nil)
	notFoundRR := httptest.NewRecorder()
	srv.ServeHTTP(notFoundRR, notFoundReq)
	if notFoundRR.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown endpoint, got %d", notFoundRR.Code)
	}
	var notFoundErr publishErrorResponse
	if err := json.NewDecoder(notFoundRR.Body).Decode(&notFoundErr); err != nil {
		t.Fatalf("decode not found error response: %v", err)
	}
	if notFoundErr.Code != publishCodeManagedEndpointNotFound {
		t.Fatalf("expected not found code=%q, got %q", publishCodeManagedEndpointNotFound, notFoundErr.Code)
	}

	partialReq := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/cancel_by_filter", strings.NewReader(`{"application":"billing","state":"queued"}`))
	partialReq.Header.Set(auditReasonHeader, "test")
	partialRR := httptest.NewRecorder()
	srv.ServeHTTP(partialRR, partialReq)
	if partialRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for partial selector in body, got %d", partialRR.Code)
	}
	var partialErr publishErrorResponse
	if err := json.NewDecoder(partialRR.Body).Decode(&partialErr); err != nil {
		t.Fatalf("decode partial selector error response: %v", err)
	}
	if partialErr.Code != publishCodeScopedSelectorForbidden {
		t.Fatalf("expected partial selector code=%q, got %q", publishCodeScopedSelectorForbidden, partialErr.Code)
	}

	routeBodyReq := httptest.NewRequest(http.MethodPost, "http://example/applications/billing/endpoints/invoice.created/messages/cancel_by_filter", strings.NewReader(`{"route":"/r","state":"queued"}`))
	routeBodyReq.Header.Set(auditReasonHeader, "test")
	routeBodyRR := httptest.NewRecorder()
	srv.ServeHTTP(routeBodyRR, routeBodyReq)
	if routeBodyRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for route hint in scoped filter body, got %d", routeBodyRR.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(routeBodyRR.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeScopedSelectorForbidden {
		t.Fatalf("expected code=%q, got %q", publishCodeScopedSelectorForbidden, errResp.Code)
	}
}

func TestServer_CancelMessagesByFilterStructuredError(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel_by_filter", strings.NewReader(`{
  "application":"billing",
  "state":"queued",
  "limit":10
}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeScopedSelectorMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeScopedSelectorMismatch, errResp.Code)
	}
}

func TestServer_FilterMutationsManagedSelectorRouteHintForbidden(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ResolveManaged = func(application, endpointName string) (route string, targets []string, ok bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "cancel",
			path: "/messages/cancel_by_filter",
			body: `{"route":"/r","application":"billing","endpoint_name":"invoice.created","state":"queued","limit":10}`,
		},
		{
			name: "requeue",
			path: "/messages/requeue_by_filter",
			body: `{"route":"/r","application":"billing","endpoint_name":"invoice.created","state":"dead","limit":10}`,
		},
		{
			name: "resume",
			path: "/messages/resume_by_filter",
			body: `{"route":"/r","application":"billing","endpoint_name":"invoice.created","state":"canceled","limit":10}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeScopedSelectorMismatch {
				t.Fatalf("expected code=%q, got %q", publishCodeScopedSelectorMismatch, errResp.Code)
			}
			if errResp.Detail != "route is not allowed when application and endpoint_name are set" {
				t.Fatalf("unexpected detail: %#v", errResp.Detail)
			}
		})
	}
}

func TestServer_FilterMutationsManagedSelectorRejectsInvalidLabels(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ResolveManaged = func(application, endpointName string) (route string, targets []string, ok bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/r", []string{"pull"}, true
		}
		return "", nil, false
	}

	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "cancel",
			path: "/messages/cancel_by_filter",
			body: `{"application":"billing core","endpoint_name":"invoice.created","state":"queued","limit":10}`,
		},
		{
			name: "requeue",
			path: "/messages/requeue_by_filter",
			body: `{"application":"billing core","endpoint_name":"invoice.created","state":"dead","limit":10}`,
		},
		{
			name: "resume",
			path: "/messages/resume_by_filter",
			body: `{"application":"billing core","endpoint_name":"invoice.created","state":"canceled","limit":10}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeInvalidBody {
				t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "application must match") {
				t.Fatalf("unexpected detail: %#v", errResp.Detail)
			}
		})
	}
}

func TestServer_FilterMutationsRejectUnknownBodyFields(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel",
			path:  "/messages/cancel_by_filter",
			body:  `{"route":"/r","state":"queued","limit":10,"unknown":true}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue",
			path:  "/messages/requeue_by_filter",
			body:  `{"route":"/r","state":"dead","limit":10,"unknown":true}`,
			state: queue.StateDead,
		},
		{
			name:  "resume",
			path:  "/messages/resume_by_filter",
			body:  `{"route":"/r","state":"canceled","limit":10,"unknown":true}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/r", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rr.Code)
			}
			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeInvalidBody {
				t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsManagedSelectorResolverUnavailable(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel",
			path:  "/messages/cancel_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue",
			path:  "/messages/requeue_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume",
			path:  "/messages/resume_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503 when managed resolver is unavailable, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedResolverMissing {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedResolverMissing, errResp.Code)
			}
			if errResp.Detail != "managed endpoint resolution is not configured" {
				t.Fatalf("unexpected detail: %#v", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsManagedSelectorNotFound(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel",
			path:  "/messages/cancel_by_filter",
			body:  `{"application":"billing","endpoint_name":"missing","state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue",
			path:  "/messages/requeue_by_filter",
			body:  `{"application":"billing","endpoint_name":"missing","state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume",
			path:  "/messages/resume_by_filter",
			body:  `{"application":"billing","endpoint_name":"missing","state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.ResolveManaged = func(application, endpointName string) (route string, targets []string, ok bool) {
				return "", nil, false
			}
			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusNotFound {
				t.Fatalf("expected 404 when managed endpoint selector is not found, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedEndpointNotFound {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedEndpointNotFound, errResp.Code)
			}
			if errResp.Detail != "managed endpoint not found" {
				t.Fatalf("unexpected detail: %#v", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsManagedSelectorOwnershipMismatch(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel",
			path:  "/messages/cancel_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue",
			path:  "/messages/requeue_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume",
			path:  "/messages/resume_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
				if application == "billing" && endpointName == "invoice.created" {
					return "/managed", []string{"pull"}, true
				}
				return "", nil, false
			}
			srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
				if route == "/managed" {
					return "billing", "invoice.updated", true, true
				}
				return "", "", false, true
			}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedTargetMismatch {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, `owned by ("billing", "invoice.updated")`) {
				t.Fatalf("unexpected detail: %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsManagedSelectorOwnershipRequiresManagedRoute(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel",
			path:  "/messages/cancel_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue",
			path:  "/messages/requeue_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume",
			path:  "/messages/resume_by_filter",
			body:  `{"application":"billing","endpoint_name":"invoice.created","state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.ResolveManaged = func(application, endpointName string) (string, []string, bool) {
				if application == "billing" && endpointName == "invoice.created" {
					return "/managed", []string{"pull"}, true
				}
				return "", nil, false
			}
			srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
				if route == "/managed" {
					return "", "", false, true
				}
				return "", "", false, true
			}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedTargetMismatch {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, `is not managed by route ownership policy`) {
				t.Fatalf("unexpected detail: %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsRouteSelectorFailClosedWhenManagementModelUnavailable(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		body         string
		initialState queue.State
	}{
		{
			name:         "cancel",
			path:         "/messages/cancel_by_filter",
			body:         `{"route":"/managed","state":"queued","limit":10}`,
			initialState: queue.StateQueued,
		},
		{
			name:         "requeue",
			path:         "/messages/requeue_by_filter",
			body:         `{"route":"/managed","state":"dead","limit":10}`,
			initialState: queue.StateDead,
		},
		{
			name:         "resume",
			path:         "/messages/resume_by_filter",
			body:         `{"route":"/managed","state":"canceled","limit":10}`,
			initialState: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.initialState}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.PublishScopedManagedFailClosed = true

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedResolverMissing {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedResolverMissing, errResp.Code)
			}
			if errResp.Detail != "management model is unavailable for route selector policy evaluation" {
				t.Fatalf("unexpected detail: %#v", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.initialState {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsRouteSelectorFailClosedOnOwnershipSourceDrift(t *testing.T) {
	model := func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/managed",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name         string
		path         string
		body         string
		initialState queue.State
	}{
		{
			name:         "cancel",
			path:         "/messages/cancel_by_filter",
			body:         `{"route":"/managed","state":"queued","limit":10}`,
			initialState: queue.StateQueued,
		},
		{
			name:         "requeue",
			path:         "/messages/requeue_by_filter",
			body:         `{"route":"/managed","state":"dead","limit":10}`,
			initialState: queue.StateDead,
		},
		{
			name:         "resume",
			path:         "/messages/resume_by_filter",
			body:         `{"route":"/managed","state":"canceled","limit":10}`,
			initialState: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.initialState}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.ManagementModel = model
			srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
				if route != "/managed" {
					return "", "", false, true
				}
				// Drift from management model ownership on endpoint name.
				return "billing", "invoice.updated", true, true
			}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503 for ownership source drift, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedTargetMismatch {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "ownership sources are out of sync") {
				t.Fatalf("expected ownership-source drift detail, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.initialState {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsManagedRouteRequireManagedSelector(t *testing.T) {
	model := func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/managed",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel_route",
			path:  "/messages/cancel_by_filter",
			body:  `{"route":"/managed","state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "cancel_global",
			path:  "/messages/cancel_by_filter",
			body:  `{"state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue_route",
			path:  "/messages/requeue_by_filter",
			body:  `{"route":"/managed","state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "requeue_global",
			path:  "/messages/requeue_by_filter",
			body:  `{"state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume_route",
			path:  "/messages/resume_by_filter",
			body:  `{"route":"/managed","state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
		{
			name:  "resume_global",
			path:  "/messages/resume_by_filter",
			body:  `{"state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.ManagementModel = model

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 when managed filter mutation is not scoped via application+endpoint_name, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedSelectorRequired {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedSelectorRequired, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "use application+endpoint_name selector") {
				t.Fatalf("expected managed selector detail, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsManagedRouteRequireManagedSelector_WithManagedRouteSetFallback(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel_route",
			path:  "/messages/cancel_by_filter",
			body:  `{"route":"/managed","state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "cancel_global",
			path:  "/messages/cancel_by_filter",
			body:  `{"state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue_route",
			path:  "/messages/requeue_by_filter",
			body:  `{"route":"/managed","state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume_route",
			path:  "/messages/resume_by_filter",
			body:  `{"route":"/managed","state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.ManagedRouteSet = func() (map[string]struct{}, bool) {
				return map[string]struct{}{
					"/managed": {},
				}, true
			}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 when managed filter mutation is not scoped via application+endpoint_name, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedSelectorRequired {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedSelectorRequired, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "use application+endpoint_name selector") {
				t.Fatalf("expected managed selector detail, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsManagedRouteRequireManagedSelector_WithManagedRouteInfoFallbackWhenManagedRouteSetEmpty(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel_route",
			path:  "/messages/cancel_by_filter",
			body:  `{"route":"/managed","state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue_route",
			path:  "/messages/requeue_by_filter",
			body:  `{"route":"/managed","state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume_route",
			path:  "/messages/resume_by_filter",
			body:  `{"route":"/managed","state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
				if route == "/managed" {
					return "billing", "invoice.created", true, true
				}
				return "", "", false, true
			}
			srv.ManagedRouteSet = func() (map[string]struct{}, bool) {
				// Simulate stale managed-route set data that drops a managed route.
				return map[string]struct{}{}, true
			}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 when managed filter mutation is not scoped via application+endpoint_name, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedSelectorRequired {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedSelectorRequired, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "use application+endpoint_name selector") {
				t.Fatalf("expected managed selector detail, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsRouteSelectorUsesRouteOwnershipWhenManagedRouteSetUnavailable(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		state queue.State
	}{
		{
			name:  "cancel_route",
			path:  "/messages/cancel_by_filter",
			body:  `{"route":"/managed","state":"queued","limit":10}`,
			state: queue.StateQueued,
		},
		{
			name:  "requeue_route",
			path:  "/messages/requeue_by_filter",
			body:  `{"route":"/managed","state":"dead","limit":10}`,
			state: queue.StateDead,
		},
		{
			name:  "resume_route",
			path:  "/messages/resume_by_filter",
			body:  `{"route":"/managed","state":"canceled","limit":10}`,
			state: queue.StateCanceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: tc.state}); err != nil {
				t.Fatalf("enqueue msg_1: %v", err)
			}

			srv := NewServer(store)
			srv.PublishScopedManagedFailClosed = true
			srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
				if route == "/managed" {
					return "billing", "invoice.created", true, true
				}
				return "", "", false, true
			}

			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 when managed filter mutation is not scoped via application+endpoint_name, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeManagedSelectorRequired {
				t.Fatalf("expected code=%q, got %q", publishCodeManagedSelectorRequired, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "use application+endpoint_name selector") {
				t.Fatalf("expected managed selector detail, got %q", errResp.Detail)
			}

			lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
			if err != nil {
				t.Fatalf("lookup msg_1: %v", err)
			}
			if len(lookup.Items) != 1 || lookup.Items[0].State != tc.state {
				t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
			}
		})
	}
}

func TestServer_FilterMutationsManagedRouteRequireManagedSelector_PrefersManagementModel(t *testing.T) {
	store := queue.NewMemoryStore()
	if err := store.Enqueue(queue.Envelope{ID: "msg_1", Route: "/managed", Target: "pull", State: queue.StateQueued}); err != nil {
		t.Fatalf("enqueue msg_1: %v", err)
	}

	srv := NewServer(store)
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/managed",
							Mode:    "pull",
							Targets: []string{"pull"},
						},
					},
				},
			},
		}
	}
	srv.ManagedRouteSet = func() (map[string]struct{}, bool) {
		// Simulate stale callback data that incorrectly reports no managed routes.
		return map[string]struct{}{}, true
	}

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel_by_filter", strings.NewReader(`{"route":"/managed","state":"queued","limit":10}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when managed filter mutation is not scoped via application+endpoint_name, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedSelectorRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedSelectorRequired, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "use application+endpoint_name selector") {
		t.Fatalf("expected managed selector detail, got %q", errResp.Detail)
	}

	lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: []string{"msg_1"}})
	if err != nil {
		t.Fatalf("lookup msg_1: %v", err)
	}
	if len(lookup.Items) != 1 || lookup.Items[0].State != queue.StateQueued {
		t.Fatalf("unexpected message state after rejected mutation: %#v", lookup.Items)
	}
}

func TestServer_RequeueMessagesByFilter(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "d_1", Route: "/r", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-4 * time.Minute)}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "c_1", Route: "/r", Target: "pull", State: queue.StateCanceled, ReceivedAt: now.Add(-3 * time.Minute)}); err != nil {
		t.Fatalf("enqueue c_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/requeue_by_filter", strings.NewReader(`{"route":"/r","state":"dead","limit":10}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesManageFilterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Requeued != 1 {
		t.Fatalf("expected requeued=1, got %d", resp.Requeued)
	}
	if resp.Matched != 1 || resp.PreviewOnly {
		t.Fatalf("unexpected manage response: %#v", resp)
	}
}

func TestServer_CancelMessagesByFilterPreviewOnly(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "q_1", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "q_2", Route: "/r", Target: "pull", State: queue.StateQueued, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue q_2: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel_by_filter", strings.NewReader(`{"route":"/r","state":"queued","limit":10,"preview_only":true}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesManageFilterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.PreviewOnly || resp.Matched != 2 || resp.Canceled != 0 {
		t.Fatalf("unexpected preview response: %#v", resp)
	}

	listReq := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/r&state=queued&limit=10", nil)
	listRR := httptest.NewRecorder()
	srv.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for queued list, got %d", listRR.Code)
	}
	var listResp messagesResponse
	if err := json.NewDecoder(listRR.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Items) != 2 {
		t.Fatalf("expected 2 queued items after preview, got %d", len(listResp.Items))
	}
}

func TestServer_RequeueMessagesByFilterPreviewOnly(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "d_1", Route: "/r", Target: "pull", State: queue.StateDead, ReceivedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("enqueue d_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "c_1", Route: "/r", Target: "pull", State: queue.StateCanceled, ReceivedAt: now.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("enqueue c_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/requeue_by_filter", strings.NewReader(`{"route":"/r","limit":10,"preview_only":true}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp messagesManageFilterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.PreviewOnly || resp.Matched != 2 || resp.Requeued != 0 {
		t.Fatalf("unexpected preview response: %#v", resp)
	}

	listReq := httptest.NewRequest(http.MethodGet, "http://example/messages?route=/r&state=queued&limit=10", nil)
	listRR := httptest.NewRecorder()
	srv.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for queued list, got %d", listRR.Code)
	}
	var listResp messagesResponse
	if err := json.NewDecoder(listRR.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Items) != 0 {
		t.Fatalf("expected 0 queued items after preview, got %d", len(listResp.Items))
	}
}

func TestServer_ListAttempts(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	if err := store.RecordAttempt(queue.DeliveryAttempt{
		ID:         "att_1",
		EventID:    "evt_1",
		Route:      "/r",
		Target:     "https://example.internal/a",
		Attempt:    1,
		StatusCode: 503,
		Outcome:    queue.AttemptOutcomeRetry,
		CreatedAt:  now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("record att_1: %v", err)
	}
	if err := store.RecordAttempt(queue.DeliveryAttempt{
		ID:         "att_2",
		EventID:    "evt_1",
		Route:      "/r",
		Target:     "https://example.internal/a",
		Attempt:    2,
		StatusCode: 502,
		Outcome:    queue.AttemptOutcomeDead,
		DeadReason: "max_retries",
		CreatedAt:  now.Add(-1 * time.Minute),
	}); err != nil {
		t.Fatalf("record att_2: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodGet, "http://example/attempts?route=/r&event_id=evt_1&outcome=dead&limit=1", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp attemptsResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "att_2" {
		t.Fatalf("expected att_2, got %q", resp.Items[0].ID)
	}
	if resp.Items[0].Outcome != queue.AttemptOutcomeDead {
		t.Fatalf("expected dead outcome, got %q", resp.Items[0].Outcome)
	}
	if resp.Items[0].DeadReason != "max_retries" {
		t.Fatalf("expected max_retries, got %q", resp.Items[0].DeadReason)
	}
}

func TestServer_ListAttemptsManagedSelector(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	if err := store.RecordAttempt(queue.DeliveryAttempt{
		ID:         "att_m_1",
		EventID:    "evt_m_1",
		Route:      "/managed",
		Target:     "https://example.internal/a",
		Attempt:    1,
		StatusCode: 503,
		Outcome:    queue.AttemptOutcomeRetry,
		CreatedAt:  now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("record att_m_1: %v", err)
	}
	if err := store.RecordAttempt(queue.DeliveryAttempt{
		ID:         "att_m_2",
		EventID:    "evt_m_1",
		Route:      "/managed",
		Target:     "https://example.internal/a",
		Attempt:    2,
		StatusCode: 502,
		Outcome:    queue.AttemptOutcomeDead,
		DeadReason: "max_retries",
		CreatedAt:  now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("record att_m_2: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (route string, targets []string, ok bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/managed", []string{"https://example.internal/a"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/attempts?application=billing&endpoint_name=invoice.created&outcome=dead&limit=1", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp attemptsResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "att_m_2" {
		t.Fatalf("expected att_m_2, got %q", resp.Items[0].ID)
	}
}

func TestServer_ListAttemptsManagedSelectorOwnershipMismatch(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	if err := store.RecordAttempt(queue.DeliveryAttempt{
		ID:         "att_m_1",
		EventID:    "evt_m_1",
		Route:      "/managed",
		Target:     "https://example.internal/a",
		Attempt:    1,
		StatusCode: 503,
		Outcome:    queue.AttemptOutcomeRetry,
		CreatedAt:  now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("record att_m_1: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (route string, targets []string, ok bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/managed", []string{"https://example.internal/a"}, true
		}
		return "", nil, false
	}
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if route == "/managed" {
			return "billing", "invoice.updated", true, true
		}
		return "", "", false, true
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/attempts?application=billing&endpoint_name=invoice.created&outcome=retry&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, `owned by ("billing", "invoice.updated")`) {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_ListAttemptsManagedSelectorTargetMismatch(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	if err := store.RecordAttempt(queue.DeliveryAttempt{
		ID:         "att_m_1",
		EventID:    "evt_m_1",
		Route:      "/managed",
		Target:     "pull",
		Attempt:    1,
		StatusCode: 503,
		Outcome:    queue.AttemptOutcomeRetry,
		CreatedAt:  now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("record att_m_1: %v", err)
	}

	srv := NewServer(store)
	srv.ResolveManaged = func(application, endpointName string) (route string, targets []string, ok bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/managed", []string{"pull"}, true
		}
		return "", nil, false
	}
	srv.TargetsForRoute = func(route string) []string {
		if route == "/managed" {
			return []string{"https://example.internal/a"}
		}
		return nil
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/attempts?application=billing&endpoint_name=invoice.created&outcome=retry&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "targets for route") {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_ListAttemptsRouteSelectorOwnershipSourceMismatch(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))

	if err := store.RecordAttempt(queue.DeliveryAttempt{
		ID:         "att_m_1",
		EventID:    "evt_m_1",
		Route:      "/managed",
		Target:     "https://example.internal/a",
		Attempt:    1,
		StatusCode: 503,
		Outcome:    queue.AttemptOutcomeRetry,
		CreatedAt:  now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("record att_m_1: %v", err)
	}

	srv := NewServer(store)
	srv.ManagementModel = func() ManagementModel {
		return ManagementModel{
			RouteCount:       1,
			ApplicationCount: 1,
			EndpointCount:    1,
			Applications: []ManagementApplication{
				{
					Name:          "billing",
					EndpointCount: 1,
					Endpoints: []ManagementEndpoint{
						{
							Name:    "invoice.created",
							Route:   "/managed",
							Mode:    "deliver",
							Targets: []string{"https://example.internal/a"},
						},
					},
				},
			},
		}
	}
	srv.ManagedRouteInfoForRoute = func(route string) (string, string, bool, bool) {
		if route == "/managed" {
			return "billing", "invoice.updated", true, true
		}
		return "", "", false, true
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/attempts?route=/managed&outcome=retry&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedTargetMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedTargetMismatch, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "ownership sources are out of sync") {
		t.Fatalf("unexpected detail: %q", errResp.Detail)
	}
}

func TestServer_ListAttemptsManagedSelectorRouteHintForbidden(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ResolveManaged = func(application, endpointName string) (route string, targets []string, ok bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/managed", []string{"https://example.internal/a"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/attempts?route=/managed&application=billing&endpoint_name=invoice.created", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeScopedSelectorMismatch {
		t.Fatalf("expected code=%q, got %q", publishCodeScopedSelectorMismatch, errResp.Code)
	}
	if errResp.Detail != "route is not allowed when application and endpoint_name are set" {
		t.Fatalf("unexpected detail: %#v", errResp.Detail)
	}
}

func TestServer_ListAttemptsManagedSelectorRejectsInvalidLabels(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ResolveManaged = func(application, endpointName string) (route string, targets []string, ok bool) {
		if application == "billing" && endpointName == "invoice.created" {
			return "/managed", []string{"https://example.internal/a"}, true
		}
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/attempts?application=billing%20core&endpoint_name=invoice.created", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "application must match") {
		t.Fatalf("unexpected detail: %#v", errResp.Detail)
	}
}

func TestServer_ListAttemptsManagedSelectorNotFound(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	srv.ResolveManaged = func(application, endpointName string) (route string, targets []string, ok bool) {
		return "", nil, false
	}

	req := httptest.NewRequest(http.MethodGet, "http://example/attempts?application=billing&endpoint_name=missing", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when managed selector is not found, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedEndpointNotFound {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedEndpointNotFound, errResp.Code)
	}
	if errResp.Detail != "managed endpoint not found" {
		t.Fatalf("unexpected detail: %#v", errResp.Detail)
	}
}

func TestServer_ListAttemptsManagedSelectorResolverUnavailable(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "http://example/attempts?application=billing&endpoint_name=invoice.created", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when managed resolver is unavailable, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeManagedResolverMissing {
		t.Fatalf("expected code=%q, got %q", publishCodeManagedResolverMissing, errResp.Code)
	}
	if errResp.Detail != "managed endpoint resolution is not configured" {
		t.Fatalf("unexpected detail: %#v", errResp.Detail)
	}
}

func TestServer_ListAttemptsBadOutcome(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	req := httptest.NewRequest(http.MethodGet, "http://example/attempts?outcome=invalid", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != readCodeInvalidQuery {
		t.Fatalf("expected code=%q, got %q", readCodeInvalidQuery, errResp.Code)
	}
}

func TestServer_DLQRequeue(t *testing.T) {
	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))

	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead, DeadReason: "max_retries"}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/dlq/requeue", strings.NewReader(`{"ids":["dead_1"]}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp dlqRequeueResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Requeued != 1 {
		t.Fatalf("expected requeued=1, got %d", resp.Requeued)
	}

	deadResp, err := store.ListDead(queue.DeadListRequest{Route: "/r", Limit: 10})
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(deadResp.Items) != 0 {
		t.Fatalf("expected empty DLQ, got %#v", deadResp.Items)
	}
}

func TestServer_DLQDelete(t *testing.T) {
	store := queue.NewMemoryStore()
	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead, DeadReason: "max_retries"}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/dlq/delete", strings.NewReader(`{"ids":["dead_1"]}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp dlqDeleteResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Deleted != 1 {
		t.Fatalf("expected deleted=1, got %d", resp.Deleted)
	}

	deadResp, err := store.ListDead(queue.DeadListRequest{Route: "/r", Limit: 10})
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(deadResp.Items) != 0 {
		t.Fatalf("expected empty DLQ, got %#v", deadResp.Items)
	}
}

func TestServer_DLQRequeueRequiresAuditReason(t *testing.T) {
	store := queue.NewMemoryStore()
	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead, DeadReason: "max_retries"}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/dlq/requeue", strings.NewReader(`{"ids":["dead_1"]}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when audit reason header missing, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditReasonRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditReasonRequired, errResp.Code)
	}
}

func TestServer_DLQDeleteRequiresAuditReason(t *testing.T) {
	store := queue.NewMemoryStore()
	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead, DeadReason: "max_retries"}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/dlq/delete", strings.NewReader(`{"ids":["dead_1"]}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when audit reason header missing, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeAuditReasonRequired {
		t.Fatalf("expected code=%q, got %q", publishCodeAuditReasonRequired, errResp.Code)
	}
}

func TestServer_DLQManageBadRequest(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	req := httptest.NewRequest(http.MethodPost, "http://example/dlq/requeue", strings.NewReader(`{"ids":[]}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
	}
}

func TestServer_DLQManageRejectsUnknownBodyFields(t *testing.T) {
	store := queue.NewMemoryStore()
	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead, DeadReason: "max_retries"}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}

	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodPost, "http://example/dlq/requeue", strings.NewReader(`{"ids":["dead_1"],"unknown":true}`))
	req.Header.Set(auditReasonHeader, "operator_cleanup")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
	}

	deadResp, err := store.ListDead(queue.DeadListRequest{Route: "/r", Limit: 10})
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(deadResp.Items) != 1 || deadResp.Items[0].ID != "dead_1" {
		t.Fatalf("unexpected DLQ state after rejected request: %#v", deadResp.Items)
	}
}

func TestServer_MessagesBadRequest(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	listReq := httptest.NewRequest(http.MethodGet, "http://example/messages?state=invalid", nil)
	listRR := httptest.NewRecorder()
	srv.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", listRR.Code)
	}
	var listErr publishErrorResponse
	if err := json.NewDecoder(listRR.Body).Decode(&listErr); err != nil {
		t.Fatalf("decode list error response: %v", err)
	}
	if listErr.Code != readCodeInvalidQuery {
		t.Fatalf("expected list code=%q, got %q", readCodeInvalidQuery, listErr.Code)
	}

	cancelReq := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel", strings.NewReader(`{"ids":[]}`))
	cancelReq.Header.Set(auditReasonHeader, "operator_cleanup")
	cancelRR := httptest.NewRecorder()
	srv.ServeHTTP(cancelRR, cancelReq)
	if cancelRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", cancelRR.Code)
	}
	var cancelErr publishErrorResponse
	if err := json.NewDecoder(cancelRR.Body).Decode(&cancelErr); err != nil {
		t.Fatalf("decode cancel error response: %v", err)
	}
	if cancelErr.Code != publishCodeInvalidBody {
		t.Fatalf("expected cancel code=%q, got %q", publishCodeInvalidBody, cancelErr.Code)
	}

	requeueReq := httptest.NewRequest(http.MethodPost, "http://example/messages/requeue", strings.NewReader(`{"ids":[]}`))
	requeueReq.Header.Set(auditReasonHeader, "operator_cleanup")
	requeueRR := httptest.NewRecorder()
	srv.ServeHTTP(requeueRR, requeueReq)
	if requeueRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", requeueRR.Code)
	}
	var requeueErr publishErrorResponse
	if err := json.NewDecoder(requeueRR.Body).Decode(&requeueErr); err != nil {
		t.Fatalf("decode requeue error response: %v", err)
	}
	if requeueErr.Code != publishCodeInvalidBody {
		t.Fatalf("expected requeue code=%q, got %q", publishCodeInvalidBody, requeueErr.Code)
	}

	resumeReq := httptest.NewRequest(http.MethodPost, "http://example/messages/resume", strings.NewReader(`{"ids":[]}`))
	resumeReq.Header.Set(auditReasonHeader, "operator_cleanup")
	resumeRR := httptest.NewRecorder()
	srv.ServeHTTP(resumeRR, resumeReq)
	if resumeRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resumeRR.Code)
	}
	var resumeErr publishErrorResponse
	if err := json.NewDecoder(resumeRR.Body).Decode(&resumeErr); err != nil {
		t.Fatalf("decode resume error response: %v", err)
	}
	if resumeErr.Code != publishCodeInvalidBody {
		t.Fatalf("expected resume code=%q, got %q", publishCodeInvalidBody, resumeErr.Code)
	}

	cancelByFilterReq := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel_by_filter", strings.NewReader(`{"state":"delivered"}`))
	cancelByFilterReq.Header.Set(auditReasonHeader, "operator_cleanup")
	cancelByFilterRR := httptest.NewRecorder()
	srv.ServeHTTP(cancelByFilterRR, cancelByFilterReq)
	if cancelByFilterRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", cancelByFilterRR.Code)
	}

	requeueByFilterReq := httptest.NewRequest(http.MethodPost, "http://example/messages/requeue_by_filter", strings.NewReader(`{"state":"leased"}`))
	requeueByFilterReq.Header.Set(auditReasonHeader, "operator_cleanup")
	requeueByFilterRR := httptest.NewRecorder()
	srv.ServeHTTP(requeueByFilterRR, requeueByFilterReq)
	if requeueByFilterRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", requeueByFilterRR.Code)
	}

	managedFilterReq := httptest.NewRequest(http.MethodPost, "http://example/messages/cancel_by_filter", strings.NewReader(`{"application":"billing","state":"queued"}`))
	managedFilterReq.Header.Set(auditReasonHeader, "operator_cleanup")
	managedFilterRR := httptest.NewRecorder()
	srv.ServeHTTP(managedFilterRR, managedFilterReq)
	if managedFilterRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for partial managed selector, got %d", managedFilterRR.Code)
	}
}

func TestServer_FilterMutationsRejectNonAbsoluteRoute(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())

	tests := []struct {
		path string
		body string
	}{
		{
			path: "/messages/cancel_by_filter",
			body: `{"route":"relative","state":"queued","limit":10}`,
		},
		{
			path: "/messages/requeue_by_filter",
			body: `{"route":"relative","state":"dead","limit":10}`,
		},
		{
			path: "/messages/resume_by_filter",
			body: `{"route":"relative","state":"canceled","limit":10}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set(auditReasonHeader, "operator_cleanup")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rr.Code)
			}

			var errResp publishErrorResponse
			if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResp.Code != publishCodeInvalidBody {
				t.Fatalf("expected code=%q, got %q", publishCodeInvalidBody, errResp.Code)
			}
			if !strings.Contains(errResp.Detail, "route must start with '/'") {
				t.Fatalf("unexpected detail: %q", errResp.Detail)
			}
		})
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

func decodeManagementError(t *testing.T, rr *httptest.ResponseRecorder) publishErrorResponse {
	t.Helper()
	if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected JSON content-type, got %q", ct)
	}
	var out publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if strings.TrimSpace(out.Code) == "" || strings.TrimSpace(out.Detail) == "" {
		t.Fatalf("expected non-empty code/detail, got %#v", out)
	}
	return out
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

// ---------- publish hardening tests ----------

func TestServer_PublishMessagesRejectsEmptyBatch(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code %q, got %q", publishCodeInvalidBody, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "between 1 and") {
		t.Fatalf("expected batch-size error detail, got %q", errResp.Detail)
	}
}

func TestServer_PublishMessagesRejectsOversizedBatch(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	// Build a batch with 1001 items (maxListLimit=1000)
	var items []string
	for i := range 1001 {
		items = append(items, fmt.Sprintf(`{"id":"item_%d","route":"/r","target":"pull"}`, i))
	}
	body := `{"items":[` + strings.Join(items, ",") + `]}`

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(body))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code %q, got %q", publishCodeInvalidBody, errResp.Code)
	}
}

func TestServer_PublishMessagesRejectsDuplicateIDsInBatch(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[
			{"id":"dup","route":"/r","target":"pull"},
			{"id":"dup","route":"/r","target":"pull"}
		]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code %q, got %q", publishCodeInvalidBody, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "duplicate") {
		t.Fatalf("expected duplicate detail, got %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 1 {
		t.Fatalf("expected item_index=1, got %v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesQueueFull(t *testing.T) {
	store := queue.NewMemoryStore(queue.WithQueueLimits(1, "reject"))
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	// Fill the queue first
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[{"id":"fill_1","route":"/r","target":"pull"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("fill: expected 200, got %d", rr.Code)
	}

	// Second publish should fail with queue_full
	req = httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[{"id":"fill_2","route":"/r","target":"pull"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeQueueFull {
		t.Fatalf("expected code %q, got %q", publishCodeQueueFull, errResp.Code)
	}
}

func TestServer_PublishMessagesQueueFullMidBatchPartialEnqueue(t *testing.T) {
	store := queue.NewMemoryStore(queue.WithQueueLimits(2, "reject"))
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	// Publish batch of 3 — with batch enqueue (all-or-nothing), all are rejected
	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[
			{"id":"mid_1","route":"/r","target":"pull"},
			{"id":"mid_2","route":"/r","target":"pull"},
			{"id":"mid_3","route":"/r","target":"pull"}
		]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeQueueFull {
		t.Fatalf("expected code %q, got %q", publishCodeQueueFull, errResp.Code)
	}

	// No items committed (transactional batch enqueue is all-or-nothing).
	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Total != 0 {
		t.Fatalf("expected 0 items in queue (all-or-nothing), got %d", stats.Total)
	}
}

func TestServer_PublishMessagesRoundTripFieldFidelity(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	payloadRaw := "hello world"
	payloadB64 := base64.StdEncoding.EncodeToString([]byte(payloadRaw))

	body := fmt.Sprintf(`{"items":[{
		"id": "fidelity_1",
		"route": "/webhooks/github",
		"target": "pull",
		"payload_b64": %q,
		"headers": {"X-Custom": "value1"},
		"trace": {"request_id": "req-abc"},
		"received_at": "2026-03-01T11:55:00Z",
		"next_run_at": "2026-03-01T11:59:00Z"
	}]}`, payloadB64)

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(body))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	// Verify fields via ListMessages (avoids Dequeue overwriting NextRunAt with lease time)
	msgs, err := store.ListMessages(queue.MessageListRequest{
		Route:          "/webhooks/github",
		Target:         "pull",
		State:          queue.StateQueued,
		Limit:          1,
		IncludePayload: true,
		IncludeHeaders: true,
		IncludeTrace:   true,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(msgs.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(msgs.Items))
	}

	env := msgs.Items[0]
	if env.ID != "fidelity_1" {
		t.Fatalf("ID: got %q, want fidelity_1", env.ID)
	}
	if env.Route != "/webhooks/github" {
		t.Fatalf("Route: got %q", env.Route)
	}
	if env.Target != "pull" {
		t.Fatalf("Target: got %q", env.Target)
	}
	if string(env.Payload) != payloadRaw {
		t.Fatalf("Payload: got %q, want %q", env.Payload, payloadRaw)
	}
	if env.Headers["X-Custom"] != "value1" {
		t.Fatalf("Headers: got %v", env.Headers)
	}
	if env.Trace["request_id"] != "req-abc" {
		t.Fatalf("Trace: got %v", env.Trace)
	}
	wantReceived := time.Date(2026, 3, 1, 11, 55, 0, 0, time.UTC)
	if !env.ReceivedAt.Equal(wantReceived) {
		t.Fatalf("ReceivedAt: got %v, want %v", env.ReceivedAt, wantReceived)
	}
	wantNext := time.Date(2026, 3, 1, 11, 59, 0, 0, time.UTC)
	if !env.NextRunAt.Equal(wantNext) {
		t.Fatalf("NextRunAt: got %v, want %v", env.NextRunAt, wantNext)
	}
}

func TestServer_PublishMessagesStoreUnavailable(t *testing.T) {
	srv := NewServer(nil)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[{"id":"x","route":"/r","target":"pull"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeStoreUnavailable {
		t.Fatalf("expected code %q, got %q", publishCodeStoreUnavailable, errResp.Code)
	}
}

func TestServer_PublishMessagesRejectsInvalidReceivedAt(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[{"id":"x","route":"/r","target":"pull","received_at":"not-a-date"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeInvalidReceivedAt {
		t.Fatalf("expected code %q, got %q", publishCodeInvalidReceivedAt, errResp.Code)
	}
}

func TestServer_PublishMessagesRejectsInvalidNextRunAt(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[{"id":"x","route":"/r","target":"pull","next_run_at":"bad"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeInvalidNextRunAt {
		t.Fatalf("expected code %q, got %q", publishCodeInvalidNextRunAt, errResp.Code)
	}
}

func TestServer_PublishMessagesRejectsInvalidPayloadB64(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[{"id":"x","route":"/r","target":"pull","payload_b64":"!!!invalid!!!"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeInvalidPayload {
		t.Fatalf("expected code %q, got %q", publishCodeInvalidPayload, errResp.Code)
	}
}

func TestServer_PublishMessagesRejectsMissingItemID(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[{"route":"/r","target":"pull"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code %q, got %q", publishCodeInvalidBody, errResp.Code)
	}
	if !strings.Contains(errResp.Detail, "item.id is required") {
		t.Fatalf("expected id-required detail, got %q", errResp.Detail)
	}
	if errResp.ItemIndex == nil || *errResp.ItemIndex != 0 {
		t.Fatalf("expected item_index=0, got %v", errResp.ItemIndex)
	}
}

func TestServer_PublishMessagesRejectsMalformedJSON(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`not json`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code %q, got %q", publishCodeInvalidBody, errResp.Code)
	}
}

func TestServer_PublishMessagesObservePublishResultAccepted(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	var observed PublishResultEvent
	srv.ObservePublishResult = func(e PublishResultEvent) { observed = e }

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[
			{"id":"obs_1","route":"/r","target":"pull"},
			{"id":"obs_2","route":"/r","target":"pull"}
		]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if observed.Accepted != 2 {
		t.Fatalf("expected Accepted=2, got %d", observed.Accepted)
	}
	if observed.Scoped {
		t.Fatal("expected Scoped=false for direct path")
	}
}

func TestServer_PublishMessagesObservePublishResultRejected(t *testing.T) {
	srv := NewServer(nil) // nil store → rejected
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	var observed PublishResultEvent
	srv.ObservePublishResult = func(e PublishResultEvent) { observed = e }

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[{"id":"x","route":"/r","target":"pull"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	if observed.Rejected != 1 {
		t.Fatalf("expected Rejected=1, got %d", observed.Rejected)
	}
	if observed.Code != publishCodeStoreUnavailable {
		t.Fatalf("expected Code=%q, got %q", publishCodeStoreUnavailable, observed.Code)
	}
}

func TestServer_PublishMessagesRequiresAuditRequestIDByPolicy(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.PublishRequireAuditRequestID = true
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[{"id":"x","route":"/r","target":"pull"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	// No X-Request-ID header
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeAuditRequestIDRequired {
		t.Fatalf("expected code %q, got %q", publishCodeAuditRequestIDRequired, errResp.Code)
	}
}

func TestServer_PublishMessagesPayloadTooLargeGlobalFallback(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.MaxBodyBytes = 4
	// No LimitsForRoute set — global fallback applies
	srv.TargetsForRoute = func(route string) []string { return []string{"pull"} }

	req := httptest.NewRequest(http.MethodPost, "http://example/messages/publish",
		strings.NewReader(`{"items":[{"id":"x","route":"/r","target":"pull","payload_b64":"aGVsbG8="}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodePayloadTooLarge {
		t.Fatalf("expected code %q, got %q", publishCodePayloadTooLarge, errResp.Code)
	}
}

func TestServer_ApplicationEndpointPublishRejectsEmptyBatch(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveManaged = func(app, ep string) (string, []string, bool) {
		return "/r", []string{"pull"}, true
	}

	req := httptest.NewRequest(http.MethodPost,
		"http://example/applications/billing/endpoints/hooks/messages/publish",
		strings.NewReader(`{"items":[]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeInvalidBody {
		t.Fatalf("expected code %q, got %q", publishCodeInvalidBody, errResp.Code)
	}
}

func TestServer_ApplicationEndpointPublishQueueFull(t *testing.T) {
	store := queue.NewMemoryStore(queue.WithQueueLimits(1, "reject"))
	srv := NewServer(store)
	srv.ResolveManaged = func(app, ep string) (string, []string, bool) {
		return "/r", []string{"pull"}, true
	}

	// Fill queue
	req := httptest.NewRequest(http.MethodPost,
		"http://example/applications/billing/endpoints/hooks/messages/publish",
		strings.NewReader(`{"items":[{"id":"fill_1"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("fill: expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	// Second should fail
	req = httptest.NewRequest(http.MethodPost,
		"http://example/applications/billing/endpoints/hooks/messages/publish",
		strings.NewReader(`{"items":[{"id":"fill_2"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rr.Code, rr.Body.String())
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeQueueFull {
		t.Fatalf("expected code %q, got %q", publishCodeQueueFull, errResp.Code)
	}
}

func TestServer_ApplicationEndpointPublishStoreUnavailable(t *testing.T) {
	srv := NewServer(nil)
	srv.ResolveManaged = func(app, ep string) (string, []string, bool) {
		return "/r", []string{"pull"}, true
	}

	req := httptest.NewRequest(http.MethodPost,
		"http://example/applications/billing/endpoints/hooks/messages/publish",
		strings.NewReader(`{"items":[{"id":"x"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeStoreUnavailable {
		t.Fatalf("expected code %q, got %q", publishCodeStoreUnavailable, errResp.Code)
	}
}

func TestServer_ApplicationEndpointPublishManagedEndpointNotFound(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveManaged = func(app, ep string) (string, []string, bool) {
		return "", nil, false // not found
	}

	req := httptest.NewRequest(http.MethodPost,
		"http://example/applications/billing/endpoints/nonexistent/messages/publish",
		strings.NewReader(`{"items":[{"id":"x"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rr.Code, rr.Body.String())
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeManagedEndpointNotFound {
		t.Fatalf("expected code %q, got %q", publishCodeManagedEndpointNotFound, errResp.Code)
	}
}

func TestServer_ApplicationEndpointPublishResolverNotConfigured(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	// No ResolveManaged set — nil resolver

	req := httptest.NewRequest(http.MethodPost,
		"http://example/applications/billing/endpoints/hooks/messages/publish",
		strings.NewReader(`{"items":[{"id":"x"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rr.Code, rr.Body.String())
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeManagedResolverMissing {
		t.Fatalf("expected code %q, got %q", publishCodeManagedResolverMissing, errResp.Code)
	}
}

func TestServer_ApplicationEndpointPublishManagedEndpointNoTargets(t *testing.T) {
	store := queue.NewMemoryStore()
	srv := NewServer(store)
	srv.ResolveManaged = func(app, ep string) (string, []string, bool) {
		return "/r", nil, true // found but no targets
	}

	req := httptest.NewRequest(http.MethodPost,
		"http://example/applications/billing/endpoints/hooks/messages/publish",
		strings.NewReader(`{"items":[{"id":"x"}]}`))
	req.Header.Set(auditReasonHeader, "test")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rr.Code, rr.Body.String())
	}
	var errResp publishErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Code != publishCodeManagedEndpointNoTargets {
		t.Fatalf("expected code %q, got %q", publishCodeManagedEndpointNoTargets, errResp.Code)
	}
}
