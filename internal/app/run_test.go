package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/admin"
	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/queue"
	"google.golang.org/grpc/metadata"
)

func TestAuthorizePull_PerRouteOverridesGlobal(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:global" }

"/x" {
  pull { path "/e" auth token "raw:route" }
}
`)
	cfg, err := config.Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := config.Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "http://example.com/e/dequeue", nil)
	req.Header.Set("Authorization", "Bearer route")
	if !state.authorizePull(req) {
		t.Fatalf("expected route token to authorize")
	}

	req2 := httptest.NewRequest(http.MethodPost, "http://example.com/e/dequeue", nil)
	req2.Header.Set("Authorization", "Bearer global")
	if state.authorizePull(req2) {
		t.Fatalf("expected global token to be rejected for route with custom tokens")
	}
}

func TestAuthorizePull_UsesGlobalWhenRouteHasNoTokens(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:global" }

"/x" {
  pull { path "/e" }
}
`)
	cfg, err := config.Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := config.Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "http://example.com/e/dequeue", nil)
	req.Header.Set("Authorization", "Bearer global")
	if !state.authorizePull(req) {
		t.Fatalf("expected global token to authorize")
	}

	req2 := httptest.NewRequest(http.MethodPost, "http://example.com/e/dequeue", nil)
	req2.Header.Set("Authorization", "Bearer wrong")
	if state.authorizePull(req2) {
		t.Fatalf("expected wrong token to be rejected")
	}
}

func TestAuthorizeWorker_PerRouteOverridesGlobal(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:global" }

"/x" {
  pull { path "/e" auth token "raw:route" }
}
`)
	cfg, err := config.Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := config.Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	routeCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer route"))
	if !state.authorizeWorker(routeCtx, "/e") {
		t.Fatalf("expected route token to authorize worker endpoint")
	}

	globalCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer global"))
	if state.authorizeWorker(globalCtx, "/e") {
		t.Fatalf("expected global token to be rejected for route with custom tokens")
	}
}

func TestAuthorizeWorker_UsesGlobalWhenRouteHasNoTokens(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:global" }

"/x" {
  pull { path "/e" }
}
`)
	cfg, err := config.Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := config.Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	globalCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer global"))
	if !state.authorizeWorker(globalCtx, "/e") {
		t.Fatalf("expected global token to authorize worker endpoint")
	}

	wrongCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong"))
	if state.authorizeWorker(wrongCtx, "/e") {
		t.Fatalf("expected wrong token to be rejected")
	}
}

func TestLoadAuth_ConfiguresRouteHMACOptions(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:global" }

"/x" {
  auth hmac {
    secret "raw:secret"
    signature_header "X-Hookaido-Signature"
    timestamp_header "X-Hookaido-Timestamp"
    nonce_header "X-Hookaido-Nonce"
    tolerance "12m"
  }
  pull { path "/e" }
}
`)
	cfg, err := config.Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := config.Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}
	hmacAuth := state.hmacAuthFor("/x")
	if hmacAuth == nil {
		t.Fatalf("expected route hmac auth config")
	}
	if hmacAuth.SignatureHeader != "X-Hookaido-Signature" {
		t.Fatalf("signature header: got %q", hmacAuth.SignatureHeader)
	}
	if hmacAuth.TimestampHeader != "X-Hookaido-Timestamp" {
		t.Fatalf("timestamp header: got %q", hmacAuth.TimestampHeader)
	}
	if hmacAuth.NonceHeader != "X-Hookaido-Nonce" {
		t.Fatalf("nonce header: got %q", hmacAuth.NonceHeader)
	}
	if hmacAuth.Tolerance != 12*time.Minute {
		t.Fatalf("tolerance: got %s", hmacAuth.Tolerance)
	}
}

func TestLoadAuth_ConfiguresRouteForwardAuth(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:global" }

"/x" {
  auth forward "https://auth.example.test/check" {
    timeout "3s"
    copy_headers "x-user-id"
    copy_headers x-role
    body_limit "64kb"
  }
  pull { path "/e" }
}
`)
	cfg, err := config.Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := config.Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	forward := state.forwardAuthFor("/x")
	if forward == nil {
		t.Fatalf("expected route forward auth config")
	}
	if forward.URL != "https://auth.example.test/check" {
		t.Fatalf("url: got %q", forward.URL)
	}
	if forward.Timeout != 3*time.Second {
		t.Fatalf("timeout: got %s", forward.Timeout)
	}
	if forward.BodyLimitBytes != 64*1024 {
		t.Fatalf("body limit: got %d", forward.BodyLimitBytes)
	}
	if len(forward.CopyHeaders) != 2 || forward.CopyHeaders[0] != "X-User-Id" || forward.CopyHeaders[1] != "X-Role" {
		t.Fatalf("copy headers: got %#v", forward.CopyHeaders)
	}
}

func TestResolveIngress_HostWildcardSubdomain(t *testing.T) {
	compiled := compileForReloadTest(t, `
"/x" {
  match { host "*.example.com" }
  deliver "https://example.org" {}
}
`)
	state := newRuntimeState(compiled)

	reqSubdomain := httptest.NewRequest(http.MethodPost, "http://api.example.com/x", nil)
	if route, ok := state.resolveIngress(reqSubdomain, "/x"); !ok || route != "/x" {
		t.Fatalf("expected wildcard host to match subdomain, got route=%q ok=%v", route, ok)
	}

	reqApex := httptest.NewRequest(http.MethodPost, "http://example.com/x", nil)
	if route, ok := state.resolveIngress(reqApex, "/x"); ok {
		t.Fatalf("expected wildcard host not to match apex domain, got route=%q", route)
	}
}

func TestResolveIngress_HostWildcardAny(t *testing.T) {
	compiled := compileForReloadTest(t, `
"/x" {
  match { host "*" }
  deliver "https://example.org" {}
}
`)
	state := newRuntimeState(compiled)

	req := httptest.NewRequest(http.MethodPost, "http://tenant.internal/x", nil)
	if route, ok := state.resolveIngress(req, "/x"); !ok || route != "/x" {
		t.Fatalf("expected wildcard host '*' to match any host, got route=%q ok=%v", route, ok)
	}
}

func TestResolveIngress_HeaderAndQueryExists(t *testing.T) {
	compiled := compileForReloadTest(t, `
"/x" {
  match {
    header_exists "X-Signature"
    query_exists "delivery_id"
  }
  deliver "https://example.org" {}
}
`)
	state := newRuntimeState(compiled)

	reqMissingHeader := httptest.NewRequest(http.MethodPost, "http://hooks.example/x?delivery_id=evt-1", nil)
	if route, ok := state.resolveIngress(reqMissingHeader, "/x"); ok {
		t.Fatalf("expected missing header_exists to reject, got route=%q", route)
	}

	reqMissingQuery := httptest.NewRequest(http.MethodPost, "http://hooks.example/x", nil)
	reqMissingQuery.Header.Set("X-Signature", "sig")
	if route, ok := state.resolveIngress(reqMissingQuery, "/x"); ok {
		t.Fatalf("expected missing query_exists to reject, got route=%q", route)
	}

	req := httptest.NewRequest(http.MethodPost, "http://hooks.example/x?delivery_id=evt-1", nil)
	req.Header.Set("x-signature", "sig")
	if route, ok := state.resolveIngress(req, "/x"); !ok || route != "/x" {
		t.Fatalf("expected header_exists+query_exists to match, got route=%q ok=%v", route, ok)
	}
}

func TestResolveIngress_RemoteIPMatcher(t *testing.T) {
	compiled := compileForReloadTest(t, `
"/x" {
  match {
    remote_ip "203.0.113.0/24"
  }
  deliver "https://example.org" {}
}
`)
	state := newRuntimeState(compiled)

	reqMatch := httptest.NewRequest(http.MethodPost, "http://hooks.example/x", nil)
	reqMatch.RemoteAddr = "203.0.113.42:9000"
	if route, ok := state.resolveIngress(reqMatch, "/x"); !ok || route != "/x" {
		t.Fatalf("expected remote_ip matcher to accept request IP, got route=%q ok=%v", route, ok)
	}

	reqNoMatch := httptest.NewRequest(http.MethodPost, "http://hooks.example/x", nil)
	reqNoMatch.RemoteAddr = "198.51.100.20:9000"
	if route, ok := state.resolveIngress(reqNoMatch, "/x"); ok {
		t.Fatalf("expected remote_ip mismatch to reject route, got route=%q", route)
	}
}

func TestAllowedMethodsFor_RemoteIPMatcher(t *testing.T) {
	compiled := compileForReloadTest(t, `
"/x" {
  match {
    method GET
    remote_ip "203.0.113.0/24"
  }
  deliver "https://example.org" {}
}
`)
	state := newRuntimeState(compiled)

	reqAllowed := httptest.NewRequest(http.MethodPost, "http://hooks.example/x", nil)
	reqAllowed.RemoteAddr = "203.0.113.42:9000"
	allowed := state.allowedMethodsFor(reqAllowed, "/x")
	if len(allowed) != 1 || allowed[0] != http.MethodGet {
		t.Fatalf("expected allowed=[GET], got %#v", allowed)
	}

	reqDenied := httptest.NewRequest(http.MethodPost, "http://hooks.example/x", nil)
	reqDenied.RemoteAddr = "198.51.100.20:9000"
	allowed = state.allowedMethodsFor(reqDenied, "/x")
	if len(allowed) != 0 {
		t.Fatalf("expected no allowed methods for non-matching remote_ip, got %#v", allowed)
	}
}

func TestAllowIngress_GlobalRateLimit(t *testing.T) {
	compiled := compileForReloadTest(t, `
ingress {
  rate_limit { rps 1 burst 1 }
}

"/x" {
  deliver "https://example.org" {}
}
`)
	state := newRuntimeState(compiled)
	now := time.Date(2026, 2, 8, 12, 0, 0, 0, time.UTC)
	state.now = func() time.Time { return now }
	state.updateAll(compiled)

	if !state.allowIngress("/x") {
		t.Fatalf("expected first request to pass global rate limit")
	}
	if state.allowIngress("/x") {
		t.Fatalf("expected second immediate request to be rate limited")
	}
}

func TestAllowIngress_RouteRateLimitOverridesGlobal(t *testing.T) {
	compiled := compileForReloadTest(t, `
ingress {
  rate_limit { rps 1 burst 1 }
}

"/x" {
  rate_limit { rps 1 burst 2 }
  deliver "https://example.org" {}
}
`)
	state := newRuntimeState(compiled)
	now := time.Date(2026, 2, 8, 12, 0, 0, 0, time.UTC)
	state.now = func() time.Time { return now }
	state.updateAll(compiled)

	if !state.allowIngress("/x") {
		t.Fatalf("expected first request to pass route rate limit")
	}
	if !state.allowIngress("/x") {
		t.Fatalf("expected second request to pass route burst override")
	}
	if state.allowIngress("/x") {
		t.Fatalf("expected third immediate request to be rate limited by route burst")
	}
}

func TestAllowIngressEnqueue_AdaptiveBackpressure(t *testing.T) {
	compiled := compileForReloadTest(t, `
defaults {
  adaptive_backpressure {
    enabled on
    min_total 1
    queued_percent 50
    ready_lag 10m
    oldest_queued_age 10m
    sustained_growth off
  }
}

pull_api { auth token "raw:t" }

"/x" {
  pull { path "/e" }
}
`)
	state := newRuntimeState(compiled)
	store := queue.NewMemoryStore()
	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/x", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	state.setQueueStore(store)

	allowed, statusCode, reason := state.allowIngressEnqueue("/x")
	if allowed {
		t.Fatal("expected adaptive backpressure to reject enqueue")
	}
	if statusCode != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d", statusCode)
	}
	if reason != "queued_pressure" {
		t.Fatalf("reason: got %q", reason)
	}
}

func TestRuntimeState_ResolveManagedEndpoint(t *testing.T) {
	compiled := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
"/x" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e" }
}
`)
	state := newRuntimeState(compiled)

	route, targets, ok := state.resolveManagedEndpoint("billing", "invoice.created")
	if !ok {
		t.Fatalf("expected managed endpoint to resolve")
	}
	if route != "/x" {
		t.Fatalf("expected route=/x, got %q", route)
	}
	if len(targets) != 1 || targets[0] != "pull" {
		t.Fatalf("expected targets=[pull], got %#v", targets)
	}
}

func TestRuntimeState_TargetsForRoute(t *testing.T) {
	compiled := compileForReloadTest(t, `
"/x" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
  deliver "https://example.org/b" {}
}
`)
	state := newRuntimeState(compiled)

	targets := state.targetsForRoute("/x")
	if len(targets) != 2 {
		t.Fatalf("expected 2 targets, got %#v", targets)
	}
}

func TestRuntimeState_ManagementModel(t *testing.T) {
	compiled := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
"/billing/invoice" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/pull/invoice" }
}
"/billing/invoice/sent" {
  application "billing"
  endpoint_name "invoice.sent"
  deliver "https://example.org/a" {}
  deliver "https://example.org/b" {}
}
"/unmanaged" {
  deliver "https://example.org/c" {}
}
`)
	state := newRuntimeState(compiled)

	model := state.managementModel()
	if model.RouteCount != 3 {
		t.Fatalf("expected route_count=3, got %d", model.RouteCount)
	}
	if model.ApplicationCount != 1 || model.EndpointCount != 2 {
		t.Fatalf("unexpected app/endpoint counts: %#v", model)
	}
	if len(model.Applications) != 1 || model.Applications[0].Name != "billing" {
		t.Fatalf("unexpected applications: %#v", model.Applications)
	}
	if model.Applications[0].EndpointCount != 2 || len(model.Applications[0].Endpoints) != 2 {
		t.Fatalf("unexpected endpoints: %#v", model.Applications[0].Endpoints)
	}
}

func TestRequiresRestartForReload_DeliverTargetChanged(t *testing.T) {
	running := compileForReloadTest(t, `
"/x" {
  deliver "https://ci.internal/one" {}
}
`)
	updated := compileForReloadTest(t, `
"/x" {
  deliver "https://ci.internal/two" {}
}
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatalf("expected restart required when deliver target changes")
	}
}

func TestRequiresRestartForReload_DeliverSigningChanged(t *testing.T) {
	running := compileForReloadTest(t, `
"/x" {
  deliver "https://ci.internal/one" {
    sign hmac "raw:sign-a"
  }
}
`)
	updated := compileForReloadTest(t, `
"/x" {
  deliver "https://ci.internal/one" {
    sign hmac "raw:sign-b"
  }
}
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatalf("expected restart required when deliver signing changes")
	}
}

func TestRequiresRestartForReload_DeliverSigningSecretRefChanged(t *testing.T) {
	running := compileForReloadTest(t, `
secrets {
  secret "S1" {
    value "raw:sign-a"
    valid_from "2026-01-01T00:00:00Z"
  }
}
"/x" {
  deliver "https://ci.internal/one" {
    sign hmac secret_ref "S1"
  }
}
`)
	updated := compileForReloadTest(t, `
secrets {
  secret "S1" {
    value "raw:sign-a"
    valid_from "2026-01-01T00:00:00Z"
  }
  secret "S2" {
    value "raw:sign-b"
    valid_from "2027-01-01T00:00:00Z"
  }
}
"/x" {
  deliver "https://ci.internal/one" {
    sign hmac secret_ref "S1"
    sign hmac secret_ref "S2"
  }
}
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatalf("expected restart required when deliver signing secret_refs change")
	}
}

func TestRequiresRestartForReload_DeliverSigningSelectionChanged(t *testing.T) {
	running := compileForReloadTest(t, `
secrets {
  secret "S1" {
    value "raw:sign-a"
    valid_from "2026-01-01T00:00:00Z"
  }
}
"/x" {
  deliver "https://ci.internal/one" {
    sign hmac secret_ref "S1"
    sign secret_selection "newest_valid"
  }
}
`)
	updated := compileForReloadTest(t, `
secrets {
  secret "S1" {
    value "raw:sign-a"
    valid_from "2026-01-01T00:00:00Z"
  }
}
"/x" {
  deliver "https://ci.internal/one" {
    sign hmac secret_ref "S1"
    sign secret_selection "oldest_valid"
  }
}
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatalf("expected restart required when deliver signing secret_selection changes")
	}
}

func TestRequiresRestartForReload_PullEndpointChanged(t *testing.T) {
	running := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
"/x" { pull { path "/e1" } }
`)
	updated := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
"/x" { pull { path "/e2" } }
`)
	if requiresRestartForReload(updated, running) {
		t.Fatalf("expected live reload for pull endpoint change")
	}
}

func TestRequiresRestartForReload_PullAPILimitsChanged(t *testing.T) {
	running := compileForReloadTest(t, `
pull_api {
  auth token "raw:t"
  max_batch 10
  default_lease_ttl 30s
  max_lease_ttl 60s
  default_max_wait 5s
  max_wait 10s
}
"/x" { pull { path "/e1" } }
`)
	updated := compileForReloadTest(t, `
pull_api {
  auth token "raw:t"
  max_batch 20
  default_lease_ttl 45s
  max_lease_ttl 90s
  default_max_wait 8s
  max_wait 20s
}
"/x" { pull { path "/e1" } }
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatalf("expected restart required when pull_api limits change")
	}
}

func TestRequiresRestartForReload_PullAPIGrpcListenChanged(t *testing.T) {
	running := compileForReloadTest(t, `
pull_api {
  auth token "raw:t"
  grpc_listen ":9943"
}
"/x" { pull { path "/e1" } }
`)
	updated := compileForReloadTest(t, `
pull_api {
  auth token "raw:t"
  grpc_listen ":9953"
}
"/x" { pull { path "/e1" } }
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatalf("expected restart required when pull_api.grpc_listen changes")
	}
}

func TestRequiresRestartForReload_EgressPolicyChangedWithDeliver(t *testing.T) {
	running := compileForReloadTest(t, `
defaults {
  egress {
    https_only on
    redirects off
    dns_rebind_protection on
  }
}
"/x" { deliver "https://ci.internal/hook" {} }
`)
	updated := compileForReloadTest(t, `
defaults {
  egress {
    https_only on
    redirects on
    dns_rebind_protection on
  }
}
"/x" { deliver "https://ci.internal/hook" {} }
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatalf("expected restart required when egress policy changes with deliver routes")
	}
}

func TestRequiresRestartForReload_EgressPolicyChangedWithoutDeliver(t *testing.T) {
	running := compileForReloadTest(t, `
defaults {
  egress {
    https_only on
    redirects off
    dns_rebind_protection on
  }
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e1" } }
`)
	updated := compileForReloadTest(t, `
defaults {
  egress {
    https_only on
    redirects on
    dns_rebind_protection on
  }
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e1" } }
`)
	if requiresRestartForReload(updated, running) {
		t.Fatalf("expected live reload when egress policy changes without deliver routes")
	}
}

func TestRequiresRestartForReload_TracingConfigChanged(t *testing.T) {
	running := compileForReloadTest(t, `
observability {
  tracing {
    enabled on
    collector "https://otel.example.com/v1/traces"
    header "X-Tenant" "a"
  }
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e1" } }
`)
	updated := compileForReloadTest(t, `
observability {
  tracing {
    enabled on
    collector "https://otel.example.com/v1/traces"
    header "X-Tenant" "b"
  }
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e1" } }
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatalf("expected restart required when tracing config changes")
	}
}

func TestRequiresRestartForReload_TracingExporterOptionChanged(t *testing.T) {
	running := compileForReloadTest(t, `
	observability {
	  tracing {
	    enabled on
	    collector "https://otel.example.com/v1/traces"
	    url_path "/v1/traces"
	    timeout 5s
	    compression none
	    insecure off
	    retry {
	      enabled on
	      initial_interval 1s
	      max_interval 2s
	      max_elapsed_time 20s
	    }
	  }
	}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e1" } }
`)
	updated := compileForReloadTest(t, `
	observability {
	  tracing {
	    enabled on
	    collector "https://otel.example.com/v1/traces"
	    url_path "/custom/traces"
	    timeout 10s
	    compression gzip
	    insecure on
	    retry {
	      enabled off
	      initial_interval 1s
	      max_interval 2s
	      max_elapsed_time off
	    }
	  }
	}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e1" } }
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatalf("expected restart required when tracing exporter options change")
	}
}

func TestRequiresRestartForReload_LogSinkChanged(t *testing.T) {
	running := compileForReloadTest(t, `
observability {
  runtime_log {
    level info
    output file
    path "logs/runtime-a.log"
    format json
  }
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e1" } }
`)
	updated := compileForReloadTest(t, `
observability {
  runtime_log {
    level info
    output file
    path "logs/runtime-b.log"
    format json
  }
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e1" } }
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatalf("expected restart required when runtime log sink changes")
	}
}

func TestRequiresRestartForReload_QueueBackendChanged(t *testing.T) {
	running := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
"/x" {
  pull { path "/e1" }
}
`)
	updated := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
"/x" {
  queue { backend memory }
  pull { path "/e1" }
}
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatalf("expected restart required when queue backend changes")
	}
}

func TestQueueBackendForCompiled_DefaultSQLite(t *testing.T) {
	compiled := config.Compiled{
		Routes: []config.CompiledRoute{
			{Path: "/x"},
		},
	}
	if got := queueBackendForCompiled(compiled); got != "sqlite" {
		t.Fatalf("queue backend: got %q", got)
	}
}

func TestQueueBackendForCompiled_Postgres(t *testing.T) {
	compiled := config.Compiled{
		Routes: []config.CompiledRoute{
			{Path: "/x", QueueBackend: "postgres"},
		},
	}
	if got := queueBackendForCompiled(compiled); got != "postgres" {
		t.Fatalf("queue backend: got %q", got)
	}
}

func TestQueueBackendForCompiled_Mixed(t *testing.T) {
	compiled := config.Compiled{
		Routes: []config.CompiledRoute{
			{Path: "/a", QueueBackend: "sqlite"},
			{Path: "/b", QueueBackend: "memory"},
		},
	}
	if got := queueBackendForCompiled(compiled); got != "mixed" {
		t.Fatalf("queue backend: got %q", got)
	}
}

func TestResolvePostgresDSN_FlagPreferred(t *testing.T) {
	t.Setenv("HOOKAIDO_POSTGRES_DSN", "postgres://env")
	if got := resolvePostgresDSN("postgres://flag"); got != "postgres://flag" {
		t.Fatalf("resolve dsn: got %q", got)
	}
}

func TestResolvePostgresDSN_EnvFallback(t *testing.T) {
	t.Setenv("HOOKAIDO_POSTGRES_DSN", "postgres://env")
	if got := resolvePostgresDSN(""); got != "postgres://env" {
		t.Fatalf("resolve dsn: got %q", got)
	}
}

func TestNewQueueStore_PostgresRequiresDSN(t *testing.T) {
	compiled := config.Compiled{
		Routes: []config.CompiledRoute{
			{Path: "/x", QueueBackend: "postgres"},
		},
	}
	_, backend, _, err := newQueueStore(compiled, filepath.Join(t.TempDir(), "hookaido.db"), "")
	if backend != "postgres" {
		t.Fatalf("queue backend: got %q", backend)
	}
	if err == nil || !strings.Contains(err.Error(), "empty postgres dsn") {
		t.Fatalf("expected empty postgres dsn error, got %v", err)
	}
}

func TestApplyManagedEndpointUpsert_Create(t *testing.T) {
	cfg, compiled := parseAndCompileConfigForMutationTest(t, `
pull_api { auth token "raw:t" }
"/a" {
  pull { path "/e1" }
}
`)

	result, err := applyManagedEndpointUpsert(cfg, compiled, admin.ManagementEndpointUpsertRequest{
		Application:  "billing",
		EndpointName: "invoice.created",
		Route:        "/a",
	}, nil)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if !result.Applied || result.Action != "created" || result.Route != "/a" {
		t.Fatalf("unexpected result: %#v", result)
	}

	updated := compileConfigForMutationTest(t, cfg)
	if route, _, found := findManagedEndpointRoute(updated, "billing", "invoice.created"); !found || route != "/a" {
		t.Fatalf("expected mapping to /a, got route=%q found=%v", route, found)
	}
}

func TestApplyManagedEndpointUpsert_Move(t *testing.T) {
	cfg, compiled := parseAndCompileConfigForMutationTest(t, `
pull_api { auth token "raw:t" }
"/a" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e1" }
}
"/b" {
  pull { path "/e2" }
}
`)

	result, err := applyManagedEndpointUpsert(cfg, compiled, admin.ManagementEndpointUpsertRequest{
		Application:  "billing",
		EndpointName: "invoice.created",
		Route:        "/b",
	}, nil)
	if err != nil {
		t.Fatalf("upsert move: %v", err)
	}
	if !result.Applied || result.Action != "updated" || result.Route != "/b" {
		t.Fatalf("unexpected result: %#v", result)
	}

	updated := compileConfigForMutationTest(t, cfg)
	route, _, found := findManagedEndpointRoute(updated, "billing", "invoice.created")
	if !found || route != "/b" {
		t.Fatalf("expected mapping moved to /b, got route=%q found=%v", route, found)
	}
}

func TestApplyManagedEndpointUpsert_Conflict(t *testing.T) {
	cfg, compiled := parseAndCompileConfigForMutationTest(t, `
pull_api { auth token "raw:t" }
"/a" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e1" }
}
"/b" {
  application "erp"
  endpoint_name "stock.updated"
  pull { path "/e2" }
}
`)

	_, err := applyManagedEndpointUpsert(cfg, compiled, admin.ManagementEndpointUpsertRequest{
		Application:  "billing",
		EndpointName: "invoice.created",
		Route:        "/b",
	}, nil)
	if !errors.Is(err, admin.ErrManagementConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), `route "/b" is already mapped to ("erp", "stock.updated")`) {
		t.Fatalf("expected conflict detail for mapped target route, got %v", err)
	}
}

func TestApplyManagedEndpointUpsert_ConflictWhenManagedPublishDisabledOnTargetRoute(t *testing.T) {
	tests := []struct {
		name            string
		targetPolicy    string
		targetDirective string
	}{
		{
			name:            "publish_off",
			targetPolicy:    "publish off",
			targetDirective: "route.publish",
		},
		{
			name:            "publish_managed_off",
			targetPolicy:    "publish { managed off }",
			targetDirective: "route.publish.managed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, compiled := parseAndCompileConfigForMutationTest(t, fmt.Sprintf(`
pull_api { auth token "raw:t" }
"/a" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e1" }
}
"/b" {
  %s
  pull { path "/e2" }
}
`, tc.targetPolicy))

			_, err := applyManagedEndpointUpsert(cfg, compiled, admin.ManagementEndpointUpsertRequest{
				Application:  "billing",
				EndpointName: "invoice.created",
				Route:        "/b",
			}, nil)
			if !errors.Is(err, admin.ErrManagementConflict) {
				t.Fatalf("expected conflict when %s disables managed publish, got %v", tc.targetDirective, err)
			}
			if err == nil || !strings.Contains(err.Error(), tc.targetDirective) {
				t.Fatalf("expected conflict detail to reference %s, got %v", tc.targetDirective, err)
			}
		})
	}
}

func TestApplyManagedEndpointUpsert_ConflictWhenTargetProfileMismatches(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		target     string
		detailHint string
	}{
		{
			name: "mode_mismatch_pull_to_deliver",
			source: `
pull_api { auth token "raw:t" }
"/a" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e1" }
}
"/b" {
  deliver "https://example.org/hook" {}
}
`,
			target:     "/b",
			detailHint: "mode=pull",
		},
		{
			name: "deliver_target_mismatch",
			source: `
"/a" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
"/b" {
  deliver "https://example.org/b" {}
}
`,
			target:     "/b",
			detailHint: `targets="https://example.org/a"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, compiled := parseAndCompileConfigForMutationTest(t, tc.source)

			_, err := applyManagedEndpointUpsert(cfg, compiled, admin.ManagementEndpointUpsertRequest{
				Application:  "billing",
				EndpointName: "invoice.created",
				Route:        tc.target,
			}, nil)
			if !errors.Is(err, admin.ErrManagementConflict) {
				t.Fatalf("expected conflict for profile mismatch, got %v", err)
			}
			code, detail, ok := admin.ExtractManagementMutationError(err)
			if !ok {
				t.Fatalf("expected typed management mutation error, got %v", err)
			}
			if code != admin.ManagementMutationCodeRouteTargetMismatch {
				t.Fatalf("expected code=%q, got %q", admin.ManagementMutationCodeRouteTargetMismatch, code)
			}
			if !strings.Contains(detail, "managed endpoint target profile mismatch") {
				t.Fatalf("expected profile mismatch detail, got %q", detail)
			}
			if !strings.Contains(detail, tc.detailHint) {
				t.Fatalf("expected detail containing %q, got %q", tc.detailHint, detail)
			}
		})
	}
}

func TestApplyManagedEndpointUpsert_ConflictWhenCurrentRouteHasActiveBacklog(t *testing.T) {
	tests := []struct {
		name  string
		state queue.State
	}{
		{
			name:  "queued",
			state: queue.StateQueued,
		},
		{
			name:  "leased",
			state: queue.StateLeased,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, compiled := parseAndCompileConfigForMutationTest(t, `
"/a" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
"/b" {
  deliver "https://example.org/a" {}
}
`)
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{
				ID:         "evt_1",
				Route:      "/a",
				Target:     "https://example.org/a",
				State:      tc.state,
				ReceivedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			_, err := applyManagedEndpointUpsert(cfg, compiled, admin.ManagementEndpointUpsertRequest{
				Application:  "billing",
				EndpointName: "invoice.created",
				Route:        "/b",
			}, store)
			if !errors.Is(err, admin.ErrManagementConflict) {
				t.Fatalf("expected conflict for active backlog, got %v", err)
			}
			code, detail, ok := admin.ExtractManagementMutationError(err)
			if !ok {
				t.Fatalf("expected typed management mutation error, got %v", err)
			}
			if code != admin.ManagementMutationCodeRouteBacklogActive {
				t.Fatalf("expected code=%q, got %q", admin.ManagementMutationCodeRouteBacklogActive, code)
			}
			if !strings.Contains(detail, `route "/a"`) || !strings.Contains(detail, string(tc.state)) {
				t.Fatalf("unexpected detail %q", detail)
			}
		})
	}
}

func TestApplyManagedEndpointDelete(t *testing.T) {
	cfg, compiled := parseAndCompileConfigForMutationTest(t, `
pull_api { auth token "raw:t" }
"/a" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e1" }
}
`)

	result, err := applyManagedEndpointDelete(cfg, compiled, admin.ManagementEndpointDeleteRequest{
		Application:  "billing",
		EndpointName: "invoice.created",
	}, nil)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !result.Applied || result.Action != "deleted" || result.Route != "/a" {
		t.Fatalf("unexpected delete result: %#v", result)
	}

	updated := compileConfigForMutationTest(t, cfg)
	if _, _, found := findManagedEndpointRoute(updated, "billing", "invoice.created"); found {
		t.Fatalf("expected mapping to be deleted")
	}
}

func TestApplyManagedEndpointDelete_ConflictWhenCurrentRouteHasActiveBacklog(t *testing.T) {
	tests := []struct {
		name  string
		state queue.State
	}{
		{
			name:  "queued",
			state: queue.StateQueued,
		},
		{
			name:  "leased",
			state: queue.StateLeased,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, compiled := parseAndCompileConfigForMutationTest(t, `
"/a" {
  application "billing"
  endpoint_name "invoice.created"
  deliver "https://example.org/a" {}
}
`)
			store := queue.NewMemoryStore()
			if err := store.Enqueue(queue.Envelope{
				ID:         "evt_1",
				Route:      "/a",
				Target:     "https://example.org/a",
				State:      tc.state,
				ReceivedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			_, err := applyManagedEndpointDelete(cfg, compiled, admin.ManagementEndpointDeleteRequest{
				Application:  "billing",
				EndpointName: "invoice.created",
			}, store)
			if !errors.Is(err, admin.ErrManagementConflict) {
				t.Fatalf("expected conflict for active backlog, got %v", err)
			}
			code, detail, ok := admin.ExtractManagementMutationError(err)
			if !ok {
				t.Fatalf("expected typed management mutation error, got %v", err)
			}
			if code != admin.ManagementMutationCodeRouteBacklogActive {
				t.Fatalf("expected code=%q, got %q", admin.ManagementMutationCodeRouteBacklogActive, code)
			}
			if !strings.Contains(detail, `route "/a"`) || !strings.Contains(detail, string(tc.state)) {
				t.Fatalf("unexpected detail %q", detail)
			}
		})
	}
}

func TestMutateManagedEndpointConfig_UpsertAndReload(t *testing.T) {
	src := `
pull_api { auth token "raw:t" }
"/a" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e1" }
}
"/b" {
  pull { path "/e2" }
}
`
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(src), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	running := compileForReloadTest(t, src)
	state := newRuntimeState(running)
	if err := state.loadAuth(running); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	result, updated, err := mutateManagedEndpointConfig(
		cfgPath,
		running,
		state,
		slog.Default(),
		func(cfg *config.Config, compiled config.Compiled) (admin.ManagementEndpointMutationResult, error) {
			return applyManagedEndpointUpsert(cfg, compiled, admin.ManagementEndpointUpsertRequest{
				Application:  "billing",
				EndpointName: "invoice.created",
				Route:        "/b",
			}, nil)
		},
		"test_mutation",
	)
	if err != nil {
		t.Fatalf("mutate config: %v", err)
	}
	if !result.Applied || result.Route != "/b" {
		t.Fatalf("unexpected result: %#v", result)
	}

	route, _, ok := state.resolveManagedEndpoint("billing", "invoice.created")
	if !ok || route != "/b" {
		t.Fatalf("expected runtime state mapping to /b, got route=%q ok=%v", route, ok)
	}
	routeUpdated, _, ok := findManagedEndpointRoute(updated, "billing", "invoice.created")
	if !ok || routeUpdated != "/b" {
		t.Fatalf("expected updated compiled mapping to /b, got route=%q ok=%v", routeUpdated, ok)
	}
}

func parseAndCompileConfigForMutationTest(t *testing.T, src string) (*config.Config, config.Compiled) {
	t.Helper()
	cfg, err := config.Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := config.Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	return cfg, compiled
}

func compileConfigForMutationTest(t *testing.T, cfg *config.Config) config.Compiled {
	t.Helper()
	compiled, res := config.Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	return compiled
}

func findManagedEndpointRoute(compiled config.Compiled, application, endpointName string) (string, []string, bool) {
	for _, rt := range compiled.Routes {
		if rt.Application == application && rt.EndpointName == endpointName {
			return rt.Path, compiledRouteTargets(rt), true
		}
	}
	return "", nil, false
}

func compileForReloadTest(t *testing.T, src string) config.Compiled {
	t.Helper()

	cfg, err := config.Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	compiled, res := config.Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	return compiled
}

func TestRequiresRestartForReload_DefaultsMaxBodyChanged(t *testing.T) {
	running := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
defaults { max_body 2mb }
"/x" { pull { path "/e" } }
`)
	updated := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
defaults { max_body 4mb }
"/x" { pull { path "/e" } }
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatal("expected restart required when defaults.max_body changes")
	}
}

func TestRequiresRestartForReload_DefaultsMaxHeadersChanged(t *testing.T) {
	running := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
defaults { max_headers 64kb }
"/x" { pull { path "/e" } }
`)
	updated := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
defaults { max_headers 128kb }
"/x" { pull { path "/e" } }
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatal("expected restart required when defaults.max_headers changes")
	}
}

func TestRequiresRestartForReload_PublishPolicyDirectChanged(t *testing.T) {
	running := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
defaults { publish_policy { direct on } }
"/x" { pull { path "/e" } }
`)
	updated := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
defaults { publish_policy { direct off } }
"/x" { pull { path "/e" } }
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatal("expected restart required when defaults.publish_policy.direct changes")
	}
}

func TestRequiresRestartForReload_PublishPolicyActorAllowChanged(t *testing.T) {
	running := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
defaults { publish_policy { actor_allow "svc-a" } }
"/x" { pull { path "/e" } }
`)
	updated := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
defaults { publish_policy { actor_allow "svc-a" "svc-b" } }
"/x" { pull { path "/e" } }
`)
	if !requiresRestartForReload(updated, running) {
		t.Fatal("expected restart required when defaults.publish_policy.actor_allow changes")
	}
}

func TestRequiresRestartForReload_PublishPolicyUnchanged(t *testing.T) {
	running := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
defaults { publish_policy { direct on managed on } }
"/x" { pull { path "/e" } }
`)
	updated := compileForReloadTest(t, `
pull_api { auth token "raw:t" }
defaults { publish_policy { direct on managed on } }
"/x" { pull { path "/e" } }
`)
	if requiresRestartForReload(updated, running) {
		t.Fatal("expected no restart when publish_policy unchanged")
	}
}

// --- reloadConfig flow tests ---

func writeReloadFile(t *testing.T, dir, content string) string {
	t.Helper()
	p := filepath.Join(dir, "Hookaidofile")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReloadConfig_Success(t *testing.T) {
	dir := t.TempDir()
	original := `
pull_api { auth token "raw:t" }
"/hooks" { pull { path "/events" } }
`
	cfgPath := writeReloadFile(t, dir, original)
	running := compileForReloadTest(t, original)
	state := newRuntimeState(running)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// Write updated config with an additional route
	updated := `
pull_api { auth token "raw:t" }
"/hooks" { pull { path "/events" } }
"/new" { pull { path "/new-events" } }
`
	_ = os.WriteFile(cfgPath, []byte(updated), 0644)

	result, ok := reloadConfig(cfgPath, running, state, logger, "test")
	if !ok {
		t.Fatal("expected reload to succeed")
	}
	if len(result.Routes) != 2 {
		t.Fatalf("expected 2 routes after reload, got %d", len(result.Routes))
	}
}

func TestReloadConfig_ParseError(t *testing.T) {
	dir := t.TempDir()
	original := `
pull_api { auth token "raw:t" }
"/hooks" { pull { path "/events" } }
`
	cfgPath := writeReloadFile(t, dir, original)
	running := compileForReloadTest(t, original)
	state := newRuntimeState(running)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// Write invalid config
	_ = os.WriteFile(cfgPath, []byte("this is not valid!!!"), 0644)

	result, ok := reloadConfig(cfgPath, running, state, logger, "test")
	if ok {
		t.Fatal("expected reload to fail on parse error")
	}
	// Should return the original running config unchanged
	if len(result.Routes) != len(running.Routes) {
		t.Fatalf("expected running config unchanged, got %d routes", len(result.Routes))
	}
}

func TestReloadConfig_CompileError(t *testing.T) {
	dir := t.TempDir()
	original := `
pull_api { auth token "raw:t" }
"/hooks" { pull { path "/events" } }
`
	cfgPath := writeReloadFile(t, dir, original)
	running := compileForReloadTest(t, original)
	state := newRuntimeState(running)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// Route without pull or deliver fails compile
	_ = os.WriteFile(cfgPath, []byte(`
pull_api { auth token "raw:t" }
"/hooks" {}
`), 0644)

	result, ok := reloadConfig(cfgPath, running, state, logger, "test")
	if ok {
		t.Fatal("expected reload to fail on compile error")
	}
	if len(result.Routes) != len(running.Routes) {
		t.Fatalf("expected running config unchanged")
	}
}

func TestReloadConfig_RestartRequired(t *testing.T) {
	dir := t.TempDir()
	original := `
ingress { listen ":8080" }
pull_api { auth token "raw:t" }
"/hooks" { pull { path "/events" } }
`
	cfgPath := writeReloadFile(t, dir, original)
	running := compileForReloadTest(t, original)
	state := newRuntimeState(running)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// Change listener address → requires restart
	_ = os.WriteFile(cfgPath, []byte(`
ingress { listen ":9999" }
pull_api { auth token "raw:t" }
"/hooks" { pull { path "/events" } }
`), 0644)

	result, ok := reloadConfig(cfgPath, running, state, logger, "test")
	if ok {
		t.Fatal("expected reload to return false when restart is required")
	}
	if len(result.Routes) != len(running.Routes) {
		t.Fatalf("expected running config unchanged")
	}
}

func TestReloadConfig_MissingFile(t *testing.T) {
	original := `
pull_api { auth token "raw:t" }
"/hooks" { pull { path "/events" } }
`
	running := compileForReloadTest(t, original)
	state := newRuntimeState(running)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	result, ok := reloadConfig("/nonexistent/path/Hookaidofile", running, state, logger, "test")
	if ok {
		t.Fatal("expected reload to fail on missing file")
	}
	if len(result.Routes) != len(running.Routes) {
		t.Fatalf("expected running config unchanged")
	}
}

func TestMutateManagedEndpointConfig_PostWriteValidateRollback(t *testing.T) {
	src := `
pull_api { auth token "raw:t" }
"/a" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e1" }
}
"/b" {
  pull { path "/e2" }
}
`
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "Hookaidofile")
	if err := os.WriteFile(cfgPath, []byte(src), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	running := compileForReloadTest(t, src)
	state := newRuntimeState(running)
	if err := state.loadAuth(running); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	// Mutate with a PostWriteValidate that always fails (simulates backlog
	// appearing between initial check and file write).
	_, _, err := mutateManagedEndpointConfig(
		cfgPath,
		running,
		state,
		slog.Default(),
		func(cfg *config.Config, compiled config.Compiled) (admin.ManagementEndpointMutationResult, error) {
			setRouteManagedEndpoint(&cfg.Routes[1], "billing", "invoice.created")
			clearRouteManagedEndpoint(&cfg.Routes[0])
			return admin.ManagementEndpointMutationResult{
				Applied: true,
				Action:  "updated",
				Route:   "/b",
				PostWriteValidate: func() error {
					return errors.New("route has active backlog (simulated TOCTOU)")
				},
			}, nil
		},
		"test_toctou",
	)
	if err == nil {
		t.Fatal("expected error from PostWriteValidate, got nil")
	}
	if !strings.Contains(err.Error(), "active backlog") {
		t.Fatalf("expected backlog error, got: %v", err)
	}

	// Verify config file was rolled back to original content.
	data, _ := os.ReadFile(cfgPath)
	if !strings.Contains(string(data), `endpoint_name "invoice.created"`) {
		t.Fatalf("expected config file to be rolled back, got:\n%s", data)
	}
	// Parse to verify route /a still has the endpoint, not /b.
	parsed, pErr := config.Parse(data)
	if pErr != nil {
		t.Fatalf("parse rolled back config: %v", pErr)
	}
	comp, _ := config.Compile(parsed)
	route, _, found := findManagedEndpointRoute(comp, "billing", "invoice.created")
	if !found {
		t.Fatal("expected billing/invoice.created to still be mapped after rollback")
	}
	if route != "/a" {
		t.Fatalf("expected mapping on /a after rollback, got %q", route)
	}
}
