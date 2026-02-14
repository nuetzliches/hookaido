package config

import (
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseFormat_PreservesPreamble(t *testing.T) {
	in := []byte(`# Hookaidofile
# test preamble

"/webhooks/github" {
  pull { path "/pull/github" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}

	got := string(out)
	if !strings.Contains(got, "# Hookaidofile") || !strings.Contains(got, "# test preamble") {
		t.Fatalf("expected preamble to be preserved, got:\n%s", got)
	}
	if !strings.Contains(got, `"/webhooks/github" {`) {
		t.Fatalf("expected route block, got:\n%s", got)
	}
	if !strings.Contains(got, `path "/pull/github"`) {
		t.Fatalf("expected path, got:\n%s", got)
	}
}

func TestCompile_MinimalConfig_UsesDefaults(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:devtoken" }

"/webhooks/github" {
  pull { path "/pull/github" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Ingress.Listen != defaultIngressListen {
		t.Fatalf("ingress listen: got %q", compiled.Ingress.Listen)
	}
	if compiled.PullAPI.Listen != defaultPullListen {
		t.Fatalf("pull listen: got %q", compiled.PullAPI.Listen)
	}
	if compiled.AdminAPI.Listen != defaultAdminListen {
		t.Fatalf("admin listen: got %q", compiled.AdminAPI.Listen)
	}
	if compiled.PullAPI.MaxBatch != defaultPullAPIMaxBatch {
		t.Fatalf("pull max_batch: got %d", compiled.PullAPI.MaxBatch)
	}
	if compiled.PullAPI.DefaultLeaseTTL != defaultPullAPIDefaultLeaseTTL {
		t.Fatalf("pull default_lease_ttl: got %s", compiled.PullAPI.DefaultLeaseTTL)
	}
	if compiled.PullAPI.MaxLeaseTTL != 0 {
		t.Fatalf("pull max_lease_ttl: got %s", compiled.PullAPI.MaxLeaseTTL)
	}
	if compiled.PullAPI.DefaultMaxWait != 0 {
		t.Fatalf("pull default_max_wait: got %s", compiled.PullAPI.DefaultMaxWait)
	}
	if compiled.PullAPI.MaxWait != 0 {
		t.Fatalf("pull max_wait: got %s", compiled.PullAPI.MaxWait)
	}
	if compiled.PathToRoute["/pull/github"] != "/webhooks/github" {
		t.Fatalf("endpoint map missing: %#v", compiled.PathToRoute)
	}
}

func TestParseFormat_IngressBlock(t *testing.T) {
	in := []byte(`
ingress { listen ":8081" }
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}

	got := string(out)
	if !strings.Contains(got, "ingress {") {
		t.Fatalf("expected ingress block, got:\n%s", got)
	}
	if !strings.Contains(got, `listen ":8081"`) {
		t.Fatalf("expected ingress listen, got:\n%s", got)
	}
}

func TestParseFormat_IngressTLSBlock(t *testing.T) {
	in := []byte(`
ingress {
  listen ":8081"
  tls {
    cert_file "certs/ing.crt"
    key_file "certs/ing.key"
  }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "ingress {") || !strings.Contains(got, "tls {") {
		t.Fatalf("expected ingress tls block, got:\n%s", got)
	}
	if !strings.Contains(got, `cert_file "certs/ing.crt"`) {
		t.Fatalf("expected cert_file, got:\n%s", got)
	}
	if !strings.Contains(got, `key_file "certs/ing.key"`) {
		t.Fatalf("expected key_file, got:\n%s", got)
	}
}

func TestParseFormat_RateLimitBlocks(t *testing.T) {
	in := []byte(`
ingress {
  rate_limit {
    rps 100
    burst 200
  }
}
pull_api { auth token "raw:t" }

"/x" {
  rate_limit {
    rps "5.5"
    burst 10
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "rate_limit {") {
		t.Fatalf("expected rate_limit block, got:\n%s", got)
	}
	if !strings.Contains(got, "rps 100") {
		t.Fatalf("expected ingress rate_limit rps, got:\n%s", got)
	}
	if !strings.Contains(got, `rps "5.5"`) {
		t.Fatalf("expected route rate_limit quoted rps, got:\n%s", got)
	}
	if !strings.Contains(got, "burst 200") || !strings.Contains(got, "burst 10") {
		t.Fatalf("expected rate_limit burst values, got:\n%s", got)
	}
}

func TestParseFormat_APITLSBlock(t *testing.T) {
	in := []byte(`
pull_api {
  listen ":9443"
  tls {
    cert_file "certs/pull.crt"
    key_file "certs/pull.key"
    client_ca "certs/ca.pem"
    client_auth require
  }
  auth token "raw:t"
}

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "tls {") {
		t.Fatalf("expected tls block, got:\n%s", got)
	}
	if !strings.Contains(got, `cert_file "certs/pull.crt"`) {
		t.Fatalf("expected cert_file, got:\n%s", got)
	}
	if !strings.Contains(got, `key_file "certs/pull.key"`) {
		t.Fatalf("expected key_file, got:\n%s", got)
	}
	if !strings.Contains(got, `client_ca "certs/ca.pem"`) {
		t.Fatalf("expected client_ca, got:\n%s", got)
	}
	if !strings.Contains(got, "client_auth require") {
		t.Fatalf("expected client_auth, got:\n%s", got)
	}
}

func TestParseFormat_PullAPILimits(t *testing.T) {
	in := []byte(`
pull_api {
  auth token "raw:t"
  grpc_listen ":9943"
  max_batch 25
  default_lease_ttl "45s"
  max_lease_ttl "60s"
  default_max_wait "8s"
  max_wait 20s
}

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "max_batch 25") {
		t.Fatalf("expected pull_api max_batch, got:\n%s", got)
	}
	if !strings.Contains(got, `grpc_listen ":9943"`) {
		t.Fatalf("expected pull_api grpc_listen, got:\n%s", got)
	}
	if !strings.Contains(got, `default_lease_ttl "45s"`) {
		t.Fatalf("expected pull_api default_lease_ttl, got:\n%s", got)
	}
	if !strings.Contains(got, `max_lease_ttl "60s"`) {
		t.Fatalf("expected pull_api max_lease_ttl, got:\n%s", got)
	}
	if !strings.Contains(got, `default_max_wait "8s"`) {
		t.Fatalf("expected pull_api default_max_wait, got:\n%s", got)
	}
	if !strings.Contains(got, "max_wait 20s") {
		t.Fatalf("expected pull_api max_wait, got:\n%s", got)
	}
}

func TestParseFormat_ObservabilityBlock(t *testing.T) {
	in := []byte(`
	observability {
	  access_log "off"
	  runtime_log info
	  metrics {
	    enabled on
	    listen ":9901"
	    prefix "/m"
	  }
	  tracing {
	    enabled on
	    collector "https://otel.example.com/v1/traces"
	    url_path "/custom/traces"
	    timeout "7s"
	    compression gzip
	    insecure off
	    proxy_url "http://127.0.0.1:8080"
	    tls {
	      ca_file "certs/otel-ca.pem"
	      cert_file "certs/otel-client.pem"
	      key_file "certs/otel-client.key"
	      server_name "otel.example.com"
	      insecure_skip_verify off
	    }
	    retry {
	      enabled on
	      initial_interval "1s"
	      max_interval "3s"
	      max_elapsed_time "30s"
	    }
	    header "Authorization" "Bearer test"
	  }
	}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `access_log "off"`) {
		t.Fatalf("expected access_log quoted, got:\n%s", got)
	}
	if !strings.Contains(got, "runtime_log info") {
		t.Fatalf("expected runtime_log info, got:\n%s", got)
	}
	if !strings.Contains(got, "enabled on") {
		t.Fatalf("expected metrics enabled directive, got:\n%s", got)
	}
	if !strings.Contains(got, `listen ":9901"`) || !strings.Contains(got, `prefix "/m"`) {
		t.Fatalf("expected metrics listen/prefix, got:\n%s", got)
	}
	if !strings.Contains(got, "tracing {") || !strings.Contains(got, "enabled on") {
		t.Fatalf("expected tracing enabled, got:\n%s", got)
	}
	if !strings.Contains(got, `collector "https://otel.example.com/v1/traces"`) {
		t.Fatalf("expected tracing collector, got:\n%s", got)
	}
	if !strings.Contains(got, `url_path "/custom/traces"`) {
		t.Fatalf("expected tracing url_path, got:\n%s", got)
	}
	if !strings.Contains(got, `timeout "7s"`) {
		t.Fatalf("expected tracing timeout, got:\n%s", got)
	}
	if !strings.Contains(got, "compression gzip") {
		t.Fatalf("expected tracing compression, got:\n%s", got)
	}
	if !strings.Contains(got, "insecure off") {
		t.Fatalf("expected tracing insecure, got:\n%s", got)
	}
	if !strings.Contains(got, `proxy_url "http://127.0.0.1:8080"`) {
		t.Fatalf("expected tracing proxy_url, got:\n%s", got)
	}
	if !strings.Contains(got, "tls {") {
		t.Fatalf("expected tracing tls block, got:\n%s", got)
	}
	if !strings.Contains(got, `ca_file "certs/otel-ca.pem"`) {
		t.Fatalf("expected tracing tls ca_file, got:\n%s", got)
	}
	if !strings.Contains(got, `cert_file "certs/otel-client.pem"`) {
		t.Fatalf("expected tracing tls cert_file, got:\n%s", got)
	}
	if !strings.Contains(got, `key_file "certs/otel-client.key"`) {
		t.Fatalf("expected tracing tls key_file, got:\n%s", got)
	}
	if !strings.Contains(got, `server_name "otel.example.com"`) {
		t.Fatalf("expected tracing tls server_name, got:\n%s", got)
	}
	if !strings.Contains(got, "insecure_skip_verify off") {
		t.Fatalf("expected tracing tls insecure_skip_verify, got:\n%s", got)
	}
	if !strings.Contains(got, "retry {") {
		t.Fatalf("expected tracing retry block, got:\n%s", got)
	}
	if !strings.Contains(got, `initial_interval "1s"`) || !strings.Contains(got, `max_interval "3s"`) || !strings.Contains(got, `max_elapsed_time "30s"`) {
		t.Fatalf("expected tracing retry directives, got:\n%s", got)
	}
	if !strings.Contains(got, `header "Authorization" "Bearer test"`) {
		t.Fatalf("expected tracing header, got:\n%s", got)
	}
}

func TestParseFormat_ObservabilityMetricsShorthand(t *testing.T) {
	in := []byte(`
observability {
  metrics "off"
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Observability == nil || cfg.Observability.Metrics == nil {
		t.Fatalf("expected metrics config, got %#v", cfg.Observability)
	}
	if !cfg.Observability.Metrics.EnabledSet {
		t.Fatalf("expected metrics shorthand enabled-set flag")
	}
	if cfg.Observability.Metrics.Enabled != "off" {
		t.Fatalf("expected metrics shorthand value off, got %q", cfg.Observability.Metrics.Enabled)
	}
	if !cfg.Observability.Metrics.EnabledQuoted {
		t.Fatalf("expected quoted metrics shorthand value")
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `metrics "off"`) {
		t.Fatalf("expected metrics shorthand to be preserved, got:\n%s", got)
	}
	if strings.Contains(got, "metrics {") {
		t.Fatalf("expected shorthand formatting, got block form:\n%s", got)
	}
}

func TestParseFormat_ObservabilityTracingShorthand(t *testing.T) {
	in := []byte(`
observability {
  tracing "off"
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Observability == nil || cfg.Observability.Tracing == nil {
		t.Fatalf("expected tracing config, got %#v", cfg.Observability)
	}
	if !cfg.Observability.Tracing.EnabledSet {
		t.Fatalf("expected tracing shorthand enabled-set flag")
	}
	if cfg.Observability.Tracing.Enabled != "off" {
		t.Fatalf("expected tracing shorthand value off, got %q", cfg.Observability.Tracing.Enabled)
	}
	if !cfg.Observability.Tracing.EnabledQuoted {
		t.Fatalf("expected quoted tracing shorthand value")
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `tracing "off"`) {
		t.Fatalf("expected tracing shorthand to be preserved, got:\n%s", got)
	}
	if strings.Contains(got, "tracing {") {
		t.Fatalf("expected shorthand formatting, got block form:\n%s", got)
	}
}

func TestParseFormat_ObservabilityLogBlocks(t *testing.T) {
	in := []byte(`
observability {
  access_log {
    enabled on
    output file
    path "logs/access.log"
    format json
  }
  runtime_log {
    level warn
    output stdout
    format json
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "access_log {") {
		t.Fatalf("expected access_log block, got:\n%s", got)
	}
	if !strings.Contains(got, "output file") || !strings.Contains(got, `path "logs/access.log"`) {
		t.Fatalf("expected access_log sink directives, got:\n%s", got)
	}
	if !strings.Contains(got, "runtime_log {") {
		t.Fatalf("expected runtime_log block, got:\n%s", got)
	}
	if !strings.Contains(got, "level warn") || !strings.Contains(got, "output stdout") {
		t.Fatalf("expected runtime_log directives, got:\n%s", got)
	}
}

func TestParseFormat_DefaultsBlock(t *testing.T) {
	in := []byte(`
defaults {
  max_body "2mb"
  max_headers 64kb
  deliver {
    retry "exponential" max 8 base "2s" cap "2m" jitter "0.2"
    timeout "10s"
    concurrency 20
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `max_body "2mb"`) {
		t.Fatalf("expected quoted max_body, got:\n%s", got)
	}
	if !strings.Contains(got, "max_headers 64kb") {
		t.Fatalf("expected max_headers 64kb, got:\n%s", got)
	}
	if !strings.Contains(got, `retry "exponential" max 8 base "2s" cap "2m" jitter "0.2"`) {
		t.Fatalf("expected quoted retry directive, got:\n%s", got)
	}
	if !strings.Contains(got, `timeout "10s"`) {
		t.Fatalf("expected quoted timeout, got:\n%s", got)
	}
	if !strings.Contains(got, "concurrency 20") {
		t.Fatalf("expected concurrency, got:\n%s", got)
	}
}

func TestParseFormat_DefaultsEgressPolicy(t *testing.T) {
	in := []byte(`
defaults {
  egress {
    allow "10.0.0.0/8"
    allow "*.example.com"
    deny "bad.example.com"
    https_only on
    redirects off
    dns_rebind_protection true
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "egress {") {
		t.Fatalf("expected egress block, got:\n%s", got)
	}
	if !strings.Contains(got, `allow "10.0.0.0/8"`) {
		t.Fatalf("expected allow, got:\n%s", got)
	}
	if !strings.Contains(got, `allow "*.example.com"`) {
		t.Fatalf("expected allow wildcard, got:\n%s", got)
	}
	if !strings.Contains(got, `deny "bad.example.com"`) {
		t.Fatalf("expected deny, got:\n%s", got)
	}
	if !strings.Contains(got, "https_only on") {
		t.Fatalf("expected https_only, got:\n%s", got)
	}
	if !strings.Contains(got, "redirects off") {
		t.Fatalf("expected redirects, got:\n%s", got)
	}
	if !strings.Contains(got, "dns_rebind_protection true") {
		t.Fatalf("expected dns_rebind_protection, got:\n%s", got)
	}
}

func TestParseFormat_DefaultsEgressPolicyMultiValueDirectives(t *testing.T) {
	in := []byte(`
defaults {
  egress {
    allow "10.0.0.0/8" "*.example.com" api.internal
    deny "bad.example.com" "192.0.2.0/24"
    https_only on
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if cfg.Defaults == nil {
		t.Fatalf("expected defaults block")
	}
	if got := cfg.Defaults.Egress.Allow; len(got) != 3 || got[0] != "10.0.0.0/8" || got[1] != "*.example.com" || got[2] != "api.internal" {
		t.Fatalf("unexpected egress allow values: %#v", got)
	}
	if got := cfg.Defaults.Egress.Deny; len(got) != 2 || got[0] != "bad.example.com" || got[1] != "192.0.2.0/24" {
		t.Fatalf("unexpected egress deny values: %#v", got)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `allow "10.0.0.0/8"`) {
		t.Fatalf("expected first allow directive, got:\n%s", got)
	}
	if !strings.Contains(got, `allow "*.example.com"`) {
		t.Fatalf("expected second allow directive, got:\n%s", got)
	}
	if !strings.Contains(got, "allow api.internal") {
		t.Fatalf("expected third allow directive, got:\n%s", got)
	}
	if !strings.Contains(got, `deny "bad.example.com"`) {
		t.Fatalf("expected first deny directive, got:\n%s", got)
	}
	if !strings.Contains(got, `deny "192.0.2.0/24"`) {
		t.Fatalf("expected second deny directive, got:\n%s", got)
	}
}

func TestParseFormat_DefaultsPublishPolicy(t *testing.T) {
	in := []byte(`
defaults {
  publish_policy {
    direct "off"
    managed on
    allow_pull_routes off
    allow_deliver_routes "on"
    require_actor on
    require_request_id "off"
    fail_closed true
    actor_allow "ops@example.test"
    actor_allow "svc-publisher"
    actor_prefix "role:ops/"
    actor_prefix "role:platform/"
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "publish_policy {") {
		t.Fatalf("expected publish_policy block, got:\n%s", got)
	}
	if !strings.Contains(got, `direct "off"`) {
		t.Fatalf("expected direct off, got:\n%s", got)
	}
	if !strings.Contains(got, "managed on") {
		t.Fatalf("expected managed on, got:\n%s", got)
	}
	if !strings.Contains(got, "allow_pull_routes off") {
		t.Fatalf("expected allow_pull_routes off, got:\n%s", got)
	}
	if !strings.Contains(got, `allow_deliver_routes "on"`) {
		t.Fatalf("expected allow_deliver_routes on, got:\n%s", got)
	}
	if !strings.Contains(got, "require_actor on") {
		t.Fatalf("expected require_actor on, got:\n%s", got)
	}
	if !strings.Contains(got, `require_request_id "off"`) {
		t.Fatalf("expected require_request_id off, got:\n%s", got)
	}
	if !strings.Contains(got, "fail_closed true") {
		t.Fatalf("expected fail_closed true, got:\n%s", got)
	}
	if !strings.Contains(got, `actor_allow "ops@example.test"`) {
		t.Fatalf("expected actor_allow ops@example.test, got:\n%s", got)
	}
	if !strings.Contains(got, `actor_allow "svc-publisher"`) {
		t.Fatalf("expected actor_allow svc-publisher, got:\n%s", got)
	}
	if !strings.Contains(got, `actor_prefix "role:ops/"`) {
		t.Fatalf("expected actor_prefix role:ops/, got:\n%s", got)
	}
	if !strings.Contains(got, `actor_prefix "role:platform/"`) {
		t.Fatalf("expected actor_prefix role:platform/, got:\n%s", got)
	}
}

func TestParseFormat_DefaultsPublishPolicyMultiValueActorDirectives(t *testing.T) {
	in := []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test" svc-publisher "platform@example.test"
    actor_prefix "role:ops/" role:platform/
    require_actor on
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if cfg.Defaults == nil || cfg.Defaults.PublishPolicy == nil {
		t.Fatalf("expected defaults.publish_policy, got %#v", cfg.Defaults)
	}
	policy := cfg.Defaults.PublishPolicy
	if got := policy.ActorAllow; len(got) != 3 || got[0] != "ops@example.test" || got[1] != "svc-publisher" || got[2] != "platform@example.test" {
		t.Fatalf("unexpected actor_allow values: %#v", got)
	}
	if got := policy.ActorPrefix; len(got) != 2 || got[0] != "role:ops/" || got[1] != "role:platform/" {
		t.Fatalf("unexpected actor_prefix values: %#v", got)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `actor_allow "ops@example.test"`) {
		t.Fatalf("expected first actor_allow directive, got:\n%s", got)
	}
	if !strings.Contains(got, "actor_allow svc-publisher") {
		t.Fatalf("expected second actor_allow directive, got:\n%s", got)
	}
	if !strings.Contains(got, `actor_allow "platform@example.test"`) {
		t.Fatalf("expected third actor_allow directive, got:\n%s", got)
	}
	if !strings.Contains(got, `actor_prefix "role:ops/"`) {
		t.Fatalf("expected first actor_prefix directive, got:\n%s", got)
	}
	if !strings.Contains(got, "actor_prefix role:platform/") {
		t.Fatalf("expected second actor_prefix directive, got:\n%s", got)
	}
}

func TestParseFormat_VarsBlock(t *testing.T) {
	in := []byte(`
vars {
  LISTEN ":18082"
  PULL_ENDPOINT "/pull/github"
}

pull_api { auth token "raw:t" }

"/x" { pull { path "{vars.PULL_ENDPOINT}" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "vars {") {
		t.Fatalf("expected vars block, got:\n%s", got)
	}
	if !strings.Contains(got, `LISTEN ":18082"`) {
		t.Fatalf("expected LISTEN var, got:\n%s", got)
	}
	if !strings.Contains(got, `PULL_ENDPOINT "/pull/github"`) {
		t.Fatalf("expected PULL_ENDPOINT var, got:\n%s", got)
	}
}

func TestParseFormat_SecretsBlock(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "env:MY_SECRET"
    valid_from "2025-01-01T00:00:00Z"
    valid_until "2026-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "secrets {") {
		t.Fatalf("expected secrets block, got:\n%s", got)
	}
	if !strings.Contains(got, `secret "S1"`) {
		t.Fatalf("expected secret id, got:\n%s", got)
	}
	if !strings.Contains(got, `value "env:MY_SECRET"`) {
		t.Fatalf("expected value, got:\n%s", got)
	}
	if !strings.Contains(got, `valid_from "2025-01-01T00:00:00Z"`) {
		t.Fatalf("expected valid_from, got:\n%s", got)
	}
	if !strings.Contains(got, `valid_until "2026-01-01T00:00:00Z"`) {
		t.Fatalf("expected valid_until, got:\n%s", got)
	}
}

func TestParseFormat_DeliverBlock(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  deliver_concurrency 5
  deliver "https://ci.internal/build" {
    retry exponential max 3 base 1s cap 10s jitter 0.1
    timeout "5s"
    sign hmac "raw:deliver-signing"
    sign signature_header "X-Signature"
    sign timestamp_header "X-Signed-At"
    sign secret_selection "oldest_valid"
  }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `deliver "https://ci.internal/build" {`) {
		t.Fatalf("expected deliver block, got:\n%s", got)
	}
	if !strings.Contains(got, "deliver_concurrency 5") {
		t.Fatalf("expected deliver_concurrency, got:\n%s", got)
	}
	if !strings.Contains(got, "retry exponential max 3 base 1s cap 10s jitter 0.1") {
		t.Fatalf("expected retry directive, got:\n%s", got)
	}
	if !strings.Contains(got, `timeout "5s"`) {
		t.Fatalf("expected timeout directive, got:\n%s", got)
	}
	if !strings.Contains(got, `sign hmac "raw:deliver-signing"`) {
		t.Fatalf("expected sign hmac directive, got:\n%s", got)
	}
	if !strings.Contains(got, `sign signature_header "X-Signature"`) {
		t.Fatalf("expected sign signature_header directive, got:\n%s", got)
	}
	if !strings.Contains(got, `sign timestamp_header "X-Signed-At"`) {
		t.Fatalf("expected sign timestamp_header directive, got:\n%s", got)
	}
	if !strings.Contains(got, `sign secret_selection "oldest_valid"`) {
		t.Fatalf("expected sign secret_selection directive, got:\n%s", got)
	}
}

func TestParseFormat_DeliverBlockSigningSecretRef(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "raw:deliver-signing"
    valid_from "2026-01-01T00:00:00Z"
  }
  secret "S2" {
    value "raw:deliver-signing-next"
    valid_from "2027-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac secret_ref "S1"
    sign hmac secret_ref "S2"
  }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `sign hmac secret_ref "S1"`) {
		t.Fatalf("expected sign hmac secret_ref directive, got:\n%s", got)
	}
	if !strings.Contains(got, `sign hmac secret_ref "S2"`) {
		t.Fatalf("expected second sign hmac secret_ref directive, got:\n%s", got)
	}
}

func TestParseFormat_DeliverBlockSigningSecretRefMultiValueDirective(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "raw:deliver-signing"
    valid_from "2026-01-01T00:00:00Z"
  }
  secret "S2" {
    value "raw:deliver-signing-next"
    valid_from "2027-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac secret_ref "S1" "S2"
  }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 || len(cfg.Routes[0].Deliveries) != 1 {
		t.Fatalf("expected one route with one deliver target, got %#v", cfg.Routes)
	}
	deliver := cfg.Routes[0].Deliveries[0]
	if got := deliver.SignHMACSecretRefs; len(got) != 2 || got[0] != "S1" || got[1] != "S2" {
		t.Fatalf("unexpected sign hmac secret_ref values: %#v", got)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `sign hmac secret_ref "S1"`) {
		t.Fatalf("expected sign hmac secret_ref S1 directive, got:\n%s", got)
	}
	if !strings.Contains(got, `sign hmac secret_ref "S2"`) {
		t.Fatalf("expected sign hmac secret_ref S2 directive, got:\n%s", got)
	}
}

func TestParseFormat_MatchBlock(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  match {
    method POST
    host "hooks.example.com"
    header "X-Event" "push"
    header_exists "X-Signature"
    query "ref" "main"
    remote_ip "203.0.113.0/24"
    query_exists "delivery_id"
  }
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "match {") {
		t.Fatalf("expected match block, got:\n%s", got)
	}
	if !strings.Contains(got, `host "hooks.example.com"`) {
		t.Fatalf("expected host matcher, got:\n%s", got)
	}
	if !strings.Contains(got, "method POST") {
		t.Fatalf("expected method matcher, got:\n%s", got)
	}
	if !strings.Contains(got, `header "X-Event" "push"`) {
		t.Fatalf("expected header matcher, got:\n%s", got)
	}
	if !strings.Contains(got, `header_exists "X-Signature"`) {
		t.Fatalf("expected header_exists matcher, got:\n%s", got)
	}
	if !strings.Contains(got, `query "ref" "main"`) {
		t.Fatalf("expected query matcher, got:\n%s", got)
	}
	if !strings.Contains(got, `remote_ip "203.0.113.0/24"`) {
		t.Fatalf("expected remote_ip matcher, got:\n%s", got)
	}
	if !strings.Contains(got, `query_exists "delivery_id"`) {
		t.Fatalf("expected query_exists matcher, got:\n%s", got)
	}
}

func TestParseFormat_MatchBlockMultiValueDirectives(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  match {
    method POST put
    host "hooks.example.com" "*.example.com"
    header "X-Event" "push" X-Tenant acme
    header_exists "X-Signature" X-Trace-Id
    query "ref" "main" env prod
    remote_ip "203.0.113.0/24" "2001:db8::1"
    query_exists "delivery_id" trace_id
  }
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if len(cfg.Routes) != 1 || cfg.Routes[0].Match == nil {
		t.Fatalf("expected route match block, got %#v", cfg.Routes)
	}
	match := cfg.Routes[0].Match
	if got := match.Methods; len(got) != 2 || got[0] != "POST" || got[1] != "put" {
		t.Fatalf("unexpected methods: %#v", got)
	}
	if got := match.Hosts; len(got) != 2 || got[0] != "hooks.example.com" || got[1] != "*.example.com" {
		t.Fatalf("unexpected hosts: %#v", got)
	}
	if got := match.Headers; len(got) != 2 || got[0].Name != "X-Event" || got[0].Value != "push" || got[1].Name != "X-Tenant" || got[1].Value != "acme" {
		t.Fatalf("unexpected headers: %#v", got)
	}
	if got := match.HeaderExists; len(got) != 2 || got[0] != "X-Signature" || got[1] != "X-Trace-Id" {
		t.Fatalf("unexpected header_exists: %#v", got)
	}
	if got := match.Query; len(got) != 2 || got[0].Name != "ref" || got[0].Value != "main" || got[1].Name != "env" || got[1].Value != "prod" {
		t.Fatalf("unexpected query: %#v", got)
	}
	if got := match.RemoteIPs; len(got) != 2 || got[0] != "203.0.113.0/24" || got[1] != "2001:db8::1" {
		t.Fatalf("unexpected remote_ip: %#v", got)
	}
	if got := match.QueryExists; len(got) != 2 || got[0] != "delivery_id" || got[1] != "trace_id" {
		t.Fatalf("unexpected query_exists: %#v", got)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "method POST") || !strings.Contains(got, "method put") {
		t.Fatalf("expected split method directives, got:\n%s", got)
	}
	if !strings.Contains(got, `host "hooks.example.com"`) || !strings.Contains(got, `host "*.example.com"`) {
		t.Fatalf("expected split host directives, got:\n%s", got)
	}
	if !strings.Contains(got, `header "X-Event" "push"`) || !strings.Contains(got, "header X-Tenant acme") {
		t.Fatalf("expected split header directives, got:\n%s", got)
	}
	if !strings.Contains(got, `header_exists "X-Signature"`) || !strings.Contains(got, "header_exists X-Trace-Id") {
		t.Fatalf("expected split header_exists directives, got:\n%s", got)
	}
	if !strings.Contains(got, `query "ref" "main"`) || !strings.Contains(got, "query env prod") {
		t.Fatalf("expected split query directives, got:\n%s", got)
	}
	if !strings.Contains(got, `remote_ip "203.0.113.0/24"`) || !strings.Contains(got, `remote_ip "2001:db8::1"`) {
		t.Fatalf("expected split remote_ip directives, got:\n%s", got)
	}
	if !strings.Contains(got, `query_exists "delivery_id"`) || !strings.Contains(got, "query_exists trace_id") {
		t.Fatalf("expected split query_exists directives, got:\n%s", got)
	}
}

func TestParseFormat_RouteManagementLabels(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `application "billing"`) {
		t.Fatalf("expected application directive, got:\n%s", got)
	}
	if !strings.Contains(got, `endpoint_name "invoice.created"`) {
		t.Fatalf("expected endpoint_name directive, got:\n%s", got)
	}
}

func TestParseFormat_RoutePublish(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  publish {
    enabled off
    direct "on"
    managed 0
  }
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "enabled off") {
		t.Fatalf("expected route publish enabled directive, got:\n%s", got)
	}
	if !strings.Contains(got, `direct "on"`) {
		t.Fatalf("expected route publish direct directive, got:\n%s", got)
	}
	if !strings.Contains(got, "managed 0") {
		t.Fatalf("expected route publish managed directive, got:\n%s", got)
	}
}

func TestParseFormat_NamedMatcher(t *testing.T) {
	in := []byte(`
@webhook {
  host "hooks.example.com"
  method POST
}

pull_api { auth token "raw:t" }

"/x" {
  match @webhook
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "@webhook {") {
		t.Fatalf("expected named matcher, got:\n%s", got)
	}
	if !strings.Contains(got, "match @webhook") {
		t.Fatalf("expected match reference, got:\n%s", got)
	}
}

func TestParseFormat_NamedMatcherMultiRefsSingleDirective(t *testing.T) {
	in := []byte(`
@hosted {
  host "hooks.example.com"
}
@signed {
  header_exists "X-Signature"
}

pull_api { auth token "raw:t" }

"/x" {
  match @hosted @signed
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected one route, got %#v", cfg.Routes)
	}
	if got := cfg.Routes[0].MatchRefs; len(got) != 2 || got[0] != "hosted" || got[1] != "signed" {
		t.Fatalf("unexpected match refs: %#v", got)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "match @hosted") {
		t.Fatalf("expected match @hosted, got:\n%s", got)
	}
	if !strings.Contains(got, "match @signed") {
		t.Fatalf("expected match @signed, got:\n%s", got)
	}
}

func TestParseFormat_AuthBasic(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth basic "ci" "s3cret"
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `auth basic "ci" "s3cret"`) {
		t.Fatalf("expected auth basic, got:\n%s", got)
	}
}

func TestParseFormat_AuthForwardShorthand(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth forward "https://auth.example.test/check"
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].AuthForward == nil {
		t.Fatalf("expected auth forward, got %#v", cfg.Routes)
	}
	if cfg.Routes[0].AuthForward.BlockSet {
		t.Fatalf("expected shorthand (no block), got block")
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `auth forward "https://auth.example.test/check"`) {
		t.Fatalf("expected auth forward shorthand, got:\n%s", got)
	}
}

func TestParseFormat_AuthForwardBlock(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

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

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].AuthForward == nil {
		t.Fatalf("expected auth forward, got %#v", cfg.Routes)
	}
	fwd := cfg.Routes[0].AuthForward
	if !fwd.BlockSet {
		t.Fatalf("expected auth forward block flag")
	}
	if !fwd.TimeoutSet || fwd.Timeout != "3s" {
		t.Fatalf("timeout: got %#v", fwd.Timeout)
	}
	if len(fwd.CopyHeaders) != 2 {
		t.Fatalf("copy_headers: got %#v", fwd.CopyHeaders)
	}
	if !fwd.BodyLimitSet || fwd.BodyLimit != "64kb" {
		t.Fatalf("body_limit: got %#v", fwd.BodyLimit)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "auth forward \"https://auth.example.test/check\" {") {
		t.Fatalf("expected auth forward block, got:\n%s", got)
	}
	if !strings.Contains(got, `copy_headers "x-user-id"`) || !strings.Contains(got, "copy_headers x-role") {
		t.Fatalf("expected copy_headers directives, got:\n%s", got)
	}
}

func TestParseFormat_AuthForwardBlockCopyHeadersMultiValueDirective(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth forward "https://auth.example.test/check" {
    copy_headers "x-user-id" x-role X-Team
  }
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].AuthForward == nil {
		t.Fatalf("expected auth forward, got %#v", cfg.Routes)
	}
	fwd := cfg.Routes[0].AuthForward
	if len(fwd.CopyHeaders) != 3 {
		t.Fatalf("copy_headers: got %#v", fwd.CopyHeaders)
	}
	if fwd.CopyHeaders[0] != "x-user-id" || fwd.CopyHeaders[1] != "x-role" || fwd.CopyHeaders[2] != "X-Team" {
		t.Fatalf("unexpected copy_headers values: %#v", fwd.CopyHeaders)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `copy_headers "x-user-id"`) || !strings.Contains(got, "copy_headers x-role") || !strings.Contains(got, "copy_headers X-Team") {
		t.Fatalf("expected split copy_headers directives, got:\n%s", got)
	}
}

func TestParseFormat_AuthForwardEmptyBlockPreserved(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth forward "https://auth.example.test/check" {}
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "auth forward \"https://auth.example.test/check\" {") {
		t.Fatalf("expected empty auth forward block to be preserved, got:\n%s", got)
	}
}

func TestParseFormat_PullAuthTokens(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  pull {
    path "/e"
    auth token "raw:p1"
    auth token "raw:p2"
  }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `auth token "raw:p1"`) || !strings.Contains(got, `auth token "raw:p2"`) {
		t.Fatalf("expected pull auth tokens, got:\n%s", got)
	}
}

func TestParseFormat_QueueBlock(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue { backend "sqlite" }
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `queue {`) {
		t.Fatalf("expected queue block, got:\n%s", got)
	}
	if !strings.Contains(got, `backend "sqlite"`) {
		t.Fatalf("expected backend sqlite, got:\n%s", got)
	}
}

func TestParseFormat_QueueShorthand(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue "memory"
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].Queue == nil {
		t.Fatalf("expected route queue config, got %#v", cfg.Routes)
	}
	if !cfg.Routes[0].Queue.BackendSet {
		t.Fatalf("expected queue shorthand backend set")
	}
	if cfg.Routes[0].Queue.Backend != "memory" {
		t.Fatalf("expected queue shorthand backend memory, got %q", cfg.Routes[0].Queue.Backend)
	}
	if !cfg.Routes[0].Queue.BackendQuoted {
		t.Fatalf("expected quoted queue shorthand backend")
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `queue "memory"`) {
		t.Fatalf("expected queue shorthand to be preserved, got:\n%s", got)
	}
	if strings.Contains(got, "queue {") {
		t.Fatalf("expected shorthand formatting, got block form:\n%s", got)
	}
}

func TestParseFormat_QueueShorthandPostgres(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue postgres
  pull { path "/e" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].Queue == nil {
		t.Fatalf("expected route queue config, got %#v", cfg.Routes)
	}
	if !cfg.Routes[0].Queue.BackendSet {
		t.Fatalf("expected queue shorthand backend set")
	}
	if cfg.Routes[0].Queue.Backend != "postgres" {
		t.Fatalf("expected queue shorthand backend postgres, got %q", cfg.Routes[0].Queue.Backend)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "queue postgres") {
		t.Fatalf("expected queue shorthand to be preserved, got:\n%s", got)
	}
	if strings.Contains(got, "queue {") {
		t.Fatalf("expected shorthand formatting, got block form:\n%s", got)
	}
}

func TestParseFormat_QueueLimitsBlock(t *testing.T) {
	in := []byte(`
queue_limits {
  max_depth "123"
  drop_policy reject
}
 
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `max_depth "123"`) {
		t.Fatalf("expected quoted max_depth, got:\n%s", got)
	}
	if !strings.Contains(got, "drop_policy reject") {
		t.Fatalf("expected drop_policy reject, got:\n%s", got)
	}
}

func TestParseFormat_QueueRetentionBlock(t *testing.T) {
	in := []byte(`
queue_retention {
  max_age "7d"
  prune_interval 10m
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `max_age "7d"`) {
		t.Fatalf("expected quoted max_age, got:\n%s", got)
	}
	if !strings.Contains(got, "prune_interval 10m") {
		t.Fatalf("expected prune_interval 10m, got:\n%s", got)
	}
}

func TestParseFormat_DeliveredRetentionBlock(t *testing.T) {
	in := []byte(`
delivered_retention {
  max_age "3d"
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `max_age "3d"`) {
		t.Fatalf("expected quoted max_age, got:\n%s", got)
	}
}

func TestParseFormat_DLQRetentionBlock(t *testing.T) {
	in := []byte(`
dlq_retention {
  max_age "30d"
  max_depth 5000
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `max_age "30d"`) {
		t.Fatalf("expected quoted max_age, got:\n%s", got)
	}
	if !strings.Contains(got, "max_depth 5000") {
		t.Fatalf("expected max_depth 5000, got:\n%s", got)
	}
}

func TestCompile_IngressMustNotShareListener(t *testing.T) {
	in := []byte(`
ingress { listen ":9443" }
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "ingress.listen") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected ingress listener error, got %#v", res.Errors)
	}
}

func TestCompile_PullAPIGrpcListenMustNotShareListener(t *testing.T) {
	in := []byte(`
ingress { listen ":8443" }
pull_api {
  listen ":9443"
  grpc_listen ":8443"
  auth token "raw:t"
}

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "pull_api.grpc_listen must not share a listener") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected pull_api.grpc_listen listener conflict error, got %#v", res.Errors)
	}
}

func TestCompile_PullAPIGrpcListenMustNotShareMetricsListener(t *testing.T) {
	in := []byte(`
observability {
  metrics {
    enabled on
    listen ":9901"
  }
}
pull_api {
  grpc_listen ":9901"
  auth token "raw:t"
}

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "pull_api.grpc_listen must not share a listener with observability.metrics.listen") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected pull_api.grpc_listen metrics conflict error, got %#v", res.Errors)
	}
}

func TestCompile_IngressTLSConfig(t *testing.T) {
	in := []byte(`
ingress { tls { cert_file "certs/ing.crt" key_file "certs/ing.key" } }
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if !compiled.Ingress.TLS.Enabled {
		t.Fatalf("expected ingress TLS enabled")
	}
	if compiled.Ingress.TLS.CertFile != "certs/ing.crt" || compiled.Ingress.TLS.KeyFile != "certs/ing.key" {
		t.Fatalf("unexpected ingress TLS config: %#v", compiled.Ingress.TLS)
	}
}

func TestCompile_IngressTLSValidation(t *testing.T) {
	in := []byte(`
ingress { tls { cert_file "certs/ing.crt" } }
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "ingress.tls.key_file") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected ingress tls key_file error, got %#v", res.Errors)
	}
}

func TestCompile_IngressRateLimit(t *testing.T) {
	in := []byte(`
ingress {
  rate_limit { rps 12.5 burst 25 }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if !compiled.Ingress.RateLimit.Enabled {
		t.Fatalf("expected ingress rate limit enabled")
	}
	if compiled.Ingress.RateLimit.RPS != 12.5 {
		t.Fatalf("ingress rate_limit.rps: got %v", compiled.Ingress.RateLimit.RPS)
	}
	if compiled.Ingress.RateLimit.Burst != 25 {
		t.Fatalf("ingress rate_limit.burst: got %d", compiled.Ingress.RateLimit.Burst)
	}
}

func TestCompile_IngressRateLimitValidation(t *testing.T) {
	in := []byte(`
ingress {
  rate_limit { burst 10 }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "ingress.rate_limit.rps must be set") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected ingress rate_limit rps validation error, got %#v", res.Errors)
	}
}

func TestCompile_PlaceholdersResolve(t *testing.T) {
	t.Setenv("HOOKAIDO_TEST_INGRESS", ":18080")
	t.Setenv("HOOKAIDO_TEST_PULL", ":19443")

	in := []byte(`
ingress { listen {$HOOKAIDO_TEST_INGRESS} }
pull_api { listen {env.HOOKAIDO_TEST_PULL} auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Ingress.Listen != ":18080" {
		t.Fatalf("ingress listen: got %q", compiled.Ingress.Listen)
	}
	if compiled.PullAPI.Listen != ":19443" {
		t.Fatalf("pull listen: got %q", compiled.PullAPI.Listen)
	}
}

func TestCompile_PlaceholderDefaultValue(t *testing.T) {
	const key = "HOOKAIDO_TEST_DEFAULT"
	if prev, ok := os.LookupEnv(key); ok {
		t.Cleanup(func() { _ = os.Setenv(key, prev) })
	} else {
		t.Cleanup(func() { _ = os.Unsetenv(key) })
	}
	_ = os.Unsetenv(key)

	in := []byte(`
ingress { listen "{$HOOKAIDO_TEST_DEFAULT::18081}" }
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Ingress.Listen != ":18081" {
		t.Fatalf("ingress listen: got %q", compiled.Ingress.Listen)
	}
}

func TestCompile_VarsResolve(t *testing.T) {
	in := []byte(`
vars {
  INGRESS_LISTEN ":18083"
  ENDPOINT "/pull/github"
  ENDPOINT_ALIAS "{vars.ENDPOINT}"
}
ingress { listen "{vars.INGRESS_LISTEN}" }
pull_api { auth token "raw:t" }

"/x" { pull { path "{vars.ENDPOINT_ALIAS}" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Ingress.Listen != ":18083" {
		t.Fatalf("ingress listen: got %q", compiled.Ingress.Listen)
	}
	if compiled.PathToRoute["/pull/github"] != "/x" {
		t.Fatalf("endpoint map missing: %#v", compiled.PathToRoute)
	}
	if compiled.Vars["ENDPOINT_ALIAS"] != "/pull/github" {
		t.Fatalf("compiled vars mismatch: %#v", compiled.Vars)
	}
}

func TestCompile_VarsUnknownReference(t *testing.T) {
	in := []byte(`
vars {
  INGRESS_LISTEN ":18084"
}
ingress { listen "{vars.MISSING}" }
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}

	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "unknown var") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected unknown var error, got %#v", res.Errors)
	}
}

func TestCompile_VarsCycle(t *testing.T) {
	in := []byte(`
vars {
  A "{vars.B}"
  B "{vars.A}"
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}

	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "vars cycle detected") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected vars cycle error, got %#v", res.Errors)
	}
}

func TestCompile_FilePlaceholderResolve(t *testing.T) {
	secretPath := filepath.Join(t.TempDir(), "listen.txt")
	if err := os.WriteFile(secretPath, []byte(":18082"), 0o600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	in := []byte(fmt.Sprintf(`
ingress { listen {file.%s} }
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`, secretPath))

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Ingress.Listen != ":18082" {
		t.Fatalf("ingress listen: got %q", compiled.Ingress.Listen)
	}
}

func TestCompile_FilePlaceholderMissingFile(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "missing.txt")

	in := []byte(fmt.Sprintf(`
ingress { listen {file.%s} }
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`, missingPath))

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}

	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "file placeholder") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected file placeholder error, got %#v", res.Errors)
	}
}

func TestCompile_ObservabilityDefaults(t *testing.T) {
	in := []byte(`
observability {
  metrics {}
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if !compiled.Observability.Metrics.Enabled {
		t.Fatalf("expected metrics enabled")
	}
	if compiled.Observability.Metrics.Listen != defaultMetricsListen {
		t.Fatalf("metrics listen: got %q", compiled.Observability.Metrics.Listen)
	}
	if compiled.Observability.Metrics.Prefix != defaultMetricsPrefix {
		t.Fatalf("metrics prefix: got %q", compiled.Observability.Metrics.Prefix)
	}
}

func TestCompile_ObservabilityMetricsShorthandEnabled(t *testing.T) {
	in := []byte(`
observability {
  metrics on
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if !compiled.Observability.Metrics.Enabled {
		t.Fatalf("expected metrics enabled")
	}
	if compiled.Observability.Metrics.Listen != defaultMetricsListen {
		t.Fatalf("metrics listen: got %q", compiled.Observability.Metrics.Listen)
	}
	if compiled.Observability.Metrics.Prefix != defaultMetricsPrefix {
		t.Fatalf("metrics prefix: got %q", compiled.Observability.Metrics.Prefix)
	}
}

func TestCompile_ObservabilityMetricsShorthandDisabled(t *testing.T) {
	in := []byte(`
observability {
  metrics off
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Observability.Metrics.Enabled {
		t.Fatalf("expected metrics disabled")
	}
	if compiled.Observability.Metrics.Listen != "" || compiled.Observability.Metrics.Prefix != "" {
		t.Fatalf("expected empty metrics listen/prefix when shorthand disabled, got listen=%q prefix=%q", compiled.Observability.Metrics.Listen, compiled.Observability.Metrics.Prefix)
	}
}

func TestCompile_ObservabilityMetricsDisabled(t *testing.T) {
	in := []byte(`
observability {
  metrics {
    enabled off
    listen ":9901"
    prefix "/m"
  }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Observability.Metrics.Enabled {
		t.Fatalf("expected metrics disabled")
	}
	if compiled.Observability.Metrics.Listen != "" || compiled.Observability.Metrics.Prefix != "" {
		t.Fatalf("expected empty metrics listen/prefix when disabled, got listen=%q prefix=%q", compiled.Observability.Metrics.Listen, compiled.Observability.Metrics.Prefix)
	}
	if len(res.Warnings) == 0 {
		t.Fatalf("expected warnings for ignored listen/prefix when metrics disabled")
	}
}

func TestCompile_ObservabilityMetricsEnabledInvalid(t *testing.T) {
	in := []byte(`
observability {
  metrics {
    enabled maybe
  }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected compile error for invalid metrics.enabled")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "observability.metrics.enabled") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected observability.metrics.enabled error, got %#v", res.Errors)
	}
}

func TestCompile_ObservabilityMetricsShorthandInvalid(t *testing.T) {
	in := []byte(`
observability {
  metrics maybe
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected compile error for invalid metrics shorthand")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "observability.metrics.enabled") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected observability.metrics.enabled error, got %#v", res.Errors)
	}
}

func TestCompile_ObservabilityTracingShorthandEnabled(t *testing.T) {
	in := []byte(`
observability {
  tracing on
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if !compiled.Observability.TracingEnabled {
		t.Fatalf("expected tracing enabled")
	}
}

func TestCompile_ObservabilityTracingShorthandDisabled(t *testing.T) {
	in := []byte(`
observability {
  tracing off
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Observability.TracingEnabled {
		t.Fatalf("expected tracing disabled")
	}
}

func TestCompile_ObservabilityTracingShorthandInvalid(t *testing.T) {
	in := []byte(`
observability {
  tracing maybe
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected compile error for invalid tracing shorthand")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "observability.tracing.enabled") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected observability.tracing.enabled error, got %#v", res.Errors)
	}
}

func TestParse_ObservabilityMetricsDuplicateDirective(t *testing.T) {
	in := []byte(`
observability {
  metrics {
    enabled on
    enabled off
  }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	_, err := Parse(in)
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate metrics enabled") {
		t.Fatalf("expected duplicate metrics enabled error, got %v", err)
	}
}

func TestParse_ObservabilityMetricsDuplicateShorthandAndBlock(t *testing.T) {
	in := []byte(`
observability {
  metrics off
  metrics {}
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	_, err := Parse(in)
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate metrics directive") {
		t.Fatalf("expected duplicate metrics directive error, got %v", err)
	}
}

func TestParse_ObservabilityTracingDuplicateShorthandAndBlock(t *testing.T) {
	in := []byte(`
observability {
  tracing off
  tracing {}
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	_, err := Parse(in)
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate tracing directive") {
		t.Fatalf("expected duplicate tracing directive error, got %v", err)
	}
}

func TestParse_ObservabilityTracingDuplicateDirective(t *testing.T) {
	in := []byte(`
observability {
  tracing {
    collector "https://otel.example.com/v1/traces"
    collector "https://otel.example.com/other"
  }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	_, err := Parse(in)
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate tracing collector") {
		t.Fatalf("expected duplicate tracing collector error, got %v", err)
	}
}

func TestCompile_ObservabilityTracing(t *testing.T) {
	in := []byte(`
	observability {
	  tracing {
	    enabled on
	    collector "https://otel.example.com/v1/traces"
	    url_path "/custom/traces"
	    timeout 7s
	    compression gzip
	    insecure off
	    proxy_url "http://127.0.0.1:8080"
	    tls {
	      ca_file "certs/otel-ca.pem"
	      cert_file "certs/otel-client.pem"
	      key_file "certs/otel-client.key"
	      server_name "otel.example.com"
	      insecure_skip_verify on
	    }
	    retry {
	      enabled on
	      initial_interval 1s
	      max_interval 3s
	      max_elapsed_time 30s
	    }
	    header "Authorization" "Bearer test"
	  }
	}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if !compiled.Observability.TracingEnabled {
		t.Fatalf("expected tracing enabled")
	}
	if compiled.Observability.TracingCollector != "https://otel.example.com/v1/traces" {
		t.Fatalf("expected tracing endpoint, got %q", compiled.Observability.TracingCollector)
	}
	if compiled.Observability.TracingURLPath != "/custom/traces" {
		t.Fatalf("expected tracing url_path /custom/traces, got %q", compiled.Observability.TracingURLPath)
	}
	if !compiled.Observability.TracingTimeoutSet || compiled.Observability.TracingTimeout != 7*time.Second {
		t.Fatalf("expected tracing timeout 7s, got set=%v duration=%s", compiled.Observability.TracingTimeoutSet, compiled.Observability.TracingTimeout)
	}
	if compiled.Observability.TracingCompression != "gzip" {
		t.Fatalf("expected tracing compression gzip, got %q", compiled.Observability.TracingCompression)
	}
	if compiled.Observability.TracingInsecure {
		t.Fatalf("expected tracing insecure off")
	}
	if compiled.Observability.TracingProxyURL != "http://127.0.0.1:8080" {
		t.Fatalf("expected tracing proxy url, got %q", compiled.Observability.TracingProxyURL)
	}
	if compiled.Observability.TracingTLSCAFile != "certs/otel-ca.pem" {
		t.Fatalf("expected tracing tls ca file, got %q", compiled.Observability.TracingTLSCAFile)
	}
	if compiled.Observability.TracingTLSCertFile != "certs/otel-client.pem" {
		t.Fatalf("expected tracing tls cert file, got %q", compiled.Observability.TracingTLSCertFile)
	}
	if compiled.Observability.TracingTLSKeyFile != "certs/otel-client.key" {
		t.Fatalf("expected tracing tls key file, got %q", compiled.Observability.TracingTLSKeyFile)
	}
	if compiled.Observability.TracingTLSServerName != "otel.example.com" {
		t.Fatalf("expected tracing tls server name, got %q", compiled.Observability.TracingTLSServerName)
	}
	if !compiled.Observability.TracingTLSInsecureSkipVerify {
		t.Fatalf("expected tracing tls insecure skip verify on")
	}
	if compiled.Observability.TracingRetry == nil {
		t.Fatalf("expected tracing retry config")
	}
	if !compiled.Observability.TracingRetry.Enabled ||
		compiled.Observability.TracingRetry.InitialInterval != 1*time.Second ||
		compiled.Observability.TracingRetry.MaxInterval != 3*time.Second ||
		compiled.Observability.TracingRetry.MaxElapsedTime != 30*time.Second {
		t.Fatalf("unexpected tracing retry config: %#v", compiled.Observability.TracingRetry)
	}
	if len(compiled.Observability.TracingHeaders) != 1 {
		t.Fatalf("expected tracing headers, got %#v", compiled.Observability.TracingHeaders)
	}
	if compiled.Observability.TracingHeaders[0].Name != "Authorization" || compiled.Observability.TracingHeaders[0].Value != "Bearer test" {
		t.Fatalf("expected tracing Authorization header, got %#v", compiled.Observability.TracingHeaders[0])
	}
}

func TestCompile_ObservabilityLogSinks(t *testing.T) {
	in := []byte(`
observability {
  access_log {
    enabled off
    output stdout
    format json
  }
  runtime_log {
    level warn
    output file
    path "logs/runtime.log"
    format json
  }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Observability.AccessLogEnabled {
		t.Fatalf("expected access log disabled")
	}
	if compiled.Observability.AccessLogOutput != "stdout" {
		t.Fatalf("expected access log stdout output, got %q", compiled.Observability.AccessLogOutput)
	}
	if compiled.Observability.RuntimeLogLevel != "warn" {
		t.Fatalf("expected runtime level warn, got %q", compiled.Observability.RuntimeLogLevel)
	}
	if compiled.Observability.RuntimeLogOutput != "file" || compiled.Observability.RuntimeLogPath != "logs/runtime.log" {
		t.Fatalf("expected runtime file sink, got output=%q path=%q", compiled.Observability.RuntimeLogOutput, compiled.Observability.RuntimeLogPath)
	}
}

func TestCompile_ObservabilityLogSinkValidation(t *testing.T) {
	in := []byte(`
observability {
  access_log {
    output nowhere
    path "/tmp/a.log"
    format text
  }
  runtime_log {
    output file
  }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	foundOutput := false
	foundPath := false
	foundFormat := false
	for _, e := range res.Errors {
		if strings.Contains(e, "observability.access_log.output") {
			foundOutput = true
		}
		if strings.Contains(e, "observability.runtime_log.path") {
			foundPath = true
		}
		if strings.Contains(e, "observability.access_log.format") {
			foundFormat = true
		}
	}
	if !foundOutput || !foundPath || !foundFormat {
		t.Fatalf("expected log sink validation errors, got %#v", res.Errors)
	}
}

func TestCompile_ObservabilityTracingExporterValidation(t *testing.T) {
	in := []byte(`
		observability {
		  tracing {
		    enabled on
		    collector "not-a-url"
		    url_path "bad"
		    timeout nope
		    compression brotli
		    insecure maybe
		    proxy_url "not-a-url"
		    tls {
		      ca_file ""
		      cert_file "certs/client.pem"
		      server_name ""
		      insecure_skip_verify maybe
		    }
		    retry {
		      enabled maybe
		      initial_interval off
		      max_interval 1s
		      max_elapsed_time -1s
		    }
		    header "X-Token" "a"
		    header "x-token" "b"
		  }
		}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	foundEndpoint := false
	foundHeaders := false
	foundURLPath := false
	foundTimeout := false
	foundCompression := false
	foundInsecure := false
	foundProxy := false
	foundTLS := false
	foundRetry := false
	for _, e := range res.Errors {
		if strings.Contains(e, "observability.tracing.collector") {
			foundEndpoint = true
		}
		if strings.Contains(e, "observability.tracing.url_path") {
			foundURLPath = true
		}
		if strings.Contains(e, "observability.tracing.timeout") {
			foundTimeout = true
		}
		if strings.Contains(e, "observability.tracing.compression") {
			foundCompression = true
		}
		if strings.Contains(e, "observability.tracing.insecure") {
			foundInsecure = true
		}
		if strings.Contains(e, "observability.tracing.proxy_url") {
			foundProxy = true
		}
		if strings.Contains(e, "observability.tracing.tls.") {
			foundTLS = true
		}
		if strings.Contains(e, "observability.tracing.retry.") {
			foundRetry = true
		}
		if strings.Contains(e, "observability.tracing.header duplicate") {
			foundHeaders = true
		}
	}
	if !foundEndpoint || !foundURLPath || !foundTimeout || !foundCompression || !foundInsecure || !foundProxy || !foundTLS || !foundRetry || !foundHeaders {
		t.Fatalf("expected tracing endpoint/header errors, got %#v", res.Errors)
	}
}

func TestCompile_ObservabilityTracingHeaderValidation(t *testing.T) {
	in := []byte(`
observability {
  tracing {
    enabled on
    collector "https://otel.example.com/v1/traces"
    header "Bad Header" "token"
    header "X-Token" "bad\nvalue"
  }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	foundName := false
	foundValue := false
	for _, e := range res.Errors {
		if strings.Contains(e, "observability.tracing.header[0]") && strings.Contains(e, "invalid field name") {
			foundName = true
		}
		if strings.Contains(e, "observability.tracing.header[1]") && strings.Contains(e, "invalid field value") {
			foundValue = true
		}
	}
	if !foundName || !foundValue {
		t.Fatalf("expected tracing header validation errors, got %#v", res.Errors)
	}
}

func TestCompile_ObservabilityTracingTLSInsecureConflict(t *testing.T) {
	in := []byte(`
observability {
  tracing {
    enabled on
    collector "https://otel.example.com/v1/traces"
    insecure on
    tls {
      ca_file "certs/otel-ca.pem"
    }
  }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "cannot be combined with TLS transport options") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected TLS/insecure conflict error, got %#v", res.Errors)
	}
}

func TestCompile_ObservabilityValidation(t *testing.T) {
	in := []byte(`
observability {
  access_log nope
  runtime_log loud
  tracing { enabled maybe }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	foundAccess := false
	foundRuntime := false
	foundTracing := false
	for _, e := range res.Errors {
		if strings.Contains(e, "observability.access_log") {
			foundAccess = true
		}
		if strings.Contains(e, "observability.runtime_log") {
			foundRuntime = true
		}
		if strings.Contains(e, "observability.tracing.enabled") {
			foundTracing = true
		}
	}
	if !foundAccess || !foundRuntime || !foundTracing {
		t.Fatalf("expected access/runtime errors, got %#v", res.Errors)
	}
}

func TestCompile_QueueLimitsDefaults(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.QueueLimits.MaxDepth != defaultQueueMaxDepth {
		t.Fatalf("max_depth: got %d", compiled.QueueLimits.MaxDepth)
	}
	if compiled.QueueLimits.DropPolicy != defaultQueueDropPolicy {
		t.Fatalf("drop_policy: got %q", compiled.QueueLimits.DropPolicy)
	}
}

func TestCompile_QueueRetentionDefaults(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.QueueRetention.MaxAge != defaultQueueRetentionMaxAge {
		t.Fatalf("max_age: got %s", compiled.QueueRetention.MaxAge)
	}
	if compiled.QueueRetention.PruneInterval != defaultQueueRetentionPruneInterval {
		t.Fatalf("prune_interval: got %s", compiled.QueueRetention.PruneInterval)
	}
	if !compiled.QueueRetention.Enabled {
		t.Fatalf("expected retention enabled")
	}
}

func TestCompile_DeliveredRetentionDefaults(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.DeliveredRetention.MaxAge != defaultDeliveredRetentionMaxAge {
		t.Fatalf("max_age: got %s", compiled.DeliveredRetention.MaxAge)
	}
	if compiled.DeliveredRetention.Enabled {
		t.Fatalf("expected delivered retention disabled by default")
	}
}

func TestCompile_Matchers(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  match {
    method post
    host "EXAMPLE.com:443"
    header "x-event" "push"
    header_exists "x-signature"
    query "ref" "main"
    remote_ip "203.0.113.0/24"
    remote_ip "2001:db8::1"
    query_exists "delivery_id"
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(compiled.Routes))
	}
	match := compiled.Routes[0].Match
	if len(match.Methods) != 1 || match.Methods[0] != "POST" {
		t.Fatalf("methods: %#v", match.Methods)
	}
	if len(match.Hosts) != 1 || match.Hosts[0] != "example.com" {
		t.Fatalf("hosts: %#v", match.Hosts)
	}
	if len(match.Headers) != 1 || match.Headers[0].Name != "X-Event" || match.Headers[0].Value != "push" {
		t.Fatalf("headers: %#v", match.Headers)
	}
	if len(match.HeaderExists) != 1 || match.HeaderExists[0] != "X-Signature" {
		t.Fatalf("header_exists: %#v", match.HeaderExists)
	}
	if len(match.Query) != 1 || match.Query[0].Name != "ref" || match.Query[0].Value != "main" {
		t.Fatalf("query: %#v", match.Query)
	}
	if len(match.RemoteIPs) != 2 || match.RemoteIPs[0].String() != "203.0.113.0/24" || match.RemoteIPs[1].String() != "2001:db8::1/128" {
		t.Fatalf("remote_ip: %#v", match.RemoteIPs)
	}
	if len(match.QueryExists) != 1 || match.QueryExists[0] != "delivery_id" {
		t.Fatalf("query_exists: %#v", match.QueryExists)
	}
}

func TestCompile_MatchersMultiValueDirectives(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  match {
    method post PUT
    host "EXAMPLE.com:443" "*.Example.com"
    header "x-event" "push" x-tenant acme
    header_exists "x-signature" X-Trace-Id
    query "ref" "main" env prod
    remote_ip "203.0.113.0/24" "2001:db8::1"
    query_exists "delivery_id" trace_id
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(compiled.Routes))
	}
	match := compiled.Routes[0].Match
	if len(match.Methods) != 2 || match.Methods[0] != "POST" || match.Methods[1] != "PUT" {
		t.Fatalf("methods: %#v", match.Methods)
	}
	if len(match.Hosts) != 2 || match.Hosts[0] != "example.com" || match.Hosts[1] != "*.example.com" {
		t.Fatalf("hosts: %#v", match.Hosts)
	}
	if len(match.Headers) != 2 || match.Headers[0].Name != "X-Event" || match.Headers[0].Value != "push" || match.Headers[1].Name != "X-Tenant" || match.Headers[1].Value != "acme" {
		t.Fatalf("headers: %#v", match.Headers)
	}
	if len(match.HeaderExists) != 2 || match.HeaderExists[0] != "X-Signature" || match.HeaderExists[1] != "X-Trace-Id" {
		t.Fatalf("header_exists: %#v", match.HeaderExists)
	}
	if len(match.Query) != 2 || match.Query[0].Name != "ref" || match.Query[0].Value != "main" || match.Query[1].Name != "env" || match.Query[1].Value != "prod" {
		t.Fatalf("query: %#v", match.Query)
	}
	if len(match.RemoteIPs) != 2 || match.RemoteIPs[0].String() != "203.0.113.0/24" || match.RemoteIPs[1].String() != "2001:db8::1/128" {
		t.Fatalf("remote_ip: %#v", match.RemoteIPs)
	}
	if len(match.QueryExists) != 2 || match.QueryExists[0] != "delivery_id" || match.QueryExists[1] != "trace_id" {
		t.Fatalf("query_exists: %#v", match.QueryExists)
	}
}

func TestCompile_MatchersHeaderExistsInvalid(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  match {
    header_exists "bad header"
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "match header_exists") && strings.Contains(e, "valid token") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected header_exists validation error, got %#v", res.Errors)
	}
}

func TestCompile_MatchersHostWildcard(t *testing.T) {
	in := []byte(`
"/x" {
  match {
    host "*.Example.com"
  }
  deliver "https://example.org" {}
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	match := compiled.Routes[0].Match
	if len(match.Hosts) != 1 || match.Hosts[0] != "*.example.com" {
		t.Fatalf("hosts: %#v", match.Hosts)
	}
}

func TestCompile_MatchersHostWildcardInvalid(t *testing.T) {
	in := []byte(`
"/x" {
  match {
    host "api.*.example.com"
  }
  deliver "https://example.org" {}
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "wildcard host must be") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected wildcard host error, got %#v", res.Errors)
	}
}

func TestCompile_MatchersRemoteIPInvalid(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  match {
    remote_ip "not-an-ip"
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "match remote_ip") && strings.Contains(e, "must be an IP or CIDR") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected remote_ip validation error, got %#v", res.Errors)
	}
}

func TestCompile_RouteManagementLabels(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(compiled.Routes))
	}
	if compiled.Routes[0].Application != "billing" {
		t.Fatalf("application: got %q", compiled.Routes[0].Application)
	}
	if compiled.Routes[0].EndpointName != "invoice.created" {
		t.Fatalf("endpoint_name: got %q", compiled.Routes[0].EndpointName)
	}
}

func TestCompile_RouteManagementLabelsRequirePair(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  application "billing"
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "application and endpoint_name must be set together") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected pair error, got %#v", res.Errors)
	}
}

func TestCompile_RouteManagementLabelsUnique(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/a" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/ea" }
}
"/b" {
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/eb" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "duplicate management endpoint") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected duplicate management endpoint error, got %#v", res.Errors)
	}
}

func TestCompile_RouteManagementLabelsPattern(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/bad-app" {
  application "billing team"
  endpoint_name "invoice.created"
  pull { path "/ea" }
}
"/bad-endpoint" {
  application "billing"
  endpoint_name "invoice/created"
  pull { path "/eb" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	foundApp := false
	foundEndpoint := false
	for _, e := range res.Errors {
		if strings.Contains(e, `route "/bad-app" application must match`) {
			foundApp = true
		}
		if strings.Contains(e, `route "/bad-endpoint" endpoint_name must match`) {
			foundEndpoint = true
		}
	}
	if !foundApp {
		t.Fatalf("expected application pattern error, got %#v", res.Errors)
	}
	if !foundEndpoint {
		t.Fatalf("expected endpoint_name pattern error, got %#v", res.Errors)
	}
}

func TestCompile_RouteManagementLabelsPatternAllowsCommonEventNames(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  application "billing-team"
  endpoint_name "invoice.created:v1"
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(compiled.Routes))
	}
	if compiled.Routes[0].Application != "billing-team" {
		t.Fatalf("application: got %q", compiled.Routes[0].Application)
	}
	if compiled.Routes[0].EndpointName != "invoice.created:v1" {
		t.Fatalf("endpoint_name: got %q", compiled.Routes[0].EndpointName)
	}
}

func TestCompile_NamedMatcher(t *testing.T) {
	in := []byte(`
@webhook {
  host "hooks.example.com"
  method post
  remote_ip "203.0.113.0/24"
  header_exists "X-Signature"
  query_exists "delivery_id"
}

pull_api { auth token "raw:t" }

"/x" {
  match @webhook
  match { header "X-Event" "push" }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	match := compiled.Routes[0].Match
	if len(match.Methods) != 1 || match.Methods[0] != "POST" {
		t.Fatalf("methods: %#v", match.Methods)
	}
	if len(match.Hosts) != 1 || match.Hosts[0] != "hooks.example.com" {
		t.Fatalf("hosts: %#v", match.Hosts)
	}
	if len(match.Headers) != 1 || match.Headers[0].Name != "X-Event" {
		t.Fatalf("headers: %#v", match.Headers)
	}
	if len(match.RemoteIPs) != 1 || match.RemoteIPs[0].String() != "203.0.113.0/24" {
		t.Fatalf("remote_ip: %#v", match.RemoteIPs)
	}
	if len(match.HeaderExists) != 1 || match.HeaderExists[0] != "X-Signature" {
		t.Fatalf("header_exists: %#v", match.HeaderExists)
	}
	if len(match.QueryExists) != 1 || match.QueryExists[0] != "delivery_id" {
		t.Fatalf("query_exists: %#v", match.QueryExists)
	}
}

func TestCompile_NamedMatcherMultiRefsSingleDirective(t *testing.T) {
	in := []byte(`
@hosted {
  host "hooks.example.com"
  method post
}
@signed {
  header_exists "X-Signature"
  query_exists "delivery_id"
}

pull_api { auth token "raw:t" }

"/x" {
  match @hosted @signed
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	match := compiled.Routes[0].Match
	if len(match.Methods) != 1 || match.Methods[0] != "POST" {
		t.Fatalf("methods: %#v", match.Methods)
	}
	if len(match.Hosts) != 1 || match.Hosts[0] != "hooks.example.com" {
		t.Fatalf("hosts: %#v", match.Hosts)
	}
	if len(match.HeaderExists) != 1 || match.HeaderExists[0] != "X-Signature" {
		t.Fatalf("header_exists: %#v", match.HeaderExists)
	}
	if len(match.QueryExists) != 1 || match.QueryExists[0] != "delivery_id" {
		t.Fatalf("query_exists: %#v", match.QueryExists)
	}
}

func TestCompile_AuthBasicConflict(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth basic "ci" "s3cret"
  auth hmac "raw:secret"
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "auth basic cannot be combined with auth hmac") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected auth conflict error, got %#v", res.Errors)
	}
}

func TestCompile_AuthBasicConflictWithHMACSecretRef(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "raw:s1"
    valid_from "2025-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/x" {
  auth basic "ci" "s3cret"
  auth hmac secret_ref "S1"
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "auth basic cannot be combined with auth hmac") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected auth conflict error, got %#v", res.Errors)
	}
}

func TestCompile_AuthForwardConflictWithBasic(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth basic "ci" "s3cret"
  auth forward "https://auth.example.test/check"
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "auth forward cannot be combined with auth basic or auth hmac") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected auth conflict error, got %#v", res.Errors)
	}
}

func TestCompile_AuthForwardConflictWithHMAC(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth hmac "raw:secret"
  auth forward "https://auth.example.test/check"
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "auth forward cannot be combined with auth basic or auth hmac") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected auth conflict error, got %#v", res.Errors)
	}
}

func TestCompile_QueueBackend(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue { backend sqlite }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Routes[0].QueueBackend != "sqlite" {
		t.Fatalf("queue backend: got %q", compiled.Routes[0].QueueBackend)
	}
}

func TestCompile_QueueBackendMemory(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue { backend "memory" }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Routes[0].QueueBackend != "memory" {
		t.Fatalf("queue backend: got %q", compiled.Routes[0].QueueBackend)
	}
}

func TestCompile_QueueBackendPostgres(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue { backend "postgres" }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Routes[0].QueueBackend != "postgres" {
		t.Fatalf("queue backend: got %q", compiled.Routes[0].QueueBackend)
	}
}

func TestCompile_QueueBackendShorthandMemory(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue memory
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Routes[0].QueueBackend != "memory" {
		t.Fatalf("queue backend: got %q", compiled.Routes[0].QueueBackend)
	}
}

func TestCompile_QueueBackendShorthandPostgres(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue postgres
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Routes[0].QueueBackend != "postgres" {
		t.Fatalf("queue backend: got %q", compiled.Routes[0].QueueBackend)
	}
}

func TestCompile_QueueBackendMixedUnsupported(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue { backend "memory" }
  pull { path "/e1" }
}
"/y" {
  queue { backend "sqlite" }
  pull { path "/e2" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "mixed route queue backends") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected mixed backend error, got %#v", res.Errors)
	}
}

func TestCompile_QueueBackendInvalid(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue { backend "redis" }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "queue.backend must be one of") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected queue.backend error, got %#v", res.Errors)
	}
}

func TestCompile_QueueBackendShorthandInvalid(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue redis
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "queue.backend must be one of") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected queue.backend error, got %#v", res.Errors)
	}
}

func TestCompile_DuplicateRoutePath(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" { pull { path "/e1" } }
"/x" { pull { path "/e2" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "duplicate route path") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected duplicate route error, got %#v", res.Errors)
	}
}

func TestCompile_DeliverRoute(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {}
  deliver "https://audit.internal/ingest" {}
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(compiled.Routes))
	}
	if len(compiled.Routes[0].Deliveries) != 2 {
		t.Fatalf("expected 2 deliver targets, got %d", len(compiled.Routes[0].Deliveries))
	}
	if compiled.Routes[0].Deliveries[0].URL != "https://ci.internal/build" {
		t.Fatalf("deliver[0]: got %q", compiled.Routes[0].Deliveries[0].URL)
	}
	if compiled.Routes[0].Deliveries[1].URL != "https://audit.internal/ingest" {
		t.Fatalf("deliver[1]: got %q", compiled.Routes[0].Deliveries[1].URL)
	}
	if compiled.Routes[0].Deliveries[0].Timeout != defaultDeliverTimeout {
		t.Fatalf("deliver[0] timeout: got %s", compiled.Routes[0].Deliveries[0].Timeout)
	}
	if compiled.Routes[0].Deliveries[0].Retry.Max != defaultDeliverRetryMax {
		t.Fatalf("deliver[0] retry max: got %d", compiled.Routes[0].Deliveries[0].Retry.Max)
	}
}

func TestCompile_DeliverURLValidation(t *testing.T) {
	cases := []struct {
		name string
		url  string
		err  string
	}{
		{"ftp scheme", `ftp://evil.example`, "must use http or https"},
		{"no scheme", `not-a-url`, "must use http or https"},
		{"missing host", `https://`, "must include host"},
		{"empty", `""`, "must not be empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(fmt.Sprintf(`
pull_api { auth token "raw:t" }
"/x" { deliver %s {} }
`, tc.url))
			cfg, err := Parse(in)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			_, res := Compile(cfg)
			if res.OK {
				t.Fatalf("expected compile error for deliver URL %s", tc.url)
			}
			found := false
			for _, e := range res.Errors {
				if strings.Contains(e, tc.err) {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected error containing %q, got %v", tc.err, res.Errors)
			}
		})
	}
}

func TestCompile_DeliverConcurrencyUpperBound(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }
"/x" {
  deliver_concurrency 99999
  deliver "https://example.com" {}
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected compile error for concurrency > 10000")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "must not exceed 10000") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected upper bound error, got %v", res.Errors)
	}
}

func TestCompile_DeliverSigningHMAC(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac "raw:deliver-secret"
    sign signature_header "X-Signature"
    sign timestamp_header "X-Timestamp"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 || len(compiled.Routes[0].Deliveries) != 1 {
		t.Fatalf("expected one delivery target, got %#v", compiled.Routes)
	}
	sign := compiled.Routes[0].Deliveries[0].SigningHMAC
	if !sign.Enabled {
		t.Fatalf("expected signing enabled")
	}
	if sign.SecretRef != "raw:deliver-secret" {
		t.Fatalf("sign secret: got %q", sign.SecretRef)
	}
	if sign.SignatureHeader != "X-Signature" {
		t.Fatalf("sign signature_header: got %q", sign.SignatureHeader)
	}
	if sign.TimestampHeader != "X-Timestamp" {
		t.Fatalf("sign timestamp_header: got %q", sign.TimestampHeader)
	}
	if sign.SecretSelection != defaultDeliverSignHMACSecretSelection {
		t.Fatalf("sign secret_selection: got %q", sign.SecretSelection)
	}
}

func TestCompile_DeliverSigningHMACDefaults(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac "raw:deliver-secret"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	sign := compiled.Routes[0].Deliveries[0].SigningHMAC
	if !sign.Enabled {
		t.Fatalf("expected signing enabled")
	}
	if sign.SignatureHeader != defaultDeliverSignHMACSignatureHeader {
		t.Fatalf("sign signature_header: got %q", sign.SignatureHeader)
	}
	if sign.TimestampHeader != defaultDeliverSignHMACTimestampHeader {
		t.Fatalf("sign timestamp_header: got %q", sign.TimestampHeader)
	}
	if sign.SecretSelection != defaultDeliverSignHMACSecretSelection {
		t.Fatalf("sign secret_selection: got %q", sign.SecretSelection)
	}
}

func TestCompile_DeliverSigningHMACSecretRef(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "raw:deliver-secret"
    valid_from "2026-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac secret_ref "S1"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	sign := compiled.Routes[0].Deliveries[0].SigningHMAC
	if !sign.Enabled {
		t.Fatalf("expected signing enabled")
	}
	if sign.SecretRef != "" {
		t.Fatalf("sign direct secret: got %q", sign.SecretRef)
	}
	if len(sign.SecretVersions) != 1 {
		t.Fatalf("expected one sign secret version, got %#v", sign.SecretVersions)
	}
	if sign.SecretVersions[0].ID != "S1" {
		t.Fatalf("sign secret version id: got %q", sign.SecretVersions[0].ID)
	}
	if sign.SecretVersions[0].ValueRef != "raw:deliver-secret" {
		t.Fatalf("sign secret version value_ref: got %q", sign.SecretVersions[0].ValueRef)
	}
}

func TestCompile_DeliverSigningHMACSecretRefMultiple(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "raw:deliver-secret-old"
    valid_from "2026-01-01T00:00:00Z"
    valid_until "2027-01-01T00:00:00Z"
  }
  secret "S2" {
    value "raw:deliver-secret-new"
    valid_from "2027-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac secret_ref "S1"
    sign hmac secret_ref "S2"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	sign := compiled.Routes[0].Deliveries[0].SigningHMAC
	if len(sign.SecretVersions) != 2 {
		t.Fatalf("expected two sign secret versions, got %#v", sign.SecretVersions)
	}
	if sign.SecretSelection != defaultDeliverSignHMACSecretSelection {
		t.Fatalf("sign secret_selection: got %q", sign.SecretSelection)
	}
	if sign.SecretVersions[0].ID != "S1" || sign.SecretVersions[1].ID != "S2" {
		t.Fatalf("unexpected sign secret version ids: %#v", sign.SecretVersions)
	}
}

func TestCompile_DeliverSigningHMACSecretRefMultipleMultiValueDirective(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "raw:deliver-secret-old"
    valid_from "2026-01-01T00:00:00Z"
    valid_until "2027-01-01T00:00:00Z"
  }
  secret "S2" {
    value "raw:deliver-secret-new"
    valid_from "2027-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac secret_ref "S1" "S2"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	sign := compiled.Routes[0].Deliveries[0].SigningHMAC
	if len(sign.SecretVersions) != 2 {
		t.Fatalf("expected two sign secret versions, got %#v", sign.SecretVersions)
	}
	if sign.SecretVersions[0].ID != "S1" || sign.SecretVersions[1].ID != "S2" {
		t.Fatalf("unexpected sign secret version ids: %#v", sign.SecretVersions)
	}
}

func TestCompile_DeliverSigningHMACValidation(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign signature_header "X-Signature"
    sign timestamp_header "bad header"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected compile error, got ok")
	}
	foundRequiresSecret := false
	foundHeaderToken := false
	for _, e := range res.Errors {
		if strings.Contains(e, "sign options require sign hmac") {
			foundRequiresSecret = true
		}
		if strings.Contains(e, "timestamp_header") && strings.Contains(e, "valid header token") {
			foundHeaderToken = true
		}
	}
	if !foundRequiresSecret {
		t.Fatalf("expected sign hmac required error, got %#v", res.Errors)
	}
	if !foundHeaderToken {
		t.Fatalf("expected sign timestamp_header validation error, got %#v", res.Errors)
	}
}

func TestCompile_DeliverSigningHMACSecretSelectionValidation(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac "raw:deliver-secret"
    sign secret_selection "latest"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected compile error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "sign secret_selection") && strings.Contains(e, "newest_valid or oldest_valid") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected sign secret_selection validation error, got %#v", res.Errors)
	}
}

func TestCompile_DeliverSigningHMACSecretSelectionRequiresHMAC(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign secret_selection "oldest_valid"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected compile error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "sign options require sign hmac") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected sign hmac required error, got %#v", res.Errors)
	}
}

func TestCompile_DeliverSigningHMACSecretSelectionRequiresSecretRef(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac "raw:deliver-secret"
    sign secret_selection "oldest_valid"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected compile error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "sign secret_selection requires sign hmac secret_ref") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected sign secret_selection requires secret_ref error, got %#v", res.Errors)
	}
}

func TestCompile_DeliverSigningHMACSecretRefValidation(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac secret_ref "MISSING"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected compile error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, `sign hmac secret_ref "MISSING" not found`) || strings.Contains(e, `sign hmac secret_ref[0]`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected missing secret_ref error, got %#v", res.Errors)
	}
}

func TestCompile_DeliverSigningHMACSecretRefDuplicateValidation(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "raw:deliver-secret"
    valid_from "2026-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac secret_ref "S1"
    sign hmac secret_ref "S1"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected compile error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, `sign hmac secret_ref duplicate "S1"`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected duplicate secret_ref error, got %#v", res.Errors)
	}
}

func TestCompile_DeliverRouteWithoutPullAPI(t *testing.T) {
	in := []byte(`
"/x" {
  deliver "https://ci.internal/build" {}
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
}

func TestCompile_PullRouteRequiresPullAPI(t *testing.T) {
	in := []byte(`
"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "pull_api block is required") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected pull_api required error, got %#v", res.Errors)
	}
}

func TestCompile_PullRouteAuthTokensAllowMissingGlobal(t *testing.T) {
	in := []byte(`
pull_api {}

"/x" {
  pull {
    path "/e"
    auth token "raw:p1"
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 || compiled.Routes[0].Pull == nil || len(compiled.Routes[0].Pull.AuthTokens) != 1 {
		t.Fatalf("expected pull auth tokens, got %#v", compiled.Routes)
	}
}

func TestCompile_PullRouteMissingAuthRequiresGlobal(t *testing.T) {
	in := []byte(`
pull_api {}

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "pull_api requires auth token allowlist") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected auth allowlist error, got %#v", res.Errors)
	}
}

func TestCompile_PullAPIGrpcListenRequiresPullRoutes(t *testing.T) {
	in := []byte(`
pull_api {
  grpc_listen ":9943"
  auth token "raw:t"
}

"/x" {
  deliver "https://ci.internal/build" {}
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "pull_api.grpc_listen requires at least one pull route") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected pull_api.grpc_listen pull-route requirement error, got %#v", res.Errors)
	}
}

func TestCompile_PullAPILimits(t *testing.T) {
	in := []byte(`
pull_api {
  auth token "raw:t"
  grpc_listen ":9943"
  max_batch 25
  default_lease_ttl 45s
  max_lease_ttl 60s
  default_max_wait 8s
  max_wait 20s
}

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.PullAPI.MaxBatch != 25 {
		t.Fatalf("pull max_batch: got %d", compiled.PullAPI.MaxBatch)
	}
	if compiled.PullAPI.GRPCListen != ":9943" {
		t.Fatalf("pull grpc_listen: got %q", compiled.PullAPI.GRPCListen)
	}
	if compiled.PullAPI.DefaultLeaseTTL != 45*time.Second {
		t.Fatalf("pull default_lease_ttl: got %s", compiled.PullAPI.DefaultLeaseTTL)
	}
	if compiled.PullAPI.MaxLeaseTTL != 60*time.Second {
		t.Fatalf("pull max_lease_ttl: got %s", compiled.PullAPI.MaxLeaseTTL)
	}
	if compiled.PullAPI.DefaultMaxWait != 8*time.Second {
		t.Fatalf("pull default_max_wait: got %s", compiled.PullAPI.DefaultMaxWait)
	}
	if compiled.PullAPI.MaxWait != 20*time.Second {
		t.Fatalf("pull max_wait: got %s", compiled.PullAPI.MaxWait)
	}
}

func TestCompile_PullAPILimitsValidation(t *testing.T) {
	in := []byte(`
pull_api {
  auth token "raw:t"
  max_batch 0
  default_lease_ttl off
  max_lease_ttl nope
  default_max_wait nope
  max_wait nope
}

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	foundBatch := false
	foundLease := false
	foundMaxLease := false
	foundDefaultWait := false
	foundWait := false
	for _, e := range res.Errors {
		if strings.Contains(e, "pull_api.max_batch") {
			foundBatch = true
		}
		if strings.Contains(e, "pull_api.default_lease_ttl") {
			foundLease = true
		}
		if strings.Contains(e, "pull_api.max_lease_ttl") {
			foundMaxLease = true
		}
		if strings.Contains(e, "pull_api.default_max_wait") {
			foundDefaultWait = true
		}
		if strings.Contains(e, "pull_api.max_wait") {
			foundWait = true
		}
	}
	if !foundBatch || !foundLease || !foundMaxLease || !foundDefaultWait || !foundWait {
		t.Fatalf("expected pull_api limit errors, got %#v", res.Errors)
	}
}

func TestCompile_PullAPIDefaultLeaseTTLMustNotExceedMaxLeaseTTL(t *testing.T) {
	in := []byte(`
pull_api {
  auth token "raw:t"
  default_lease_ttl 90s
  max_lease_ttl 60s
}

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "default_lease_ttl must not exceed") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected max_lease_ttl relation error, got %#v", res.Errors)
	}
}

func TestCompile_PullAPIDefaultMaxWaitMustNotExceedMaxWait(t *testing.T) {
	in := []byte(`
pull_api {
  auth token "raw:t"
  default_max_wait 30s
  max_wait 10s
}

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "default_max_wait must not exceed") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected default_max_wait relation error, got %#v", res.Errors)
	}
}

func TestParse_AdminAPIRejectsPullLimits(t *testing.T) {
	in := []byte(`
admin_api {
  max_batch 10
}

"/x" { deliver "https://example.org" {} }
`)
	_, err := Parse(in)
	if err == nil {
		t.Fatalf("expected parse error")
	}
	if !strings.Contains(err.Error(), `unknown admin_api directive "max_batch"`) {
		t.Fatalf("unexpected parse error: %v", err)
	}
}

func TestParse_IngressDuplicateListenDirective(t *testing.T) {
	in := []byte(`
ingress {
  listen ":8443"
  listen ":9443"
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	_, err := Parse(in)
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate ingress listen") {
		t.Fatalf("expected duplicate ingress listen error, got %v", err)
	}
}

func TestParse_PullAPIDuplicateDirective(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
		want   string
	}{
		{name: "Listen", first: `listen ":9443"`, second: `listen ":9555"`, want: "duplicate pull_api listen"},
		{name: "Prefix", first: `prefix "/pull"`, second: `prefix "/v2/pull"`, want: "duplicate pull_api prefix"},
		{name: "MaxBatch", first: "max_batch 10", second: "max_batch 20", want: "duplicate pull_api max_batch"},
		{name: "GRPCListen", first: `grpc_listen ":9943"`, second: `grpc_listen ":9955"`, want: "duplicate pull_api grpc_listen"},
		{name: "DefaultLeaseTTL", first: "default_lease_ttl 30s", second: "default_lease_ttl 60s", want: "duplicate pull_api default_lease_ttl"},
		{name: "MaxLeaseTTL", first: "max_lease_ttl 90s", second: "max_lease_ttl 120s", want: "duplicate pull_api max_lease_ttl"},
		{name: "DefaultMaxWait", first: "default_max_wait 5s", second: "default_max_wait 8s", want: "duplicate pull_api default_max_wait"},
		{name: "MaxWait", first: "max_wait 20s", second: "max_wait 30s", want: "duplicate pull_api max_wait"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(fmt.Sprintf(`
pull_api {
  auth token "raw:t"
  %s
  %s
}

"/x" { pull { path "/e" } }
`, tc.first, tc.second))
			_, err := Parse(in)
			if err == nil {
				t.Fatalf("expected parse error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestParse_AdminAPIDuplicateDirective(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
		want   string
	}{
		{name: "Listen", first: `listen "127.0.0.1:2019"`, second: `listen "127.0.0.1:2020"`, want: "duplicate admin_api listen"},
		{name: "Prefix", first: `prefix "/admin"`, second: `prefix "/ops"`, want: "duplicate admin_api prefix"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(fmt.Sprintf(`
admin_api {
  %s
  %s
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`, tc.first, tc.second))
			_, err := Parse(in)
			if err == nil {
				t.Fatalf("expected parse error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestParse_QueueLimitsDuplicateDirective(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
		want   string
	}{
		{name: "MaxDepth", first: "max_depth 100", second: "max_depth 200", want: "duplicate queue_limits max_depth"},
		{name: "DropPolicy", first: `drop_policy "reject"`, second: `drop_policy "drop_oldest"`, want: "duplicate queue_limits drop_policy"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(fmt.Sprintf(`
queue_limits {
  %s
  %s
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`, tc.first, tc.second))
			_, err := Parse(in)
			if err == nil {
				t.Fatalf("expected parse error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestParse_QueueRetentionDuplicateDirective(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
		want   string
	}{
		{name: "MaxAge", first: `max_age "7d"`, second: `max_age "14d"`, want: "duplicate queue_retention max_age"},
		{name: "PruneInterval", first: "prune_interval 5m", second: "prune_interval 10m", want: "duplicate queue_retention prune_interval"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(fmt.Sprintf(`
queue_retention {
  %s
  %s
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`, tc.first, tc.second))
			_, err := Parse(in)
			if err == nil {
				t.Fatalf("expected parse error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestParse_DeliveredRetentionDuplicateDirective(t *testing.T) {
	in := []byte(`
delivered_retention {
  max_age "3d"
  max_age "7d"
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	_, err := Parse(in)
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate delivered_retention max_age") {
		t.Fatalf("expected duplicate delivered_retention max_age error, got %v", err)
	}
}

func TestParse_DLQRetentionDuplicateDirective(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
		want   string
	}{
		{name: "MaxAge", first: `max_age "30d"`, second: `max_age "60d"`, want: "duplicate dlq_retention max_age"},
		{name: "MaxDepth", first: "max_depth 1000", second: "max_depth 2000", want: "duplicate dlq_retention max_depth"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(fmt.Sprintf(`
dlq_retention {
  %s
  %s
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`, tc.first, tc.second))
			_, err := Parse(in)
			if err == nil {
				t.Fatalf("expected parse error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestParse_PullDuplicateEndpointDirective(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  pull {
    path "/e1"
    path "/e2"
  }
}
`)
	_, err := Parse(in)
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate pull path") {
		t.Fatalf("expected duplicate pull path error, got %v", err)
	}
}

func TestParse_QueueDuplicateBackendDirective(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue {
    backend sqlite
    backend memory
  }
  pull { path "/e" }
}
`)
	_, err := Parse(in)
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate queue backend") {
		t.Fatalf("expected duplicate queue backend error, got %v", err)
	}
}

func TestParse_QueueDuplicateShorthandAndBlock(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  queue memory
  queue { backend sqlite }
  pull { path "/e" }
}
`)
	_, err := Parse(in)
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate queue block") {
		t.Fatalf("expected duplicate queue block error, got %v", err)
	}
}

func TestParse_DeliverSignDuplicateDirective(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
		want   string
	}{
		{name: "HMAC", first: `sign hmac "raw:s1"`, second: `sign hmac "raw:s2"`, want: "duplicate deliver sign hmac"},
		{name: "HMACMixedRef", first: `sign hmac "raw:s1"`, second: `sign hmac secret_ref "S1"`, want: "duplicate deliver sign hmac"},
		{name: "SignatureHeader", first: `sign signature_header "X-Signature"`, second: `sign signature_header "X-Alt-Signature"`, want: "duplicate deliver sign signature_header"},
		{name: "TimestampHeader", first: `sign timestamp_header "X-Timestamp"`, second: `sign timestamp_header "X-Alt-Timestamp"`, want: "duplicate deliver sign timestamp_header"},
		{name: "SecretSelection", first: `sign secret_selection "newest_valid"`, second: `sign secret_selection "oldest_valid"`, want: "duplicate deliver sign secret_selection"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(fmt.Sprintf(`
pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    %s
    %s
  }
}
`, tc.first, tc.second))
			_, err := Parse(in)
			if err == nil {
				t.Fatalf("expected parse error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestParse_DeliverSignSecretRefMultipleAllowed(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    sign hmac secret_ref "S1"
    sign hmac secret_ref "S2"
  }
}
`)
	if _, err := Parse(in); err != nil {
		t.Fatalf("expected parse success for multiple sign hmac secret_ref directives, got %v", err)
	}
}

func TestCompile_DeliverOverrides(t *testing.T) {
	in := []byte(`
defaults {
  deliver {
    retry exponential max 8 base 2s cap 2m jitter 0.2
    timeout 10s
  }
}

pull_api { auth token "raw:t" }

"/x" {
  deliver "https://ci.internal/build" {
    retry exponential max 5 base 1s cap 30s jitter 0.4
    timeout 5s
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 || len(compiled.Routes[0].Deliveries) != 1 {
		t.Fatalf("expected 1 deliver, got %#v", compiled.Routes)
	}
	d := compiled.Routes[0].Deliveries[0]
	if d.Timeout != 5*time.Second {
		t.Fatalf("timeout: got %s", d.Timeout)
	}
	if d.Retry.Max != 5 {
		t.Fatalf("retry max: got %d", d.Retry.Max)
	}
	if d.Retry.Base != 1*time.Second {
		t.Fatalf("retry base: got %s", d.Retry.Base)
	}
	if d.Retry.Cap != 30*time.Second {
		t.Fatalf("retry cap: got %s", d.Retry.Cap)
	}
	if d.Retry.Jitter != 0.4 {
		t.Fatalf("retry jitter: got %f", d.Retry.Jitter)
	}
}

func TestCompile_RouteDeliverConcurrencyOverride(t *testing.T) {
	in := []byte(`
defaults {
  deliver {
    concurrency 20
  }
}

pull_api { auth token "raw:t" }

"/x" {
  deliver_concurrency 3
  deliver "https://ci.internal/build" {}
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(compiled.Routes))
	}
	if compiled.Routes[0].DeliverConcurrency != 3 {
		t.Fatalf("deliver_concurrency: got %d", compiled.Routes[0].DeliverConcurrency)
	}
}

func TestCompile_RouteRequiresPullOrDeliver(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" { auth hmac "raw:s1" }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "missing pull or deliver") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected missing pull/deliver error, got %#v", res.Errors)
	}
}

func TestCompile_PullAndDeliverMutualExclusive(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  pull { path "/e" }
  deliver "https://ci.internal/build" {}
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "mutually exclusive") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected pull/deliver conflict, got %#v", res.Errors)
	}
}

func TestCompile_DefaultsLimits(t *testing.T) {
	in := []byte(`
defaults {
  max_body 1mb
  max_headers 8kb
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(compiled.Routes))
	}
	if compiled.Routes[0].MaxBodyBytes != 1*1024*1024 {
		t.Fatalf("max_body: got %d", compiled.Routes[0].MaxBodyBytes)
	}
	if compiled.Routes[0].MaxHeaderBytes != 8*1024 {
		t.Fatalf("max_headers: got %d", compiled.Routes[0].MaxHeaderBytes)
	}
}

func TestCompile_RouteLimitOverrides(t *testing.T) {
	in := []byte(`
defaults {
  max_body 2mb
  max_headers 64kb
}

pull_api { auth token "raw:t" }

"/x" {
  max_body 512kb
  max_headers 4kb
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(compiled.Routes))
	}
	if compiled.Routes[0].MaxBodyBytes != 512*1024 {
		t.Fatalf("route max_body: got %d", compiled.Routes[0].MaxBodyBytes)
	}
	if compiled.Routes[0].MaxHeaderBytes != 4*1024 {
		t.Fatalf("route max_headers: got %d", compiled.Routes[0].MaxHeaderBytes)
	}
}

func TestCompile_RouteRateLimitOverride(t *testing.T) {
	in := []byte(`
ingress {
  rate_limit { rps 100 burst 200 }
}

pull_api { auth token "raw:t" }

"/x" {
  rate_limit { rps 5.5 }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if !compiled.Ingress.RateLimit.Enabled || compiled.Ingress.RateLimit.RPS != 100 || compiled.Ingress.RateLimit.Burst != 200 {
		t.Fatalf("unexpected ingress rate limit: %#v", compiled.Ingress.RateLimit)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(compiled.Routes))
	}
	if !compiled.Routes[0].RateLimit.Enabled {
		t.Fatalf("expected route rate limit enabled")
	}
	if compiled.Routes[0].RateLimit.RPS != 5.5 {
		t.Fatalf("route rate_limit.rps: got %v", compiled.Routes[0].RateLimit.RPS)
	}
	if compiled.Routes[0].RateLimit.Burst != 6 {
		t.Fatalf("route rate_limit.burst (default ceil): got %d", compiled.Routes[0].RateLimit.Burst)
	}
}

func TestCompile_RouteRateLimitValidation(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  rate_limit { rps 0 burst 10 }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, `route "/x" rate_limit.rps must be a positive number`) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected route rate_limit validation error, got %#v", res.Errors)
	}
}

func TestCompile_RoutePublishDefaultsAndOverrides(t *testing.T) {
	defaultsIn := []byte(`
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(defaultsIn)
	if err != nil {
		t.Fatalf("parse defaults: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile defaults: %#v", res)
	}
	if !compiled.Routes[0].Publish {
		t.Fatalf("expected route publish default on")
	}
	if !compiled.Routes[0].PublishDirect {
		t.Fatalf("expected route publish.direct default on")
	}
	if !compiled.Routes[0].PublishManaged {
		t.Fatalf("expected route publish.managed default on")
	}

	overridesIn := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  publish {
    enabled 0
    direct false
    managed off
  }
  pull { path "/e" }
}
`)
	cfg, err = Parse(overridesIn)
	if err != nil {
		t.Fatalf("parse overrides: %v", err)
	}
	compiled, res = Compile(cfg)
	if !res.OK {
		t.Fatalf("compile overrides: %#v", res)
	}
	if compiled.Routes[0].Publish {
		t.Fatalf("expected route publish off")
	}
	if compiled.Routes[0].PublishDirect {
		t.Fatalf("expected route publish.direct off")
	}
	if compiled.Routes[0].PublishManaged {
		t.Fatalf("expected route publish.managed off")
	}
}

func TestCompile_RoutePublishValidation(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  publish {
    enabled maybe
    direct perhaps
    managed invalid
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	foundDirect := false
	foundManaged := false
	for _, e := range res.Errors {
		if strings.Contains(e, `route "/x" publish`) {
			found = true
		}
		if strings.Contains(e, `route "/x" publish.direct`) {
			foundDirect = true
		}
		if strings.Contains(e, `route "/x" publish.managed`) {
			foundManaged = true
		}
	}
	if !found || !foundDirect || !foundManaged {
		t.Fatalf("expected route publish validation error, got %#v", res.Errors)
	}
}

func TestCompile_DefaultsDeliverConcurrency(t *testing.T) {
	in := []byte(`
defaults {
  deliver {
    concurrency 42
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Defaults.DeliverConcurrency != 42 {
		t.Fatalf("deliver_concurrency: got %d", compiled.Defaults.DeliverConcurrency)
	}
}

func TestCompile_TrendSignalsDefaults(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	ts := compiled.Defaults.TrendSignals
	if ts.Window != defaultTrendSignalsWindow {
		t.Fatalf("window: got %s", ts.Window)
	}
	if ts.ExpectedCaptureInterval != defaultTrendSignalsCaptureInterval {
		t.Fatalf("expected_capture_interval: got %s", ts.ExpectedCaptureInterval)
	}
	if ts.StaleGraceFactor != defaultTrendSignalsStaleGrace {
		t.Fatalf("stale_grace_factor: got %d", ts.StaleGraceFactor)
	}
	if ts.RecentSurgePercent != defaultTrendSurgePercent {
		t.Fatalf("recent_surge_percent: got %d", ts.RecentSurgePercent)
	}
}

func TestCompile_TrendSignalsOverrides(t *testing.T) {
	in := []byte(`
defaults {
  trend_signals {
    window 30m
    expected_capture_interval 2m
    stale_grace_factor 4
    sustained_growth_consecutive 4
    sustained_growth_min_samples 6
    sustained_growth_min_delta 12
    recent_surge_min_total 25
    recent_surge_min_delta 15
    recent_surge_percent 60
    dead_share_high_min_total 20
    dead_share_high_percent 30
    queued_pressure_min_total 30
    queued_pressure_percent 80
    queued_pressure_leased_multiplier 3
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	ts := compiled.Defaults.TrendSignals
	if ts.Window != 30*time.Minute ||
		ts.ExpectedCaptureInterval != 2*time.Minute ||
		ts.StaleGraceFactor != 4 ||
		ts.SustainedGrowthConsecutive != 4 ||
		ts.SustainedGrowthMinSamples != 6 ||
		ts.SustainedGrowthMinDelta != 12 ||
		ts.RecentSurgeMinTotal != 25 ||
		ts.RecentSurgeMinDelta != 15 ||
		ts.RecentSurgePercent != 60 ||
		ts.DeadShareHighMinTotal != 20 ||
		ts.DeadShareHighPercent != 30 ||
		ts.QueuedPressureMinTotal != 30 ||
		ts.QueuedPressurePercent != 80 ||
		ts.QueuedPressureLeasedMultiplier != 3 {
		t.Fatalf("unexpected trend_signals config: %#v", ts)
	}
}

func TestCompile_TrendSignalsValidation(t *testing.T) {
	in := []byte(`
defaults {
  trend_signals {
    window off
    stale_grace_factor 0
    dead_share_high_percent 101
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	var foundWindow, foundGrace, foundDead bool
	for _, e := range res.Errors {
		if strings.Contains(e, "defaults.trend_signals.window") {
			foundWindow = true
		}
		if strings.Contains(e, "defaults.trend_signals.stale_grace_factor") {
			foundGrace = true
		}
		if strings.Contains(e, "defaults.trend_signals.dead_share_high_percent") {
			foundDead = true
		}
	}
	if !foundWindow || !foundGrace || !foundDead {
		t.Fatalf("expected trend_signals validation errors, got %#v", res.Errors)
	}
}

func TestParseFormat_DefaultsTrendSignals(t *testing.T) {
	in := []byte(`
defaults {
  trend_signals {
    window "30m"
    dead_share_high_percent "35"
  }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "trend_signals {") ||
		!strings.Contains(got, `window "30m"`) ||
		!strings.Contains(got, `dead_share_high_percent "35"`) {
		t.Fatalf("expected formatted trend_signals block, got:\n%s", got)
	}
}

func TestCompile_AdaptiveBackpressureDefaults(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}

	ab := compiled.Defaults.AdaptiveBackpressure
	if ab.Enabled != defaultAdaptiveBackpressureEnabled {
		t.Fatalf("enabled: got %v", ab.Enabled)
	}
	if ab.MinTotal != defaultAdaptiveBackpressureMinTotal {
		t.Fatalf("min_total: got %d", ab.MinTotal)
	}
	if ab.QueuedPercent != defaultAdaptiveBackpressureQueuedPct {
		t.Fatalf("queued_percent: got %d", ab.QueuedPercent)
	}
	if ab.ReadyLag != defaultAdaptiveBackpressureReadyLag {
		t.Fatalf("ready_lag: got %s", ab.ReadyLag)
	}
	if ab.OldestQueuedAge != defaultAdaptiveBackpressureOldestAge {
		t.Fatalf("oldest_queued_age: got %s", ab.OldestQueuedAge)
	}
	if ab.SustainedGrowth != defaultAdaptiveBackpressureSustained {
		t.Fatalf("sustained_growth: got %v", ab.SustainedGrowth)
	}
}

func TestCompile_AdaptiveBackpressureOverrides(t *testing.T) {
	in := []byte(`
defaults {
  adaptive_backpressure {
    enabled on
    min_total 500
    queued_percent 70
    ready_lag 45s
    oldest_queued_age 90s
    sustained_growth off
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}

	ab := compiled.Defaults.AdaptiveBackpressure
	if !ab.Enabled ||
		ab.MinTotal != 500 ||
		ab.QueuedPercent != 70 ||
		ab.ReadyLag != 45*time.Second ||
		ab.OldestQueuedAge != 90*time.Second ||
		ab.SustainedGrowth {
		t.Fatalf("unexpected adaptive_backpressure config: %#v", ab)
	}
}

func TestCompile_AdaptiveBackpressureValidation(t *testing.T) {
	in := []byte(`
defaults {
  adaptive_backpressure {
    enabled maybe
    min_total 0
    queued_percent 101
    ready_lag off
    oldest_queued_age -1s
    sustained_growth maybe
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}

	var (
		foundEnabled  bool
		foundMinTotal bool
		foundPercent  bool
		foundReadyLag bool
		foundOldest   bool
		foundGrowth   bool
	)
	for _, e := range res.Errors {
		if strings.Contains(e, "defaults.adaptive_backpressure.enabled") {
			foundEnabled = true
		}
		if strings.Contains(e, "defaults.adaptive_backpressure.min_total") {
			foundMinTotal = true
		}
		if strings.Contains(e, "defaults.adaptive_backpressure.queued_percent") {
			foundPercent = true
		}
		if strings.Contains(e, "defaults.adaptive_backpressure.ready_lag") {
			foundReadyLag = true
		}
		if strings.Contains(e, "defaults.adaptive_backpressure.oldest_queued_age") {
			foundOldest = true
		}
		if strings.Contains(e, "defaults.adaptive_backpressure.sustained_growth") {
			foundGrowth = true
		}
	}
	if !foundEnabled || !foundMinTotal || !foundPercent || !foundReadyLag || !foundOldest || !foundGrowth {
		t.Fatalf("expected adaptive_backpressure validation errors, got %#v", res.Errors)
	}
}

func TestParseFormat_DefaultsAdaptiveBackpressure(t *testing.T) {
	in := []byte(`
defaults {
  adaptive_backpressure {
    enabled "on"
    min_total "250"
    oldest_queued_age "75s"
  }
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "adaptive_backpressure {") ||
		!strings.Contains(got, `enabled "on"`) ||
		!strings.Contains(got, `min_total "250"`) ||
		!strings.Contains(got, `oldest_queued_age "75s"`) {
		t.Fatalf("expected formatted adaptive_backpressure block, got:\n%s", got)
	}
}

func TestCompile_EgressPolicyDefaults(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if !compiled.Defaults.EgressPolicy.HTTPSOnly {
		t.Fatalf("https_only default: got false")
	}
	if compiled.Defaults.EgressPolicy.Redirects {
		t.Fatalf("redirects default: got true")
	}
	if !compiled.Defaults.EgressPolicy.DNSRebindProtection {
		t.Fatalf("dns_rebind_protection default: got false")
	}
}

func TestCompile_EgressPolicyOverrides(t *testing.T) {
	in := []byte(`
defaults {
  egress {
    allow "*"
    allow "10.0.0.0/8"
    deny "*.bad.example.com"
    https_only off
    redirects on
    dns_rebind_protection off
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Defaults.EgressPolicy.HTTPSOnly {
		t.Fatalf("https_only override: got true")
	}
	if !compiled.Defaults.EgressPolicy.Redirects {
		t.Fatalf("redirects override: got false")
	}
	if compiled.Defaults.EgressPolicy.DNSRebindProtection {
		t.Fatalf("dns_rebind_protection override: got true")
	}
	if len(compiled.Defaults.EgressPolicy.Allow) != 2 {
		t.Fatalf("expected allow rule, got %#v", compiled.Defaults.EgressPolicy.Allow)
	}
	if compiled.Defaults.EgressPolicy.Allow[0].Host != "*" {
		t.Fatalf("expected wildcard allow host, got %#v", compiled.Defaults.EgressPolicy.Allow[0])
	}
	if len(compiled.Defaults.EgressPolicy.Deny) != 1 {
		t.Fatalf("expected deny rule, got %#v", compiled.Defaults.EgressPolicy.Deny)
	}
	if !compiled.Defaults.EgressPolicy.Deny[0].Subdomains {
		t.Fatalf("expected wildcard deny rule, got %#v", compiled.Defaults.EgressPolicy.Deny[0])
	}
}

func TestCompile_EgressPolicyValidation(t *testing.T) {
	in := []byte(`
defaults {
  egress {
    https_only maybe
    allow "http://bad.example.com"
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	foundRule := false
	for _, e := range res.Errors {
		if strings.Contains(e, "defaults.egress.https_only") {
			found = true
		}
		if strings.Contains(e, "defaults.egress.allow") {
			foundRule = true
		}
	}
	if !found || !foundRule {
		t.Fatalf("expected egress allow + deny error, got %#v", res.Errors)
	}
}

func TestCompile_EgressPolicyMultiValueDirectives(t *testing.T) {
	in := []byte(`
defaults {
  egress {
    allow "10.0.0.0/8" "*.example.com" api.internal
    deny "bad.example.com" "192.0.2.0/24"
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}

	if got := compiled.Defaults.EgressPolicy.Allow; len(got) != 3 {
		t.Fatalf("expected 3 egress allow rules, got %#v", got)
	}
	if got := compiled.Defaults.EgressPolicy.Allow[0]; !got.IsCIDR || got.CIDR.String() != "10.0.0.0/8" {
		t.Fatalf("unexpected first allow rule: %#v", got)
	}
	if got := compiled.Defaults.EgressPolicy.Allow[1]; got.Host != "example.com" || !got.Subdomains {
		t.Fatalf("unexpected second allow rule: %#v", got)
	}
	if got := compiled.Defaults.EgressPolicy.Allow[2]; got.Host != "api.internal" || got.Subdomains || got.IsCIDR {
		t.Fatalf("unexpected third allow rule: %#v", got)
	}

	if got := compiled.Defaults.EgressPolicy.Deny; len(got) != 2 {
		t.Fatalf("expected 2 egress deny rules, got %#v", got)
	}
	if got := compiled.Defaults.EgressPolicy.Deny[0]; got.Host != "bad.example.com" || got.Subdomains || got.IsCIDR {
		t.Fatalf("unexpected first deny rule: %#v", got)
	}
	if got := compiled.Defaults.EgressPolicy.Deny[1]; !got.IsCIDR || got.CIDR.String() != "192.0.2.0/24" {
		t.Fatalf("unexpected second deny rule: %#v", got)
	}
}

func TestCompile_PublishPolicyDefaults(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if !compiled.Defaults.PublishPolicy.DirectEnabled {
		t.Fatalf("expected direct default on")
	}
	if !compiled.Defaults.PublishPolicy.ManagedEnabled {
		t.Fatalf("expected managed default on")
	}
	if !compiled.Defaults.PublishPolicy.AllowPullRoutes {
		t.Fatalf("expected allow_pull_routes default on")
	}
	if !compiled.Defaults.PublishPolicy.AllowDeliverRoutes {
		t.Fatalf("expected allow_deliver_routes default on")
	}
	if compiled.Defaults.PublishPolicy.RequireActor {
		t.Fatalf("expected require_actor default off")
	}
	if compiled.Defaults.PublishPolicy.RequireRequestID {
		t.Fatalf("expected require_request_id default off")
	}
	if compiled.Defaults.PublishPolicy.FailClosed {
		t.Fatalf("expected fail_closed default off")
	}
	if len(compiled.Defaults.PublishPolicy.ActorAllowlist) != 0 {
		t.Fatalf("expected actor_allow default empty")
	}
	if len(compiled.Defaults.PublishPolicy.ActorPrefixes) != 0 {
		t.Fatalf("expected actor_prefix default empty")
	}
}

func TestCompile_PublishPolicyOverrides(t *testing.T) {
	in := []byte(`
defaults {
  publish_policy {
    direct off
    managed false
    allow_pull_routes 0
    allow_deliver_routes off
    require_actor true
    require_request_id 1
    fail_closed on
    actor_allow "ops@example.test"
    actor_allow "ops@example.test"
    actor_allow "svc-publisher"
    actor_prefix "role:ops/"
    actor_prefix "role:ops/"
    actor_prefix "role:platform/"
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.Defaults.PublishPolicy.DirectEnabled {
		t.Fatalf("expected direct off")
	}
	if compiled.Defaults.PublishPolicy.ManagedEnabled {
		t.Fatalf("expected managed off")
	}
	if compiled.Defaults.PublishPolicy.AllowPullRoutes {
		t.Fatalf("expected allow_pull_routes off")
	}
	if compiled.Defaults.PublishPolicy.AllowDeliverRoutes {
		t.Fatalf("expected allow_deliver_routes off")
	}
	if !compiled.Defaults.PublishPolicy.RequireActor {
		t.Fatalf("expected require_actor on")
	}
	if !compiled.Defaults.PublishPolicy.RequireRequestID {
		t.Fatalf("expected require_request_id on")
	}
	if !compiled.Defaults.PublishPolicy.FailClosed {
		t.Fatalf("expected fail_closed on")
	}
	if got := compiled.Defaults.PublishPolicy.ActorAllowlist; len(got) != 2 || got[0] != "ops@example.test" || got[1] != "svc-publisher" {
		t.Fatalf("unexpected actor_allowlist: %#v", got)
	}
	if got := compiled.Defaults.PublishPolicy.ActorPrefixes; len(got) != 2 || got[0] != "role:ops/" || got[1] != "role:platform/" {
		t.Fatalf("unexpected actor_prefixes: %#v", got)
	}
}

func TestCompile_PublishPolicyOverridesMultiValueActorDirectives(t *testing.T) {
	in := []byte(`
defaults {
  publish_policy {
    actor_allow "ops@example.test" "ops@example.test" svc-publisher
    actor_allow svc-publisher
    actor_prefix role:ops/ role:ops/ role:platform/
    actor_prefix role:platform/
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if got := compiled.Defaults.PublishPolicy.ActorAllowlist; len(got) != 2 || got[0] != "ops@example.test" || got[1] != "svc-publisher" {
		t.Fatalf("unexpected actor_allowlist: %#v", got)
	}
	if got := compiled.Defaults.PublishPolicy.ActorPrefixes; len(got) != 2 || got[0] != "role:ops/" || got[1] != "role:platform/" {
		t.Fatalf("unexpected actor_prefixes: %#v", got)
	}
}

func TestCompile_PublishPolicyValidation(t *testing.T) {
	in := []byte(`
defaults {
  publish_policy {
    direct maybe
    allow_pull_routes perhaps
    require_actor invalid
    fail_closed maybe
    actor_allow ""
    actor_prefix "   "
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	foundPull := false
	foundAuditActor := false
	foundManagedActorAllow := false
	foundManagedActorPrefix := false
	foundScopedFailClosed := false
	for _, e := range res.Errors {
		if strings.Contains(e, "defaults.publish_policy.direct") {
			found = true
		}
		if strings.Contains(e, "defaults.publish_policy.allow_pull_routes") {
			foundPull = true
		}
		if strings.Contains(e, "defaults.publish_policy.require_actor") {
			foundAuditActor = true
		}
		if strings.Contains(e, "defaults.publish_policy.actor_allow") {
			foundManagedActorAllow = true
		}
		if strings.Contains(e, "defaults.publish_policy.actor_prefix") {
			foundManagedActorPrefix = true
		}
		if strings.Contains(e, "defaults.publish_policy.fail_closed") {
			foundScopedFailClosed = true
		}
	}
	if !found || !foundPull || !foundAuditActor || !foundScopedFailClosed || !foundManagedActorAllow || !foundManagedActorPrefix {
		t.Fatalf("expected publish_policy validation error, got %#v", res.Errors)
	}
}

func TestCompile_SecretsValidation(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "env:MY_SECRET"
    valid_from "nope"
  }
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "valid_from") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected valid_from error, got %#v", res.Errors)
	}
}

func TestCompile_SecretsValidationEdgeCases(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name: "empty value",
			input: `secrets {
  secret "S1" {
    value ""
    valid_from "2025-01-01T00:00:00Z"
  }
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e" } }`,
			wantErr: "value must not be empty",
		},
		{
			name: "missing valid_from",
			input: `secrets {
  secret "S1" {
    value "raw:s"
  }
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e" } }`,
			wantErr: "valid_from is required",
		},
		{
			name: "duplicate secret ID",
			input: `secrets {
  secret "S1" {
    value "raw:a"
    valid_from "2025-01-01T00:00:00Z"
  }
  secret "S1" {
    value "raw:b"
    valid_from "2025-06-01T00:00:00Z"
  }
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e" } }`,
			wantErr: `duplicate secret "S1"`,
		},
		{
			name: "valid_until before valid_from",
			input: `secrets {
  secret "S1" {
    value "raw:s"
    valid_from "2025-06-01T00:00:00Z"
    valid_until "2025-01-01T00:00:00Z"
  }
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e" } }`,
			wantErr: "valid_until must be after valid_from",
		},
		{
			name: "valid_until empty string",
			input: `secrets {
  secret "S1" {
    value "raw:s"
    valid_from "2025-01-01T00:00:00Z"
    valid_until ""
  }
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e" } }`,
			wantErr: "valid_until must not be empty",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Parse([]byte(tc.input))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			_, res := Compile(cfg)
			if res.OK {
				t.Fatalf("expected error, got ok")
			}
			found := false
			for _, e := range res.Errors {
				if strings.Contains(e, tc.wantErr) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, res.Errors)
			}
		})
	}
}

func TestCompile_SecretRefSchemeValidation(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name: "pull_api token invalid scheme",
			input: `
pull_api { auth token "kms:abc" }
"/x" { pull { path "/e" } }
`,
			wantErr: "pull_api.auth token[0]",
		},
		{
			name: "route pull token invalid scheme",
			input: `
pull_api { auth token "raw:t" }
"/x" { pull { path "/e" auth token "kms:abc" } }
`,
			wantErr: `route "/x" pull.auth token[0]`,
		},
		{
			name: "auth hmac direct secret invalid scheme",
			input: `
pull_api { auth token "raw:t" }
"/x" { auth hmac "kms:abc" pull { path "/e" } }
`,
			wantErr: `route "/x" auth hmac secret[0]`,
		},
		{
			name: "deliver sign hmac invalid scheme",
			input: `
pull_api { auth token "raw:t" }
"/x" {
  deliver "https://example.com/hook" {
    sign hmac "kms:abc"
  }
}
`,
			wantErr: `route "/x" deliver[0].sign hmac`,
		},
		{
			name: "secrets value invalid scheme",
			input: `
secrets {
  secret "S1" {
    value "kms:abc"
    valid_from "2025-01-01T00:00:00Z"
  }
}
pull_api { auth token "raw:t" }
"/x" { auth hmac secret_ref "S1" pull { path "/e" } }
`,
			wantErr: `secret "S1" value`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Parse([]byte(tc.input))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			_, res := Compile(cfg)
			if res.OK {
				t.Fatalf("expected error, got ok")
			}
			found := false
			for _, e := range res.Errors {
				if strings.Contains(e, tc.wantErr) && strings.Contains(e, "invalid secret reference") {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("expected secret ref validation error containing %q, got %v", tc.wantErr, res.Errors)
			}
		})
	}
}

func TestCompile_SecretRefRequiresSecret(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth hmac secret_ref "S1"
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "secret_ref") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected secret_ref error, got %#v", res.Errors)
	}
}

func TestCompile_SecretRefOK(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "raw:secret"
    valid_from "2025-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/x" {
  auth hmac secret_ref "S1"
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 || len(compiled.Routes[0].AuthHMACSecretRefs) != 1 {
		t.Fatalf("expected secret_ref to compile, got %#v", compiled.Routes)
	}
}

func TestCompile_DLQRetentionDefaults(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if compiled.DLQRetention.MaxAge != defaultDLQRetentionMaxAge {
		t.Fatalf("max_age: got %s", compiled.DLQRetention.MaxAge)
	}
	if compiled.DLQRetention.MaxDepth != defaultDLQRetentionMaxDepth {
		t.Fatalf("max_depth: got %d", compiled.DLQRetention.MaxDepth)
	}
	if !compiled.DLQRetention.Enabled {
		t.Fatalf("expected dlq retention enabled")
	}
}

func TestCompile_QueueLimitsValidation(t *testing.T) {
	in := []byte(`
queue_limits {
  max_depth nope
  drop_policy unknown
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	foundDepth := false
	foundPolicy := false
	for _, e := range res.Errors {
		if strings.Contains(e, "queue_limits.max_depth") {
			foundDepth = true
		}
		if strings.Contains(e, "queue_limits.drop_policy") {
			foundPolicy = true
		}
	}
	if !foundDepth || !foundPolicy {
		t.Fatalf("expected queue_limits errors, got %#v", res.Errors)
	}
}

func TestCompile_DefaultsValidation(t *testing.T) {
	in := []byte(`
defaults { max_body nope }

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "defaults.max_body") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected defaults.max_body error, got %#v", res.Errors)
	}
}

func TestCompile_QueueRetentionValidation(t *testing.T) {
	in := []byte(`
queue_retention {
  max_age nope
  prune_interval off
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	foundAge := false
	foundInterval := false
	for _, e := range res.Errors {
		if strings.Contains(e, "queue_retention.max_age") {
			foundAge = true
		}
		if strings.Contains(e, "queue_retention.prune_interval") {
			foundInterval = true
		}
	}
	if !foundAge || !foundInterval {
		t.Fatalf("expected queue_retention errors, got %#v", res.Errors)
	}
}

func TestCompile_DLQRetentionValidation(t *testing.T) {
	in := []byte(`
dlq_retention {
  max_age nope
  max_depth -1
}
pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	foundAge := false
	foundDepth := false
	for _, e := range res.Errors {
		if strings.Contains(e, "dlq_retention.max_age") {
			foundAge = true
		}
		if strings.Contains(e, "dlq_retention.max_depth") {
			foundDepth = true
		}
	}
	if !foundAge || !foundDepth {
		t.Fatalf("expected dlq_retention errors, got %#v", res.Errors)
	}
}

func TestCompile_SharedListenerRequiresPrefixes(t *testing.T) {
	in := []byte(`
pull_api { listen ":9999" auth token "raw:t" }
admin_api { listen ":9999" }

"/x" {
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "shared listener requires") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected shared listener error, got %#v", res.Errors)
	}
}

func TestCompile_APITLSConfig(t *testing.T) {
	in := []byte(`
pull_api {
  tls { cert_file "certs/pull.crt" key_file "certs/pull.key" client_ca "certs/ca.pem" client_auth require }
  auth token "raw:t"
}

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if !compiled.PullAPI.TLS.Enabled {
		t.Fatalf("expected TLS enabled")
	}
	if compiled.PullAPI.TLS.CertFile != "certs/pull.crt" || compiled.PullAPI.TLS.KeyFile != "certs/pull.key" {
		t.Fatalf("unexpected TLS config: %#v", compiled.PullAPI.TLS)
	}
	if compiled.PullAPI.TLS.ClientCA != "certs/ca.pem" {
		t.Fatalf("unexpected client_ca: %#v", compiled.PullAPI.TLS)
	}
	if compiled.PullAPI.TLS.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("unexpected client_auth: %#v", compiled.PullAPI.TLS)
	}
}

func TestCompile_APITLSValidation(t *testing.T) {
	in := []byte(`
pull_api {
  tls { cert_file "certs/pull.crt" }
  auth token "raw:t"
}

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "pull_api.tls.key_file") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected tls key_file error, got %#v", res.Errors)
	}
}

func TestCompile_APITLSClientAuthRequiresCA(t *testing.T) {
	in := []byte(`
pull_api {
  tls { cert_file "certs/pull.crt" key_file "certs/pull.key" client_auth require }
  auth token "raw:t"
}

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "pull_api.tls.client_ca is required") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected client_ca required error, got %#v", res.Errors)
	}
}

func TestCompile_APITLSClientAuthInvalid(t *testing.T) {
	in := []byte(`
pull_api {
  tls { cert_file "certs/pull.crt" key_file "certs/pull.key" client_auth nope }
  auth token "raw:t"
}

"/x" { pull { path "/e" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "pull_api.tls.client_auth") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected client_auth error, got %#v", res.Errors)
	}
}

func TestCompile_EndpointMustBeRelativeToPrefix(t *testing.T) {
	in := []byte(`
pull_api { prefix "/pull" auth token "raw:t" }

"/webhooks/github" {
  pull { path "/pull/github" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "must be relative to pull_api.prefix") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected prefix-relative error, got %#v", res.Errors)
	}
}

func TestParse_HooksWrapper(t *testing.T) {
	in := []byte(`hooks { "/x" { deliver "http://example.com" } }`)
	_, err := Parse(in)
	if err == nil || !strings.Contains(err.Error(), `unknown top-level block "hooks"`) {
		t.Fatalf("expected hooks rejection, got: %v", err)
	}
}

func TestParse_UnquotedRoutePaths(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

/webhooks/github { pull { path "/pull/github" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(cfg.Routes))
	}
	if cfg.Routes[0].Path != "/webhooks/github" {
		t.Fatalf("unexpected path: %q", cfg.Routes[0].Path)
	}
}

func TestParse_UnquotedRoutesInHooks(t *testing.T) {
	in := []byte(`hooks { /a { pull { path "/ea" } } }`)
	_, err := Parse(in)
	if err == nil || !strings.Contains(err.Error(), `unknown top-level block "hooks"`) {
		t.Fatalf("expected hooks rejection, got: %v", err)
	}
}

func TestFormat_PreservesHooksWrapper(t *testing.T) {
	in := []byte(`hooks { /a { pull { path "/ea" } } }`)
	_, err := Parse(in)
	if err == nil || !strings.Contains(err.Error(), `unknown top-level block "hooks"`) {
		t.Fatalf("expected hooks rejection, got: %v", err)
	}
}

func TestFormat_MixedHooksAndTopLevelRoutesFlattensRoutes(t *testing.T) {
	in := []byte(`hooks { /nested { pull { path "/nested" } } }`)
	_, err := Parse(in)
	if err == nil || !strings.Contains(err.Error(), `unknown top-level block "hooks"`) {
		t.Fatalf("expected hooks rejection, got: %v", err)
	}
}

func TestFormat_RoutePathQuoting(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

/plain { pull { path "/e1" } }
"/space here" { pull { path "/e2" } }
"/has#comment" { pull { path "/e3" } }
"/has{brace}" { pull { path "/e4" } }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)

	if !strings.Contains(got, "/plain {") {
		t.Fatalf("expected unquoted /plain, got:\n%s", got)
	}
	if !strings.Contains(got, `"/space here" {`) {
		t.Fatalf("expected quoted /space here, got:\n%s", got)
	}
	if !strings.Contains(got, `"/has#comment" {`) {
		t.Fatalf("expected quoted /has#comment, got:\n%s", got)
	}
	if !strings.Contains(got, `"/has{brace}" {`) {
		t.Fatalf("expected quoted /has{brace}, got:\n%s", got)
	}
}

func TestFormat_ValueQuoting(t *testing.T) {
	in := []byte(`
ingress { listen ":8080" }
pull_api {
  listen :9443
  prefix "/pull"
  auth token env:HOOKAIDO_PULL_TOKEN
}

/x {
  auth hmac env:HOOKAIDO_INGRESS_SECRET
  pull { path "/pull/github" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)

	if !strings.Contains(got, `listen ":8080"`) {
		t.Fatalf("expected quoted listen :8080, got:\n%s", got)
	}
	if !strings.Contains(got, "listen :9443") {
		t.Fatalf("expected unquoted listen :9443, got:\n%s", got)
	}
	if !strings.Contains(got, `prefix "/pull"`) {
		t.Fatalf("expected quoted prefix, got:\n%s", got)
	}
	if !strings.Contains(got, "auth token env:HOOKAIDO_PULL_TOKEN") {
		t.Fatalf("expected unquoted auth token, got:\n%s", got)
	}
	if !strings.Contains(got, "auth hmac env:HOOKAIDO_INGRESS_SECRET") {
		t.Fatalf("expected unquoted auth hmac, got:\n%s", got)
	}
	if !strings.Contains(got, `path "/pull/github"`) {
		t.Fatalf("expected quoted endpoint, got:\n%s", got)
	}
}

func TestParse_AuthDirectives(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t1" auth token "raw:t2" }

"/x" {
  auth hmac "raw:s1"
  auth hmac "raw:s2"
  auth hmac secret_ref "S1"
  pull { path "/e" auth token "raw:p1" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.PullAPI == nil || len(cfg.PullAPI.AuthTokens) != 2 {
		t.Fatalf("expected 2 pull_api tokens, got %#v", cfg.PullAPI)
	}
	if len(cfg.Routes) != 1 || len(cfg.Routes[0].AuthHMACSecrets) != 3 {
		t.Fatalf("expected 3 hmac entries, got %#v", cfg.Routes)
	}
	if len(cfg.Routes) != 1 || len(cfg.Routes[0].AuthHMACSecretIsRef) != 3 || !cfg.Routes[0].AuthHMACSecretIsRef[2] {
		t.Fatalf("expected secret_ref flag, got %#v", cfg.Routes[0].AuthHMACSecretIsRef)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].Pull == nil || len(cfg.Routes[0].Pull.AuthTokens) != 1 {
		t.Fatalf("expected pull auth token, got %#v", cfg.Routes)
	}
}

func TestParseFormat_AuthHMACBlock(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth hmac {
    secret "raw:s1"
    secret_ref "S1"
    signature_header "X-Hookaido-Signature"
    timestamp_header "X-Hookaido-Timestamp"
    nonce_header "X-Hookaido-Nonce"
    tolerance "10m"
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected one route, got %#v", cfg.Routes)
	}
	route := cfg.Routes[0]
	if !route.AuthHMACBlockSet {
		t.Fatalf("expected auth hmac block flag")
	}
	if len(route.AuthHMACSecrets) != 2 {
		t.Fatalf("expected 2 hmac secrets from block, got %#v", route.AuthHMACSecrets)
	}
	if len(route.AuthHMACSecretIsRef) != 2 || !route.AuthHMACSecretIsRef[1] {
		t.Fatalf("expected second hmac secret as secret_ref, got %#v", route.AuthHMACSecretIsRef)
	}
	if !route.AuthHMACSignatureHeaderSet || route.AuthHMACSignatureHeader != "X-Hookaido-Signature" {
		t.Fatalf("expected signature_header set, got %#v", route.AuthHMACSignatureHeader)
	}
	if !route.AuthHMACTimestampHeaderSet || route.AuthHMACTimestampHeader != "X-Hookaido-Timestamp" {
		t.Fatalf("expected timestamp_header set, got %#v", route.AuthHMACTimestampHeader)
	}
	if !route.AuthHMACNonceHeaderSet || route.AuthHMACNonceHeader != "X-Hookaido-Nonce" {
		t.Fatalf("expected nonce_header set, got %#v", route.AuthHMACNonceHeader)
	}
	if !route.AuthHMACToleranceSet || route.AuthHMACTolerance != "10m" {
		t.Fatalf("expected tolerance set, got %#v", route.AuthHMACTolerance)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "auth hmac {") {
		t.Fatalf("expected auth hmac block, got:\n%s", got)
	}
	if !strings.Contains(got, `signature_header "X-Hookaido-Signature"`) {
		t.Fatalf("expected signature_header in format output, got:\n%s", got)
	}
	if !strings.Contains(got, `timestamp_header "X-Hookaido-Timestamp"`) {
		t.Fatalf("expected timestamp_header in format output, got:\n%s", got)
	}
	if !strings.Contains(got, `nonce_header "X-Hookaido-Nonce"`) {
		t.Fatalf("expected nonce_header in format output, got:\n%s", got)
	}
	if !strings.Contains(got, `tolerance "10m"`) {
		t.Fatalf("expected tolerance in format output, got:\n%s", got)
	}
}

func TestParseFormat_AuthHMACBlockMultiValueSecretDirectives(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth hmac {
    secret "raw:s1" "raw:s2"
    secret_ref "S1" S2
    tolerance "10m"
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected one route, got %#v", cfg.Routes)
	}
	route := cfg.Routes[0]
	if !route.AuthHMACBlockSet {
		t.Fatalf("expected auth hmac block flag")
	}
	if got := route.AuthHMACSecrets; len(got) != 4 || got[0] != "raw:s1" || got[1] != "raw:s2" || got[2] != "S1" || got[3] != "S2" {
		t.Fatalf("unexpected hmac secrets from block: %#v", got)
	}
	if got := route.AuthHMACSecretIsRef; len(got) != 4 || got[0] || got[1] || !got[2] || !got[3] {
		t.Fatalf("unexpected hmac secret_ref flags: %#v", got)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `secret "raw:s1"`) || !strings.Contains(got, `secret "raw:s2"`) {
		t.Fatalf("expected split secret directives in format output, got:\n%s", got)
	}
	if !strings.Contains(got, `secret_ref "S1"`) || !strings.Contains(got, "secret_ref S2") {
		t.Fatalf("expected split secret_ref directives in format output, got:\n%s", got)
	}
}

func TestParseFormat_AuthHMACShorthandWithInlineBlock(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth hmac secret_ref "S1" {
    signature_header "X-Hookaido-Signature"
    timestamp_header "X-Hookaido-Timestamp"
    nonce_header "X-Hookaido-Nonce"
    tolerance "10m"
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected one route, got %#v", cfg.Routes)
	}
	route := cfg.Routes[0]
	if !route.AuthHMACBlockSet {
		t.Fatalf("expected auth hmac block flag")
	}
	if len(route.AuthHMACSecrets) != 1 || route.AuthHMACSecrets[0] != "S1" {
		t.Fatalf("expected one secret_ref from shorthand, got %#v", route.AuthHMACSecrets)
	}
	if len(route.AuthHMACSecretIsRef) != 1 || !route.AuthHMACSecretIsRef[0] {
		t.Fatalf("expected first hmac secret as secret_ref, got %#v", route.AuthHMACSecretIsRef)
	}
	if !route.AuthHMACSignatureHeaderSet || route.AuthHMACSignatureHeader != "X-Hookaido-Signature" {
		t.Fatalf("expected signature_header set, got %#v", route.AuthHMACSignatureHeader)
	}
	if !route.AuthHMACTimestampHeaderSet || route.AuthHMACTimestampHeader != "X-Hookaido-Timestamp" {
		t.Fatalf("expected timestamp_header set, got %#v", route.AuthHMACTimestampHeader)
	}
	if !route.AuthHMACNonceHeaderSet || route.AuthHMACNonceHeader != "X-Hookaido-Nonce" {
		t.Fatalf("expected nonce_header set, got %#v", route.AuthHMACNonceHeader)
	}
	if !route.AuthHMACToleranceSet || route.AuthHMACTolerance != "10m" {
		t.Fatalf("expected tolerance set, got %#v", route.AuthHMACTolerance)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "auth hmac {") {
		t.Fatalf("expected auth hmac block, got:\n%s", got)
	}
	if !strings.Contains(got, `secret_ref "S1"`) {
		t.Fatalf("expected secret_ref in format output, got:\n%s", got)
	}
	if !strings.Contains(got, `tolerance "10m"`) {
		t.Fatalf("expected tolerance in format output, got:\n%s", got)
	}
}

func TestCompile_AuthHMACBlockOptions(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth hmac {
    secret "raw:s1"
    signature_header x-hookaido-signature
    timestamp_header x-hookaido-timestamp
    nonce_header x-hookaido-nonce
    tolerance 15m
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected one route, got %#v", compiled.Routes)
	}
	route := compiled.Routes[0]
	if route.AuthHMACSignatureHeader != "X-Hookaido-Signature" {
		t.Fatalf("signature header: got %q", route.AuthHMACSignatureHeader)
	}
	if route.AuthHMACTimestampHeader != "X-Hookaido-Timestamp" {
		t.Fatalf("timestamp header: got %q", route.AuthHMACTimestampHeader)
	}
	if route.AuthHMACNonceHeader != "X-Hookaido-Nonce" {
		t.Fatalf("nonce header: got %q", route.AuthHMACNonceHeader)
	}
	if route.AuthHMACTolerance != 15*time.Minute {
		t.Fatalf("tolerance: got %s", route.AuthHMACTolerance)
	}
}

func TestCompile_AuthHMACBlockMultiValueSecretDirectives(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "raw:s1"
    valid_from "2025-01-01T00:00:00Z"
  }
  secret "S2" {
    value "raw:s2"
    valid_from "2025-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/x" {
  auth hmac {
    secret "raw:local1" "raw:local2"
    secret_ref "S1" "S2"
    tolerance 7m
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected one route, got %#v", compiled.Routes)
	}
	route := compiled.Routes[0]
	if len(route.AuthHMACSecrets) != 2 || route.AuthHMACSecrets[0] != "raw:local1" || route.AuthHMACSecrets[1] != "raw:local2" {
		t.Fatalf("unexpected direct hmac secrets: %#v", route.AuthHMACSecrets)
	}
	if len(route.AuthHMACSecretRefs) != 2 || route.AuthHMACSecretRefs[0] != "S1" || route.AuthHMACSecretRefs[1] != "S2" {
		t.Fatalf("unexpected hmac secret refs: %#v", route.AuthHMACSecretRefs)
	}
	if route.AuthHMACTolerance != 7*time.Minute {
		t.Fatalf("tolerance: got %s", route.AuthHMACTolerance)
	}
}

func TestCompile_AuthHMACShorthandSecretRefWithInlineBlock(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "raw:s1"
    valid_from "2025-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/x" {
  auth hmac secret_ref "S1" {
    signature_header x-hookaido-signature
    timestamp_header x-hookaido-timestamp
    nonce_header x-hookaido-nonce
    tolerance 7m
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected one route, got %#v", compiled.Routes)
	}
	route := compiled.Routes[0]
	if len(route.AuthHMACSecrets) != 0 {
		t.Fatalf("expected zero direct hmac secrets, got %#v", route.AuthHMACSecrets)
	}
	if len(route.AuthHMACSecretRefs) != 1 || route.AuthHMACSecretRefs[0] != "S1" {
		t.Fatalf("expected one hmac secret_ref S1, got %#v", route.AuthHMACSecretRefs)
	}
	if route.AuthHMACSignatureHeader != "X-Hookaido-Signature" {
		t.Fatalf("signature header: got %q", route.AuthHMACSignatureHeader)
	}
	if route.AuthHMACTimestampHeader != "X-Hookaido-Timestamp" {
		t.Fatalf("timestamp header: got %q", route.AuthHMACTimestampHeader)
	}
	if route.AuthHMACNonceHeader != "X-Hookaido-Nonce" {
		t.Fatalf("nonce header: got %q", route.AuthHMACNonceHeader)
	}
	if route.AuthHMACTolerance != 7*time.Minute {
		t.Fatalf("tolerance: got %s", route.AuthHMACTolerance)
	}
}

func TestCompile_AuthHMACHeaderNamesMustDiffer(t *testing.T) {
	tests := []struct {
		name       string
		directives string
		want       string
	}{
		{
			name: "SignatureTimestampExplicitCollision",
			directives: `
    signature_header "X-Hookaido-Signature"
    timestamp_header "X-Hookaido-Signature"
`,
			want: "auth hmac signature_header and timestamp_header must differ",
		},
		{
			name: "SignatureTimestampDefaultCollision",
			directives: `
    signature_header "X-Timestamp"
`,
			want: "auth hmac signature_header and timestamp_header must differ",
		},
		{
			name: "SignatureNonceDefaultCollision",
			directives: `
    nonce_header "X-Signature"
`,
			want: "auth hmac signature_header and nonce_header must differ",
		},
		{
			name: "TimestampNonceExplicitCollision",
			directives: `
    timestamp_header "X-Hookaido-Shared"
    nonce_header "X-Hookaido-Shared"
`,
			want: "auth hmac timestamp_header and nonce_header must differ",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(fmt.Sprintf(`
pull_api { auth token "raw:t" }

"/x" {
  auth hmac {
    secret "raw:s1"%s
  }
  pull { path "/e" }
}
`, tc.directives))
			cfg, err := Parse(in)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			_, res := Compile(cfg)
			if res.OK {
				t.Fatalf("expected error, got ok")
			}
			found := false
			for _, e := range res.Errors {
				if strings.Contains(e, tc.want) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("expected %q error, got %#v", tc.want, res.Errors)
			}
		})
	}
}

func TestCompile_AuthHMACBlockOptionsRequireSecret(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth hmac {
    signature_header "X-Hookaido-Signature"
    tolerance "10m"
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "auth hmac options require at least one secret or secret_ref") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected auth hmac options require secret error, got %#v", res.Errors)
	}
}

func TestCompile_AuthHMACBlockOptionsValidation(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth hmac {
    secret "raw:s1"
    signature_header "bad header"
    tolerance 0s
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	foundHeader := false
	foundTolerance := false
	for _, e := range res.Errors {
		if strings.Contains(e, "signature_header") && strings.Contains(e, "valid header token") {
			foundHeader = true
		}
		if strings.Contains(e, "auth hmac tolerance") && strings.Contains(e, "positive duration") {
			foundTolerance = true
		}
	}
	if !foundHeader {
		t.Fatalf("expected signature header validation error, got %#v", res.Errors)
	}
	if !foundTolerance {
		t.Fatalf("expected tolerance validation error, got %#v", res.Errors)
	}
}

func TestCompile_AuthForwardBlockOptions(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth forward "https://auth.example.test/check" {
    timeout 1500ms
    copy_headers x-user-id
    copy_headers X-User-Id
    copy_headers x-role
    body_limit 64kb
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected one route, got %#v", compiled.Routes)
	}
	route := compiled.Routes[0]
	if !route.AuthForward.Enabled {
		t.Fatalf("expected forward auth to be enabled")
	}
	if route.AuthForward.URL != "https://auth.example.test/check" {
		t.Fatalf("url: got %q", route.AuthForward.URL)
	}
	if route.AuthForward.Timeout != 1500*time.Millisecond {
		t.Fatalf("timeout: got %s", route.AuthForward.Timeout)
	}
	if len(route.AuthForward.CopyHeaders) != 2 || route.AuthForward.CopyHeaders[0] != "X-User-Id" || route.AuthForward.CopyHeaders[1] != "X-Role" {
		t.Fatalf("copy headers: got %#v", route.AuthForward.CopyHeaders)
	}
	if route.AuthForward.BodyLimitBytes != 64*1024 {
		t.Fatalf("body limit: got %d", route.AuthForward.BodyLimitBytes)
	}
}

func TestCompile_AuthForwardBlockOptionsMultiValueCopyHeadersDirective(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth forward "https://auth.example.test/check" {
    copy_headers x-user-id X-User-Id x-role
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected one route, got %#v", compiled.Routes)
	}
	route := compiled.Routes[0]
	if !route.AuthForward.Enabled {
		t.Fatalf("expected forward auth to be enabled")
	}
	if len(route.AuthForward.CopyHeaders) != 2 || route.AuthForward.CopyHeaders[0] != "X-User-Id" || route.AuthForward.CopyHeaders[1] != "X-Role" {
		t.Fatalf("copy headers: got %#v", route.AuthForward.CopyHeaders)
	}
}

func TestCompile_AuthForwardValidation(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  auth forward "ftp://auth.example.test/check" {
    timeout 0s
    copy_headers "bad header"
    body_limit "bad"
  }
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatalf("expected error, got ok")
	}
	foundURL := false
	foundTimeout := false
	foundHeaders := false
	foundBodyLimit := false
	for _, e := range res.Errors {
		if strings.Contains(e, "auth forward.url must use http or https") {
			foundURL = true
		}
		if strings.Contains(e, "auth forward.timeout") && strings.Contains(e, "positive duration") {
			foundTimeout = true
		}
		if strings.Contains(e, "auth forward.copy_headers") && strings.Contains(e, "valid header token") {
			foundHeaders = true
		}
		if strings.Contains(e, "auth forward.body_limit") && strings.Contains(e, "positive size") {
			foundBodyLimit = true
		}
	}
	if !foundURL || !foundTimeout || !foundHeaders || !foundBodyLimit {
		t.Fatalf("expected forward auth validation errors, got %#v", res.Errors)
	}
}

// ── Channel type: parse ──────────────────────────────────────────────

func TestParseFormat_OutboundShorthand(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

outbound /notifications/slack {
  deliver "https://hooks.slack.com/x" { }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(cfg.Routes))
	}
	if cfg.Routes[0].ChannelType != ChannelOutbound {
		t.Fatalf("expected outbound, got %q", cfg.Routes[0].ChannelType)
	}
	if cfg.Routes[0].Path != "/notifications/slack" {
		t.Fatalf("unexpected path %q", cfg.Routes[0].Path)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "outbound /notifications/slack {") {
		t.Fatalf("expected shorthand output, got:\n%s", got)
	}
}

func TestParseFormat_OutboundWrapper(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

outbound {
  /jobs/a {
    deliver "https://a.internal/hook" { }
  }
  /jobs/b {
    deliver "https://b.internal/hook" { }
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(cfg.Routes))
	}
	for _, r := range cfg.Routes {
		if r.ChannelType != ChannelOutbound {
			t.Fatalf("expected outbound, got %q", r.ChannelType)
		}
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "outbound {") {
		t.Fatalf("expected wrapper output, got:\n%s", got)
	}
	if !strings.Contains(got, "  /jobs/a {") {
		t.Fatalf("expected indented route, got:\n%s", got)
	}
}

func TestParseFormat_InternalShorthand(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

internal /jobs/nightly {
  pull { path /pull/nightly }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(cfg.Routes))
	}
	if cfg.Routes[0].ChannelType != ChannelInternal {
		t.Fatalf("expected internal, got %q", cfg.Routes[0].ChannelType)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "internal /jobs/nightly {") {
		t.Fatalf("expected shorthand output, got:\n%s", got)
	}
}

func TestParseFormat_InternalWrapper(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

internal {
  /jobs/a {
    pull { path /pull/a }
  }
  /jobs/b {
    pull { path /pull/b }
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(cfg.Routes))
	}
	for _, r := range cfg.Routes {
		if r.ChannelType != ChannelInternal {
			t.Fatalf("expected internal, got %q", r.ChannelType)
		}
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "internal {") {
		t.Fatalf("expected wrapper output, got:\n%s", got)
	}
}

func TestParseFormat_InboundExplicitWrapper(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

inbound {
  /webhooks/github {
    pull { path /pull/github }
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(cfg.Routes))
	}
	if cfg.Routes[0].ChannelType != ChannelInbound {
		t.Fatalf("expected inbound, got %q", cfg.Routes[0].ChannelType)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "inbound /webhooks/github {") {
		t.Fatalf("expected inbound shorthand output, got:\n%s", got)
	}
}

func TestParseFormat_BareRouteImplicitInbound(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

/webhooks/github {
  pull { path /pull/github }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(cfg.Routes))
	}
	if cfg.Routes[0].ChannelType != ChannelDefault {
		t.Fatalf("expected empty ChannelType, got %q", cfg.Routes[0].ChannelType)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	// Bare routes should NOT have a channel type prefix.
	if strings.Contains(got, "inbound") || strings.Contains(got, "outbound") || strings.Contains(got, "internal") {
		t.Fatalf("expected bare route without channel prefix, got:\n%s", got)
	}
}

func TestParseFormat_MixedChannelTypes(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

/webhooks/github {
  pull { path /pull/github }
}

outbound /jobs/deploy {
  deliver "https://ci.internal/deploy" { }
}

internal /jobs/cleanup {
  pull { path /pull/cleanup }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes) != 3 {
		t.Fatalf("expected 3 routes, got %d", len(cfg.Routes))
	}
	if cfg.Routes[0].ChannelType != ChannelDefault {
		t.Fatalf("route 0: expected default, got %q", cfg.Routes[0].ChannelType)
	}
	if cfg.Routes[1].ChannelType != ChannelOutbound {
		t.Fatalf("route 1: expected outbound, got %q", cfg.Routes[1].ChannelType)
	}
	if cfg.Routes[2].ChannelType != ChannelInternal {
		t.Fatalf("route 2: expected internal, got %q", cfg.Routes[2].ChannelType)
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)

	// Re-parse to verify round-trip produces identical output.
	cfg2, err := Parse(out)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	out2, err := Format(cfg2)
	if err != nil {
		t.Fatalf("re-format: %v", err)
	}
	if string(out2) != got {
		t.Fatalf("format not stable:\n--- first ---\n%s\n--- second ---\n%s", got, string(out2))
	}
}

// ── Channel type: compile constraints ────────────────────────────────

func TestCompile_OutboundValid(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

outbound /jobs/deploy {
  deliver "https://ci.internal/deploy" { }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("expected OK, got errors: %v", res.Errors)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected 1 compiled route, got %d", len(compiled.Routes))
	}
	if compiled.Routes[0].ChannelType != ChannelOutbound {
		t.Fatalf("expected outbound in compiled route, got %q", compiled.Routes[0].ChannelType)
	}
}

func TestCompile_OutboundForbidsPull(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

outbound /jobs/deploy {
  pull { path /pull/deploy }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected error, got ok")
	}
	assertCompileError(t, res, "outbound", "pull is forbidden")
}

func TestCompile_OutboundRequiresDeliver(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

outbound /jobs/deploy {
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected error, got ok")
	}
	assertCompileError(t, res, "outbound", "deliver is required")
}

func TestCompile_OutboundForbidsMatch(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

outbound /jobs/deploy {
  match { method POST }
  deliver "https://ci.internal/deploy" { }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected error, got ok")
	}
	assertCompileError(t, res, "outbound", "match is forbidden")
}

func TestCompile_OutboundForbidsAuth(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

outbound /jobs/deploy {
  auth basic "user" "pass"
  deliver "https://ci.internal/deploy" { }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected error, got ok")
	}
	assertCompileError(t, res, "outbound", "auth is forbidden")
}

func TestCompile_OutboundForbidsRateLimit(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

outbound /jobs/deploy {
  rate_limit { rps 10 }
  deliver "https://ci.internal/deploy" { }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected error, got ok")
	}
	assertCompileError(t, res, "outbound", "rate_limit is forbidden")
}

func TestCompile_InternalValid(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

internal /jobs/nightly {
  pull { path /pull/nightly }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("expected OK, got errors: %v", res.Errors)
	}
	if len(compiled.Routes) != 1 {
		t.Fatalf("expected 1 compiled route, got %d", len(compiled.Routes))
	}
	if compiled.Routes[0].ChannelType != ChannelInternal {
		t.Fatalf("expected internal in compiled route, got %q", compiled.Routes[0].ChannelType)
	}
}

func TestCompile_InternalForbidsDeliver(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

internal /jobs/nightly {
  deliver "https://ci.internal/deploy" { }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected error, got ok")
	}
	assertCompileError(t, res, "internal", "deliver is forbidden")
}

func TestCompile_InternalRequiresPull(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

internal /jobs/nightly {
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected error, got ok")
	}
	assertCompileError(t, res, "internal", "pull is required")
}

func TestCompile_InternalForbidsMatch(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

internal /jobs/nightly {
  match { method POST }
  pull { path /pull/nightly }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected error, got ok")
	}
	assertCompileError(t, res, "internal", "match is forbidden")
}

func TestCompile_InternalForbidsAuth(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

internal /jobs/nightly {
  auth hmac "env:SECRET"
  pull { path /pull/nightly }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected error, got ok")
	}
	assertCompileError(t, res, "internal", "auth is forbidden")
}

func TestCompile_InternalForbidsRateLimit(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

internal /jobs/nightly {
  rate_limit { rps 5 }
  pull { path /pull/nightly }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected error, got ok")
	}
	assertCompileError(t, res, "internal", "rate_limit is forbidden")
}

// ── Channel type: format round-trip ──────────────────────────────────

func TestFormat_OutboundRoundTrip(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

outbound /jobs/deploy {
  deliver "https://ci.internal/deploy" { }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	cfg2, err := Parse(out)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	out2, err := Format(cfg2)
	if err != nil {
		t.Fatalf("re-format: %v", err)
	}
	if string(out) != string(out2) {
		t.Fatalf("format not stable:\n--- first ---\n%s\n--- second ---\n%s", out, out2)
	}
}

func TestFormat_InternalWrapperRoundTrip(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

internal {
  /jobs/a {
    pull { path /pull/a }
  }
  /jobs/b {
    pull { path /pull/b }
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	cfg2, err := Parse(out)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	out2, err := Format(cfg2)
	if err != nil {
		t.Fatalf("re-format: %v", err)
	}
	if string(out) != string(out2) {
		t.Fatalf("format not stable:\n--- first ---\n%s\n--- second ---\n%s", out, out2)
	}
}

// assertCompileError checks that at least one compilation error contains all needles.
func assertCompileError(t *testing.T, res ValidationResult, needles ...string) {
	t.Helper()
	for _, e := range res.Errors {
		allMatch := true
		for _, n := range needles {
			if !strings.Contains(e, n) {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
	}
	t.Fatalf("expected compile error containing %v, got: %v", needles, res.Errors)
}

// --- publish.direct / publish.managed dot-notation tests ---

func TestParseFormat_PublishDotDirect(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  publish.direct off
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Routes[0].Publish == nil || !cfg.Routes[0].Publish.DirectSet {
		t.Fatal("expected Publish.DirectSet")
	}
	if cfg.Routes[0].Publish.Direct != "off" {
		t.Fatalf("expected Direct=off, got %q", cfg.Routes[0].Publish.Direct)
	}
	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "publish.direct off") {
		t.Fatalf("expected dot-notation preserved in format, got:\n%s", got)
	}
}

func TestParseFormat_PublishDotManaged(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  publish.managed off
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Routes[0].Publish == nil || !cfg.Routes[0].Publish.ManagedSet {
		t.Fatal("expected Publish.ManagedSet")
	}
	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "publish.managed off") {
		t.Fatalf("expected dot-notation preserved in format, got:\n%s", got)
	}
}

func TestParseFormat_PublishDotBoth(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  publish.direct off
  publish.managed on
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	p := cfg.Routes[0].Publish
	if p == nil || !p.DirectSet || !p.ManagedSet {
		t.Fatal("expected both DirectSet and ManagedSet")
	}
	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "publish.direct off") || !strings.Contains(got, "publish.managed on") {
		t.Fatalf("expected both dot-notation directives, got:\n%s", got)
	}
}

func TestParse_PublishDotDuplicate(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  publish.direct off
  publish.direct on
  pull { path "/e" }
}
`)
	_, err := Parse(in)
	if err == nil || !strings.Contains(err.Error(), "duplicate publish.direct") {
		t.Fatalf("expected duplicate error, got: %v", err)
	}
}

func TestParse_PublishDotConflictsWithShorthand(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  publish on
  publish.direct off
  pull { path "/e" }
}
`)
	_, err := Parse(in)
	if err == nil || !strings.Contains(err.Error(), "conflicts with publish shorthand") {
		t.Fatalf("expected conflict error, got: %v", err)
	}
}

func TestParse_PublishBlockConflictsWithDotNotation(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  publish.direct off
  publish { managed on }
  pull { path "/e" }
}
`)
	_, err := Parse(in)
	if err == nil || !strings.Contains(err.Error(), "conflicts with publish.direct/publish.managed") {
		t.Fatalf("expected conflict error, got: %v", err)
	}
}

func TestCompile_PublishDotNotation(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

"/x" {
  publish.direct off
  publish.managed on
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if len(res.Errors) > 0 {
		t.Fatalf("compile errors: %v", res.Errors)
	}
	r := compiled.Routes[0]
	if r.PublishDirect {
		t.Fatal("expected PublishDirect=false")
	}
	if !r.PublishManaged {
		t.Fatal("expected PublishManaged=true")
	}
}

func TestParseFormat_PublishDotInChannelWrapper(t *testing.T) {
	in := []byte(`
pull_api { auth token "raw:t" }

internal {
  /jobs/report {
    publish.managed off
    pull { path /pull/reports }
  }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "publish.managed off") {
		t.Fatalf("expected dot-notation in channel wrapper output, got:\n%s", got)
	}
}
