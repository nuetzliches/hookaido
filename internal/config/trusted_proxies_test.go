package config

import (
	"strings"
	"testing"
)

func TestParseCompile_IngressTrustedProxies(t *testing.T) {
	in := []byte(`
ingress {
  listen :8080
  trusted_proxies "10.0.0.0/8" "fd00::/8" "192.0.2.7"
}

"/webhooks/source" {
  match { remote_ip "203.0.113.0/24" }
  pull { path "/pull/source" }
}

pull_api { auth token "raw:devtoken" }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := len(cfg.Ingress.TrustedProxies); got != 3 {
		t.Fatalf("parsed trusted_proxies = %d, want 3", got)
	}

	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %s", FormatValidationText(res))
	}
	got := compiled.Ingress.TrustedProxies
	if len(got) != 3 {
		t.Fatalf("compiled trusted_proxies = %#v, want 3 entries", got)
	}
	if got[0].String() != "10.0.0.0/8" {
		t.Fatalf("trusted_proxies[0] = %q", got[0].String())
	}
	if got[1].String() != "fd00::/8" {
		t.Fatalf("trusted_proxies[1] = %q", got[1].String())
	}
	// A bare address compiles to a host prefix, as `match remote_ip` does.
	if got[2].String() != "192.0.2.7/32" {
		t.Fatalf("trusted_proxies[2] = %q", got[2].String())
	}
}

func TestCompile_IngressTrustedProxiesDefaultsEmpty(t *testing.T) {
	cfg, err := Parse([]byte(`
ingress { listen :8080 }

"/x" { pull { path "/pull/x" } }

pull_api { auth token "raw:devtoken" }
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %s", FormatValidationText(res))
	}
	if len(compiled.Ingress.TrustedProxies) != 0 {
		t.Fatalf("expected trusted_proxies to default to empty, got %#v", compiled.Ingress.TrustedProxies)
	}
}

func TestCompile_IngressTrustedProxiesRejectsInvalid(t *testing.T) {
	cfg, err := Parse([]byte(`
ingress {
  listen :8080
  trusted_proxies "not-an-ip"
}

"/x" { pull { path "/pull/x" } }

pull_api { auth token "raw:devtoken" }
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected an invalid trusted_proxies entry to fail compilation")
	}
	if !strings.Contains(FormatValidationText(res), "trusted_proxies") {
		t.Fatalf("expected the error to name trusted_proxies, got:\n%s", FormatValidationText(res))
	}
}

func TestCompile_IngressTrustedProxiesWarnsOnDuplicate(t *testing.T) {
	cfg, err := Parse([]byte(`
ingress {
  listen :8080
  trusted_proxies "10.0.0.0/8" "10.0.0.0/8"
}

"/x" { pull { path "/pull/x" } }

pull_api { auth token "raw:devtoken" }
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %s", FormatValidationText(res))
	}
	if len(compiled.Ingress.TrustedProxies) != 1 {
		t.Fatalf("expected the duplicate to be dropped, got %#v", compiled.Ingress.TrustedProxies)
	}
	joined := strings.Join(res.Warnings, "\n")
	if !strings.Contains(joined, "trusted_proxies") {
		t.Fatalf("expected a duplicate warning, got warnings:\n%s", joined)
	}
}

func TestFormat_IngressTrustedProxiesRoundTrips(t *testing.T) {
	in := []byte(`ingress {
  listen :8080
  trusted_proxies "10.0.0.0/8" "fd00::/8"
}

"/x" {
  pull { path "/pull/x" }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	first, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	if !strings.Contains(string(first), `trusted_proxies "10.0.0.0/8" "fd00::/8"`) {
		t.Fatalf("expected trusted_proxies to be written back, got:\n%s", first)
	}

	// config fmt has to be stable: a second pass must not change anything.
	reparsed, err := Parse(first)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	second, err := Format(reparsed)
	if err != nil {
		t.Fatalf("reformat: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("format is not stable:\nfirst:\n%s\nsecond:\n%s", first, second)
	}
}

func TestParse_IngressTrustedProxiesFollowedByAnotherDirective(t *testing.T) {
	cfg, err := Parse([]byte(`ingress {
  trusted_proxies "10.0.0.0/8" "172.16.0.0/12"
  listen :8080
  rate_limit { rps 10 }
}

"/x" {
  pull { path "/pull/x" }
}
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := len(cfg.Ingress.TrustedProxies); got != 2 {
		t.Fatalf("trusted_proxies = %#v, want 2 entries", cfg.Ingress.TrustedProxies)
	}
	if cfg.Ingress.Listen != ":8080" {
		t.Fatalf("listen = %q, want :8080", cfg.Ingress.Listen)
	}
	if cfg.Ingress.RateLimit == nil {
		t.Fatal("expected the rate_limit block to be parsed")
	}
}
