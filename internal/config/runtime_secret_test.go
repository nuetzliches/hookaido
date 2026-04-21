package config

import (
	"strings"
	"testing"
)

func TestCompile_RuntimeSecretWithSeed(t *testing.T) {
	in := []byte(`
secrets {
  secret "cituro" {
    runtime true
    max_versions 8
    value "raw:bootstrap"
    valid_from "2026-04-21T00:00:00Z"
  }
}

pull_api { auth token "raw:t" }

"/cituro" {
  auth hmac secret_ref "cituro"
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if len(res.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	sc, ok := compiled.Secrets["cituro"]
	if !ok {
		t.Fatalf("expected compiled secret 'cituro'")
	}
	if !sc.Runtime {
		t.Fatalf("expected Runtime=true")
	}
	if sc.MaxVersions != 8 {
		t.Fatalf("expected MaxVersions=8, got %d", sc.MaxVersions)
	}
	if sc.ValueRef != "raw:bootstrap" {
		t.Fatalf("expected bootstrap ValueRef, got %q", sc.ValueRef)
	}
	if sc.ValidFrom.IsZero() {
		t.Fatalf("expected ValidFrom set")
	}
}

func TestCompile_RuntimeSecretEmpty(t *testing.T) {
	// runtime=true with no value is valid (pool starts empty, admin API pushes later).
	in := []byte(`
secrets {
  secret "cituro" {
    runtime true
  }
}

pull_api { auth token "raw:t" }

"/cituro" {
  auth hmac secret_ref "cituro"
  pull { path "/e" }
}
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	if len(res.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	sc, ok := compiled.Secrets["cituro"]
	if !ok {
		t.Fatalf("expected compiled secret 'cituro'")
	}
	if !sc.Runtime {
		t.Fatalf("expected Runtime=true")
	}
	if sc.ValueRef != "" {
		t.Fatalf("expected empty ValueRef, got %q", sc.ValueRef)
	}
}

func TestCompile_NonRuntimeSecretRequiresValue(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" { }
}
pull_api { auth token "raw:t" }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if len(res.Errors) == 0 {
		t.Fatalf("expected validation error")
	}
	joined := strings.Join(res.Errors, "\n")
	if !strings.Contains(joined, "value is required") {
		t.Fatalf("expected 'value is required' error, got: %v", res.Errors)
	}
}

func TestCompile_MaxVersionsOnlyWithRuntime(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    value "raw:x"
    valid_from "2026-01-01T00:00:00Z"
    max_versions 5
  }
}
pull_api { auth token "raw:t" }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if len(res.Errors) == 0 {
		t.Fatalf("expected validation error")
	}
	joined := strings.Join(res.Errors, "\n")
	if !strings.Contains(joined, "max_versions is only valid when runtime = true") {
		t.Fatalf("expected max_versions-only-with-runtime error, got: %v", res.Errors)
	}
}

func TestCompile_MaxVersionsInvalid(t *testing.T) {
	in := []byte(`
secrets {
  secret "S1" {
    runtime true
    max_versions -5
  }
}
pull_api { auth token "raw:t" }
`)
	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if len(res.Errors) == 0 {
		t.Fatalf("expected validation error")
	}
	joined := strings.Join(res.Errors, "\n")
	if !strings.Contains(joined, "max_versions must be a positive integer") {
		t.Fatalf("expected max_versions validation error, got: %v", res.Errors)
	}
}

func TestParseFormat_RuntimeSecretRoundTrip(t *testing.T) {
	in := []byte(`secrets {
  secret "cituro" {
    value "raw:bootstrap"
    valid_from "2026-04-21T00:00:00Z"
    runtime true
    max_versions 16
  }
}

pull_api {
  auth token "raw:t"
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
	if !strings.Contains(got, "runtime true") {
		t.Fatalf("runtime directive missing from formatted output:\n%s", got)
	}
	if !strings.Contains(got, "max_versions 16") {
		t.Fatalf("max_versions directive missing from formatted output:\n%s", got)
	}
}
