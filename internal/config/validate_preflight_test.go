package config

import (
	"strings"
	"testing"
)

const strictSecretsConfig = `
pull_api {
  auth token env:HOOKAIDO_PULL_TOKEN
}

"/hooks" {
  pull {
    path /pull/hooks
  }
}
`

func TestValidateWithResultOptions_SecretPreflightDisabled(t *testing.T) {
	cfg, err := Parse([]byte(strictSecretsConfig))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	t.Setenv("HOOKAIDO_PULL_TOKEN", "")
	res := ValidateWithResultOptions(cfg, ValidationOptions{SecretPreflight: false})
	if !res.OK {
		t.Fatalf("expected ok=true without strict secret preflight, got %#v", res)
	}
}

func TestValidateWithResultOptions_SecretPreflightEnabled(t *testing.T) {
	cfg, err := Parse([]byte(strictSecretsConfig))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	t.Setenv("HOOKAIDO_PULL_TOKEN", "")
	res := ValidateWithResultOptions(cfg, ValidationOptions{SecretPreflight: true})
	if res.OK {
		t.Fatalf("expected ok=false with strict secret preflight and missing env var, got %#v", res)
	}

	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "secret preflight") && strings.Contains(e, "HOOKAIDO_PULL_TOKEN") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected secret preflight env error, got %#v", res.Errors)
	}
}

func TestValidateWithResultOptions_SecretPreflightEnabledSuccess(t *testing.T) {
	cfg, err := Parse([]byte(strictSecretsConfig))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	t.Setenv("HOOKAIDO_PULL_TOKEN", "test-token")
	res := ValidateWithResultOptions(cfg, ValidationOptions{SecretPreflight: true})
	if !res.OK {
		t.Fatalf("expected ok=true with strict secret preflight and present env var, got %#v", res)
	}
}
