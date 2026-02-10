package app

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersionCmd_Default(t *testing.T) {
	restore := setVersionMetadataForTest("v1.2.3", "abc123", "2026-02-09T12:00:00Z")
	defer restore()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVersionCmd(nil, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d", code)
	}
	if got := strings.TrimSpace(stdout.String()); got != "v1.2.3" {
		t.Fatalf("expected version output %q, got %q", "v1.2.3", got)
	}
	if got := strings.TrimSpace(stderr.String()); got != "" {
		t.Fatalf("expected empty stderr, got %q", got)
	}
}

func TestVersionCmd_Long(t *testing.T) {
	restore := setVersionMetadataForTest("v1.2.3", "abc123", "2026-02-09T12:00:00Z")
	defer restore()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVersionCmd([]string{"--long"}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d", code)
	}
	got := strings.TrimSpace(stdout.String())
	want := "v1.2.3 (commit=abc123, build_date=2026-02-09T12:00:00Z)"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
	if got := strings.TrimSpace(stderr.String()); got != "" {
		t.Fatalf("expected empty stderr, got %q", got)
	}
}

func TestVersionCmd_JSON(t *testing.T) {
	restore := setVersionMetadataForTest("v1.2.3", "abc123", "2026-02-09T12:00:00Z")
	defer restore()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVersionCmd([]string{"--json"}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d", code)
	}
	got := strings.TrimSpace(stdout.String())
	if !strings.Contains(got, `"version":"v1.2.3"`) {
		t.Fatalf("expected json version field, got %q", got)
	}
	if !strings.Contains(got, `"commit":"abc123"`) {
		t.Fatalf("expected json commit field, got %q", got)
	}
	if !strings.Contains(got, `"build_date":"2026-02-09T12:00:00Z"`) {
		t.Fatalf("expected json build_date field, got %q", got)
	}
	if got := strings.TrimSpace(stderr.String()); got != "" {
		t.Fatalf("expected empty stderr, got %q", got)
	}
}

func TestVersionCmd_BadArgs(t *testing.T) {
	restore := setVersionMetadataForTest("v1.2.3", "abc123", "2026-02-09T12:00:00Z")
	defer restore()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVersionCmd([]string{"positional"}, stdout, stderr)
	if code != 2 {
		t.Fatalf("expected exit code 2, got %d", code)
	}
	if got := strings.TrimSpace(stdout.String()); got != "" {
		t.Fatalf("expected empty stdout, got %q", got)
	}
	if got := stderr.String(); !strings.Contains(got, "unexpected positional arguments") {
		t.Fatalf("expected positional argument error, got %q", got)
	}
}

func setVersionMetadataForTest(v, c, d string) func() {
	origVersion := version
	origCommit := commit
	origBuildDate := buildDate
	version = v
	commit = c
	buildDate = d
	return func() {
		version = origVersion
		commit = origCommit
		buildDate = origBuildDate
	}
}
