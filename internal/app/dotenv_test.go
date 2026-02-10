package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDotenv_SetsVars(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	data := []byte(`
# comment
HOOKAIDO_PULL_TOKEN=devtoken
export HOOKAIDO_INGRESS_SECRET="devsecret"
SINGLE='a b'
`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	t.Setenv("HOOKAIDO_PULL_TOKEN", "")
	if err := loadDotenv(path); err != nil {
		t.Fatalf("loadDotenv: %v", err)
	}

	if got := os.Getenv("HOOKAIDO_PULL_TOKEN"); got != "devtoken" {
		t.Fatalf("HOOKAIDO_PULL_TOKEN=%q, want devtoken", got)
	}
	if got := os.Getenv("HOOKAIDO_INGRESS_SECRET"); got != "devsecret" {
		t.Fatalf("HOOKAIDO_INGRESS_SECRET=%q, want devsecret", got)
	}
	if got := os.Getenv("SINGLE"); got != "a b" {
		t.Fatalf("SINGLE=%q, want 'a b'", got)
	}
}

func TestLoadDotenv_DoesNotOverrideNonEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("HOOKAIDO_PULL_TOKEN=devtoken\n"), 0o644); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	t.Setenv("HOOKAIDO_PULL_TOKEN", "prodtoken")
	if err := loadDotenv(path); err != nil {
		t.Fatalf("loadDotenv: %v", err)
	}
	if got := os.Getenv("HOOKAIDO_PULL_TOKEN"); got != "prodtoken" {
		t.Fatalf("HOOKAIDO_PULL_TOKEN=%q, want prodtoken", got)
	}
}

func TestLoadDotenv_InvalidLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("NOEQUALS\n"), 0o644); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	if err := loadDotenv(path); err == nil {
		t.Fatalf("expected error")
	}
}
