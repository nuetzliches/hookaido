package app

import (
	"path/filepath"
	"testing"

	"github.com/nuetzliches/hookaido/internal/config"
)

func TestBuildTracingTLSConfig_None(t *testing.T) {
	cfg, err := buildTracingTLSConfig(config.ObservabilityConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil config, got %#v", cfg)
	}
}

func TestBuildTracingTLSConfig_MissingCAFile(t *testing.T) {
	_, err := buildTracingTLSConfig(config.ObservabilityConfig{
		TracingTLSCAFile: filepath.Join(t.TempDir(), "missing-ca.pem"),
	})
	if err == nil {
		t.Fatalf("expected error for missing ca file")
	}
}

func TestBuildTracingTLSConfig_ServerNameAndSkipVerify(t *testing.T) {
	cfg, err := buildTracingTLSConfig(config.ObservabilityConfig{
		TracingTLSServerName:         "otel.example.com",
		TracingTLSInsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatalf("expected tls config")
	}
	if cfg.ServerName != "otel.example.com" {
		t.Fatalf("unexpected server name %q", cfg.ServerName)
	}
	if !cfg.InsecureSkipVerify {
		t.Fatalf("expected insecure skip verify true")
	}
}
