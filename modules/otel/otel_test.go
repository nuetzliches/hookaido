package otel

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/config"
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

func TestBuildTracingTLSConfig_LoadValidCAFile(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeSelfSignedCert(t, caPath, "")

	cfg, err := buildTracingTLSConfig(config.ObservabilityConfig{
		TracingTLSCAFile: caPath,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatalf("expected tls config with RootCAs, got %#v", cfg)
	}
}

func TestBuildTracingTLSConfig_InvalidCAPEM(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("not a real PEM"), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}

	_, err := buildTracingTLSConfig(config.ObservabilityConfig{
		TracingTLSCAFile: caPath,
	})
	if err == nil {
		t.Fatalf("expected error for invalid PEM")
	}
}

func TestBuildTracingTLSConfig_LoadClientCertAndKey(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	writeSelfSignedCert(t, certPath, keyPath)

	cfg, err := buildTracingTLSConfig(config.ObservabilityConfig{
		TracingTLSCertFile: certPath,
		TracingTLSKeyFile:  keyPath,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatalf("expected tls config")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(cfg.Certificates))
	}
}

func TestBuildTracingTLSConfig_MissingClientCert(t *testing.T) {
	_, err := buildTracingTLSConfig(config.ObservabilityConfig{
		TracingTLSCertFile: filepath.Join(t.TempDir(), "missing-cert.pem"),
		TracingTLSKeyFile:  filepath.Join(t.TempDir(), "missing-key.pem"),
	})
	if err == nil {
		t.Fatalf("expected error for missing client cert/key")
	}
}

func TestOtelModule_Name(t *testing.T) {
	m := &otelModule{}
	if got := m.Name(); got != "otel" {
		t.Fatalf("Name = %q, want otel", got)
	}
}

func TestOtelModule_Init_RejectsWrongConfigType(t *testing.T) {
	m := &otelModule{}
	_, err := m.Init(context.Background(), "not-an-observability-config", "v1", nil)
	if err == nil {
		t.Fatalf("expected error for wrong config type")
	}
}

func TestOtelModule_Init_InvalidProxyURL(t *testing.T) {
	m := &otelModule{}
	_, err := m.Init(context.Background(), config.ObservabilityConfig{
		TracingProxyURL: "://broken",
	}, "v1", nil)
	if err == nil {
		t.Fatalf("expected error for invalid proxy url")
	}
}

func TestOtelModule_Init_BadTLSPropagatesError(t *testing.T) {
	m := &otelModule{}
	_, err := m.Init(context.Background(), config.ObservabilityConfig{
		TracingTLSCAFile: filepath.Join(t.TempDir(), "missing-ca.pem"),
	}, "v1", nil)
	if err == nil {
		t.Fatalf("expected error for missing CA file")
	}
}

func TestOtelModule_Init_FullConfigSucceeds(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeSelfSignedCert(t, caPath, "")

	// The exporter is constructed lazily — no real connection happens at
	// Init time, so a placeholder https URL is sufficient to exercise the
	// TLS path without standing up a real collector.
	var caughtErr error
	m := &otelModule{}
	shutdown, err := m.Init(context.Background(), config.ObservabilityConfig{
		TracingCollector:   "https://otel-collector.example.com:4318",
		TracingURLPath:     "/v1/traces",
		TracingCompression: "gzip",
		TracingTimeout:     2 * time.Second,
		TracingTimeoutSet:  true,
		TracingHeaders: []config.TracingHeaderConfig{
			{Name: "X-Tenant", Value: "acme"},
		},
		TracingTLSCAFile: caPath,
		TracingRetry: &config.TracingRetryConfig{
			Enabled:         true,
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     500 * time.Millisecond,
			MaxElapsedTime:  1 * time.Second,
		},
	}, "v1.2.3", func(err error) { caughtErr = err })
	if err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}
	if shutdown == nil {
		t.Fatalf("expected non-nil shutdown")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	_ = caughtErr // captured by error handler if anything fired
}

func TestOtelModule_Init_NonGzipCompression(t *testing.T) {
	m := &otelModule{}
	shutdown, err := m.Init(context.Background(), config.ObservabilityConfig{
		TracingCompression: "none",
		TracingInsecure:    true,
	}, "", nil)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestOtelModule_Init_ValidProxyURL(t *testing.T) {
	m := &otelModule{}
	shutdown, err := m.Init(context.Background(), config.ObservabilityConfig{
		TracingProxyURL: "http://proxy.example.com:8080",
		TracingInsecure: true,
	}, "v1", nil)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestOtelModule_WrapHandler(t *testing.T) {
	m := &otelModule{}
	called := false
	inner := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	})
	wrapped := m.WrapHandler("test-handler", inner)
	if wrapped == nil {
		t.Fatalf("WrapHandler returned nil")
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/path", nil)
	wrapped.ServeHTTP(rec, req)
	if !called {
		t.Fatalf("inner handler was not invoked")
	}
}

func TestOtelModule_HTTPClient(t *testing.T) {
	m := &otelModule{}
	client := m.HTTPClient()
	if client == nil {
		t.Fatalf("HTTPClient returned nil")
	}
	if client.Transport == nil {
		t.Fatalf("HTTPClient transport should be wrapped, got nil")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("client.Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status=%d, want 204", resp.StatusCode)
	}
}

// writeSelfSignedCert writes a small self-signed ECDSA cert (and optional
// matching key) to the given paths so TLS-loading code paths can be exercised
// without external fixtures. If keyPath is empty, only the cert is written.
func writeSelfSignedCert(t *testing.T, certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "hookaido-test"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if keyPath == "" {
		return
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
}
