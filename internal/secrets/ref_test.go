package secrets

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRef_Env(t *testing.T) {
	t.Setenv("HOOKAIDO_TEST_SECRET", "top-secret")

	got, err := LoadRef("env:HOOKAIDO_TEST_SECRET")
	if err != nil {
		t.Fatalf("LoadRef(env): %v", err)
	}
	if string(got) != "top-secret" {
		t.Fatalf("unexpected env secret: %q", string(got))
	}
}

func TestLoadRef_File(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret.txt")
	if err := os.WriteFile(path, []byte("  file-secret \n"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := LoadRef("file:" + path)
	if err != nil {
		t.Fatalf("LoadRef(file): %v", err)
	}
	if string(got) != "file-secret" {
		t.Fatalf("unexpected file secret: %q", string(got))
	}
}

func TestLoadRef_Raw(t *testing.T) {
	got, err := LoadRef("raw:raw-secret")
	if err != nil {
		t.Fatalf("LoadRef(raw): %v", err)
	}
	if string(got) != "raw-secret" {
		t.Fatalf("unexpected raw secret: %q", string(got))
	}
}

func TestLoadRef_VaultKV2(t *testing.T) {
	t.Setenv(vaultTokenEnv, "vault-token")
	t.Setenv(vaultNamespaceEnv, "team/platform")

	var seenPath string
	var seenToken string
	var seenNamespace string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPath = r.URL.Path
		seenToken = r.Header.Get("X-Vault-Token")
		seenNamespace = r.Header.Get("X-Vault-Namespace")
		_, _ = w.Write([]byte(`{"data":{"data":{"signing":"s3cr3t"}}}`))
	}))
	defer srv.Close()

	t.Setenv(vaultAddrEnv, srv.URL)

	got, err := LoadRef("vault:secret/data/hookaido#signing")
	if err != nil {
		t.Fatalf("LoadRef(vault kv2): %v", err)
	}
	if string(got) != "s3cr3t" {
		t.Fatalf("unexpected vault secret: %q", string(got))
	}
	if seenPath != "/v1/secret/data/hookaido" {
		t.Fatalf("unexpected vault path: %q", seenPath)
	}
	if seenToken != "vault-token" {
		t.Fatalf("unexpected vault token header: %q", seenToken)
	}
	if seenNamespace != "team/platform" {
		t.Fatalf("unexpected vault namespace header: %q", seenNamespace)
	}
}

func TestLoadRef_VaultDefaultFieldFallback(t *testing.T) {
	t.Setenv(vaultTokenEnv, "vault-token")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"api_key":"key-123"}}`))
	}))
	defer srv.Close()
	t.Setenv(vaultAddrEnv, srv.URL)

	got, err := LoadRef("vault:secret/hookaido")
	if err != nil {
		t.Fatalf("LoadRef(vault kv1 fallback): %v", err)
	}
	if string(got) != "key-123" {
		t.Fatalf("unexpected fallback value: %q", string(got))
	}
}

func TestLoadRef_VaultErrorStatus(t *testing.T) {
	t.Setenv(vaultTokenEnv, "vault-token")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
	}))
	defer srv.Close()
	t.Setenv(vaultAddrEnv, srv.URL)

	_, err := LoadRef("vault:secret/data/hookaido#token")
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied error, got %v", err)
	}
}

func TestLoadRef_VaultMissingTokenEnv(t *testing.T) {
	t.Setenv(vaultAddrEnv, "http://127.0.0.1:8200")
	t.Setenv(vaultTokenEnv, "")

	_, err := LoadRef("vault:secret/data/hookaido#token")
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), vaultTokenEnv) {
		t.Fatalf("expected missing token env error, got %v", err)
	}
}

func TestLoadRef_VaultRejectsDotSegments(t *testing.T) {
	t.Setenv(vaultAddrEnv, "http://127.0.0.1:8200")
	t.Setenv(vaultTokenEnv, "vault-token")

	_, err := LoadRef("vault:secret/../sys/health#status")
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "dot segments") {
		t.Fatalf("expected dot segment validation error, got %v", err)
	}
}

func TestLoadRef_UnsupportedScheme(t *testing.T) {
	_, err := LoadRef("kms:key")
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "vault:") {
		t.Fatalf("expected vault scheme hint in error, got %v", err)
	}
}

func TestLoadRef_VaultAddressWithPathPrefix(t *testing.T) {
	t.Setenv(vaultTokenEnv, "vault-token")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/proxy/v1/secret/data/hookaido" {
			http.Error(w, fmt.Sprintf("unexpected path %s", r.URL.Path), http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"data":{"token":"prefixed"}}}`))
	}))
	defer srv.Close()
	t.Setenv(vaultAddrEnv, srv.URL+"/proxy")

	got, err := LoadRef("vault:secret/data/hookaido#token")
	if err != nil {
		t.Fatalf("LoadRef(vault prefixed addr): %v", err)
	}
	if string(got) != "prefixed" {
		t.Fatalf("unexpected secret: %q", string(got))
	}
}
