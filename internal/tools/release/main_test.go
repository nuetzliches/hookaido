package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseTargets_Default(t *testing.T) {
	targets, err := parseTargets("")
	if err != nil {
		t.Fatalf("parseTargets default: %v", err)
	}
	if len(targets) != len(defaultTargets) {
		t.Fatalf("expected %d default targets, got %d", len(defaultTargets), len(targets))
	}
}

func TestParseTargets_CustomAndDedup(t *testing.T) {
	targets, err := parseTargets("linux/amd64,linux/amd64,darwin/arm64")
	if err != nil {
		t.Fatalf("parseTargets custom: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("expected 2 deduped targets, got %d", len(targets))
	}
	if targets[0].GOOS != "linux" || targets[0].GOARCH != "amd64" {
		t.Fatalf("unexpected first target: %#v", targets[0])
	}
	if targets[1].GOOS != "darwin" || targets[1].GOARCH != "arm64" {
		t.Fatalf("unexpected second target: %#v", targets[1])
	}
}

func TestParseTargets_Invalid(t *testing.T) {
	if _, err := parseTargets("linux"); err == nil {
		t.Fatalf("expected invalid target error")
	}
}

func TestSafeFileNameFragment(t *testing.T) {
	got := safeFileNameFragment("v1.0.0+build/meta")
	if got != "v1.0.0_build_meta" {
		t.Fatalf("expected sanitized version, got %q", got)
	}
}

func TestArtifactFileName(t *testing.T) {
	linux := artifactFileName("v1.0.0", buildTarget{GOOS: "linux", GOARCH: "amd64"})
	if linux != "hookaido_v1.0.0_linux_amd64.tar.gz" {
		t.Fatalf("unexpected linux artifact name: %q", linux)
	}
	windows := artifactFileName("v1.0.0", buildTarget{GOOS: "windows", GOARCH: "amd64"})
	if windows != "hookaido_v1.0.0_windows_amd64.zip" {
		t.Fatalf("unexpected windows artifact name: %q", windows)
	}
}

func TestBuildChecksumsAndWrite(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "a.txt")
	second := filepath.Join(dir, "b.txt")
	if err := os.WriteFile(first, []byte("alpha"), 0o644); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if err := os.WriteFile(second, []byte("beta"), 0o644); err != nil {
		t.Fatalf("write second: %v", err)
	}
	checksums, err := buildChecksums([]string{first, second})
	if err != nil {
		t.Fatalf("buildChecksums: %v", err)
	}
	if len(checksums) != 2 {
		t.Fatalf("expected two checksum entries, got %d", len(checksums))
	}
	checksumFile := filepath.Join(dir, "checksums.txt")
	if err := writeChecksums(checksumFile, checksums); err != nil {
		t.Fatalf("writeChecksums: %v", err)
	}
	body, err := os.ReadFile(checksumFile)
	if err != nil {
		t.Fatalf("read checksum file: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "  a.txt\n") {
		t.Fatalf("checksum output missing first file: %q", text)
	}
	if !strings.Contains(text, "  b.txt\n") {
		t.Fatalf("checksum output missing second file: %q", text)
	}
}

func TestSignChecksums(t *testing.T) {
	dir := t.TempDir()
	checksumPath := filepath.Join(dir, "hookaido_v1_checksums.txt")
	checksumBody := []byte("abcd  artifact.tar.gz\n")
	if err := os.WriteFile(checksumPath, checksumBody, 0o644); err != nil {
		t.Fatalf("write checksums: %v", err)
	}
	keyPath, publicKey := writeTestSigningKey(t, dir)

	signaturePath, publicKeyPath, err := signChecksums(checksumPath, keyPath)
	if err != nil {
		t.Fatalf("signChecksums: %v", err)
	}
	signatureBody, err := os.ReadFile(signaturePath)
	if err != nil {
		t.Fatalf("read signature: %v", err)
	}
	signatureRaw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(signatureBody)))
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	if !ed25519.Verify(publicKey, checksumBody, signatureRaw) {
		t.Fatalf("signature verification failed")
	}

	publicKeyPEM, err := os.ReadFile(publicKeyPath)
	if err != nil {
		t.Fatalf("read public key: %v", err)
	}
	block, _ := pem.Decode(publicKeyPEM)
	if block == nil {
		t.Fatalf("public key pem decode failed")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	parsedPublicKey, ok := parsed.(ed25519.PublicKey)
	if !ok {
		t.Fatalf("unexpected public key type: %T", parsed)
	}
	if string(parsedPublicKey) != string(publicKey) {
		t.Fatalf("public key mismatch")
	}
}

func TestWriteManifest(t *testing.T) {
	dir := t.TempDir()
	artifactPath := filepath.Join(dir, "hookaido_v1_linux_amd64.tar.gz")
	if err := os.WriteFile(artifactPath, []byte("artifact"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	checksumPath := filepath.Join(dir, "hookaido_v1_checksums.txt")
	if err := os.WriteFile(checksumPath, []byte("sum  hookaido_v1_linux_amd64.tar.gz\n"), 0o644); err != nil {
		t.Fatalf("write checksums: %v", err)
	}
	checksums, err := buildChecksums([]string{artifactPath})
	if err != nil {
		t.Fatalf("buildChecksums: %v", err)
	}
	manifestPath := filepath.Join(dir, "hookaido_v1_manifest.json")
	cfg := releaseConfig{
		Version:   "v1.0.0",
		Commit:    "abc123",
		BuildDate: "2026-02-09T00:00:00Z",
	}
	if err := writeManifest(manifestPath, cfg, checksums, checksumPath, "", ""); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	body, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest releaseManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if manifest.Schema != "hookaido.release-manifest.v1" {
		t.Fatalf("unexpected schema: %q", manifest.Schema)
	}
	if manifest.Version != cfg.Version || manifest.Commit != cfg.Commit || manifest.BuildDate != cfg.BuildDate {
		t.Fatalf("manifest metadata mismatch: %#v", manifest)
	}
	if len(manifest.Artifacts) != 1 || manifest.Artifacts[0].File != filepath.Base(artifactPath) {
		t.Fatalf("unexpected artifact list: %#v", manifest.Artifacts)
	}
	if manifest.Checksums.File != filepath.Base(checksumPath) {
		t.Fatalf("unexpected checksum file: %#v", manifest.Checksums)
	}
	if manifest.Checksums.SignatureFile != "" || manifest.Checksums.PublicKeyFile != "" {
		t.Fatalf("unexpected signing fields for unsigned manifest: %#v", manifest.Checksums)
	}
}

func writeTestSigningKey(t *testing.T, dir string) (string, ed25519.PublicKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})
	keyPath := filepath.Join(dir, "signing-key.pem")
	if err := os.WriteFile(keyPath, privatePEM, 0o600); err != nil {
		t.Fatalf("write private key: %v", err)
	}
	return keyPath, publicKey
}
