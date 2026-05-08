package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// --- writeZIP / addZIPFile ---

func TestWriteZIP_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "hello.txt")
	if err := os.WriteFile(src, []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	dst := filepath.Join(dir, "out.zip")
	if err := writeZIP(dst, []archiveFile{
		{DiskPath: src, ArchivePath: "hookaido_v1/hello.txt", Mode: 0o755},
	}); err != nil {
		t.Fatalf("writeZIP: %v", err)
	}

	zr, err := zip.OpenReader(dst)
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	defer zr.Close()
	if len(zr.File) != 1 {
		t.Fatalf("zip files=%d, want 1", len(zr.File))
	}
	if zr.File[0].Name != "hookaido_v1/hello.txt" {
		t.Fatalf("archive path=%q", zr.File[0].Name)
	}
	rc, err := zr.File[0].Open()
	if err != nil {
		t.Fatalf("open zip entry: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read zip entry: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("payload=%q, want hello world", got)
	}
	if zr.File[0].Method != zip.Deflate {
		t.Fatalf("method=%d, want Deflate", zr.File[0].Method)
	}
}

func TestWriteZIP_CreateError(t *testing.T) {
	// Writing to a directory path triggers os.Create failure.
	if err := writeZIP(t.TempDir(), nil); err == nil {
		t.Fatalf("expected error writing zip to directory path")
	}
}

func TestWriteZIP_MissingSourceFile(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "out.zip")
	err := writeZIP(dst, []archiveFile{
		{DiskPath: filepath.Join(t.TempDir(), "missing.txt"), ArchivePath: "x", Mode: 0o644},
	})
	if err == nil {
		t.Fatalf("expected error for missing source file")
	}
	if !strings.Contains(err.Error(), "stat") {
		t.Fatalf("err=%v, want stat-related error", err)
	}
}

// --- writeTarGz / addTarFile ---

func TestWriteTarGz_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "payload.txt")
	if err := os.WriteFile(src, []byte("tar contents"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	dst := filepath.Join(dir, "out.tar.gz")
	if err := writeTarGz(dst, []archiveFile{
		{DiskPath: src, ArchivePath: "hookaido_v1/payload.txt", Mode: 0o644},
	}); err != nil {
		t.Fatalf("writeTarGz: %v", err)
	}

	f, err := os.Open(dst)
	if err != nil {
		t.Fatalf("open tar.gz: %v", err)
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gr.Close()
	tr := tar.NewReader(gr)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next: %v", err)
	}
	if hdr.Name != "hookaido_v1/payload.txt" {
		t.Fatalf("tar name=%q", hdr.Name)
	}
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatalf("read tar entry: %v", err)
	}
	if string(got) != "tar contents" {
		t.Fatalf("payload=%q", got)
	}
	if _, err := tr.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestWriteTarGz_CreateError(t *testing.T) {
	if err := writeTarGz(t.TempDir(), nil); err == nil {
		t.Fatalf("expected error writing tar.gz to directory path")
	}
}

func TestWriteTarGz_MissingSourceFile(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "out.tar.gz")
	err := writeTarGz(dst, []archiveFile{
		{DiskPath: filepath.Join(t.TempDir(), "missing.txt"), ArchivePath: "x", Mode: 0o644},
	})
	if err == nil {
		t.Fatalf("expected error for missing source file")
	}
}

// --- gitOutput ---

func TestGitOutput_InRepo(t *testing.T) {
	out := gitOutput("rev-parse", "--show-toplevel")
	if strings.TrimSpace(out) == "" {
		t.Skipf("not running inside a git repo (or git unavailable)")
	}
}

func TestGitOutput_FailureReturnsEmpty(t *testing.T) {
	out := gitOutput("not-a-real-subcommand-xyz")
	if out != "" {
		// Newer git versions print errors but still exit non-zero, so the
		// helper should swallow the output and return an empty string.
		t.Fatalf("gitOutput on bad subcommand returned %q, want empty", out)
	}
}

// --- firstNonEmpty ---

func TestFirstNonEmpty(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want string
	}{
		{"all_empty", []string{"", "  ", "\t"}, ""},
		{"first_set", []string{"a", "b"}, "a"},
		{"second_set", []string{"", "fallback"}, "fallback"},
		{"trims_whitespace", []string{"  spaced  "}, "spaced"},
		{"no_args", nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := firstNonEmpty(tc.in...); got != tc.want {
				t.Fatalf("firstNonEmpty(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// --- loadEd25519PrivateKey error paths ---

func TestLoadEd25519PrivateKey_MissingFile(t *testing.T) {
	_, err := loadEd25519PrivateKey(filepath.Join(t.TempDir(), "missing.pem"))
	if err == nil {
		t.Fatalf("expected error for missing file")
	}
}

func TestLoadEd25519PrivateKey_NotPEM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-pem.pem")
	if err := os.WriteFile(path, []byte("plain text not PEM"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := loadEd25519PrivateKey(path)
	if err == nil || !strings.Contains(err.Error(), "expected PEM") {
		t.Fatalf("err=%v, want expected-PEM", err)
	}
}

func TestLoadEd25519PrivateKey_WrongPEMType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wrong-type.pem")
	body := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte{0x01, 0x02}})
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := loadEd25519PrivateKey(path)
	if err == nil || !strings.Contains(err.Error(), "unsupported PEM type") {
		t.Fatalf("err=%v, want unsupported-PEM-type", err)
	}
}

func TestLoadEd25519PrivateKey_NotPKCS8(t *testing.T) {
	path := filepath.Join(t.TempDir(), "garbage.pem")
	body := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("not valid PKCS#8 DER")})
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := loadEd25519PrivateKey(path)
	if err == nil || !strings.Contains(err.Error(), "PKCS#8") {
		t.Fatalf("err=%v, want PKCS#8 parse error", err)
	}
}

func TestLoadEd25519PrivateKey_WrongKeyType(t *testing.T) {
	// Generate an RSA key (valid PKCS#8) so the parse succeeds but the type
	// assertion to ed25519.PrivateKey fails.
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(rsaKey)
	if err != nil {
		t.Fatalf("marshal rsa: %v", err)
	}
	path := filepath.Join(t.TempDir(), "rsa.pem")
	body := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err = loadEd25519PrivateKey(path)
	if err == nil || !strings.Contains(err.Error(), "expected Ed25519") {
		t.Fatalf("err=%v, want Ed25519 type error", err)
	}
}

// --- signChecksums error paths ---

func TestSignChecksums_MissingChecksumFile(t *testing.T) {
	dir := t.TempDir()
	keyPath, _ := writeTestSigningKey(t, dir)
	_, _, err := signChecksums(filepath.Join(dir, "missing.txt"), keyPath)
	if err == nil {
		t.Fatalf("expected error for missing checksum file")
	}
}

func TestSignChecksums_BadKey(t *testing.T) {
	dir := t.TempDir()
	checksumPath := filepath.Join(dir, "checksums.txt")
	if err := os.WriteFile(checksumPath, []byte("sum  file\n"), 0o644); err != nil {
		t.Fatalf("write checksums: %v", err)
	}
	_, _, err := signChecksums(checksumPath, filepath.Join(dir, "missing.pem"))
	if err == nil {
		t.Fatalf("expected error for missing key file")
	}
}

// --- sha256File error path ---

func TestSha256File_MissingFile(t *testing.T) {
	if _, err := sha256File(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatalf("expected error for missing file")
	}
}

// --- parseConfig ---

func saveAndRestoreEnv(t *testing.T, keys ...string) {
	t.Helper()
	saved := make(map[string]string, len(keys))
	hadKey := make(map[string]bool, len(keys))
	for _, k := range keys {
		v, ok := os.LookupEnv(k)
		saved[k] = v
		hadKey[k] = ok
		_ = os.Unsetenv(k)
	}
	t.Cleanup(func() {
		for _, k := range keys {
			if hadKey[k] {
				_ = os.Setenv(k, saved[k])
			} else {
				_ = os.Unsetenv(k)
			}
		}
	})
}

func saveAndRestoreArgs(t *testing.T, args []string) {
	t.Helper()
	prev := os.Args
	os.Args = args
	t.Cleanup(func() { os.Args = prev })
}

func TestParseConfig_DefaultsViaEnv(t *testing.T) {
	saveAndRestoreEnv(t,
		"HOOKAIDO_VERSION", "HOOKAIDO_COMMIT", "HOOKAIDO_BUILD_DATE",
		"HOOKAIDO_SIGNING_KEY_FILE",
	)
	_ = os.Setenv("HOOKAIDO_VERSION", "1.2.3-test")
	_ = os.Setenv("HOOKAIDO_COMMIT", "deadbeef")
	_ = os.Setenv("HOOKAIDO_BUILD_DATE", "2026-05-01T00:00:00Z")
	_ = os.Setenv("HOOKAIDO_SIGNING_KEY_FILE", "/tmp/key.pem")

	saveAndRestoreArgs(t, []string{"release"})
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Version != "1.2.3-test" || cfg.Commit != "deadbeef" || cfg.BuildDate != "2026-05-01T00:00:00Z" {
		t.Fatalf("env not honored: %#v", cfg)
	}
	if cfg.SigningKeyFile != "/tmp/key.pem" {
		t.Fatalf("signing key from env=%q", cfg.SigningKeyFile)
	}
	if cfg.OutDir != "dist" {
		t.Fatalf("out dir=%q, want dist", cfg.OutDir)
	}
	if len(cfg.Targets) != len(defaultTargets) {
		t.Fatalf("default targets count=%d", len(cfg.Targets))
	}
	if cfg.RepoRoot == "" {
		t.Fatalf("RepoRoot should be detected")
	}
}

func TestParseConfig_FlagsOverrideEnv(t *testing.T) {
	saveAndRestoreEnv(t, "HOOKAIDO_VERSION", "HOOKAIDO_COMMIT", "HOOKAIDO_BUILD_DATE", "HOOKAIDO_SIGNING_KEY_FILE")
	_ = os.Setenv("HOOKAIDO_VERSION", "env-version")

	saveAndRestoreArgs(t, []string{
		"release",
		"--version", "flag-version",
		"--commit", "flag-commit",
		"--date", "2026-05-01T12:00:00Z",
		"--out", "custom-dist",
		"--targets", "linux/amd64",
	})
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Version != "flag-version" {
		t.Fatalf("flags should override env, got %q", cfg.Version)
	}
	if cfg.Commit != "flag-commit" {
		t.Fatalf("commit=%q", cfg.Commit)
	}
	if cfg.OutDir != "custom-dist" {
		t.Fatalf("out=%q", cfg.OutDir)
	}
	if len(cfg.Targets) != 1 || cfg.Targets[0].GOOS != "linux" {
		t.Fatalf("targets=%v", cfg.Targets)
	}
}

func TestParseConfig_PositionalArgsRejected(t *testing.T) {
	saveAndRestoreEnv(t, "HOOKAIDO_VERSION", "HOOKAIDO_COMMIT", "HOOKAIDO_BUILD_DATE", "HOOKAIDO_SIGNING_KEY_FILE")
	saveAndRestoreArgs(t, []string{"release", "stray"})
	if _, err := parseConfig(); err == nil {
		t.Fatalf("expected error for positional args")
	}
}

func TestParseConfig_EmptyOutDirRejected(t *testing.T) {
	saveAndRestoreEnv(t, "HOOKAIDO_VERSION", "HOOKAIDO_COMMIT", "HOOKAIDO_BUILD_DATE", "HOOKAIDO_SIGNING_KEY_FILE")
	saveAndRestoreArgs(t, []string{"release", "--out", "   "})
	if _, err := parseConfig(); err == nil || !strings.Contains(err.Error(), "out directory") {
		t.Fatalf("err=%v, want empty-out-dir error", err)
	}
}

func TestParseConfig_InvalidFlag(t *testing.T) {
	saveAndRestoreEnv(t, "HOOKAIDO_VERSION", "HOOKAIDO_COMMIT", "HOOKAIDO_BUILD_DATE", "HOOKAIDO_SIGNING_KEY_FILE")
	saveAndRestoreArgs(t, []string{"release", "--no-such-flag"})
	if _, err := parseConfig(); err == nil {
		t.Fatalf("expected error for unknown flag")
	}
}

func TestParseConfig_InvalidTargets(t *testing.T) {
	saveAndRestoreEnv(t, "HOOKAIDO_VERSION", "HOOKAIDO_COMMIT", "HOOKAIDO_BUILD_DATE", "HOOKAIDO_SIGNING_KEY_FILE")
	saveAndRestoreArgs(t, []string{"release", "--targets", "linux"})
	if _, err := parseConfig(); err == nil {
		t.Fatalf("expected error for malformed target")
	}
}

// --- run() integration test ---

func TestRun_HappyPath_SingleTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping go-build integration test in -short mode")
	}
	if _, err := os.Stat(filepath.Join("..", "..", "..", "go.mod")); err != nil {
		t.Skipf("not running from repo (go.mod not at expected relative path): %v", err)
	}

	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("abs repo root: %v", err)
	}
	outDir := t.TempDir()

	// Sign too, so signChecksums + writeManifest signing branch is covered.
	keyPath, _ := writeTestSigningKey(t, t.TempDir())

	cfg := releaseConfig{
		Version:        "v0.0.0-coverage",
		Commit:         "coveragetest",
		BuildDate:      "2026-05-01T00:00:00Z",
		OutDir:         outDir,
		RepoRoot:       repoRoot,
		Targets:        []buildTarget{{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH}},
		SigningKeyFile: keyPath,
	}
	if err := run(cfg); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Verify expected outputs exist.
	wantSubstrings := []string{
		"hookaido_v0.0.0-coverage_" + runtime.GOOS + "_" + runtime.GOARCH,
		"hookaido_v0.0.0-coverage_sbom.spdx.json",
		"hookaido_v0.0.0-coverage_checksums.txt",
		"hookaido_v0.0.0-coverage_checksums.txt.sig",
		"hookaido_v0.0.0-coverage_checksums.txt.pub.pem",
		"hookaido_v0.0.0-coverage_manifest.json",
	}
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("read out dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	joined := strings.Join(names, "\n")
	for _, want := range wantSubstrings {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in dist outputs:\n%s", want, joined)
		}
	}

	// Manifest should be valid JSON with our metadata.
	manifestPath := filepath.Join(outDir, "hookaido_v0.0.0-coverage_manifest.json")
	body, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m releaseManifest
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if m.Version != cfg.Version || m.Commit != cfg.Commit {
		t.Fatalf("manifest metadata mismatch: %#v", m)
	}
	if m.Checksums.SignatureFile == "" || m.Checksums.SignatureAlgorithm != "ed25519" {
		t.Fatalf("manifest signing fields missing: %#v", m.Checksums)
	}
	// We expect the host-platform archive plus the SBOM in the artifact list.
	if len(m.Artifacts) != 2 {
		t.Fatalf("artifacts=%d, want 2 (archive + sbom)", len(m.Artifacts))
	}
}

func TestRun_BuildFailureSurfaced(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping go-build integration test in -short mode")
	}
	// Pointing RepoRoot at a directory without go.mod makes `go build`
	// fail; run() should propagate the error.
	cfg := releaseConfig{
		Version:   "v0.0.0-coverage-fail",
		Commit:    "x",
		BuildDate: "2026-05-01T00:00:00Z",
		OutDir:    t.TempDir(),
		RepoRoot:  t.TempDir(), // empty dir, no Go module
		Targets:   []buildTarget{{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH}},
	}
	if err := run(cfg); err == nil {
		t.Fatalf("expected build error")
	}
}

// Compile-time check that ed25519 is referenced (used by writeTestSigningKey
// in the existing test file). Otherwise some build configurations might
// flag unused imports for our additions.
var _ = ed25519.PublicKey(nil)
