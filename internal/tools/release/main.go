package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"hookaido/internal/release/sbom"
)

type buildTarget struct {
	GOOS   string
	GOARCH string
}

var defaultTargets = []buildTarget{
	{GOOS: "linux", GOARCH: "amd64"},
	{GOOS: "linux", GOARCH: "arm64"},
	{GOOS: "darwin", GOARCH: "amd64"},
	{GOOS: "darwin", GOARCH: "arm64"},
	{GOOS: "windows", GOARCH: "amd64"},
	{GOOS: "windows", GOARCH: "arm64"},
}

type releaseConfig struct {
	Version        string
	Commit         string
	BuildDate      string
	OutDir         string
	RepoRoot       string
	Targets        []buildTarget
	SigningKeyFile string
}

func main() {
	cfg, err := parseConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "release: %v\n", err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "release: %v\n", err)
		os.Exit(1)
	}
}

func parseConfig() (releaseConfig, error) {
	var cfg releaseConfig
	var targetsRaw string

	fs := flag.NewFlagSet("release", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.Version, "version", "", "release version (defaults: HOOKAIDO_VERSION or git describe)")
	fs.StringVar(&cfg.Commit, "commit", "", "commit SHA/identifier (defaults: HOOKAIDO_COMMIT or git rev-parse --short HEAD)")
	fs.StringVar(&cfg.BuildDate, "date", "", "build date RFC3339 (defaults: HOOKAIDO_BUILD_DATE or current UTC time)")
	fs.StringVar(&cfg.OutDir, "out", "dist", "output directory for archives/checksums")
	fs.StringVar(&cfg.SigningKeyFile, "signing-key", "", "path to Ed25519 PKCS#8 PEM private key for checksum signing (optional)")
	fs.StringVar(&targetsRaw, "targets", "", "comma separated os/arch list (default matrix)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return releaseConfig{}, err
	}
	if fs.NArg() != 0 {
		return releaseConfig{}, errors.New("unexpected positional arguments")
	}

	cfg.Version = firstNonEmpty(
		cfg.Version,
		os.Getenv("HOOKAIDO_VERSION"),
		strings.TrimSpace(gitOutput("describe", "--tags", "--always", "--dirty")),
		"0.0.0-dev",
	)
	cfg.Commit = firstNonEmpty(
		cfg.Commit,
		os.Getenv("HOOKAIDO_COMMIT"),
		strings.TrimSpace(gitOutput("rev-parse", "--short", "HEAD")),
		"unknown",
	)
	cfg.BuildDate = firstNonEmpty(
		cfg.BuildDate,
		os.Getenv("HOOKAIDO_BUILD_DATE"),
		time.Now().UTC().Format(time.RFC3339),
	)
	cfg.OutDir = strings.TrimSpace(cfg.OutDir)
	if cfg.OutDir == "" {
		return releaseConfig{}, errors.New("out directory must not be empty")
	}
	cfg.SigningKeyFile = firstNonEmpty(cfg.SigningKeyFile, os.Getenv("HOOKAIDO_SIGNING_KEY_FILE"))
	cfg.SigningKeyFile = strings.TrimSpace(cfg.SigningKeyFile)

	targets, err := parseTargets(targetsRaw)
	if err != nil {
		return releaseConfig{}, err
	}
	cfg.Targets = targets

	root, err := sbom.DetectRepoRoot()
	if err != nil {
		return releaseConfig{}, err
	}
	cfg.RepoRoot = root
	return cfg, nil
}

func run(cfg releaseConfig) error {
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return fmt.Errorf("create out dir: %w", err)
	}

	safeVersion := safeFileNameFragment(cfg.Version)
	artifacts := make([]string, 0, len(cfg.Targets))
	for _, target := range cfg.Targets {
		archiveName := artifactFileName(safeVersion, target)
		archivePath := filepath.Join(cfg.OutDir, archiveName)
		if err := buildAndPackageTarget(cfg, target, archivePath, safeVersion); err != nil {
			return err
		}
		artifacts = append(artifacts, archivePath)
	}
	sbomPath := filepath.Join(cfg.OutDir, fmt.Sprintf("hookaido_%s_sbom.spdx.json", safeVersion))
	if err := sbom.Generate(sbom.Options{
		Name:      "hookaido",
		Version:   cfg.Version,
		Commit:    cfg.Commit,
		BuildDate: cfg.BuildDate,
		RepoRoot:  cfg.RepoRoot,
		OutPath:   sbomPath,
	}); err != nil {
		return fmt.Errorf("generate sbom: %w", err)
	}
	artifacts = append(artifacts, sbomPath)

	sort.Strings(artifacts)
	checksums, err := buildChecksums(artifacts)
	if err != nil {
		return err
	}

	checksumPath := filepath.Join(cfg.OutDir, fmt.Sprintf("hookaido_%s_checksums.txt", safeVersion))
	if err := writeChecksums(checksumPath, checksums); err != nil {
		return err
	}
	artifactOutputs := []string{checksumPath}
	signaturePath := ""
	publicKeyPath := ""
	if cfg.SigningKeyFile != "" {
		signaturePath, publicKeyPath, err = signChecksums(checksumPath, cfg.SigningKeyFile)
		if err != nil {
			return err
		}
		artifactOutputs = append(artifactOutputs, signaturePath, publicKeyPath)
	}

	manifestPath := filepath.Join(cfg.OutDir, fmt.Sprintf("hookaido_%s_manifest.json", safeVersion))
	if err := writeManifest(manifestPath, cfg, checksums, checksumPath, signaturePath, publicKeyPath); err != nil {
		return err
	}
	artifactOutputs = append(artifactOutputs, manifestPath)

	fmt.Printf("version=%s commit=%s build_date=%s\n", cfg.Version, cfg.Commit, cfg.BuildDate)
	for _, artifact := range artifacts {
		fmt.Println(filepath.ToSlash(artifact))
	}
	for _, artifact := range artifactOutputs {
		fmt.Println(filepath.ToSlash(artifact))
	}
	return nil
}

func buildAndPackageTarget(cfg releaseConfig, target buildTarget, archivePath, safeVersion string) error {
	tmpDir, err := os.MkdirTemp("", "hookaido-release-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	binName := "hookaido"
	binMode := os.FileMode(0o755)
	if target.GOOS == "windows" {
		binName += ".exe"
		binMode = 0o644
	}
	binPath := filepath.Join(tmpDir, binName)
	ldflags := fmt.Sprintf(
		"-X hookaido/internal/app.version=%s -X hookaido/internal/app.commit=%s -X hookaido/internal/app.buildDate=%s",
		cfg.Version,
		cfg.Commit,
		cfg.BuildDate,
	)
	cmd := exec.Command("go", "build", "-trimpath", "-ldflags", ldflags, "-o", binPath, "./cmd/hookaido")
	cmd.Dir = cfg.RepoRoot
	cmd.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS="+target.GOOS,
		"GOARCH="+target.GOARCH,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("go build %s/%s: %w\n%s", target.GOOS, target.GOARCH, err, strings.TrimSpace(string(out)))
	}

	rootName := fmt.Sprintf("hookaido_%s_%s_%s", safeVersion, target.GOOS, target.GOARCH)
	files := []archiveFile{
		{
			DiskPath:    binPath,
			ArchivePath: path.Join(rootName, binName),
			Mode:        binMode,
		},
		{
			DiskPath:    filepath.Join(cfg.RepoRoot, "LICENSE"),
			ArchivePath: path.Join(rootName, "LICENSE"),
			Mode:        0o644,
		},
		{
			DiskPath:    filepath.Join(cfg.RepoRoot, "README.md"),
			ArchivePath: path.Join(rootName, "README.md"),
			Mode:        0o644,
		},
		{
			DiskPath:    filepath.Join(cfg.RepoRoot, "CHANGELOG.md"),
			ArchivePath: path.Join(rootName, "CHANGELOG.md"),
			Mode:        0o644,
		},
	}

	if strings.HasSuffix(archivePath, ".zip") {
		if err := writeZIP(archivePath, files); err != nil {
			return err
		}
		return nil
	}
	if err := writeTarGz(archivePath, files); err != nil {
		return err
	}
	return nil
}

type archiveFile struct {
	DiskPath    string
	ArchivePath string
	Mode        os.FileMode
}

func writeZIP(dst string, files []archiveFile) error {
	f, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create archive %q: %w", dst, err)
	}
	defer f.Close()

	zw := zip.NewWriter(f)
	for _, file := range files {
		if err := addZIPFile(zw, file); err != nil {
			_ = zw.Close()
			return err
		}
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("close zip %q: %w", dst, err)
	}
	return nil
}

func addZIPFile(zw *zip.Writer, file archiveFile) error {
	info, err := os.Stat(file.DiskPath)
	if err != nil {
		return fmt.Errorf("stat %q: %w", file.DiskPath, err)
	}
	hdr, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("zip header %q: %w", file.DiskPath, err)
	}
	hdr.Name = filepath.ToSlash(file.ArchivePath)
	hdr.Method = zip.Deflate
	hdr.SetMode(file.Mode)
	w, err := zw.CreateHeader(hdr)
	if err != nil {
		return fmt.Errorf("zip create %q: %w", file.ArchivePath, err)
	}

	in, err := os.Open(file.DiskPath)
	if err != nil {
		return fmt.Errorf("open %q: %w", file.DiskPath, err)
	}
	defer in.Close()
	if _, err := io.Copy(w, in); err != nil {
		return fmt.Errorf("zip write %q: %w", file.ArchivePath, err)
	}
	return nil
}

func writeTarGz(dst string, files []archiveFile) error {
	f, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create archive %q: %w", dst, err)
	}
	defer f.Close()

	gw := gzip.NewWriter(f)
	tw := tar.NewWriter(gw)
	for _, file := range files {
		if err := addTarFile(tw, file); err != nil {
			_ = tw.Close()
			_ = gw.Close()
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("close tar %q: %w", dst, err)
	}
	if err := gw.Close(); err != nil {
		return fmt.Errorf("close gzip %q: %w", dst, err)
	}
	return nil
}

func addTarFile(tw *tar.Writer, file archiveFile) error {
	info, err := os.Stat(file.DiskPath)
	if err != nil {
		return fmt.Errorf("stat %q: %w", file.DiskPath, err)
	}
	hdr := &tar.Header{
		Name:    filepath.ToSlash(file.ArchivePath),
		Mode:    int64(file.Mode.Perm()),
		Size:    info.Size(),
		ModTime: time.Now().UTC(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar header %q: %w", file.ArchivePath, err)
	}
	in, err := os.Open(file.DiskPath)
	if err != nil {
		return fmt.Errorf("open %q: %w", file.DiskPath, err)
	}
	defer in.Close()
	if _, err := io.Copy(tw, in); err != nil {
		return fmt.Errorf("tar write %q: %w", file.ArchivePath, err)
	}
	return nil
}

type checksumEntry struct {
	File string
	Sum  string
}

func buildChecksums(files []string) ([]checksumEntry, error) {
	out := make([]checksumEntry, 0, len(files))
	for _, file := range files {
		sum, err := sha256File(file)
		if err != nil {
			return nil, err
		}
		out = append(out, checksumEntry{File: file, Sum: sum})
	}
	return out, nil
}

func writeChecksums(dst string, checksums []checksumEntry) error {
	var b strings.Builder
	for _, entry := range checksums {
		fmt.Fprintf(&b, "%s  %s\n", entry.Sum, filepath.Base(entry.File))
	}
	if err := os.WriteFile(dst, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("write checksums: %w", err)
	}
	return nil
}

func signChecksums(checksumPath, keyPath string) (string, string, error) {
	privateKey, err := loadEd25519PrivateKey(keyPath)
	if err != nil {
		return "", "", err
	}
	checksumData, err := os.ReadFile(checksumPath)
	if err != nil {
		return "", "", fmt.Errorf("read checksums %q: %w", checksumPath, err)
	}
	signature := ed25519.Sign(privateKey, checksumData)
	signaturePath := checksumPath + ".sig"
	signatureBody := base64.StdEncoding.EncodeToString(signature) + "\n"
	if err := os.WriteFile(signaturePath, []byte(signatureBody), 0o644); err != nil {
		return "", "", fmt.Errorf("write checksum signature: %w", err)
	}

	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok {
		return "", "", errors.New("derive public key: unexpected key type")
	}
	pubDER, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", "", fmt.Errorf("marshal public key: %w", err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	publicKeyPath := checksumPath + ".pub.pem"
	if err := os.WriteFile(publicKeyPath, pubPEM, 0o644); err != nil {
		return "", "", fmt.Errorf("write public key: %w", err)
	}
	return signaturePath, publicKeyPath, nil
}

func loadEd25519PrivateKey(p string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("read signing key %q: %w", p, err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("parse signing key %q: expected PEM", p)
	}
	if block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("parse signing key %q: unsupported PEM type %q", p, block.Type)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse signing key %q as PKCS#8: %w", p, err)
	}
	privateKey, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("parse signing key %q: expected Ed25519 private key", p)
	}
	return privateKey, nil
}

type releaseManifest struct {
	Schema      string             `json:"schema"`
	GeneratedAt string             `json:"generated_at"`
	Version     string             `json:"version"`
	Commit      string             `json:"commit"`
	BuildDate   string             `json:"build_date"`
	Artifacts   []manifestArtifact `json:"artifacts"`
	Checksums   manifestChecksums  `json:"checksums"`
}

type manifestArtifact struct {
	File   string `json:"file"`
	SHA256 string `json:"sha256"`
}

type manifestChecksums struct {
	File               string `json:"file"`
	SHA256             string `json:"sha256"`
	SignatureFile      string `json:"signature_file,omitempty"`
	PublicKeyFile      string `json:"public_key_file,omitempty"`
	SignatureAlgorithm string `json:"signature_algorithm,omitempty"`
}

func writeManifest(
	dst string,
	cfg releaseConfig,
	artifactChecksums []checksumEntry,
	checksumPath string,
	signaturePath string,
	publicKeyPath string,
) error {
	checksumSum, err := sha256File(checksumPath)
	if err != nil {
		return err
	}
	artifacts := make([]manifestArtifact, 0, len(artifactChecksums))
	for _, entry := range artifactChecksums {
		artifacts = append(artifacts, manifestArtifact{
			File:   filepath.Base(entry.File),
			SHA256: entry.Sum,
		})
	}
	manifest := releaseManifest{
		Schema:      "hookaido.release-manifest.v1",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Version:     cfg.Version,
		Commit:      cfg.Commit,
		BuildDate:   cfg.BuildDate,
		Artifacts:   artifacts,
		Checksums: manifestChecksums{
			File:   filepath.Base(checksumPath),
			SHA256: checksumSum,
		},
	}
	if signaturePath != "" {
		manifest.Checksums.SignatureFile = filepath.Base(signaturePath)
		manifest.Checksums.PublicKeyFile = filepath.Base(publicKeyPath)
		manifest.Checksums.SignatureAlgorithm = "ed25519"
	}
	body, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	body = append(body, '\n')
	if err := os.WriteFile(dst, body, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

func sha256File(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", fmt.Errorf("open checksum file %q: %w", p, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash file %q: %w", p, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func parseTargets(raw string) ([]buildTarget, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		out := make([]buildTarget, len(defaultTargets))
		copy(out, defaultTargets)
		return out, nil
	}

	parts := strings.Split(raw, ",")
	out := make([]buildTarget, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		pair := strings.Split(part, "/")
		if len(pair) != 2 {
			return nil, fmt.Errorf("invalid target %q (expected os/arch)", part)
		}
		goos := strings.TrimSpace(pair[0])
		goarch := strings.TrimSpace(pair[1])
		if goos == "" || goarch == "" {
			return nil, fmt.Errorf("invalid target %q (empty os/arch)", part)
		}
		key := goos + "/" + goarch
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, buildTarget{GOOS: goos, GOARCH: goarch})
	}
	if len(out) == 0 {
		return nil, errors.New("at least one build target is required")
	}
	return out, nil
}

func artifactFileName(safeVersion string, target buildTarget) string {
	base := fmt.Sprintf("hookaido_%s_%s_%s", safeVersion, target.GOOS, target.GOARCH)
	if target.GOOS == "windows" {
		return base + ".zip"
	}
	return base + ".tar.gz"
}

func safeFileNameFragment(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "0.0.0-dev"
	}
	var b strings.Builder
	b.Grow(len(v))
	for _, r := range v {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '.' || r == '_' || r == '-' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "0.0.0-dev"
	}
	return out
}

func gitOutput(args ...string) string {
	cmd := exec.Command("git", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return ""
	}
	return string(out)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}
