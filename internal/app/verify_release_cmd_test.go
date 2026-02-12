package app

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyReleaseCmd_UnsignedSuccess(t *testing.T) {
	dir := t.TempDir()
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, map[string]string{
		"hookaido_v1_linux_amd64.tar.gz":  "artifact-linux",
		"hookaido_v1_windows_amd64.zip":   "artifact-windows",
		"hookaido_v1_darwin_amd64.tar.gz": "artifact-darwin",
	})

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{"--checksums", checksumsPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d; stderr=%q", code, stderr.String())
	}
	if got := stdout.String(); !strings.Contains(got, "verified 3 artifact(s)") {
		t.Fatalf("unexpected stdout: %q", got)
	}
	if got := stdout.String(); !strings.Contains(got, "sbom: not verified") {
		t.Fatalf("expected sbom note in stdout: %q", got)
	}
	if got := stdout.String(); !strings.Contains(got, "signature: not verified") {
		t.Fatalf("expected unsigned note in stdout: %q", got)
	}
	if got := strings.TrimSpace(stderr.String()); got != "" {
		t.Fatalf("expected empty stderr, got %q", got)
	}
}

func TestVerifyReleaseCmd_SignedSuccessWithAutoDetect(t *testing.T) {
	dir := t.TempDir()
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
		"hookaido_v1_sbom.spdx.json":     validSBOMDocument(),
	})
	signChecksumsForTest(t, checksumsPath)

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{"--checksums", checksumsPath, "--require-signature", "--json"}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d; stderr=%q", code, stderr.String())
	}
	var out verifyReleaseResult
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if !out.SignatureVerified {
		t.Fatalf("expected signature verification to be true")
	}
	if out.SignatureAlgorithm != "ed25519" {
		t.Fatalf("unexpected signature algorithm: %q", out.SignatureAlgorithm)
	}
	if out.ArtifactCount != 2 {
		t.Fatalf("unexpected artifact count: %d", out.ArtifactCount)
	}
	if !out.SBOMVerified {
		t.Fatalf("expected sbom verification to be true")
	}
	if out.SBOMFile == "" {
		t.Fatalf("expected sbom file in output")
	}
	if got := strings.TrimSpace(stderr.String()); got != "" {
		t.Fatalf("expected empty stderr, got %q", got)
	}
}

func TestVerifyReleaseCmd_ChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
	})
	artifactPath := filepath.Join(dir, "hookaido_v1_linux_amd64.tar.gz")
	if err := os.WriteFile(artifactPath, []byte("tampered"), 0o644); err != nil {
		t.Fatalf("tamper artifact: %v", err)
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{"--checksums", checksumsPath}, stdout, stderr)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if got := strings.TrimSpace(stdout.String()); got != "" {
		t.Fatalf("expected empty stdout, got %q", got)
	}
	if got := stderr.String(); !strings.Contains(got, "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got %q", got)
	}
}

func TestVerifyReleaseCmd_RequireSignatureMissing(t *testing.T) {
	dir := t.TempDir()
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
	})

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{"--checksums", checksumsPath, "--require-signature"}, stdout, stderr)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if got := stderr.String(); !strings.Contains(got, "signature required") {
		t.Fatalf("expected signature required error, got %q", got)
	}
}

func TestVerifyReleaseCmd_RequireSBOMMissing(t *testing.T) {
	dir := t.TempDir()
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
	})

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{"--checksums", checksumsPath, "--require-sbom"}, stdout, stderr)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if got := strings.TrimSpace(stdout.String()); got != "" {
		t.Fatalf("expected empty stdout, got %q", got)
	}
	if got := stderr.String(); !strings.Contains(got, "sbom required") {
		t.Fatalf("expected sbom required error, got %q", got)
	}
}

func TestVerifyReleaseCmd_RequireSBOMInvalid(t *testing.T) {
	dir := t.TempDir()
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
		"hookaido_v1_sbom.spdx.json": `{
  "spdxVersion": "SPDX-2.3",
  "dataLicense": "CC0-1.0",
  "documentNamespace": "https://example.test/spdx/hookaido/v1",
  "packages": [],
  "documentDescribes": []
}`,
	})

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{"--checksums", checksumsPath, "--require-sbom"}, stdout, stderr)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if got := strings.TrimSpace(stdout.String()); got != "" {
		t.Fatalf("expected empty stdout, got %q", got)
	}
	if got := stderr.String(); !strings.Contains(got, "no packages found") {
		t.Fatalf("expected sbom validation error, got %q", got)
	}
}

func TestVerifyReleaseCmd_RequireSBOMValid(t *testing.T) {
	dir := t.TempDir()
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
		"hookaido_v1_sbom.spdx.json":     validSBOMDocument(),
	})

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{"--checksums", checksumsPath, "--require-sbom", "--json"}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d; stderr=%q", code, stderr.String())
	}
	var out verifyReleaseResult
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if !out.SBOMVerified {
		t.Fatalf("expected sbom verification to be true")
	}
	if !strings.HasSuffix(out.SBOMFile, "hookaido_v1_sbom.spdx.json") {
		t.Fatalf("unexpected sbom file: %q", out.SBOMFile)
	}
	if got := strings.TrimSpace(stderr.String()); got != "" {
		t.Fatalf("expected empty stderr, got %q", got)
	}
}

func TestVerifyReleaseCmd_BadArgs(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{"positional"}, stdout, stderr)
	if code != 2 {
		t.Fatalf("expected exit code 2, got %d", code)
	}
	if got := stderr.String(); !strings.Contains(got, "unexpected positional arguments") {
		t.Fatalf("expected positional argument error, got %q", got)
	}
}

func writeReleaseArtifactsWithChecksums(t *testing.T, dir string, artifacts map[string]string) string {
	t.Helper()
	var checksums strings.Builder
	for name, body := range artifacts {
		artifactPath := filepath.Join(dir, name)
		if err := os.WriteFile(artifactPath, []byte(body), 0o644); err != nil {
			t.Fatalf("write artifact %q: %v", name, err)
		}
		sum := sha256.Sum256([]byte(body))
		fmt.Fprintf(&checksums, "%s  %s\n", hex.EncodeToString(sum[:]), name)
	}
	checksumsPath := filepath.Join(dir, "hookaido_v1_checksums.txt")
	if err := os.WriteFile(checksumsPath, []byte(checksums.String()), 0o644); err != nil {
		t.Fatalf("write checksums: %v", err)
	}
	return checksumsPath
}

func validSBOMDocument() string {
	return `{
  "spdxVersion": "SPDX-2.3",
  "dataLicense": "CC0-1.0",
  "SPDXID": "SPDXRef-DOCUMENT",
  "name": "hookaido-v1-sbom",
  "documentNamespace": "https://example.test/spdx/hookaido/v1",
  "documentDescribes": ["SPDXRef-Package-hookaido"],
  "packages": [
    {
      "name": "hookaido",
      "SPDXID": "SPDXRef-Package-hookaido",
      "downloadLocation": "NONE",
      "filesAnalyzed": false,
      "licenseConcluded": "NOASSERTION",
      "licenseDeclared": "NOASSERTION",
      "primaryPackagePurpose": "APPLICATION"
    }
  ]
}`
}

func signChecksumsForTest(t *testing.T, checksumsPath string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	checksumBody, err := os.ReadFile(checksumsPath)
	if err != nil {
		t.Fatalf("read checksums: %v", err)
	}
	signature := ed25519.Sign(privateKey, checksumBody)
	signaturePath := checksumsPath + ".sig"
	if err := os.WriteFile(signaturePath, []byte(base64.StdEncoding.EncodeToString(signature)+"\n"), 0o644); err != nil {
		t.Fatalf("write signature: %v", err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	publicPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})
	publicPath := checksumsPath + ".pub.pem"
	if err := os.WriteFile(publicPath, publicPEM, 0o644); err != nil {
		t.Fatalf("write public key: %v", err)
	}
}

// buildAttestationBundle creates a minimal Sigstore DSSE attestation bundle JSON.
func buildAttestationBundle(t *testing.T, predicateType string, subjects []intotoSubject) string {
	t.Helper()
	stmt := intotoStatement{
		Type:          "https://in-toto.io/Statement/v1",
		PredicateType: predicateType,
		Subject:       subjects,
	}
	stmtJSON, err := json.Marshal(stmt)
	if err != nil {
		t.Fatalf("marshal in-toto statement: %v", err)
	}
	bundle := sigstoreBundle{
		MediaType: "application/vnd.dev.sigstore.bundle.v0.3+json",
		DSSEEnvelope: &dsseEnvelope{
			PayloadType: "application/vnd.in-toto+json",
			Payload:     base64.StdEncoding.EncodeToString(stmtJSON),
		},
	}
	out, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal bundle: %v", err)
	}
	return string(out)
}

func writeAttestationBundle(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write attestation bundle %q: %v", name, err)
	}
	return p
}

func TestVerifyReleaseCmd_ProvenanceAttestationSuccess(t *testing.T) {
	dir := t.TempDir()
	artifacts := map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
	}
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, artifacts)

	// Build provenance bundle with subjects matching the checksums.
	var subjects []intotoSubject
	for name, body := range artifacts {
		sum := sha256.Sum256([]byte(body))
		subjects = append(subjects, intotoSubject{
			Name:   name,
			Digest: map[string]string{"sha256": hex.EncodeToString(sum[:])},
		})
	}
	bundleJSON := buildAttestationBundle(t, provenancePredicateType, subjects)
	writeAttestationBundle(t, dir, "hookaido_v1_provenance.attestation.json", bundleJSON)

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{
		"--checksums", checksumsPath,
		"--require-provenance",
		"--json",
	}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d; stderr=%q", code, stderr.String())
	}
	var out verifyReleaseResult
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if !out.ProvenanceVerified {
		t.Fatal("expected provenance_verified=true")
	}
	if !strings.HasSuffix(out.ProvenanceFile, "_provenance.attestation.json") {
		t.Fatalf("unexpected provenance file: %q", out.ProvenanceFile)
	}
}

func TestVerifyReleaseCmd_ProvenanceAutoDetectBoth(t *testing.T) {
	dir := t.TempDir()
	artifacts := map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
	}
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, artifacts)

	var subjects []intotoSubject
	for name, body := range artifacts {
		sum := sha256.Sum256([]byte(body))
		subjects = append(subjects, intotoSubject{
			Name:   name,
			Digest: map[string]string{"sha256": hex.EncodeToString(sum[:])},
		})
	}

	// Write both legacy and .intoto attestation bundles.
	writeAttestationBundle(t, dir, "hookaido_v1_provenance.attestation.json",
		buildAttestationBundle(t, provenancePredicateType, subjects))
	writeAttestationBundle(t, dir, "hookaido_v1_provenance.intoto.jsonl",
		buildAttestationBundle(t, provenancePredicateType, subjects))
	writeAttestationBundle(t, dir, "hookaido_v1_sbom.attestation.json",
		buildAttestationBundle(t, sbomAttestPredicateType, subjects))
	writeAttestationBundle(t, dir, "hookaido_v1_sbom.intoto.jsonl",
		buildAttestationBundle(t, sbomAttestPredicateType, subjects))

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{
		"--checksums", checksumsPath,
		"--require-provenance",
		"--json",
	}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d; stderr=%q", code, stderr.String())
	}
	var out verifyReleaseResult
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if !out.ProvenanceVerified {
		t.Fatal("expected provenance_verified=true")
	}
	if !strings.HasSuffix(out.ProvenanceFile, "_provenance.intoto.jsonl") {
		t.Fatalf("expected .intoto provenance preference, got %q", out.ProvenanceFile)
	}
	if !out.SBOMAttestVerified {
		t.Fatal("expected sbom_attest_verified=true")
	}
	if !strings.HasSuffix(out.SBOMAttestFile, "_sbom.intoto.jsonl") {
		t.Fatalf("expected .intoto sbom attestation preference, got %q", out.SBOMAttestFile)
	}
}

func TestVerifyReleaseCmd_RequireProvenanceMissing(t *testing.T) {
	dir := t.TempDir()
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
	})

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{
		"--checksums", checksumsPath,
		"--require-provenance",
	}, stdout, stderr)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if got := stderr.String(); !strings.Contains(got, "provenance attestation required") {
		t.Fatalf("expected provenance required error, got %q", got)
	}
}

func TestVerifyReleaseCmd_ProvenanceSubjectMismatch(t *testing.T) {
	dir := t.TempDir()
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
	})

	// Bundle with a subject that doesn't match any checksum.
	subjects := []intotoSubject{
		{
			Name:   "hookaido_v1_linux_amd64.tar.gz",
			Digest: map[string]string{"sha256": "0000000000000000000000000000000000000000000000000000000000000000"},
		},
	}
	writeAttestationBundle(t, dir, "hookaido_v1_provenance.attestation.json",
		buildAttestationBundle(t, provenancePredicateType, subjects))

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{
		"--checksums", checksumsPath,
		"--require-provenance",
	}, stdout, stderr)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if got := stderr.String(); !strings.Contains(got, "not found in checksums") {
		t.Fatalf("expected subject mismatch error, got %q", got)
	}
}

func TestVerifyReleaseCmd_ProvenanceWrongPredicateType(t *testing.T) {
	dir := t.TempDir()
	artifacts := map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
	}
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, artifacts)

	var subjects []intotoSubject
	for name, body := range artifacts {
		sum := sha256.Sum256([]byte(body))
		subjects = append(subjects, intotoSubject{
			Name:   name,
			Digest: map[string]string{"sha256": hex.EncodeToString(sum[:])},
		})
	}
	// Use wrong predicate type.
	writeAttestationBundle(t, dir, "hookaido_v1_provenance.attestation.json",
		buildAttestationBundle(t, "https://wrong.dev/predicate/v1", subjects))

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{
		"--checksums", checksumsPath,
		"--require-provenance",
	}, stdout, stderr)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if got := stderr.String(); !strings.Contains(got, "predicateType") {
		t.Fatalf("expected predicateType error, got %q", got)
	}
}

func TestVerifyReleaseCmd_ProvenanceInvalidBundle(t *testing.T) {
	dir := t.TempDir()
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
	})
	writeAttestationBundle(t, dir, "hookaido_v1_provenance.attestation.json", `{"not": "a bundle"}`)

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{
		"--checksums", checksumsPath,
		"--require-provenance",
	}, stdout, stderr)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if got := stderr.String(); !strings.Contains(got, "missing dsseEnvelope") {
		t.Fatalf("expected missing dsseEnvelope error, got %q", got)
	}
}

func TestVerifyReleaseCmd_ProvenanceTextOutput(t *testing.T) {
	dir := t.TempDir()
	artifacts := map[string]string{
		"hookaido_v1_linux_amd64.tar.gz": "artifact-linux",
	}
	checksumsPath := writeReleaseArtifactsWithChecksums(t, dir, artifacts)

	var subjects []intotoSubject
	for name, body := range artifacts {
		sum := sha256.Sum256([]byte(body))
		subjects = append(subjects, intotoSubject{
			Name:   name,
			Digest: map[string]string{"sha256": hex.EncodeToString(sum[:])},
		})
	}
	writeAttestationBundle(t, dir, "hookaido_v1_provenance.attestation.json",
		buildAttestationBundle(t, provenancePredicateType, subjects))

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runVerifyReleaseCmd([]string{
		"--checksums", checksumsPath,
		"--require-provenance",
	}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d; stderr=%q", code, stderr.String())
	}
	if got := stdout.String(); !strings.Contains(got, "provenance: verified") {
		t.Fatalf("expected provenance verified in text output, got %q", got)
	}
}
