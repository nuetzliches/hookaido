package app

import (
	"bufio"
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
	"path/filepath"
	"regexp"
	"strings"
)

var checksumLinePattern = regexp.MustCompile(`^\s*([a-fA-F0-9]{64})\s+\*?(.+?)\s*$`)

type verifyReleaseOptions struct {
	ChecksumsPath     string
	BaseDir           string
	SignaturePath     string
	PublicKeyPath     string
	RequireSignature  bool
	SBOMPath          string
	RequireSBOM       bool
	RequireProvenance bool
	ProvenancePath    string
	SBOMAttestPath    string
}

type verifyReleaseArtifact struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}

type verifyReleaseResult struct {
	ChecksumsFile      string                  `json:"checksums_file"`
	BaseDir            string                  `json:"base_dir"`
	ArtifactCount      int                     `json:"artifact_count"`
	Artifacts          []verifyReleaseArtifact `json:"artifacts"`
	SBOMVerified       bool                    `json:"sbom_verified"`
	SBOMFile           string                  `json:"sbom_file,omitempty"`
	SignatureVerified  bool                    `json:"signature_verified"`
	SignatureFile      string                  `json:"signature_file,omitempty"`
	PublicKeyFile      string                  `json:"public_key_file,omitempty"`
	SignatureAlgorithm string                  `json:"signature_algorithm,omitempty"`
	ProvenanceVerified bool                    `json:"provenance_verified"`
	ProvenanceFile     string                  `json:"provenance_file,omitempty"`
	SBOMAttestVerified bool                    `json:"sbom_attest_verified"`
	SBOMAttestFile     string                  `json:"sbom_attest_file,omitempty"`
}

func verifyReleaseCmd(args []string) int {
	return runVerifyReleaseCmd(args, os.Stdout, os.Stderr)
}

func runVerifyReleaseCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("verify-release", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	checksumsPath := fs.String("checksums", "", "path to checksums file")
	baseDir := fs.String("base-dir", "", "base directory for artifact files (default: checksums file directory)")
	signaturePath := fs.String("signature", "", "path to detached Base64 Ed25519 signature (default: <checksums>.sig when present)")
	publicKeyPath := fs.String("public-key", "", "path to Ed25519 public key PEM (default: <checksums>.pub.pem when present)")
	requireSignature := fs.Bool("require-signature", false, "require detached signature verification")
	sbomPath := fs.String("sbom", "", "path to SPDX SBOM file (default: auto-detect *_sbom.spdx.json entry from checksums)")
	requireSBOM := fs.Bool("require-sbom", false, "require SPDX SBOM presence and quality validation")
	requireProvenance := fs.Bool("require-provenance", false, "require provenance attestation bundle validation")
	provenancePath := fs.String("provenance", "", "path to provenance attestation bundle (default: auto-detect *_provenance.attestation.json)")
	sbomAttestPath := fs.String("sbom-attestation", "", "path to SBOM attestation bundle (default: auto-detect *_sbom.attestation.json)")
	jsonOutput := fs.Bool("json", false, "print JSON output")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(stderr, "verify-release: %v\n", err)
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "verify-release: unexpected positional arguments")
		return 2
	}

	opts := verifyReleaseOptions{
		ChecksumsPath:     strings.TrimSpace(*checksumsPath),
		BaseDir:           strings.TrimSpace(*baseDir),
		SignaturePath:     strings.TrimSpace(*signaturePath),
		PublicKeyPath:     strings.TrimSpace(*publicKeyPath),
		RequireSignature:  *requireSignature,
		SBOMPath:          strings.TrimSpace(*sbomPath),
		RequireSBOM:       *requireSBOM,
		RequireProvenance: *requireProvenance,
		ProvenancePath:    strings.TrimSpace(*provenancePath),
		SBOMAttestPath:    strings.TrimSpace(*sbomAttestPath),
	}
	if opts.ChecksumsPath == "" {
		fmt.Fprintln(stderr, "verify-release: --checksums is required")
		return 2
	}

	result, err := verifyReleaseArtifacts(opts)
	if err != nil {
		fmt.Fprintf(stderr, "verify-release: %v\n", err)
		return 1
	}

	if *jsonOutput {
		enc := json.NewEncoder(stdout)
		if err := enc.Encode(result); err != nil {
			fmt.Fprintf(stderr, "verify-release: %v\n", err)
			return 1
		}
		return 0
	}

	fmt.Fprintf(stdout, "verified %d artifact(s) using %s\n", result.ArtifactCount, result.ChecksumsFile)
	if result.SBOMVerified {
		fmt.Fprintf(stdout, "sbom: verified via %s\n", result.SBOMFile)
	} else {
		fmt.Fprintln(stdout, "sbom: not verified")
	}
	if result.SignatureVerified {
		fmt.Fprintf(stdout, "signature: verified (%s) via %s\n", result.SignatureAlgorithm, result.PublicKeyFile)
	} else {
		fmt.Fprintln(stdout, "signature: not verified")
	}
	if result.ProvenanceVerified {
		fmt.Fprintf(stdout, "provenance: verified via %s\n", result.ProvenanceFile)
	} else {
		fmt.Fprintln(stdout, "provenance: not verified")
	}
	if result.SBOMAttestVerified {
		fmt.Fprintf(stdout, "sbom-attestation: verified via %s\n", result.SBOMAttestFile)
	} else {
		fmt.Fprintln(stdout, "sbom-attestation: not verified")
	}
	return 0
}

type checksumEntry struct {
	SHA256 string
	File   string
}

func verifyReleaseArtifacts(opts verifyReleaseOptions) (verifyReleaseResult, error) {
	checksumsPath, err := filepath.Abs(opts.ChecksumsPath)
	if err != nil {
		return verifyReleaseResult{}, fmt.Errorf("resolve checksums path: %w", err)
	}
	checksumBody, err := os.ReadFile(checksumsPath)
	if err != nil {
		return verifyReleaseResult{}, fmt.Errorf("read checksums %q: %w", checksumsPath, err)
	}
	entries, err := parseChecksumEntries(checksumBody)
	if err != nil {
		return verifyReleaseResult{}, err
	}

	baseDir := strings.TrimSpace(opts.BaseDir)
	if baseDir == "" {
		baseDir = filepath.Dir(checksumsPath)
	}
	baseDir, err = filepath.Abs(baseDir)
	if err != nil {
		return verifyReleaseResult{}, fmt.Errorf("resolve base dir: %w", err)
	}

	artifacts := make([]verifyReleaseArtifact, 0, len(entries))
	for _, entry := range entries {
		artifactPath := resolveArtifactPath(baseDir, entry.File)
		actual, err := sha256FileHex(artifactPath)
		if err != nil {
			return verifyReleaseResult{}, err
		}
		if !strings.EqualFold(actual, entry.SHA256) {
			return verifyReleaseResult{}, fmt.Errorf(
				"checksum mismatch for %q: expected %s, got %s",
				entry.File,
				strings.ToLower(entry.SHA256),
				strings.ToLower(actual),
			)
		}
		artifacts = append(artifacts, verifyReleaseArtifact{
			Name:   entry.File,
			SHA256: strings.ToLower(entry.SHA256),
		})
	}

	signaturePath, publicKeyPath, err := resolveSignaturePaths(checksumsPath, opts.SignaturePath, opts.PublicKeyPath, opts.RequireSignature)
	if err != nil {
		return verifyReleaseResult{}, err
	}

	res := verifyReleaseResult{
		ChecksumsFile: filepath.ToSlash(checksumsPath),
		BaseDir:       filepath.ToSlash(baseDir),
		ArtifactCount: len(artifacts),
		Artifacts:     artifacts,
	}

	sbomPath, err := resolveSBOMPath(baseDir, entries, opts.SBOMPath, opts.RequireSBOM)
	if err != nil {
		return verifyReleaseResult{}, err
	}
	if sbomPath != "" {
		if err := validateSPDXSBOM(sbomPath); err != nil {
			return verifyReleaseResult{}, err
		}
		res.SBOMVerified = true
		res.SBOMFile = filepath.ToSlash(sbomPath)
	}

	if signaturePath != "" {
		if err := verifyChecksumSignature(checksumBody, signaturePath, publicKeyPath); err != nil {
			return verifyReleaseResult{}, err
		}
		res.SignatureVerified = true
		res.SignatureFile = filepath.ToSlash(signaturePath)
		res.PublicKeyFile = filepath.ToSlash(publicKeyPath)
		res.SignatureAlgorithm = "ed25519"
	}

	checksumDigests := make(map[string]string, len(entries))
	for _, e := range entries {
		checksumDigests[strings.ToLower(e.SHA256)] = e.File
	}

	provenancePath, err := resolveAttestationBundlePath(baseDir, entries, opts.ProvenancePath, "_provenance.attestation.json", opts.RequireProvenance, "provenance attestation")
	if err != nil {
		return verifyReleaseResult{}, err
	}
	if provenancePath != "" {
		if err := validateAttestationBundle(provenancePath, checksumDigests, provenancePredicateType); err != nil {
			return verifyReleaseResult{}, err
		}
		res.ProvenanceVerified = true
		res.ProvenanceFile = filepath.ToSlash(provenancePath)
	}

	sbomAttestPath, err := resolveAttestationBundlePath(baseDir, entries, opts.SBOMAttestPath, "_sbom.attestation.json", opts.SBOMAttestPath != "", "sbom attestation")
	if err != nil {
		return verifyReleaseResult{}, err
	}
	if sbomAttestPath != "" {
		if err := validateAttestationBundle(sbomAttestPath, checksumDigests, sbomAttestPredicateType); err != nil {
			return verifyReleaseResult{}, err
		}
		res.SBOMAttestVerified = true
		res.SBOMAttestFile = filepath.ToSlash(sbomAttestPath)
	}

	return res, nil
}

func parseChecksumEntries(data []byte) ([]checksumEntry, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	entries := make([]checksumEntry, 0)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		match := checksumLinePattern.FindStringSubmatch(line)
		if len(match) != 3 {
			return nil, fmt.Errorf("invalid checksums format at line %d", lineNo)
		}
		sumHex := strings.TrimSpace(match[1])
		name := strings.TrimSpace(match[2])
		if name == "" {
			return nil, fmt.Errorf("invalid checksums format at line %d: empty file name", lineNo)
		}
		if _, err := hex.DecodeString(sumHex); err != nil {
			return nil, fmt.Errorf("invalid checksum hex at line %d: %w", lineNo, err)
		}
		entries = append(entries, checksumEntry{
			SHA256: strings.ToLower(sumHex),
			File:   filepath.FromSlash(name),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read checksums: %w", err)
	}
	if len(entries) == 0 {
		return nil, errors.New("checksums file has no entries")
	}
	return entries, nil
}

func resolveArtifactPath(baseDir, listed string) string {
	if filepath.IsAbs(listed) {
		return filepath.Clean(listed)
	}
	return filepath.Join(baseDir, listed)
}

func sha256FileHex(p string) (string, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return "", fmt.Errorf("read artifact %q: %w", p, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func resolveSignaturePaths(checksumsPath, signaturePath, publicKeyPath string, requireSignature bool) (string, string, error) {
	dir := filepath.Dir(checksumsPath)
	sig := strings.TrimSpace(signaturePath)
	pub := strings.TrimSpace(publicKeyPath)

	if sig == "" && pub == "" {
		defaultSig := checksumsPath + ".sig"
		defaultPub := checksumsPath + ".pub.pem"
		if fileExists(defaultSig) && fileExists(defaultPub) {
			sig = defaultSig
			pub = defaultPub
		}
	}
	if (sig == "") != (pub == "") {
		return "", "", errors.New("signature verification requires both --signature and --public-key")
	}
	if requireSignature && (sig == "" || pub == "") {
		return "", "", errors.New("signature required but no signature/public key was provided or auto-detected")
	}
	if sig == "" {
		return "", "", nil
	}
	if !filepath.IsAbs(sig) {
		sig = filepath.Join(dir, filepath.FromSlash(sig))
	}
	if !filepath.IsAbs(pub) {
		pub = filepath.Join(dir, filepath.FromSlash(pub))
	}
	return filepath.Clean(sig), filepath.Clean(pub), nil
}

func verifyChecksumSignature(checksumBody []byte, signaturePath, publicKeyPath string) error {
	signatureEncoded, err := os.ReadFile(signaturePath)
	if err != nil {
		return fmt.Errorf("read signature %q: %w", signaturePath, err)
	}
	signature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(signatureEncoded)))
	if err != nil {
		return fmt.Errorf("decode signature %q: %w", signaturePath, err)
	}

	publicKey, err := loadEd25519PublicKey(publicKeyPath)
	if err != nil {
		return err
	}
	if !ed25519.Verify(publicKey, checksumBody, signature) {
		return errors.New("signature verification failed")
	}
	return nil
}

func loadEd25519PublicKey(path string) (ed25519.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read public key %q: %w", path, err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("parse public key %q: expected PEM", path)
	}
	if block.Type != "PUBLIC KEY" {
		return nil, fmt.Errorf("parse public key %q: unsupported PEM type %q", path, block.Type)
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse public key %q: %w", path, err)
	}
	publicKey, ok := parsed.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("parse public key %q: expected Ed25519 public key", path)
	}
	return publicKey, nil
}

func fileExists(p string) bool {
	info, err := os.Stat(p)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func resolveSBOMPath(baseDir string, entries []checksumEntry, explicit string, requireSBOM bool) (string, error) {
	entryByBase := make(map[string]checksumEntry, len(entries))
	sbomCandidates := make([]string, 0, 1)
	for _, entry := range entries {
		base := filepath.Base(entry.File)
		entryByBase[base] = entry
		if strings.HasSuffix(base, "_sbom.spdx.json") {
			sbomCandidates = append(sbomCandidates, entry.File)
		}
	}

	explicit = strings.TrimSpace(explicit)
	if explicit != "" {
		explicitPath := explicit
		if !filepath.IsAbs(explicitPath) {
			explicitPath = filepath.Join(baseDir, filepath.FromSlash(explicitPath))
		}
		explicitPath = filepath.Clean(explicitPath)
		base := filepath.Base(explicitPath)
		if _, ok := entryByBase[base]; !ok {
			return "", fmt.Errorf("sbom file %q is not covered by checksums entries", base)
		}
		return explicitPath, nil
	}

	switch len(sbomCandidates) {
	case 0:
		if requireSBOM {
			return "", errors.New("sbom required but no *_sbom.spdx.json entry was found in checksums")
		}
		return "", nil
	case 1:
		return filepath.Clean(resolveArtifactPath(baseDir, sbomCandidates[0])), nil
	default:
		return "", errors.New("multiple sbom entries found in checksums; specify --sbom explicitly")
	}
}

type spdxSBOMDocument struct {
	SPDXVersion       string            `json:"spdxVersion"`
	DataLicense       string            `json:"dataLicense"`
	DocumentNamespace string            `json:"documentNamespace"`
	DocumentDescribes []string          `json:"documentDescribes"`
	Packages          []spdxSBOMPackage `json:"packages"`
}

type spdxSBOMPackage struct {
	SPDXID                string `json:"SPDXID"`
	Name                  string `json:"name"`
	PrimaryPackagePurpose string `json:"primaryPackagePurpose"`
}

func validateSPDXSBOM(path string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read sbom %q: %w", path, err)
	}
	var doc spdxSBOMDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		return fmt.Errorf("parse sbom %q: %w", path, err)
	}
	if strings.TrimSpace(doc.SPDXVersion) != "SPDX-2.3" {
		return fmt.Errorf("sbom %q: unsupported spdxVersion %q", path, strings.TrimSpace(doc.SPDXVersion))
	}
	if strings.TrimSpace(doc.DataLicense) != "CC0-1.0" {
		return fmt.Errorf("sbom %q: unexpected dataLicense %q", path, strings.TrimSpace(doc.DataLicense))
	}
	if strings.TrimSpace(doc.DocumentNamespace) == "" {
		return fmt.Errorf("sbom %q: missing documentNamespace", path)
	}
	if len(doc.Packages) == 0 {
		return fmt.Errorf("sbom %q: no packages found", path)
	}
	packageIDs := make(map[string]struct{}, len(doc.Packages))
	hasApplication := false
	for _, pkg := range doc.Packages {
		id := strings.TrimSpace(pkg.SPDXID)
		name := strings.TrimSpace(pkg.Name)
		if id == "" || name == "" {
			return fmt.Errorf("sbom %q: package entries must include SPDXID and name", path)
		}
		packageIDs[id] = struct{}{}
		if strings.EqualFold(strings.TrimSpace(pkg.PrimaryPackagePurpose), "APPLICATION") {
			hasApplication = true
		}
	}
	if !hasApplication {
		return fmt.Errorf("sbom %q: missing APPLICATION package", path)
	}
	if len(doc.DocumentDescribes) == 0 {
		return fmt.Errorf("sbom %q: missing documentDescribes", path)
	}
	for _, described := range doc.DocumentDescribes {
		id := strings.TrimSpace(described)
		if id == "" {
			return fmt.Errorf("sbom %q: documentDescribes contains empty SPDXID", path)
		}
		if _, ok := packageIDs[id]; !ok {
			return fmt.Errorf("sbom %q: documentDescribes references unknown package %q", path, id)
		}
	}
	return nil
}

// Attestation bundle validation — Sigstore Bundle / DSSE / in-toto format.

const (
	provenancePredicateType = "https://slsa.dev/provenance/v1"
	sbomAttestPredicateType = "https://spdx.dev/Document/v2.3"
)

// dsseEnvelope models a DSSE (Dead Simple Signing Envelope).
type dsseEnvelope struct {
	PayloadType string `json:"payloadType"`
	Payload     string `json:"payload"` // base64-encoded
}

// intotoStatement models an in-toto v1 statement.
type intotoStatement struct {
	Type          string          `json:"_type"`
	PredicateType string          `json:"predicateType"`
	Subject       []intotoSubject `json:"subject"`
}

type intotoSubject struct {
	Name   string            `json:"name"`
	Digest map[string]string `json:"digest"`
}

// sigstoreBundle models the top-level Sigstore bundle format.
type sigstoreBundle struct {
	MediaType    string        `json:"mediaType"`
	DSSEEnvelope *dsseEnvelope `json:"dsseEnvelope"`
}

func resolveAttestationBundlePath(baseDir string, entries []checksumEntry, explicit, suffix string, required bool, label string) (string, error) {
	explicit = strings.TrimSpace(explicit)
	if explicit != "" {
		if !filepath.IsAbs(explicit) {
			explicit = filepath.Join(baseDir, filepath.FromSlash(explicit))
		}
		return filepath.Clean(explicit), nil
	}

	// Auto-detect: look in baseDir for files matching suffix.
	candidates := make([]string, 0, 1)
	dirEntries, _ := os.ReadDir(baseDir)
	for _, de := range dirEntries {
		if !de.IsDir() && strings.HasSuffix(de.Name(), suffix) {
			candidates = append(candidates, filepath.Join(baseDir, de.Name()))
		}
	}
	// Also check checksums entries for attestation bundles shipped alongside artifacts.
	for _, entry := range entries {
		if strings.HasSuffix(entry.File, suffix) {
			p := filepath.Clean(resolveArtifactPath(baseDir, entry.File))
			found := false
			for _, c := range candidates {
				if c == p {
					found = true
					break
				}
			}
			if !found {
				candidates = append(candidates, p)
			}
		}
	}

	switch len(candidates) {
	case 0:
		if required {
			return "", fmt.Errorf("%s required but no *%s file was found", label, suffix)
		}
		return "", nil
	case 1:
		return candidates[0], nil
	default:
		return "", fmt.Errorf("multiple %s bundle candidates found; specify path explicitly", label)
	}
}

func validateAttestationBundle(path string, checksumDigests map[string]string, expectedPredicate string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read attestation bundle %q: %w", path, err)
	}

	var bundle sigstoreBundle
	if err := json.Unmarshal(body, &bundle); err != nil {
		return fmt.Errorf("parse attestation bundle %q: %w", path, err)
	}

	if bundle.DSSEEnvelope == nil {
		return fmt.Errorf("attestation bundle %q: missing dsseEnvelope", path)
	}

	if bundle.DSSEEnvelope.PayloadType != "application/vnd.in-toto+json" {
		return fmt.Errorf("attestation bundle %q: unexpected payloadType %q", path, bundle.DSSEEnvelope.PayloadType)
	}

	payloadJSON, err := base64.StdEncoding.DecodeString(bundle.DSSEEnvelope.Payload)
	if err != nil {
		return fmt.Errorf("attestation bundle %q: decode payload: %w", path, err)
	}

	var stmt intotoStatement
	if err := json.Unmarshal(payloadJSON, &stmt); err != nil {
		return fmt.Errorf("attestation bundle %q: parse in-toto statement: %w", path, err)
	}

	if stmt.Type != "https://in-toto.io/Statement/v1" {
		return fmt.Errorf("attestation bundle %q: unexpected statement type %q", path, stmt.Type)
	}

	if expectedPredicate != "" && stmt.PredicateType != expectedPredicate {
		return fmt.Errorf("attestation bundle %q: expected predicateType %q, got %q", path, expectedPredicate, stmt.PredicateType)
	}

	if len(stmt.Subject) == 0 {
		return fmt.Errorf("attestation bundle %q: no subjects in statement", path)
	}

	for _, subj := range stmt.Subject {
		sha, ok := subj.Digest["sha256"]
		if !ok {
			return fmt.Errorf("attestation bundle %q: subject %q missing sha256 digest", path, subj.Name)
		}
		sha = strings.ToLower(sha)
		if len(checksumDigests) > 0 {
			if _, found := checksumDigests[sha]; !found {
				return fmt.Errorf("attestation bundle %q: subject %q digest %s not found in checksums", path, subj.Name, sha)
			}
		}
	}

	return nil
}
