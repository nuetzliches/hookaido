package sbom

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Options struct {
	Name        string
	Version     string
	Commit      string
	BuildDate   string
	RepoRoot    string
	OutPath     string
	GeneratedAt time.Time
}

type goModule struct {
	Path    string           `json:"Path"`
	Version string           `json:"Version"`
	Main    bool             `json:"Main"`
	Replace *goModuleReplace `json:"Replace,omitempty"`
}

type goModuleReplace struct {
	Path    string `json:"Path"`
	Version string `json:"Version"`
}

type moduleDescriptor struct {
	Path    string
	Version string
	Main    bool
}

type spdxDocument struct {
	SPDXVersion       string             `json:"spdxVersion"`
	DataLicense       string             `json:"dataLicense"`
	SPDXID            string             `json:"SPDXID"`
	Name              string             `json:"name"`
	DocumentNamespace string             `json:"documentNamespace"`
	CreationInfo      spdxCreationInfo   `json:"creationInfo"`
	DocumentDescribes []string           `json:"documentDescribes,omitempty"`
	Packages          []spdxPackage      `json:"packages"`
	Relationships     []spdxRelationship `json:"relationships,omitempty"`
}

type spdxCreationInfo struct {
	Created            string   `json:"created"`
	Creators           []string `json:"creators"`
	LicenseListVersion string   `json:"licenseListVersion,omitempty"`
}

type spdxPackage struct {
	Name                  string            `json:"name"`
	SPDXID                string            `json:"SPDXID"`
	VersionInfo           string            `json:"versionInfo,omitempty"`
	DownloadLocation      string            `json:"downloadLocation"`
	FilesAnalyzed         bool              `json:"filesAnalyzed"`
	LicenseConcluded      string            `json:"licenseConcluded"`
	LicenseDeclared       string            `json:"licenseDeclared"`
	PrimaryPackagePurpose string            `json:"primaryPackagePurpose,omitempty"`
	ExternalRefs          []spdxExternalRef `json:"externalRefs,omitempty"`
}

type spdxExternalRef struct {
	ReferenceCategory string `json:"referenceCategory"`
	ReferenceType     string `json:"referenceType"`
	ReferenceLocator  string `json:"referenceLocator"`
}

type spdxRelationship struct {
	SpdxElementID      string `json:"spdxElementId"`
	RelationshipType   string `json:"relationshipType"`
	RelatedSpdxElement string `json:"relatedSpdxElement"`
}

func Generate(opts Options) error {
	repoRoot := strings.TrimSpace(opts.RepoRoot)
	if repoRoot == "" {
		return errors.New("repo root is required")
	}
	outPath := strings.TrimSpace(opts.OutPath)
	if outPath == "" {
		return errors.New("out path is required")
	}

	modules, err := loadModules(repoRoot)
	if err != nil {
		return err
	}
	doc, err := buildDocument(opts, modules)
	if err != nil {
		return err
	}
	body, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal sbom: %w", err)
	}
	body = append(body, '\n')
	if err := os.WriteFile(outPath, body, 0o644); err != nil {
		return fmt.Errorf("write sbom %q: %w", outPath, err)
	}
	return nil
}

func loadModules(repoRoot string) ([]goModule, error) {
	cmd := exec.Command("go", "list", "-m", "-json", "all")
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("go list modules: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	modules, err := parseModuleList(out)
	if err != nil {
		return nil, err
	}
	return modules, nil
}

func parseModuleList(data []byte) ([]goModule, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	modules := make([]goModule, 0, 16)
	for {
		var mod goModule
		if err := dec.Decode(&mod); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode go module list: %w", err)
		}
		if strings.TrimSpace(mod.Path) == "" {
			continue
		}
		mod.Path = strings.TrimSpace(mod.Path)
		mod.Version = strings.TrimSpace(mod.Version)
		if mod.Replace != nil {
			mod.Replace.Path = strings.TrimSpace(mod.Replace.Path)
			mod.Replace.Version = strings.TrimSpace(mod.Replace.Version)
		}
		modules = append(modules, mod)
	}
	if len(modules) == 0 {
		return nil, errors.New("no modules found in go list output")
	}
	return modules, nil
}

func buildDocument(opts Options, modules []goModule) (spdxDocument, error) {
	descriptors, err := normalizeModules(opts, modules)
	if err != nil {
		return spdxDocument{}, err
	}

	mainMod := descriptors[0]
	deps := descriptors[1:]
	now := opts.GeneratedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if now.Location() != time.UTC {
		now = now.UTC()
	}

	docName := strings.TrimSpace(opts.Name)
	if docName == "" {
		docName = "hookaido"
	}
	docName = fmt.Sprintf("%s-%s-sbom", docName, safeNameFragment(strings.TrimSpace(opts.Version), "0.0.0-dev"))

	namespaceBase := safeNameFragment(strings.TrimSpace(opts.Version), "0.0.0-dev")
	docNamespace := fmt.Sprintf("https://hookaido.dev/spdx/%s/%d", namespaceBase, now.Unix())

	usedIDs := map[string]int{}
	mainID := newSPDXID(mainMod.Path, mainMod.Version, usedIDs)
	pkgs := make([]spdxPackage, 0, len(descriptors))
	pkgs = append(pkgs, toSPDXPackage(mainMod, mainID))

	depIDs := make([]string, 0, len(deps))
	for _, dep := range deps {
		depID := newSPDXID(dep.Path, dep.Version, usedIDs)
		pkgs = append(pkgs, toSPDXPackage(dep, depID))
		depIDs = append(depIDs, depID)
	}

	rels := make([]spdxRelationship, 0, len(depIDs)+1)
	rels = append(rels, spdxRelationship{
		SpdxElementID:      "SPDXRef-DOCUMENT",
		RelationshipType:   "DESCRIBES",
		RelatedSpdxElement: mainID,
	})
	for _, depID := range depIDs {
		rels = append(rels, spdxRelationship{
			SpdxElementID:      mainID,
			RelationshipType:   "DEPENDS_ON",
			RelatedSpdxElement: depID,
		})
	}

	creators := []string{
		"Organization: Hookaido",
		"Tool: github.com/nuetzliches/hookaido/internal/release/sbom",
	}
	if commit := strings.TrimSpace(opts.Commit); commit != "" {
		creators = append(creators, "Tool: commit="+commit)
	}
	if buildDate := strings.TrimSpace(opts.BuildDate); buildDate != "" {
		creators = append(creators, "Tool: build_date="+buildDate)
	}

	return spdxDocument{
		SPDXVersion:       "SPDX-2.3",
		DataLicense:       "CC0-1.0",
		SPDXID:            "SPDXRef-DOCUMENT",
		Name:              docName,
		DocumentNamespace: docNamespace,
		CreationInfo: spdxCreationInfo{
			Created:            now.Format(time.RFC3339),
			Creators:           creators,
			LicenseListVersion: "3.25",
		},
		DocumentDescribes: []string{mainID},
		Packages:          pkgs,
		Relationships:     rels,
	}, nil
}

func normalizeModules(opts Options, modules []goModule) ([]moduleDescriptor, error) {
	mainMods := make([]moduleDescriptor, 0, 1)
	deps := make([]moduleDescriptor, 0, len(modules))
	seen := map[string]struct{}{}
	for _, mod := range modules {
		version := strings.TrimSpace(mod.Version)
		if mod.Replace != nil && strings.TrimSpace(mod.Replace.Version) != "" {
			version = strings.TrimSpace(mod.Replace.Version)
		}
		if mod.Main {
			mainVersion := strings.TrimSpace(opts.Version)
			if mainVersion != "" {
				version = mainVersion
			}
			mainMods = append(mainMods, moduleDescriptor{
				Path:    mod.Path,
				Version: version,
				Main:    true,
			})
			continue
		}
		key := mod.Path + "@" + version
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		deps = append(deps, moduleDescriptor{
			Path:    mod.Path,
			Version: version,
			Main:    false,
		})
	}
	if len(mainMods) == 0 {
		return nil, errors.New("no main module found for sbom generation")
	}
	main := mainMods[0]
	sort.Slice(deps, func(i, j int) bool {
		if deps[i].Path == deps[j].Path {
			return deps[i].Version < deps[j].Version
		}
		return deps[i].Path < deps[j].Path
	})
	out := make([]moduleDescriptor, 0, len(deps)+1)
	out = append(out, main)
	out = append(out, deps...)
	return out, nil
}

func toSPDXPackage(mod moduleDescriptor, id string) spdxPackage {
	download := "NOASSERTION"
	purpose := "LIBRARY"
	if mod.Main {
		download = "NONE"
		purpose = "APPLICATION"
	}
	pkg := spdxPackage{
		Name:                  mod.Path,
		SPDXID:                id,
		DownloadLocation:      download,
		FilesAnalyzed:         false,
		LicenseConcluded:      "NOASSERTION",
		LicenseDeclared:       "NOASSERTION",
		PrimaryPackagePurpose: purpose,
	}
	if mod.Version != "" {
		pkg.VersionInfo = mod.Version
	}
	if purl := modulePURL(mod.Path, mod.Version); purl != "" {
		pkg.ExternalRefs = []spdxExternalRef{
			{
				ReferenceCategory: "PACKAGE-MANAGER",
				ReferenceType:     "purl",
				ReferenceLocator:  purl,
			},
		}
	}
	return pkg
}

func modulePURL(path, version string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	version = strings.TrimSpace(version)
	if version == "" {
		return "pkg:golang/" + path
	}
	return "pkg:golang/" + path + "@" + version
}

func newSPDXID(path, version string, used map[string]int) string {
	base := "SPDXRef-" + sanitizeSPDXFragment(path)
	if v := sanitizeSPDXFragment(version); v != "" {
		base += "-" + v
	}
	if count, ok := used[base]; ok {
		next := count + 1
		used[base] = next
		return fmt.Sprintf("%s-%d", base, next)
	}
	used[base] = 0
	return base
}

func sanitizeSPDXFragment(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "item"
	}
	var b strings.Builder
	b.Grow(len(v))
	lastDash := false
	for _, r := range v {
		isAlphaNum := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if isAlphaNum || r == '.' {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "item"
	}
	return out
}

func safeNameFragment(v, fallback string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
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
		return fallback
	}
	return out
}

func DetectRepoRoot() (string, error) {
	cmd := exec.Command("go", "env", "GOMOD")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("detect go.mod: %w", err)
	}
	mod := strings.TrimSpace(string(out))
	if mod == "" || mod == os.DevNull || strings.EqualFold(mod, "NUL") {
		return "", errors.New("go.mod not found")
	}
	return filepath.Dir(mod), nil
}
