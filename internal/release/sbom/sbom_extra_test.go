package sbom

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- Generate ---

func TestGenerate_HappyPath(t *testing.T) {
	repoRoot, err := DetectRepoRoot()
	if err != nil {
		t.Skipf("DetectRepoRoot: %v", err)
	}
	out := filepath.Join(t.TempDir(), "sbom.spdx.json")
	if err := Generate(Options{
		Name:        "hookaido",
		Version:     "v0.0.0-coverage",
		Commit:      "test",
		BuildDate:   "2026-05-09T00:00:00Z",
		RepoRoot:    repoRoot,
		OutPath:     out,
		GeneratedAt: time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read sbom: %v", err)
	}
	var doc spdxDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if doc.SPDXVersion != "SPDX-2.3" {
		t.Fatalf("spdxVersion=%q", doc.SPDXVersion)
	}
	if doc.Name != "hookaido-v0.0.0-coverage-sbom" {
		t.Fatalf("doc name=%q", doc.Name)
	}
	if len(doc.Packages) < 2 {
		t.Fatalf("expected main module + at least one dep, got %d packages", len(doc.Packages))
	}
	if doc.Packages[0].PrimaryPackagePurpose != "APPLICATION" {
		t.Fatalf("first package should be APPLICATION, got %q", doc.Packages[0].PrimaryPackagePurpose)
	}
}

func TestGenerate_RequiresRepoRoot(t *testing.T) {
	if err := Generate(Options{OutPath: "/tmp/x"}); err == nil ||
		!strings.Contains(err.Error(), "repo root is required") {
		t.Fatalf("err=%v, want repo-root-required", err)
	}
}

func TestGenerate_RequiresOutPath(t *testing.T) {
	if err := Generate(Options{RepoRoot: t.TempDir()}); err == nil ||
		!strings.Contains(err.Error(), "out path is required") {
		t.Fatalf("err=%v, want out-path-required", err)
	}
}

func TestGenerate_LoadModulesFails(t *testing.T) {
	// An empty directory has no go.mod, so `go list -m -json all` fails.
	dir := t.TempDir()
	err := Generate(Options{
		Name:     "x",
		Version:  "v1",
		RepoRoot: dir,
		OutPath:  filepath.Join(dir, "out.json"),
	})
	if err == nil {
		t.Fatalf("expected error from go list in empty dir")
	}
}

func TestGenerate_WriteFailsOnDirectoryPath(t *testing.T) {
	repoRoot, err := DetectRepoRoot()
	if err != nil {
		t.Skipf("DetectRepoRoot: %v", err)
	}
	// OutPath that points at a directory triggers WriteFile failure.
	dir := t.TempDir()
	if err := Generate(Options{
		Name:     "x",
		Version:  "v1",
		RepoRoot: repoRoot,
		OutPath:  dir,
	}); err == nil {
		t.Fatalf("expected error writing to directory path")
	}
}

// --- DetectRepoRoot ---

func TestDetectRepoRoot_FromRepo(t *testing.T) {
	root, err := DetectRepoRoot()
	if err != nil {
		t.Fatalf("DetectRepoRoot: %v", err)
	}
	if root == "" {
		t.Fatalf("empty root")
	}
	// go.mod should exist there.
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("go.mod not at detected root %q: %v", root, err)
	}
}

func TestDetectRepoRoot_OutsideRepoFails(t *testing.T) {
	// Switch CWD to a temp dir outside any Go module.
	dir := t.TempDir()
	prev, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(prev) })

	// `go env GOMOD` returns os.DevNull or NUL outside a module.
	if _, err := DetectRepoRoot(); err == nil {
		t.Fatalf("expected error outside a Go module")
	}
}

// --- loadModules ---

func TestLoadModules_NonGoDirFails(t *testing.T) {
	if _, err := loadModules(t.TempDir()); err == nil {
		t.Fatalf("expected error in non-Go directory")
	}
}

// --- parseModuleList edge cases ---

func TestParseModuleList_Empty(t *testing.T) {
	if _, err := parseModuleList([]byte("")); err == nil {
		t.Fatalf("expected error for empty input")
	}
}

func TestParseModuleList_BlankPathsSkipped(t *testing.T) {
	input := []byte(`{"Path":"","Version":""}
{"Path":"github.com/example/mod","Version":"v1.0.0"}`)
	mods, err := parseModuleList(input)
	if err != nil {
		t.Fatalf("parseModuleList: %v", err)
	}
	if len(mods) != 1 || mods[0].Path != "github.com/example/mod" {
		t.Fatalf("unexpected mods: %#v", mods)
	}
}

func TestParseModuleList_TrimsAndPreservesReplace(t *testing.T) {
	input := []byte(`{"Path":"  hookaido  ","Main":true}
{"Path":"github.com/example/mod","Version":"  v1.0.0  ","Replace":{"Path":"  github.com/replace/mod  ","Version":"  v9  "}}`)
	mods, err := parseModuleList(input)
	if err != nil {
		t.Fatalf("parseModuleList: %v", err)
	}
	if len(mods) != 2 {
		t.Fatalf("len=%d", len(mods))
	}
	if mods[0].Path != "hookaido" {
		t.Fatalf("path not trimmed: %q", mods[0].Path)
	}
	if mods[1].Version != "v1.0.0" {
		t.Fatalf("version not trimmed: %q", mods[1].Version)
	}
	if mods[1].Replace == nil || mods[1].Replace.Path != "github.com/replace/mod" || mods[1].Replace.Version != "v9" {
		t.Fatalf("replace not normalized: %#v", mods[1].Replace)
	}
}

func TestParseModuleList_DecodeError(t *testing.T) {
	if _, err := parseModuleList([]byte("not json")); err == nil ||
		!strings.Contains(err.Error(), "decode go module list") {
		t.Fatalf("err=%v, want decode error", err)
	}
}

// --- normalizeModules edge cases ---

func TestNormalizeModules_NoMainModule(t *testing.T) {
	mods := []goModule{
		{Path: "github.com/a/b", Version: "v1"},
	}
	if _, err := normalizeModules(Options{}, mods); err == nil ||
		!errors.Is(err, errors.New("no main module found for sbom generation")) && !strings.Contains(err.Error(), "no main module") {
		t.Fatalf("err=%v, want no-main-module", err)
	}
}

func TestNormalizeModules_MainVersionFromOptsOverridesGoList(t *testing.T) {
	mods := []goModule{
		{Path: "hookaido", Main: true, Version: "v0.0.0"},
		{Path: "github.com/a/b", Version: "v1.2.3"},
	}
	descs, err := normalizeModules(Options{Version: "v9.9.9"}, mods)
	if err != nil {
		t.Fatalf("normalizeModules: %v", err)
	}
	if !descs[0].Main || descs[0].Version != "v9.9.9" {
		t.Fatalf("main descriptor=%#v, want v9.9.9", descs[0])
	}
}

func TestNormalizeModules_ReplaceVersionWins(t *testing.T) {
	mods := []goModule{
		{Path: "hookaido", Main: true},
		{
			Path:    "github.com/a/b",
			Version: "v1.2.3",
			Replace: &goModuleReplace{Path: "fork", Version: "v9.9.9"},
		},
	}
	descs, err := normalizeModules(Options{}, mods)
	if err != nil {
		t.Fatalf("normalizeModules: %v", err)
	}
	if descs[1].Version != "v9.9.9" {
		t.Fatalf("expected replace version to win, got %q", descs[1].Version)
	}
}

func TestNormalizeModules_DedupsByPathVersion(t *testing.T) {
	mods := []goModule{
		{Path: "hookaido", Main: true},
		{Path: "github.com/a/b", Version: "v1"},
		{Path: "github.com/a/b", Version: "v1"}, // duplicate
		{Path: "github.com/a/b", Version: "v2"}, // different version, kept
	}
	descs, err := normalizeModules(Options{}, mods)
	if err != nil {
		t.Fatalf("normalizeModules: %v", err)
	}
	// 1 main + 2 unique deps.
	if len(descs) != 3 {
		t.Fatalf("descs=%d, want 3", len(descs))
	}
}

func TestNormalizeModules_SortsDeps(t *testing.T) {
	mods := []goModule{
		{Path: "hookaido", Main: true},
		{Path: "github.com/zzz/pkg", Version: "v1"},
		{Path: "github.com/aaa/pkg", Version: "v1"},
		{Path: "github.com/aaa/pkg", Version: "v2"},
	}
	descs, err := normalizeModules(Options{}, mods)
	if err != nil {
		t.Fatalf("normalizeModules: %v", err)
	}
	// main, then deps sorted by path+version
	if descs[1].Path != "github.com/aaa/pkg" || descs[1].Version != "v1" {
		t.Fatalf("descs[1]=%#v", descs[1])
	}
	if descs[2].Path != "github.com/aaa/pkg" || descs[2].Version != "v2" {
		t.Fatalf("descs[2]=%#v", descs[2])
	}
	if descs[3].Path != "github.com/zzz/pkg" {
		t.Fatalf("descs[3]=%#v", descs[3])
	}
}

// --- buildDocument additional paths ---

func TestBuildDocument_DefaultsAndCreators(t *testing.T) {
	mods := []goModule{
		{Path: "hookaido", Main: true},
		{Path: "github.com/a/b", Version: "v1"},
	}
	// Empty Name + Empty Version => defaults applied.
	doc, err := buildDocument(Options{}, mods)
	if err != nil {
		t.Fatalf("buildDocument: %v", err)
	}
	if doc.Name != "hookaido-0.0.0-dev-sbom" {
		t.Fatalf("default name=%q", doc.Name)
	}
	if !strings.Contains(doc.DocumentNamespace, "0.0.0-dev") {
		t.Fatalf("namespace should contain default version: %q", doc.DocumentNamespace)
	}
	// Without Commit/BuildDate, only the two mandatory creators are present.
	if len(doc.CreationInfo.Creators) != 2 {
		t.Fatalf("creators=%d, want 2", len(doc.CreationInfo.Creators))
	}

	// With Commit + BuildDate, two extra creator lines.
	doc2, err := buildDocument(Options{Commit: "abc", BuildDate: "2026-05-09T00:00:00Z"}, mods)
	if err != nil {
		t.Fatalf("buildDocument: %v", err)
	}
	if len(doc2.CreationInfo.Creators) != 4 {
		t.Fatalf("creators=%d, want 4 (incl. commit+build_date)", len(doc2.CreationInfo.Creators))
	}
}

func TestBuildDocument_GeneratedAtNonUTCNormalizedToUTC(t *testing.T) {
	mods := []goModule{{Path: "hookaido", Main: true}}
	loc := time.FixedZone("CET", 60*60)
	doc, err := buildDocument(Options{
		GeneratedAt: time.Date(2026, 5, 9, 14, 0, 0, 0, loc),
	}, mods)
	if err != nil {
		t.Fatalf("buildDocument: %v", err)
	}
	// 14:00 CET => 13:00 UTC, so output must end with Z and reflect UTC hour.
	if !strings.HasSuffix(doc.CreationInfo.Created, "Z") {
		t.Fatalf("created=%q, want UTC suffix Z", doc.CreationInfo.Created)
	}
	if !strings.Contains(doc.CreationInfo.Created, "T13:") {
		t.Fatalf("created=%q, want UTC-normalized hour 13", doc.CreationInfo.Created)
	}
}

// --- toSPDXPackage / modulePURL ---

func TestToSPDXPackage_DependencyDefaults(t *testing.T) {
	pkg := toSPDXPackage(moduleDescriptor{Path: "github.com/a/b", Version: "v1"}, "SPDXRef-x")
	if pkg.PrimaryPackagePurpose != "LIBRARY" {
		t.Fatalf("purpose=%q, want LIBRARY", pkg.PrimaryPackagePurpose)
	}
	if pkg.DownloadLocation != "NOASSERTION" {
		t.Fatalf("download=%q", pkg.DownloadLocation)
	}
	if len(pkg.ExternalRefs) != 1 || pkg.ExternalRefs[0].ReferenceLocator != "pkg:golang/github.com/a/b@v1" {
		t.Fatalf("purl ref=%#v", pkg.ExternalRefs)
	}
}

func TestToSPDXPackage_MainNoVersion(t *testing.T) {
	pkg := toSPDXPackage(moduleDescriptor{Path: "hookaido", Main: true}, "SPDXRef-Main")
	if pkg.DownloadLocation != "NONE" || pkg.PrimaryPackagePurpose != "APPLICATION" {
		t.Fatalf("main pkg=%#v", pkg)
	}
	if pkg.VersionInfo != "" {
		t.Fatalf("expected empty VersionInfo for unversioned module, got %q", pkg.VersionInfo)
	}
}

func TestModulePURL(t *testing.T) {
	tests := []struct {
		name          string
		path, version string
		want          string
	}{
		{"empty_path", "", "v1", ""},
		{"with_version", "github.com/a/b", "v1.0.0", "pkg:golang/github.com/a/b@v1.0.0"},
		{"trims_whitespace", "  github.com/a/b  ", "  v1  ", "pkg:golang/github.com/a/b@v1"},
		{"no_version", "github.com/a/b", "", "pkg:golang/github.com/a/b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := modulePURL(tc.path, tc.version); got != tc.want {
				t.Fatalf("modulePURL(%q, %q) = %q, want %q", tc.path, tc.version, got, tc.want)
			}
		})
	}
}

// --- sanitizeSPDXFragment / safeNameFragment ---

func TestSanitizeSPDXFragment(t *testing.T) {
	tests := map[string]string{
		"":                  "item",
		"   ":               "item",
		"github.com/a/b":    "github.com-a-b",
		"////":              "item",
		"foo  bar":          "foo-bar",
		"v1.0.0+build/meta": "v1.0.0-build-meta",
		"++--__":            "item",
	}
	for in, want := range tests {
		if got := sanitizeSPDXFragment(in); got != want {
			t.Errorf("sanitizeSPDXFragment(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSafeNameFragment(t *testing.T) {
	tests := []struct {
		in       string
		fallback string
		want     string
	}{
		{"", "fallback", "fallback"},
		{"  ", "fallback", "fallback"},
		{"v1.0.0", "fallback", "v1.0.0"},
		{"v1.0.0+build/meta", "fb", "v1.0.0_build_meta"},
		{"___", "fb", "fb"},
		{"a-b_c.d", "fb", "a-b_c.d"},
	}
	for _, tc := range tests {
		if got := safeNameFragment(tc.in, tc.fallback); got != tc.want {
			t.Errorf("safeNameFragment(%q, %q) = %q, want %q", tc.in, tc.fallback, got, tc.want)
		}
	}
}
