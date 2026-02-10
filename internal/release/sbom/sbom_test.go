package sbom

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParseModuleList(t *testing.T) {
	input := []byte(`{"Path":"hookaido","Main":true}
{"Path":"github.com/example/dependency","Version":"v1.2.3"}`)

	modules, err := parseModuleList(input)
	if err != nil {
		t.Fatalf("parseModuleList: %v", err)
	}
	if len(modules) != 2 {
		t.Fatalf("expected 2 modules, got %d", len(modules))
	}
	if !modules[0].Main || modules[0].Path != "hookaido" {
		t.Fatalf("unexpected main module: %#v", modules[0])
	}
	if modules[1].Path != "github.com/example/dependency" || modules[1].Version != "v1.2.3" {
		t.Fatalf("unexpected dependency module: %#v", modules[1])
	}
}

func TestBuildDocument(t *testing.T) {
	opts := Options{
		Name:        "hookaido",
		Version:     "v1.0.0",
		Commit:      "abc123",
		BuildDate:   "2026-02-09T00:00:00Z",
		GeneratedAt: time.Date(2026, 2, 9, 0, 0, 0, 0, time.UTC),
	}
	modules := []goModule{
		{Path: "hookaido", Main: true},
		{Path: "github.com/example/dependency", Version: "v1.2.3"},
	}
	doc, err := buildDocument(opts, modules)
	if err != nil {
		t.Fatalf("buildDocument: %v", err)
	}
	if doc.SPDXVersion != "SPDX-2.3" {
		t.Fatalf("unexpected spdxVersion: %q", doc.SPDXVersion)
	}
	if doc.Name != "hookaido-v1.0.0-sbom" {
		t.Fatalf("unexpected document name: %q", doc.Name)
	}
	if len(doc.Packages) != 2 {
		t.Fatalf("expected 2 packages, got %d", len(doc.Packages))
	}
	if doc.Packages[0].Name != "hookaido" || doc.Packages[0].PrimaryPackagePurpose != "APPLICATION" {
		t.Fatalf("unexpected main package: %#v", doc.Packages[0])
	}
	if len(doc.Relationships) != 2 {
		t.Fatalf("expected 2 relationships, got %d", len(doc.Relationships))
	}
	if doc.Relationships[0].RelationshipType != "DESCRIBES" {
		t.Fatalf("expected first relationship DESCRIBES, got %q", doc.Relationships[0].RelationshipType)
	}
	if doc.Relationships[1].RelationshipType != "DEPENDS_ON" {
		t.Fatalf("expected second relationship DEPENDS_ON, got %q", doc.Relationships[1].RelationshipType)
	}
}

func TestNewSPDXID_Unique(t *testing.T) {
	used := map[string]int{}
	first := newSPDXID("github.com/example/mod", "v1.0.0", used)
	second := newSPDXID("github.com/example/mod", "v1.0.0", used)
	if first == second {
		t.Fatalf("expected unique IDs, both were %q", first)
	}
	if !strings.HasPrefix(second, first+"-") {
		t.Fatalf("expected second id to be suffixed from %q, got %q", first, second)
	}
}

func TestDocumentJSONRoundTrip(t *testing.T) {
	opts := Options{
		Name:        "hookaido",
		Version:     "v1.0.1",
		GeneratedAt: time.Date(2026, 2, 9, 0, 0, 0, 0, time.UTC),
	}
	modules := []goModule{
		{Path: "hookaido", Main: true},
		{Path: "github.com/example/dependency", Version: "v2.0.0"},
	}
	doc, err := buildDocument(opts, modules)
	if err != nil {
		t.Fatalf("buildDocument: %v", err)
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}
	var got spdxDocument
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if got.Name != doc.Name || len(got.Packages) != len(doc.Packages) {
		t.Fatalf("round trip mismatch: %#v vs %#v", got, doc)
	}
}
