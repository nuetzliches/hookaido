package config

import (
	"errors"
	"strings"
	"testing"
)

const managedEndpointSource = `# Hookaidofile for the billing DMZ.
pull_api { auth token "raw:t" }

# Invoice sink. Do not repoint without telling the billing team.
"/a" {
  # mapped by the Admin API
  application "billing"
  endpoint_name "invoice.created"
  pull { path "/e1" }
}

# Spare route, kept for the cutover.
"/b" {
  pull { path "/e2" }
}
`

func parseForEdit(t *testing.T, src string) *Config {
	t.Helper()
	cfg, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return cfg
}

func setManaged(r *Route, application, endpointName string) {
	r.Application = application
	r.ApplicationQuoted = true
	r.ApplicationSet = true
	r.EndpointName = endpointName
	r.EndpointNameQuoted = true
	r.EndpointNameSet = true
}

func clearManaged(r *Route) {
	r.Application = ""
	r.ApplicationQuoted = false
	r.ApplicationSet = false
	r.EndpointName = ""
	r.EndpointNameQuoted = false
	r.EndpointNameSet = false
}

// TestRewriteManagedEndpoints_MoveKeepsComments is the regression test for the
// Admin API deleting every in-body comment: a managed endpoint moved from one
// route to another must leave the rest of the file byte-identical.
func TestRewriteManagedEndpoints_MoveKeepsComments(t *testing.T) {
	cfg := parseForEdit(t, managedEndpointSource)
	clearManaged(&cfg.Routes[0])
	setManaged(&cfg.Routes[1], "billing", "invoice.created")

	out, err := RewriteManagedEndpoints([]byte(managedEndpointSource), cfg)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	got := string(out)

	for _, comment := range []string{
		"# Hookaidofile for the billing DMZ.",
		"# Invoice sink. Do not repoint without telling the billing team.",
		"# mapped by the Admin API",
		"# Spare route, kept for the cutover.",
	} {
		if !strings.Contains(got, comment) {
			t.Fatalf("comment %q was dropped:\n%s", comment, got)
		}
	}

	reparsed := parseForEdit(t, got)
	if reparsed.Routes[0].ApplicationSet || reparsed.Routes[0].EndpointNameSet {
		t.Fatalf("route /a still carries a managed endpoint:\n%s", got)
	}
	if reparsed.Routes[1].Application != "billing" || reparsed.Routes[1].EndpointName != "invoice.created" {
		t.Fatalf("route /b did not receive the managed endpoint:\n%s", got)
	}

	// Everything that is not a managed-endpoint directive must be untouched,
	// down to blank lines and indentation.
	if before, after := withoutManagedLines(managedEndpointSource), withoutManagedLines(got); before != after {
		t.Fatalf("rewrite touched more than the managed-endpoint lines:\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestRewriteManagedEndpoints_UpdateInPlace(t *testing.T) {
	cfg := parseForEdit(t, managedEndpointSource)
	setManaged(&cfg.Routes[0], "billing", "invoice.paid")

	out, err := RewriteManagedEndpoints([]byte(managedEndpointSource), cfg)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `  endpoint_name "invoice.paid"`) {
		t.Fatalf("value was not replaced in place:\n%s", got)
	}
	if before, after := withoutManagedLines(managedEndpointSource), withoutManagedLines(got); before != after {
		t.Fatalf("rewrite touched more than the managed-endpoint line:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	if strings.Contains(got, "invoice.created") {
		t.Fatalf("old value survived:\n%s", got)
	}
}

func TestRewriteManagedEndpoints_ClearKeepsTrailingComment(t *testing.T) {
	src := `pull_api { auth token "raw:t" }
"/a" {
  application "billing" # owned by billing
  endpoint_name "invoice.created"
  pull { path "/e1" }
}
`
	cfg := parseForEdit(t, src)
	clearManaged(&cfg.Routes[0])

	out, err := RewriteManagedEndpoints([]byte(src), cfg)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "# owned by billing") {
		t.Fatalf("trailing comment was dropped:\n%s", got)
	}
	if strings.Contains(got, "application ") || strings.Contains(got, "endpoint_name ") {
		t.Fatalf("directives were not removed:\n%s", got)
	}
}

func TestRewriteManagedEndpoints_InsertUsesBodyIndent(t *testing.T) {
	src := `pull_api { auth token "raw:t" }
"/a" {
    pull { path "/e1" }
}
`
	cfg := parseForEdit(t, src)
	setManaged(&cfg.Routes[0], "billing", "invoice.created")

	out, err := RewriteManagedEndpoints([]byte(src), cfg)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "\n    application \"billing\"\n    endpoint_name \"invoice.created\"\n") {
		t.Fatalf("inserted directives do not follow the body indentation:\n%s", got)
	}
}

func TestRewriteManagedEndpoints_CRLFSource(t *testing.T) {
	src := strings.ReplaceAll(managedEndpointSource, "\n", "\r\n")
	cfg := parseForEdit(t, src)
	setManaged(&cfg.Routes[1], "billing", "invoice.created")
	clearManaged(&cfg.Routes[0])

	out, err := RewriteManagedEndpoints([]byte(src), cfg)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	got := string(out)
	if strings.Contains(got, "\r") {
		t.Fatalf("output kept carriage returns, Parse would normalize them away:\n%q", got)
	}
	if !strings.Contains(got, "# mapped by the Admin API") {
		t.Fatalf("comment was dropped from CRLF source:\n%s", got)
	}
}

// A body that holds everything on one line is not spliced: inserting a
// directive line there would produce a mangled block, so the caller falls back
// to the canonical rewrite.
func TestRewriteManagedEndpoints_SingleLineBodyUnsupported(t *testing.T) {
	src := `pull_api { auth token "raw:t" }
"/a" { pull { path "/e1" } }
`
	cfg := parseForEdit(t, src)
	setManaged(&cfg.Routes[0], "billing", "invoice.created")

	if _, err := RewriteManagedEndpoints([]byte(src), cfg); !errors.Is(err, ErrRewriteUnsupported) {
		t.Fatalf("expected ErrRewriteUnsupported, got %v", err)
	}
}

// The self-check is what keeps the splice honest: any change the rewrite does
// not model must be reported rather than silently written to disk.
func TestRewriteManagedEndpoints_UnmodelledChangeUnsupported(t *testing.T) {
	cfg := parseForEdit(t, managedEndpointSource)
	setManaged(&cfg.Routes[1], "billing", "invoice.created")
	cfg.Routes[1].Pull.Path = "/moved"

	if _, err := RewriteManagedEndpoints([]byte(managedEndpointSource), cfg); !errors.Is(err, ErrRewriteUnsupported) {
		t.Fatalf("expected ErrRewriteUnsupported for an unmodelled change, got %v", err)
	}
}

func TestRewriteManagedEndpoints_RouteCountMismatch(t *testing.T) {
	cfg := parseForEdit(t, managedEndpointSource)
	cfg.Routes = cfg.Routes[:1]

	if _, err := RewriteManagedEndpoints([]byte(managedEndpointSource), cfg); !errors.Is(err, ErrRewriteUnsupported) {
		t.Fatalf("expected ErrRewriteUnsupported, got %v", err)
	}
}

func TestRewriteManagedEndpoints_NoChangeIsByteIdentical(t *testing.T) {
	cfg := parseForEdit(t, managedEndpointSource)

	out, err := RewriteManagedEndpoints([]byte(managedEndpointSource), cfg)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if string(out) != managedEndpointSource {
		t.Fatalf("no-op rewrite changed the file:\n%s", out)
	}
}

func TestHasInBodyComments(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want bool
	}{
		{name: "preamble only", src: "# header\npull_api { auth token \"raw:t\" }\n", want: false},
		{name: "in body", src: "pull_api { auth token \"raw:t\" }\n# note\n\"/a\" { pull { path \"/e1\" } }\n", want: true},
		{name: "inside block", src: "pull_api {\n  # note\n  auth token \"raw:t\"\n}\n", want: true},
		{name: "none", src: "pull_api { auth token \"raw:t\" }\n", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := HasInBodyComments([]byte(tc.src)); got != tc.want {
				t.Fatalf("HasInBodyComments = %v, want %v", got, tc.want)
			}
		})
	}
}

// withoutManagedLines drops the managed-endpoint directive lines so a test can
// assert that a rewrite left every other byte of the file alone.
func withoutManagedLines(src string) string {
	var kept []string
	for _, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "application ") || strings.HasPrefix(trimmed, "endpoint_name ") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}
