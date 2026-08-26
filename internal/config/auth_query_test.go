package config

import (
	"strings"
	"testing"
)

func compileText(t *testing.T, in string) (Compiled, ValidationResult) {
	t.Helper()
	cfg, err := Parse([]byte(in))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	return compiled, res
}

func routeByPath(t *testing.T, compiled Compiled, path string) CompiledRoute {
	t.Helper()
	for _, rt := range compiled.Routes {
		if rt.Path == path {
			return rt
		}
	}
	t.Fatalf("route %q not found", path)
	return CompiledRoute{}
}

func TestCompile_AuthQuery_LiteralSecret(t *testing.T) {
	compiled, res := compileText(t, `
pull_api { auth token "raw:devtoken" }

"/webhooks/source" {
  auth query "t" "raw:url-token"
  pull { path "/source" }
}
`)
	if !res.OK {
		t.Fatalf("compile: %s", FormatValidationText(res))
	}
	rt := routeByPath(t, compiled, "/webhooks/source")
	if !rt.AuthQuery.Enabled {
		t.Fatal("expected auth query to be enabled")
	}
	if rt.AuthQuery.Param != "t" {
		t.Fatalf("param: got %q", rt.AuthQuery.Param)
	}
	if len(rt.AuthQuery.Secrets) != 1 || rt.AuthQuery.Secrets[0] != "raw:url-token" {
		t.Fatalf("secrets: got %#v", rt.AuthQuery.Secrets)
	}
	if len(rt.AuthQuery.SecretRefs) != 0 {
		t.Fatalf("secret refs: got %#v", rt.AuthQuery.SecretRefs)
	}
}

func TestCompile_AuthQuery_SecretRefAndLiteralCombined(t *testing.T) {
	compiled, res := compileText(t, `
secrets {
  secret "source-token" {
    value "raw:v2"
    valid_from "2026-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:devtoken" }

"/webhooks/source" {
  auth query "t" secret_ref "source-token"
  auth query "t" "raw:v1"
  pull { path "/source" }
}
`)
	if !res.OK {
		t.Fatalf("compile: %s", FormatValidationText(res))
	}
	rt := routeByPath(t, compiled, "/webhooks/source")
	if len(rt.AuthQuery.SecretRefs) != 1 || rt.AuthQuery.SecretRefs[0] != "source-token" {
		t.Fatalf("secret refs: got %#v", rt.AuthQuery.SecretRefs)
	}
	if len(rt.AuthQuery.Secrets) != 1 || rt.AuthQuery.Secrets[0] != "raw:v1" {
		t.Fatalf("secrets: got %#v", rt.AuthQuery.Secrets)
	}
}

func TestCompile_AuthQuery_Rejections(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantSub string
	}{
		{
			name: "unknown secret_ref",
			config: `
pull_api { auth token "raw:devtoken" }
"/x" {
  auth query "t" secret_ref "absent"
  pull { path "/x" }
}
`,
			wantSub: `auth query secret_ref "absent" not found`,
		},
		{
			name: "duplicate secret_ref",
			config: `
secrets {
  secret "tok" { value "raw:v1" valid_from "2026-01-01T00:00:00Z" }
}
pull_api { auth token "raw:devtoken" }
"/x" {
  auth query "t" secret_ref "tok"
  auth query "t" secret_ref "tok"
  pull { path "/x" }
}
`,
			wantSub: "auth query secret_ref duplicate",
		},
		{
			name: "invalid secret reference",
			config: `
pull_api { auth token "raw:devtoken" }
"/x" {
  auth query "t" "bogus:thing"
  pull { path "/x" }
}
`,
			wantSub: "auth query secret[0]",
		},
		{
			name: "combined with auth hmac",
			config: `
pull_api { auth token "raw:devtoken" }
"/x" {
  auth query "t" "raw:tok"
  auth hmac "raw:sig"
  pull { path "/x" }
}
`,
			wantSub: "auth query cannot be combined",
		},
		{
			name: "combined with auth basic",
			config: `
pull_api { auth token "raw:devtoken" }
"/x" {
  auth query "t" "raw:tok"
  auth basic "u" "p"
  pull { path "/x" }
}
`,
			wantSub: "auth query cannot be combined",
		},
		{
			name: "combined with auth forward",
			config: `
pull_api { auth token "raw:devtoken" }
"/x" {
  auth query "t" "raw:tok"
  auth forward "https://auth.example/check"
  pull { path "/x" }
}
`,
			wantSub: "auth query cannot be combined",
		},
		{
			// Both would read the same parameter and the matcher runs first, so
			// a wrong token would fall through to a later route instead of being
			// rejected here.
			name: "collides with match query on the same parameter",
			config: `
pull_api { auth token "raw:devtoken" }
"/x" {
  auth query "t" "raw:tok"
  match { query "t" "tok" }
  pull { path "/x" }
}
`,
			wantSub: "collides with match query",
		},
		{
			name: "collides with match query_exists on the same parameter",
			config: `
pull_api { auth token "raw:devtoken" }
"/x" {
  auth query "t" "raw:tok"
  match { query_exists "t" }
  pull { path "/x" }
}
`,
			wantSub: "collides with match query_exists",
		},
		{
			name: "collides with a named matcher's query",
			config: `
pull_api { auth token "raw:devtoken" }
@named { query "t" "tok" }
"/x" {
  auth query "t" "raw:tok"
  match @named
  pull { path "/x" }
}
`,
			wantSub: "collides with match query",
		},
		{
			name: "forbidden on an outbound route",
			config: `
pull_api { auth token "raw:devtoken" }
outbound {
  "/x" {
    auth query "t" "raw:tok"
    deliver "https://example.org" {}
  }
}
`,
			wantSub: "auth is forbidden",
		},
		{
			name: "forbidden on an internal route",
			config: `
pull_api { auth token "raw:devtoken" }
internal {
  "/x" {
    auth query "t" "raw:tok"
    pull { path "/x" }
  }
}
`,
			wantSub: "auth is forbidden",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, res := compileText(t, tt.config)
			if res.OK {
				t.Fatal("expected compilation to fail")
			}
			text := FormatValidationText(res)
			if !strings.Contains(text, tt.wantSub) {
				t.Fatalf("expected error containing %q, got:\n%s", tt.wantSub, text)
			}
		})
	}
}

// A different parameter name on a second `auth query` is a parse error rather
// than a silent last-one-wins, since only one parameter is ever read.
func TestParse_AuthQuery_ParamMismatchIsRejected(t *testing.T) {
	_, err := Parse([]byte(`
"/x" {
  auth query "t" "raw:a"
  auth query "u" "raw:b"
  pull { path "/x" }
}
`))
	if err == nil {
		t.Fatal("expected a parse error for two different parameter names")
	}
	if !strings.Contains(err.Error(), "auth query parameter mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// A parameter with no usable secret must not compile. run.go maps a route with
// no secrets to a nil authenticator, and ingress reads nil as "no auth", so this
// would have served the route wide open.
func TestCompile_AuthQuery_ParamWithoutSecretIsRejected(t *testing.T) {
	_, err := Parse([]byte(`
"/x" {
  auth query "t"
  pull { path "/x" }
}
`))
	if err == nil {
		t.Fatal("expected `auth query` without a secret to be rejected")
	}
}

func TestCompile_AuthQuery_EmptySecretIsRejected(t *testing.T) {
	_, res := compileText(t, `
vars { EMPTY "" }
pull_api { auth token "raw:devtoken" }
"/x" {
  auth query "t" "{vars.EMPTY}"
  pull { path "/x" }
}
`)
	if res.OK {
		t.Fatal("expected an empty secret to fail compilation")
	}
	text := FormatValidationText(res)
	if !strings.Contains(text, "auth query") {
		t.Fatalf("expected the error to name auth query, got:\n%s", text)
	}
}

func TestCompile_AuthQuery_MatchQueryOnADifferentParameterIsFine(t *testing.T) {
	compiled, res := compileText(t, `
pull_api { auth token "raw:devtoken" }
"/x" {
  auth query "t" "raw:tok"
  match { query "env" "production" }
  pull { path "/x" }
}
`)
	if !res.OK {
		t.Fatalf("compile: %s", FormatValidationText(res))
	}
	rt := routeByPath(t, compiled, "/x")
	if !rt.AuthQuery.Enabled {
		t.Fatal("expected auth query to stay enabled")
	}
	if len(rt.Match.Query) != 1 {
		t.Fatalf("expected the unrelated matcher to survive, got %#v", rt.Match.Query)
	}
}

func TestFormat_AuthQueryRoundTrips(t *testing.T) {
	in := []byte(`"/webhooks/source" {
  auth query "t" secret_ref "source-token"
  auth query "t" "env:URL_TOKEN"
  pull {
    path "/source"
  }
}
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	first, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(first)
	if !strings.Contains(got, `auth query "t" secret_ref "source-token"`) {
		t.Fatalf("expected the secret_ref form to be written back, got:\n%s", got)
	}
	if !strings.Contains(got, `auth query "t" "env:URL_TOKEN"`) {
		t.Fatalf("expected the literal form to be written back, got:\n%s", got)
	}

	// config fmt has to be stable.
	reparsed, err := Parse(first)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	second, err := Format(reparsed)
	if err != nil {
		t.Fatalf("reformat: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("format is not stable:\nfirst:\n%s\nsecond:\n%s", first, second)
	}
}

// --strict-secrets preflight has to cover auth query secrets too, or a route
// whose token env var is not deployed passes validation and fails at startup.
func TestPreflight_CoversAuthQuerySecrets(t *testing.T) {
	compiled, res := compileText(t, `
pull_api { auth token "raw:devtoken" }

"/webhooks/source" {
  auth query "t" "env:HOOKAIDO_TEST_ABSENT_URL_TOKEN"
  pull { path "/source" }
}
`)
	if !res.OK {
		t.Fatalf("compile: %s", FormatValidationText(res))
	}

	errs := validateSecretPreflight(compiled)
	joined := strings.Join(errs, "\n")
	if !strings.Contains(joined, "auth query secret[0]") {
		t.Fatalf("expected the preflight to name the auth query secret, got:\n%s", joined)
	}
}
