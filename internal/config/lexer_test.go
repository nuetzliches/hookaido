package config

import (
	"strings"
	"testing"
)

// Unknown escapes used to lose their backslash, so a quoted Windows path was
// silently corrupted (`"C:\certs\server.pem"` parsed as `C:certsserver.pem`)
// and `config fmt` -- or an Admin API config rewrite -- then persisted the
// corruption.
func TestParseQuotedString_UnknownEscapesKeepBackslash(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "windows path", in: `C:\certs\server.pem`, want: `C:\certs\server.pem`},
		{name: "regex class", in: `^/hooks/\d+$`, want: `^/hooks/\d+$`},
		{name: "escaped backslash", in: `a\\b`, want: `a\b`},
		{name: "escaped quote", in: `say \"hi\"`, want: `say "hi"`},
		{name: "newline", in: `line1\nline2`, want: "line1\nline2"},
		{name: "tab", in: `a\tb`, want: "a\tb"},
		{name: "carriage return", in: `a\rb`, want: "a\rb"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := "ingress {\n  listen \"" + tc.in + "\"\n}\n"
			cfg, err := Parse([]byte(src))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := cfg.Ingress.Listen; got != tc.want {
				t.Fatalf("listen = %q, want %q", got, tc.want)
			}
		})
	}
}

// The fix is only worth anything if the value also survives the formatter,
// since `config fmt` and the Admin API config rewrite both write Format's
// output back to the operator's file.
func TestFormatRoundTrip_PreservesBackslashes(t *testing.T) {
	const want = `C:\certs\server.pem`
	src := "ingress {\n  listen \"" + want + "\"\n}\n"

	cfg, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	formatted, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	if !strings.Contains(string(formatted), `"C:\\certs\\server.pem"`) {
		t.Fatalf("formatter did not re-escape the backslashes:\n%s", formatted)
	}

	reparsed, err := Parse(formatted)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	if got := reparsed.Ingress.Listen; got != want {
		t.Fatalf("value after round trip = %q, want %q", got, want)
	}

	again, err := Format(reparsed)
	if err != nil {
		t.Fatalf("reformat: %v", err)
	}
	if string(again) != string(formatted) {
		t.Fatalf("format is not stable:\nfirst:\n%s\nsecond:\n%s", formatted, again)
	}
}

// parseValue is the one call site that reaches the lexer without peeking
// first, so it has to propagate the lexer's error instead of reporting
// "expected value" at the impossible position 0:0.
func TestParseValue_ReportsLexerError(t *testing.T) {
	tests := []struct {
		name    string
		src     string
		wantSub string
	}{
		{
			name:    "unterminated string",
			src:     "ingress {\n  listen \"oops\n}\n",
			wantSub: "unterminated string at 2:",
		},
		{
			name:    "invalid utf-8",
			src:     "ingress {\n  listen \"a" + string([]byte{0xff}) + "b\"\n}\n",
			wantSub: "invalid utf-8 at 2:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.src))
			if err == nil {
				t.Fatal("expected a parse error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not contain %q", err, tc.wantSub)
			}
			if strings.Contains(err.Error(), "0:0") {
				t.Fatalf("error still reports the zero position: %v", err)
			}
		})
	}
}
