package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseFormat_AttemptsRetentionBlock(t *testing.T) {
	in := []byte(`
attempts_retention {
  max_age "3d"
  max_rows 50000
}

pull_api { auth token "raw:t" }

"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.AttemptsRetention == nil {
		t.Fatal("expected an attempts_retention block")
	}

	out, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `max_age "3d"`) {
		t.Fatalf("expected quoted max_age, got:\n%s", got)
	}
	if !strings.Contains(got, "max_rows 50000") {
		t.Fatalf("expected max_rows 50000, got:\n%s", got)
	}

	again, err := Parse(out)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	second, err := Format(again)
	if err != nil {
		t.Fatalf("reformat: %v", err)
	}
	if string(second) != got {
		t.Fatalf("format is not stable:\n%s\n---\n%s", got, second)
	}
}

func TestCompile_AttemptsRetention(t *testing.T) {
	const preamble = `
pull_api { auth token "raw:t" }
"/x" { pull { path "/e" } }
`

	tests := []struct {
		name      string
		block     string
		wantAge   time.Duration
		wantRows  int
		wantOn    bool
		wantError string
	}{
		{
			name:     "defaults are finite",
			block:    "",
			wantAge:  7 * 24 * time.Hour,
			wantRows: 200000,
			wantOn:   true,
		},
		{
			name:     "explicit values",
			block:    "attempts_retention {\n  max_age \"12h\"\n  max_rows 1000\n}\n",
			wantAge:  12 * time.Hour,
			wantRows: 1000,
			wantOn:   true,
		},
		{
			name:     "age off keeps the row cap",
			block:    "attempts_retention {\n  max_age off\n}\n",
			wantAge:  0,
			wantRows: 200000,
			wantOn:   true,
		},
		{
			name:     "both off disables retention",
			block:    "attempts_retention {\n  max_age off\n  max_rows off\n}\n",
			wantAge:  0,
			wantRows: 0,
			wantOn:   false,
		},
		{
			name:      "negative rows rejected",
			block:     "attempts_retention {\n  max_rows -1\n}\n",
			wantError: "attempts_retention.max_rows must be a non-negative integer",
		},
		{
			name:      "invalid age rejected",
			block:     "attempts_retention {\n  max_age \"soon\"\n}\n",
			wantError: "attempts_retention.max_age",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Parse([]byte(tc.block + preamble))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			compiled, res := Compile(cfg)

			if tc.wantError != "" {
				if res.OK {
					t.Fatalf("expected a validation error containing %q", tc.wantError)
				}
				joined := strings.Join(res.Errors, "; ")
				if !strings.Contains(joined, tc.wantError) {
					t.Fatalf("errors %q do not contain %q", joined, tc.wantError)
				}
				return
			}

			if !res.OK {
				t.Fatalf("compile: %v", res.Errors)
			}
			got := compiled.AttemptsRetention
			if got.MaxAge != tc.wantAge {
				t.Fatalf("MaxAge = %v, want %v", got.MaxAge, tc.wantAge)
			}
			if got.MaxRows != tc.wantRows {
				t.Fatalf("MaxRows = %d, want %d", got.MaxRows, tc.wantRows)
			}
			if got.Enabled != tc.wantOn {
				t.Fatalf("Enabled = %v, want %v", got.Enabled, tc.wantOn)
			}
		})
	}
}

func TestCompile_AttemptsRetentionRequiresPruneInterval(t *testing.T) {
	in := []byte(`
queue_retention {
  max_age off
  prune_interval off
}
attempts_retention {
  max_age "12h"
}
pull_api { auth token "raw:t" }
"/x" { pull { path "/e" } }
`)

	cfg, err := Parse(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, res := Compile(cfg)
	if res.OK {
		t.Fatal("expected retention without a prune interval to be rejected")
	}
	if !strings.Contains(strings.Join(res.Errors, "; "), "queue_retention.prune_interval") {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
}

func TestParse_AttemptsRetentionRejectsDuplicatesAndUnknownDirectives(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "duplicate block",
			in:   "attempts_retention { max_age \"1h\" }\nattempts_retention { max_age \"2h\" }\n",
			want: "duplicate attempts_retention block",
		},
		{
			name: "duplicate max_age",
			in:   "attempts_retention {\n  max_age \"1h\"\n  max_age \"2h\"\n}\n",
			want: "duplicate attempts_retention max_age",
		},
		{
			name: "duplicate max_rows",
			in:   "attempts_retention {\n  max_rows 1\n  max_rows 2\n}\n",
			want: "duplicate attempts_retention max_rows",
		},
		{
			name: "unknown directive",
			in:   "attempts_retention {\n  max_depth 5\n}\n",
			want: `unknown attempts_retention directive "max_depth"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.in))
			if err == nil {
				t.Fatalf("expected an error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}
