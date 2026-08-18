package config

import "testing"

func FuzzParseFormatRoundTrip(f *testing.F) {
	f.Add([]byte(`"/hooks" { pull { path /pull/hooks } }`))
	f.Add([]byte(`
ingress { listen :8080 }
pull_api { auth token "raw:test-token" }
"/hooks" { pull { path /pull/hooks } }
`))
	f.Add([]byte(`
secrets {
  secret "S1" {
    value "raw:s1"
    valid_from "2026-01-01T00:00:00Z"
  }
}
"/x" {
  auth hmac secret_ref "S1"
  pull { path "/pull/x" }
}
`))
	// Values that only survive the round trip if the formatter re-quotes them.
	// The target's assertions were always strong enough to catch a formatter
	// dropping quotes -- what was missing was an input that carried any. A
	// quoted `env` key with a space reformatted to `env MY KEY "v"`, which no
	// longer parses, and nothing in the corpus reached that line.
	f.Add([]byte(`
"/x" {
  deliver exec "/opt/hooks/run.sh" {
    env "MY KEY" "value with spaces"
    env PLAIN plain
  }
}
`))

	// Backslashes: unknown escapes used to lose theirs, and the formatter has
	// to re-escape what survives or the second parse sees a different value.
	f.Add([]byte(`
"/x" {
  deliver "https://example.org/x" {
    header "X-Path" "C:\certs\server.pem"
    header "X-Regex" "^/hooks/\d+$"
  }
}
`))

	f.Fuzz(func(t *testing.T, input []byte) {
		cfg, err := Parse(input)
		if err != nil {
			return
		}

		formatted, err := Format(cfg)
		if err != nil {
			t.Fatalf("format parsed config: %v", err)
		}

		cfg2, err := Parse(formatted)
		if err != nil {
			t.Fatalf("parse formatted config: %v\nformatted:\n%s", err, string(formatted))
		}

		again, err := Format(cfg2)
		if err != nil {
			t.Fatalf("format re-parsed config: %v", err)
		}

		// `config fmt` is required to be stable, and the file it writes is the
		// one the next parse reads: if the second format differs from the
		// first, formatting a config repeatedly keeps changing the operator's
		// file, and the value the runtime sees drifts with it.
		if string(again) != string(formatted) {
			t.Fatalf("format is not stable:\nfirst:\n%s\nsecond:\n%s", string(formatted), string(again))
		}

		_ = ValidateWithResult(cfg2)
	})
}
