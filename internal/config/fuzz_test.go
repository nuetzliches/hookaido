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

		if _, err := Format(cfg2); err != nil {
			t.Fatalf("format re-parsed config: %v", err)
		}

		_ = ValidateWithResult(cfg2)
	})
}
