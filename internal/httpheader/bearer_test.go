package httpheader

import "testing"

// Table moved here from internal/workerapi, which held the only implementation
// of this parsing before the Pull and Admin HTTP APIs started sharing it.
func TestParseBearerToken(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantToken string
		wantOK    bool
	}{
		{
			name:      "valid bearer token",
			raw:       "Bearer mytoken",
			wantToken: "mytoken",
			wantOK:    true,
		},
		{
			name:      "too short input",
			raw:       "Bear",
			wantToken: "",
			wantOK:    false,
		},
		{
			name:      "exactly 6 chars (too short)",
			raw:       "Bearer",
			wantToken: "",
			wantOK:    false,
		},
		{
			name:      "Bearer with empty token",
			raw:       "Bearer ",
			wantToken: "",
			wantOK:    false,
		},
		{
			name:      "Bearer with whitespace-only token",
			raw:       "Bearer   ",
			wantToken: "",
			wantOK:    false,
		},
		{
			name:      "wrong prefix",
			raw:       "Token mytoken",
			wantToken: "",
			wantOK:    false,
		},
		{
			name:      "case-insensitive prefix",
			raw:       "bEaReR mytoken",
			wantToken: "mytoken",
			wantOK:    true,
		},
		{
			name:      "lowercase prefix",
			raw:       "bearer mytoken",
			wantToken: "mytoken",
			wantOK:    true,
		},
		{
			name:      "uppercase prefix",
			raw:       "BEARER mytoken",
			wantToken: "mytoken",
			wantOK:    true,
		},
		{
			name:      "leading and trailing whitespace",
			raw:       "  Bearer mytoken  ",
			wantToken: "mytoken",
			wantOK:    true,
		},
		{
			name:      "empty string",
			raw:       "",
			wantToken: "",
			wantOK:    false,
		},
		{
			name:      "scheme without separating space",
			raw:       "Bearermytoken",
			wantToken: "",
			wantOK:    false,
		},
		{
			name:      "token containing spaces keeps inner text",
			raw:       "Bearer two words",
			wantToken: "two words",
			wantOK:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, ok := ParseBearerToken(tt.raw)
			if ok != tt.wantOK {
				t.Errorf("ParseBearerToken(%q) ok = %v, want %v", tt.raw, ok, tt.wantOK)
			}
			if token != tt.wantToken {
				t.Errorf("ParseBearerToken(%q) token = %q, want %q", tt.raw, token, tt.wantToken)
			}
		})
	}
}
