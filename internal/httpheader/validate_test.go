package httpheader

import "testing"

func TestValidateMap(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		wantErr bool
	}{
		{
			name:    "empty",
			headers: nil,
		},
		{
			name: "valid",
			headers: map[string]string{
				"X-Event":       "push",
				"Content-Type":  "application/json",
				"X-Custom-Flag": "ok\tvalue",
			},
		},
		{
			name: "invalid_name_space",
			headers: map[string]string{
				"X Event": "push",
			},
			wantErr: true,
		},
		{
			name: "invalid_name_whitespace_prefix",
			headers: map[string]string{
				" X-Event": "push",
			},
			wantErr: true,
		},
		{
			name: "invalid_value_newline",
			headers: map[string]string{
				"X-Event": "push\nother",
			},
			wantErr: true,
		},
		{
			name: "invalid_value_ctrl",
			headers: map[string]string{
				"X-Event": "push\x01",
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateMap(tc.headers)
			if tc.wantErr && err == nil {
				t.Fatalf("expected validation error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}
