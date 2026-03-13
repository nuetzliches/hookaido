package router

import "testing"

func TestMatchPath(t *testing.T) {
	tests := []struct {
		name        string
		requestPath string
		routePath   string
		want        bool
	}{
		// Exact match
		{
			name:        "exact match",
			requestPath: "/webhooks/stripe",
			routePath:   "/webhooks/stripe",
			want:        true,
		},
		// Prefix match on segment boundary
		{
			name:        "prefix match on segment boundary",
			requestPath: "/webhooks/stripe/events",
			routePath:   "/webhooks/stripe",
			want:        true,
		},
		// No partial (non-boundary) match
		{
			name:        "no partial match within segment",
			requestPath: "/webhooks/stripeXYZ",
			routePath:   "/webhooks/stripe",
			want:        false,
		},
		// Root route matches everything
		{
			name:        "root route matches any path",
			requestPath: "/webhooks/stripe",
			routePath:   "/",
			want:        true,
		},
		{
			name:        "root route matches root request",
			requestPath: "/",
			routePath:   "/",
			want:        true,
		},
		// Empty route path matches nothing
		{
			name:        "empty route matches nothing",
			requestPath: "/webhooks/stripe",
			routePath:   "",
			want:        false,
		},
		{
			name:        "empty route with empty request",
			requestPath: "",
			routePath:   "",
			want:        false,
		},
		// Trailing slash variations
		{
			name:        "route with trailing slash exact match",
			requestPath: "/webhooks/stripe/",
			routePath:   "/webhooks/stripe/",
			want:        true,
		},
		{
			name:        "request with trailing slash vs route without",
			requestPath: "/webhooks/stripe/",
			routePath:   "/webhooks/stripe",
			want:        true,
		},
		{
			name:        "request without trailing slash vs route with",
			requestPath: "/webhooks/stripe",
			routePath:   "/webhooks/stripe/",
			want:        false,
		},
		// Same length, no match
		{
			name:        "same length different paths",
			requestPath: "/webhooks/github",
			routePath:   "/webhooks/stripe",
			want:        false,
		},
		// Shorter request than route
		{
			name:        "request shorter than route",
			requestPath: "/webhooks",
			routePath:   "/webhooks/stripe",
			want:        false,
		},
		// Additional edge cases
		{
			name:        "deeply nested prefix match",
			requestPath: "/a/b/c/d/e",
			routePath:   "/a/b",
			want:        true,
		},
		{
			name:        "single segment exact match",
			requestPath: "/hooks",
			routePath:   "/hooks",
			want:        true,
		},
		{
			name:        "empty request with root route",
			requestPath: "",
			routePath:   "/",
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchPath(tt.requestPath, tt.routePath)
			if got != tt.want {
				t.Errorf("MatchPath(%q, %q) = %v, want %v",
					tt.requestPath, tt.routePath, got, tt.want)
			}
		})
	}
}
