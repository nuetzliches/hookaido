package workerapi

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

// ctxWithAuth builds a gRPC incoming context with an Authorization header.
func ctxWithAuth(token string) context.Context {
	md := metadata.Pairs("authorization", token)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestBearerTokenAuthorizer(t *testing.T) {
	tests := []struct {
		name   string
		tokens [][]byte
		ctx    context.Context
		want   bool
	}{
		{
			name:   "valid token accepted",
			tokens: [][]byte{[]byte("secret123")},
			ctx:    ctxWithAuth("Bearer secret123"),
			want:   true,
		},
		{
			name:   "invalid token rejected",
			tokens: [][]byte{[]byte("secret123")},
			ctx:    ctxWithAuth("Bearer wrong-token"),
			want:   false,
		},
		{
			name:   "empty token list allows everything",
			tokens: [][]byte{},
			ctx:    ctxWithAuth("Bearer anything"),
			want:   true,
		},
		{
			name:   "nil token list allows everything",
			tokens: nil,
			ctx:    ctxWithAuth("Bearer anything"),
			want:   true,
		},
		{
			name:   "no gRPC metadata in context rejected",
			tokens: [][]byte{[]byte("secret123")},
			ctx:    context.Background(),
			want:   false,
		},
		{
			name:   "wrong prefix rejected",
			tokens: [][]byte{[]byte("secret123")},
			ctx:    ctxWithAuth("Basic secret123"),
			want:   false,
		},
		{
			name:   "case-insensitive bearer prefix",
			tokens: [][]byte{[]byte("secret123")},
			ctx:    ctxWithAuth("bearer secret123"),
			want:   true,
		},
		{
			name:   "case-insensitive BEARER prefix",
			tokens: [][]byte{[]byte("secret123")},
			ctx:    ctxWithAuth("BEARER secret123"),
			want:   true,
		},
		{
			name:   "empty token after Bearer rejected",
			tokens: [][]byte{[]byte("secret123")},
			ctx:    ctxWithAuth("Bearer "),
			want:   false,
		},
		{
			name:   "whitespace-only token after Bearer rejected",
			tokens: [][]byte{[]byte("secret123")},
			ctx:    ctxWithAuth("Bearer    "),
			want:   false,
		},
		{
			name:   "leading whitespace trimmed from header",
			tokens: [][]byte{[]byte("secret123")},
			ctx:    ctxWithAuth("  Bearer secret123"),
			want:   true,
		},
		{
			name:   "trailing whitespace trimmed from token",
			tokens: [][]byte{[]byte("secret123")},
			ctx:    ctxWithAuth("Bearer secret123  "),
			want:   true,
		},
		{
			name:   "multiple allowed tokens any one suffices",
			tokens: [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")},
			ctx:    ctxWithAuth("Bearer beta"),
			want:   true,
		},
		{
			name:   "multiple allowed tokens none match",
			tokens: [][]byte{[]byte("alpha"), []byte("beta")},
			ctx:    ctxWithAuth("Bearer delta"),
			want:   false,
		},
		{
			name:   "empty tokens in config are filtered out",
			tokens: [][]byte{[]byte(""), []byte("valid"), []byte("")},
			ctx:    ctxWithAuth("Bearer valid"),
			want:   true,
		},
		{
			name:   "all-empty tokens in config means no auth required",
			tokens: [][]byte{[]byte(""), []byte("")},
			ctx:    ctxWithAuth("Bearer anything"),
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			auth := BearerTokenAuthorizer(tt.tokens)
			got := auth(tt.ctx, "/some.Service/Method")
			if got != tt.want {
				t.Errorf("BearerTokenAuthorizer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBearerTokenAuthorizer_InputIsolation(t *testing.T) {
	original := []byte("secret")
	tokens := [][]byte{original}
	auth := BearerTokenAuthorizer(tokens)

	// Mutate the original slice after creating the authorizer.
	original[0] = 'X'

	// The authorizer should still accept the original token, not the mutated one.
	got := auth(ctxWithAuth("Bearer secret"), "/test")
	if !got {
		t.Error("authorizer should use a copy of the token, not a reference to the original")
	}
}

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, ok := parseBearerToken(tt.raw)
			if ok != tt.wantOK {
				t.Errorf("parseBearerToken(%q) ok = %v, want %v", tt.raw, ok, tt.wantOK)
			}
			if token != tt.wantToken {
				t.Errorf("parseBearerToken(%q) token = %q, want %q", tt.raw, token, tt.wantToken)
			}
		})
	}
}
