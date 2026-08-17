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
