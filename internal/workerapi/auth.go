package workerapi

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc/metadata"
)

// Authorizer decides whether a worker API request is authorized.
type Authorizer func(ctx context.Context, endpoint string) bool

// BearerTokenAuthorizer validates gRPC metadata Authorization headers.
// Expected format: "Authorization: Bearer <token>".
func BearerTokenAuthorizer(tokens [][]byte) Authorizer {
	allowed := make([][]byte, 0, len(tokens))
	for _, t := range tokens {
		if len(t) == 0 {
			continue
		}
		cp := make([]byte, len(t))
		copy(cp, t)
		allowed = append(allowed, cp)
	}

	return func(ctx context.Context, _ string) bool {
		if len(allowed) == 0 {
			return true
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return false
		}
		values := md.Get("authorization")
		for _, raw := range values {
			token, ok := parseBearerToken(raw)
			if !ok {
				continue
			}
			gb := []byte(token)
			for _, want := range allowed {
				if subtle.ConstantTimeCompare(gb, want) == 1 {
					return true
				}
			}
		}
		return false
	}
}

func parseBearerToken(raw string) (string, bool) {
	h := strings.TrimSpace(raw)
	if len(h) < 7 {
		return "", false
	}
	if !strings.EqualFold(h[:7], "Bearer ") {
		return "", false
	}
	token := strings.TrimSpace(h[7:])
	if token == "" {
		return "", false
	}
	return token, true
}
