package pullapi

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/nuetzliches/hookaido/v2/internal/httpheader"
)

type Authorizer func(r *http.Request) bool

// TokenIdentifier names the configured token reference a request
// authenticated with — never the token value. See BearerTokenIdentifier.
type TokenIdentifier func(r *http.Request) string

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

	return func(r *http.Request) bool {
		if len(allowed) == 0 {
			return true
		}

		got, ok := httpheader.ParseBearerToken(r.Header.Get("Authorization"))
		if !ok {
			return false
		}
		gb := []byte(got)
		for _, want := range allowed {
			if subtle.ConstantTimeCompare(gb, want) == 1 {
				return true
			}
		}
		return false
	}
}

// BearerTokenIdentifier names which configured token reference a request
// authenticated with, for the consumer registry.
//
// refs and tokens are index-aligned as the config resolves them; a ref without
// a token, or a token past the end of refs, is skipped rather than reported
// under the wrong name. The return value is the reference (`env.PULL_TOKEN`),
// never the secret — an operator needs to map an unexpected consumer back to a
// Hookaidofile line, and a token value in an Admin API response or a log line
// would be a credential leak by way of a diagnostic.
//
// It returns "" when nothing matches, including when the Pull API runs without
// tokens at all. That is not an authorization decision: the authorizer above
// has already made it by the time this is consulted.
func BearerTokenIdentifier(refs []string, tokens [][]byte) TokenIdentifier {
	type labeled struct {
		ref   string
		token []byte
	}

	allowed := make([]labeled, 0, len(tokens))
	for i, t := range tokens {
		if len(t) == 0 || i >= len(refs) {
			continue
		}
		ref := strings.TrimSpace(refs[i])
		if ref == "" {
			continue
		}
		cp := make([]byte, len(t))
		copy(cp, t)
		allowed = append(allowed, labeled{ref: ref, token: cp})
	}

	return func(r *http.Request) string {
		if len(allowed) == 0 || r == nil {
			return ""
		}
		got, ok := httpheader.ParseBearerToken(r.Header.Get("Authorization"))
		if !ok {
			return ""
		}
		gb := []byte(got)
		for _, want := range allowed {
			if subtle.ConstantTimeCompare(gb, want.token) == 1 {
				return want.ref
			}
		}
		return ""
	}
}
