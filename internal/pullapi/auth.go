package pullapi

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

type Authorizer func(r *http.Request) bool

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

		h := r.Header.Get("Authorization")
		if h == "" {
			return false
		}
		const prefix = "Bearer "
		if !strings.HasPrefix(h, prefix) {
			return false
		}
		got := strings.TrimSpace(strings.TrimPrefix(h, prefix))
		if got == "" {
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
