package pullapi

import (
	"crypto/subtle"
	"net/http"

	"github.com/nuetzliches/hookaido/v2/internal/httpheader"
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
