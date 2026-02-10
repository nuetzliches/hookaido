package ingress

import (
	"crypto/subtle"
	"net/http"
)

type BasicAuth struct {
	Users map[string]string
}

func NewBasicAuth(users map[string]string) *BasicAuth {
	if len(users) == 0 {
		return nil
	}
	out := make(map[string]string, len(users))
	for k, v := range users {
		out[k] = v
	}
	return &BasicAuth{Users: out}
}

func (a *BasicAuth) Verify(r *http.Request) bool {
	if a == nil || len(a.Users) == 0 {
		return true
	}
	user, pass, ok := r.BasicAuth()
	if !ok {
		return false
	}
	want, ok := a.Users[user]
	if !ok {
		return false
	}
	return secureEqual(pass, want)
}

func secureEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
