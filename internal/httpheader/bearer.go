package httpheader

import "strings"

const bearerPrefix = "Bearer "

// ParseBearerToken extracts the credentials from an Authorization header value
// carrying the Bearer scheme, reporting false when the header is absent, uses a
// different scheme, or carries an empty token.
//
// The scheme is matched case-insensitively and surrounding whitespace is
// ignored, because RFC 7235 defines the auth-scheme token as case-insensitive
// and permits leading whitespace before it. The gRPC worker API already parsed
// it that way while the Pull and Admin HTTP APIs required exactly "Bearer ",
// so the same client credential was accepted on one transport and rejected on
// the other. This is the single implementation for all three.
//
// The token itself is returned verbatim apart from surrounding whitespace;
// callers compare it in constant time.
func ParseBearerToken(raw string) (string, bool) {
	h := strings.TrimSpace(raw)
	if len(h) < len(bearerPrefix) {
		return "", false
	}
	if !strings.EqualFold(h[:len(bearerPrefix)], bearerPrefix) {
		return "", false
	}
	token := strings.TrimSpace(h[len(bearerPrefix):])
	if token == "" {
		return "", false
	}
	return token, true
}
