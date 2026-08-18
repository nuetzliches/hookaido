package ingress

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
)

// BasicAuth verifies HTTP Basic credentials against a fixed user list.
//
// Credentials are kept as SHA-256 digests, not plaintext, so verification does
// the same amount of work for every request: hashing the caller's password and
// one constant-time compare. An unknown user is compared against a decoy digest
// rather than returning early, and comparing digests rather than the passwords
// themselves keeps the configured password's length out of the timing. Without
// both, a network attacker could enumerate valid usernames and learn how long
// the configured password is -- this is the one credential check on the ingress
// path that was not constant time.
type BasicAuth struct {
	users map[string][sha256.Size]byte
	decoy [sha256.Size]byte
}

func NewBasicAuth(users map[string]string) *BasicAuth {
	if len(users) == 0 {
		return nil
	}
	out := make(map[string][sha256.Size]byte, len(users))
	for k, v := range users {
		out[k] = sha256.Sum256([]byte(v))
	}

	// A random decoy, so the comparison an unknown user runs against cannot be
	// distinguished from a real one by its result either.
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		// crypto/rand does not fail on any supported platform, and a decoy that
		// is merely fixed still costs the same to compare.
		seed = [32]byte{}
	}
	return &BasicAuth{users: out, decoy: sha256.Sum256(seed[:])}
}

func (a *BasicAuth) Verify(r *http.Request) bool {
	if a == nil || len(a.users) == 0 {
		return true
	}
	user, pass, ok := r.BasicAuth()
	if !ok {
		return false
	}
	want, known := a.users[user]
	if !known {
		want = a.decoy
	}
	// The compare runs before `known` is consulted, so an unknown user costs
	// exactly what a known one with the wrong password costs.
	return secureEqualDigest(pass, want) && known
}

// secureEqualDigest hashes the supplied password and compares it against a
// stored digest in constant time. Both sides are 32 bytes, so no length
// information about the configured password is observable; the SHA-256 of the
// caller's own input is the only length-dependent work, and that length is
// something the caller already knows.
func secureEqualDigest(got string, want [sha256.Size]byte) bool {
	sum := sha256.Sum256([]byte(got))
	return subtle.ConstantTimeCompare(sum[:], want[:]) == 1
}
