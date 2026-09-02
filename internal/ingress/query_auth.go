package ingress

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"time"
)

// QueryAuth verifies a shared token carried in a query parameter.
//
// It exists for event sources that can be given nothing but a URL: no custom
// header, no signing secret, no basic-auth credentials. Telephony platforms,
// appliance webhooks and older ERP systems are the usual examples -- the
// configuration UI has one "URL" field and that is the entire contract. None of
// basic, hmac or forward auth can be satisfied by such a source, so before this
// the only way through was to abuse `match query` as a credential check.
//
// Three properties are inherited from that workaround deliberately, because they
// are what made it the least-bad option:
//
//   - A failure answers with the status an unmatched request would get (404),
//     not 401. There is no realistic client that benefits from a distinguishable
//     auth error here, and 404 does not confirm that the path exists.
//   - Verification happens before the rate limiter and before the body is read,
//     so a wrong token costs no queue work -- the same as a matcher miss today.
//   - The token never reaches the access log, the envelope, the queue, metrics
//     labels or the delivery target. That property is what makes a query token
//     preferable to one in the path, which is simultaneously queue key,
//     access-log field, envelope trace and Prometheus label.
//
// What it cannot do is make the token replay-safe: there is no nonce and no
// timestamp, so anyone who learns the URL can inject events. That is a property
// of the source, not of Hookaido. A URL token is a gate against opportunistic
// traffic, not a signature.
type QueryAuth struct {
	// Param is the query parameter carrying the token.
	Param string

	// SelectSecrets, when set, supplies the accepted tokens at verification
	// time. It is how a `secret_ref` pool contributes several live versions at
	// once, which is what gives a rotation its overlap window.
	SelectSecrets func(at time.Time) [][]byte

	Now func() time.Time

	// secrets are the statically configured tokens, kept as digests so
	// verification does the same work regardless of their length.
	secrets [][sha256.Size]byte
	decoy   [sha256.Size]byte
}

func NewQueryAuth(param string, secrets [][]byte) *QueryAuth {
	a := &QueryAuth{
		Param: param,
		Now:   time.Now,
	}
	for _, s := range secrets {
		a.secrets = append(a.secrets, sha256.Sum256(s))
	}
	// Compared against when there is nothing else to compare against, so a
	// request to a route with no live token costs what a wrong token costs.
	a.decoy = sha256.Sum256([]byte("hookaido/query-auth/decoy"))
	return a
}

// Verify reports whether the request carries an accepted token.
//
// Every candidate is compared, without an early return, so neither the number of
// configured tokens nor the position of the matching one is observable. Tokens
// are compared as SHA-256 digests, which keeps the configured token's length out
// of the timing as well.
func (a *QueryAuth) Verify(r *http.Request) bool {
	ok, _ := a.VerifyCause(r)
	return ok
}

// VerifyCause is Verify plus the reason it refused, as one of the AuthReject*
// causes (empty when it accepted).
//
// The cause matters here for the same reason it does for HMAC: a route whose
// `secret_ref` pool holds no valid version refuses every request, and that is a
// Hookaido-side outage rather than a caller presenting a wrong token. Query auth
// answers 404 for both, so without the classification the two are
// indistinguishable from the outside *and* from the metrics.
func (a *QueryAuth) VerifyCause(r *http.Request) (bool, string) {
	if a == nil {
		return true, ""
	}
	if r == nil || r.URL == nil {
		return false, AuthRejectMalformed
	}

	values, ok := r.URL.Query()[a.Param]
	if !ok || len(values) == 0 {
		return false, AuthRejectCredentials
	}

	candidates := a.candidates()
	failClosed := len(candidates) == 0
	if failClosed {
		// Reached when every secret_ref pool the route names is empty of
		// versions valid now -- the #295 failure. (Compilation rejects an
		// `auth query` with no secret at all, so a static-only route cannot get
		// here.) Compare against the decoy anyway, so the refusal costs what a
		// wrong token costs, and fail closed rather than trusting that.
		candidates = [][sha256.Size]byte{a.decoy}
	}

	matched := false
	for _, v := range values {
		sum := sha256.Sum256([]byte(v))
		for _, want := range candidates {
			if subtle.ConstantTimeCompare(sum[:], want[:]) == 1 {
				matched = true
			}
		}
	}
	switch {
	case failClosed:
		return false, AuthRejectNoValidSecret
	case !matched:
		return false, AuthRejectCredentials
	default:
		return true, ""
	}
}

func (a *QueryAuth) candidates() [][sha256.Size]byte {
	if a.SelectSecrets == nil {
		return a.secrets
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	selected := a.SelectSecrets(now())
	out := make([][sha256.Size]byte, 0, len(selected)+len(a.secrets))
	for _, s := range selected {
		out = append(out, sha256.Sum256(s))
	}
	out = append(out, a.secrets...)
	return out
}
