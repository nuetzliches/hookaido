package ingress

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrUnauthorized = errors.New("unauthorized")

type HMACAuth struct {
	Secrets       [][]byte
	SelectSecrets func(at time.Time) [][]byte

	SignatureHeader string
	TimestampHeader string
	NonceHeader     string
	Tolerance       time.Duration
	Provider        string // "github", "gitea", or "" (canonical)

	Now func() time.Time

	nonce *nonceCache
}

func NewHMACAuth(secrets [][]byte) *HMACAuth {
	a := &HMACAuth{
		Secrets:         cloneByteSlices(secrets),
		SignatureHeader: "X-Signature",
		TimestampHeader: "X-Timestamp",
		NonceHeader:     "X-Nonce",
		Tolerance:       5 * time.Minute,
		Now:             time.Now,
	}
	a.nonce = newNonceCache(a.Now)
	return a
}

// Verify checks:
// - timestamp header is present and within tolerance
// - nonce header is present and not reused within tolerance window
// - signature matches any configured secret
//
// String-to-sign:
//
//	ts + "\n" + method + "\n" + path + "\n" + hex(sha256(body))
func (a *HMACAuth) Verify(r *http.Request, requestPath string, body []byte) error {
	if a == nil {
		return nil
	}
	if a.Provider != "" {
		return a.verifyProvider(r, body)
	}
	if len(a.Secrets) == 0 && a.SelectSecrets == nil {
		return nil
	}

	now := time.Now
	if a.Now != nil {
		now = a.Now
	}

	sigHex := strings.TrimSpace(r.Header.Get(a.SignatureHeader))
	tsStr := strings.TrimSpace(r.Header.Get(a.TimestampHeader))
	nonce := strings.TrimSpace(r.Header.Get(a.NonceHeader))
	if sigHex == "" || tsStr == "" || nonce == "" {
		return ErrUnauthorized
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return ErrUnauthorized
	}
	t := time.Unix(ts, 0).UTC()
	if a.Tolerance > 0 {
		d := now().UTC().Sub(t)
		if d < -a.Tolerance || d > a.Tolerance {
			return ErrUnauthorized
		}
	}

	if a.nonce == nil {
		a.nonce = newNonceCache(now)
	} else {
		a.nonce.setNow(now)
	}
	if !a.nonce.seenOnce(nonce, t.Add(a.Tolerance)) {
		return ErrUnauthorized
	}

	gotSig, err := hex.DecodeString(sigHex)
	if err != nil || len(gotSig) == 0 {
		return ErrUnauthorized
	}

	bodyHash := sha256.Sum256(body)
	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s", tsStr, r.Method, requestPath, hex.EncodeToString(bodyHash[:]))
	msg := []byte(stringToSign)

	secrets := a.Secrets
	if a.SelectSecrets != nil {
		secrets = a.SelectSecrets(t)
	}
	if len(secrets) == 0 {
		return ErrUnauthorized
	}

	for _, secret := range secrets {
		if len(secret) == 0 {
			continue
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(msg)
		want := mac.Sum(nil)
		if subtle.ConstantTimeCompare(gotSig, want) == 1 {
			return nil
		}
	}

	return ErrUnauthorized
}

func cloneByteSlices(in [][]byte) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, b := range in {
		if len(b) == 0 {
			continue
		}
		cp := make([]byte, len(b))
		copy(cp, b)
		out = append(out, cp)
	}
	return out
}

// allSecrets returns static secrets combined with SelectSecrets at the given time.
func (a *HMACAuth) allSecrets(at time.Time) [][]byte {
	var out [][]byte
	if a.SelectSecrets != nil {
		out = append(out, a.SelectSecrets(at)...)
	}
	out = append(out, a.Secrets...)
	return out
}

func (a *HMACAuth) verifyProvider(r *http.Request, body []byte) error {
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	secrets := a.allSecrets(now())
	if len(secrets) == 0 {
		return ErrUnauthorized
	}

	switch a.Provider {
	case "github":
		return a.verifyGitHub(r, body, secrets)
	case "gitea":
		return a.verifyGitea(r, body, secrets)
	case "stripe":
		return a.verifyStripe(r, body, secrets, "Stripe-Signature", "v1", time.Second)
	case "cituro":
		// Cituro uses the same "t=<ts>,<tag>=<hex>" header format and
		// "<ts>.<body>" signed-payload scheme as Stripe, but with a
		// different header name, signature tag, and timestamp unit:
		// Cituro emits t= as Unix milliseconds (13-digit), Stripe as
		// Unix seconds (10-digit). The PDF's truncated "t=1592..." example
		// hid the unit; confirmed from live X-CITURO-SIGNATURE headers.
		return a.verifyStripe(r, body, secrets, "X-CITURO-SIGNATURE", "s", time.Millisecond)
	default:
		return ErrUnauthorized
	}
}

func (a *HMACAuth) verifyGitHub(r *http.Request, body []byte, secrets [][]byte) error {
	sigHeader := strings.TrimSpace(r.Header.Get("X-Hub-Signature-256"))
	if sigHeader == "" {
		return ErrUnauthorized
	}
	if !strings.HasPrefix(sigHeader, "sha256=") {
		return ErrUnauthorized
	}
	gotSig, err := hex.DecodeString(sigHeader[len("sha256="):])
	if err != nil || len(gotSig) == 0 {
		return ErrUnauthorized
	}
	for _, secret := range secrets {
		if len(secret) == 0 {
			continue
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(body)
		want := mac.Sum(nil)
		if hmac.Equal(gotSig, want) {
			return nil
		}
	}
	return ErrUnauthorized
}

func (a *HMACAuth) verifyGitea(r *http.Request, body []byte, secrets [][]byte) error {
	sigHeader := strings.TrimSpace(r.Header.Get("X-Gitea-Signature"))
	if sigHeader == "" {
		return ErrUnauthorized
	}
	gotSig, err := hex.DecodeString(sigHeader)
	if err != nil || len(gotSig) == 0 {
		return ErrUnauthorized
	}
	for _, secret := range secrets {
		if len(secret) == 0 {
			continue
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(body)
		want := mac.Sum(nil)
		if hmac.Equal(gotSig, want) {
			return nil
		}
	}
	return ErrUnauthorized
}

// verifyStripe verifies a Stripe-style HMAC signature.
//
// Header format: "t=<unix-ts>,<sigTag>=<hex>[,<sigTag>=<hex>...]"
// (Stripe: `Stripe-Signature` with sigTag "v1". Other providers reuse the
// same scheme with different header + tag names — e.g. Cituro uses
// `X-CITURO-SIGNATURE` with sigTag "s".)
//
// String-to-sign: "<ts>.<body>" (HMAC-SHA256, lowercase hex). The ts used
// in the signed payload is the *raw string* from the header, so senders
// that emit ms-precision timestamps and senders emitting second-precision
// both work as long as their Hookaido-side tsUnit matches.
//
// tsUnit selects how the numeric t= value is interpreted for the
// tolerance check: time.Second for Stripe (10-digit unix seconds),
// time.Millisecond for Cituro (13-digit unix milliseconds).
//
// Replay protection: the timestamp must be within a.Tolerance of the current
// time (default 5m). No nonce — retries within the tolerance window are
// accepted, which matches Stripe/Cituro semantics.
func (a *HMACAuth) verifyStripe(r *http.Request, body []byte, secrets [][]byte, headerName, sigTag string, tsUnit time.Duration) error {
	raw := strings.TrimSpace(r.Header.Get(headerName))
	if raw == "" {
		slog.Warn("hmac_stripe_failed", "reason", "header_missing", "header", headerName)
		return ErrUnauthorized
	}

	var tsStr, sigHex string
	for _, part := range strings.Split(raw, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		switch key {
		case "t":
			if tsStr == "" {
				tsStr = val
			}
		case sigTag:
			if sigHex == "" {
				sigHex = val
			}
		}
	}
	if tsStr == "" || sigHex == "" {
		slog.Warn("hmac_stripe_failed", "reason", "parse_incomplete",
			"header", headerName, "sig_tag", sigTag,
			"ts_present", tsStr != "", "sig_present", sigHex != "",
			"header_value", raw)
		return ErrUnauthorized
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		slog.Warn("hmac_stripe_failed", "reason", "ts_parse_error",
			"header", headerName, "ts_str", tsStr, "err", err.Error())
		return ErrUnauthorized
	}

	tolerance := a.Tolerance
	if tolerance <= 0 {
		tolerance = 5 * time.Minute
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	// Convert the raw numeric ts into absolute time via the provider-specific
	// unit (seconds for stripe, milliseconds for cituro). time.Unix(0, ns)
	// takes nanoseconds so we multiply by tsUnit (a time.Duration = int64 ns).
	t := time.Unix(0, ts*int64(tsUnit)).UTC()
	if d := now().UTC().Sub(t); d < -tolerance || d > tolerance {
		slog.Warn("hmac_stripe_failed", "reason", "ts_out_of_tolerance",
			"header", headerName, "ts_unix", ts, "ts_unit", tsUnit.String(),
			"delta_seconds", d.Seconds(), "tolerance_seconds", tolerance.Seconds())
		return ErrUnauthorized
	}

	gotSig, err := hex.DecodeString(sigHex)
	if err != nil || len(gotSig) == 0 {
		slog.Warn("hmac_stripe_failed", "reason", "sig_hex_decode_error",
			"header", headerName, "sig_hex_len", len(sigHex), "sig_hex_prefix", safePrefix(sigHex, 6))
		return ErrUnauthorized
	}

	msg := make([]byte, 0, len(tsStr)+1+len(body))
	msg = append(msg, tsStr...)
	msg = append(msg, '.')
	msg = append(msg, body...)

	for _, secret := range secrets {
		if len(secret) == 0 {
			continue
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(msg)
		want := mac.Sum(nil)
		if hmac.Equal(gotSig, want) {
			return nil
		}
	}

	// No secret matched: emit diagnostic fingerprint (never raw secrets or full signatures).
	// "got_prefix" and "want_prefix" are the first 8 hex chars of the received vs.
	// the computed signature for the *first* secret in the pool -- enough to spot
	// a wholesale mismatch (wrong key material / wrong body framing) without
	// leaking the full MAC.
	var wantPrefix string
	if len(secrets) > 0 && len(secrets[0]) > 0 {
		mac := hmac.New(sha256.New, secrets[0])
		_, _ = mac.Write(msg)
		wantPrefix = hex.EncodeToString(mac.Sum(nil))[:8]
	}
	slog.Warn("hmac_stripe_failed", "reason", "no_secret_matched",
		"header", headerName, "sig_tag", sigTag,
		"secrets_tried", len(secrets),
		"ts_str", tsStr, "body_len", len(body),
		"got_prefix", hex.EncodeToString(gotSig)[:min(8, 2*len(gotSig))],
		"want_prefix_first_secret", wantPrefix)
	return ErrUnauthorized
}

// safePrefix returns the first n characters of s, or all of s if shorter.
// Used for diagnostic logging where we want a fingerprint without leaking
// the full value (e.g. signature prefix, not full signature).
func safePrefix(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

type nonceCache struct {
	mu  sync.Mutex
	now func() time.Time
	m   map[string]time.Time
}

func newNonceCache(now func() time.Time) *nonceCache {
	if now == nil {
		now = time.Now
	}
	return &nonceCache{
		now: now,
		m:   make(map[string]time.Time),
	}
}

func (c *nonceCache) setNow(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

func (c *nonceCache) seenOnce(nonce string, expiresAt time.Time) bool {
	if nonce == "" {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Opportunistic cleanup.
	now := c.now().UTC()
	for k, exp := range c.m {
		if !now.Before(exp) {
			delete(c.m, k)
		}
	}

	if exp, ok := c.m[nonce]; ok && now.Before(exp) {
		return false
	}
	c.m[nonce] = expiresAt.UTC()
	return true
}
