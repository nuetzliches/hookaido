package ingress

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestHMACAuth_SelectSecrets(t *testing.T) {
	secret := []byte("s1")
	ts := time.Unix(1735689600, 0).UTC() // 2025-01-01T00:00:00Z
	body := []byte("payload")
	path := "/hooks"

	auth := NewHMACAuth(nil)
	auth.SelectSecrets = func(at time.Time) [][]byte {
		if at.Equal(ts) {
			return [][]byte{secret}
		}
		return nil
	}
	auth.Now = func() time.Time { return ts }

	req := httptest.NewRequest(http.MethodPost, "http://example.com"+path, bytes.NewReader(body))
	req.Header.Set(auth.TimestampHeader, strconv.FormatInt(ts.Unix(), 10))
	req.Header.Set(auth.NonceHeader, "n1")
	req.Header.Set(auth.SignatureHeader, signHMAC(ts.Unix(), http.MethodPost, path, body, secret))

	if _, err := auth.Verify(req, path, body); err != nil {
		t.Fatalf("expected verify ok, got %v", err)
	}
}

func TestHMACAuth_SelectSecretsEmptyDenied(t *testing.T) {
	ts := time.Unix(1735689600, 0).UTC()
	body := []byte("payload")
	path := "/hooks"

	auth := NewHMACAuth(nil)
	auth.SelectSecrets = func(_ time.Time) [][]byte { return nil }
	auth.Now = func() time.Time { return ts }

	req := httptest.NewRequest(http.MethodPost, "http://example.com"+path, bytes.NewReader(body))
	req.Header.Set(auth.TimestampHeader, strconv.FormatInt(ts.Unix(), 10))
	req.Header.Set(auth.NonceHeader, "n1")
	req.Header.Set(auth.SignatureHeader, "deadbeef")

	if _, err := auth.Verify(req, path, body); err == nil {
		t.Fatalf("expected unauthorized when no secrets are available")
	}
}

// newCanonicalAuth builds an HMACAuth with a fixed clock and one secret, plus a
// helper that produces a request for a given nonce and signature.
func newCanonicalAuth(t *testing.T, secret []byte, ts time.Time) (*HMACAuth, func(nonce, sig string) *http.Request, []byte, string) {
	t.Helper()
	body := []byte("payload")
	path := "/hooks"

	auth := NewHMACAuth([][]byte{secret})
	auth.Now = func() time.Time { return ts }

	mk := func(nonce, sig string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "http://example.com"+path, bytes.NewReader(body))
		req.Header.Set(auth.TimestampHeader, strconv.FormatInt(ts.Unix(), 10))
		req.Header.Set(auth.NonceHeader, nonce)
		req.Header.Set(auth.SignatureHeader, sig)
		return req
	}
	return auth, mk, body, path
}

// A request that fails signature verification must not claim its nonce.
// Claiming it before verification let any unauthenticated caller burn a nonce,
// so the genuine signed webhook carrying it was then rejected as a replay --
// and let that same caller grow the cache without ever authenticating.
func TestHMACAuth_RejectedRequestDoesNotClaimNonce(t *testing.T) {
	secret := []byte("s1")
	ts := time.Unix(1735689600, 0).UTC()
	auth, mk, body, path := newCanonicalAuth(t, secret, ts)

	valid := signHMAC(ts.Unix(), http.MethodPost, path, body, secret)

	// An attacker who knows the nonce sends it with a bogus signature first.
	if _, err := auth.Verify(mk("n-shared", "00"), path, body); err == nil {
		t.Fatal("expected unauthorized for a bad signature")
	}

	// The genuine, correctly signed delivery carrying the same nonce must
	// still be accepted.
	if _, err := auth.Verify(mk("n-shared", valid), path, body); err != nil {
		t.Fatalf("genuine request rejected after an unauthenticated caller used the same nonce: %v", err)
	}
}

// Replay protection itself must keep working: a second delivery of the same
// verified request is rejected.
func TestHMACAuth_VerifiedNonceIsClaimedOnce(t *testing.T) {
	secret := []byte("s1")
	ts := time.Unix(1735689600, 0).UTC()
	auth, mk, body, path := newCanonicalAuth(t, secret, ts)

	sig := signHMAC(ts.Unix(), http.MethodPost, path, body, secret)

	if _, err := auth.Verify(mk("n1", sig), path, body); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if _, err := auth.Verify(mk("n1", sig), path, body); err == nil {
		t.Fatal("expected the replayed delivery to be rejected")
	}
}

// A nonce we cannot track is not waved through: the route opted into replay
// protection, and an unbounded nonce is also what let a single request occupy
// megabytes of cache.
func TestHMACAuth_OverlongNonceRejected(t *testing.T) {
	secret := []byte("s1")
	ts := time.Unix(1735689600, 0).UTC()
	auth, mk, body, path := newCanonicalAuth(t, secret, ts)

	sig := signHMAC(ts.Unix(), http.MethodPost, path, body, secret)

	if _, err := auth.Verify(mk(strings.Repeat("a", nonceMaxLen), sig), path, body); err != nil {
		t.Fatalf("nonce at the length limit should be accepted, got %v", err)
	}
	if _, err := auth.Verify(mk(strings.Repeat("b", nonceMaxLen+1), sig), path, body); err == nil {
		t.Fatal("expected a nonce over the length limit to be rejected")
	}
}

// The cache must not grow without bound, and must evict the entry closest to
// expiring rather than refuse a validly signed request.
func TestNonceCache_EvictsEarliestWhenFull(t *testing.T) {
	now := time.Unix(1735689600, 0).UTC()
	c := newNonceCache(func() time.Time { return now })

	// Fill to capacity with entries expiring far out, plus one expiring soon.
	c.m["soonest"] = nonceEntry{expiresAt: now.Add(time.Second)}
	for i := 0; i < nonceMaxEntries-1; i++ {
		c.m[strconv.Itoa(i)] = nonceEntry{expiresAt: now.Add(time.Hour)}
	}
	c.lastSweep = now // suppress the sweep so the cap path is exercised

	if _, ok := c.claim("fresh", now.Add(time.Hour)); !ok {
		t.Fatal("a validly signed request must be recorded even when the cache is full")
	}
	if len(c.m) > nonceMaxEntries {
		t.Fatalf("cache grew to %d, want <= %d", len(c.m), nonceMaxEntries)
	}
	if _, ok := c.m["soonest"]; ok {
		t.Fatal("expected the entry closest to expiring to be evicted")
	}
}

func signHMAC(ts int64, method, path string, body []byte, secret []byte) string {
	bodyHash := sha256.Sum256(body)
	msg := []byte(strconv.FormatInt(ts, 10) + "\n" + method + "\n" + path + "\n" + hex.EncodeToString(bodyHash[:]))
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(msg)
	return hex.EncodeToString(mac.Sum(nil))
}

// signGitHub produces a GitHub-style signature: sha256=hex(HMAC-SHA256(secret, body))
func signGitHub(body, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// signGitea produces a Gitea-style signature: hex(HMAC-SHA256(secret, body))
func signGitea(body, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func TestHMACAuth_VerifyGitHub(t *testing.T) {
	secret := []byte("gh-webhook-secret")
	body := []byte(`{"action":"push","ref":"refs/heads/main"}`)

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "github"

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signGitHub(body, secret))

	if _, err := auth.Verify(req, "/webhooks/github", body); err != nil {
		t.Fatalf("expected verify ok, got %v", err)
	}
}

func TestHMACAuth_VerifyGitHub_InvalidSignature(t *testing.T) {
	secret := []byte("gh-webhook-secret")
	body := []byte(`{"action":"push"}`)

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "github"

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signGitHub(body, []byte("wrong-secret")))

	if _, err := auth.Verify(req, "/webhooks/github", body); err == nil {
		t.Fatalf("expected unauthorized for invalid signature")
	}
}

func TestHMACAuth_VerifyGitHub_MissingHeader(t *testing.T) {
	secret := []byte("gh-webhook-secret")
	body := []byte(`{"action":"push"}`)

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "github"

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/github", bytes.NewReader(body))
	// No X-Hub-Signature-256 header

	if _, err := auth.Verify(req, "/webhooks/github", body); err == nil {
		t.Fatalf("expected unauthorized for missing header")
	}
}

func TestHMACAuth_VerifyGitHub_WrongPrefix(t *testing.T) {
	secret := []byte("gh-webhook-secret")
	body := []byte(`{"action":"push"}`)

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "github"

	// Send signature without sha256= prefix
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	rawSig := hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", rawSig)

	if _, err := auth.Verify(req, "/webhooks/github", body); err == nil {
		t.Fatalf("expected unauthorized for missing sha256= prefix")
	}
}

func TestHMACAuth_VerifyGitea(t *testing.T) {
	secret := []byte("gitea-webhook-secret")
	body := []byte(`{"action":"push","ref":"refs/heads/main"}`)

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "gitea"

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/gitea", bytes.NewReader(body))
	req.Header.Set("X-Gitea-Signature", signGitea(body, secret))

	if _, err := auth.Verify(req, "/webhooks/gitea", body); err != nil {
		t.Fatalf("expected verify ok, got %v", err)
	}
}

func TestHMACAuth_VerifyGitea_InvalidSignature(t *testing.T) {
	secret := []byte("gitea-webhook-secret")
	body := []byte(`{"action":"push"}`)

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "gitea"

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/gitea", bytes.NewReader(body))
	req.Header.Set("X-Gitea-Signature", signGitea(body, []byte("wrong-secret")))

	if _, err := auth.Verify(req, "/webhooks/gitea", body); err == nil {
		t.Fatalf("expected unauthorized for invalid signature")
	}
}

func TestHMACAuth_VerifyGitea_MissingHeader(t *testing.T) {
	secret := []byte("gitea-webhook-secret")
	body := []byte(`{"action":"push"}`)

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "gitea"

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/gitea", bytes.NewReader(body))
	// No X-Gitea-Signature header

	if _, err := auth.Verify(req, "/webhooks/gitea", body); err == nil {
		t.Fatalf("expected unauthorized for missing header")
	}
}

func TestHMACAuth_VerifyProvider_SelectSecrets(t *testing.T) {
	secret := []byte("dynamic-secret")
	body := []byte(`{"event":"test"}`)
	now := time.Now()

	auth := NewHMACAuth(nil)
	auth.Provider = "github"
	auth.SelectSecrets = func(at time.Time) [][]byte {
		return [][]byte{secret}
	}
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signGitHub(body, secret))

	if _, err := auth.Verify(req, "/webhooks/github", body); err != nil {
		t.Fatalf("expected verify ok with SelectSecrets, got %v", err)
	}
}

func TestHMACAuth_VerifyProvider_NoSecrets(t *testing.T) {
	body := []byte(`{"event":"test"}`)

	auth := NewHMACAuth(nil)
	auth.Provider = "github"
	// No secrets and no SelectSecrets

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", "sha256=deadbeef")

	if _, err := auth.Verify(req, "/webhooks/github", body); err == nil {
		t.Fatalf("expected unauthorized when no secrets configured")
	}
}

// The rejection log must never carry a value derived from the secret. Both
// halves of the signed message are attacker-controlled, so emitting
// HMAC(secret, msg) for a caller-chosen msg -- even truncated to a few hex
// characters -- is a chosen-message oracle: every rejected request hands out
// verified bits of the correct MAC for a message the attacker picked.
func TestHMACAuth_StripeRejectionLogCarriesNoSecretDerivedValue(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)
	now := time.Unix(1735689600, 0).UTC()

	// What an oracle would have leaked: the expected MAC over the attacker's
	// chosen message.
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(strconv.FormatInt(now.Unix(), 10) + "." + string(body)))
	expectedMAC := hex.EncodeToString(mac.Sum(nil))

	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", "t="+strconv.FormatInt(now.Unix(), 10)+",v1=deadbeef")

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
		t.Fatal("expected unauthorized for a bogus signature")
	}

	out := logs.String()
	if !strings.Contains(out, "no_secret_matched") {
		t.Fatalf("expected the no_secret_matched warning, got: %s", out)
	}
	// Even the first 8 hex characters must not appear.
	if strings.Contains(out, expectedMAC[:8]) {
		t.Fatalf("rejection log leaked a prefix of the expected MAC (%s): %s", expectedMAC[:8], out)
	}
	if strings.Contains(out, "want_prefix") {
		t.Fatalf("rejection log still carries a want_prefix field: %s", out)
	}
}

// Attacker-controlled strings must not be echoed into the log sink whole.
func TestHMACAuth_StripeTimestampIsBoundedInLog(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)
	huge := strings.Repeat("9", 4096)

	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return time.Unix(1735689600, 0).UTC() }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", "t="+huge+",v1=abcdef")

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
		t.Fatal("expected unauthorized for an unparseable timestamp")
	}

	out := logs.String()
	if strings.Contains(out, huge) {
		t.Fatalf("rejection log echoed the full %d-byte timestamp", len(huge))
	}
	// Not even a bounded prefix: the timestamp is attacker-controlled header
	// material, and CodeQL's go/clear-text-logging flags any of it reaching a
	// log call. A run of the padding character is enough to catch a prefix.
	if strings.Contains(out, strings.Repeat("9", 8)) {
		t.Fatalf("rejection log echoed a prefix of the timestamp, got: %s", out)
	}
	if !strings.Contains(out, "ts_len=4096") {
		t.Fatalf("expected the length to be reported instead, got: %s", out)
	}
	if !strings.Contains(out, "cause=out_of_range") {
		t.Fatalf("expected a parse-failure classification, got: %s", out)
	}
}

// signStripe produces a Stripe-style signature header value:
// "t=<unix-ts>,<sigTag>=hex(HMAC-SHA256(secret, <ts>.<body>))"
func signStripe(ts int64, body, secret []byte, sigTag string) string {
	tsStr := strconv.FormatInt(ts, 10)
	msg := []byte(tsStr + "." + string(body))
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(msg)
	return "t=" + tsStr + "," + sigTag + "=" + hex.EncodeToString(mac.Sum(nil))
}

func TestHMACAuth_VerifyStripe(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123","type":"invoice.paid"}`)
	now := time.Unix(1735689600, 0).UTC() // 2025-01-01T00:00:00Z

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(now.Unix(), body, secret, "v1"))

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err != nil {
		t.Fatalf("expected verify ok, got %v", err)
	}
}

func TestHMACAuth_VerifyStripe_MultipleSigs(t *testing.T) {
	// Stripe supports multiple signatures in the same header (for key rotation).
	// Verify we accept when any of them matches.
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)
	now := time.Unix(1735689600, 0).UTC()

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return now }

	tsStr := strconv.FormatInt(now.Unix(), 10)
	validSig := signStripe(now.Unix(), body, secret, "v1")
	// Prepend a bogus v0 signature
	header := "t=" + tsStr + ",v0=deadbeef," + strings.TrimPrefix(validSig, "t="+tsStr+",")

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", header)

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err != nil {
		t.Fatalf("expected verify ok with mixed v0/v1 sigs, got %v", err)
	}
}

// During a secret roll Stripe signs with every active secret and emits one
// `v1=` entry per secret. Keeping only the first rejected validly signed
// webhooks with 401 for the whole roll window whenever the entry matching the
// configured secret was not listed first.
func TestHMACAuth_VerifyStripe_SecretRollSecondV1Matches(t *testing.T) {
	oldSecret := []byte("stripe-secret-old")
	newSecret := []byte("stripe-secret-new")
	body := []byte(`{"id":"evt_123"}`)
	now := time.Unix(1735689600, 0).UTC()

	tsStr := strconv.FormatInt(now.Unix(), 10)
	sigFor := func(secret []byte) string {
		return strings.TrimPrefix(signStripe(now.Unix(), body, secret, "v1"), "t="+tsStr+",")
	}
	// The endpoint is still configured with the old secret; the header lists
	// the new secret's signature first, as Stripe emits them.
	header := "t=" + tsStr + "," + sigFor(newSecret) + "," + sigFor(oldSecret)

	auth := NewHMACAuth([][]byte{oldSecret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", header)

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err != nil {
		t.Fatalf("expected verify ok when a later v1 matches, got %v", err)
	}
}

// A malformed entry must not reject a request another entry signs correctly.
func TestHMACAuth_VerifyStripe_UndecodableSignatureIsSkipped(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)
	now := time.Unix(1735689600, 0).UTC()

	tsStr := strconv.FormatInt(now.Unix(), 10)
	valid := strings.TrimPrefix(signStripe(now.Unix(), body, secret, "v1"), "t="+tsStr+",")
	header := "t=" + tsStr + ",v1=nothex," + valid

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", header)

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err != nil {
		t.Fatalf("expected verify ok when one of the entries is not hex, got %v", err)
	}
}

// The candidate list is bounded, so a caller cannot push an unbounded number of
// values into the compare loop.
func TestHMACAuth_VerifyStripe_SignatureCandidatesAreBounded(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)
	now := time.Unix(1735689600, 0).UTC()

	tsStr := strconv.FormatInt(now.Unix(), 10)
	valid := strings.TrimPrefix(signStripe(now.Unix(), body, secret, "v1"), "t="+tsStr+",")

	header := "t=" + tsStr
	for i := 0; i < stripeMaxSignatures; i++ {
		header += ",v1=deadbeef"
	}
	// One past the cap: the valid signature is never reached.
	header += "," + valid

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", header)

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
		t.Fatal("expected unauthorized once the candidate cap is exceeded")
	}
}

func TestHMACAuth_VerifyStripe_InvalidSignature(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)
	now := time.Unix(1735689600, 0).UTC()

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(now.Unix(), body, []byte("wrong-secret"), "v1"))

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
		t.Fatalf("expected unauthorized for invalid signature")
	}
}

func TestHMACAuth_VerifyStripe_MissingHeader(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	// No Stripe-Signature header

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
		t.Fatalf("expected unauthorized for missing header")
	}
}

func TestHMACAuth_VerifyStripe_ExpiredTimestamp(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)
	sigTS := time.Unix(1735689600, 0).UTC()
	now := sigTS.Add(10 * time.Minute) // 10min after signature — outside default 5min tolerance

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(sigTS.Unix(), body, secret, "v1"))

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
		t.Fatalf("expected unauthorized for expired timestamp")
	}
}

func TestHMACAuth_VerifyStripe_FutureTimestamp(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)
	sigTS := time.Unix(1735689600, 0).UTC()
	now := sigTS.Add(-10 * time.Minute) // now is 10min before ts — also outside tolerance

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(sigTS.Unix(), body, secret, "v1"))

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
		t.Fatalf("expected unauthorized for future timestamp")
	}
}

func TestHMACAuth_VerifyStripe_CustomTolerance(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)
	sigTS := time.Unix(1735689600, 0).UTC()
	now := sigTS.Add(8 * time.Minute)

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Tolerance = 10 * time.Minute // widen tolerance so 8min ago still counts
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signStripe(sigTS.Unix(), body, secret, "v1"))

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err != nil {
		t.Fatalf("expected verify ok with 10m tolerance, got %v", err)
	}
}

func TestHMACAuth_VerifyStripe_MalformedHeader(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)
	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return time.Unix(1735689600, 0).UTC() }

	cases := []string{
		"",        // empty
		"garbage", // no comma-separated kv
		"t=notanumber,v1=abcdef",
		"v1=abcdef",         // timestamp missing
		"t=1735689600",      // signature missing
		"t=1735689600,v1=Z", // sig not hex
	}
	for _, h := range cases {
		req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
		if h != "" {
			req.Header.Set("Stripe-Signature", h)
		}
		if _, err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
			t.Fatalf("expected unauthorized for malformed header %q", h)
		}
	}
}

// The parse_incomplete rejection path must not move header material into the
// log. A header that fails to parse can still carry a complete signature --
// here the tag name simply does not match what the route expects. Guards the
// diagnostic fingerprint contract documented on verifyStripeLike.
func TestHMACAuth_VerifyStripe_ParseIncompleteDoesNotLogHeaderValue(t *testing.T) {
	secret := []byte("stripe-webhook-secret")
	body := []byte(`{"id":"evt_123"}`)
	now := time.Unix(1735689600, 0).UTC()

	// Valid signature under an unexpected tag and without "t=", so parsing
	// fails while the signature itself is fully present in the header.
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte("1735689600." + string(body)))
	sigHex := hex.EncodeToString(mac.Sum(nil))

	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "stripe"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", "v0="+sigHex)

	if _, err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
		t.Fatalf("expected unauthorized for header without timestamp")
	}

	out := logs.String()
	if !strings.Contains(out, "parse_incomplete") {
		t.Fatalf("expected parse_incomplete warning, got: %s", out)
	}
	if strings.Contains(out, sigHex) {
		t.Fatalf("signature leaked into log output: %s", out)
	}
	for _, want := range []string{"pairs_parsed=1", "header_value_len=", "sig_present=false", "ts_present=false"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in log output, got: %s", want, out)
		}
	}
}

// Cituro reuses the Stripe scheme with a different header and sig tag,
// plus millisecond-precision timestamps instead of seconds.
func TestHMACAuth_VerifyCituro(t *testing.T) {
	secret := []byte("whs_cituro_abc123")
	body := []byte(`{"eventId":"11f0","type":"booking.created"}`)
	now := time.Unix(1735689600, 0).UTC()

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "cituro"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/cituro", bytes.NewReader(body))
	req.Header.Set("X-CITURO-SIGNATURE", signStripe(now.UnixMilli(), body, secret, "s"))

	if _, err := auth.Verify(req, "/webhooks/cituro", body); err != nil {
		t.Fatalf("expected verify ok for cituro alias, got %v", err)
	}
}

func TestHMACAuth_VerifyCituro_RejectsSecondsTimestamp(t *testing.T) {
	// Regression guard: a second-precision timestamp (Stripe-style) against
	// the cituro provider must be rejected as out-of-tolerance (interpreted
	// as 1970-era after the ms->ns conversion).
	secret := []byte("whs_cituro_abc123")
	body := []byte(`{"type":"booking.created"}`)
	now := time.Unix(1735689600, 0).UTC()

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "cituro"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/cituro", bytes.NewReader(body))
	req.Header.Set("X-CITURO-SIGNATURE", signStripe(now.Unix(), body, secret, "s"))

	if _, err := auth.Verify(req, "/webhooks/cituro", body); err == nil {
		t.Fatalf("expected unauthorized when seconds-precision ts is used with cituro provider")
	}
}

func TestHMACAuth_VerifyCituro_InvalidSignature(t *testing.T) {
	secret := []byte("whs_cituro_abc123")
	body := []byte(`{"type":"booking.canceled"}`)
	now := time.Unix(1735689600, 0).UTC()

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "cituro"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/cituro", bytes.NewReader(body))
	req.Header.Set("X-CITURO-SIGNATURE", signStripe(now.UnixMilli(), body, []byte("wrong"), "s"))

	if _, err := auth.Verify(req, "/webhooks/cituro", body); err == nil {
		t.Fatalf("expected unauthorized for invalid cituro signature")
	}
}

func TestHMACAuth_VerifyCituro_WrongTag(t *testing.T) {
	// A sig under tag "v1" (Stripe) should NOT verify when the provider is cituro
	// (which expects sig tag "s").
	secret := []byte("whs_cituro_abc123")
	body := []byte(`{"type":"booking.created"}`)
	now := time.Unix(1735689600, 0).UTC()

	auth := NewHMACAuth([][]byte{secret})
	auth.Provider = "cituro"
	auth.Now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/cituro", bytes.NewReader(body))
	req.Header.Set("X-CITURO-SIGNATURE", signStripe(now.UnixMilli(), body, secret, "v1"))

	if _, err := auth.Verify(req, "/webhooks/cituro", body); err == nil {
		t.Fatalf("expected unauthorized when sig tag doesn't match provider")
	}
}
