package ingress

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

	if err := auth.Verify(req, path, body); err != nil {
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

	if err := auth.Verify(req, path, body); err == nil {
		t.Fatalf("expected unauthorized when no secrets are available")
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

	if err := auth.Verify(req, "/webhooks/github", body); err != nil {
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

	if err := auth.Verify(req, "/webhooks/github", body); err == nil {
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

	if err := auth.Verify(req, "/webhooks/github", body); err == nil {
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

	if err := auth.Verify(req, "/webhooks/github", body); err == nil {
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

	if err := auth.Verify(req, "/webhooks/gitea", body); err != nil {
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

	if err := auth.Verify(req, "/webhooks/gitea", body); err == nil {
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

	if err := auth.Verify(req, "/webhooks/gitea", body); err == nil {
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

	if err := auth.Verify(req, "/webhooks/github", body); err != nil {
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

	if err := auth.Verify(req, "/webhooks/github", body); err == nil {
		t.Fatalf("expected unauthorized when no secrets configured")
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

	if err := auth.Verify(req, "/webhooks/stripe", body); err != nil {
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

	if err := auth.Verify(req, "/webhooks/stripe", body); err != nil {
		t.Fatalf("expected verify ok with mixed v0/v1 sigs, got %v", err)
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

	if err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
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

	if err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
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

	if err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
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

	if err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
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

	if err := auth.Verify(req, "/webhooks/stripe", body); err != nil {
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
		"",                  // empty
		"garbage",           // no comma-separated kv
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
		if err := auth.Verify(req, "/webhooks/stripe", body); err == nil {
			t.Fatalf("expected unauthorized for malformed header %q", h)
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

	if err := auth.Verify(req, "/webhooks/cituro", body); err != nil {
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

	if err := auth.Verify(req, "/webhooks/cituro", body); err == nil {
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

	if err := auth.Verify(req, "/webhooks/cituro", body); err == nil {
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

	if err := auth.Verify(req, "/webhooks/cituro", body); err == nil {
		t.Fatalf("expected unauthorized when sig tag doesn't match provider")
	}
}
