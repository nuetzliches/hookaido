package ingress

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
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
