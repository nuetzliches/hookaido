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
