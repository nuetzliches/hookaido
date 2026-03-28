package ingress

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
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
