package secrets

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

func testKeyB64(t *testing.T) string {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return base64.StdEncoding.EncodeToString(key)
}

func TestSealer_RoundTrip(t *testing.T) {
	s, err := NewSealer(testKeyB64(t))
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	cases := [][]byte{
		[]byte(""),
		[]byte("x"),
		[]byte("whs_abc123"),
		bytes.Repeat([]byte{0xA5}, 4096),
	}
	for _, pt := range cases {
		sealed, err := s.Seal(pt)
		if err != nil {
			t.Fatalf("Seal: %v", err)
		}
		if sealed[0] != sealVersionV1 {
			t.Fatalf("expected version byte 0x%02x, got 0x%02x", sealVersionV1, sealed[0])
		}
		got, err := s.Open(sealed)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if !bytes.Equal(got, pt) {
			t.Fatalf("round-trip mismatch: got %q want %q", got, pt)
		}
	}
}

func TestSealer_RejectsWrongKey(t *testing.T) {
	a, err := NewSealer(testKeyB64(t))
	if err != nil {
		t.Fatalf("NewSealer a: %v", err)
	}
	b, err := NewSealer(testKeyB64(t))
	if err != nil {
		t.Fatalf("NewSealer b: %v", err)
	}
	sealed, err := a.Seal([]byte("whs_secret"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if _, err := b.Open(sealed); !errors.Is(err, ErrSealedAuthFailure) {
		t.Fatalf("expected ErrSealedAuthFailure, got %v", err)
	}
}

func TestSealer_RejectsTamperedCiphertext(t *testing.T) {
	s, err := NewSealer(testKeyB64(t))
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	sealed, err := s.Seal([]byte("whs_secret"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	tampered := append([]byte(nil), sealed...)
	tampered[len(tampered)-1] ^= 0x01
	if _, err := s.Open(tampered); !errors.Is(err, ErrSealedAuthFailure) {
		t.Fatalf("expected ErrSealedAuthFailure, got %v", err)
	}
}

func TestSealer_RejectsTruncated(t *testing.T) {
	s, err := NewSealer(testKeyB64(t))
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	if _, err := s.Open([]byte{0x01, 0x02}); !errors.Is(err, ErrSealedTruncated) {
		t.Fatalf("expected ErrSealedTruncated, got %v", err)
	}
}

func TestSealer_RejectsUnsupportedVersion(t *testing.T) {
	s, err := NewSealer(testKeyB64(t))
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	sealed, err := s.Seal([]byte("x"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	sealed[0] = 0xFF
	if _, err := s.Open(sealed); !errors.Is(err, ErrSealedVersion) {
		t.Fatalf("expected ErrSealedVersion, got %v", err)
	}
}

func TestSealer_KeyErrors(t *testing.T) {
	if _, err := NewSealer(""); !errors.Is(err, ErrSealKeyMissing) {
		t.Fatalf("expected ErrSealKeyMissing, got %v", err)
	}
	if _, err := NewSealer("   \n\t"); !errors.Is(err, ErrSealKeyMissing) {
		t.Fatalf("expected ErrSealKeyMissing for whitespace, got %v", err)
	}
	// 16 bytes instead of 32
	short := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 16))
	if _, err := NewSealer(short); !errors.Is(err, ErrSealKeyLength) {
		t.Fatalf("expected ErrSealKeyLength, got %v", err)
	}
	// non-base64
	if _, err := NewSealer("not-b64!@#$%^&*"); err == nil {
		t.Fatalf("expected decode error, got nil")
	}
}

func TestSealer_AcceptsPaddedAndRawBase64(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	padded := base64.StdEncoding.EncodeToString(key)
	raw := base64.RawStdEncoding.EncodeToString(key)
	if strings.HasSuffix(raw, "=") {
		t.Fatalf("raw encoding should not contain padding")
	}
	for _, k := range []string{padded, raw} {
		if _, err := NewSealer(k); err != nil {
			t.Fatalf("NewSealer(%q): %v", k, err)
		}
	}
}

func TestSealer_NonceUniqueness(t *testing.T) {
	s, err := NewSealer(testKeyB64(t))
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	const iterations = 10000
	seen := make(map[string]struct{}, iterations)
	for i := 0; i < iterations; i++ {
		sealed, err := s.Seal([]byte("x"))
		if err != nil {
			t.Fatalf("Seal: %v", err)
		}
		nonce := string(sealed[1 : 1+sealNonceLen])
		if _, dup := seen[nonce]; dup {
			t.Fatalf("duplicate nonce at iteration %d", i)
		}
		seen[nonce] = struct{}{}
	}
}
