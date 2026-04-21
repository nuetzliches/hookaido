package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
)

// Sealer wraps plaintext secrets in AES-256-GCM. The output layout is:
//
//	[1B version=0x01][12B nonce][N-byte ciphertext || 16B tag]
//
// A version prefix is included so that future algorithm changes (e.g. key rotation
// via KMS, a different AEAD) can be rolled out without breaking stored records.
type Sealer struct {
	aead cipher.AEAD
}

const (
	sealVersionV1  byte = 0x01
	sealNonceLen        = 12
	sealMinLength       = 1 + sealNonceLen + 16 // version + nonce + GCM tag (empty payload)
)

var (
	ErrSealKeyMissing    = errors.New("secrets: encryption key is empty")
	ErrSealKeyLength     = errors.New("secrets: encryption key must decode to 32 bytes")
	ErrSealedTruncated   = errors.New("secrets: sealed payload is truncated")
	ErrSealedVersion     = errors.New("secrets: unsupported sealed version")
	ErrSealedAuthFailure = errors.New("secrets: sealed payload failed authentication")
)

// NewSealer decodes a base64-encoded 32-byte key and constructs an AES-256-GCM AEAD.
// Whitespace around the key is tolerated; both standard and URL encodings are accepted,
// with or without padding.
func NewSealer(keyB64 string) (*Sealer, error) {
	trimmed := ""
	for _, r := range keyB64 {
		if r == ' ' || r == '\n' || r == '\r' || r == '\t' {
			continue
		}
		trimmed += string(r)
	}
	if trimmed == "" {
		return nil, ErrSealKeyMissing
	}

	key, err := decodeBase64Flexible(trimmed)
	if err != nil {
		return nil, fmt.Errorf("secrets: decode encryption key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("%w (got %d)", ErrSealKeyLength, len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("secrets: aes.NewCipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secrets: cipher.NewGCM: %w", err)
	}
	if aead.NonceSize() != sealNonceLen {
		return nil, fmt.Errorf("secrets: unexpected GCM nonce size %d", aead.NonceSize())
	}
	return &Sealer{aead: aead}, nil
}

func decodeBase64Flexible(s string) ([]byte, error) {
	encodings := []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	}
	var lastErr error
	for _, enc := range encodings {
		if decoded, err := enc.DecodeString(s); err == nil {
			return decoded, nil
		} else {
			lastErr = err
		}
	}
	return nil, lastErr
}

// Seal encrypts plaintext and returns the versioned sealed payload.
func (s *Sealer) Seal(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, sealNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("secrets: nonce: %w", err)
	}
	ct := s.aead.Seal(nil, nonce, plaintext, nil)

	out := make([]byte, 0, 1+sealNonceLen+len(ct))
	out = append(out, sealVersionV1)
	out = append(out, nonce...)
	out = append(out, ct...)
	return out, nil
}

// Open reverses Seal. It returns ErrSealedAuthFailure on tag mismatch or wrong key.
func (s *Sealer) Open(sealed []byte) ([]byte, error) {
	if len(sealed) < sealMinLength {
		return nil, ErrSealedTruncated
	}
	if sealed[0] != sealVersionV1 {
		return nil, fmt.Errorf("%w: 0x%02x", ErrSealedVersion, sealed[0])
	}
	nonce := sealed[1 : 1+sealNonceLen]
	ct := sealed[1+sealNonceLen:]
	pt, err := s.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSealedAuthFailure, err)
	}
	return pt, nil
}
