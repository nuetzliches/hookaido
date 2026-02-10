package secrets

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

var ErrSecretRef = errors.New("invalid secret reference")

// LoadRef loads a secret value from a reference string.
//
// Supported forms:
// - env:NAME
// - file:/path/to/secret
// - raw:literal-value (intended for tests/dev only)
func LoadRef(ref string) ([]byte, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, fmt.Errorf("%w: empty", ErrSecretRef)
	}

	switch {
	case strings.HasPrefix(ref, "env:"):
		name := strings.TrimSpace(strings.TrimPrefix(ref, "env:"))
		if name == "" {
			return nil, fmt.Errorf("%w: env var name is empty", ErrSecretRef)
		}
		val := os.Getenv(name)
		if val == "" {
			return nil, fmt.Errorf("%w: env var %q is empty or missing", ErrSecretRef, name)
		}
		return []byte(val), nil
	case strings.HasPrefix(ref, "file:"):
		path := strings.TrimSpace(strings.TrimPrefix(ref, "file:"))
		if path == "" {
			return nil, fmt.Errorf("%w: file path is empty", ErrSecretRef)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		val := strings.TrimSpace(string(b))
		if val == "" {
			return nil, fmt.Errorf("%w: file %q is empty", ErrSecretRef, path)
		}
		return []byte(val), nil
	case strings.HasPrefix(ref, "raw:"):
		val := strings.TrimPrefix(ref, "raw:")
		if val == "" {
			return nil, fmt.Errorf("%w: raw value is empty", ErrSecretRef)
		}
		return []byte(val), nil
	default:
		return nil, fmt.Errorf("%w: unsupported scheme (use env:, file:, or raw:)", ErrSecretRef)
	}
}
