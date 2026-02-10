package secrets

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

var (
	ErrInvalidSecret = errors.New("invalid secret")
)

// Version is a single secret value with a validity window.
//
// Semantics:
// - ValidFrom is inclusive.
// - ValidUntil is exclusive; a zero value means "no end".
type Version struct {
	ID         string
	Value      []byte
	ValidFrom  time.Time
	ValidUntil time.Time
}

func (v Version) IsValidAt(t time.Time) bool {
	if v.ValidFrom.IsZero() {
		return false
	}
	if t.Before(v.ValidFrom) {
		return false
	}
	if v.ValidUntil.IsZero() {
		return true
	}
	return t.Before(v.ValidUntil)
}

type Set struct {
	Versions []Version
}

func (s Set) Validate() error {
	if len(s.Versions) == 0 {
		return fmt.Errorf("%w: empty set", ErrInvalidSecret)
	}

	seen := make(map[string]struct{}, len(s.Versions))
	for i, v := range s.Versions {
		if v.ID == "" {
			return fmt.Errorf("%w: versions[%d].id is empty", ErrInvalidSecret, i)
		}
		if _, ok := seen[v.ID]; ok {
			return fmt.Errorf("%w: duplicate secret id %q", ErrInvalidSecret, v.ID)
		}
		seen[v.ID] = struct{}{}

		if len(v.Value) == 0 {
			return fmt.Errorf("%w: versions[%d].value is empty", ErrInvalidSecret, i)
		}
		if v.ValidFrom.IsZero() {
			return fmt.Errorf("%w: versions[%d].valid_from is missing", ErrInvalidSecret, i)
		}
		if !v.ValidUntil.IsZero() && !v.ValidUntil.After(v.ValidFrom) {
			return fmt.Errorf("%w: versions[%d].valid_until must be after valid_from", ErrInvalidSecret, i)
		}
	}
	return nil
}

// ValidAt returns all secrets valid at time t, ordered by ValidFrom descending.
func (s Set) ValidAt(t time.Time) []Version {
	out := make([]Version, 0, len(s.Versions))
	for _, v := range s.Versions {
		if v.IsValidAt(t) {
			out = append(out, v)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ValidFrom.Equal(out[j].ValidFrom) {
			return out[i].ID < out[j].ID
		}
		return out[i].ValidFrom.After(out[j].ValidFrom)
	})
	return out
}

// SigningAt returns the newest (most recent ValidFrom) secret that is valid at time t.
func (s Set) SigningAt(t time.Time) (Version, bool) {
	valid := s.ValidAt(t)
	if len(valid) == 0 {
		return Version{}, false
	}
	return valid[0], true
}
