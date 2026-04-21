package secrets

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// DefaultMaxVersions caps how many versions a Pool may hold before Add returns
// ErrPoolFull. Operators can override per-pool via the `max_versions` DSL flag.
const DefaultMaxVersions = 32

var (
	ErrDuplicateID   = errors.New("secrets: version id already present in pool")
	ErrPoolFull      = errors.New("secrets: pool is full")
	ErrInvalidWindow = errors.New("secrets: valid_until must be after valid_from")
	ErrEmptyID       = errors.New("secrets: version id is empty")
	ErrEmptyValue    = errors.New("secrets: version value is empty")
	ErrMissingFrom   = errors.New("secrets: version valid_from is missing")
)

// VersionMetadata is the safe-to-expose subset of a Version: everything except
// the secret material itself. Admin GET endpoints return this shape.
type VersionMetadata struct {
	ID         string    `json:"id"`
	ValidFrom  time.Time `json:"not_before"`
	ValidUntil time.Time `json:"not_after,omitempty"`
}

// Pool holds a named, mutable, thread-safe collection of secret Versions.
//
// Reads (ValidAt, List, ListMetadata, Size) use RLock and are cheap. Writes
// (Add, Remove, Replace) use Lock.
//
// Pointer identity matters: route-level closures built in internal/app/run.go
// capture *Pool and call ValidAt on every inbound request. Config reloads must
// mutate the pool in place (Add, Remove, Replace) rather than creating a new
// Pool, so that in-flight verification still sees fresh state.
type Pool struct {
	mu          sync.RWMutex
	name        string
	runtime     bool
	maxVersions int
	versions    []Version
}

// NewPool constructs a pool with an initial set of versions. If maxVersions <= 0,
// DefaultMaxVersions is used. The seed slice is validated and copied.
func NewPool(name string, runtime bool, maxVersions int, seed []Version) (*Pool, error) {
	if maxVersions <= 0 {
		maxVersions = DefaultMaxVersions
	}
	p := &Pool{name: name, runtime: runtime, maxVersions: maxVersions}
	if err := p.Replace(seed); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Pool) Name() string    { return p.name }
func (p *Pool) Runtime() bool   { return p.runtime }
func (p *Pool) MaxVersions() int { return p.maxVersions }

// Size returns the number of versions currently in the pool.
func (p *Pool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.versions)
}

// Add inserts v. Returns ErrDuplicateID, ErrPoolFull, or a validation error.
// If the pool is at capacity, Add first tries to prune versions whose
// ValidUntil is in the past (relative to time.Now()).
func (p *Pool) Add(v Version) error {
	if err := validateVersion(v); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for i := range p.versions {
		if p.versions[i].ID == v.ID {
			return fmt.Errorf("%w: %q", ErrDuplicateID, v.ID)
		}
	}

	if len(p.versions) >= p.maxVersions {
		p.pruneExpiredLocked(time.Now())
	}
	if len(p.versions) >= p.maxVersions {
		return fmt.Errorf("%w: pool %q has %d versions (cap %d)",
			ErrPoolFull, p.name, len(p.versions), p.maxVersions)
	}

	p.versions = append(p.versions, cloneVersion(v))
	sortVersionsDesc(p.versions)
	return nil
}

// Remove deletes the version with the given id. Returns (removed, true) if found.
func (p *Pool) Remove(id string) (Version, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.versions {
		if p.versions[i].ID == id {
			removed := cloneVersion(p.versions[i])
			p.versions = append(p.versions[:i], p.versions[i+1:]...)
			return removed, true
		}
	}
	return Version{}, false
}

// Replace swaps the entire version set. It validates each entry and enforces
// the maxVersions cap. Used at startup (seeding from Store) and during config
// reload for non-runtime pools.
func (p *Pool) Replace(versions []Version) error {
	cleaned := make([]Version, 0, len(versions))
	seen := make(map[string]struct{}, len(versions))
	for _, v := range versions {
		if err := validateVersion(v); err != nil {
			return err
		}
		if _, dup := seen[v.ID]; dup {
			return fmt.Errorf("%w: %q", ErrDuplicateID, v.ID)
		}
		seen[v.ID] = struct{}{}
		cleaned = append(cleaned, cloneVersion(v))
	}
	if len(cleaned) > p.maxVersionsOrDefault() {
		return fmt.Errorf("%w: %d versions exceed cap %d",
			ErrPoolFull, len(cleaned), p.maxVersionsOrDefault())
	}
	sortVersionsDesc(cleaned)

	p.mu.Lock()
	p.versions = cleaned
	p.mu.Unlock()
	return nil
}

// ValidAt returns a freshly-allocated slice of versions valid at t, sorted by
// ValidFrom descending (most recent first). Safe for concurrent callers.
func (p *Pool) ValidAt(t time.Time) []Version {
	p.mu.RLock()
	defer p.mu.RUnlock()

	out := make([]Version, 0, len(p.versions))
	for _, v := range p.versions {
		if v.IsValidAt(t) {
			out = append(out, cloneVersion(v))
		}
	}
	sortVersionsDesc(out)
	return out
}

// List returns a deep copy of all versions, sorted by ValidFrom descending.
func (p *Pool) List() []Version {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]Version, len(p.versions))
	for i, v := range p.versions {
		out[i] = cloneVersion(v)
	}
	return out
}

// ListMetadata returns id + validity window for each version, without the
// secret value. Admin GET handlers use this.
func (p *Pool) ListMetadata() []VersionMetadata {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]VersionMetadata, len(p.versions))
	for i, v := range p.versions {
		out[i] = VersionMetadata{ID: v.ID, ValidFrom: v.ValidFrom, ValidUntil: v.ValidUntil}
	}
	return out
}

func (p *Pool) pruneExpiredLocked(now time.Time) {
	kept := p.versions[:0]
	for _, v := range p.versions {
		if !v.ValidUntil.IsZero() && !v.ValidUntil.After(now) {
			continue
		}
		kept = append(kept, v)
	}
	p.versions = kept
}

func (p *Pool) maxVersionsOrDefault() int {
	if p.maxVersions <= 0 {
		return DefaultMaxVersions
	}
	return p.maxVersions
}

func validateVersion(v Version) error {
	if v.ID == "" {
		return ErrEmptyID
	}
	if len(v.Value) == 0 {
		return ErrEmptyValue
	}
	if v.ValidFrom.IsZero() {
		return ErrMissingFrom
	}
	if !v.ValidUntil.IsZero() && !v.ValidUntil.After(v.ValidFrom) {
		return fmt.Errorf("%w: id=%q", ErrInvalidWindow, v.ID)
	}
	return nil
}

func cloneVersion(v Version) Version {
	dup := v
	if v.Value != nil {
		dup.Value = append([]byte(nil), v.Value...)
	}
	return dup
}

func sortVersionsDesc(vs []Version) {
	sort.Slice(vs, func(i, j int) bool {
		if vs[i].ValidFrom.Equal(vs[j].ValidFrom) {
			return vs[i].ID < vs[j].ID
		}
		return vs[i].ValidFrom.After(vs[j].ValidFrom)
	})
}
