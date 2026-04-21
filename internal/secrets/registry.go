package secrets

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	ErrPoolExists   = errors.New("secrets: pool already registered")
	ErrPoolNotFound = errors.New("secrets: pool not found")
)

// Registry maps secret names to *Pool. Route-level closures capture pointers
// from here, so the registry is the single source of truth for "which pool does
// this secret_ref resolve to".
//
// Pointer identity of a Pool must survive config reload for runtime-mutable
// pools. Callers achieve this by calling Pool(name) and mutating via
// Pool.Add/Remove/Replace, rather than creating a fresh Pool and re-registering.
type Registry struct {
	mu    sync.RWMutex
	pools map[string]*Pool
}

func NewRegistry() *Registry {
	return &Registry{pools: make(map[string]*Pool)}
}

// Register inserts a pool under its name. Returns ErrPoolExists if the name is
// already taken by a different pool. Re-registering the exact same *Pool is a
// no-op (idempotent).
func (r *Registry) Register(p *Pool) error {
	if p == nil {
		return errors.New("secrets: Register nil pool")
	}
	if p.Name() == "" {
		return errors.New("secrets: Register pool with empty name")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.pools[p.Name()]; ok {
		if existing == p {
			return nil
		}
		return fmt.Errorf("%w: %q", ErrPoolExists, p.Name())
	}
	r.pools[p.Name()] = p
	return nil
}

// Pool returns the named pool, if registered.
func (r *Registry) Pool(name string) (*Pool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.pools[name]
	return p, ok
}

// Names returns the registered pool names in sorted order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.pools))
	for n := range r.pools {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Unregister removes a pool. Returns false if the name was not present.
// In-flight closures that captured the *Pool pointer continue to see the
// (now unreferenced-by-registry) pool until they are rebuilt on reload.
func (r *Registry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.pools[name]; !ok {
		return false
	}
	delete(r.pools, name)
	return true
}
