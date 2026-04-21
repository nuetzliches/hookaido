package secrets

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func makeVersion(id string, from time.Time, until time.Time) Version {
	return Version{
		ID:         id,
		Value:      []byte("secret-" + id),
		ValidFrom:  from,
		ValidUntil: until,
	}
}

func TestPool_AddListValidAt(t *testing.T) {
	p, err := NewPool("test", true, 0, nil)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if p.MaxVersions() != DefaultMaxVersions {
		t.Fatalf("expected default %d, got %d", DefaultMaxVersions, p.MaxVersions())
	}

	base := time.Date(2026, 4, 21, 0, 0, 0, 0, time.UTC)
	v1 := makeVersion("v1", base, time.Time{})
	v2 := makeVersion("v2", base.Add(time.Hour), time.Time{})

	if err := p.Add(v1); err != nil {
		t.Fatalf("Add v1: %v", err)
	}
	if err := p.Add(v2); err != nil {
		t.Fatalf("Add v2: %v", err)
	}
	if p.Size() != 2 {
		t.Fatalf("Size: got %d want 2", p.Size())
	}

	list := p.List()
	if len(list) != 2 || list[0].ID != "v2" || list[1].ID != "v1" {
		t.Fatalf("expected [v2,v1], got %+v", list)
	}
	// Deep copy — caller mutation must not affect the pool.
	list[0].Value[0] = 'X'
	if bytes.Equal(list[0].Value, p.List()[0].Value) {
		t.Fatalf("List did not deep-copy Value")
	}

	valid := p.ValidAt(base.Add(30 * time.Minute))
	if len(valid) != 1 || valid[0].ID != "v1" {
		t.Fatalf("ValidAt+30min expected [v1], got %+v", valid)
	}
	valid = p.ValidAt(base.Add(2 * time.Hour))
	if len(valid) != 2 || valid[0].ID != "v2" {
		t.Fatalf("ValidAt+2h expected [v2,v1], got %+v", valid)
	}
}

func TestPool_Metadata_OmitsValue(t *testing.T) {
	p, _ := NewPool("m", true, 0, nil)
	from := time.Now().UTC()
	if err := p.Add(makeVersion("v1", from, from.Add(time.Hour))); err != nil {
		t.Fatalf("Add: %v", err)
	}
	md := p.ListMetadata()
	if len(md) != 1 || md[0].ID != "v1" {
		t.Fatalf("Metadata: %+v", md)
	}
	// VersionMetadata type itself has no Value field; compile-time guarantee.
	_ = md[0].ValidFrom
	_ = md[0].ValidUntil
}

func TestPool_Add_Duplicate(t *testing.T) {
	p, _ := NewPool("d", true, 0, nil)
	base := time.Now().UTC()
	if err := p.Add(makeVersion("v1", base, time.Time{})); err != nil {
		t.Fatalf("Add: %v", err)
	}
	err := p.Add(makeVersion("v1", base.Add(time.Hour), time.Time{}))
	if !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("expected ErrDuplicateID, got %v", err)
	}
}

func TestPool_Add_Validation(t *testing.T) {
	p, _ := NewPool("val", true, 0, nil)
	base := time.Now().UTC()

	if err := p.Add(Version{ID: "", Value: []byte("x"), ValidFrom: base}); !errors.Is(err, ErrEmptyID) {
		t.Fatalf("expected ErrEmptyID, got %v", err)
	}
	if err := p.Add(Version{ID: "a", Value: nil, ValidFrom: base}); !errors.Is(err, ErrEmptyValue) {
		t.Fatalf("expected ErrEmptyValue, got %v", err)
	}
	if err := p.Add(Version{ID: "a", Value: []byte("x")}); !errors.Is(err, ErrMissingFrom) {
		t.Fatalf("expected ErrMissingFrom, got %v", err)
	}
	bad := Version{ID: "a", Value: []byte("x"), ValidFrom: base, ValidUntil: base}
	if err := p.Add(bad); !errors.Is(err, ErrInvalidWindow) {
		t.Fatalf("expected ErrInvalidWindow, got %v", err)
	}
}

func TestPool_PoolFull_PrunesExpired(t *testing.T) {
	p, _ := NewPool("cap", true, 3, nil)
	base := time.Now().UTC()

	// Fill with expired entries (ValidUntil in the past)
	past := base.Add(-2 * time.Hour)
	pastEnd := base.Add(-time.Hour)
	for i := 0; i < 3; i++ {
		v := makeVersion(fmt.Sprintf("old%d", i), past, pastEnd)
		if err := p.Add(v); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	if p.Size() != 3 {
		t.Fatalf("setup size: %d", p.Size())
	}

	// One more Add should auto-prune expired and succeed.
	fresh := makeVersion("new1", base, time.Time{})
	if err := p.Add(fresh); err != nil {
		t.Fatalf("Add after prune: %v", err)
	}
	if p.Size() != 1 {
		t.Fatalf("after prune expected 1, got %d", p.Size())
	}
}

func TestPool_PoolFull_ReturnsError(t *testing.T) {
	p, _ := NewPool("cap", true, 2, nil)
	base := time.Now().UTC()
	for i := 0; i < 2; i++ {
		v := makeVersion(fmt.Sprintf("v%d", i), base, time.Time{})
		if err := p.Add(v); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	err := p.Add(makeVersion("v3", base, time.Time{}))
	if !errors.Is(err, ErrPoolFull) {
		t.Fatalf("expected ErrPoolFull, got %v", err)
	}
}

func TestPool_Remove(t *testing.T) {
	p, _ := NewPool("r", true, 0, nil)
	base := time.Now().UTC()
	_ = p.Add(makeVersion("v1", base, time.Time{}))
	_ = p.Add(makeVersion("v2", base.Add(time.Hour), time.Time{}))

	removed, ok := p.Remove("v1")
	if !ok || removed.ID != "v1" {
		t.Fatalf("Remove v1: ok=%v removed=%+v", ok, removed)
	}
	if p.Size() != 1 {
		t.Fatalf("Size after remove: %d", p.Size())
	}
	if _, ok := p.Remove("unknown"); ok {
		t.Fatalf("Remove unknown should be false")
	}
}

func TestPool_Replace(t *testing.T) {
	p, _ := NewPool("repl", false, 4, nil)
	base := time.Now().UTC()
	seed := []Version{
		makeVersion("a", base, time.Time{}),
		makeVersion("b", base.Add(time.Hour), time.Time{}),
	}
	if err := p.Replace(seed); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if p.Size() != 2 {
		t.Fatalf("Size: %d", p.Size())
	}

	// Caller-side mutation of seed slice must not leak in.
	seed[0].Value[0] = 'X'
	if p.List()[1].Value[0] == 'X' {
		t.Fatalf("Replace failed to deep-copy")
	}

	// Duplicate ID in Replace
	err := p.Replace([]Version{
		makeVersion("a", base, time.Time{}),
		makeVersion("a", base.Add(time.Hour), time.Time{}),
	})
	if !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("expected ErrDuplicateID, got %v", err)
	}

	// Over-cap Replace
	big := make([]Version, 5)
	for i := range big {
		big[i] = makeVersion(fmt.Sprintf("k%d", i), base.Add(time.Duration(i)*time.Minute), time.Time{})
	}
	if err := p.Replace(big); !errors.Is(err, ErrPoolFull) {
		t.Fatalf("expected ErrPoolFull, got %v", err)
	}
}

func TestPool_PruneExpired(t *testing.T) {
	base := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)

	t.Run("empty pool returns nil", func(t *testing.T) {
		p, _ := NewPool("empty", true, 0, nil)
		if got := p.PruneExpired(base); got != nil {
			t.Fatalf("expected nil, got %v", got)
		}
	})

	t.Run("unbounded versions are never pruned", func(t *testing.T) {
		p, _ := NewPool("unbounded", true, 0, nil)
		_ = p.Add(makeVersion("v1", base.Add(-2*time.Hour), time.Time{}))
		_ = p.Add(makeVersion("v2", base.Add(-time.Hour), time.Time{}))
		if got := p.PruneExpired(base); len(got) != 0 {
			t.Fatalf("expected 0, got %v", got)
		}
		if p.Size() != 2 {
			t.Fatalf("Size: %d want 2", p.Size())
		}
	})

	t.Run("expired versions are removed, live ones kept", func(t *testing.T) {
		p, _ := NewPool("mixed", true, 0, nil)
		_ = p.Add(makeVersion("exp1", base.Add(-3*time.Hour), base.Add(-2*time.Hour)))
		_ = p.Add(makeVersion("exp2", base.Add(-3*time.Hour), base.Add(-time.Hour)))
		_ = p.Add(makeVersion("live", base, base.Add(time.Hour)))
		_ = p.Add(makeVersion("unbounded", base, time.Time{}))

		removed := p.PruneExpired(base)
		if len(removed) != 2 {
			t.Fatalf("expected 2 removed, got %v", removed)
		}
		gotIDs := map[string]bool{}
		for _, id := range removed {
			gotIDs[id] = true
		}
		if !gotIDs["exp1"] || !gotIDs["exp2"] {
			t.Fatalf("expected exp1+exp2, got %v", removed)
		}
		if p.Size() != 2 {
			t.Fatalf("Size after prune: %d want 2", p.Size())
		}
		for _, v := range p.List() {
			if v.ID == "exp1" || v.ID == "exp2" {
				t.Fatalf("pruned version %q still present", v.ID)
			}
		}
	})

	t.Run("ValidUntil equal to now is pruned (exclusive boundary)", func(t *testing.T) {
		p, _ := NewPool("boundary", true, 0, nil)
		_ = p.Add(makeVersion("edge", base.Add(-time.Hour), base))
		removed := p.PruneExpired(base)
		if len(removed) != 1 || removed[0] != "edge" {
			t.Fatalf("expected [edge] removed, got %v", removed)
		}
	})

	t.Run("idempotent", func(t *testing.T) {
		p, _ := NewPool("idem", true, 0, nil)
		_ = p.Add(makeVersion("expired", base.Add(-2*time.Hour), base.Add(-time.Hour)))
		_ = p.Add(makeVersion("live", base, time.Time{}))

		first := p.PruneExpired(base)
		second := p.PruneExpired(base)
		if len(first) != 1 || first[0] != "expired" {
			t.Fatalf("first sweep: %v", first)
		}
		if len(second) != 0 {
			t.Fatalf("second sweep should be a no-op, got %v", second)
		}
	})

	t.Run("concurrent PruneExpired + ValidAt + Add", func(t *testing.T) {
		p, _ := NewPool("concurrent-gc", true, 512, nil)
		// Seed with a mix of expired and live versions.
		for i := 0; i < 32; i++ {
			_ = p.Add(makeVersion(fmt.Sprintf("exp%d", i),
				base.Add(-2*time.Hour), base.Add(-time.Hour)))
		}
		for i := 0; i < 32; i++ {
			_ = p.Add(makeVersion(fmt.Sprintf("live%d", i),
				base, time.Time{}))
		}

		var wg sync.WaitGroup
		wg.Add(3)
		// Pruner
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				_ = p.PruneExpired(base)
			}
		}()
		// Reader
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_ = p.ValidAt(base)
				_ = p.List()
			}
		}()
		// Writer
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = p.Add(makeVersion(fmt.Sprintf("new%d", i),
					base.Add(time.Duration(i)*time.Millisecond), time.Time{}))
			}
		}()
		wg.Wait()

		// After the race, only live + new versions should remain; all exp* must be gone.
		for _, v := range p.List() {
			if len(v.ID) >= 3 && v.ID[:3] == "exp" {
				t.Fatalf("expired version %q survived prune race", v.ID)
			}
		}
	})
}

func TestPool_ConcurrentAddAndValidAt(t *testing.T) {
	p, _ := NewPool("concurrent", true, 256, nil)
	base := time.Now().UTC()

	const n = 128
	var wg sync.WaitGroup
	wg.Add(n * 2)

	// Writers
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			v := makeVersion(fmt.Sprintf("v%03d", i), base.Add(time.Duration(i)*time.Millisecond), time.Time{})
			if err := p.Add(v); err != nil {
				t.Errorf("Add: %v", err)
			}
		}(i)
	}

	// Readers
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_ = p.ValidAt(time.Now().UTC())
			_ = p.List()
			_ = p.ListMetadata()
			_ = p.Size()
		}()
	}

	wg.Wait()
	if got := p.Size(); got != n {
		t.Fatalf("expected %d versions, got %d", n, got)
	}
}
