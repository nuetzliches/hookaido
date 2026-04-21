package secrets

import (
	"errors"
	"testing"
	"time"
)

func TestRegistry_RegisterAndLookup(t *testing.T) {
	r := NewRegistry()
	p1, _ := NewPool("alpha", true, 0, nil)
	p2, _ := NewPool("beta", false, 0, nil)

	if err := r.Register(p1); err != nil {
		t.Fatalf("Register p1: %v", err)
	}
	if err := r.Register(p2); err != nil {
		t.Fatalf("Register p2: %v", err)
	}

	got, ok := r.Pool("alpha")
	if !ok || got != p1 {
		t.Fatalf("Pool alpha: ok=%v pointer-match=%v", ok, got == p1)
	}
	got, ok = r.Pool("missing")
	if ok || got != nil {
		t.Fatalf("Pool missing: ok=%v got=%v", ok, got)
	}

	names := r.Names()
	if len(names) != 2 || names[0] != "alpha" || names[1] != "beta" {
		t.Fatalf("Names: %v", names)
	}
}

func TestRegistry_Register_Idempotent(t *testing.T) {
	r := NewRegistry()
	p, _ := NewPool("n", true, 0, nil)
	if err := r.Register(p); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := r.Register(p); err != nil {
		t.Fatalf("second Register (same pointer): %v", err)
	}
}

func TestRegistry_Register_Conflict(t *testing.T) {
	r := NewRegistry()
	p1, _ := NewPool("n", true, 0, nil)
	p2, _ := NewPool("n", true, 0, nil)
	if err := r.Register(p1); err != nil {
		t.Fatalf("Register p1: %v", err)
	}
	err := r.Register(p2)
	if !errors.Is(err, ErrPoolExists) {
		t.Fatalf("expected ErrPoolExists, got %v", err)
	}
}

func TestRegistry_PoolPointerSurvivesReload(t *testing.T) {
	// Simulates the critical invariant: a route-closure captures *Pool once;
	// subsequent config reloads must preserve the pointer so mutations via
	// Pool.Add/Remove remain visible to the closure.
	r := NewRegistry()
	p, _ := NewPool("cituro", true, 0, nil)
	if err := r.Register(p); err != nil {
		t.Fatalf("Register: %v", err)
	}
	captured, _ := r.Pool("cituro")

	base := time.Now().UTC()
	for cycle := 0; cycle < 10; cycle++ {
		// Simulate admin POST: mutation goes through Pool.Add on the same pointer.
		v := Version{
			ID:        "reload-" + string(rune('a'+cycle)),
			Value:     []byte("x"),
			ValidFrom: base.Add(time.Duration(cycle) * time.Millisecond),
		}
		if err := captured.Add(v); err != nil {
			t.Fatalf("Add at cycle %d: %v", cycle, err)
		}

		// Simulate reload: Register called again with the SAME pointer (idempotent).
		if err := r.Register(p); err != nil {
			t.Fatalf("Re-Register at cycle %d: %v", cycle, err)
		}

		got, _ := r.Pool("cituro")
		if got != captured {
			t.Fatalf("pointer changed at cycle %d", cycle)
		}
	}
	if captured.Size() != 10 {
		t.Fatalf("expected 10 versions after 10 reload cycles, got %d", captured.Size())
	}
}

func TestRegistry_Unregister(t *testing.T) {
	r := NewRegistry()
	p, _ := NewPool("x", true, 0, nil)
	_ = r.Register(p)
	if ok := r.Unregister("x"); !ok {
		t.Fatalf("Unregister should return true")
	}
	if _, ok := r.Pool("x"); ok {
		t.Fatalf("Pool still present after Unregister")
	}
	if ok := r.Unregister("x"); ok {
		t.Fatalf("Unregister of missing should return false")
	}
}
