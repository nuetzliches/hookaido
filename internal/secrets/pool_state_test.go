package secrets

import (
	"testing"
	"time"
)

func TestPool_StateAt_CensusesByValidity(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	p, err := NewPool("rotating", true, 0, []Version{
		// Expired an hour ago.
		makeVersion("gone", now.Add(-3*time.Hour), now.Add(-time.Hour)),
		// Live, and the first of the live ones to lapse.
		makeVersion("current", now.Add(-time.Hour), now.Add(30*time.Minute)),
		// Live, and the last one standing.
		makeVersion("next", now.Add(-time.Minute), now.Add(2*time.Hour)),
		// Not live yet.
		makeVersion("future", now.Add(time.Hour), now.Add(5*time.Hour)),
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	st := p.StateAt(now)
	if st.Name != "rotating" || !st.Runtime {
		t.Fatalf("identity = (%q, runtime=%v), want (\"rotating\", true)", st.Name, st.Runtime)
	}
	if st.Total != 4 {
		t.Fatalf("Total = %d, want 4", st.Total)
	}
	if st.Valid != 2 || st.Pending != 1 || st.Expired != 1 {
		t.Fatalf("census = valid %d / pending %d / expired %d, want 2 / 1 / 1", st.Valid, st.Pending, st.Expired)
	}
	if st.Unbounded {
		t.Fatalf("Unbounded = true, want false: every valid version has a ValidUntil")
	}
	if want := now.Add(30 * time.Minute); !st.NextExpiry.Equal(want) {
		t.Fatalf("NextExpiry = %s, want %s (the soonest lapse among valid versions)", st.NextExpiry, want)
	}
	// The point of separating the two: NextExpiry is 30 minutes out, but the
	// pool keeps authenticating for two hours. An alert on NextExpiry would
	// fire on every handover of this perfectly healthy rotation.
	if want := now.Add(2 * time.Hour); !st.Exhaustion.Equal(want) {
		t.Fatalf("Exhaustion = %s, want %s (when the last valid version lapses)", st.Exhaustion, want)
	}
}

func TestPool_StateAt_EmptyOfValidVersions(t *testing.T) {
	// The #295 shape: the pool is not empty, but nothing in it is live, so
	// every request it backs is rejected. Size() cannot tell this apart from a
	// healthy pool.
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	p, err := NewPool("lapsed", true, 0, []Version{
		makeVersion("old", now.Add(-3*time.Hour), now.Add(-time.Hour)),
		makeVersion("scheduled", now.Add(time.Hour), time.Time{}),
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	st := p.StateAt(now)
	if st.Total != 2 {
		t.Fatalf("Total = %d, want 2", st.Total)
	}
	if st.Valid != 0 {
		t.Fatalf("Valid = %d, want 0", st.Valid)
	}
	if st.Expired != 1 || st.Pending != 1 {
		t.Fatalf("census = pending %d / expired %d, want 1 / 1", st.Pending, st.Expired)
	}
	if !st.NextExpiry.IsZero() || !st.Exhaustion.IsZero() {
		t.Fatalf("deadlines = next %s / exhaustion %s, want both zero when nothing is valid", st.NextExpiry, st.Exhaustion)
	}
	if st.Unbounded {
		t.Fatalf("Unbounded = true, want false: the unbounded version is not valid yet")
	}
}

func TestPool_StateAt_UnboundedVersionClearsExhaustion(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	p, err := NewPool("mixed", false, 0, []Version{
		makeVersion("bounded", now.Add(-time.Hour), now.Add(time.Hour)),
		makeVersion("forever", now.Add(-time.Hour), time.Time{}),
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	st := p.StateAt(now)
	if st.Valid != 2 {
		t.Fatalf("Valid = %d, want 2", st.Valid)
	}
	if !st.Unbounded {
		t.Fatalf("Unbounded = false, want true")
	}
	if want := now.Add(time.Hour); !st.NextExpiry.Equal(want) {
		t.Fatalf("NextExpiry = %s, want %s: the bounded version still lapses", st.NextExpiry, want)
	}
	if !st.Exhaustion.IsZero() {
		t.Fatalf("Exhaustion = %s, want zero: the pool never runs dry while an unbounded version is live", st.Exhaustion)
	}
}

func TestRegistry_StatesAt_OrderedByName(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	r := NewRegistry()
	for _, name := range []string{"zulu", "alpha", "mike"} {
		p, err := NewPool(name, true, 0, []Version{makeVersion(name, now.Add(-time.Hour), time.Time{})})
		if err != nil {
			t.Fatalf("NewPool %q: %v", name, err)
		}
		if err := r.Register(p); err != nil {
			t.Fatalf("Register %q: %v", name, err)
		}
	}

	states := r.StatesAt(now)
	got := make([]string, 0, len(states))
	for _, st := range states {
		got = append(got, st.Name)
		if st.Valid != 1 {
			t.Fatalf("pool %q Valid = %d, want 1", st.Name, st.Valid)
		}
	}
	want := []string{"alpha", "mike", "zulu"}
	if len(got) != len(want) {
		t.Fatalf("StatesAt returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("StatesAt returned %v, want %v", got, want)
		}
	}
}
