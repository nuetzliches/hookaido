package secrets

import (
	"testing"
	"time"
)

func TestVersion_IsValidAt_Boundaries(t *testing.T) {
	from := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	until := from.Add(10 * time.Minute)
	v := Version{
		ID:         "k1",
		Value:      []byte("secret"),
		ValidFrom:  from,
		ValidUntil: until,
	}

	if !v.IsValidAt(from) {
		t.Fatalf("expected valid at valid_from (inclusive)")
	}
	if v.IsValidAt(until) {
		t.Fatalf("expected invalid at valid_until (exclusive)")
	}
	if v.IsValidAt(from.Add(-time.Nanosecond)) {
		t.Fatalf("expected invalid before valid_from")
	}
}

func TestSet_ValidAtAndSigningAt(t *testing.T) {
	v1 := Version{
		ID:         "k1",
		Value:      []byte("s1"),
		ValidFrom:  time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		ValidUntil: time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC),
	}
	v2 := Version{
		ID:        "k2",
		Value:     []byte("s2"),
		ValidFrom: time.Date(2026, 2, 5, 0, 0, 0, 0, time.UTC),
		// no end
	}
	s := Set{Versions: []Version{v1, v2}}

	t1 := time.Date(2026, 2, 4, 0, 0, 0, 0, time.UTC)
	got := s.ValidAt(t1)
	if len(got) != 1 || got[0].ID != "k1" {
		t.Fatalf("expected only k1 to be valid at %s, got %#v", t1, got)
	}
	sign, ok := s.SigningAt(t1)
	if !ok || sign.ID != "k1" {
		t.Fatalf("expected signing secret k1 at %s, got ok=%v sign=%#v", t1, ok, sign)
	}

	t2 := time.Date(2026, 2, 6, 0, 0, 0, 0, time.UTC)
	got = s.ValidAt(t2)
	if len(got) != 2 || got[0].ID != "k2" || got[1].ID != "k1" {
		t.Fatalf("expected [k2,k1] at %s, got %#v", t2, got)
	}
	sign, ok = s.SigningAt(t2)
	if !ok || sign.ID != "k2" {
		t.Fatalf("expected signing secret k2 at %s, got ok=%v sign=%#v", t2, ok, sign)
	}

	// At v1's valid_until (exclusive), only v2 remains.
	t3 := v1.ValidUntil
	got = s.ValidAt(t3)
	if len(got) != 1 || got[0].ID != "k2" {
		t.Fatalf("expected only k2 at %s, got %#v", t3, got)
	}
}

func TestSet_Validate(t *testing.T) {
	// ok
	okSet := Set{Versions: []Version{
		{
			ID:        "k1",
			Value:     []byte("s1"),
			ValidFrom: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		},
	}}
	if err := okSet.Validate(); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}

	now := time.Now()
	cases := []Set{
		{},
		{Versions: []Version{{ID: "", Value: []byte("x"), ValidFrom: now}}},
		{Versions: []Version{{ID: "k1", Value: nil, ValidFrom: now}}},
		{Versions: []Version{{ID: "k1", Value: []byte("x"), ValidFrom: time.Time{}}}},
		{Versions: []Version{{ID: "k1", Value: []byte("x"), ValidFrom: now, ValidUntil: now}}},
		{Versions: []Version{
			{ID: "k1", Value: []byte("x"), ValidFrom: now},
			{ID: "k1", Value: []byte("y"), ValidFrom: now.Add(1 * time.Hour)},
		}},
	}
	for i, c := range cases {
		if err := c.Validate(); err == nil {
			t.Fatalf("case %d: expected error", i)
		}
	}
}
