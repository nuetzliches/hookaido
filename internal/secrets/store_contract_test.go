package secrets_test

import (
	"bytes"
	"sort"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/secrets"
)

type secretStoreFactory struct {
	name string
	new  func(t *testing.T) secrets.Store
}

// extraSecretStoreFactories lets backend-specific test files (added in Steps 5,
// 6 for sqlite/postgres) append their factories via init().
var extraSecretStoreFactories []secretStoreFactory

// contractSecretStoreFactories returns all Store implementations to exercise
// against the shared contract.
func contractSecretStoreFactories() []secretStoreFactory {
	out := []secretStoreFactory{
		{
			name: "memory",
			new: func(t *testing.T) secrets.Store {
				t.Helper()
				return secrets.NewMemoryStore()
			},
		},
	}
	out = append(out, extraSecretStoreFactories...)
	return out
}

func TestSecretStoreContract_UpsertAndList(t *testing.T) {
	base := time.Date(2026, 4, 21, 10, 0, 0, 0, time.UTC)
	for _, factory := range contractSecretStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			store := factory.new(t)

			recs := []secrets.Record{
				{PoolName: "alpha", ID: "sec_a1", Sealed: []byte{0x01, 0xde, 0xad},
					NotBefore: base, CreatedAt: base},
				{PoolName: "alpha", ID: "sec_a2", Sealed: []byte{0x01, 0xbe, 0xef},
					NotBefore: base.Add(time.Hour), NotAfter: base.Add(2 * time.Hour), CreatedAt: base},
				{PoolName: "beta", ID: "sec_b1", Sealed: []byte{0x01, 0xca, 0xfe},
					NotBefore: base, CreatedAt: base},
			}
			for _, r := range recs {
				if err := store.Upsert(r); err != nil {
					t.Fatalf("Upsert %s/%s: %v", r.PoolName, r.ID, err)
				}
			}

			got, err := store.ListByPool("alpha")
			if err != nil {
				t.Fatalf("ListByPool: %v", err)
			}
			sort.Slice(got, func(i, j int) bool { return got[i].ID < got[j].ID })
			if len(got) != 2 || got[0].ID != "sec_a1" || got[1].ID != "sec_a2" {
				t.Fatalf("ListByPool(alpha): %+v", got)
			}
			if !bytes.Equal(got[0].Sealed, []byte{0x01, 0xde, 0xad}) {
				t.Fatalf("Sealed round-trip mismatch: %v", got[0].Sealed)
			}
			if !got[0].NotBefore.Equal(base) {
				t.Fatalf("NotBefore round-trip: got %v want %v", got[0].NotBefore, base)
			}
			if !got[1].NotAfter.Equal(base.Add(2 * time.Hour)) {
				t.Fatalf("NotAfter round-trip: got %v", got[1].NotAfter)
			}
			if !got[0].NotAfter.IsZero() {
				t.Fatalf("NotAfter should be zero for unbounded, got %v", got[0].NotAfter)
			}

			all, err := store.ListAll()
			if err != nil {
				t.Fatalf("ListAll: %v", err)
			}
			if len(all) != 3 {
				t.Fatalf("ListAll: got %d, want 3", len(all))
			}
		})
	}
}

func TestSecretStoreContract_UpsertIdempotent(t *testing.T) {
	for _, factory := range contractSecretStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			store := factory.new(t)
			base := time.Date(2026, 4, 21, 10, 0, 0, 0, time.UTC)

			rec := secrets.Record{
				PoolName: "alpha", ID: "sec_a1", Sealed: []byte{0x01, 0xaa},
				NotBefore: base, CreatedAt: base,
			}
			if err := store.Upsert(rec); err != nil {
				t.Fatalf("Upsert 1: %v", err)
			}
			// Second Upsert with same key, different Sealed.
			rec.Sealed = []byte{0x01, 0xbb}
			if err := store.Upsert(rec); err != nil {
				t.Fatalf("Upsert 2: %v", err)
			}
			got, err := store.ListByPool("alpha")
			if err != nil {
				t.Fatalf("ListByPool: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("expected 1 record after idempotent upsert, got %d", len(got))
			}
			if !bytes.Equal(got[0].Sealed, []byte{0x01, 0xbb}) {
				t.Fatalf("expected updated Sealed, got %v", got[0].Sealed)
			}
		})
	}
}

func TestSecretStoreContract_Delete(t *testing.T) {
	for _, factory := range contractSecretStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			store := factory.new(t)
			base := time.Date(2026, 4, 21, 10, 0, 0, 0, time.UTC)

			rec := secrets.Record{
				PoolName: "alpha", ID: "sec_a1", Sealed: []byte{0x01, 0x02},
				NotBefore: base, CreatedAt: base,
			}
			if err := store.Upsert(rec); err != nil {
				t.Fatalf("Upsert: %v", err)
			}

			ok, err := store.Delete("alpha", "sec_a1")
			if err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if !ok {
				t.Fatalf("expected Delete to return true")
			}
			ok, err = store.Delete("alpha", "sec_a1")
			if err != nil {
				t.Fatalf("Delete missing: %v", err)
			}
			if ok {
				t.Fatalf("expected Delete of missing to return false")
			}
			got, _ := store.ListByPool("alpha")
			if len(got) != 0 {
				t.Fatalf("expected empty, got %+v", got)
			}
		})
	}
}

func TestSecretStoreContract_ListByPool_Unknown(t *testing.T) {
	for _, factory := range contractSecretStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			store := factory.new(t)
			got, err := store.ListByPool("nowhere")
			if err != nil {
				t.Fatalf("ListByPool: %v", err)
			}
			if len(got) != 0 {
				t.Fatalf("expected empty, got %+v", got)
			}
		})
	}
}
