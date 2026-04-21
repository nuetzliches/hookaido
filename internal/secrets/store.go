package secrets

import "time"

// Record is the persisted shape of a single Pool entry. The Sealed field must
// already be AES-GCM wrapped (see Sealer); Store implementations never see the
// plaintext secret.
type Record struct {
	PoolName  string
	ID        string
	Sealed    []byte
	NotBefore time.Time
	NotAfter  time.Time // zero = unbounded
	CreatedAt time.Time
}

// Store persists runtime-added secrets so they survive process restart.
//
// Implementations must be safe for concurrent use. Upsert is idempotent on
// (PoolName, ID): a second call with the same key replaces the previous record.
// This keeps the store tolerant of admin-API retries and reload replays.
//
// The Store does not know about Version validity windows at the application
// layer — those live in Pool. The Store only stores and returns Records.
type Store interface {
	// Upsert inserts or replaces the record keyed by (PoolName, ID).
	Upsert(rec Record) error

	// Delete removes the record with the given (poolName, id). Returns true if
	// something was removed, false if not found. A missing record is not an error.
	Delete(poolName, id string) (bool, error)

	// ListByPool returns all records for the pool. Order is unspecified.
	ListByPool(poolName string) ([]Record, error)

	// ListAll returns records for every pool. Used on startup to hydrate all
	// registered pools in one round trip.
	ListAll() ([]Record, error)
}

func cloneRecord(r Record) Record {
	dup := r
	if r.Sealed != nil {
		dup.Sealed = append([]byte(nil), r.Sealed...)
	}
	return dup
}
