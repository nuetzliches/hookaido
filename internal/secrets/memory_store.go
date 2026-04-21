package secrets

import "sync"

// MemoryStore is an in-memory Store. It is the default when no persistent
// backend is configured, and it is used throughout tests.
type MemoryStore struct {
	mu    sync.Mutex
	pools map[string]map[string]Record
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{pools: make(map[string]map[string]Record)}
}

func (m *MemoryStore) Upsert(rec Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	bucket, ok := m.pools[rec.PoolName]
	if !ok {
		bucket = make(map[string]Record)
		m.pools[rec.PoolName] = bucket
	}
	bucket[rec.ID] = cloneRecord(rec)
	return nil
}

func (m *MemoryStore) Delete(poolName, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bucket, ok := m.pools[poolName]
	if !ok {
		return false, nil
	}
	if _, ok := bucket[id]; !ok {
		return false, nil
	}
	delete(bucket, id)
	if len(bucket) == 0 {
		delete(m.pools, poolName)
	}
	return true, nil
}

func (m *MemoryStore) ListByPool(poolName string) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bucket, ok := m.pools[poolName]
	if !ok {
		return nil, nil
	}
	out := make([]Record, 0, len(bucket))
	for _, rec := range bucket {
		out = append(out, cloneRecord(rec))
	}
	return out, nil
}

func (m *MemoryStore) ListAll() ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Record, 0, 8)
	for _, bucket := range m.pools {
		for _, rec := range bucket {
			out = append(out, cloneRecord(rec))
		}
	}
	return out, nil
}
