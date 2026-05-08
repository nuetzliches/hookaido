package postgres

import "testing"

// TruncateForTest clears all queue/attempt/trend/secret tables on the given
// store. It is exported solely for cross-package integration tests and panics
// on any error so callers don't have to plumb the *testing.T deeper.
func TruncateForTest(t *testing.T, s *Store) {
	t.Helper()
	if s == nil || s.db == nil {
		return
	}
	for _, table := range []string{"queue_items", "delivery_attempts", "backlog_trend_samples", "runtime_secrets"} {
		if _, err := s.db.Exec("DELETE FROM " + table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
}
