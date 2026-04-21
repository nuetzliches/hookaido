package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/nuetzliches/hookaido/internal/secrets"
)

// Compile-time check: *Store satisfies secrets.Store.
var _ secrets.Store = (*Store)(nil)

// Upsert inserts or replaces a runtime secret record. Idempotent on (pool_name, id).
func (s *Store) Upsert(rec secrets.Record) error {
	if rec.PoolName == "" {
		return fmt.Errorf("sqlite: runtime_secrets upsert: empty pool_name")
	}
	if rec.ID == "" {
		return fmt.Errorf("sqlite: runtime_secrets upsert: empty id")
	}
	if len(rec.Sealed) == 0 {
		return fmt.Errorf("sqlite: runtime_secrets upsert: empty sealed payload")
	}
	if rec.NotBefore.IsZero() {
		return fmt.Errorf("sqlite: runtime_secrets upsert: not_before is zero")
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = s.now()
	}

	startedAt := time.Now()
	_, err := s.db.ExecContext(context.Background(), `
INSERT INTO runtime_secrets (pool_name, id, sealed, not_before, not_after, created_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(pool_name, id) DO UPDATE SET
  sealed     = excluded.sealed,
  not_before = excluded.not_before,
  not_after  = excluded.not_after,
  created_at = excluded.created_at;
`,
		rec.PoolName,
		rec.ID,
		rec.Sealed,
		rec.NotBefore.UnixNano(),
		timeToUnixNanoOrZero(rec.NotAfter),
		rec.CreatedAt.UnixNano(),
	)
	if err != nil {
		s.observeSQLiteError(err)
		s.observeSQLiteTx(sqliteTxClassWrite, startedAt, false)
		return fmt.Errorf("sqlite: runtime_secrets upsert: %w", err)
	}
	s.observeSQLiteTx(sqliteTxClassWrite, startedAt, true)
	return nil
}

func (s *Store) Delete(poolName, id string) (bool, error) {
	startedAt := time.Now()
	res, err := s.db.ExecContext(context.Background(),
		`DELETE FROM runtime_secrets WHERE pool_name = ? AND id = ?;`,
		poolName, id,
	)
	if err != nil {
		s.observeSQLiteError(err)
		s.observeSQLiteTx(sqliteTxClassWrite, startedAt, false)
		return false, fmt.Errorf("sqlite: runtime_secrets delete: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		s.observeSQLiteTx(sqliteTxClassWrite, startedAt, false)
		return false, fmt.Errorf("sqlite: runtime_secrets delete rows: %w", err)
	}
	s.observeSQLiteTx(sqliteTxClassWrite, startedAt, true)
	return n > 0, nil
}

func (s *Store) ListByPool(poolName string) ([]secrets.Record, error) {
	rows, err := s.db.QueryContext(context.Background(),
		`SELECT pool_name, id, sealed, not_before, not_after, created_at
		 FROM runtime_secrets
		 WHERE pool_name = ?
		 ORDER BY not_before DESC, id ASC;`,
		poolName,
	)
	if err != nil {
		s.observeSQLiteError(err)
		return nil, fmt.Errorf("sqlite: runtime_secrets list: %w", err)
	}
	defer rows.Close()
	return scanSecretRows(rows)
}

func (s *Store) ListAll() ([]secrets.Record, error) {
	rows, err := s.db.QueryContext(context.Background(),
		`SELECT pool_name, id, sealed, not_before, not_after, created_at
		 FROM runtime_secrets
		 ORDER BY pool_name ASC, not_before DESC, id ASC;`,
	)
	if err != nil {
		s.observeSQLiteError(err)
		return nil, fmt.Errorf("sqlite: runtime_secrets list all: %w", err)
	}
	defer rows.Close()
	return scanSecretRows(rows)
}

func scanSecretRows(rows *sql.Rows) ([]secrets.Record, error) {
	out := make([]secrets.Record, 0, 4)
	for rows.Next() {
		var (
			rec       secrets.Record
			notBefore int64
			notAfter  int64
			createdAt int64
		)
		if err := rows.Scan(&rec.PoolName, &rec.ID, &rec.Sealed, &notBefore, &notAfter, &createdAt); err != nil {
			return nil, fmt.Errorf("sqlite: scan runtime_secrets: %w", err)
		}
		rec.NotBefore = time.Unix(0, notBefore).UTC()
		rec.NotAfter = unixNanoOrZeroToTime(notAfter)
		rec.CreatedAt = time.Unix(0, createdAt).UTC()
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: runtime_secrets rows: %w", err)
	}
	return out, nil
}

func timeToUnixNanoOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func unixNanoOrZeroToTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}
