package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/nuetzliches/hookaido/internal/secrets"
)

// Compile-time check: *Store satisfies secrets.Store.
var _ secrets.Store = (*Store)(nil)

func (s *Store) Upsert(rec secrets.Record) error {
	return s.runStoreOperation("secrets_upsert", func() error {
		if s == nil || s.db == nil {
			return errors.New("postgres store is closed")
		}
		if rec.PoolName == "" {
			return errors.New("runtime_secrets upsert: empty pool_name")
		}
		if rec.ID == "" {
			return errors.New("runtime_secrets upsert: empty id")
		}
		if len(rec.Sealed) == 0 {
			return errors.New("runtime_secrets upsert: empty sealed payload")
		}
		if rec.NotBefore.IsZero() {
			return errors.New("runtime_secrets upsert: not_before is zero")
		}
		if rec.CreatedAt.IsZero() {
			rec.CreatedAt = s.now()
		}

		var notAfter sql.NullTime
		if !rec.NotAfter.IsZero() {
			notAfter = sql.NullTime{Time: rec.NotAfter.UTC(), Valid: true}
		}

		_, err := s.db.ExecContext(context.Background(), `
INSERT INTO runtime_secrets (pool_name, id, sealed, not_before, not_after, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (pool_name, id) DO UPDATE SET
  sealed     = EXCLUDED.sealed,
  not_before = EXCLUDED.not_before,
  not_after  = EXCLUDED.not_after,
  created_at = EXCLUDED.created_at
`,
			rec.PoolName,
			rec.ID,
			rec.Sealed,
			rec.NotBefore.UTC(),
			notAfter,
			rec.CreatedAt.UTC(),
		)
		if err != nil {
			return fmt.Errorf("postgres: runtime_secrets upsert: %w", err)
		}
		return nil
	})
}

func (s *Store) Delete(poolName, id string) (bool, error) {
	var removed bool
	err := s.runStoreOperation("secrets_delete", func() error {
		if s == nil || s.db == nil {
			return errors.New("postgres store is closed")
		}
		res, err := s.db.ExecContext(context.Background(),
			`DELETE FROM runtime_secrets WHERE pool_name = $1 AND id = $2`,
			poolName, id,
		)
		if err != nil {
			return fmt.Errorf("postgres: runtime_secrets delete: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("postgres: runtime_secrets delete rows: %w", err)
		}
		removed = n > 0
		return nil
	})
	return removed, err
}

func (s *Store) ListByPool(poolName string) ([]secrets.Record, error) {
	var out []secrets.Record
	err := s.runStoreOperation("secrets_list_by_pool", func() error {
		if s == nil || s.db == nil {
			return errors.New("postgres store is closed")
		}
		rows, err := s.db.QueryContext(context.Background(),
			`SELECT pool_name, id, sealed, not_before, not_after, created_at
			 FROM runtime_secrets
			 WHERE pool_name = $1
			 ORDER BY not_before DESC, id ASC`,
			poolName,
		)
		if err != nil {
			return fmt.Errorf("postgres: runtime_secrets list: %w", err)
		}
		defer rows.Close()
		out, err = scanSecretRows(rows)
		return err
	})
	return out, err
}

func (s *Store) ListAll() ([]secrets.Record, error) {
	var out []secrets.Record
	err := s.runStoreOperation("secrets_list_all", func() error {
		if s == nil || s.db == nil {
			return errors.New("postgres store is closed")
		}
		rows, err := s.db.QueryContext(context.Background(),
			`SELECT pool_name, id, sealed, not_before, not_after, created_at
			 FROM runtime_secrets
			 ORDER BY pool_name ASC, not_before DESC, id ASC`,
		)
		if err != nil {
			return fmt.Errorf("postgres: runtime_secrets list all: %w", err)
		}
		defer rows.Close()
		out, err = scanSecretRows(rows)
		return err
	})
	return out, err
}

func scanSecretRows(rows *sql.Rows) ([]secrets.Record, error) {
	out := make([]secrets.Record, 0, 4)
	for rows.Next() {
		var (
			rec       secrets.Record
			notAfter  sql.NullTime
			notBefore time.Time
			createdAt time.Time
		)
		if err := rows.Scan(&rec.PoolName, &rec.ID, &rec.Sealed, &notBefore, &notAfter, &createdAt); err != nil {
			return nil, fmt.Errorf("postgres: scan runtime_secrets: %w", err)
		}
		rec.NotBefore = notBefore.UTC()
		if notAfter.Valid {
			rec.NotAfter = notAfter.Time.UTC()
		}
		rec.CreatedAt = createdAt.UTC()
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: runtime_secrets rows: %w", err)
	}
	return out, nil
}
