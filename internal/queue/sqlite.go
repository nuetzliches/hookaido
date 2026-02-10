package queue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	sqlite3 "modernc.org/sqlite"
)

const schemaVersion = 5
const backlogTrendRetention = 30 * 24 * time.Hour

const schemaV1 = `
CREATE TABLE IF NOT EXISTS queue_items (
  id            TEXT PRIMARY KEY,
  route         TEXT NOT NULL,
  target        TEXT NOT NULL,
  state         TEXT NOT NULL,
  received_at   INTEGER NOT NULL,
  attempt       INTEGER NOT NULL,
  next_run_at   INTEGER NOT NULL,
  payload       BLOB NOT NULL,
  headers_json  TEXT,
  trace_json    TEXT,
  schema_version INTEGER NOT NULL,
  lease_id      TEXT,
  lease_until   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_queue_ready
  ON queue_items(state, route, target, next_run_at, received_at);
CREATE INDEX IF NOT EXISTS idx_queue_lease_id
  ON queue_items(lease_id);
CREATE INDEX IF NOT EXISTS idx_queue_leases
  ON queue_items(state, lease_until);
`

const schemaV2 = `
ALTER TABLE queue_items ADD COLUMN dead_reason TEXT;
`

const schemaV3 = `
CREATE TABLE IF NOT EXISTS delivery_attempts (
  id          TEXT PRIMARY KEY,
  event_id    TEXT NOT NULL,
  route       TEXT NOT NULL,
  target      TEXT NOT NULL,
  attempt     INTEGER NOT NULL,
  status_code INTEGER,
  error       TEXT,
  outcome     TEXT NOT NULL,
  dead_reason TEXT,
  created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_created
  ON delivery_attempts(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_event
  ON delivery_attempts(event_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_route_target
  ON delivery_attempts(route, target, created_at DESC, id DESC);
`

const schemaV4 = `
CREATE INDEX IF NOT EXISTS idx_queue_state_received_at
  ON queue_items(state, received_at);
CREATE INDEX IF NOT EXISTS idx_queue_state_next_run_at
  ON queue_items(state, next_run_at);
`

const schemaV5 = `
CREATE TABLE IF NOT EXISTS backlog_trend_samples (
  captured_at INTEGER NOT NULL,
  route       TEXT NOT NULL,
  target      TEXT NOT NULL,
  queued      INTEGER NOT NULL,
  leased      INTEGER NOT NULL,
  dead        INTEGER NOT NULL,
  PRIMARY KEY (captured_at, route, target)
);
CREATE INDEX IF NOT EXISTS idx_backlog_trend_time
  ON backlog_trend_samples(captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_backlog_trend_route_target_time
  ON backlog_trend_samples(route, target, captured_at DESC);
`

type SQLiteOption func(*SQLiteStore)

func WithSQLiteNowFunc(now func() time.Time) SQLiteOption {
	return func(s *SQLiteStore) {
		if now != nil {
			s.nowFn = now
		}
	}
}

func WithSQLitePollInterval(d time.Duration) SQLiteOption {
	return func(s *SQLiteStore) {
		if d > 0 {
			s.pollInterval = d
		}
	}
}

func WithSQLiteRetention(maxAge, pruneInterval time.Duration) SQLiteOption {
	return func(s *SQLiteStore) {
		if maxAge > 0 {
			s.retentionMaxAge = maxAge
		} else {
			s.retentionMaxAge = 0
		}
		if pruneInterval > 0 {
			s.pruneInterval = pruneInterval
		} else {
			s.pruneInterval = 0
		}
	}
}

func WithSQLiteDeliveredRetention(maxAge time.Duration) SQLiteOption {
	return func(s *SQLiteStore) {
		if maxAge > 0 {
			s.deliveredRetentionMaxAge = maxAge
		} else {
			s.deliveredRetentionMaxAge = 0
		}
	}
}

func WithSQLiteDLQRetention(maxAge time.Duration, maxDepth int) SQLiteOption {
	return func(s *SQLiteStore) {
		if maxAge > 0 {
			s.dlqRetentionMaxAge = maxAge
		} else {
			s.dlqRetentionMaxAge = 0
		}
		if maxDepth > 0 {
			s.dlqMaxDepth = maxDepth
		} else {
			s.dlqMaxDepth = 0
		}
	}
}

type SQLiteStore struct {
	db *sql.DB

	mu                       sync.Mutex
	nowFn                    func() time.Time
	notify                   chan struct{}
	pollInterval             time.Duration
	maxDepth                 int
	dropPolicy               string
	retentionMaxAge          time.Duration
	pruneInterval            time.Duration
	lastPrune                time.Time
	pruneMu                  sync.Mutex
	deliveredRetentionMaxAge time.Duration
	dlqRetentionMaxAge       time.Duration
	dlqMaxDepth              int
}

func NewSQLiteStore(dbPath string, opts ...SQLiteOption) (*SQLiteStore, error) {
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		return nil, errors.New("empty db path")
	}

	dir := filepath.Dir(dbPath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	s := &SQLiteStore{
		db:           db,
		nowFn:        time.Now,
		notify:       make(chan struct{}),
		pollInterval: 25 * time.Millisecond,
		dropPolicy:   "reject",
	}
	for _, opt := range opts {
		opt(s)
	}

	if err := s.init(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return s, nil
}

func WithSQLiteQueueLimits(maxDepth int, dropPolicy string) SQLiteOption {
	return func(s *SQLiteStore) {
		if maxDepth >= 0 {
			s.maxDepth = maxDepth
		}
		if strings.TrimSpace(dropPolicy) != "" {
			s.dropPolicy = strings.ToLower(strings.TrimSpace(dropPolicy))
		}
	}
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) init() error {
	ctx := context.Background()

	var journalMode string
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL;").Scan(&journalMode); err != nil {
		return fmt.Errorf("sqlite: set journal_mode=wal: %w", err)
	}
	if strings.ToLower(journalMode) != "wal" {
		return fmt.Errorf("sqlite: journal_mode=%q, want wal", journalMode)
	}

	if _, err := s.db.ExecContext(ctx, "PRAGMA synchronous=FULL;"); err != nil {
		return fmt.Errorf("sqlite: set synchronous=full: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA busy_timeout=5000;"); err != nil {
		return fmt.Errorf("sqlite: set busy_timeout: %w", err)
	}

	if err := s.migrate(ctx); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStore) migrate(ctx context.Context) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE;"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_, _ = conn.ExecContext(ctx, "ROLLBACK;")
	}()

	if _, err := conn.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER NOT NULL
);
`); err != nil {
		return fmt.Errorf("sqlite: init migrations table: %w", err)
	}

	current, hasVersion, err := readSchemaVersion(ctx, conn)
	if err != nil {
		return err
	}
	if !hasVersion {
		current = 0
	}

	if current > schemaVersion {
		return fmt.Errorf("sqlite: schema_version=%d, want <=%d", current, schemaVersion)
	}

	for v := current + 1; v <= schemaVersion; v++ {
		switch v {
		case 1:
			if _, err := conn.ExecContext(ctx, schemaV1); err != nil {
				return fmt.Errorf("sqlite: migrate v1: %w", err)
			}
		case 2:
			if _, err := conn.ExecContext(ctx, schemaV2); err != nil {
				return fmt.Errorf("sqlite: migrate v2: %w", err)
			}
		case 3:
			if _, err := conn.ExecContext(ctx, schemaV3); err != nil {
				return fmt.Errorf("sqlite: migrate v3: %w", err)
			}
		case 4:
			if _, err := conn.ExecContext(ctx, schemaV4); err != nil {
				return fmt.Errorf("sqlite: migrate v4: %w", err)
			}
		case 5:
			if _, err := conn.ExecContext(ctx, schemaV5); err != nil {
				return fmt.Errorf("sqlite: migrate v5: %w", err)
			}
		default:
			return fmt.Errorf("sqlite: unknown migration %d", v)
		}
	}

	if !hasVersion || current != schemaVersion {
		if err := writeSchemaVersion(ctx, conn, schemaVersion); err != nil {
			return err
		}
	}

	if _, err := conn.ExecContext(ctx, "COMMIT;"); err != nil {
		return err
	}
	committed = true
	return nil
}

func readSchemaVersion(ctx context.Context, conn *sql.Conn) (int, bool, error) {
	var v int
	err := conn.QueryRowContext(ctx, `SELECT version FROM schema_migrations LIMIT 1;`).Scan(&v)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("sqlite: read schema_version: %w", err)
	}
	return v, true, nil
}

func writeSchemaVersion(ctx context.Context, conn *sql.Conn, v int) error {
	if _, err := conn.ExecContext(ctx, `INSERT OR REPLACE INTO schema_migrations(rowid, version) VALUES (1, ?);`, v); err != nil {
		return fmt.Errorf("sqlite: write schema_version: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Enqueue(env Envelope) error {
	now := s.now()
	if err := s.maybePrune(now); err != nil {
		return err
	}

	if env.ID == "" {
		env.ID = newHexID("evt_")
	}
	if env.State == "" {
		env.State = StateQueued
	}
	if env.State != StateDead {
		env.DeadReason = ""
	}
	if env.ReceivedAt.IsZero() {
		env.ReceivedAt = now
	}
	if env.NextRunAt.IsZero() {
		env.NextRunAt = env.ReceivedAt
	}
	if env.SchemaVersion == 0 {
		env.SchemaVersion = 1
	}
	if env.Payload == nil {
		env.Payload = []byte{}
	}

	headersJSON, err := marshalStringMap(env.Headers)
	if err != nil {
		return err
	}
	traceJSON, err := marshalStringMap(env.Trace)
	if err != nil {
		return err
	}
	var deadReason any
	if strings.TrimSpace(env.DeadReason) != "" {
		deadReason = env.DeadReason
	}

	if s.maxDepth > 0 {
		return s.enqueueWithLimit(env, headersJSON, traceJSON)
	}

	_, err = s.db.ExecContext(context.Background(), `
INSERT INTO queue_items (
  id, route, target, state, received_at, attempt, next_run_at,
  payload, headers_json, trace_json, schema_version, dead_reason,
  lease_id, lease_until
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL);
`,
		env.ID,
		env.Route,
		env.Target,
		string(env.State),
		env.ReceivedAt.UnixNano(),
		env.Attempt,
		env.NextRunAt.UnixNano(),
		env.Payload,
		headersJSON,
		traceJSON,
		env.SchemaVersion,
		deadReason,
	)
	if err != nil {
		return mapQueueInsertError(err)
	}

	s.signal()
	return nil
}

func (s *SQLiteStore) enqueueWithLimit(env Envelope, headersJSON any, traceJSON any) error {
	ctx := context.Background()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE;"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_, _ = conn.ExecContext(ctx, "ROLLBACK;")
	}()

	var count int
	err = conn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM queue_items
WHERE state IN (?, ?);
`, string(StateQueued), string(StateLeased)).Scan(&count)
	if err != nil {
		return err
	}

	if count >= s.maxDepth {
		if s.dropPolicy == "drop_oldest" {
			dropped, err := s.dropOldestQueued(ctx, conn)
			if err != nil {
				return err
			}
			if !dropped {
				return ErrQueueFull
			}
			count--
		} else {
			return ErrQueueFull
		}
	}

	var deadReason any
	if strings.TrimSpace(env.DeadReason) != "" {
		deadReason = env.DeadReason
	}

	_, err = conn.ExecContext(ctx, `
INSERT INTO queue_items (
  id, route, target, state, received_at, attempt, next_run_at,
  payload, headers_json, trace_json, schema_version, dead_reason,
  lease_id, lease_until
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL);
`,
		env.ID,
		env.Route,
		env.Target,
		string(env.State),
		env.ReceivedAt.UnixNano(),
		env.Attempt,
		env.NextRunAt.UnixNano(),
		env.Payload,
		headersJSON,
		traceJSON,
		env.SchemaVersion,
		deadReason,
	)
	if err != nil {
		return mapQueueInsertError(err)
	}

	if _, err := conn.ExecContext(ctx, "COMMIT;"); err != nil {
		return err
	}
	committed = true
	s.signal()
	return nil
}

// EnqueueBatch atomically enqueues all items or none (all-or-nothing) in a
// single transaction. Returns the number of items enqueued and an error.
func (s *SQLiteStore) EnqueueBatch(items []Envelope) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}

	now := s.now()
	if err := s.maybePrune(now); err != nil {
		return 0, err
	}

	type preparedEnv struct {
		env         Envelope
		headersJSON any
		traceJSON   any
		deadReason  any
	}

	prepared := make([]preparedEnv, 0, len(items))
	for i := range items {
		env := items[i]
		if env.ID == "" {
			env.ID = newHexID("evt_")
		}
		if env.State == "" {
			env.State = StateQueued
		}
		if env.State != StateDead {
			env.DeadReason = ""
		}
		if env.ReceivedAt.IsZero() {
			env.ReceivedAt = now
		}
		if env.NextRunAt.IsZero() {
			env.NextRunAt = env.ReceivedAt
		}
		if env.SchemaVersion == 0 {
			env.SchemaVersion = 1
		}
		if env.Payload == nil {
			env.Payload = []byte{}
		}
		hJSON, err := marshalStringMap(env.Headers)
		if err != nil {
			return 0, err
		}
		tJSON, err := marshalStringMap(env.Trace)
		if err != nil {
			return 0, err
		}
		var dr any
		if strings.TrimSpace(env.DeadReason) != "" {
			dr = env.DeadReason
		}
		prepared = append(prepared, preparedEnv{env: env, headersJSON: hJSON, traceJSON: tJSON, deadReason: dr})
	}

	ctx := context.Background()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE;"); err != nil {
		return 0, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(ctx, "ROLLBACK;")
		}
	}()

	// Check depth limit for all items at once.
	if s.maxDepth > 0 {
		var count int
		if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM queue_items WHERE state IN (?, ?);
`, string(StateQueued), string(StateLeased)).Scan(&count); err != nil {
			return 0, err
		}
		needed := count + len(prepared)
		if s.dropPolicy == "drop_oldest" {
			for needed > s.maxDepth {
				dropped, dErr := s.dropOldestQueued(ctx, conn)
				if dErr != nil {
					return 0, dErr
				}
				if !dropped {
					return 0, ErrQueueFull
				}
				needed--
			}
		} else if needed > s.maxDepth {
			return 0, ErrQueueFull
		}
	}

	// Insert all items.
	for _, p := range prepared {
		_, err := conn.ExecContext(ctx, `
INSERT INTO queue_items (
  id, route, target, state, received_at, attempt, next_run_at,
  payload, headers_json, trace_json, schema_version, dead_reason,
  lease_id, lease_until
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL);
`,
			p.env.ID,
			p.env.Route,
			p.env.Target,
			string(p.env.State),
			p.env.ReceivedAt.UnixNano(),
			p.env.Attempt,
			p.env.NextRunAt.UnixNano(),
			p.env.Payload,
			p.headersJSON,
			p.traceJSON,
			p.env.SchemaVersion,
			p.deadReason,
		)
		if err != nil {
			return 0, mapQueueInsertError(err)
		}
	}

	if _, err := conn.ExecContext(ctx, "COMMIT;"); err != nil {
		return 0, err
	}
	committed = true
	s.signal()
	return len(prepared), nil
}

func (s *SQLiteStore) dropOldestQueued(ctx context.Context, conn *sql.Conn) (bool, error) {
	res, err := conn.ExecContext(ctx, `
DELETE FROM queue_items
WHERE id = (
  SELECT id FROM queue_items
  WHERE state = ?
  ORDER BY received_at ASC
  LIMIT 1
);
`, string(StateQueued))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *SQLiteStore) maybePrune(now time.Time) error {
	if s.pruneInterval <= 0 {
		return nil
	}
	if s.retentionMaxAge <= 0 && s.deliveredRetentionMaxAge <= 0 && s.dlqRetentionMaxAge <= 0 && s.dlqMaxDepth <= 0 {
		return nil
	}

	s.pruneMu.Lock()
	defer s.pruneMu.Unlock()

	if !s.lastPrune.IsZero() && now.Sub(s.lastPrune) < s.pruneInterval {
		return nil
	}

	if s.retentionMaxAge > 0 {
		cutoff := now.Add(-s.retentionMaxAge)
		_, err := s.db.ExecContext(context.Background(), `
DELETE FROM queue_items
WHERE state = ?
  AND received_at <= ?;
`, string(StateQueued), cutoff.UnixNano())
		if err != nil {
			return err
		}
	}

	if s.deliveredRetentionMaxAge > 0 {
		cutoff := now.Add(-s.deliveredRetentionMaxAge)
		_, err := s.db.ExecContext(context.Background(), `
DELETE FROM queue_items
WHERE state = ?
  AND next_run_at <= ?;
`, string(StateDelivered), cutoff.UnixNano())
		if err != nil {
			return err
		}
	}

	if s.dlqRetentionMaxAge > 0 {
		cutoff := now.Add(-s.dlqRetentionMaxAge)
		_, err := s.db.ExecContext(context.Background(), `
DELETE FROM queue_items
WHERE state = ?
  AND received_at <= ?;
`, string(StateDead), cutoff.UnixNano())
		if err != nil {
			return err
		}
	}

	if s.dlqMaxDepth > 0 {
		_, err := s.db.ExecContext(context.Background(), `
DELETE FROM queue_items
WHERE id IN (
  SELECT id FROM queue_items
  WHERE state = ?
  ORDER BY received_at DESC
  LIMIT -1 OFFSET ?
);
`, string(StateDead), s.dlqMaxDepth)
		if err != nil {
			return err
		}
	}
	s.lastPrune = now
	return nil
}

func (s *SQLiteStore) Dequeue(req DequeueRequest) (DequeueResponse, error) {
	pruneNow := req.Now
	if pruneNow.IsZero() {
		pruneNow = s.now()
	}
	if err := s.maybePrune(pruneNow); err != nil {
		return DequeueResponse{}, err
	}

	batch := req.Batch
	if batch <= 0 {
		batch = 1
	}
	if batch > 100 {
		batch = 100
	}

	leaseTTL := req.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}

	maxWait := req.MaxWait
	if maxWait < 0 {
		maxWait = 0
	}

	deadline := time.Now().Add(maxWait)

	for {
		resp, err := s.dequeueOnce(req, batch, leaseTTL)
		if err != nil {
			return DequeueResponse{}, err
		}
		if len(resp.Items) > 0 || maxWait == 0 {
			return resp, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return DequeueResponse{}, nil
		}

		waitCh := s.waitCh()
		sleep := remaining
		if s.pollInterval > 0 && sleep > s.pollInterval {
			sleep = s.pollInterval
		}
		timer := time.NewTimer(sleep)
		select {
		case <-waitCh:
			if !timer.Stop() {
				<-timer.C
			}
			continue
		case <-timer.C:
			continue
		}
	}
}

func (s *SQLiteStore) dequeueOnce(req DequeueRequest, batch int, leaseTTL time.Duration) (DequeueResponse, error) {
	now := req.Now
	if now.IsZero() {
		now = s.now()
	}
	leaseUntil := now.Add(leaseTTL)

	ctx := context.Background()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return DequeueResponse{}, err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE;"); err != nil {
		return DequeueResponse{}, err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_, _ = conn.ExecContext(ctx, "ROLLBACK;")
	}()

	if err := s.requeueExpiredLeases(ctx, conn, now); err != nil {
		return DequeueResponse{}, err
	}

	query := `
SELECT id, route, target, state, received_at, attempt, next_run_at, payload, headers_json, trace_json, schema_version, dead_reason
FROM queue_items
WHERE state = ?
  AND next_run_at <= ?`
	args := []any{string(StateQueued), now.UnixNano()}
	if req.Route != "" {
		query += " AND route = ?"
		args = append(args, req.Route)
	}
	if req.Target != "" {
		query += " AND target = ?"
		args = append(args, req.Target)
	}
	query += " ORDER BY next_run_at ASC, received_at ASC LIMIT ?"
	args = append(args, batch)

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return DequeueResponse{}, err
	}
	defer rows.Close()

	out := make([]Envelope, 0, batch)
	for rows.Next() {
		var env Envelope
		var receivedAtNanos int64
		var nextRunAtNanos int64
		var state string
		var headersJSON sql.NullString
		var traceJSON sql.NullString
		var deadReason sql.NullString

		if err := rows.Scan(
			&env.ID,
			&env.Route,
			&env.Target,
			&state,
			&receivedAtNanos,
			&env.Attempt,
			&nextRunAtNanos,
			&env.Payload,
			&headersJSON,
			&traceJSON,
			&env.SchemaVersion,
			&deadReason,
		); err != nil {
			return DequeueResponse{}, err
		}

		env.State = State(state)
		env.ReceivedAt = time.Unix(0, receivedAtNanos).UTC()
		env.NextRunAt = time.Unix(0, nextRunAtNanos).UTC()
		env.Headers = unmarshalStringMap(headersJSON)
		env.Trace = unmarshalStringMap(traceJSON)
		if deadReason.Valid {
			env.DeadReason = deadReason.String
		}

		leaseID := newHexID("lease_")
		if _, err := conn.ExecContext(ctx, `
UPDATE queue_items
SET state = ?, attempt = attempt + 1, lease_id = ?, lease_until = ?, next_run_at = ?
WHERE id = ? AND state = ?;
`,
			string(StateLeased),
			leaseID,
			leaseUntil.UnixNano(),
			leaseUntil.UnixNano(),
			env.ID,
			string(StateQueued),
		); err != nil {
			return DequeueResponse{}, err
		}

		env.State = StateLeased
		env.Attempt++
		env.LeaseID = leaseID
		env.LeaseUntil = leaseUntil.UTC()
		env.NextRunAt = env.LeaseUntil

		out = append(out, env)
	}
	if err := rows.Err(); err != nil {
		return DequeueResponse{}, err
	}

	if _, err := conn.ExecContext(ctx, "COMMIT;"); err != nil {
		return DequeueResponse{}, err
	}
	committed = true

	return DequeueResponse{Items: out}, nil
}

func (s *SQLiteStore) Ack(leaseID string) error {
	return s.withLease(leaseID, func(ctx context.Context, conn *sql.Conn, item leaseItem) error {
		if item.expired {
			if err := s.requeueLease(ctx, conn, item.id, item.route, item.target, item.attempt); err != nil {
				return err
			}
			return ErrLeaseExpired
		}

		if s.deliveredRetentionMaxAge > 0 {
			now := s.now()
			_, err := conn.ExecContext(ctx, `
UPDATE queue_items
SET state = ?, lease_id = NULL, lease_until = NULL, next_run_at = ?, dead_reason = NULL
WHERE id = ?;
`,
				string(StateDelivered),
				now.UnixNano(),
				item.id,
			)
			if err != nil {
				return err
			}
			return nil
		}

		_, err := conn.ExecContext(ctx, `DELETE FROM queue_items WHERE id = ?;`, item.id)
		return err
	})
}

func (s *SQLiteStore) Nack(leaseID string, delay time.Duration) error {
	if delay < 0 {
		delay = 0
	}

	return s.withLease(leaseID, func(ctx context.Context, conn *sql.Conn, item leaseItem) error {
		if item.expired {
			if err := s.requeueLease(ctx, conn, item.id, item.route, item.target, item.attempt); err != nil {
				return err
			}
			return ErrLeaseExpired
		}

		nextRunAt := s.now().Add(delay)
		_, err := conn.ExecContext(ctx, `
UPDATE queue_items
SET state = ?, lease_id = NULL, lease_until = NULL, next_run_at = ?, dead_reason = NULL
WHERE id = ?;
`,
			string(StateQueued),
			nextRunAt.UnixNano(),
			item.id,
		)
		if err != nil {
			return err
		}
		s.signal()
		return nil
	})
}

func (s *SQLiteStore) Extend(leaseID string, extendBy time.Duration) error {
	if extendBy <= 0 {
		return nil
	}

	return s.withLease(leaseID, func(ctx context.Context, conn *sql.Conn, item leaseItem) error {
		if item.expired {
			if err := s.requeueLease(ctx, conn, item.id, item.route, item.target, item.attempt); err != nil {
				return err
			}
			return ErrLeaseExpired
		}

		newUntil := item.leaseUntil.Add(extendBy)
		_, err := conn.ExecContext(ctx, `
UPDATE queue_items
SET lease_until = ?, next_run_at = ?
WHERE id = ?;
`,
			newUntil.UnixNano(),
			newUntil.UnixNano(),
			item.id,
		)
		return err
	})
}

func (s *SQLiteStore) MarkDead(leaseID string, reason string) error {
	return s.withLease(leaseID, func(ctx context.Context, conn *sql.Conn, item leaseItem) error {
		if item.expired {
			if err := s.requeueLease(ctx, conn, item.id, item.route, item.target, item.attempt); err != nil {
				return err
			}
			return ErrLeaseExpired
		}

		now := s.now()
		var deadReason any
		if strings.TrimSpace(reason) != "" {
			deadReason = reason
		}

		_, err := conn.ExecContext(ctx, `
UPDATE queue_items
SET state = ?, lease_id = NULL, lease_until = NULL, next_run_at = ?, dead_reason = ?
WHERE id = ?;
`,
			string(StateDead),
			now.UnixNano(),
			deadReason,
			item.id,
		)
		if err != nil {
			return err
		}
		s.signal()
		return nil
	})
}

func (s *SQLiteStore) ListDead(req DeadListRequest) (DeadListResponse, error) {
	pruneNow := s.now()
	if err := s.maybePrune(pruneNow); err != nil {
		return DeadListResponse{}, err
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	includePayload := 0
	if req.IncludePayload {
		includePayload = 1
	}
	includeHeaders := 0
	if req.IncludeHeaders {
		includeHeaders = 1
	}
	includeTrace := 0
	if req.IncludeTrace {
		includeTrace = 1
	}

	query := `
SELECT id, route, target, state, received_at, attempt, next_run_at,
  CASE WHEN ? THEN payload ELSE NULL END,
  CASE WHEN ? THEN headers_json ELSE NULL END,
  CASE WHEN ? THEN trace_json ELSE NULL END,
  schema_version, dead_reason
FROM queue_items
WHERE state = ?`
	args := []any{includePayload, includeHeaders, includeTrace, string(StateDead)}
	if req.Route != "" {
		query += " AND route = ?"
		args = append(args, req.Route)
	}
	if !req.Before.IsZero() {
		query += " AND received_at < ?"
		args = append(args, req.Before.UnixNano())
	}
	query += " ORDER BY received_at DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return DeadListResponse{}, err
	}
	defer rows.Close()

	items := make([]Envelope, 0, limit)
	for rows.Next() {
		var env Envelope
		var receivedAtNanos int64
		var nextRunAtNanos int64
		var state string
		var headersJSON sql.NullString
		var traceJSON sql.NullString
		var deadReason sql.NullString

		if err := rows.Scan(
			&env.ID,
			&env.Route,
			&env.Target,
			&state,
			&receivedAtNanos,
			&env.Attempt,
			&nextRunAtNanos,
			&env.Payload,
			&headersJSON,
			&traceJSON,
			&env.SchemaVersion,
			&deadReason,
		); err != nil {
			return DeadListResponse{}, err
		}

		env.State = State(state)
		env.ReceivedAt = time.Unix(0, receivedAtNanos).UTC()
		env.NextRunAt = time.Unix(0, nextRunAtNanos).UTC()
		env.Headers = unmarshalStringMap(headersJSON)
		env.Trace = unmarshalStringMap(traceJSON)
		if deadReason.Valid {
			env.DeadReason = deadReason.String
		}

		items = append(items, env)
	}
	if err := rows.Err(); err != nil {
		return DeadListResponse{}, err
	}

	return DeadListResponse{Items: items}, nil
}

func (s *SQLiteStore) RequeueDead(req DeadRequeueRequest) (DeadRequeueResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return DeadRequeueResponse{}, nil
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, 3+len(ids))
	args = append(args, string(StateQueued), s.now().UnixNano(), string(StateDead))
	for _, id := range ids {
		args = append(args, id)
	}

	query := `
UPDATE queue_items
SET state = ?, lease_id = NULL, lease_until = NULL, next_run_at = ?, dead_reason = NULL
WHERE state = ?
  AND id IN (` + placeholders + `);`

	res, err := s.db.ExecContext(context.Background(), query, args...)
	if err != nil {
		return DeadRequeueResponse{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return DeadRequeueResponse{}, err
	}
	if n > 0 {
		s.signal()
	}
	return DeadRequeueResponse{Requeued: int(n)}, nil
}

func (s *SQLiteStore) DeleteDead(req DeadDeleteRequest) (DeadDeleteResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return DeadDeleteResponse{}, nil
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, 1+len(ids))
	args = append(args, string(StateDead))
	for _, id := range ids {
		args = append(args, id)
	}

	query := `
DELETE FROM queue_items
WHERE state = ?
  AND id IN (` + placeholders + `);`

	res, err := s.db.ExecContext(context.Background(), query, args...)
	if err != nil {
		return DeadDeleteResponse{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return DeadDeleteResponse{}, err
	}
	return DeadDeleteResponse{Deleted: int(n)}, nil
}

func (s *SQLiteStore) ListMessages(req MessageListRequest) (MessageListResponse, error) {
	pruneNow := s.now()
	if err := s.maybePrune(pruneNow); err != nil {
		return MessageListResponse{}, err
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	order := strings.ToLower(strings.TrimSpace(req.Order))
	orderByReceived := "DESC"
	orderByID := "DESC"
	switch order {
	case "", MessageOrderDesc:
	case MessageOrderAsc:
		orderByReceived = "ASC"
		orderByID = "ASC"
	default:
		return MessageListResponse{}, fmt.Errorf("invalid message order %q", req.Order)
	}

	includePayload := 0
	if req.IncludePayload {
		includePayload = 1
	}
	includeHeaders := 0
	if req.IncludeHeaders {
		includeHeaders = 1
	}
	includeTrace := 0
	if req.IncludeTrace {
		includeTrace = 1
	}

	query := `
SELECT id, route, target, state, received_at, attempt, next_run_at,
  CASE WHEN ? THEN payload ELSE NULL END,
  CASE WHEN ? THEN headers_json ELSE NULL END,
  CASE WHEN ? THEN trace_json ELSE NULL END,
  schema_version, dead_reason
FROM queue_items
WHERE 1 = 1`
	args := []any{includePayload, includeHeaders, includeTrace}
	if req.Route != "" {
		query += " AND route = ?"
		args = append(args, req.Route)
	}
	if req.Target != "" {
		query += " AND target = ?"
		args = append(args, req.Target)
	}
	if req.State != "" {
		query += " AND state = ?"
		args = append(args, string(req.State))
	}
	if !req.Before.IsZero() {
		query += " AND received_at < ?"
		args = append(args, req.Before.UnixNano())
	}
	query += " ORDER BY received_at " + orderByReceived + ", id " + orderByID + " LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return MessageListResponse{}, err
	}
	defer rows.Close()

	items := make([]Envelope, 0, limit)
	for rows.Next() {
		var env Envelope
		var receivedAtNanos int64
		var nextRunAtNanos int64
		var state string
		var headersJSON sql.NullString
		var traceJSON sql.NullString
		var deadReason sql.NullString

		if err := rows.Scan(
			&env.ID,
			&env.Route,
			&env.Target,
			&state,
			&receivedAtNanos,
			&env.Attempt,
			&nextRunAtNanos,
			&env.Payload,
			&headersJSON,
			&traceJSON,
			&env.SchemaVersion,
			&deadReason,
		); err != nil {
			return MessageListResponse{}, err
		}

		env.State = State(state)
		env.ReceivedAt = time.Unix(0, receivedAtNanos).UTC()
		env.NextRunAt = time.Unix(0, nextRunAtNanos).UTC()
		env.Headers = unmarshalStringMap(headersJSON)
		env.Trace = unmarshalStringMap(traceJSON)
		if deadReason.Valid {
			env.DeadReason = deadReason.String
		}
		items = append(items, env)
	}
	if err := rows.Err(); err != nil {
		return MessageListResponse{}, err
	}

	return MessageListResponse{Items: items}, nil
}

func (s *SQLiteStore) LookupMessages(req MessageLookupRequest) (MessageLookupResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return MessageLookupResponse{}, nil
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}

	query := `
SELECT id, route, state
FROM queue_items
WHERE id IN (` + placeholders + `);`

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return MessageLookupResponse{}, err
	}
	defer rows.Close()

	byID := make(map[string]MessageLookupItem, len(ids))
	for rows.Next() {
		var item MessageLookupItem
		var state string
		if err := rows.Scan(&item.ID, &item.Route, &state); err != nil {
			return MessageLookupResponse{}, err
		}
		item.State = State(state)
		byID[item.ID] = item
	}
	if err := rows.Err(); err != nil {
		return MessageLookupResponse{}, err
	}

	items := make([]MessageLookupItem, 0, len(byID))
	for _, id := range ids {
		item, ok := byID[id]
		if !ok {
			continue
		}
		items = append(items, item)
	}
	return MessageLookupResponse{Items: items}, nil
}

func (s *SQLiteStore) CancelMessages(req MessageCancelRequest) (MessageCancelResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return MessageCancelResponse{}, nil
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, 6+len(ids))
	args = append(
		args,
		string(StateCanceled),
		s.now().UnixNano(),
		string(StateQueued),
		string(StateLeased),
		string(StateDead),
	)
	for _, id := range ids {
		args = append(args, id)
	}

	query := `
UPDATE queue_items
SET state = ?, lease_id = NULL, lease_until = NULL, next_run_at = ?, dead_reason = NULL
WHERE state IN (?, ?, ?)
  AND id IN (` + placeholders + `);`

	res, err := s.db.ExecContext(context.Background(), query, args...)
	if err != nil {
		return MessageCancelResponse{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return MessageCancelResponse{}, err
	}
	return MessageCancelResponse{Canceled: int(n), Matched: int(n)}, nil
}

func (s *SQLiteStore) RequeueMessages(req MessageRequeueRequest) (MessageRequeueResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return MessageRequeueResponse{}, nil
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, 5+len(ids))
	args = append(
		args,
		string(StateQueued),
		s.now().UnixNano(),
		string(StateDead),
		string(StateCanceled),
	)
	for _, id := range ids {
		args = append(args, id)
	}

	query := `
UPDATE queue_items
SET state = ?, lease_id = NULL, lease_until = NULL, next_run_at = ?, dead_reason = NULL
WHERE state IN (?, ?)
  AND id IN (` + placeholders + `);`

	res, err := s.db.ExecContext(context.Background(), query, args...)
	if err != nil {
		return MessageRequeueResponse{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return MessageRequeueResponse{}, err
	}
	if n > 0 {
		s.signal()
	}
	return MessageRequeueResponse{Requeued: int(n), Matched: int(n)}, nil
}

func (s *SQLiteStore) ResumeMessages(req MessageResumeRequest) (MessageResumeResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return MessageResumeResponse{}, nil
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, 4+len(ids))
	args = append(
		args,
		string(StateQueued),
		s.now().UnixNano(),
		string(StateCanceled),
	)
	for _, id := range ids {
		args = append(args, id)
	}

	query := `
UPDATE queue_items
SET state = ?, lease_id = NULL, lease_until = NULL, next_run_at = ?, dead_reason = NULL
WHERE state = ?
  AND id IN (` + placeholders + `);`

	res, err := s.db.ExecContext(context.Background(), query, args...)
	if err != nil {
		return MessageResumeResponse{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return MessageResumeResponse{}, err
	}
	if n > 0 {
		s.signal()
	}
	return MessageResumeResponse{Resumed: int(n), Matched: int(n)}, nil
}

func (s *SQLiteStore) CancelMessagesByFilter(req MessageManageFilterRequest) (MessageCancelResponse, error) {
	ids, err := s.selectMessageIDsByFilter(req, []State{StateQueued, StateLeased, StateDead})
	if err != nil {
		return MessageCancelResponse{}, err
	}
	if len(ids) == 0 {
		if req.PreviewOnly {
			return MessageCancelResponse{PreviewOnly: true}, nil
		}
		return MessageCancelResponse{}, nil
	}
	if req.PreviewOnly {
		return MessageCancelResponse{
			Canceled:    0,
			Matched:     len(ids),
			PreviewOnly: true,
		}, nil
	}
	resp, err := s.CancelMessages(MessageCancelRequest{IDs: ids})
	if err != nil {
		return MessageCancelResponse{}, err
	}
	resp.Matched = len(ids)
	return resp, nil
}

func (s *SQLiteStore) ResumeMessagesByFilter(req MessageManageFilterRequest) (MessageResumeResponse, error) {
	ids, err := s.selectMessageIDsByFilter(req, []State{StateCanceled})
	if err != nil {
		return MessageResumeResponse{}, err
	}
	if len(ids) == 0 {
		if req.PreviewOnly {
			return MessageResumeResponse{PreviewOnly: true}, nil
		}
		return MessageResumeResponse{}, nil
	}
	if req.PreviewOnly {
		return MessageResumeResponse{
			Resumed:     0,
			Matched:     len(ids),
			PreviewOnly: true,
		}, nil
	}
	resp, err := s.ResumeMessages(MessageResumeRequest{IDs: ids})
	if err != nil {
		return MessageResumeResponse{}, err
	}
	resp.Matched = len(ids)
	return resp, nil
}

func (s *SQLiteStore) RequeueMessagesByFilter(req MessageManageFilterRequest) (MessageRequeueResponse, error) {
	ids, err := s.selectMessageIDsByFilter(req, []State{StateDead, StateCanceled})
	if err != nil {
		return MessageRequeueResponse{}, err
	}
	if len(ids) == 0 {
		if req.PreviewOnly {
			return MessageRequeueResponse{PreviewOnly: true}, nil
		}
		return MessageRequeueResponse{}, nil
	}
	if req.PreviewOnly {
		return MessageRequeueResponse{
			Requeued:    0,
			Matched:     len(ids),
			PreviewOnly: true,
		}, nil
	}
	resp, err := s.RequeueMessages(MessageRequeueRequest{IDs: ids})
	if err != nil {
		return MessageRequeueResponse{}, err
	}
	resp.Matched = len(ids)
	return resp, nil
}

func (s *SQLiteStore) Stats() (Stats, error) {
	pruneNow := s.now()
	if err := s.maybePrune(pruneNow); err != nil {
		return Stats{}, err
	}

	rows, err := s.db.QueryContext(context.Background(), `
SELECT state, COUNT(*)
FROM queue_items
GROUP BY state;
`)
	if err != nil {
		return Stats{}, err
	}
	defer rows.Close()

	byState := map[State]int{
		StateQueued:    0,
		StateLeased:    0,
		StateDelivered: 0,
		StateDead:      0,
		StateCanceled:  0,
	}
	total := 0
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return Stats{}, err
		}
		st := State(state)
		byState[st] = count
		total += count
	}
	if err := rows.Err(); err != nil {
		return Stats{}, err
	}

	var oldestQueuedReceivedNanos sql.NullInt64
	var earliestQueuedNextRunNanos sql.NullInt64
	if err := s.db.QueryRowContext(context.Background(), `
SELECT MIN(received_at), MIN(next_run_at)
FROM queue_items
WHERE state = ?;
`, string(StateQueued)).Scan(&oldestQueuedReceivedNanos, &earliestQueuedNextRunNanos); err != nil {
		return Stats{}, err
	}

	var oldestQueuedReceivedAt time.Time
	if oldestQueuedReceivedNanos.Valid {
		oldestQueuedReceivedAt = time.Unix(0, oldestQueuedReceivedNanos.Int64).UTC()
	}
	var earliestQueuedNextRun time.Time
	if earliestQueuedNextRunNanos.Valid {
		earliestQueuedNextRun = time.Unix(0, earliestQueuedNextRunNanos.Int64).UTC()
	}

	oldestQueuedAge := time.Duration(0)
	if !oldestQueuedReceivedAt.IsZero() && !pruneNow.Before(oldestQueuedReceivedAt) {
		oldestQueuedAge = pruneNow.Sub(oldestQueuedReceivedAt)
	}
	readyLag := time.Duration(0)
	if !earliestQueuedNextRun.IsZero() && pruneNow.After(earliestQueuedNextRun) {
		readyLag = pruneNow.Sub(earliestQueuedNextRun)
	}

	backlogRows, err := s.db.QueryContext(context.Background(), `
SELECT route, target, COUNT(*), MIN(received_at), MIN(next_run_at)
FROM queue_items
WHERE state = ?
GROUP BY route, target
ORDER BY COUNT(*) DESC, route ASC, target ASC
LIMIT ?;
`, string(StateQueued), statsTopBacklogLimit)
	if err != nil {
		return Stats{}, err
	}
	defer backlogRows.Close()

	topQueued := make([]BacklogBucket, 0, statsTopBacklogLimit)
	for backlogRows.Next() {
		var b BacklogBucket
		var oldest sql.NullInt64
		var earliest sql.NullInt64
		if err := backlogRows.Scan(&b.Route, &b.Target, &b.Queued, &oldest, &earliest); err != nil {
			return Stats{}, err
		}
		if oldest.Valid {
			b.OldestQueuedReceivedAt = time.Unix(0, oldest.Int64).UTC()
		}
		if earliest.Valid {
			b.EarliestQueuedNextRun = time.Unix(0, earliest.Int64).UTC()
		}
		if !b.OldestQueuedReceivedAt.IsZero() && !pruneNow.Before(b.OldestQueuedReceivedAt) {
			b.OldestQueuedAge = pruneNow.Sub(b.OldestQueuedReceivedAt)
		}
		if !b.EarliestQueuedNextRun.IsZero() && pruneNow.After(b.EarliestQueuedNextRun) {
			b.ReadyLag = pruneNow.Sub(b.EarliestQueuedNextRun)
		}
		topQueued = append(topQueued, b)
	}
	if err := backlogRows.Err(); err != nil {
		return Stats{}, err
	}

	return Stats{
		Total:                  total,
		ByState:                byState,
		OldestQueuedReceivedAt: oldestQueuedReceivedAt,
		EarliestQueuedNextRun:  earliestQueuedNextRun,
		OldestQueuedAge:        oldestQueuedAge,
		ReadyLag:               readyLag,
		TopQueued:              topQueued,
	}, nil
}

func (s *SQLiteStore) CaptureBacklogTrendSample(at time.Time) error {
	capturedAt := at
	if capturedAt.IsZero() {
		capturedAt = s.now()
	}
	capturedAt = capturedAt.UTC()
	if err := s.maybePrune(capturedAt); err != nil {
		return err
	}

	type counts struct {
		queued int
		leased int
		dead   int
	}

	rows, err := s.db.QueryContext(context.Background(), `
SELECT route, target, state, COUNT(*)
FROM queue_items
WHERE state IN (?, ?, ?)
GROUP BY route, target, state;
`, string(StateQueued), string(StateLeased), string(StateDead))
	if err != nil {
		return err
	}
	defer rows.Close()

	perRoute := make(map[string]counts)
	global := counts{}
	for rows.Next() {
		var route string
		var target string
		var state string
		var count int
		if err := rows.Scan(&route, &target, &state, &count); err != nil {
			return err
		}
		key := route + "\x00" + target
		c := perRoute[key]
		switch State(state) {
		case StateQueued:
			c.queued += count
			global.queued += count
		case StateLeased:
			c.leased += count
			global.leased += count
		case StateDead:
			c.dead += count
			global.dead += count
		}
		perRoute[key] = c
	}
	if err := rows.Err(); err != nil {
		return err
	}

	ctx := context.Background()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE;"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_, _ = conn.ExecContext(ctx, "ROLLBACK;")
	}()

	capturedNanos := capturedAt.UnixNano()
	if _, err := conn.ExecContext(ctx, `DELETE FROM backlog_trend_samples WHERE captured_at = ?;`, capturedNanos); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `DELETE FROM backlog_trend_samples WHERE captured_at < ?;`, capturedAt.Add(-backlogTrendRetention).UnixNano()); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, `
INSERT INTO backlog_trend_samples (captured_at, route, target, queued, leased, dead)
VALUES (?, '', '', ?, ?, ?);
`, capturedNanos, global.queued, global.leased, global.dead); err != nil {
		return err
	}

	keys := make([]string, 0, len(perRoute))
	for key := range perRoute {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		c := perRoute[key]
		parts := strings.SplitN(key, "\x00", 2)
		route := parts[0]
		target := ""
		if len(parts) > 1 {
			target = parts[1]
		}
		if c.queued == 0 && c.leased == 0 && c.dead == 0 {
			continue
		}
		if _, err := conn.ExecContext(ctx, `
INSERT INTO backlog_trend_samples (captured_at, route, target, queued, leased, dead)
VALUES (?, ?, ?, ?, ?, ?);
`, capturedNanos, route, target, c.queued, c.leased, c.dead); err != nil {
			return err
		}
	}

	if _, err := conn.ExecContext(ctx, "COMMIT;"); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *SQLiteStore) ListBacklogTrend(req BacklogTrendListRequest) (BacklogTrendListResponse, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 1000
	}
	if limit > 20000 {
		limit = 20000
	}

	route := strings.TrimSpace(req.Route)
	target := strings.TrimSpace(req.Target)
	since := req.Since.UTC()
	until := req.Until.UTC()

	query := ""
	args := make([]any, 0, 8)
	if route == "" && target == "" {
		query = `
SELECT captured_at, queued, leased, dead
FROM backlog_trend_samples
WHERE route = '' AND target = ''`
	} else {
		query = `
SELECT captured_at, SUM(queued), SUM(leased), SUM(dead)
FROM backlog_trend_samples
WHERE NOT (route = '' AND target = '')`
		if route != "" {
			query += " AND route = ?"
			args = append(args, route)
		}
		if target != "" {
			query += " AND target = ?"
			args = append(args, target)
		}
	}
	if !since.IsZero() {
		query += " AND captured_at >= ?"
		args = append(args, since.UnixNano())
	}
	if !until.IsZero() {
		query += " AND captured_at < ?"
		args = append(args, until.UnixNano())
	}
	if route != "" || target != "" {
		query += " GROUP BY captured_at"
	}
	query += " ORDER BY captured_at DESC LIMIT ?"
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return BacklogTrendListResponse{}, err
	}
	defer rows.Close()

	out := make([]BacklogTrendSample, 0, limit+1)
	for rows.Next() {
		var capturedNanos int64
		var queued int
		var leased int
		var dead int
		if err := rows.Scan(&capturedNanos, &queued, &leased, &dead); err != nil {
			return BacklogTrendListResponse{}, err
		}
		out = append(out, BacklogTrendSample{
			CapturedAt: time.Unix(0, capturedNanos).UTC(),
			Queued:     queued,
			Leased:     leased,
			Dead:       dead,
		})
	}
	if err := rows.Err(); err != nil {
		return BacklogTrendListResponse{}, err
	}

	truncated := false
	if len(out) > limit {
		truncated = true
		out = out[:limit]
	}

	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}

	return BacklogTrendListResponse{
		Items:     out,
		Truncated: truncated,
	}, nil
}

func (s *SQLiteStore) selectMessageIDsByFilter(req MessageManageFilterRequest, allowed []State) ([]string, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	allowedSet := make(map[State]struct{}, len(allowed))
	for _, st := range allowed {
		allowedSet[st] = struct{}{}
	}

	states := make([]State, 0, len(allowed))
	if req.State != "" {
		if _, ok := allowedSet[req.State]; !ok {
			return nil, nil
		}
		states = append(states, req.State)
	} else {
		states = append(states, allowed...)
	}

	query := `
SELECT id
FROM queue_items
WHERE 1 = 1`
	args := make([]any, 0, 6+len(states))
	if req.Route != "" {
		query += " AND route = ?"
		args = append(args, req.Route)
	}
	if req.Target != "" {
		query += " AND target = ?"
		args = append(args, req.Target)
	}
	if !req.Before.IsZero() {
		query += " AND received_at < ?"
		args = append(args, req.Before.UnixNano())
	}

	if len(states) == 1 {
		query += " AND state = ?"
		args = append(args, string(states[0]))
	} else {
		query += " AND state IN (" + strings.TrimRight(strings.Repeat("?,", len(states)), ",") + ")"
		for _, st := range states {
			args = append(args, string(st))
		}
	}

	query += " ORDER BY received_at DESC, id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *SQLiteStore) RecordAttempt(attempt DeliveryAttempt) error {
	if strings.TrimSpace(attempt.ID) == "" {
		attempt.ID = newHexID("att_")
	}
	if attempt.CreatedAt.IsZero() {
		attempt.CreatedAt = s.now()
	}
	attempt.CreatedAt = attempt.CreatedAt.UTC()
	attempt.Error = strings.TrimSpace(attempt.Error)
	attempt.DeadReason = strings.TrimSpace(attempt.DeadReason)
	if attempt.Outcome == "" {
		attempt.Outcome = AttemptOutcomeRetry
	}

	var statusCode any
	if attempt.StatusCode > 0 {
		statusCode = attempt.StatusCode
	}
	var errVal any
	if attempt.Error != "" {
		errVal = attempt.Error
	}
	var deadReason any
	if attempt.DeadReason != "" {
		deadReason = attempt.DeadReason
	}

	_, err := s.db.ExecContext(context.Background(), `
INSERT INTO delivery_attempts (
  id, event_id, route, target, attempt, status_code, error, outcome, dead_reason, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`,
		attempt.ID,
		attempt.EventID,
		attempt.Route,
		attempt.Target,
		attempt.Attempt,
		statusCode,
		errVal,
		string(attempt.Outcome),
		deadReason,
		attempt.CreatedAt.UnixNano(),
	)
	return err
}

func (s *SQLiteStore) ListAttempts(req AttemptListRequest) (AttemptListResponse, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	query := `
SELECT id, event_id, route, target, attempt, status_code, error, outcome, dead_reason, created_at
FROM delivery_attempts
WHERE 1 = 1`
	args := make([]any, 0, 6)
	if req.Route != "" {
		query += " AND route = ?"
		args = append(args, req.Route)
	}
	if req.Target != "" {
		query += " AND target = ?"
		args = append(args, req.Target)
	}
	if req.EventID != "" {
		query += " AND event_id = ?"
		args = append(args, req.EventID)
	}
	if req.Outcome != "" {
		query += " AND outcome = ?"
		args = append(args, string(req.Outcome))
	}
	if !req.Before.IsZero() {
		query += " AND created_at < ?"
		args = append(args, req.Before.UnixNano())
	}
	query += " ORDER BY created_at DESC, id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return AttemptListResponse{}, err
	}
	defer rows.Close()

	items := make([]DeliveryAttempt, 0, limit)
	for rows.Next() {
		var item DeliveryAttempt
		var createdAtNanos int64
		var statusCode sql.NullInt64
		var errText sql.NullString
		var deadReason sql.NullString
		var outcome string

		if err := rows.Scan(
			&item.ID,
			&item.EventID,
			&item.Route,
			&item.Target,
			&item.Attempt,
			&statusCode,
			&errText,
			&outcome,
			&deadReason,
			&createdAtNanos,
		); err != nil {
			return AttemptListResponse{}, err
		}

		if statusCode.Valid {
			item.StatusCode = int(statusCode.Int64)
		}
		if errText.Valid {
			item.Error = errText.String
		}
		item.Outcome = AttemptOutcome(outcome)
		if deadReason.Valid {
			item.DeadReason = deadReason.String
		}
		item.CreatedAt = time.Unix(0, createdAtNanos).UTC()

		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return AttemptListResponse{}, err
	}

	return AttemptListResponse{Items: items}, nil
}

type leaseItem struct {
	id         string
	route      string
	target     string
	attempt    int
	expired    bool
	leaseUntil time.Time
}

func (s *SQLiteStore) withLease(leaseID string, fn func(ctx context.Context, conn *sql.Conn, item leaseItem) error) error {
	if strings.TrimSpace(leaseID) == "" {
		return ErrLeaseNotFound
	}

	now := s.now()

	ctx := context.Background()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE;"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_, _ = conn.ExecContext(ctx, "ROLLBACK;")
	}()

	var item leaseItem
	var state string
	var leaseUntilNanos sql.NullInt64

	err = conn.QueryRowContext(ctx, `
SELECT id, route, target, state, attempt, lease_until
FROM queue_items
WHERE lease_id = ?
LIMIT 1;
`, leaseID).Scan(&item.id, &item.route, &item.target, &state, &item.attempt, &leaseUntilNanos)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLeaseNotFound
		}
		return err
	}
	if state != string(StateLeased) {
		return ErrLeaseNotFound
	}
	if leaseUntilNanos.Valid {
		item.leaseUntil = time.Unix(0, leaseUntilNanos.Int64).UTC()
		if !now.Before(item.leaseUntil) {
			item.expired = true
		}
	}

	if err := fn(ctx, conn, item); err != nil {
		if errors.Is(err, ErrLeaseExpired) {
			if _, err := conn.ExecContext(ctx, "COMMIT;"); err != nil {
				return err
			}
			committed = true
			return ErrLeaseExpired
		}
		return err
	}

	if _, err := conn.ExecContext(ctx, "COMMIT;"); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *SQLiteStore) requeueExpiredLeases(ctx context.Context, conn *sql.Conn, now time.Time) error {
	_, err := conn.ExecContext(ctx, `
UPDATE queue_items
SET state = ?, lease_id = NULL, lease_until = NULL, next_run_at = ?, dead_reason = NULL
WHERE state = ?
  AND lease_until IS NOT NULL
  AND lease_until <= ?;
`,
		string(StateQueued),
		now.UnixNano(),
		string(StateLeased),
		now.UnixNano(),
	)
	return err
}

func (s *SQLiteStore) requeueLease(ctx context.Context, conn *sql.Conn, id string, _ string, _ string, _ int) error {
	_, err := conn.ExecContext(ctx, `
UPDATE queue_items
SET state = ?, lease_id = NULL, lease_until = NULL, next_run_at = ?, dead_reason = NULL
WHERE id = ?;
`,
		string(StateQueued),
		s.now().UnixNano(),
		id,
	)
	return err
}

func (s *SQLiteStore) now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nowFn()
}

func (s *SQLiteStore) signal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.notify)
	s.notify = make(chan struct{})
}

func (s *SQLiteStore) waitCh() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notify
}

func marshalStringMap(in map[string]string) (any, error) {
	if in == nil {
		return nil, nil
	}
	b, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func unmarshalStringMap(in sql.NullString) map[string]string {
	if !in.Valid || strings.TrimSpace(in.String) == "" {
		return nil
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(in.String), &out); err != nil {
		return nil
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func newHexID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

func normalizeUniqueIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func mapQueueInsertError(err error) error {
	if err == nil {
		return nil
	}
	if isSQLiteConstraintError(err) {
		return ErrEnvelopeExists
	}
	return err
}

func isSQLiteConstraintError(err error) bool {
	var sqliteErr *sqlite3.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	// Extended sqlite result codes include base code in the lower 8 bits.
	const sqliteConstraintBase = 19
	return sqliteErr.Code()&0xff == sqliteConstraintBase
}
