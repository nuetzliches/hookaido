package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nuetzliches/hookaido/internal/hookaido"
	"github.com/nuetzliches/hookaido/internal/queue"
)

type Option func(*Store)

type Store struct {
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
	deliveredRetentionMaxAge time.Duration
	dlqRetentionMaxAge       time.Duration
	dlqMaxDepth              int

	metrics *postgresRuntimeMetrics
}

type postgresRuntimeMetrics struct {
	mu sync.Mutex

	operationDuration map[string]sqliteHistogram
	operationTotal    map[string]int64
	errorsTotal       map[string]map[string]int64
}

func newPostgresRuntimeMetrics() *postgresRuntimeMetrics {
	return &postgresRuntimeMetrics{
		operationDuration: make(map[string]sqliteHistogram),
		operationTotal:    make(map[string]int64),
		errorsTotal:       make(map[string]map[string]int64),
	}
}

var _ queue.Store = (*Store)(nil)
var _ queue.BacklogTrendStore = (*Store)(nil)

const (
	postgresMaxDequeueBatch = 100
	postgresMaxListLimit    = 1000
	postgresBacklogMaxLimit = 20000
)

const postgresSchemaV1 = `
CREATE TABLE IF NOT EXISTS queue_items (
  id             TEXT PRIMARY KEY,
  route          TEXT NOT NULL,
  target         TEXT NOT NULL,
  state          TEXT NOT NULL,
  received_at    TIMESTAMPTZ NOT NULL,
  attempt        INTEGER NOT NULL,
  next_run_at    TIMESTAMPTZ NOT NULL,
  payload        BYTEA NOT NULL,
  headers_json   JSONB NOT NULL DEFAULT '{}'::jsonb,
  trace_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
  dead_reason    TEXT,
  schema_version INTEGER NOT NULL,
  lease_id       TEXT UNIQUE,
  lease_until    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_queue_ready
  ON queue_items(state, route, target, next_run_at, received_at);
CREATE INDEX IF NOT EXISTS idx_queue_lease_id
  ON queue_items(lease_id);
CREATE INDEX IF NOT EXISTS idx_queue_leases
  ON queue_items(state, lease_until);
CREATE INDEX IF NOT EXISTS idx_queue_state_received_at
  ON queue_items(state, received_at);
CREATE INDEX IF NOT EXISTS idx_queue_state_next_run_at
  ON queue_items(state, next_run_at);

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
  created_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_created
  ON delivery_attempts(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_event
  ON delivery_attempts(event_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_route_target
  ON delivery_attempts(route, target, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS backlog_trend_samples (
  captured_at BIGINT NOT NULL,
  route       TEXT NOT NULL,
  target      TEXT NOT NULL,
  queued      INTEGER NOT NULL,
  leased      INTEGER NOT NULL,
  dead        INTEGER NOT NULL,
  PRIMARY KEY (captured_at, route, target)
);
CREATE INDEX IF NOT EXISTS idx_pg_backlog_trend_time
  ON backlog_trend_samples(captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_pg_backlog_trend_route_target_time
  ON backlog_trend_samples(route, target, captured_at DESC);

CREATE TABLE IF NOT EXISTS runtime_secrets (
  pool_name  TEXT NOT NULL,
  id         TEXT NOT NULL,
  sealed     BYTEA NOT NULL,
  not_before TIMESTAMPTZ NOT NULL,
  not_after  TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (pool_name, id)
);
CREATE INDEX IF NOT EXISTS idx_runtime_secrets_pool
  ON runtime_secrets(pool_name, not_before DESC);
`

func WithNowFunc(now func() time.Time) Option {
	return func(s *Store) {
		if now != nil {
			s.nowFn = now
		}
	}
}

func WithPollInterval(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.pollInterval = d
		}
	}
}

func WithQueueLimits(maxDepth int, dropPolicy string) Option {
	return func(s *Store) {
		if maxDepth >= 0 {
			s.maxDepth = maxDepth
		}
		if strings.TrimSpace(dropPolicy) != "" {
			s.dropPolicy = strings.ToLower(strings.TrimSpace(dropPolicy))
		}
	}
}

func WithRetention(maxAge, pruneInterval time.Duration) Option {
	return func(s *Store) {
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

func WithDeliveredRetention(maxAge time.Duration) Option {
	return func(s *Store) {
		if maxAge > 0 {
			s.deliveredRetentionMaxAge = maxAge
		} else {
			s.deliveredRetentionMaxAge = 0
		}
	}
}

func WithDLQRetention(maxAge time.Duration, maxDepth int) Option {
	return func(s *Store) {
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

func NewStore(dsn string, opts ...Option) (*Store, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, errors.New("empty postgres dsn")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	s := &Store{
		db:           db,
		nowFn:        time.Now,
		notify:       make(chan struct{}),
		pollInterval: 1 * time.Second,
		dropPolicy:   "reject",
		metrics:      newPostgresRuntimeMetrics(),
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

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) init() error {
	_, err := s.db.ExecContext(context.Background(), postgresSchemaV1)
	return err
}

func (s *Store) signal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.notify)
	s.notify = make(chan struct{})
}

func (s *Store) waitCh() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notify
}

// NotifyCh returns a channel that is closed when new items become available.
// After the channel fires, callers must call NotifyCh again to get the next signal.
func (s *Store) NotifyCh() <-chan struct{} { return s.waitCh() }

func (s *Store) Enqueue(env queue.Envelope) error {
	return s.runStoreOperation("enqueue", func() error {
		if s == nil || s.db == nil {
			return errors.New("postgres store is closed")
		}
		now := s.now()
		if err := s.maybePrune(now); err != nil {
			return err
		}

		if env.ID == "" {
			env.ID = newHexID("evt_")
		}
		if env.Route == "" {
			return errors.New("route is required")
		}
		if env.Target == "" {
			return errors.New("target is required")
		}
		if env.ReceivedAt.IsZero() {
			env.ReceivedAt = now
		}
		env.ReceivedAt = env.ReceivedAt.UTC()
		if env.NextRunAt.IsZero() {
			env.NextRunAt = now
		}
		env.NextRunAt = env.NextRunAt.UTC()
		if env.State == "" {
			env.State = queue.StateQueued
		}
		if env.Attempt < 0 {
			env.Attempt = 0
		}
		if env.SchemaVersion == 0 {
			env.SchemaVersion = 1
		}

		headersJSON, err := encodeStringMapJSON(env.Headers)
		if err != nil {
			return err
		}
		traceJSON, err := encodeStringMapJSON(env.Trace)
		if err != nil {
			return err
		}

		if s.maxDepth > 0 {
			active, err := s.activeCount()
			if err != nil {
				return err
			}
			if active >= s.maxDepth {
				switch s.dropPolicy {
				case "drop_oldest":
					dropped, err := s.dropOldestQueued()
					if err != nil {
						return err
					}
					if !dropped {
						return queue.ErrQueueFull
					}
				default:
					return queue.ErrQueueFull
				}
			}
		}

		_, err = s.db.ExecContext(context.Background(), `
INSERT INTO queue_items (
  id, route, target, state, received_at, attempt, next_run_at,
  payload, headers_json, trace_json, dead_reason, schema_version, lease_id, lease_until
) VALUES (
  $1, $2, $3, $4, $5, $6, $7,
  $8, $9, $10, $11, $12, $13, $14
)
`,
			env.ID,
			env.Route,
			env.Target,
			string(env.State),
			env.ReceivedAt,
			env.Attempt,
			env.NextRunAt,
			env.Payload,
			headersJSON,
			traceJSON,
			strings.TrimSpace(env.DeadReason),
			env.SchemaVersion,
			nullIfEmpty(strings.TrimSpace(env.LeaseID)),
			nullTime(env.LeaseUntil),
		)
		if err != nil {
			return mapPostgresInsertError(err)
		}
		s.signal()
		return nil
	})
}

func (s *Store) Dequeue(req queue.DequeueRequest) (queue.DequeueResponse, error) {
	return runPostgresStoreOperationResult(s, "dequeue", func() (queue.DequeueResponse, error) {
		if s == nil || s.db == nil {
			return queue.DequeueResponse{}, errors.New("postgres store is closed")
		}

		pruneNow := req.Now
		if pruneNow.IsZero() {
			pruneNow = s.now()
		}
		if err := s.maybePrune(pruneNow); err != nil {
			return queue.DequeueResponse{}, err
		}

		batch := req.Batch
		if batch <= 0 {
			batch = 1
		}
		if batch > postgresMaxDequeueBatch {
			batch = postgresMaxDequeueBatch
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
				return queue.DequeueResponse{}, err
			}
			if len(resp.Items) > 0 || maxWait == 0 {
				return resp, nil
			}

			remaining := time.Until(deadline)
			if remaining <= 0 {
				return queue.DequeueResponse{}, nil
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
	})
}

func (s *Store) dequeueOnce(req queue.DequeueRequest, batch int, leaseTTL time.Duration) (queue.DequeueResponse, error) {
	batch = clampSliceCap(batch, postgresMaxDequeueBatch)
	if batch == 0 {
		batch = 1
	}

	now := req.Now
	if now.IsZero() {
		now = s.now()
	}
	now = now.UTC()
	leaseUntil := now.Add(leaseTTL).UTC()

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return queue.DequeueResponse{}, err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback()
	}()

	if err := s.requeueExpiredLeasesTx(ctx, tx, now); err != nil {
		return queue.DequeueResponse{}, err
	}

	rows, err := tx.QueryContext(ctx, `
SELECT id, route, target, state, received_at, attempt, next_run_at,
       payload, headers_json, trace_json, dead_reason, schema_version
FROM queue_items
WHERE route = $1
  AND target = $2
  AND state = $3
  AND next_run_at <= $4
ORDER BY next_run_at ASC, received_at ASC, id ASC
LIMIT $5
FOR UPDATE SKIP LOCKED
`,
		req.Route,
		req.Target,
		string(queue.StateQueued),
		now,
		batch,
	)
	if err != nil {
		return queue.DequeueResponse{}, err
	}
	defer rows.Close()

	items := make([]queue.Envelope, 0, clampSliceCap(batch, postgresMaxDequeueBatch))
	for rows.Next() {
		var (
			item        queue.Envelope
			state       string
			headersJSON []byte
			traceJSON   []byte
			deadReason  sql.NullString
		)
		if err := rows.Scan(
			&item.ID,
			&item.Route,
			&item.Target,
			&state,
			&item.ReceivedAt,
			&item.Attempt,
			&item.NextRunAt,
			&item.Payload,
			&headersJSON,
			&traceJSON,
			&deadReason,
			&item.SchemaVersion,
		); err != nil {
			return queue.DequeueResponse{}, err
		}
		item.State = queue.State(state)
		item.ReceivedAt = item.ReceivedAt.UTC()
		item.NextRunAt = item.NextRunAt.UTC()
		item.Headers = decodeStringMapJSON(headersJSON)
		item.Trace = decodeStringMapJSON(traceJSON)
		if deadReason.Valid {
			item.DeadReason = deadReason.String
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return queue.DequeueResponse{}, err
	}

	if len(items) == 0 {
		if err := tx.Commit(); err != nil {
			return queue.DequeueResponse{}, err
		}
		committed = true
		return queue.DequeueResponse{}, nil
	}

	for i := range items {
		leaseID := newHexID("lease_")
		_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, attempt = attempt + 1, lease_id = $2, lease_until = $3, next_run_at = $3, dead_reason = NULL
WHERE id = $4
  AND state = $5
`,
			string(queue.StateLeased),
			leaseID,
			leaseUntil,
			items[i].ID,
			string(queue.StateQueued),
		)
		if err != nil {
			return queue.DequeueResponse{}, err
		}
		items[i].State = queue.StateLeased
		items[i].Attempt++
		items[i].LeaseID = leaseID
		items[i].LeaseUntil = leaseUntil
		items[i].NextRunAt = leaseUntil
		items[i].DeadReason = ""
	}

	if err := tx.Commit(); err != nil {
		return queue.DequeueResponse{}, err
	}
	committed = true
	return queue.DequeueResponse{Items: items}, nil
}

func (s *Store) Ack(leaseID string) error {
	return s.runStoreOperation("ack", func() error {
		now := s.now().UTC()
		return s.withLease(leaseID, now, func(ctx context.Context, tx *sql.Tx, itemID string, _ time.Time) error {
			if s.deliveredRetentionMaxAge > 0 {
				_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE id = $3
  AND state = $4
`,
					string(queue.StateDelivered),
					now,
					itemID,
					string(queue.StateLeased),
				)
				return err
			}

			_, err := tx.ExecContext(ctx, `
DELETE FROM queue_items
WHERE id = $1
  AND state = $2
`,
				itemID,
				string(queue.StateLeased),
			)
			return err
		})
	})
}

func (s *Store) AckBatch(leaseIDs []string) (queue.LeaseBatchResult, error) {
	res := queue.LeaseBatchResult{Conflicts: make([]queue.LeaseBatchConflict, 0)}

	for _, rawLeaseID := range leaseIDs {
		leaseID := strings.TrimSpace(rawLeaseID)
		if leaseID == "" {
			res.Conflicts = append(res.Conflicts, queue.LeaseBatchConflict{LeaseID: rawLeaseID})
			continue
		}

		err := s.Ack(leaseID)
		if err == nil {
			res.Succeeded++
			continue
		}
		switch {
		case errors.Is(err, queue.ErrLeaseExpired):
			res.Conflicts = append(res.Conflicts, queue.LeaseBatchConflict{
				LeaseID: leaseID,
				Expired: true,
			})
		case errors.Is(err, queue.ErrLeaseNotFound):
			res.Conflicts = append(res.Conflicts, queue.LeaseBatchConflict{LeaseID: leaseID})
		default:
			return queue.LeaseBatchResult{}, err
		}
	}

	return res, nil
}

func (s *Store) Nack(leaseID string, delay time.Duration) error {
	return s.runStoreOperation("nack", func() error {
		if delay < 0 {
			delay = 0
		}
		now := s.now().UTC()
		return s.withLease(leaseID, now, func(ctx context.Context, tx *sql.Tx, itemID string, _ time.Time) error {
			_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE id = $3
  AND state = $4
`,
				string(queue.StateQueued),
				now.Add(delay).UTC(),
				itemID,
				string(queue.StateLeased),
			)
			return err
		})
	})
}

func (s *Store) NackBatch(leaseIDs []string, delay time.Duration) (queue.LeaseBatchResult, error) {
	res := queue.LeaseBatchResult{Conflicts: make([]queue.LeaseBatchConflict, 0)}

	for _, rawLeaseID := range leaseIDs {
		leaseID := strings.TrimSpace(rawLeaseID)
		if leaseID == "" {
			res.Conflicts = append(res.Conflicts, queue.LeaseBatchConflict{LeaseID: rawLeaseID})
			continue
		}

		err := s.Nack(leaseID, delay)
		if err == nil {
			res.Succeeded++
			continue
		}
		switch {
		case errors.Is(err, queue.ErrLeaseExpired):
			res.Conflicts = append(res.Conflicts, queue.LeaseBatchConflict{
				LeaseID: leaseID,
				Expired: true,
			})
		case errors.Is(err, queue.ErrLeaseNotFound):
			res.Conflicts = append(res.Conflicts, queue.LeaseBatchConflict{LeaseID: leaseID})
		default:
			return queue.LeaseBatchResult{}, err
		}
	}

	return res, nil
}

func (s *Store) Extend(leaseID string, extendBy time.Duration) error {
	return s.runStoreOperation("extend", func() error {
		if extendBy <= 0 {
			return nil
		}
		now := s.now().UTC()
		return s.withLease(leaseID, now, func(ctx context.Context, tx *sql.Tx, itemID string, leaseUntil time.Time) error {
			updated := leaseUntil.Add(extendBy).UTC()
			_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET lease_until = $1, next_run_at = $1
WHERE id = $2
  AND state = $3
`,
				updated,
				itemID,
				string(queue.StateLeased),
			)
			return err
		})
	})
}

func (s *Store) MarkDead(leaseID string, reason string) error {
	return s.runStoreOperation("mark_dead", func() error {
		now := s.now().UTC()
		return s.withLease(leaseID, now, func(ctx context.Context, tx *sql.Tx, itemID string, _ time.Time) error {
			_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = $3
WHERE id = $4
  AND state = $5
`,
				string(queue.StateDead),
				now,
				strings.TrimSpace(reason),
				itemID,
				string(queue.StateLeased),
			)
			return err
		})
	})
}

func (s *Store) MarkDeadBatch(leaseIDs []string, reason string) (queue.LeaseBatchResult, error) {
	res := queue.LeaseBatchResult{Conflicts: make([]queue.LeaseBatchConflict, 0)}

	for _, rawLeaseID := range leaseIDs {
		leaseID := strings.TrimSpace(rawLeaseID)
		if leaseID == "" {
			res.Conflicts = append(res.Conflicts, queue.LeaseBatchConflict{LeaseID: rawLeaseID})
			continue
		}

		err := s.MarkDead(leaseID, reason)
		if err == nil {
			res.Succeeded++
			continue
		}
		switch {
		case errors.Is(err, queue.ErrLeaseExpired):
			res.Conflicts = append(res.Conflicts, queue.LeaseBatchConflict{
				LeaseID: leaseID,
				Expired: true,
			})
		case errors.Is(err, queue.ErrLeaseNotFound):
			res.Conflicts = append(res.Conflicts, queue.LeaseBatchConflict{LeaseID: leaseID})
		default:
			return queue.LeaseBatchResult{}, err
		}
	}

	return res, nil
}

func (s *Store) ListDead(req queue.DeadListRequest) (queue.DeadListResponse, error) {
	return runPostgresStoreOperationResult(s, "list_dead", func() (queue.DeadListResponse, error) {
		limit := req.Limit
		if limit <= 0 {
			limit = 100
		}
		if limit > postgresMaxListLimit {
			limit = postgresMaxListLimit
		}

		args := make([]any, 0, 4)
		var where []string
		where = append(where, "state = $1")
		args = append(args, string(queue.StateDead))
		if req.Route != "" {
			where = append(where, fmt.Sprintf("route = $%d", len(args)+1))
			args = append(args, req.Route)
		}
		if !req.Before.IsZero() {
			where = append(where, fmt.Sprintf("received_at < $%d", len(args)+1))
			args = append(args, req.Before.UTC())
		}
		args = append(args, limit)
		limitIdx := len(args)

		query := `
SELECT id, route, target, state, received_at, attempt, next_run_at,
       payload, headers_json, trace_json, dead_reason, schema_version, lease_id, lease_until
FROM queue_items
WHERE ` + strings.Join(where, " AND ") + `
ORDER BY received_at DESC, id DESC
LIMIT $` + fmt.Sprintf("%d", limitIdx)

		rows, err := s.db.QueryContext(context.Background(), query, args...)
		if err != nil {
			return queue.DeadListResponse{}, err
		}
		defer rows.Close()

		items := make([]queue.Envelope, 0, clampSliceCap(limit, postgresMaxListLimit))
		for rows.Next() {
			item, err := scanQueueItem(rows)
			if err != nil {
				return queue.DeadListResponse{}, err
			}
			if !req.IncludePayload {
				item.Payload = nil
			}
			if !req.IncludeHeaders {
				item.Headers = nil
			}
			if !req.IncludeTrace {
				item.Trace = nil
			}
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			return queue.DeadListResponse{}, err
		}

		return queue.DeadListResponse{Items: items}, nil
	})
}

func (s *Store) RequeueDead(req queue.DeadRequeueRequest) (queue.DeadRequeueResponse, error) {
	return runPostgresStoreOperationResult(s, "requeue_dead", func() (queue.DeadRequeueResponse, error) {
		ids := normalizeUniqueIDs(req.IDs)
		if len(ids) == 0 {
			return queue.DeadRequeueResponse{}, nil
		}

		now := s.now().UTC()
		ctx := context.Background()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return queue.DeadRequeueResponse{}, err
		}
		committed := false
		defer func() {
			if committed {
				return
			}
			_ = tx.Rollback()
		}()

		requeued := 0
		for _, id := range ids {
			res, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE id = $3
  AND state = $4
`,
				string(queue.StateQueued),
				now,
				id,
				string(queue.StateDead),
			)
			if err != nil {
				return queue.DeadRequeueResponse{}, err
			}
			n, _ := res.RowsAffected()
			requeued += int(n)
		}

		if err := tx.Commit(); err != nil {
			return queue.DeadRequeueResponse{}, err
		}
		committed = true
		return queue.DeadRequeueResponse{Requeued: requeued}, nil
	})
}

func (s *Store) DeleteDead(req queue.DeadDeleteRequest) (queue.DeadDeleteResponse, error) {
	return runPostgresStoreOperationResult(s, "delete_dead", func() (queue.DeadDeleteResponse, error) {
		ids := normalizeUniqueIDs(req.IDs)
		if len(ids) == 0 {
			return queue.DeadDeleteResponse{}, nil
		}

		ctx := context.Background()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return queue.DeadDeleteResponse{}, err
		}
		committed := false
		defer func() {
			if committed {
				return
			}
			_ = tx.Rollback()
		}()

		deleted := 0
		for _, id := range ids {
			res, err := tx.ExecContext(ctx, `
DELETE FROM queue_items
WHERE id = $1
  AND state = $2
`,
				id,
				string(queue.StateDead),
			)
			if err != nil {
				return queue.DeadDeleteResponse{}, err
			}
			n, _ := res.RowsAffected()
			deleted += int(n)
		}

		if err := tx.Commit(); err != nil {
			return queue.DeadDeleteResponse{}, err
		}
		committed = true
		return queue.DeadDeleteResponse{Deleted: deleted}, nil
	})
}

func (s *Store) ListMessages(req queue.MessageListRequest) (queue.MessageListResponse, error) {
	return runPostgresStoreOperationResult(s, "list_messages", func() (queue.MessageListResponse, error) {
		if s == nil || s.db == nil {
			return queue.MessageListResponse{}, errors.New("postgres store is closed")
		}
		if err := s.maybePrune(s.now()); err != nil {
			return queue.MessageListResponse{}, err
		}

		limit := req.Limit
		if limit <= 0 {
			limit = 100
		}
		if limit > postgresMaxListLimit {
			limit = postgresMaxListLimit
		}

		order := strings.ToLower(strings.TrimSpace(req.Order))
		orderByReceived := "DESC"
		orderByID := "DESC"
		switch order {
		case "", queue.MessageOrderDesc:
		case queue.MessageOrderAsc:
			orderByReceived = "ASC"
			orderByID = "ASC"
		default:
			return queue.MessageListResponse{}, fmt.Errorf("invalid message order %q", req.Order)
		}

		query := `
SELECT id, route, target, state, received_at, attempt, next_run_at,
       payload, headers_json, trace_json, dead_reason, schema_version, lease_id, lease_until
FROM queue_items
WHERE 1 = 1
`
		args := make([]any, 0, 8)
		if req.Route != "" {
			query += fmt.Sprintf(" AND route = $%d", len(args)+1)
			args = append(args, req.Route)
		}
		if req.Target != "" {
			query += fmt.Sprintf(" AND target = $%d", len(args)+1)
			args = append(args, req.Target)
		}
		if req.State != "" {
			query += fmt.Sprintf(" AND state = $%d", len(args)+1)
			args = append(args, string(req.State))
		}
		if !req.Before.IsZero() {
			query += fmt.Sprintf(" AND received_at < $%d", len(args)+1)
			args = append(args, req.Before.UTC())
		}

		args = append(args, limit)
		query += fmt.Sprintf(" ORDER BY received_at %s, id %s LIMIT $%d", orderByReceived, orderByID, len(args))

		rows, err := s.db.QueryContext(context.Background(), query, args...)
		if err != nil {
			return queue.MessageListResponse{}, err
		}
		defer rows.Close()

		items := make([]queue.Envelope, 0, clampSliceCap(limit, postgresMaxListLimit))
		for rows.Next() {
			item, err := scanQueueItem(rows)
			if err != nil {
				return queue.MessageListResponse{}, err
			}
			if !req.IncludePayload {
				item.Payload = nil
			}
			if !req.IncludeHeaders {
				item.Headers = nil
			}
			if !req.IncludeTrace {
				item.Trace = nil
			}
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			return queue.MessageListResponse{}, err
		}

		return queue.MessageListResponse{Items: items}, nil
	})
}

func (s *Store) LookupMessages(req queue.MessageLookupRequest) (queue.MessageLookupResponse, error) {
	return runPostgresStoreOperationResult(s, "lookup_messages", func() (queue.MessageLookupResponse, error) {
		ids := normalizeUniqueIDs(req.IDs)
		if len(ids) == 0 {
			return queue.MessageLookupResponse{}, nil
		}

		rows, err := s.db.QueryContext(context.Background(), `
SELECT id, route, state
FROM queue_items
WHERE id = ANY($1)
`,
			ids,
		)
		if err != nil {
			return queue.MessageLookupResponse{}, err
		}
		defer rows.Close()

		byID := make(map[string]queue.MessageLookupItem, len(ids))
		for rows.Next() {
			var item queue.MessageLookupItem
			var state string
			if err := rows.Scan(&item.ID, &item.Route, &state); err != nil {
				return queue.MessageLookupResponse{}, err
			}
			item.State = queue.State(state)
			byID[item.ID] = item
		}
		if err := rows.Err(); err != nil {
			return queue.MessageLookupResponse{}, err
		}

		items := make([]queue.MessageLookupItem, 0, len(ids))
		for _, id := range ids {
			item, ok := byID[id]
			if !ok {
				continue
			}
			items = append(items, item)
		}
		return queue.MessageLookupResponse{Items: items}, nil
	})
}

func (s *Store) CancelMessages(req queue.MessageCancelRequest) (queue.MessageCancelResponse, error) {
	return runPostgresStoreOperationResult(s, "cancel_messages", func() (queue.MessageCancelResponse, error) {
		ids := normalizeUniqueIDs(req.IDs)
		if len(ids) == 0 {
			return queue.MessageCancelResponse{}, nil
		}

		res, err := s.db.ExecContext(context.Background(), `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE state = ANY($3)
  AND id = ANY($4)
`,
			string(queue.StateCanceled),
			s.now().UTC(),
			[]string{string(queue.StateQueued), string(queue.StateLeased), string(queue.StateDead)},
			ids,
		)
		if err != nil {
			return queue.MessageCancelResponse{}, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return queue.MessageCancelResponse{}, err
		}
		return queue.MessageCancelResponse{Canceled: int(n), Matched: int(n)}, nil
	})
}

func (s *Store) RequeueMessages(req queue.MessageRequeueRequest) (queue.MessageRequeueResponse, error) {
	return runPostgresStoreOperationResult(s, "requeue_messages", func() (queue.MessageRequeueResponse, error) {
		ids := normalizeUniqueIDs(req.IDs)
		if len(ids) == 0 {
			return queue.MessageRequeueResponse{}, nil
		}

		res, err := s.db.ExecContext(context.Background(), `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE state = ANY($3)
  AND id = ANY($4)
`,
			string(queue.StateQueued),
			s.now().UTC(),
			[]string{string(queue.StateDead), string(queue.StateCanceled)},
			ids,
		)
		if err != nil {
			return queue.MessageRequeueResponse{}, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return queue.MessageRequeueResponse{}, err
		}
		return queue.MessageRequeueResponse{Requeued: int(n), Matched: int(n)}, nil
	})
}

func (s *Store) ResumeMessages(req queue.MessageResumeRequest) (queue.MessageResumeResponse, error) {
	return runPostgresStoreOperationResult(s, "resume_messages", func() (queue.MessageResumeResponse, error) {
		ids := normalizeUniqueIDs(req.IDs)
		if len(ids) == 0 {
			return queue.MessageResumeResponse{}, nil
		}

		res, err := s.db.ExecContext(context.Background(), `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE state = $3
  AND id = ANY($4)
`,
			string(queue.StateQueued),
			s.now().UTC(),
			string(queue.StateCanceled),
			ids,
		)
		if err != nil {
			return queue.MessageResumeResponse{}, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return queue.MessageResumeResponse{}, err
		}
		return queue.MessageResumeResponse{Resumed: int(n), Matched: int(n)}, nil
	})
}

func (s *Store) CancelMessagesByFilter(req queue.MessageManageFilterRequest) (queue.MessageCancelResponse, error) {
	ids, err := s.selectMessageIDsByFilter(req, []queue.State{queue.StateQueued, queue.StateLeased, queue.StateDead})
	if err != nil {
		return queue.MessageCancelResponse{}, err
	}
	if len(ids) == 0 {
		if req.PreviewOnly {
			return queue.MessageCancelResponse{PreviewOnly: true}, nil
		}
		return queue.MessageCancelResponse{}, nil
	}
	if req.PreviewOnly {
		return queue.MessageCancelResponse{
			Canceled:    0,
			Matched:     len(ids),
			PreviewOnly: true,
		}, nil
	}
	resp, err := s.CancelMessages(queue.MessageCancelRequest{IDs: ids})
	if err != nil {
		return queue.MessageCancelResponse{}, err
	}
	resp.Matched = len(ids)
	return resp, nil
}

func (s *Store) RequeueMessagesByFilter(req queue.MessageManageFilterRequest) (queue.MessageRequeueResponse, error) {
	ids, err := s.selectMessageIDsByFilter(req, []queue.State{queue.StateDead, queue.StateCanceled})
	if err != nil {
		return queue.MessageRequeueResponse{}, err
	}
	if len(ids) == 0 {
		if req.PreviewOnly {
			return queue.MessageRequeueResponse{PreviewOnly: true}, nil
		}
		return queue.MessageRequeueResponse{}, nil
	}
	if req.PreviewOnly {
		return queue.MessageRequeueResponse{
			Requeued:    0,
			Matched:     len(ids),
			PreviewOnly: true,
		}, nil
	}
	resp, err := s.RequeueMessages(queue.MessageRequeueRequest{IDs: ids})
	if err != nil {
		return queue.MessageRequeueResponse{}, err
	}
	resp.Matched = len(ids)
	return resp, nil
}

func (s *Store) ResumeMessagesByFilter(req queue.MessageManageFilterRequest) (queue.MessageResumeResponse, error) {
	ids, err := s.selectMessageIDsByFilter(req, []queue.State{queue.StateCanceled})
	if err != nil {
		return queue.MessageResumeResponse{}, err
	}
	if len(ids) == 0 {
		if req.PreviewOnly {
			return queue.MessageResumeResponse{PreviewOnly: true}, nil
		}
		return queue.MessageResumeResponse{}, nil
	}
	if req.PreviewOnly {
		return queue.MessageResumeResponse{
			Resumed:     0,
			Matched:     len(ids),
			PreviewOnly: true,
		}, nil
	}
	resp, err := s.ResumeMessages(queue.MessageResumeRequest{IDs: ids})
	if err != nil {
		return queue.MessageResumeResponse{}, err
	}
	resp.Matched = len(ids)
	return resp, nil
}

func (s *Store) Stats() (queue.Stats, error) {
	return runPostgresStoreOperationResult(s, "stats", func() (queue.Stats, error) {
		stats := queue.Stats{
			ByState: map[queue.State]int{
				queue.StateQueued:    0,
				queue.StateLeased:    0,
				queue.StateDead:      0,
				queue.StateDelivered: 0,
				queue.StateCanceled:  0,
			},
			TopQueued: make([]queue.BacklogBucket, 0),
		}

		rows, err := s.db.QueryContext(context.Background(), `
SELECT state, COUNT(*)
FROM queue_items
GROUP BY state
`)
		if err != nil {
			return queue.Stats{}, err
		}
		defer rows.Close()

		total := 0
		for rows.Next() {
			var (
				state string
				count int
			)
			if err := rows.Scan(&state, &count); err != nil {
				return queue.Stats{}, err
			}
			st := queue.State(state)
			stats.ByState[st] = count
			total += count
		}
		if err := rows.Err(); err != nil {
			return queue.Stats{}, err
		}
		stats.Total = total

		now := s.now().UTC()

		var oldestQueued sql.NullTime
		if err := s.db.QueryRowContext(context.Background(), `
SELECT MIN(received_at)
FROM queue_items
WHERE state = $1
`,
			string(queue.StateQueued),
		).Scan(&oldestQueued); err != nil {
			return queue.Stats{}, err
		}
		if oldestQueued.Valid {
			stats.OldestQueuedReceivedAt = oldestQueued.Time.UTC()
			stats.OldestQueuedAge = now.Sub(stats.OldestQueuedReceivedAt)
			if stats.OldestQueuedAge < 0 {
				stats.OldestQueuedAge = 0
			}
		}

		var earliestReady sql.NullTime
		if err := s.db.QueryRowContext(context.Background(), `
SELECT MIN(next_run_at)
FROM queue_items
WHERE state = $1
`,
			string(queue.StateQueued),
		).Scan(&earliestReady); err != nil {
			return queue.Stats{}, err
		}
		if earliestReady.Valid {
			stats.EarliestQueuedNextRun = earliestReady.Time.UTC()
			stats.ReadyLag = now.Sub(stats.EarliestQueuedNextRun)
			if stats.ReadyLag < 0 {
				stats.ReadyLag = 0
			}
		}

		topRows, err := s.db.QueryContext(context.Background(), `
SELECT route, target, COUNT(*)
FROM queue_items
WHERE state = $1
GROUP BY route, target
ORDER BY COUNT(*) DESC, route ASC, target ASC
LIMIT $2
`,
			string(queue.StateQueued),
			statsTopBacklogLimit,
		)
		if err != nil {
			return queue.Stats{}, err
		}
		defer topRows.Close()

		for topRows.Next() {
			var b queue.BacklogBucket
			if err := topRows.Scan(&b.Route, &b.Target, &b.Queued); err != nil {
				return queue.Stats{}, err
			}
			stats.TopQueued = append(stats.TopQueued, b)
		}
		if err := topRows.Err(); err != nil {
			return queue.Stats{}, err
		}

		return stats, nil
	})
}

func (s *Store) CaptureBacklogTrendSample(at time.Time) error {
	return s.runStoreOperation("capture_backlog_trend", func() error {
		if s == nil || s.db == nil {
			return errors.New("postgres store is closed")
		}

		capturedAt := at
		if capturedAt.IsZero() {
			capturedAt = s.now()
		}
		capturedAt = capturedAt.UTC()
		if err := s.maybePrune(capturedAt); err != nil {
			return err
		}

		capturedNanos := capturedAt.UnixNano()
		retentionCutoff := capturedAt.Add(-backlogTrendRetention).UnixNano()

		ctx := context.Background()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		committed := false
		defer func() {
			if committed {
				return
			}
			_ = tx.Rollback()
		}()

		if _, err := tx.ExecContext(ctx, `
DELETE FROM backlog_trend_samples
WHERE captured_at = $1
`, capturedNanos); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
DELETE FROM backlog_trend_samples
WHERE captured_at < $1
`, retentionCutoff); err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx, `
INSERT INTO backlog_trend_samples (captured_at, route, target, queued, leased, dead)
SELECT
  $1,
  '',
  '',
  COALESCE(SUM(CASE WHEN state = $2 THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN state = $3 THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN state = $4 THEN 1 ELSE 0 END), 0)
FROM queue_items
WHERE state IN ($2, $3, $4)
`, capturedNanos, string(queue.StateQueued), string(queue.StateLeased), string(queue.StateDead)); err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx, `
INSERT INTO backlog_trend_samples (captured_at, route, target, queued, leased, dead)
SELECT
  $1,
  route,
  target,
  SUM(CASE WHEN state = $2 THEN 1 ELSE 0 END) AS queued,
  SUM(CASE WHEN state = $3 THEN 1 ELSE 0 END) AS leased,
  SUM(CASE WHEN state = $4 THEN 1 ELSE 0 END) AS dead
FROM queue_items
WHERE state IN ($2, $3, $4)
GROUP BY route, target
`, capturedNanos, string(queue.StateQueued), string(queue.StateLeased), string(queue.StateDead)); err != nil {
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		return nil
	})
}

func (s *Store) ListBacklogTrend(req queue.BacklogTrendListRequest) (queue.BacklogTrendListResponse, error) {
	return runPostgresStoreOperationResult(s, "list_backlog_trend", func() (queue.BacklogTrendListResponse, error) {
		if s == nil || s.db == nil {
			return queue.BacklogTrendListResponse{}, errors.New("postgres store is closed")
		}

		limit := req.Limit
		if limit <= 0 {
			limit = 1000
		}
		if limit > postgresBacklogMaxLimit {
			limit = postgresBacklogMaxLimit
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
				query += fmt.Sprintf(" AND route = $%d", len(args)+1)
				args = append(args, route)
			}
			if target != "" {
				query += fmt.Sprintf(" AND target = $%d", len(args)+1)
				args = append(args, target)
			}
		}
		if !since.IsZero() {
			query += fmt.Sprintf(" AND captured_at >= $%d", len(args)+1)
			args = append(args, since.UnixNano())
		}
		if !until.IsZero() {
			query += fmt.Sprintf(" AND captured_at < $%d", len(args)+1)
			args = append(args, until.UnixNano())
		}
		if route != "" || target != "" {
			query += " GROUP BY captured_at"
		}
		query += fmt.Sprintf(" ORDER BY captured_at DESC LIMIT $%d", len(args)+1)
		args = append(args, limit+1)

		rows, err := s.db.QueryContext(context.Background(), query, args...)
		if err != nil {
			return queue.BacklogTrendListResponse{}, err
		}
		defer rows.Close()

		out := make([]queue.BacklogTrendSample, 0, limit+1)
		for rows.Next() {
			var capturedNanos int64
			var queued int
			var leased int
			var dead int
			if err := rows.Scan(&capturedNanos, &queued, &leased, &dead); err != nil {
				return queue.BacklogTrendListResponse{}, err
			}
			out = append(out, queue.BacklogTrendSample{
				CapturedAt: time.Unix(0, capturedNanos).UTC(),
				Queued:     queued,
				Leased:     leased,
				Dead:       dead,
			})
		}
		if err := rows.Err(); err != nil {
			return queue.BacklogTrendListResponse{}, err
		}

		truncated := false
		if len(out) > limit {
			truncated = true
			out = out[:limit]
		}
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}

		return queue.BacklogTrendListResponse{
			Items:     out,
			Truncated: truncated,
		}, nil
	})
}

func (s *Store) RecordAttempt(attempt queue.DeliveryAttempt) error {
	return s.runStoreOperation("record_attempt", func() error {
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
			attempt.Outcome = queue.AttemptOutcomeRetry
		}

		_, err := s.db.ExecContext(context.Background(), `
INSERT INTO delivery_attempts (
  id, event_id, route, target, attempt, status_code, error, outcome, dead_reason, created_at
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
`,
			attempt.ID,
			attempt.EventID,
			attempt.Route,
			attempt.Target,
			attempt.Attempt,
			nullInt(attempt.StatusCode),
			nullIfEmpty(attempt.Error),
			string(attempt.Outcome),
			nullIfEmpty(attempt.DeadReason),
			attempt.CreatedAt,
		)
		if err != nil {
			return mapPostgresInsertError(err)
		}
		return nil
	})
}

func (s *Store) ListAttempts(req queue.AttemptListRequest) (queue.AttemptListResponse, error) {
	return runPostgresStoreOperationResult(s, "list_attempts", func() (queue.AttemptListResponse, error) {
		limit := req.Limit
		if limit <= 0 {
			limit = 100
		}
		if limit > postgresMaxListLimit {
			limit = postgresMaxListLimit
		}

		query := `
SELECT id, event_id, route, target, attempt, status_code, error, outcome, dead_reason, created_at
FROM delivery_attempts
`
		args := make([]any, 0, 8)
		where := make([]string, 0, 6)

		if req.Route != "" {
			args = append(args, req.Route)
			where = append(where, fmt.Sprintf("route = $%d", len(args)))
		}
		if req.Target != "" {
			args = append(args, req.Target)
			where = append(where, fmt.Sprintf("target = $%d", len(args)))
		}
		if req.EventID != "" {
			args = append(args, req.EventID)
			where = append(where, fmt.Sprintf("event_id = $%d", len(args)))
		}
		if req.Outcome != "" {
			args = append(args, string(req.Outcome))
			where = append(where, fmt.Sprintf("outcome = $%d", len(args)))
		}
		if !req.Before.IsZero() {
			args = append(args, req.Before.UTC())
			where = append(where, fmt.Sprintf("created_at < $%d", len(args)))
		}
		if len(where) > 0 {
			query += " WHERE " + strings.Join(where, " AND ")
		}

		args = append(args, limit)
		query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))

		rows, err := s.db.QueryContext(context.Background(), query, args...)
		if err != nil {
			return queue.AttemptListResponse{}, err
		}
		defer rows.Close()

		items := make([]queue.DeliveryAttempt, 0, clampSliceCap(limit, postgresMaxListLimit))
		for rows.Next() {
			var (
				attempt    queue.DeliveryAttempt
				statusCode sql.NullInt64
				errText    sql.NullString
				outcome    string
				deadReason sql.NullString
			)
			if err := rows.Scan(
				&attempt.ID,
				&attempt.EventID,
				&attempt.Route,
				&attempt.Target,
				&attempt.Attempt,
				&statusCode,
				&errText,
				&outcome,
				&deadReason,
				&attempt.CreatedAt,
			); err != nil {
				return queue.AttemptListResponse{}, err
			}
			if statusCode.Valid {
				attempt.StatusCode = int(statusCode.Int64)
			}
			if errText.Valid {
				attempt.Error = errText.String
			}
			attempt.Outcome = queue.AttemptOutcome(outcome)
			if deadReason.Valid {
				attempt.DeadReason = deadReason.String
			}
			attempt.CreatedAt = attempt.CreatedAt.UTC()
			items = append(items, attempt)
		}
		if err := rows.Err(); err != nil {
			return queue.AttemptListResponse{}, err
		}

		return queue.AttemptListResponse{Items: items}, nil
	})
}

func (s *Store) RuntimeMetrics() queue.StoreRuntimeMetrics {
	out := queue.StoreRuntimeMetrics{Backend: "postgres"}
	if s == nil || s.metrics == nil {
		return out
	}

	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	out.Common.OperationDurationSeconds = make([]queue.StoreOperationDurationRuntimeMetric, 0, len(s.metrics.operationDuration))
	for operation, duration := range s.metrics.operationDuration {
		out.Common.OperationDurationSeconds = append(
			out.Common.OperationDurationSeconds,
			queue.StoreOperationDurationRuntimeMetric{
				Operation:       operation,
				DurationSeconds: duration.snapshot(),
			},
		)
	}

	out.Common.OperationTotal = make([]queue.StoreOperationCounterRuntimeMetric, 0, len(s.metrics.operationTotal))
	for operation, total := range s.metrics.operationTotal {
		out.Common.OperationTotal = append(
			out.Common.OperationTotal,
			queue.StoreOperationCounterRuntimeMetric{
				Operation: operation,
				Total:     total,
			},
		)
	}

	for operation, byKind := range s.metrics.errorsTotal {
		for kind, total := range byKind {
			out.Common.ErrorsTotal = append(
				out.Common.ErrorsTotal,
				queue.StoreOperationErrorRuntimeMetric{
					Operation: operation,
					Kind:      kind,
					Total:     total,
				},
			)
		}
	}

	return out
}

func (s *Store) runStoreOperation(operation string, fn func() error) error {
	startedAt := time.Now()
	err := fn()
	s.observeStoreOperation(operation, startedAt, err)
	return err
}

func (s *Store) observeStoreOperation(operation string, startedAt time.Time, err error) {
	if s == nil || s.metrics == nil {
		return
	}
	operation = strings.TrimSpace(operation)
	if operation == "" {
		return
	}

	seconds := 0.0
	if !startedAt.IsZero() {
		seconds = time.Since(startedAt).Seconds()
		if seconds < 0 {
			seconds = 0
		}
	}

	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	hist := s.metrics.operationDuration[operation]
	if hist.counts == nil {
		hist = newSQLiteHistogram(sqliteDurationHistogramBounds)
	}
	hist.observe(seconds)
	s.metrics.operationDuration[operation] = hist

	s.metrics.operationTotal[operation]++

	if err == nil {
		return
	}

	kind := postgresMetricErrorKind(err)
	if kind == "" {
		return
	}
	if s.metrics.errorsTotal[operation] == nil {
		s.metrics.errorsTotal[operation] = make(map[string]int64)
	}
	s.metrics.errorsTotal[operation][kind]++
}

func postgresMetricErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, queue.ErrQueueFull):
		return "queue_full"
	case errors.Is(err, queue.ErrEnvelopeExists):
		return "duplicate"
	case errors.Is(err, queue.ErrLeaseNotFound):
		return "lease_not_found"
	case errors.Is(err, queue.ErrLeaseExpired):
		return "lease_expired"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001":
			return "serialization_failure"
		case "40P01":
			return "deadlock"
		case "23505":
			return "unique_violation"
		default:
			return "pg_" + pgErr.Code
		}
	}

	return "other"
}

func runPostgresStoreOperationResult[T any](s *Store, operation string, fn func() (T, error)) (T, error) {
	startedAt := time.Now()
	out, err := fn()
	if s != nil {
		s.observeStoreOperation(operation, startedAt, err)
	}
	return out, err
}

func (s *Store) withLease(leaseID string, now time.Time, fn func(ctx context.Context, tx *sql.Tx, itemID string, leaseUntil time.Time) error) error {
	leaseID = strings.TrimSpace(leaseID)
	if leaseID == "" {
		return queue.ErrLeaseNotFound
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback()
	}()

	var (
		itemID     string
		state      string
		leaseUntil sql.NullTime
	)
	err = tx.QueryRowContext(ctx, `
SELECT id, state, lease_until
FROM queue_items
WHERE lease_id = $1
LIMIT 1
FOR UPDATE
`,
		leaseID,
	).Scan(&itemID, &state, &leaseUntil)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return queue.ErrLeaseNotFound
		}
		return err
	}
	if state != string(queue.StateLeased) {
		return queue.ErrLeaseNotFound
	}

	if leaseUntil.Valid {
		until := leaseUntil.Time.UTC()
		if !now.Before(until) {
			if err := s.requeueLeaseTx(ctx, tx, itemID, now); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			committed = true
			return queue.ErrLeaseExpired
		}
		if err := fn(ctx, tx, itemID, until); err != nil {
			return err
		}
	} else {
		if err := fn(ctx, tx, itemID, now); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *Store) selectMessageIDsByFilter(req queue.MessageManageFilterRequest, allowed []queue.State) ([]string, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > postgresMaxListLimit {
		limit = postgresMaxListLimit
	}

	allowedSet := make(map[queue.State]struct{}, len(allowed))
	for _, st := range allowed {
		allowedSet[st] = struct{}{}
	}

	states := make([]queue.State, 0, len(allowed))
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
WHERE 1 = 1
`
	args := make([]any, 0, 6)
	if req.Route != "" {
		query += fmt.Sprintf(" AND route = $%d", len(args)+1)
		args = append(args, req.Route)
	}
	if req.Target != "" {
		query += fmt.Sprintf(" AND target = $%d", len(args)+1)
		args = append(args, req.Target)
	}
	if !req.Before.IsZero() {
		query += fmt.Sprintf(" AND received_at < $%d", len(args)+1)
		args = append(args, req.Before.UTC())
	}
	stateStrings := make([]string, 0, len(states))
	for _, st := range states {
		stateStrings = append(stateStrings, string(st))
	}
	query += fmt.Sprintf(" AND state = ANY($%d)", len(args)+1)
	args = append(args, stateStrings)

	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY received_at DESC, id DESC LIMIT $%d", len(args))

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0, clampSliceCap(limit, postgresMaxListLimit))
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

func (s *Store) requeueExpiredLeasesTx(ctx context.Context, tx *sql.Tx, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE state = $3
  AND lease_until IS NOT NULL
  AND lease_until <= $2
`,
		string(queue.StateQueued),
		now.UTC(),
		string(queue.StateLeased),
	)
	return err
}

func (s *Store) requeueLeaseTx(ctx context.Context, tx *sql.Tx, itemID string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE id = $3
`,
		string(queue.StateQueued),
		now.UTC(),
		itemID,
	)
	return err
}

func (s *Store) activeCount() (int, error) {
	var count int
	err := s.db.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM queue_items
WHERE state = $1 OR state = $2
`,
		string(queue.StateQueued),
		string(queue.StateLeased),
	).Scan(&count)
	return count, err
}

func (s *Store) dropOldestQueued() (bool, error) {
	res, err := s.db.ExecContext(context.Background(), `
DELETE FROM queue_items
WHERE id = (
  SELECT id
  FROM queue_items
  WHERE state = $1
  ORDER BY received_at ASC, id ASC
  LIMIT 1
)
`,
		string(queue.StateQueued),
	)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) maybePrune(now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.retentionMaxAge <= 0 && s.deliveredRetentionMaxAge <= 0 && s.dlqRetentionMaxAge <= 0 && s.dlqMaxDepth <= 0 {
		return nil
	}
	if s.pruneInterval <= 0 {
		return nil
	}
	if !s.lastPrune.IsZero() && now.Before(s.lastPrune.Add(s.pruneInterval)) {
		return nil
	}

	ctx := context.Background()
	if s.retentionMaxAge > 0 {
		cutoff := now.Add(-s.retentionMaxAge).UTC()
		if _, err := s.db.ExecContext(ctx, `
DELETE FROM queue_items
WHERE state = $1
  AND received_at < $2
`,
			string(queue.StateQueued),
			cutoff,
		); err != nil {
			return err
		}
	}
	if s.deliveredRetentionMaxAge > 0 {
		cutoff := now.Add(-s.deliveredRetentionMaxAge).UTC()
		if _, err := s.db.ExecContext(ctx, `
DELETE FROM queue_items
WHERE state = $1
  AND received_at < $2
`,
			string(queue.StateDelivered),
			cutoff,
		); err != nil {
			return err
		}
	}
	if s.dlqRetentionMaxAge > 0 {
		cutoff := now.Add(-s.dlqRetentionMaxAge).UTC()
		if _, err := s.db.ExecContext(ctx, `
DELETE FROM queue_items
WHERE state = $1
  AND received_at < $2
`,
			string(queue.StateDead),
			cutoff,
		); err != nil {
			return err
		}
	}
	if s.dlqMaxDepth > 0 {
		if _, err := s.db.ExecContext(ctx, `
DELETE FROM queue_items
WHERE id IN (
  SELECT id
  FROM queue_items
  WHERE state = $1
  ORDER BY received_at DESC, id DESC
  OFFSET $2
)
`,
			string(queue.StateDead),
			s.dlqMaxDepth,
		); err != nil {
			return err
		}
	}
	s.lastPrune = now
	return nil
}

func (s *Store) now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nowFn().UTC()
}

func mapPostgresInsertError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return queue.ErrEnvelopeExists
	}
	return err
}

func encodeStringMapJSON(in map[string]string) ([]byte, error) {
	if len(in) == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(in)
}

func decodeStringMapJSON(raw []byte) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]string
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func scanQueueItem(rows *sql.Rows) (queue.Envelope, error) {
	var (
		item        queue.Envelope
		state       string
		headersJSON []byte
		traceJSON   []byte
		deadReason  sql.NullString
		leaseID     sql.NullString
		leaseUntil  sql.NullTime
	)
	if err := rows.Scan(
		&item.ID,
		&item.Route,
		&item.Target,
		&state,
		&item.ReceivedAt,
		&item.Attempt,
		&item.NextRunAt,
		&item.Payload,
		&headersJSON,
		&traceJSON,
		&deadReason,
		&item.SchemaVersion,
		&leaseID,
		&leaseUntil,
	); err != nil {
		return queue.Envelope{}, err
	}
	item.State = queue.State(state)
	item.ReceivedAt = item.ReceivedAt.UTC()
	item.NextRunAt = item.NextRunAt.UTC()
	item.Headers = decodeStringMapJSON(headersJSON)
	item.Trace = decodeStringMapJSON(traceJSON)
	if deadReason.Valid {
		item.DeadReason = deadReason.String
	}
	if leaseID.Valid {
		item.LeaseID = leaseID.String
	}
	if leaseUntil.Valid {
		item.LeaseUntil = leaseUntil.Time.UTC()
	}
	return item, nil
}

func nullIfEmpty(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func nullTime(v time.Time) any {
	if v.IsZero() {
		return nil
	}
	return v.UTC()
}

func nullInt(v int) any {
	if v == 0 {
		return nil
	}
	return v
}

func clampSliceCap(size, max int) int {
	if size <= 0 || max <= 0 {
		return 0
	}
	if size > max {
		return max
	}
	return size
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

func newHexID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

const statsTopBacklogLimit = 10

const backlogTrendRetention = 30 * 24 * time.Hour

// sqliteHistogram is a simple histogram used for operation duration tracking.
// The name is historical; it is used by both SQLite and Postgres backends.
type sqliteHistogram struct {
	bounds []float64
	counts []int64
	count  int64
	sum    float64
}

var sqliteDurationHistogramBounds = []float64{
	0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

func newSQLiteHistogram(bounds []float64) sqliteHistogram {
	sorted := make([]float64, len(bounds))
	copy(sorted, bounds)
	return sqliteHistogram{
		bounds: sorted,
		counts: make([]int64, len(sorted)+1),
	}
}

func (h *sqliteHistogram) observe(seconds float64) {
	if h.counts == nil {
		return
	}
	h.count++
	h.sum += seconds
	for i, bound := range h.bounds {
		if seconds <= bound {
			h.counts[i]++
			return
		}
	}
	h.counts[len(h.bounds)]++
}

func (h *sqliteHistogram) snapshot() queue.HistogramSnapshot {
	if h.counts == nil {
		return queue.HistogramSnapshot{}
	}
	snap := queue.HistogramSnapshot{
		Count:   h.count,
		Sum:     h.sum,
		Buckets: make([]queue.HistogramBucket, 0, len(h.bounds)),
	}
	var cumulative int64
	for i, bound := range h.bounds {
		cumulative += h.counts[i]
		snap.Buckets = append(snap.Buckets, queue.HistogramBucket{
			Le:    bound,
			Count: cumulative,
		})
	}
	return snap
}

// backend implements hookaido.QueueBackend for the PostgreSQL queue.
type backend struct{}

func (backend) Name() string { return "postgres" }

func (backend) OpenStore(cfg hookaido.QueueBackendConfig) (any, func() error, error) {
	store, err := NewStore(
		cfg.DSN,
		WithQueueLimits(cfg.QueueMaxDepth, cfg.QueueDropPolicy),
		WithRetention(cfg.RetentionMaxAge, cfg.RetentionPruneInterval),
		WithDeliveredRetention(cfg.DeliveredRetentionMaxAge),
		WithDLQRetention(cfg.DLQRetentionMaxAge, cfg.DLQRetentionMaxDepth),
	)
	if err != nil {
		return nil, nil, err
	}
	return store, store.Close, nil
}

func init() {
	hookaido.RegisterQueueBackend(backend{})
}
