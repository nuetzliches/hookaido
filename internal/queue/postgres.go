package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresOption func(*PostgresStore)

type PostgresStore struct {
	db *sql.DB

	mu                       sync.Mutex
	nowFn                    func() time.Time
	pollInterval             time.Duration
	maxDepth                 int
	dropPolicy               string
	retentionMaxAge          time.Duration
	pruneInterval            time.Duration
	lastPrune                time.Time
	deliveredRetentionMaxAge time.Duration
	dlqRetentionMaxAge       time.Duration
	dlqMaxDepth              int
}

var _ Store = (*PostgresStore)(nil)

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
`

func WithPostgresNowFunc(now func() time.Time) PostgresOption {
	return func(s *PostgresStore) {
		if now != nil {
			s.nowFn = now
		}
	}
}

func WithPostgresPollInterval(d time.Duration) PostgresOption {
	return func(s *PostgresStore) {
		if d > 0 {
			s.pollInterval = d
		}
	}
}

func WithPostgresQueueLimits(maxDepth int, dropPolicy string) PostgresOption {
	return func(s *PostgresStore) {
		if maxDepth >= 0 {
			s.maxDepth = maxDepth
		}
		if strings.TrimSpace(dropPolicy) != "" {
			s.dropPolicy = strings.ToLower(strings.TrimSpace(dropPolicy))
		}
	}
}

func WithPostgresRetention(maxAge, pruneInterval time.Duration) PostgresOption {
	return func(s *PostgresStore) {
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

func WithPostgresDeliveredRetention(maxAge time.Duration) PostgresOption {
	return func(s *PostgresStore) {
		if maxAge > 0 {
			s.deliveredRetentionMaxAge = maxAge
		} else {
			s.deliveredRetentionMaxAge = 0
		}
	}
}

func WithPostgresDLQRetention(maxAge time.Duration, maxDepth int) PostgresOption {
	return func(s *PostgresStore) {
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

func NewPostgresStore(dsn string, opts ...PostgresOption) (*PostgresStore, error) {
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

	s := &PostgresStore{
		db:           db,
		nowFn:        time.Now,
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

func (s *PostgresStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *PostgresStore) init() error {
	_, err := s.db.ExecContext(context.Background(), postgresSchemaV1)
	return err
}

func (s *PostgresStore) Enqueue(env Envelope) error {
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
		env.State = StateQueued
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
					return ErrQueueFull
				}
			default:
				return ErrQueueFull
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
	return nil
}

func (s *PostgresStore) Dequeue(req DequeueRequest) (DequeueResponse, error) {
	if s == nil || s.db == nil {
		return DequeueResponse{}, errors.New("postgres store is closed")
	}

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
		sleep := remaining
		if s.pollInterval > 0 && sleep > s.pollInterval {
			sleep = s.pollInterval
		}
		time.Sleep(sleep)
	}
}

func (s *PostgresStore) dequeueOnce(req DequeueRequest, batch int, leaseTTL time.Duration) (DequeueResponse, error) {
	now := req.Now
	if now.IsZero() {
		now = s.now()
	}
	now = now.UTC()
	leaseUntil := now.Add(leaseTTL).UTC()

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DequeueResponse{}, err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback()
	}()

	if err := s.requeueExpiredLeasesTx(ctx, tx, now); err != nil {
		return DequeueResponse{}, err
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
		string(StateQueued),
		now,
		batch,
	)
	if err != nil {
		return DequeueResponse{}, err
	}
	defer rows.Close()

	items := make([]Envelope, 0, batch)
	for rows.Next() {
		var (
			item        Envelope
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
			return DequeueResponse{}, err
		}
		item.State = State(state)
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
		return DequeueResponse{}, err
	}

	if len(items) == 0 {
		if err := tx.Commit(); err != nil {
			return DequeueResponse{}, err
		}
		committed = true
		return DequeueResponse{}, nil
	}

	for i := range items {
		leaseID := newHexID("lease_")
		_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, attempt = attempt + 1, lease_id = $2, lease_until = $3, next_run_at = $3, dead_reason = NULL
WHERE id = $4
  AND state = $5
`,
			string(StateLeased),
			leaseID,
			leaseUntil,
			items[i].ID,
			string(StateQueued),
		)
		if err != nil {
			return DequeueResponse{}, err
		}
		items[i].State = StateLeased
		items[i].Attempt++
		items[i].LeaseID = leaseID
		items[i].LeaseUntil = leaseUntil
		items[i].NextRunAt = leaseUntil
		items[i].DeadReason = ""
	}

	if err := tx.Commit(); err != nil {
		return DequeueResponse{}, err
	}
	committed = true
	return DequeueResponse{Items: items}, nil
}

func (s *PostgresStore) Ack(leaseID string) error {
	now := s.now().UTC()
	return s.withLease(leaseID, now, func(ctx context.Context, tx *sql.Tx, itemID string, _ time.Time) error {
		if s.deliveredRetentionMaxAge > 0 {
			_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE id = $3
  AND state = $4
`,
				string(StateDelivered),
				now,
				itemID,
				string(StateLeased),
			)
			return err
		}

		_, err := tx.ExecContext(ctx, `
DELETE FROM queue_items
WHERE id = $1
  AND state = $2
`,
			itemID,
			string(StateLeased),
		)
		return err
	})
}

func (s *PostgresStore) AckBatch(leaseIDs []string) (LeaseBatchResult, error) {
	res := LeaseBatchResult{Conflicts: make([]LeaseBatchConflict, 0)}

	for _, rawLeaseID := range leaseIDs {
		leaseID := strings.TrimSpace(rawLeaseID)
		if leaseID == "" {
			res.Conflicts = append(res.Conflicts, LeaseBatchConflict{LeaseID: rawLeaseID})
			continue
		}

		err := s.Ack(leaseID)
		if err == nil {
			res.Succeeded++
			continue
		}
		switch {
		case errors.Is(err, ErrLeaseExpired):
			res.Conflicts = append(res.Conflicts, LeaseBatchConflict{
				LeaseID: leaseID,
				Expired: true,
			})
		case errors.Is(err, ErrLeaseNotFound):
			res.Conflicts = append(res.Conflicts, LeaseBatchConflict{LeaseID: leaseID})
		default:
			return LeaseBatchResult{}, err
		}
	}

	return res, nil
}

func (s *PostgresStore) Nack(leaseID string, delay time.Duration) error {
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
			string(StateQueued),
			now.Add(delay).UTC(),
			itemID,
			string(StateLeased),
		)
		return err
	})
}

func (s *PostgresStore) NackBatch(leaseIDs []string, delay time.Duration) (LeaseBatchResult, error) {
	res := LeaseBatchResult{Conflicts: make([]LeaseBatchConflict, 0)}

	for _, rawLeaseID := range leaseIDs {
		leaseID := strings.TrimSpace(rawLeaseID)
		if leaseID == "" {
			res.Conflicts = append(res.Conflicts, LeaseBatchConflict{LeaseID: rawLeaseID})
			continue
		}

		err := s.Nack(leaseID, delay)
		if err == nil {
			res.Succeeded++
			continue
		}
		switch {
		case errors.Is(err, ErrLeaseExpired):
			res.Conflicts = append(res.Conflicts, LeaseBatchConflict{
				LeaseID: leaseID,
				Expired: true,
			})
		case errors.Is(err, ErrLeaseNotFound):
			res.Conflicts = append(res.Conflicts, LeaseBatchConflict{LeaseID: leaseID})
		default:
			return LeaseBatchResult{}, err
		}
	}

	return res, nil
}

func (s *PostgresStore) Extend(leaseID string, extendBy time.Duration) error {
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
			string(StateLeased),
		)
		return err
	})
}

func (s *PostgresStore) MarkDead(leaseID string, reason string) error {
	now := s.now().UTC()
	return s.withLease(leaseID, now, func(ctx context.Context, tx *sql.Tx, itemID string, _ time.Time) error {
		_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = $3
WHERE id = $4
  AND state = $5
`,
			string(StateDead),
			now,
			strings.TrimSpace(reason),
			itemID,
			string(StateLeased),
		)
		return err
	})
}

func (s *PostgresStore) MarkDeadBatch(leaseIDs []string, reason string) (LeaseBatchResult, error) {
	res := LeaseBatchResult{Conflicts: make([]LeaseBatchConflict, 0)}

	for _, rawLeaseID := range leaseIDs {
		leaseID := strings.TrimSpace(rawLeaseID)
		if leaseID == "" {
			res.Conflicts = append(res.Conflicts, LeaseBatchConflict{LeaseID: rawLeaseID})
			continue
		}

		err := s.MarkDead(leaseID, reason)
		if err == nil {
			res.Succeeded++
			continue
		}
		switch {
		case errors.Is(err, ErrLeaseExpired):
			res.Conflicts = append(res.Conflicts, LeaseBatchConflict{
				LeaseID: leaseID,
				Expired: true,
			})
		case errors.Is(err, ErrLeaseNotFound):
			res.Conflicts = append(res.Conflicts, LeaseBatchConflict{LeaseID: leaseID})
		default:
			return LeaseBatchResult{}, err
		}
	}

	return res, nil
}

func (s *PostgresStore) ListDead(req DeadListRequest) (DeadListResponse, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	args := make([]any, 0, 4)
	var where []string
	where = append(where, "state = $1")
	args = append(args, string(StateDead))
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
		return DeadListResponse{}, err
	}
	defer rows.Close()

	items := make([]Envelope, 0, limit)
	for rows.Next() {
		item, err := scanQueueItem(rows)
		if err != nil {
			return DeadListResponse{}, err
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
		return DeadListResponse{}, err
	}

	return DeadListResponse{Items: items}, nil
}

func (s *PostgresStore) RequeueDead(req DeadRequeueRequest) (DeadRequeueResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return DeadRequeueResponse{}, nil
	}

	now := s.now().UTC()
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DeadRequeueResponse{}, err
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
			string(StateQueued),
			now,
			id,
			string(StateDead),
		)
		if err != nil {
			return DeadRequeueResponse{}, err
		}
		n, _ := res.RowsAffected()
		requeued += int(n)
	}

	if err := tx.Commit(); err != nil {
		return DeadRequeueResponse{}, err
	}
	committed = true
	return DeadRequeueResponse{Requeued: requeued}, nil
}

func (s *PostgresStore) DeleteDead(req DeadDeleteRequest) (DeadDeleteResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return DeadDeleteResponse{}, nil
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DeadDeleteResponse{}, err
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
			string(StateDead),
		)
		if err != nil {
			return DeadDeleteResponse{}, err
		}
		n, _ := res.RowsAffected()
		deleted += int(n)
	}

	if err := tx.Commit(); err != nil {
		return DeadDeleteResponse{}, err
	}
	committed = true
	return DeadDeleteResponse{Deleted: deleted}, nil
}

func (s *PostgresStore) ListMessages(req MessageListRequest) (MessageListResponse, error) {
	if s == nil || s.db == nil {
		return MessageListResponse{}, errors.New("postgres store is closed")
	}
	if err := s.maybePrune(s.now()); err != nil {
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
		return MessageListResponse{}, err
	}
	defer rows.Close()

	items := make([]Envelope, 0, limit)
	for rows.Next() {
		item, err := scanQueueItem(rows)
		if err != nil {
			return MessageListResponse{}, err
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
		return MessageListResponse{}, err
	}

	return MessageListResponse{Items: items}, nil
}

func (s *PostgresStore) LookupMessages(req MessageLookupRequest) (MessageLookupResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return MessageLookupResponse{}, nil
	}

	rows, err := s.db.QueryContext(context.Background(), `
SELECT id, route, state
FROM queue_items
WHERE id = ANY($1)
`,
		ids,
	)
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

	items := make([]MessageLookupItem, 0, len(ids))
	for _, id := range ids {
		item, ok := byID[id]
		if !ok {
			continue
		}
		items = append(items, item)
	}
	return MessageLookupResponse{Items: items}, nil
}

func (s *PostgresStore) CancelMessages(req MessageCancelRequest) (MessageCancelResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return MessageCancelResponse{}, nil
	}

	res, err := s.db.ExecContext(context.Background(), `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE state = ANY($3)
  AND id = ANY($4)
`,
		string(StateCanceled),
		s.now().UTC(),
		[]string{string(StateQueued), string(StateLeased), string(StateDead)},
		ids,
	)
	if err != nil {
		return MessageCancelResponse{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return MessageCancelResponse{}, err
	}
	return MessageCancelResponse{Canceled: int(n), Matched: int(n)}, nil
}

func (s *PostgresStore) RequeueMessages(req MessageRequeueRequest) (MessageRequeueResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return MessageRequeueResponse{}, nil
	}

	res, err := s.db.ExecContext(context.Background(), `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE state = ANY($3)
  AND id = ANY($4)
`,
		string(StateQueued),
		s.now().UTC(),
		[]string{string(StateDead), string(StateCanceled)},
		ids,
	)
	if err != nil {
		return MessageRequeueResponse{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return MessageRequeueResponse{}, err
	}
	return MessageRequeueResponse{Requeued: int(n), Matched: int(n)}, nil
}

func (s *PostgresStore) ResumeMessages(req MessageResumeRequest) (MessageResumeResponse, error) {
	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return MessageResumeResponse{}, nil
	}

	res, err := s.db.ExecContext(context.Background(), `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE state = $3
  AND id = ANY($4)
`,
		string(StateQueued),
		s.now().UTC(),
		string(StateCanceled),
		ids,
	)
	if err != nil {
		return MessageResumeResponse{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return MessageResumeResponse{}, err
	}
	return MessageResumeResponse{Resumed: int(n), Matched: int(n)}, nil
}

func (s *PostgresStore) CancelMessagesByFilter(req MessageManageFilterRequest) (MessageCancelResponse, error) {
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

func (s *PostgresStore) RequeueMessagesByFilter(req MessageManageFilterRequest) (MessageRequeueResponse, error) {
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

func (s *PostgresStore) ResumeMessagesByFilter(req MessageManageFilterRequest) (MessageResumeResponse, error) {
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

func (s *PostgresStore) Stats() (Stats, error) {
	stats := Stats{
		ByState: map[State]int{
			StateQueued:    0,
			StateLeased:    0,
			StateDead:      0,
			StateDelivered: 0,
			StateCanceled:  0,
		},
		TopQueued: make([]BacklogBucket, 0),
	}

	rows, err := s.db.QueryContext(context.Background(), `
SELECT state, COUNT(*)
FROM queue_items
GROUP BY state
`)
	if err != nil {
		return Stats{}, err
	}
	defer rows.Close()

	total := 0
	for rows.Next() {
		var (
			state string
			count int
		)
		if err := rows.Scan(&state, &count); err != nil {
			return Stats{}, err
		}
		st := State(state)
		stats.ByState[st] = count
		total += count
	}
	if err := rows.Err(); err != nil {
		return Stats{}, err
	}
	stats.Total = total

	now := s.now().UTC()

	var oldestQueued sql.NullTime
	if err := s.db.QueryRowContext(context.Background(), `
SELECT MIN(received_at)
FROM queue_items
WHERE state = $1
`,
		string(StateQueued),
	).Scan(&oldestQueued); err != nil {
		return Stats{}, err
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
		string(StateQueued),
	).Scan(&earliestReady); err != nil {
		return Stats{}, err
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
		string(StateQueued),
		statsTopBacklogLimit,
	)
	if err != nil {
		return Stats{}, err
	}
	defer topRows.Close()

	for topRows.Next() {
		var b BacklogBucket
		if err := topRows.Scan(&b.Route, &b.Target, &b.Queued); err != nil {
			return Stats{}, err
		}
		stats.TopQueued = append(stats.TopQueued, b)
	}
	if err := topRows.Err(); err != nil {
		return Stats{}, err
	}

	return stats, nil
}

func (s *PostgresStore) RecordAttempt(attempt DeliveryAttempt) error {
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
}

func (s *PostgresStore) ListAttempts(req AttemptListRequest) (AttemptListResponse, error) {
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
		return AttemptListResponse{}, err
	}
	defer rows.Close()

	items := make([]DeliveryAttempt, 0, limit)
	for rows.Next() {
		var (
			attempt    DeliveryAttempt
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
			return AttemptListResponse{}, err
		}
		if statusCode.Valid {
			attempt.StatusCode = int(statusCode.Int64)
		}
		if errText.Valid {
			attempt.Error = errText.String
		}
		attempt.Outcome = AttemptOutcome(outcome)
		if deadReason.Valid {
			attempt.DeadReason = deadReason.String
		}
		attempt.CreatedAt = attempt.CreatedAt.UTC()
		items = append(items, attempt)
	}
	if err := rows.Err(); err != nil {
		return AttemptListResponse{}, err
	}

	return AttemptListResponse{Items: items}, nil
}

func (s *PostgresStore) RuntimeMetrics() StoreRuntimeMetrics {
	return StoreRuntimeMetrics{Backend: "postgres"}
}

func (s *PostgresStore) withLease(leaseID string, now time.Time, fn func(ctx context.Context, tx *sql.Tx, itemID string, leaseUntil time.Time) error) error {
	leaseID = strings.TrimSpace(leaseID)
	if leaseID == "" {
		return ErrLeaseNotFound
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
			return ErrLeaseNotFound
		}
		return err
	}
	if state != string(StateLeased) {
		return ErrLeaseNotFound
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
			return ErrLeaseExpired
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

func (s *PostgresStore) selectMessageIDsByFilter(req MessageManageFilterRequest, allowed []State) ([]string, error) {
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

func (s *PostgresStore) requeueExpiredLeasesTx(ctx context.Context, tx *sql.Tx, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE state = $3
  AND lease_until IS NOT NULL
  AND lease_until <= $2
`,
		string(StateQueued),
		now.UTC(),
		string(StateLeased),
	)
	return err
}

func (s *PostgresStore) requeueLeaseTx(ctx context.Context, tx *sql.Tx, itemID string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
UPDATE queue_items
SET state = $1, lease_id = NULL, lease_until = NULL, next_run_at = $2, dead_reason = NULL
WHERE id = $3
`,
		string(StateQueued),
		now.UTC(),
		itemID,
	)
	return err
}

func (s *PostgresStore) activeCount() (int, error) {
	var count int
	err := s.db.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM queue_items
WHERE state = $1 OR state = $2
`,
		string(StateQueued),
		string(StateLeased),
	).Scan(&count)
	return count, err
}

func (s *PostgresStore) dropOldestQueued() (bool, error) {
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
		string(StateQueued),
	)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *PostgresStore) maybePrune(now time.Time) error {
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
			string(StateQueued),
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
			string(StateDelivered),
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
			string(StateDead),
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
			string(StateDead),
			s.dlqMaxDepth,
		); err != nil {
			return err
		}
	}
	s.lastPrune = now
	return nil
}

func (s *PostgresStore) now() time.Time {
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
		return ErrEnvelopeExists
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

func scanQueueItem(rows *sql.Rows) (Envelope, error) {
	var (
		item        Envelope
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
		return Envelope{}, err
	}
	item.State = State(state)
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
