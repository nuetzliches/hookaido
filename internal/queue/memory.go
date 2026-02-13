package queue

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrLeaseNotFound  = errors.New("lease not found")
	ErrLeaseExpired   = errors.New("lease expired")
	ErrQueueFull      = errors.New("queue full")
	ErrMemoryPressure = errors.New("memory pressure")
	ErrEnvelopeExists = errors.New("envelope already exists")
)

type MemoryOption func(*MemoryStore)

func WithNowFunc(now func() time.Time) MemoryOption {
	return func(s *MemoryStore) {
		if now != nil {
			s.nowFn = now
		}
	}
}

type MemoryStore struct {
	mu                       sync.Mutex
	nowFn                    func() time.Time
	items                    map[string]*Envelope
	attempts                 []DeliveryAttempt
	trendRows                []backlogTrendRow
	order                    []string
	leases                   map[string]string // lease_id -> item_id
	notify                   chan struct{}
	maxDepth                 int
	dropPolicy               string
	retentionMaxAge          time.Duration
	pruneInterval            time.Duration
	lastPrune                time.Time
	deliveredRetentionMaxAge time.Duration
	dlqRetentionMaxAge       time.Duration
	dlqMaxDepth              int
	evictionsTotalByReason   map[string]int64
	memoryPressureRejects    int64
	memoryPressureItemLimit  int
	memoryPressureBytesLimit int64
}

type backlogTrendRow struct {
	CapturedAt time.Time
	Route      string
	Target     string
	Queued     int
	Leased     int
	Dead       int
}

const backlogTrendMaxRows = 200000

const (
	memoryEvictionReasonDropOldest         = "drop_oldest"
	memoryEvictionReasonQueueRetentionAge  = "queue_retention_age"
	memoryEvictionReasonDeliveredRetention = "delivered_retention_age"
	memoryEvictionReasonDeadRetentionAge   = "dead_retention_age"
	memoryEvictionReasonDeadRetentionDepth = "dead_retention_depth"

	defaultMemoryPressureRetainedItemFloor = 1000
	defaultMemoryPressureRetainedBytes     = 256 << 20 // 256 MiB
)

type memoryInventory struct {
	itemsByState         map[State]int64
	retainedBytesByState map[State]int64
	totalRetainedBytes   int64
	retainedItems        int64
	retainedBytes        int64
}

func NewMemoryStore(opts ...MemoryOption) *MemoryStore {
	s := &MemoryStore{
		nowFn:                  time.Now,
		items:                  make(map[string]*Envelope),
		leases:                 make(map[string]string),
		notify:                 make(chan struct{}),
		dropPolicy:             "reject",
		evictionsTotalByReason: make(map[string]int64),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// WithMemoryPressureLimits overrides memory pressure limits for the in-memory
// backend. Positive values enable explicit thresholds.
func WithMemoryPressureLimits(retainedItems int, retainedBytes int64) MemoryOption {
	return func(s *MemoryStore) {
		if retainedItems > 0 {
			s.memoryPressureItemLimit = retainedItems
		}
		if retainedBytes > 0 {
			s.memoryPressureBytesLimit = retainedBytes
		}
	}
}

func WithQueueLimits(maxDepth int, dropPolicy string) MemoryOption {
	return func(s *MemoryStore) {
		if maxDepth >= 0 {
			s.maxDepth = maxDepth
		}
		if strings.TrimSpace(dropPolicy) != "" {
			s.dropPolicy = strings.ToLower(strings.TrimSpace(dropPolicy))
		}
	}
}

func WithQueueRetention(maxAge, pruneInterval time.Duration) MemoryOption {
	return func(s *MemoryStore) {
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

func WithDeliveredRetention(maxAge time.Duration) MemoryOption {
	return func(s *MemoryStore) {
		if maxAge > 0 {
			s.deliveredRetentionMaxAge = maxAge
		} else {
			s.deliveredRetentionMaxAge = 0
		}
	}
}

func WithDLQRetention(maxAge time.Duration, maxDepth int) MemoryOption {
	return func(s *MemoryStore) {
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

func (s *MemoryStore) Enqueue(env Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	s.maybePruneLocked(now)

	if s.maxDepth > 0 {
		activeCount := s.activeCountLocked()
		activeDeliveredCount := s.activeDeliveredCountLocked()
		for activeCount >= s.maxDepth || (s.deliveredRetentionMaxAge > 0 && activeDeliveredCount >= s.maxDepth) {
			if s.dropPolicy != "drop_oldest" {
				return ErrQueueFull
			}
			if !s.dropOldestQueuedLocked() {
				return ErrQueueFull
			}
			activeCount = s.activeCountLocked()
			activeDeliveredCount = s.activeDeliveredCountLocked()
		}
	}

	if pressure := s.memoryPressureStatusLocked(); pressure.Active {
		s.memoryPressureRejects++
		return ErrMemoryPressure
	}

	if env.ID == "" {
		env.ID = newHexID("evt_")
	}
	if _, exists := s.items[env.ID]; exists {
		return ErrEnvelopeExists
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
	if env.Headers != nil {
		env.Headers = cloneStringMap(env.Headers)
	}
	if env.Trace != nil {
		env.Trace = cloneStringMap(env.Trace)
	}

	cpy := env
	s.items[env.ID] = &cpy
	s.order = append(s.order, env.ID)

	// Wake up any long-polling Dequeue calls.
	close(s.notify)
	s.notify = make(chan struct{})

	return nil
}

// EnqueueBatch atomically enqueues all items or none (all-or-nothing).
// Returns the number of items enqueued (0 or len(items)) and an error.
func (s *MemoryStore) EnqueueBatch(items []Envelope) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	s.maybePruneLocked(now)

	// Pre-validate: check depth, duplicates, and prepare copies.
	activeCount := s.activeCountLocked()
	activeDeliveredCount := s.activeDeliveredCountLocked()
	needed := len(items)
	if s.maxDepth > 0 {
		if s.dropPolicy != "drop_oldest" {
			if activeCount+needed > s.maxDepth {
				return 0, ErrQueueFull
			}
			if s.deliveredRetentionMaxAge > 0 && activeDeliveredCount+needed > s.maxDepth {
				return 0, ErrQueueFull
			}
		}
	}

	prepared := make([]*Envelope, 0, needed)
	seenIDs := make(map[string]struct{}, needed)
	for i := range items {
		env := items[i]
		if env.ID == "" {
			env.ID = newHexID("evt_")
		}
		if _, dup := seenIDs[env.ID]; dup {
			return 0, ErrEnvelopeExists
		}
		seenIDs[env.ID] = struct{}{}
		if _, exists := s.items[env.ID]; exists {
			return 0, ErrEnvelopeExists
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
		if env.Headers != nil {
			env.Headers = cloneStringMap(env.Headers)
		}
		if env.Trace != nil {
			env.Trace = cloneStringMap(env.Trace)
		}
		cpy := env
		prepared = append(prepared, &cpy)
	}

	// Handle depth overflow with drop_oldest.
	if s.maxDepth > 0 {
		for activeCount+len(prepared) > s.maxDepth || (s.deliveredRetentionMaxAge > 0 && activeDeliveredCount+len(prepared) > s.maxDepth) {
			if !s.dropOldestQueuedLocked() {
				return 0, ErrQueueFull
			}
			activeCount = s.activeCountLocked()
			activeDeliveredCount = s.activeDeliveredCountLocked()
		}
	}

	if pressure := s.memoryPressureStatusLocked(); pressure.Active {
		s.memoryPressureRejects++
		return 0, ErrMemoryPressure
	}

	// Commit all items.
	for _, env := range prepared {
		s.items[env.ID] = env
		s.order = append(s.order, env.ID)
	}

	close(s.notify)
	s.notify = make(chan struct{})

	return len(prepared), nil
}

// activeCountLocked counts items in queued or leased state,
// matching the SQLite store's depth semantics.
func (s *MemoryStore) activeCountLocked() int {
	n := 0
	for _, env := range s.items {
		if env.State == StateQueued || env.State == StateLeased {
			n++
		}
	}
	return n
}

// activeDeliveredCountLocked counts queued, leased, and delivered items.
// For memory backend this protects against unbounded delivered-retention growth
// under sustained pull workloads.
func (s *MemoryStore) activeDeliveredCountLocked() int {
	n := 0
	for _, env := range s.items {
		if env.State == StateQueued || env.State == StateLeased || env.State == StateDelivered {
			n++
		}
	}
	return n
}

func (s *MemoryStore) evictLocked(id string, reason string) bool {
	env := s.items[id]
	if env == nil {
		return false
	}
	delete(s.items, id)
	if env.LeaseID != "" {
		delete(s.leases, env.LeaseID)
	}
	s.incEvictionLocked(reason)
	return true
}

func (s *MemoryStore) incEvictionLocked(reason string) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return
	}
	if s.evictionsTotalByReason == nil {
		s.evictionsTotalByReason = make(map[string]int64)
	}
	s.evictionsTotalByReason[reason]++
}

func (s *MemoryStore) memoryPressureStatusLocked() MemoryPressureRuntimeMetrics {
	inv := s.memoryInventoryLocked()
	itemLimit := int64(s.effectiveMemoryPressureItemLimitLocked())
	bytesLimit := s.effectiveMemoryPressureBytesLimitLocked()

	pressure := MemoryPressureRuntimeMetrics{
		Active:             false,
		Reason:             "",
		RetainedItems:      inv.retainedItems,
		RetainedBytes:      inv.retainedBytes,
		RetainedItemLimit:  itemLimit,
		RetainedBytesLimit: bytesLimit,
		RejectTotal:        s.memoryPressureRejects,
	}
	if itemLimit > 0 && inv.retainedItems >= itemLimit {
		pressure.Active = true
		pressure.Reason = "retained_items"
		return pressure
	}
	if bytesLimit > 0 && inv.retainedBytes >= bytesLimit {
		pressure.Active = true
		pressure.Reason = "retained_bytes"
		return pressure
	}
	return pressure
}

func (s *MemoryStore) effectiveMemoryPressureItemLimitLocked() int {
	if s.memoryPressureItemLimit > 0 {
		return s.memoryPressureItemLimit
	}
	if s.maxDepth <= 0 {
		return 0
	}
	if s.maxDepth < defaultMemoryPressureRetainedItemFloor {
		return defaultMemoryPressureRetainedItemFloor
	}
	return s.maxDepth
}

func (s *MemoryStore) effectiveMemoryPressureBytesLimitLocked() int64 {
	if s.memoryPressureBytesLimit > 0 {
		return s.memoryPressureBytesLimit
	}
	return defaultMemoryPressureRetainedBytes
}

func (s *MemoryStore) memoryInventoryLocked() memoryInventory {
	inv := memoryInventory{
		itemsByState:         make(map[State]int64),
		retainedBytesByState: make(map[State]int64),
	}
	for _, env := range s.items {
		if env == nil {
			continue
		}
		inv.itemsByState[env.State]++
		size := envelopeRetainedBytes(env)
		inv.retainedBytesByState[env.State] += size
		inv.totalRetainedBytes += size
		switch env.State {
		case StateDelivered, StateDead, StateCanceled:
			inv.retainedItems++
			inv.retainedBytes += size
		}
	}
	return inv
}

func envelopeRetainedBytes(env *Envelope) int64 {
	if env == nil {
		return 0
	}

	size := int64(0)
	size += int64(len(env.ID))
	size += int64(len(env.Route))
	size += int64(len(env.Target))
	size += int64(len(env.Payload))
	size += int64(len(env.DeadReason))
	size += int64(len(env.LeaseID))

	for k, v := range env.Headers {
		size += int64(len(k) + len(v))
	}
	for k, v := range env.Trace {
		size += int64(len(k) + len(v))
	}
	return size
}

func (s *MemoryStore) dropOldestQueuedLocked() bool {
	for _, id := range s.order {
		env := s.items[id]
		if env == nil {
			continue
		}
		if env.State != StateQueued {
			continue
		}
		return s.evictLocked(id, memoryEvictionReasonDropOldest)
	}
	return false
}

func (s *MemoryStore) maybePruneLocked(now time.Time) {
	if s.pruneInterval <= 0 {
		return
	}
	if s.retentionMaxAge <= 0 && s.deliveredRetentionMaxAge <= 0 && s.dlqRetentionMaxAge <= 0 && s.dlqMaxDepth <= 0 {
		return
	}
	if !s.lastPrune.IsZero() && now.Sub(s.lastPrune) < s.pruneInterval {
		return
	}

	if s.retentionMaxAge > 0 {
		cutoff := now.Add(-s.retentionMaxAge)
		for id, env := range s.items {
			if env == nil {
				continue
			}
			if env.State != StateQueued {
				continue
			}
			if env.ReceivedAt.IsZero() || env.ReceivedAt.After(cutoff) {
				continue
			}
			s.evictLocked(id, memoryEvictionReasonQueueRetentionAge)
		}
	}

	if s.dlqRetentionMaxAge > 0 {
		cutoff := now.Add(-s.dlqRetentionMaxAge)
		for id, env := range s.items {
			if env == nil {
				continue
			}
			if env.State != StateDead {
				continue
			}
			if env.ReceivedAt.IsZero() || env.ReceivedAt.After(cutoff) {
				continue
			}
			s.evictLocked(id, memoryEvictionReasonDeadRetentionAge)
		}
	}

	if s.deliveredRetentionMaxAge > 0 {
		cutoff := now.Add(-s.deliveredRetentionMaxAge)
		for id, env := range s.items {
			if env == nil {
				continue
			}
			if env.State != StateDelivered {
				continue
			}
			ts := env.NextRunAt
			if ts.IsZero() {
				ts = env.ReceivedAt
			}
			if ts.IsZero() || ts.After(cutoff) {
				continue
			}
			s.evictLocked(id, memoryEvictionReasonDeliveredRetention)
		}
	}

	if s.dlqMaxDepth > 0 {
		type deadItem struct {
			id         string
			receivedAt time.Time
		}
		items := make([]deadItem, 0, s.dlqMaxDepth)
		for id, env := range s.items {
			if env == nil || env.State != StateDead {
				continue
			}
			items = append(items, deadItem{id: id, receivedAt: env.ReceivedAt})
		}
		if len(items) > s.dlqMaxDepth {
			sort.Slice(items, func(i, j int) bool {
				return items[i].receivedAt.Before(items[j].receivedAt)
			})
			excess := len(items) - s.dlqMaxDepth
			for i := 0; i < excess; i++ {
				s.evictLocked(items[i].id, memoryEvictionReasonDeadRetentionDepth)
			}
		}
	}
	s.lastPrune = now
}

func (s *MemoryStore) Dequeue(req DequeueRequest) (DequeueResponse, error) {
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
		s.mu.Lock()
		now := req.Now
		if now.IsZero() {
			now = s.nowFn()
		}

		s.requeueExpiredLeasesLocked(now)
		s.maybePruneLocked(now)

		var out []Envelope
		for _, id := range s.order {
			if len(out) >= batch {
				break
			}

			env := s.items[id]
			if env == nil {
				continue
			}
			if env.State != StateQueued {
				continue
			}
			if req.Route != "" && env.Route != req.Route {
				continue
			}
			if req.Target != "" && env.Target != req.Target {
				continue
			}
			if !env.NextRunAt.IsZero() && env.NextRunAt.After(now) {
				continue
			}

			leaseID := newHexID("lease_")
			env.State = StateLeased
			env.Attempt++
			env.LeaseID = leaseID
			env.LeaseUntil = now.Add(leaseTTL)
			env.NextRunAt = env.LeaseUntil
			s.leases[leaseID] = env.ID

			out = append(out, *env)
		}

		if len(out) > 0 {
			s.compactOrderLocked()
			s.mu.Unlock()
			return DequeueResponse{Items: out}, nil
		}

		if maxWait == 0 {
			s.compactOrderLocked()
			s.mu.Unlock()
			return DequeueResponse{}, nil
		}

		waitCh := s.notify
		s.compactOrderLocked()
		s.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return DequeueResponse{}, nil
		}

		timer := time.NewTimer(remaining)
		select {
		case <-waitCh:
			if !timer.Stop() {
				<-timer.C
			}
			continue
		case <-timer.C:
			return DequeueResponse{}, nil
		}
	}
}

func (s *MemoryStore) Ack(leaseID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	itemID, ok := s.leases[leaseID]
	if !ok {
		return ErrLeaseNotFound
	}

	env := s.items[itemID]
	if env == nil || env.State != StateLeased || env.LeaseID != leaseID {
		delete(s.leases, leaseID)
		return ErrLeaseNotFound
	}

	if !env.LeaseUntil.IsZero() && !now.Before(env.LeaseUntil) {
		// Expired leases become visible again; treat this ack as invalid.
		s.requeueLocked(now, env)
		return ErrLeaseExpired
	}

	delete(s.leases, leaseID)
	if s.deliveredRetentionMaxAge > 0 {
		env.State = StateDelivered
		env.LeaseID = ""
		env.LeaseUntil = time.Time{}
		env.NextRunAt = now
		env.DeadReason = ""
		return nil
	}

	delete(s.items, itemID)
	return nil
}

func (s *MemoryStore) Nack(leaseID string, delay time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	itemID, ok := s.leases[leaseID]
	if !ok {
		return ErrLeaseNotFound
	}

	env := s.items[itemID]
	if env == nil || env.State != StateLeased || env.LeaseID != leaseID {
		delete(s.leases, leaseID)
		return ErrLeaseNotFound
	}

	if !env.LeaseUntil.IsZero() && !now.Before(env.LeaseUntil) {
		s.requeueLocked(now, env)
		return ErrLeaseExpired
	}

	delete(s.leases, leaseID)
	env.State = StateQueued
	env.LeaseID = ""
	env.LeaseUntil = time.Time{}
	env.DeadReason = ""
	if delay < 0 {
		delay = 0
	}
	env.NextRunAt = now.Add(delay)
	return nil
}

func (s *MemoryStore) Extend(leaseID string, extendBy time.Duration) error {
	if extendBy <= 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	itemID, ok := s.leases[leaseID]
	if !ok {
		return ErrLeaseNotFound
	}

	env := s.items[itemID]
	if env == nil || env.State != StateLeased || env.LeaseID != leaseID {
		delete(s.leases, leaseID)
		return ErrLeaseNotFound
	}

	if !env.LeaseUntil.IsZero() && !now.Before(env.LeaseUntil) {
		s.requeueLocked(now, env)
		return ErrLeaseExpired
	}

	env.LeaseUntil = env.LeaseUntil.Add(extendBy)
	env.NextRunAt = env.LeaseUntil
	return nil
}

func (s *MemoryStore) MarkDead(leaseID string, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	itemID, ok := s.leases[leaseID]
	if !ok {
		return ErrLeaseNotFound
	}

	env := s.items[itemID]
	if env == nil || env.State != StateLeased || env.LeaseID != leaseID {
		delete(s.leases, leaseID)
		return ErrLeaseNotFound
	}

	if !env.LeaseUntil.IsZero() && !now.Before(env.LeaseUntil) {
		s.requeueLocked(now, env)
		return ErrLeaseExpired
	}

	delete(s.leases, leaseID)
	env.State = StateDead
	env.LeaseID = ""
	env.LeaseUntil = time.Time{}
	env.NextRunAt = now
	env.DeadReason = reason
	return nil
}

func (s *MemoryStore) ListDead(req DeadListRequest) (DeadListResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	s.maybePruneLocked(now)

	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	items := make([]Envelope, 0)
	for _, env := range s.items {
		if env == nil || env.State != StateDead {
			continue
		}
		if req.Route != "" && env.Route != req.Route {
			continue
		}
		if !req.Before.IsZero() && !env.ReceivedAt.Before(req.Before) {
			continue
		}

		cp := *env
		if req.IncludePayload {
			if len(env.Payload) > 0 {
				cp.Payload = append([]byte(nil), env.Payload...)
			}
		} else {
			cp.Payload = nil
		}
		if req.IncludeHeaders {
			cp.Headers = cloneStringMap(env.Headers)
		} else {
			cp.Headers = nil
		}
		if req.IncludeTrace {
			cp.Trace = cloneStringMap(env.Trace)
		} else {
			cp.Trace = nil
		}

		items = append(items, cp)
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].ReceivedAt.Equal(items[j].ReceivedAt) {
			return items[i].ID > items[j].ID
		}
		return items[i].ReceivedAt.After(items[j].ReceivedAt)
	})

	if len(items) > limit {
		items = items[:limit]
	}

	return DeadListResponse{Items: items}, nil
}

func (s *MemoryStore) RequeueDead(req DeadRequeueRequest) (DeadRequeueResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	seen := make(map[string]struct{}, len(req.IDs))
	requeued := 0

	for _, raw := range req.IDs {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		env := s.items[id]
		if env == nil || env.State != StateDead {
			continue
		}

		env.State = StateQueued
		env.LeaseID = ""
		env.LeaseUntil = time.Time{}
		env.NextRunAt = now
		env.DeadReason = ""
		requeued++
	}

	if requeued > 0 {
		close(s.notify)
		s.notify = make(chan struct{})
	}

	return DeadRequeueResponse{Requeued: requeued}, nil
}

func (s *MemoryStore) DeleteDead(req DeadDeleteRequest) (DeadDeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[string]struct{}, len(req.IDs))
	deleted := 0

	for _, raw := range req.IDs {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		env := s.items[id]
		if env == nil || env.State != StateDead {
			continue
		}
		delete(s.items, id)
		deleted++
	}

	return DeadDeleteResponse{Deleted: deleted}, nil
}

func (s *MemoryStore) ListMessages(req MessageListRequest) (MessageListResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	s.maybePruneLocked(now)

	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	order := strings.ToLower(strings.TrimSpace(req.Order))
	if order == "" {
		order = MessageOrderDesc
	}
	if order != MessageOrderDesc && order != MessageOrderAsc {
		return MessageListResponse{}, errors.New("invalid message order")
	}

	items := make([]Envelope, 0)
	for _, env := range s.items {
		if env == nil {
			continue
		}
		if req.Route != "" && env.Route != req.Route {
			continue
		}
		if req.Target != "" && env.Target != req.Target {
			continue
		}
		if req.State != "" && env.State != req.State {
			continue
		}
		if !req.Before.IsZero() && !env.ReceivedAt.Before(req.Before) {
			continue
		}

		cp := *env
		if req.IncludePayload {
			if len(env.Payload) > 0 {
				cp.Payload = append([]byte(nil), env.Payload...)
			}
		} else {
			cp.Payload = nil
		}
		if req.IncludeHeaders {
			cp.Headers = cloneStringMap(env.Headers)
		} else {
			cp.Headers = nil
		}
		if req.IncludeTrace {
			cp.Trace = cloneStringMap(env.Trace)
		} else {
			cp.Trace = nil
		}

		items = append(items, cp)
	}

	if order == MessageOrderAsc {
		sort.Slice(items, func(i, j int) bool {
			if items[i].ReceivedAt.Equal(items[j].ReceivedAt) {
				return items[i].ID < items[j].ID
			}
			return items[i].ReceivedAt.Before(items[j].ReceivedAt)
		})
	} else {
		sort.Slice(items, func(i, j int) bool {
			if items[i].ReceivedAt.Equal(items[j].ReceivedAt) {
				return items[i].ID > items[j].ID
			}
			return items[i].ReceivedAt.After(items[j].ReceivedAt)
		})
	}

	if len(items) > limit {
		items = items[:limit]
	}

	return MessageListResponse{Items: items}, nil
}

func (s *MemoryStore) LookupMessages(req MessageLookupRequest) (MessageLookupResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return MessageLookupResponse{}, nil
	}

	items := make([]MessageLookupItem, 0, len(ids))
	for _, id := range ids {
		env := s.items[id]
		if env == nil {
			continue
		}
		items = append(items, MessageLookupItem{
			ID:    env.ID,
			Route: env.Route,
			State: env.State,
		})
	}
	return MessageLookupResponse{Items: items}, nil
}

func (s *MemoryStore) CancelMessages(req MessageCancelRequest) (MessageCancelResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	seen := make(map[string]struct{}, len(req.IDs))
	canceled := 0

	for _, raw := range req.IDs {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		env := s.items[id]
		if env == nil {
			continue
		}
		if env.State != StateQueued && env.State != StateLeased && env.State != StateDead {
			continue
		}

		if env.State == StateLeased && env.LeaseID != "" {
			delete(s.leases, env.LeaseID)
		}
		env.State = StateCanceled
		env.LeaseID = ""
		env.LeaseUntil = time.Time{}
		env.NextRunAt = now
		env.DeadReason = ""
		canceled++
	}

	return MessageCancelResponse{Canceled: canceled, Matched: canceled}, nil
}

func (s *MemoryStore) RequeueMessages(req MessageRequeueRequest) (MessageRequeueResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	seen := make(map[string]struct{}, len(req.IDs))
	requeued := 0

	for _, raw := range req.IDs {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		env := s.items[id]
		if env == nil {
			continue
		}
		if env.State != StateDead && env.State != StateCanceled {
			continue
		}

		env.State = StateQueued
		env.LeaseID = ""
		env.LeaseUntil = time.Time{}
		env.NextRunAt = now
		env.DeadReason = ""
		requeued++
	}

	if requeued > 0 {
		close(s.notify)
		s.notify = make(chan struct{})
	}

	return MessageRequeueResponse{Requeued: requeued, Matched: requeued}, nil
}

func (s *MemoryStore) ResumeMessages(req MessageResumeRequest) (MessageResumeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := normalizeUniqueIDs(req.IDs)
	if len(ids) == 0 {
		return MessageResumeResponse{}, nil
	}

	now := s.nowFn()
	resumed := 0
	for _, id := range ids {
		env := s.items[id]
		if env == nil {
			continue
		}
		if env.State != StateCanceled {
			continue
		}
		env.State = StateQueued
		env.LeaseID = ""
		env.LeaseUntil = time.Time{}
		env.NextRunAt = now
		env.DeadReason = ""
		resumed++
	}

	if resumed > 0 {
		close(s.notify)
		s.notify = make(chan struct{})
	}

	return MessageResumeResponse{Resumed: resumed, Matched: resumed}, nil
}

func (s *MemoryStore) CancelMessagesByFilter(req MessageManageFilterRequest) (MessageCancelResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	items := s.filterManageCandidatesLocked(req, []State{StateQueued, StateLeased, StateDead})
	if req.PreviewOnly {
		return MessageCancelResponse{
			Canceled:    0,
			Matched:     len(items),
			PreviewOnly: true,
		}, nil
	}
	canceled := 0

	for _, env := range items {
		if env.State == StateLeased && env.LeaseID != "" {
			delete(s.leases, env.LeaseID)
		}
		env.State = StateCanceled
		env.LeaseID = ""
		env.LeaseUntil = time.Time{}
		env.NextRunAt = now
		env.DeadReason = ""
		canceled++
	}

	return MessageCancelResponse{Canceled: canceled, Matched: len(items)}, nil
}

func (s *MemoryStore) RequeueMessagesByFilter(req MessageManageFilterRequest) (MessageRequeueResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	items := s.filterManageCandidatesLocked(req, []State{StateDead, StateCanceled})
	if req.PreviewOnly {
		return MessageRequeueResponse{
			Requeued:    0,
			Matched:     len(items),
			PreviewOnly: true,
		}, nil
	}
	requeued := 0

	for _, env := range items {
		env.State = StateQueued
		env.LeaseID = ""
		env.LeaseUntil = time.Time{}
		env.NextRunAt = now
		env.DeadReason = ""
		requeued++
	}

	if requeued > 0 {
		close(s.notify)
		s.notify = make(chan struct{})
	}

	return MessageRequeueResponse{Requeued: requeued, Matched: len(items)}, nil
}

func (s *MemoryStore) ResumeMessagesByFilter(req MessageManageFilterRequest) (MessageResumeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	items := s.filterManageCandidatesLocked(req, []State{StateCanceled})
	if req.PreviewOnly {
		return MessageResumeResponse{
			Resumed:     0,
			Matched:     len(items),
			PreviewOnly: true,
		}, nil
	}
	resumed := 0

	for _, env := range items {
		env.State = StateQueued
		env.LeaseID = ""
		env.LeaseUntil = time.Time{}
		env.NextRunAt = now
		env.DeadReason = ""
		resumed++
	}

	if resumed > 0 {
		close(s.notify)
		s.notify = make(chan struct{})
	}

	return MessageResumeResponse{Resumed: resumed, Matched: len(items)}, nil
}

func (s *MemoryStore) Stats() (Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn()
	s.maybePruneLocked(now)

	byState := map[State]int{
		StateQueued:    0,
		StateLeased:    0,
		StateDelivered: 0,
		StateDead:      0,
		StateCanceled:  0,
	}
	total := 0
	var oldestQueuedReceivedAt time.Time
	var earliestQueuedNextRun time.Time
	backlog := map[string]*BacklogBucket{}
	for _, env := range s.items {
		if env == nil {
			continue
		}
		byState[env.State]++
		total++
		if env.State == StateQueued {
			if !env.ReceivedAt.IsZero() && (oldestQueuedReceivedAt.IsZero() || env.ReceivedAt.Before(oldestQueuedReceivedAt)) {
				oldestQueuedReceivedAt = env.ReceivedAt
			}
			if !env.NextRunAt.IsZero() && (earliestQueuedNextRun.IsZero() || env.NextRunAt.Before(earliestQueuedNextRun)) {
				earliestQueuedNextRun = env.NextRunAt
			}
			key := env.Route + "\x00" + env.Target
			b := backlog[key]
			if b == nil {
				b = &BacklogBucket{
					Route:  env.Route,
					Target: env.Target,
				}
				backlog[key] = b
			}
			b.Queued++
			if !env.ReceivedAt.IsZero() && (b.OldestQueuedReceivedAt.IsZero() || env.ReceivedAt.Before(b.OldestQueuedReceivedAt)) {
				b.OldestQueuedReceivedAt = env.ReceivedAt
			}
			if !env.NextRunAt.IsZero() && (b.EarliestQueuedNextRun.IsZero() || env.NextRunAt.Before(b.EarliestQueuedNextRun)) {
				b.EarliestQueuedNextRun = env.NextRunAt
			}
		}
	}

	oldestQueuedAge := time.Duration(0)
	if !oldestQueuedReceivedAt.IsZero() && !now.Before(oldestQueuedReceivedAt) {
		oldestQueuedAge = now.Sub(oldestQueuedReceivedAt)
	}
	readyLag := time.Duration(0)
	if !earliestQueuedNextRun.IsZero() && now.After(earliestQueuedNextRun) {
		readyLag = now.Sub(earliestQueuedNextRun)
	}

	topQueued := make([]BacklogBucket, 0, len(backlog))
	for _, b := range backlog {
		if b == nil {
			continue
		}
		if !b.OldestQueuedReceivedAt.IsZero() && !now.Before(b.OldestQueuedReceivedAt) {
			b.OldestQueuedAge = now.Sub(b.OldestQueuedReceivedAt)
		}
		if !b.EarliestQueuedNextRun.IsZero() && now.After(b.EarliestQueuedNextRun) {
			b.ReadyLag = now.Sub(b.EarliestQueuedNextRun)
		}
		topQueued = append(topQueued, *b)
	}
	sort.Slice(topQueued, func(i, j int) bool {
		if topQueued[i].Queued != topQueued[j].Queued {
			return topQueued[i].Queued > topQueued[j].Queued
		}
		if topQueued[i].Route != topQueued[j].Route {
			return topQueued[i].Route < topQueued[j].Route
		}
		return topQueued[i].Target < topQueued[j].Target
	})
	if len(topQueued) > statsTopBacklogLimit {
		topQueued = topQueued[:statsTopBacklogLimit]
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

func (s *MemoryStore) CaptureBacklogTrendSample(at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	capturedAt := at
	if capturedAt.IsZero() {
		capturedAt = s.nowFn()
	}
	capturedAt = capturedAt.UTC()

	byKey := make(map[string]*backlogTrendRow)
	global := backlogTrendRow{CapturedAt: capturedAt}
	for _, env := range s.items {
		if env == nil {
			continue
		}
		switch env.State {
		case StateQueued, StateLeased, StateDead:
		default:
			continue
		}

		switch env.State {
		case StateQueued:
			global.Queued++
		case StateLeased:
			global.Leased++
		case StateDead:
			global.Dead++
		}

		key := env.Route + "\x00" + env.Target
		row := byKey[key]
		if row == nil {
			row = &backlogTrendRow{
				CapturedAt: capturedAt,
				Route:      env.Route,
				Target:     env.Target,
			}
			byKey[key] = row
		}
		switch env.State {
		case StateQueued:
			row.Queued++
		case StateLeased:
			row.Leased++
		case StateDead:
			row.Dead++
		}
	}

	s.trendRows = append(s.trendRows, global)
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		row := byKey[k]
		if row == nil {
			continue
		}
		s.trendRows = append(s.trendRows, *row)
	}
	if len(s.trendRows) > backlogTrendMaxRows {
		s.trendRows = append([]backlogTrendRow(nil), s.trendRows[len(s.trendRows)-backlogTrendMaxRows:]...)
	}
	return nil
}

func (s *MemoryStore) ListBacklogTrend(req BacklogTrendListRequest) (BacklogTrendListResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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

	inRange := func(at time.Time) bool {
		if !since.IsZero() && at.Before(since) {
			return false
		}
		if !until.IsZero() && !at.Before(until) {
			return false
		}
		return true
	}

	samples := make([]BacklogTrendSample, 0, len(s.trendRows))
	if route == "" && target == "" {
		for _, row := range s.trendRows {
			if row.Route != "" || row.Target != "" {
				continue
			}
			if !inRange(row.CapturedAt) {
				continue
			}
			samples = append(samples, BacklogTrendSample{
				CapturedAt: row.CapturedAt,
				Queued:     row.Queued,
				Leased:     row.Leased,
				Dead:       row.Dead,
			})
		}
	} else {
		byCaptured := make(map[int64]*BacklogTrendSample)
		for _, row := range s.trendRows {
			if row.Route == "" && row.Target == "" {
				continue
			}
			if route != "" && row.Route != route {
				continue
			}
			if target != "" && row.Target != target {
				continue
			}
			if !inRange(row.CapturedAt) {
				continue
			}
			key := row.CapturedAt.UnixNano()
			sample := byCaptured[key]
			if sample == nil {
				sample = &BacklogTrendSample{CapturedAt: row.CapturedAt}
				byCaptured[key] = sample
			}
			sample.Queued += row.Queued
			sample.Leased += row.Leased
			sample.Dead += row.Dead
		}
		samples = make([]BacklogTrendSample, 0, len(byCaptured))
		for _, sample := range byCaptured {
			if sample == nil {
				continue
			}
			samples = append(samples, *sample)
		}
	}

	sort.Slice(samples, func(i, j int) bool {
		if samples[i].CapturedAt.Equal(samples[j].CapturedAt) {
			return samples[i].Queued+samples[i].Leased+samples[i].Dead > samples[j].Queued+samples[j].Leased+samples[j].Dead
		}
		return samples[i].CapturedAt.After(samples[j].CapturedAt)
	})

	truncated := false
	if len(samples) > limit {
		truncated = true
		samples = samples[:limit]
	}

	for i, j := 0, len(samples)-1; i < j; i, j = i+1, j-1 {
		samples[i], samples[j] = samples[j], samples[i]
	}
	return BacklogTrendListResponse{
		Items:     samples,
		Truncated: truncated,
	}, nil
}

func (s *MemoryStore) filterManageCandidatesLocked(req MessageManageFilterRequest, allowed []State) []*Envelope {
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
	if req.State != "" {
		if _, ok := allowedSet[req.State]; !ok {
			return nil
		}
		allowedSet = map[State]struct{}{req.State: {}}
	}

	candidates := make([]*Envelope, 0, len(s.items))
	for _, env := range s.items {
		if env == nil {
			continue
		}
		if _, ok := allowedSet[env.State]; !ok {
			continue
		}
		if req.Route != "" && env.Route != req.Route {
			continue
		}
		if req.Target != "" && env.Target != req.Target {
			continue
		}
		if !req.Before.IsZero() && !env.ReceivedAt.Before(req.Before) {
			continue
		}
		candidates = append(candidates, env)
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ReceivedAt.Equal(candidates[j].ReceivedAt) {
			return candidates[i].ID > candidates[j].ID
		}
		return candidates[i].ReceivedAt.After(candidates[j].ReceivedAt)
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates
}

func (s *MemoryStore) RecordAttempt(attempt DeliveryAttempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if strings.TrimSpace(attempt.ID) == "" {
		attempt.ID = newHexID("att_")
	}
	if attempt.CreatedAt.IsZero() {
		attempt.CreatedAt = s.nowFn()
	}
	attempt.CreatedAt = attempt.CreatedAt.UTC()
	attempt.Error = strings.TrimSpace(attempt.Error)
	attempt.DeadReason = strings.TrimSpace(attempt.DeadReason)

	if attempt.Outcome == "" {
		attempt.Outcome = AttemptOutcomeRetry
	}

	s.attempts = append(s.attempts, attempt)
	return nil
}

func (s *MemoryStore) ListAttempts(req AttemptListRequest) (AttemptListResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	items := make([]DeliveryAttempt, 0)
	for _, attempt := range s.attempts {
		if req.Route != "" && attempt.Route != req.Route {
			continue
		}
		if req.Target != "" && attempt.Target != req.Target {
			continue
		}
		if req.EventID != "" && attempt.EventID != req.EventID {
			continue
		}
		if req.Outcome != "" && attempt.Outcome != req.Outcome {
			continue
		}
		if !req.Before.IsZero() && !attempt.CreatedAt.Before(req.Before) {
			continue
		}
		items = append(items, attempt)
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].ID > items[j].ID
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})

	if len(items) > limit {
		items = items[:limit]
	}

	return AttemptListResponse{Items: items}, nil
}

func (s *MemoryStore) RuntimeMetrics() StoreRuntimeMetrics {
	out := StoreRuntimeMetrics{Backend: "memory"}
	if s == nil {
		return out
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	inv := s.memoryInventoryLocked()
	itemsByState := make(map[State]int64, len(inv.itemsByState))
	for st, n := range inv.itemsByState {
		itemsByState[st] = n
	}
	retainedBytesByState := make(map[State]int64, len(inv.retainedBytesByState))
	for st, n := range inv.retainedBytesByState {
		retainedBytesByState[st] = n
	}
	evictions := make(map[string]int64, len(s.evictionsTotalByReason))
	for reason, n := range s.evictionsTotalByReason {
		evictions[reason] = n
	}
	pressure := s.memoryPressureStatusLocked()

	out.Memory = &MemoryRuntimeMetrics{
		ItemsByState:           itemsByState,
		RetainedBytesByState:   retainedBytesByState,
		RetainedBytesTotal:     inv.totalRetainedBytes,
		EvictionsTotalByReason: evictions,
		Pressure:               pressure,
	}
	return out
}

func (s *MemoryStore) requeueExpiredLeasesLocked(now time.Time) {
	for leaseID, itemID := range s.leases {
		env := s.items[itemID]
		if env == nil {
			delete(s.leases, leaseID)
			continue
		}
		if env.State != StateLeased || env.LeaseID != leaseID {
			delete(s.leases, leaseID)
			continue
		}
		if env.LeaseUntil.IsZero() {
			continue
		}
		if now.Before(env.LeaseUntil) {
			continue
		}

		s.requeueLocked(now, env)
	}
}

func (s *MemoryStore) requeueLocked(now time.Time, env *Envelope) {
	delete(s.leases, env.LeaseID)
	env.State = StateQueued
	env.LeaseID = ""
	env.LeaseUntil = time.Time{}
	env.NextRunAt = now
	env.DeadReason = ""
}

func (s *MemoryStore) compactOrderLocked() {
	// If the order list contains too many deleted IDs, compact it.
	if len(s.order) < 1024 {
		return
	}
	if len(s.items) == 0 {
		s.order = s.order[:0]
		return
	}
	if len(s.order) <= 4*len(s.items) {
		return
	}
	out := make([]string, 0, len(s.items))
	for _, id := range s.order {
		if s.items[id] != nil {
			out = append(out, id)
		}
	}
	s.order = out
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
