package pullapi

import (
	"container/list"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

const (
	pullErrMethodNotAllowed  = "method_not_allowed"
	pullErrUnauthorized      = "unauthorized"
	pullErrRouteNotFound     = "route_not_found"
	pullErrOperationNotFound = "operation_not_found"
	pullErrInvalidBody       = "invalid_body"
	pullErrLeaseConflict     = "lease_conflict"
	pullErrStoreUnavailable  = "store_unavailable"
	pullErrInternal          = "internal_error"

	recentLeaseOpAck  = "ack"
	recentLeaseOpNack = "nack"
)

type Server struct {
	Store           queue.Store
	Target          string
	ResolveRoute    func(endpoint string) (route string, ok bool)
	Authorize       Authorizer
	ObserveDequeue  func(route string, statusCode int, items []queue.Envelope)
	ObserveAck      func(route string, statusCode int, leaseID string, leaseExpired bool)
	ObserveNack     func(route string, statusCode int, leaseID string, leaseExpired bool)
	ObserveExtend   func(route string, statusCode int, leaseID string, extendBy time.Duration, leaseExpired bool)
	DefaultLeaseTTL time.Duration
	MaxBatch        int
	MaxLeaseBatch   int
	MaxLeaseTTL     time.Duration
	DefaultMaxWait  time.Duration
	MaxWait         time.Duration

	RecentLeaseOpTTL time.Duration
	RecentLeaseOpCap int

	recentLeaseMu    sync.Mutex
	recentLeaseOps   map[recentLeaseOpKey]*list.Element
	recentLeaseOrder list.List
	now              func() time.Time
}

func NewServer(store queue.Store) *Server {
	return &Server{
		Store:            store,
		Target:           "pull",
		ResolveRoute:     nil,
		DefaultLeaseTTL:  30 * time.Second,
		MaxBatch:         100,
		MaxLeaseBatch:    100,
		RecentLeaseOpTTL: 2 * time.Minute,
		RecentLeaseOpCap: 20000,
		now:              time.Now,
	}
}

type recentLeaseOpKey struct {
	leaseID string
	op      string
}

type recentLeaseOpEntry struct {
	key       recentLeaseOpKey
	expiresAt time.Time
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, pullErrMethodNotAllowed, "method must be POST")
		return
	}

	if s.Authorize != nil && !s.Authorize(r) {
		writeError(w, http.StatusUnauthorized, pullErrUnauthorized, "request is not authorized")
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	op := path.Base(cleanPath)
	endpoint := strings.TrimSuffix(cleanPath, "/"+op)
	if endpoint == "" {
		endpoint = "/"
	}

	route, ok := s.resolveRoute(endpoint)
	if !ok {
		writeError(w, http.StatusNotFound, pullErrRouteNotFound, "pull endpoint is not configured")
		return
	}
	switch op {
	case "dequeue":
		s.handleDequeue(w, r, route)
	case "ack":
		s.handleAck(w, r, route)
	case "nack":
		s.handleNack(w, r, route)
	case "extend":
		s.handleExtend(w, r, route)
	default:
		writeError(w, http.StatusNotFound, pullErrOperationNotFound, "pull operation was not found")
	}
}

type dequeueRequest struct {
	Batch    int    `json:"batch"`
	MaxWait  string `json:"max_wait,omitempty"`
	LeaseTTL string `json:"lease_ttl,omitempty"`
}

type dequeueResponse struct {
	Items []dequeueItem `json:"items"`
}

type dequeueItem struct {
	ID         string            `json:"id"`
	LeaseID    string            `json:"lease_id"`
	ReceivedAt time.Time         `json:"received_at"`
	Attempt    int               `json:"attempt"`
	NextRunAt  time.Time         `json:"next_run_at"`
	Route      string            `json:"route"`
	PayloadB64 string            `json:"payload_b64"`
	Headers    map[string]string `json:"headers,omitempty"`
	Trace      map[string]string `json:"trace,omitempty"`
}

func (s *Server) handleDequeue(w http.ResponseWriter, r *http.Request, route string) {
	var req dequeueRequest
	if r.Body != nil && !decodeJSONBodyStrict(w, r, &req, true) {
		s.observeDequeue(route, http.StatusBadRequest, nil)
		return
	}

	maxWait, ok := parseDuration(req.MaxWait)
	if !ok {
		writeError(w, http.StatusBadRequest, pullErrInvalidBody, "max_wait must be a valid duration")
		s.observeDequeue(route, http.StatusBadRequest, nil)
		return
	}
	if req.MaxWait == "" && s.DefaultMaxWait > 0 {
		maxWait = s.DefaultMaxWait
	}
	if s.MaxWait > 0 && maxWait > s.MaxWait {
		maxWait = s.MaxWait
	}

	leaseTTL := s.DefaultLeaseTTL
	if req.LeaseTTL != "" {
		d, ok := parseDuration(req.LeaseTTL)
		if !ok {
			writeError(w, http.StatusBadRequest, pullErrInvalidBody, "lease_ttl must be a valid duration")
			s.observeDequeue(route, http.StatusBadRequest, nil)
			return
		}
		leaseTTL = d
	}

	outcome, opErr := s.Dequeue(route, DequeueParams{
		Batch:       req.Batch,
		MaxWait:     maxWait,
		HasMaxWait:  req.MaxWait != "",
		LeaseTTL:    leaseTTL,
		HasLeaseTTL: req.LeaseTTL != "",
	})
	if opErr != nil {
		writeError(w, opErr.StatusCode, opErr.Code, opErr.Detail)
		return
	}

	out := dequeueResponse{Items: make([]dequeueItem, 0, len(outcome.Items))}
	for _, it := range outcome.Items {
		out.Items = append(out.Items, dequeueItem{
			ID:         it.ID,
			LeaseID:    it.LeaseID,
			ReceivedAt: it.ReceivedAt,
			Attempt:    it.Attempt,
			NextRunAt:  it.NextRunAt,
			Route:      it.Route,
			PayloadB64: base64.StdEncoding.EncodeToString(it.Payload),
			Headers:    it.Headers,
			Trace:      it.Trace,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	_ = enc.Encode(out)
}

type leaseRequest struct {
	LeaseID  string   `json:"lease_id"`
	LeaseIDs []string `json:"lease_ids,omitempty"`
	Delay    string   `json:"delay,omitempty"`
	ExtendBy string   `json:"extend_by,omitempty"`
	Dead     bool     `json:"dead,omitempty"`
	Reason   string   `json:"reason,omitempty"`
}

type ackBatchConflict struct {
	LeaseID string `json:"lease_id"`
	Reason  string `json:"reason"`
}

type ackBatchResponse struct {
	Code      string             `json:"code,omitempty"`
	Detail    string             `json:"detail,omitempty"`
	Acked     int                `json:"acked"`
	Conflicts []ackBatchConflict `json:"conflicts,omitempty"`
}

type nackBatchResponse struct {
	Code      string             `json:"code,omitempty"`
	Detail    string             `json:"detail,omitempty"`
	Succeeded int                `json:"succeeded"`
	Conflicts []ackBatchConflict `json:"conflicts,omitempty"`
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request, route string) {
	req, ok := readLeaseRequest(w, r)
	if !ok {
		s.observeAck(route, http.StatusBadRequest, "", false)
		return
	}

	leaseIDs, isBatch, errDetail := normalizeLeaseIDs(req, s.MaxLeaseBatch)
	if errDetail != "" {
		writeError(w, http.StatusBadRequest, pullErrInvalidBody, errDetail)
		s.observeAck(route, http.StatusBadRequest, "", false)
		return
	}
	if !isBatch {
		if opErr := s.AckSingle(route, leaseIDs[0]); opErr != nil {
			writeError(w, opErr.StatusCode, opErr.Code, opErr.Detail)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	outcome, opErr := s.AckBatch(route, leaseIDs)
	if opErr != nil {
		writeError(w, opErr.StatusCode, opErr.Code, opErr.Detail)
		return
	}

	status := http.StatusOK
	out := ackBatchResponse{
		Acked:     outcome.Succeeded,
		Conflicts: mapLeaseBatchConflicts(outcome.Conflicts),
	}
	if len(outcome.Conflicts) > 0 {
		status = http.StatusConflict
		out.Code = pullErrLeaseConflict
		out.Detail = "one or more leases are invalid or expired"
	}
	writeJSON(w, status, out)
}

func (s *Server) handleNack(w http.ResponseWriter, r *http.Request, route string) {
	req, ok := readLeaseRequest(w, r)
	if !ok {
		s.observeNack(route, http.StatusBadRequest, "", false)
		return
	}

	leaseIDs, isBatch, errDetail := normalizeLeaseIDs(req, s.MaxLeaseBatch)
	if errDetail != "" {
		writeError(w, http.StatusBadRequest, pullErrInvalidBody, errDetail)
		s.observeNack(route, http.StatusBadRequest, "", false)
		return
	}
	if !isBatch {
		delay, ok := parseDuration(req.Delay)
		if !req.Dead && !ok {
			writeError(w, http.StatusBadRequest, pullErrInvalidBody, "delay must be a valid duration")
			s.observeNack(route, http.StatusBadRequest, leaseIDs[0], false)
			return
		}
		if opErr := s.NackSingle(route, leaseIDs[0], req.Dead, req.Reason, delay); opErr != nil {
			writeError(w, opErr.StatusCode, opErr.Code, opErr.Detail)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	delay, ok := parseDuration(req.Delay)
	if !req.Dead && !ok {
		writeError(w, http.StatusBadRequest, pullErrInvalidBody, "delay must be a valid duration")
		s.observeNack(route, http.StatusBadRequest, "", false)
		return
	}

	outcome, opErr := s.NackBatch(route, leaseIDs, req.Dead, req.Reason, delay)
	if opErr != nil {
		writeError(w, opErr.StatusCode, opErr.Code, opErr.Detail)
		return
	}

	status := http.StatusOK
	out := nackBatchResponse{
		Succeeded: outcome.Succeeded,
		Conflicts: mapLeaseBatchConflicts(outcome.Conflicts),
	}
	if len(outcome.Conflicts) > 0 {
		status = http.StatusConflict
		out.Code = pullErrLeaseConflict
		out.Detail = "one or more leases are invalid or expired"
	}
	writeJSON(w, status, out)
}

func mapLeaseBatchConflicts(conflicts []queue.LeaseBatchConflict) []ackBatchConflict {
	if len(conflicts) == 0 {
		return nil
	}
	out := make([]ackBatchConflict, 0, len(conflicts))
	for _, conflict := range conflicts {
		reason := "lease_not_found"
		if conflict.Expired {
			reason = "lease_expired"
		}
		out = append(out, ackBatchConflict{
			LeaseID: conflict.LeaseID,
			Reason:  reason,
		})
	}
	return out
}

func observeBatchAck(s *Server, route string, leaseIDs []string, res queue.LeaseBatchResult) {
	conflicts := make(map[string]bool, len(res.Conflicts))
	for _, conflict := range res.Conflicts {
		conflicts[conflict.LeaseID] = conflict.Expired
	}
	for _, leaseID := range leaseIDs {
		expired, isConflict := conflicts[leaseID]
		if isConflict {
			s.observeAck(route, http.StatusConflict, leaseID, expired)
			continue
		}
		s.observeAck(route, http.StatusNoContent, leaseID, false)
	}
}

func observeBatchNack(s *Server, route string, leaseIDs []string, res queue.LeaseBatchResult) {
	conflicts := make(map[string]bool, len(res.Conflicts))
	for _, conflict := range res.Conflicts {
		conflicts[conflict.LeaseID] = conflict.Expired
	}
	for _, leaseID := range leaseIDs {
		expired, isConflict := conflicts[leaseID]
		if isConflict {
			s.observeNack(route, http.StatusConflict, leaseID, expired)
			continue
		}
		s.observeNack(route, http.StatusNoContent, leaseID, false)
	}
}

func (s *Server) handleExtend(w http.ResponseWriter, r *http.Request, route string) {
	req, ok := readLeaseRequest(w, r)
	if !ok {
		s.observeExtend(route, http.StatusBadRequest, "", 0, false)
		return
	}
	if req.LeaseID == "" || req.ExtendBy == "" {
		writeError(w, http.StatusBadRequest, pullErrInvalidBody, "lease_id and extend_by are required")
		s.observeExtend(route, http.StatusBadRequest, req.LeaseID, 0, false)
		return
	}

	extendBy, ok := parseDuration(req.ExtendBy)
	if !ok {
		writeError(w, http.StatusBadRequest, pullErrInvalidBody, "extend_by must be a valid duration")
		s.observeExtend(route, http.StatusBadRequest, req.LeaseID, 0, false)
		return
	}

	if opErr := s.Extend(route, req.LeaseID, extendBy); opErr != nil {
		writeError(w, opErr.StatusCode, opErr.Code, opErr.Detail)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) resolveRoute(endpoint string) (string, bool) {
	if s.ResolveRoute == nil {
		return endpoint, true
	}
	return s.ResolveRoute(endpoint)
}

func (s *Server) observeDequeue(route string, statusCode int, items []queue.Envelope) {
	if s.ObserveDequeue != nil {
		s.ObserveDequeue(route, statusCode, items)
	}
}

func (s *Server) observeAck(route string, statusCode int, leaseID string, leaseExpired bool) {
	if s.ObserveAck != nil {
		s.ObserveAck(route, statusCode, leaseID, leaseExpired)
	}
}

func (s *Server) observeNack(route string, statusCode int, leaseID string, leaseExpired bool) {
	if s.ObserveNack != nil {
		s.ObserveNack(route, statusCode, leaseID, leaseExpired)
	}
}

func (s *Server) observeExtend(route string, statusCode int, leaseID string, extendBy time.Duration, leaseExpired bool) {
	if s.ObserveExtend != nil {
		s.ObserveExtend(route, statusCode, leaseID, extendBy, leaseExpired)
	}
}

func (s *Server) nowTime() time.Time {
	if s != nil && s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *Server) isRecentlyCompletedLease(leaseID string, op string) bool {
	leaseID = strings.TrimSpace(leaseID)
	op = strings.TrimSpace(op)
	if leaseID == "" || op == "" {
		return false
	}
	if s.RecentLeaseOpTTL <= 0 || s.RecentLeaseOpCap <= 0 {
		return false
	}

	now := s.nowTime()
	key := recentLeaseOpKey{leaseID: leaseID, op: op}

	s.recentLeaseMu.Lock()
	defer s.recentLeaseMu.Unlock()

	s.pruneRecentLeaseOpsLocked(now)
	elem, ok := s.recentLeaseOps[key]
	if !ok {
		return false
	}

	entry, _ := elem.Value.(*recentLeaseOpEntry)
	if entry == nil || !now.Before(entry.expiresAt) {
		s.removeRecentLeaseOpLocked(elem)
		return false
	}
	return true
}

func (s *Server) rememberCompletedLease(leaseID string, op string) {
	leaseID = strings.TrimSpace(leaseID)
	op = strings.TrimSpace(op)
	if leaseID == "" || op == "" {
		return
	}
	if s.RecentLeaseOpTTL <= 0 || s.RecentLeaseOpCap <= 0 {
		return
	}

	now := s.nowTime()
	expiresAt := now.Add(s.RecentLeaseOpTTL)
	key := recentLeaseOpKey{leaseID: leaseID, op: op}

	s.recentLeaseMu.Lock()
	defer s.recentLeaseMu.Unlock()

	s.pruneRecentLeaseOpsLocked(now)
	if s.recentLeaseOps == nil {
		s.recentLeaseOps = make(map[recentLeaseOpKey]*list.Element)
	}

	if elem, ok := s.recentLeaseOps[key]; ok {
		entry, _ := elem.Value.(*recentLeaseOpEntry)
		if entry == nil {
			entry = &recentLeaseOpEntry{key: key, expiresAt: expiresAt}
			elem.Value = entry
		}
		entry.expiresAt = expiresAt
		s.recentLeaseOrder.MoveToBack(elem)
		return
	}

	elem := s.recentLeaseOrder.PushBack(&recentLeaseOpEntry{
		key:       key,
		expiresAt: expiresAt,
	})
	s.recentLeaseOps[key] = elem

	for len(s.recentLeaseOps) > s.RecentLeaseOpCap {
		front := s.recentLeaseOrder.Front()
		if front == nil {
			break
		}
		s.removeRecentLeaseOpLocked(front)
	}
}

func (s *Server) partitionRecentlyCompletedLeases(leaseIDs []string, op string) (pending []string, completed []string) {
	if len(leaseIDs) == 0 {
		return nil, nil
	}
	pending = make([]string, 0, len(leaseIDs))
	completed = make([]string, 0, len(leaseIDs))
	for _, leaseID := range leaseIDs {
		if s.isRecentlyCompletedLease(leaseID, op) {
			completed = append(completed, leaseID)
			continue
		}
		pending = append(pending, leaseID)
	}
	return pending, completed
}

func (s *Server) successfulLeaseIDs(leaseIDs []string, conflicts []queue.LeaseBatchConflict) []string {
	if len(leaseIDs) == 0 {
		return nil
	}
	if len(conflicts) == 0 {
		return append([]string(nil), leaseIDs...)
	}
	conflictByID := make(map[string]struct{}, len(conflicts))
	for _, conflict := range conflicts {
		conflictByID[conflict.LeaseID] = struct{}{}
	}
	out := make([]string, 0, len(leaseIDs))
	for _, leaseID := range leaseIDs {
		if _, ok := conflictByID[leaseID]; ok {
			continue
		}
		out = append(out, leaseID)
	}
	return out
}

func (s *Server) pruneRecentLeaseOpsLocked(now time.Time) {
	for {
		front := s.recentLeaseOrder.Front()
		if front == nil {
			return
		}
		entry, _ := front.Value.(*recentLeaseOpEntry)
		if entry == nil || now.Before(entry.expiresAt) {
			return
		}
		s.removeRecentLeaseOpLocked(front)
	}
}

func (s *Server) removeRecentLeaseOpLocked(elem *list.Element) {
	if elem == nil {
		return
	}
	entry, _ := elem.Value.(*recentLeaseOpEntry)
	if entry != nil && s.recentLeaseOps != nil {
		delete(s.recentLeaseOps, entry.key)
	}
	s.recentLeaseOrder.Remove(elem)
}

func readLeaseRequest(w http.ResponseWriter, r *http.Request) (leaseRequest, bool) {
	var req leaseRequest
	if !decodeJSONBodyStrict(w, r, &req, false) {
		return leaseRequest{}, false
	}
	return req, true
}

func decodeJSONBodyStrict(w http.ResponseWriter, r *http.Request, dst any, allowEmpty bool) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if allowEmpty && errors.Is(err, io.EOF) {
			return true
		}
		writeError(w, http.StatusBadRequest, pullErrInvalidBody, "invalid JSON body: "+err.Error())
		return false
	}

	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			writeError(w, http.StatusBadRequest, pullErrInvalidBody, "invalid JSON body: trailing JSON document is not allowed")
			return false
		}
		writeError(w, http.StatusBadRequest, pullErrInvalidBody, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func normalizeLeaseIDs(req leaseRequest, maxBatch int) ([]string, bool, string) {
	single := strings.TrimSpace(req.LeaseID)
	if single != "" && len(req.LeaseIDs) > 0 {
		return nil, false, "use either lease_id or lease_ids, not both"
	}
	if single != "" {
		return []string{single}, false, ""
	}

	if len(req.LeaseIDs) == 0 {
		return nil, false, "lease_id or lease_ids is required"
	}

	seen := make(map[string]struct{}, len(req.LeaseIDs))
	out := make([]string, 0, len(req.LeaseIDs))
	for _, raw := range req.LeaseIDs {
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
	if len(out) == 0 {
		return nil, false, "lease_ids must include at least one non-empty lease id"
	}
	if maxBatch > 0 && len(out) > maxBatch {
		return nil, false, "lease_ids exceeds max batch"
	}
	return out, true, ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errorResponse struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func writeError(w http.ResponseWriter, status int, code string, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{
		Code:   code,
		Detail: detail,
	})
}

func parseDuration(s string) (time.Duration, bool) {
	if s == "" {
		return 0, true
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, false
	}
	return d, true
}
