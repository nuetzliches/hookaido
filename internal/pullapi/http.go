package pullapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strings"
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
	MaxLeaseTTL     time.Duration
	DefaultMaxWait  time.Duration
	MaxWait         time.Duration
}

func NewServer(store queue.Store) *Server {
	return &Server{
		Store:           store,
		Target:          "pull",
		ResolveRoute:    nil,
		DefaultLeaseTTL: 30 * time.Second,
		MaxBatch:        100,
	}
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

	batch := req.Batch
	if batch <= 0 {
		batch = 1
	}
	if s.MaxBatch > 0 && batch > s.MaxBatch {
		batch = s.MaxBatch
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
	if s.MaxLeaseTTL > 0 && leaseTTL > s.MaxLeaseTTL {
		leaseTTL = s.MaxLeaseTTL
	}

	resp, err := s.Store.Dequeue(queue.DequeueRequest{
		Route:    route,
		Target:   s.Target,
		Batch:    batch,
		MaxWait:  maxWait,
		LeaseTTL: leaseTTL,
	})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, pullErrStoreUnavailable, "dequeue is temporarily unavailable")
		s.observeDequeue(route, http.StatusServiceUnavailable, nil)
		return
	}

	out := dequeueResponse{Items: make([]dequeueItem, 0, len(resp.Items))}
	for _, it := range resp.Items {
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
	s.observeDequeue(route, http.StatusOK, resp.Items)
}

type leaseRequest struct {
	LeaseID  string `json:"lease_id"`
	Delay    string `json:"delay,omitempty"`
	ExtendBy string `json:"extend_by,omitempty"`
	Dead     bool   `json:"dead,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request, route string) {
	req, ok := readLeaseRequest(w, r)
	if !ok {
		s.observeAck(route, http.StatusBadRequest, "", false)
		return
	}
	if req.LeaseID == "" {
		writeError(w, http.StatusBadRequest, pullErrInvalidBody, "lease_id is required")
		s.observeAck(route, http.StatusBadRequest, "", false)
		return
	}

	if err := s.Store.Ack(req.LeaseID); err != nil {
		if errors.Is(err, queue.ErrLeaseNotFound) || errors.Is(err, queue.ErrLeaseExpired) {
			writeError(w, http.StatusConflict, pullErrLeaseConflict, "lease is invalid or expired")
			s.observeAck(route, http.StatusConflict, req.LeaseID, errors.Is(err, queue.ErrLeaseExpired))
			return
		}
		writeError(w, http.StatusInternalServerError, pullErrInternal, "ack failed")
		s.observeAck(route, http.StatusInternalServerError, req.LeaseID, false)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s.observeAck(route, http.StatusNoContent, req.LeaseID, false)
}

func (s *Server) handleNack(w http.ResponseWriter, r *http.Request, route string) {
	req, ok := readLeaseRequest(w, r)
	if !ok {
		s.observeNack(route, http.StatusBadRequest, "", false)
		return
	}
	if req.LeaseID == "" {
		writeError(w, http.StatusBadRequest, pullErrInvalidBody, "lease_id is required")
		s.observeNack(route, http.StatusBadRequest, "", false)
		return
	}

	if req.Dead {
		if err := s.Store.MarkDead(req.LeaseID, req.Reason); err != nil {
			if errors.Is(err, queue.ErrLeaseNotFound) || errors.Is(err, queue.ErrLeaseExpired) {
				writeError(w, http.StatusConflict, pullErrLeaseConflict, "lease is invalid or expired")
				s.observeNack(route, http.StatusConflict, req.LeaseID, errors.Is(err, queue.ErrLeaseExpired))
				return
			}
			writeError(w, http.StatusInternalServerError, pullErrInternal, "mark dead failed")
			s.observeNack(route, http.StatusInternalServerError, req.LeaseID, false)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		s.observeNack(route, http.StatusNoContent, req.LeaseID, false)
		return
	}

	delay, ok := parseDuration(req.Delay)
	if !ok {
		writeError(w, http.StatusBadRequest, pullErrInvalidBody, "delay must be a valid duration")
		s.observeNack(route, http.StatusBadRequest, req.LeaseID, false)
		return
	}

	if err := s.Store.Nack(req.LeaseID, delay); err != nil {
		if errors.Is(err, queue.ErrLeaseNotFound) || errors.Is(err, queue.ErrLeaseExpired) {
			writeError(w, http.StatusConflict, pullErrLeaseConflict, "lease is invalid or expired")
			s.observeNack(route, http.StatusConflict, req.LeaseID, errors.Is(err, queue.ErrLeaseExpired))
			return
		}
		writeError(w, http.StatusInternalServerError, pullErrInternal, "nack failed")
		s.observeNack(route, http.StatusInternalServerError, req.LeaseID, false)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s.observeNack(route, http.StatusNoContent, req.LeaseID, false)
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

	if err := s.Store.Extend(req.LeaseID, extendBy); err != nil {
		if errors.Is(err, queue.ErrLeaseNotFound) || errors.Is(err, queue.ErrLeaseExpired) {
			writeError(w, http.StatusConflict, pullErrLeaseConflict, "lease is invalid or expired")
			s.observeExtend(route, http.StatusConflict, req.LeaseID, extendBy, errors.Is(err, queue.ErrLeaseExpired))
			return
		}
		writeError(w, http.StatusInternalServerError, pullErrInternal, "extend failed")
		s.observeExtend(route, http.StatusInternalServerError, req.LeaseID, extendBy, false)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s.observeExtend(route, http.StatusNoContent, req.LeaseID, extendBy, false)
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
