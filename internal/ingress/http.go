package ingress

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/nuetzliches/hookaido/v2/internal/queue"
)

// RouteSnapshot is the configuration a single request is served with.
//
// It exists so the handler can take everything it needs in one go. Resolving
// the route and then looking each piece up again by route name lets a config
// reload land in between: the lookups then miss, and a missing authenticator is
// indistinguishable from "this route has no auth configured".
type RouteSnapshot struct {
	Route       string
	BasicAuth   *BasicAuth
	ForwardAuth *ForwardAuth
	HMACAuth    *HMACAuth
	QueryAuth   *QueryAuth
	MaxBody     int64
	MaxHeaders  int
	Targets     []string
}

type Server struct {
	Store  queue.Store
	Target string

	// ResolveRequest returns the whole per-request configuration at once and,
	// when set, supersedes ResolveRoute, BasicAuthFor, ForwardAuthFor,
	// HMACAuthFor, LimitsFor and TargetsFor. The runtime wires this so the
	// snapshot is taken under a single lock; see snapshot below.
	ResolveRequest func(r *http.Request, requestPath string) (RouteSnapshot, bool)

	ResolveRoute          func(r *http.Request, requestPath string) (route string, ok bool)
	AllowedMethodsFor     func(r *http.Request, requestPath string) []string
	AllowRequestFor       func(route string) bool
	AllowEnqueueFor       func(route string) (allowed bool, statusCode int, reason string)
	BasicAuthFor          func(route string) *BasicAuth
	ForwardAuthFor        func(route string) *ForwardAuth
	HMACAuthFor           func(route string) *HMACAuth
	QueryAuthFor          func(route string) *QueryAuth
	LimitsFor             func(route string) (maxBodyBytes int64, maxHeaderBytes int)
	TargetsFor            func(route string) []string
	ObserveResult         func(accepted bool, enqueued int)
	ObserveAdaptiveReject func(route string, reason string)
	ObserveReject         func(route string, statusCode int, reason string)
	MaxBodyBytes          int64
	MaxHeaderBytes        int
}

func NewServer(store queue.Store) *Server {
	return &Server{
		Store:          store,
		Target:         "pull",
		ResolveRoute:   nil,
		HMACAuthFor:    nil,
		MaxBodyBytes:   2 << 20,  // 2 MiB (default in DESIGN.md)
		MaxHeaderBytes: 64 << 10, // 64 KiB (default in DESIGN.md)
	}
}

// snapshot resolves the route and everything else the request is served with.
//
// When ResolveRequest is wired it is authoritative and takes all of it under a
// single lock. That is the point: the handler used to resolve the route and
// then look each piece up again by route name, so a reload landing between
// those lookups returned nil for a route that had just been renamed or removed
// — and nil reads as "no auth configured" a few lines further down.
//
// The per-hook fallback below has that interleaving problem by construction. It
// exists for callers that wire the individual funcs, which in practice means
// tests; the runtime wires ResolveRequest.
func (s *Server) snapshot(r *http.Request, requestPath string) (RouteSnapshot, bool) {
	if s.ResolveRequest != nil {
		return s.ResolveRequest(r, requestPath)
	}

	route, ok := s.resolveRoute(r, requestPath)
	if !ok {
		return RouteSnapshot{}, false
	}
	snap := RouteSnapshot{Route: route}
	if s.BasicAuthFor != nil {
		snap.BasicAuth = s.BasicAuthFor(route)
	}
	if s.ForwardAuthFor != nil {
		snap.ForwardAuth = s.ForwardAuthFor(route)
	}
	if s.HMACAuthFor != nil {
		snap.HMACAuth = s.HMACAuthFor(route)
	}
	if s.QueryAuthFor != nil {
		snap.QueryAuth = s.QueryAuthFor(route)
	}
	if s.LimitsFor != nil {
		snap.MaxBody, snap.MaxHeaders = s.LimitsFor(route)
	}
	if s.TargetsFor != nil {
		snap.Targets = s.TargetsFor(route)
	}
	return snap, true
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestPath := path.Clean(r.URL.Path)
	snap, ok := s.snapshot(r, requestPath)
	route := snap.Route
	if !ok {
		if s.AllowedMethodsFor != nil {
			if allowed := s.AllowedMethodsFor(r, requestPath); len(allowed) > 0 {
				w.Header().Set("Allow", strings.Join(allowed, ", "))
				w.WriteHeader(http.StatusMethodNotAllowed)
				s.observe(false, 0)
				s.observeReject("", http.StatusMethodNotAllowed, "not_found")
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
		s.observe(false, 0)
		s.observeReject("", http.StatusNotFound, "not_found")
		return
	}

	// `auth query` is checked here -- before the rate limiter, before the
	// adaptive gate and before the body is read -- and answers 404 rather than
	// 401.
	//
	// Both are deliberate, and both preserve what the `match query` workaround
	// this variant replaces already did. A source that can only be handed a URL
	// has no client that benefits from a distinguishable auth error, and 404
	// avoids confirming that the path exists at all. Rejecting before the
	// limiter means a wrong token costs no queue work and cannot consume the
	// route's token budget, which is exactly how a matcher miss behaves today.
	if a := snap.QueryAuth; a != nil && !a.Verify(r) {
		w.WriteHeader(http.StatusNotFound)
		s.observe(false, 0)
		// Reported as "auth" rather than "not_found" so an operator can still
		// tell a wrong token from a genuinely unknown path. The token itself is
		// never a label.
		s.observeReject(route, http.StatusNotFound, "auth")
		return
	}

	if s.AllowRequestFor != nil && !s.AllowRequestFor(route) {
		w.WriteHeader(http.StatusTooManyRequests)
		s.observe(false, 0)
		s.observeReject(route, http.StatusTooManyRequests, "rate_limit")
		return
	}
	if s.AllowEnqueueFor != nil {
		allowed, statusCode, reason := s.AllowEnqueueFor(route)
		if !allowed {
			if statusCode <= 0 {
				statusCode = http.StatusServiceUnavailable
			}
			w.WriteHeader(statusCode)
			s.observe(false, 0)
			s.observeReject(route, statusCode, "adaptive_backpressure")
			if s.ObserveAdaptiveReject != nil {
				s.ObserveAdaptiveReject(route, strings.TrimSpace(reason))
			}
			return
		}
	}

	if a := snap.BasicAuth; a != nil && !a.Verify(r) {
		w.WriteHeader(http.StatusUnauthorized)
		s.observe(false, 0)
		s.observeReject(route, http.StatusUnauthorized, "auth")
		return
	}

	maxBody := s.MaxBodyBytes
	maxHeaders := s.MaxHeaderBytes
	if snap.MaxBody > 0 {
		maxBody = snap.MaxBody
	}
	if snap.MaxHeaders > 0 {
		maxHeaders = snap.MaxHeaders
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			s.observe(false, 0)
			s.observeReject(route, http.StatusRequestEntityTooLarge, "policy")
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		s.observe(false, 0)
		s.observeReject(route, http.StatusBadRequest, "policy")
		return
	}

	var forwardCopied map[string]string
	if a := snap.ForwardAuth; a != nil {
		copied, status := a.Authorize(r, requestPath, body)
		if status != 0 {
			w.WriteHeader(status)
			s.observe(false, 0)
			s.observeReject(route, status, "auth")
			return
		}
		forwardCopied = copied
	}

	// The nonce claim taken by HMAC verification stays provisional until the
	// request is durably enqueued. Releasing it on every path that does not
	// reach the 202 is what keeps a 503 retryable: the sender's identical
	// signed retry is the normal reaction to backpressure, and a burned nonce
	// turned it into a 401 for the rest of the tolerance window.
	var nonceClaim *NonceClaim
	defer func() { nonceClaim.Release() }()

	if a := snap.HMACAuth; a != nil {
		claim, err := a.Verify(r, requestPath, body)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			s.observe(false, 0)
			s.observeReject(route, http.StatusUnauthorized, "auth")
			return
		}
		nonceClaim = claim
	}

	env := queue.Envelope{
		Route:   route,
		Payload: body,
		Trace: map[string]string{
			"remote_addr": r.RemoteAddr,
			"path":        requestPath,
		},
	}
	headers, ok := copyHeadersWithExtra(r.Header, maxHeaders, forwardCopied)
	if !ok {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		s.observe(false, 0)
		s.observeReject(route, http.StatusRequestEntityTooLarge, "policy")
		return
	}
	env.Headers = headers

	targets := []string{s.Target}
	if len(snap.Targets) > 0 {
		targets = snap.Targets
	}

	enqueued := 0
	for _, target := range targets {
		env.Target = target
		if err := s.Store.Enqueue(env); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			s.observe(false, enqueued)
			reason := "other"
			switch {
			case errors.Is(err, queue.ErrMemoryPressure):
				reason = "memory_pressure"
			case errors.Is(err, queue.ErrQueueFull):
				reason = "queue_full"
			}
			s.observeReject(route, http.StatusServiceUnavailable, reason)
			return
		}
		enqueued++
	}

	// Every target is durably enqueued: the nonce claim becomes permanent, and
	// any later replay of this exact request is rejected for the rest of the
	// tolerance window.
	nonceClaim.Commit()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "queued"})
	s.observe(true, enqueued)
}

func (s *Server) observe(accepted bool, enqueued int) {
	if s.ObserveResult != nil {
		s.ObserveResult(accepted, enqueued)
	}
}

func (s *Server) observeReject(route string, statusCode int, reason string) {
	if s.ObserveReject != nil {
		s.ObserveReject(route, statusCode, strings.TrimSpace(reason))
	}
}

func copyHeadersWithExtra(h http.Header, maxBytes int, extra map[string]string) (map[string]string, bool) {
	if maxBytes <= 0 {
		return nil, len(h) == 0 && len(extra) == 0
	}

	out := make(map[string]string, len(h)+len(extra))
	for k, v := range h {
		lower := strings.ToLower(k)
		switch lower {
		case "authorization", "proxy-authorization", "cookie":
			continue
		}

		joined := strings.Join(v, ",")
		out[http.CanonicalHeaderKey(k)] = joined
	}
	out = appendHeaderExtras(out, extra)
	if headerKVSize(out) > maxBytes {
		return nil, false
	}

	if len(out) == 0 {
		return nil, true
	}
	return out, true
}

func appendHeaderExtras(headers map[string]string, extra map[string]string) map[string]string {
	if len(extra) == 0 {
		return headers
	}
	if headers == nil {
		headers = make(map[string]string, len(extra))
	}

	for key, value := range extra {
		name := http.CanonicalHeaderKey(strings.TrimSpace(key))
		if name == "" {
			continue
		}
		headers[name] = value
	}
	return headers
}

func headerKVSize(headers map[string]string) int {
	total := 0
	for k, v := range headers {
		total += len(k) + len(v)
	}
	return total
}

func (s *Server) resolveRoute(req *http.Request, requestPath string) (string, bool) {
	if s.ResolveRoute == nil {
		return requestPath, true
	}
	return s.ResolveRoute(req, requestPath)
}
