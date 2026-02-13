package ingress

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/nuetzliches/hookaido/internal/queue"
)

type Server struct {
	Store                 queue.Store
	Target                string
	ResolveRoute          func(r *http.Request, requestPath string) (route string, ok bool)
	AllowedMethodsFor     func(r *http.Request, requestPath string) []string
	AllowRequestFor       func(route string) bool
	AllowEnqueueFor       func(route string) (allowed bool, statusCode int, reason string)
	BasicAuthFor          func(route string) *BasicAuth
	ForwardAuthFor        func(route string) *ForwardAuth
	HMACAuthFor           func(route string) *HMACAuth
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

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestPath := path.Clean(r.URL.Path)
	route, ok := s.resolveRoute(r, requestPath)
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

	if s.BasicAuthFor != nil {
		if a := s.BasicAuthFor(route); a != nil {
			if !a.Verify(r) {
				w.WriteHeader(http.StatusUnauthorized)
				s.observe(false, 0)
				s.observeReject(route, http.StatusUnauthorized, "auth")
				return
			}
		}
	}

	maxBody := s.MaxBodyBytes
	maxHeaders := s.MaxHeaderBytes
	if s.LimitsFor != nil {
		mb, mh := s.LimitsFor(route)
		if mb > 0 {
			maxBody = mb
		}
		if mh > 0 {
			maxHeaders = mh
		}
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
	if s.ForwardAuthFor != nil {
		if a := s.ForwardAuthFor(route); a != nil {
			copied, status := a.Authorize(r, requestPath, body)
			if status != 0 {
				w.WriteHeader(status)
				s.observe(false, 0)
				s.observeReject(route, status, "auth")
				return
			}
			forwardCopied = copied
		}
	}

	if s.HMACAuthFor != nil {
		if a := s.HMACAuthFor(route); a != nil {
			if err := a.Verify(r, requestPath, body); err != nil {
				w.WriteHeader(http.StatusUnauthorized)
				s.observe(false, 0)
				s.observeReject(route, http.StatusUnauthorized, "auth")
				return
			}
		}
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
	if s.TargetsFor != nil {
		if t := s.TargetsFor(route); len(t) > 0 {
			targets = t
		}
	}

	enqueued := 0
	for _, target := range targets {
		env.Target = target
		if err := s.Store.Enqueue(env); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			s.observe(false, enqueued)
			reason := "other"
			if errors.Is(err, queue.ErrQueueFull) {
				reason = "queue_full"
			}
			s.observeReject(route, http.StatusServiceUnavailable, reason)
			return
		}
		enqueued++
	}

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

func copyHeaders(h http.Header, maxBytes int) map[string]string {
	out, _ := copyHeadersWithExtra(h, maxBytes, nil)
	return out
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
