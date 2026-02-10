package ingress

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultForwardAuthTimeout = 2 * time.Second

// ForwardAuth performs pre-enqueue authorization against an external endpoint.
// Behavior:
// - 2xx: allow
// - 401/403: deny with same status
// - timeout/network/5xx/other: fail closed with 503
type ForwardAuth struct {
	URL string

	Timeout time.Duration

	CopyHeaders []string

	BodyLimitBytes int64

	Client *http.Client
}

func NewForwardAuth(url string) *ForwardAuth {
	return &ForwardAuth{
		URL:     strings.TrimSpace(url),
		Timeout: defaultForwardAuthTimeout,
	}
}

func (a *ForwardAuth) Authorize(r *http.Request, requestPath string, body []byte) (map[string]string, int) {
	if a == nil || strings.TrimSpace(a.URL) == "" {
		return nil, 0
	}
	method := http.MethodPost
	if r != nil && strings.TrimSpace(r.Method) != "" {
		method = r.Method
	}

	timeout := a.Timeout
	if timeout <= 0 {
		timeout = defaultForwardAuthTimeout
	}
	baseCtx := context.Background()
	if r != nil && r.Context() != nil {
		baseCtx = r.Context()
	}
	ctx, cancel := context.WithTimeout(baseCtx, timeout)
	defer cancel()

	var reqBody []byte
	if len(body) > 0 {
		reqBody = body
		if a.BodyLimitBytes > 0 && int64(len(reqBody)) > a.BodyLimitBytes {
			reqBody = reqBody[:a.BodyLimitBytes]
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, a.URL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, http.StatusServiceUnavailable
	}

	if r != nil {
		for k, v := range r.Header {
			for _, vv := range v {
				req.Header.Add(k, vv)
			}
		}
	}
	if strings.TrimSpace(requestPath) != "" {
		req.Header.Set("X-Hookaido-Original-Path", requestPath)
	}
	req.Header.Set("X-Hookaido-Original-Method", method)

	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, http.StatusServiceUnavailable
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if len(a.CopyHeaders) == 0 {
			return nil, 0
		}
		out := make(map[string]string, len(a.CopyHeaders))
		for _, name := range a.CopyHeaders {
			n := http.CanonicalHeaderKey(strings.TrimSpace(name))
			if n == "" {
				continue
			}
			values := resp.Header.Values(n)
			if len(values) == 0 {
				continue
			}
			out[n] = strings.Join(values, ",")
		}
		return out, 0
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, resp.StatusCode
	default:
		return nil, http.StatusServiceUnavailable
	}
}
