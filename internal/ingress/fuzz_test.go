package ingress

import (
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func FuzzHMACAuthVerify(f *testing.F) {
	f.Add(http.MethodPost, "/hooks", []byte(`{"ok":true}`), int64(1735689600), "nonce-1", true)
	f.Add(http.MethodPost, "/hooks", []byte(`{}`), int64(1735689600), "", false)

	f.Fuzz(func(t *testing.T, method string, requestPath string, body []byte, ts int64, nonce string, useValidSignature bool) {
		if requestPath == "" {
			requestPath = "/hooks"
		}
		if !strings.HasPrefix(requestPath, "/") {
			requestPath = "/" + requestPath
		}
		if len(requestPath) > 256 {
			requestPath = requestPath[:256]
		}
		if len(body) > 1<<20 {
			body = body[:1<<20]
		}
		if len(nonce) > 256 {
			nonce = nonce[:256]
		}
		if strings.ContainsAny(nonce, "\r\n") {
			nonce = ""
		}
		if ts < 0 {
			ts = -ts
		}
		if ts > 4102444800 {
			ts = ts % 4102444800
		}

		now := time.Unix(1735689600, 0).UTC()
		auth := NewHMACAuth([][]byte{[]byte("fuzz-secret")})
		auth.Now = func() time.Time { return now }
		auth.Tolerance = 10 * 365 * 24 * time.Hour

		req := &http.Request{
			Method: method,
			URL: &url.URL{
				Scheme: "http",
				Host:   "example.test",
				Path:   requestPath,
			},
			Header: make(http.Header),
			Body:   io.NopCloser(strings.NewReader("")),
		}

		tsHeader := strconv.FormatInt(ts, 10)
		req.Header.Set(auth.TimestampHeader, tsHeader)
		req.Header.Set(auth.NonceHeader, nonce)

		signature := "deadbeef"
		if useValidSignature {
			signature = signHMAC(ts, method, requestPath, body, []byte("fuzz-secret"))
		}
		req.Header.Set(auth.SignatureHeader, signature)

		err := auth.Verify(req, requestPath, body)
		if err != nil && err != ErrUnauthorized {
			t.Fatalf("unexpected verify error: %v", err)
		}
	})
}
