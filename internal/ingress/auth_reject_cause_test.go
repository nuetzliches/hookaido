package ingress

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// Every canonical-HMAC refusal has to arrive with a cause attached. Before the
// split, all of these produced one indistinguishable `auth` reject, which is
// what made an emptied secret pool look like a misconfigured sender.
func TestHMACAuth_VerifyClassifiesRejectCause(t *testing.T) {
	secret := []byte("s1")
	ts := time.Unix(1735689600, 0).UTC()
	body := []byte("payload")
	path := "/hooks"

	newAuth := func() *HMACAuth {
		a := NewHMACAuth([][]byte{secret})
		a.Now = func() time.Time { return ts }
		return a
	}
	request := func(a *HMACAuth, tsHeader, nonce, sig string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "http://example.com"+path, bytes.NewReader(body))
		if tsHeader != "" {
			req.Header.Set(a.TimestampHeader, tsHeader)
		}
		if nonce != "" {
			req.Header.Set(a.NonceHeader, nonce)
		}
		if sig != "" {
			req.Header.Set(a.SignatureHeader, sig)
		}
		return req
	}
	valid := signHMAC(ts.Unix(), http.MethodPost, path, body, secret)

	tests := []struct {
		name  string
		build func() (*HMACAuth, *http.Request)
		want  string
	}{
		{
			name: "empty pool",
			build: func() (*HMACAuth, *http.Request) {
				a := NewHMACAuth(nil)
				a.Now = func() time.Time { return ts }
				a.SelectSecrets = func(time.Time) [][]byte { return nil }
				return a, request(a, strconv.FormatInt(ts.Unix(), 10), "n1", valid)
			},
			want: AuthRejectNoValidSecret,
		},
		{
			name: "wrong signature",
			build: func() (*HMACAuth, *http.Request) {
				a := newAuth()
				return a, request(a, strconv.FormatInt(ts.Unix(), 10), "n1", signHMAC(ts.Unix(), http.MethodPost, path, body, []byte("other")))
			},
			want: AuthRejectSignatureMismatch,
		},
		{
			name: "stale timestamp",
			build: func() (*HMACAuth, *http.Request) {
				a := newAuth()
				stale := ts.Add(-time.Hour).Unix()
				return a, request(a, strconv.FormatInt(stale, 10), "n1", signHMAC(stale, http.MethodPost, path, body, secret))
			},
			want: AuthRejectTimestampOutOfWindow,
		},
		{
			name: "missing signature header",
			build: func() (*HMACAuth, *http.Request) {
				a := newAuth()
				return a, request(a, strconv.FormatInt(ts.Unix(), 10), "n1", "")
			},
			want: AuthRejectMalformed,
		},
		{
			name: "non numeric timestamp",
			build: func() (*HMACAuth, *http.Request) {
				a := newAuth()
				return a, request(a, "not-a-number", "n1", valid)
			},
			want: AuthRejectMalformed,
		},
		{
			name: "signature not hex",
			build: func() (*HMACAuth, *http.Request) {
				a := newAuth()
				return a, request(a, strconv.FormatInt(ts.Unix(), 10), "n1", "zzzz")
			},
			want: AuthRejectMalformed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, req := tc.build()
			_, err := a.Verify(req, path, body)
			if err == nil {
				t.Fatalf("expected the request to be refused")
			}
			if !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("err = %v, want it to wrap ErrUnauthorized", err)
			}
			if got := AuthRejectReason(err); got != tc.want {
				t.Fatalf("AuthRejectReason = %q, want %q (err %v)", got, tc.want, err)
			}
		})
	}
}

func TestHMACAuth_ReplayedNonceIsItsOwnCause(t *testing.T) {
	secret := []byte("s1")
	ts := time.Unix(1735689600, 0).UTC()
	auth, mk, body, path := newCanonicalAuth(t, secret, ts)
	valid := signHMAC(ts.Unix(), http.MethodPost, path, body, secret)

	claim, err := auth.Verify(mk("n1", valid), path, body)
	if err != nil {
		t.Fatalf("first verify: %v", err)
	}
	claim.Commit()

	_, err = auth.Verify(mk("n1", valid), path, body)
	if err == nil {
		t.Fatalf("expected the replay to be refused")
	}
	// A replay is neither a bad signature (it verified) nor an empty pool. It
	// used to be reported as the same thing as both.
	if got := AuthRejectReason(err); got != AuthRejectReplay {
		t.Fatalf("AuthRejectReason = %q, want %q", got, AuthRejectReplay)
	}
}

func TestHMACAuth_ProviderEmptyPoolIsNoValidSecret(t *testing.T) {
	body := []byte(`{"ok":true}`)
	auth := NewHMACAuth(nil)
	auth.Provider = "github"
	auth.SelectSecrets = func(time.Time) [][]byte { return nil }

	req := httptest.NewRequest(http.MethodPost, "http://example.com/hooks", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signGitHub(body, []byte("s1")))

	_, err := auth.Verify(req, "/hooks", body)
	if got := AuthRejectReason(err); got != AuthRejectNoValidSecret {
		t.Fatalf("AuthRejectReason = %q, want %q (err %v)", got, AuthRejectNoValidSecret, err)
	}
}

func TestHMACAuth_ProviderWrongSignatureIsMismatch(t *testing.T) {
	body := []byte(`{"ok":true}`)
	auth := NewHMACAuth([][]byte{[]byte("s1")})
	auth.Provider = "github"

	req := httptest.NewRequest(http.MethodPost, "http://example.com/hooks", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", signGitHub(body, []byte("other")))

	_, err := auth.Verify(req, "/hooks", body)
	if got := AuthRejectReason(err); got != AuthRejectSignatureMismatch {
		t.Fatalf("AuthRejectReason = %q, want %q (err %v)", got, AuthRejectSignatureMismatch, err)
	}
}

func TestQueryAuth_VerifyCauseSeparatesEmptyPoolFromWrongToken(t *testing.T) {
	newReq := func(query string) *http.Request {
		return httptest.NewRequest(http.MethodPost, "http://example.com/hooks?"+query, nil)
	}

	// An empty pool: `auth query` answers 404 either way, so the cause is the
	// only thing that tells an operator this is their outage and not a caller's
	// bad token.
	empty := NewQueryAuth("token", nil)
	empty.SelectSecrets = func(time.Time) [][]byte { return nil }
	ok, cause := empty.VerifyCause(newReq("token=whatever"))
	if ok {
		t.Fatalf("expected refusal when the pool holds no valid version")
	}
	if cause != AuthRejectNoValidSecret {
		t.Fatalf("cause = %q, want %q", cause, AuthRejectNoValidSecret)
	}

	live := NewQueryAuth("token", [][]byte{[]byte("right")})
	if ok, cause := live.VerifyCause(newReq("token=wrong")); ok || cause != AuthRejectCredentials {
		t.Fatalf("wrong token: (ok, cause) = (%v, %q), want (false, %q)", ok, cause, AuthRejectCredentials)
	}
	if ok, cause := live.VerifyCause(newReq("other=right")); ok || cause != AuthRejectCredentials {
		t.Fatalf("missing param: (ok, cause) = (%v, %q), want (false, %q)", ok, cause, AuthRejectCredentials)
	}
	if ok, cause := live.VerifyCause(newReq("token=right")); !ok || cause != "" {
		t.Fatalf("valid token: (ok, cause) = (%v, %q), want (true, \"\")", ok, cause)
	}
}

// The handler has to report both views for the same request: the coarse
// `auth` bucket every reject reason shares, and the classified cause plus the
// route it happened on.
func TestServeHTTP_ObserveAuthRejectCarriesRouteAndCause(t *testing.T) {
	ts := time.Unix(1735689600, 0).UTC()
	auth := NewHMACAuth(nil)
	auth.Now = func() time.Time { return ts }
	auth.SelectSecrets = func(time.Time) [][]byte { return nil }

	type authReject struct {
		route  string
		reason string
	}
	type coarseReject struct {
		route  string
		status int
		reason string
	}
	var authRejects []authReject
	var rejects []coarseReject

	s := NewServer(nil)
	s.ResolveRequest = func(*http.Request, string) (RouteSnapshot, bool) {
		return RouteSnapshot{Route: "/webhooks/source", HMACAuth: auth}, true
	}
	s.ObserveReject = func(route string, statusCode int, reason string) {
		rejects = append(rejects, coarseReject{route, statusCode, reason})
	}
	s.ObserveAuthReject = func(route string, reason string) {
		authRejects = append(authRejects, authReject{route, reason})
	}

	body := []byte("payload")
	req := httptest.NewRequest(http.MethodPost, "http://example.com/webhooks/source", bytes.NewReader(body))
	req.Header.Set(auth.TimestampHeader, strconv.FormatInt(ts.Unix(), 10))
	req.Header.Set(auth.NonceHeader, "n1")
	req.Header.Set(auth.SignatureHeader, signHMAC(ts.Unix(), http.MethodPost, "/webhooks/source", body, []byte("s1")))

	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}
	if len(rejects) != 1 || rejects[0] != (coarseReject{"/webhooks/source", http.StatusUnauthorized, "auth"}) {
		t.Fatalf("ObserveReject calls = %#v, want one auth/401 on /webhooks/source", rejects)
	}
	if len(authRejects) != 1 {
		t.Fatalf("ObserveAuthReject calls = %#v, want exactly one", authRejects)
	}
	if authRejects[0].route != "/webhooks/source" {
		t.Fatalf("auth reject route = %q, want %q", authRejects[0].route, "/webhooks/source")
	}
	if authRejects[0].reason != AuthRejectNoValidSecret {
		t.Fatalf("auth reject reason = %q, want %q", authRejects[0].reason, AuthRejectNoValidSecret)
	}
}
