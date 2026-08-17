package dispatcher

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// The policy check resolves the host and validates the answers, then hands the
// *hostname* to the transport, which resolves it again. This reproduces what
// happens when the second answer differs from the first — DNS rebinding — by
// giving the check a resolver that reports a public address for a name the
// operating system resolves to loopback.
//
// Before the dial-time guard the delivery went through: the check passed on the
// reported address and the connection was made to the real one.
func TestHTTPDeliverer_RebindBetweenCheckAndDialIsRefused(t *testing.T) {
	reached := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server url: %v", err)
	}
	// httptest listens on 127.0.0.1; address it by a name the OS also resolves
	// to loopback, so the transport's own lookup differs from the stub's.
	target := "http://localhost:" + u.Port() + "/hook"

	policy := EgressPolicy{DNSRebindProtection: true}
	d := NewHTTPDeliverer(nil, policy)
	// The lie: the check is told the host is public.
	d.Resolver = fakeResolver{records: map[string][]net.IPAddr{
		"localhost": {{IP: net.ParseIP("203.0.113.10")}},
	}}

	// Sanity: the URL-level check accepts it, which is why the dial-time guard
	// has to be the thing that refuses.
	if err := checkEgressPolicy(context.Background(), target, policy, d.Resolver); err != nil {
		t.Fatalf("expected the URL-level check to pass on the reported address, got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := d.Deliver(ctx, Delivery{
		URL:    target,
		Method: http.MethodPost,
		Body:   []byte(`{}`),
	})

	if reached {
		t.Fatal("the delivery reached the loopback server: the connection was not checked against the address actually dialed")
	}
	if res.Err == nil {
		t.Fatalf("expected an error, got status %d", res.StatusCode)
	}
	if !strings.Contains(res.Err.Error(), "disallowed ip") {
		t.Fatalf("expected an egress-policy refusal naming the ip, got %v", res.Err)
	}
}

func TestEgressDialControl(t *testing.T) {
	deny := EgressPolicy{
		Deny: []EgressRule{{IsCIDR: true, CIDR: mustPrefix(t, "203.0.113.0/24")}},
	}
	rebind := EgressPolicy{DNSRebindProtection: true}

	cases := []struct {
		name    string
		policy  EgressPolicy
		address string
		wantErr bool
	}{
		{name: "rebind blocks metadata endpoint", policy: rebind, address: "169.254.169.254:80", wantErr: true},
		{name: "rebind blocks loopback", policy: rebind, address: "127.0.0.1:8080", wantErr: true},
		{name: "rebind blocks rfc1918", policy: rebind, address: "10.1.2.3:443", wantErr: true},
		{name: "rebind allows public", policy: rebind, address: "203.0.113.10:443", wantErr: false},
		{name: "deny cidr blocks", policy: deny, address: "203.0.113.10:443", wantErr: true},
		{name: "deny cidr allows outside", policy: deny, address: "198.51.100.7:443", wantErr: false},
		{name: "malformed address refused", policy: rebind, address: "not-an-address", wantErr: true},
		{name: "unresolved host refused", policy: rebind, address: "example.com:443", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := egressDialControl(tc.policy)("tcp", tc.address, nil)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected %q to be refused", tc.address)
				}
				if !errors.Is(err, ErrPolicyDenied) {
					t.Fatalf("expected ErrPolicyDenied, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected %q to be allowed, got %v", tc.address, err)
			}
		})
	}
}

// The guard has to survive the transport being wrapped for tracing, which is
// how it reaches delivery in a traced deployment.
func TestNewEgressTransport_GuardsDialContext(t *testing.T) {
	tr := NewEgressTransport(EgressPolicy{DNSRebindProtection: true})
	if tr.DialContext == nil {
		t.Fatal("expected the transport to carry a guarded DialContext")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := tr.DialContext(ctx, "tcp", "127.0.0.1:9"); !errors.Is(err, ErrPolicyDenied) {
		t.Fatalf("expected a policy refusal dialing loopback, got %v", err)
	}
}
