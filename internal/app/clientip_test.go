package app

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

func mustPrefixes(t *testing.T, raw ...string) []netip.Prefix {
	t.Helper()
	out := make([]netip.Prefix, 0, len(raw))
	for _, r := range raw {
		pfx, err := netip.ParsePrefix(r)
		if err != nil {
			t.Fatalf("ParsePrefix(%q): %v", r, err)
		}
		out = append(out, pfx.Masked())
	}
	return out
}

func TestResolveClientIP(t *testing.T) {
	trusted := mustPrefixes(t, "10.0.0.0/8", "fd00::/8")

	tests := []struct {
		name       string
		remoteAddr string
		forwarded  []string
		trusted    []netip.Prefix
		want       string
		wantOK     bool
	}{
		{
			name:       "no trusted proxies configured ignores the header",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  []string{"203.0.113.9"},
			trusted:    nil,
			want:       "10.0.0.7",
			wantOK:     true,
		},
		{
			name:       "untrusted peer keeps its own address",
			remoteAddr: "198.51.100.4:41234",
			forwarded:  []string{"203.0.113.9"},
			trusted:    trusted,
			want:       "198.51.100.4",
			wantOK:     true,
		},
		{
			name:       "trusted peer takes the forwarded client",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  []string{"203.0.113.9"},
			trusted:    trusted,
			want:       "203.0.113.9",
			wantOK:     true,
		},
		{
			name:       "rightmost untrusted entry wins",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  []string{"203.0.113.9, 198.51.100.4, 10.0.0.9"},
			trusted:    trusted,
			want:       "198.51.100.4",
			wantOK:     true,
		},
		{
			name:       "client-supplied entries to the left are ignored",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  []string{"1.2.3.4, 203.0.113.9"},
			trusted:    trusted,
			want:       "203.0.113.9",
			wantOK:     true,
		},
		{
			name:       "several headers are read in arrival order",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  []string{"1.2.3.4", "203.0.113.9, 10.0.0.9"},
			trusted:    trusted,
			want:       "203.0.113.9",
			wantOK:     true,
		},
		{
			name:       "chain of only trusted hops falls back to the peer",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  []string{"10.0.0.5, 10.0.0.9"},
			trusted:    trusted,
			want:       "10.0.0.7",
			wantOK:     true,
		},
		{
			name:       "trusted peer without the header keeps its address",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  nil,
			trusted:    trusted,
			want:       "10.0.0.7",
			wantOK:     true,
		},
		{
			name:       "unparsable entry stops the walk",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  []string{"203.0.113.9, unknown, 10.0.0.9"},
			trusted:    trusted,
			want:       "10.0.0.7",
			wantOK:     true,
		},
		{
			name:       "forwarded entry with a port",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  []string{"203.0.113.9:1234"},
			trusted:    trusted,
			want:       "203.0.113.9",
			wantOK:     true,
		},
		{
			name:       "bracketed ipv6 without a port",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  []string{"[2001:db8::1]"},
			trusted:    trusted,
			want:       "2001:db8::1",
			wantOK:     true,
		},
		{
			name:       "bracketed ipv6 with a port",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  []string{"[2001:db8::1]:443"},
			trusted:    trusted,
			want:       "2001:db8::1",
			wantOK:     true,
		},
		{
			name:       "trusted ipv6 proxy",
			remoteAddr: "[fd00::1]:41234",
			forwarded:  []string{"203.0.113.9"},
			trusted:    trusted,
			want:       "203.0.113.9",
			wantOK:     true,
		},
		{
			name:       "empty entries are skipped",
			remoteAddr: "10.0.0.7:41234",
			forwarded:  []string{"203.0.113.9, , "},
			trusted:    trusted,
			want:       "203.0.113.9",
			wantOK:     true,
		},
		{
			name:       "unparsable peer stays unresolved",
			remoteAddr: "not-an-address",
			forwarded:  []string{"203.0.113.9"},
			trusted:    trusted,
			want:       "",
			wantOK:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodPost, "http://example.test/hook", nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			r.RemoteAddr = tt.remoteAddr
			for _, v := range tt.forwarded {
				r.Header.Add(headerXForwardedFor, v)
			}

			got, ok := resolveClientIP(r, tt.trusted)
			if ok != tt.wantOK {
				t.Fatalf("resolveClientIP ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if got.String() != tt.want {
				t.Fatalf("resolveClientIP = %q, want %q", got.String(), tt.want)
			}
		})
	}
}

func TestResolveClientIPNilRequest(t *testing.T) {
	if _, ok := resolveClientIP(nil, nil); ok {
		t.Fatal("expected a nil request to resolve nothing")
	}
}

func TestResolveIngress_RemoteIPBehindTrustedProxy(t *testing.T) {
	compiled := compileForReloadTest(t, `
ingress {
  listen :8080
  trusted_proxies "10.0.0.0/8"
}

"/x" {
  match {
    remote_ip "203.0.113.0/24"
  }
  deliver "https://example.org" {}
}
`)
	state := newRuntimeState(compiled)

	// The proxy's own address is not in the allowlist; without the header the
	// route stays unmatched — the failure mode the directive exists to explain.
	reqNoHeader := httptest.NewRequest(http.MethodPost, "http://hooks.example/x", nil)
	reqNoHeader.RemoteAddr = "10.0.0.7:41234"
	if route, ok := state.resolveIngress(reqNoHeader, "/x"); ok {
		t.Fatalf("expected the proxy address not to match the allowlist, got route=%q", route)
	}

	reqForwarded := httptest.NewRequest(http.MethodPost, "http://hooks.example/x", nil)
	reqForwarded.RemoteAddr = "10.0.0.7:41234"
	reqForwarded.Header.Set("X-Forwarded-For", "203.0.113.42")
	if route, ok := state.resolveIngress(reqForwarded, "/x"); !ok || route != "/x" {
		t.Fatalf("expected the forwarded client to match, got route=%q ok=%v", route, ok)
	}

	// A client that spoofs the header while talking to Hookaido directly gains
	// nothing: its peer address is not a trusted proxy, so the header is ignored.
	reqSpoofed := httptest.NewRequest(http.MethodPost, "http://hooks.example/x", nil)
	reqSpoofed.RemoteAddr = "198.51.100.4:41234"
	reqSpoofed.Header.Set("X-Forwarded-For", "203.0.113.42")
	if route, ok := state.resolveIngress(reqSpoofed, "/x"); ok {
		t.Fatalf("expected a spoofed header from an untrusted peer to be ignored, got route=%q", route)
	}
}

func TestResolveIngress_RemoteIPWithoutTrustedProxiesIgnoresForwardedFor(t *testing.T) {
	compiled := compileForReloadTest(t, `
"/x" {
  match {
    remote_ip "203.0.113.0/24"
  }
  deliver "https://example.org" {}
}
`)
	state := newRuntimeState(compiled)

	req := httptest.NewRequest(http.MethodPost, "http://hooks.example/x", nil)
	req.RemoteAddr = "10.0.0.7:41234"
	req.Header.Set("X-Forwarded-For", "203.0.113.42")
	if route, ok := state.resolveIngress(req, "/x"); ok {
		t.Fatalf("expected X-Forwarded-For to be ignored by default, got route=%q", route)
	}
}

func TestReload_PicksUpTrustedProxiesChange(t *testing.T) {
	compiled := compileForReloadTest(t, `
"/x" {
  match {
    remote_ip "203.0.113.0/24"
  }
  deliver "https://example.org" {}
}
`)
	state := newRuntimeState(compiled)

	req := httptest.NewRequest(http.MethodPost, "http://hooks.example/x", nil)
	req.RemoteAddr = "10.0.0.7:41234"
	req.Header.Set("X-Forwarded-For", "203.0.113.42")
	if _, ok := state.resolveIngress(req, "/x"); ok {
		t.Fatal("expected no match before trusted_proxies is configured")
	}

	state.updateAll(compileForReloadTest(t, `
ingress {
  listen :8080
  trusted_proxies "10.0.0.0/8"
}

"/x" {
  match {
    remote_ip "203.0.113.0/24"
  }
  deliver "https://example.org" {}
}
`))

	if route, ok := state.resolveIngress(req, "/x"); !ok || route != "/x" {
		t.Fatalf("expected the reload to apply trusted_proxies live, got route=%q ok=%v", route, ok)
	}
}

func TestAllowedMethodsFor_UsesForwardedClientIP(t *testing.T) {
	compiled := compileForReloadTest(t, `
ingress {
  listen :8080
  trusted_proxies "10.0.0.0/8"
}

"/x" {
  match {
    method PUT
    remote_ip "203.0.113.0/24"
  }
  deliver "https://example.org" {}
}
`)
	state := newRuntimeState(compiled)

	req := httptest.NewRequest(http.MethodPost, "http://hooks.example/x", nil)
	req.RemoteAddr = "10.0.0.7:41234"
	req.Header.Set("X-Forwarded-For", "203.0.113.42")

	// A 405 needs the same client address as the route match, or the Allow
	// header would be empty and the response a 404 instead.
	methods := state.allowedMethodsFor(req, "/x")
	if len(methods) != 1 || methods[0] != http.MethodPut {
		t.Fatalf("allowedMethodsFor = %#v, want [PUT]", methods)
	}
}
