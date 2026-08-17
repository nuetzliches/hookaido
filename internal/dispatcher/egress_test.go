package dispatcher

import (
	"context"
	"net"
	"net/netip"
	"testing"
)

type fakeResolver struct {
	records map[string][]net.IPAddr
	err     error
}

func (f fakeResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	if f.err != nil {
		return nil, f.err
	}
	if ips, ok := f.records[host]; ok {
		return ips, nil
	}
	return nil, nil
}

func TestEgressPolicy_HTTPSOnly(t *testing.T) {
	policy := EgressPolicy{HTTPSOnly: true}
	if err := checkEgressPolicy(context.Background(), "http://example.com", policy, nil); err == nil {
		t.Fatalf("expected https_only to deny http URL")
	}
	if err := checkEgressPolicy(context.Background(), "https://example.com", policy, nil); err != nil {
		t.Fatalf("expected https URL to pass, got %v", err)
	}
}

func TestEgressPolicy_DNSRebind_DeniesPrivateIP(t *testing.T) {
	policy := EgressPolicy{DNSRebindProtection: true}
	if err := checkEgressPolicy(context.Background(), "https://127.0.0.1/hook", policy, nil); err == nil {
		t.Fatalf("expected private ip to be denied")
	}
}

func TestEgressPolicy_DNSRebind_Resolver(t *testing.T) {
	policy := EgressPolicy{DNSRebindProtection: true}
	resolver := fakeResolver{
		records: map[string][]net.IPAddr{
			"public.example":  {{IP: net.ParseIP("93.184.216.34")}},
			"private.example": {{IP: net.ParseIP("10.0.0.1")}},
		},
	}

	if err := checkEgressPolicy(context.Background(), "https://public.example/hook", policy, resolver); err != nil {
		t.Fatalf("expected public host to pass, got %v", err)
	}
	if err := checkEgressPolicy(context.Background(), "https://private.example/hook", policy, resolver); err == nil {
		t.Fatalf("expected private host to be denied")
	}
}

func TestEgressPolicy_AllowDenyRules(t *testing.T) {
	allow := []EgressRule{{Host: "good.example"}}
	deny := []EgressRule{{CIDR: mustPrefix(t, "10.0.0.0/8"), IsCIDR: true}}
	policy := EgressPolicy{Allow: allow, Deny: deny}
	resolver := fakeResolver{
		records: map[string][]net.IPAddr{
			"good.example":  {{IP: net.ParseIP("93.184.216.34")}},
			"other.example": {{IP: net.ParseIP("93.184.216.34")}},
		},
	}

	if err := checkEgressPolicy(context.Background(), "https://good.example/hook", policy, resolver); err != nil {
		t.Fatalf("expected allowlisted host to pass, got %v", err)
	}
	if err := checkEgressPolicy(context.Background(), "https://other.example/hook", policy, resolver); err == nil {
		t.Fatalf("expected non-allowlisted host to be denied")
	}
	if err := checkEgressPolicy(context.Background(), "https://10.1.2.3/hook", policy, nil); err == nil {
		t.Fatalf("expected denied cidr host to be denied")
	}
}

func TestEgressPolicy_AllowWildcardHost(t *testing.T) {
	policy := EgressPolicy{
		Allow: []EgressRule{{Host: "*"}},
	}
	if err := checkEgressPolicy(context.Background(), "https://any.example/hook", policy, nil); err != nil {
		t.Fatalf("expected wildcard allow host to pass, got %v", err)
	}
}

func TestEgressPolicy_DenyWildcardHost(t *testing.T) {
	policy := EgressPolicy{
		Deny: []EgressRule{{Host: "*"}},
	}
	if err := checkEgressPolicy(context.Background(), "https://any.example/hook", policy, nil); err == nil {
		t.Fatalf("expected wildcard deny host to reject")
	}
}

func mustPrefix(t *testing.T, s string) netip.Prefix {
	t.Helper()
	p, err := netip.ParsePrefix(s)
	if err != nil {
		t.Fatalf("parse prefix: %v", err)
	}
	return p
}

func TestEgressPolicy_DenyBeforeAllow(t *testing.T) {
	// A host that matches both deny and allow should be denied (deny wins).
	policy := EgressPolicy{
		Allow: []EgressRule{{Host: "evil.example"}},
		Deny:  []EgressRule{{Host: "evil.example"}},
	}
	if err := checkEgressPolicy(context.Background(), "https://evil.example/hook", policy, nil); err == nil {
		t.Fatal("expected deny to take precedence over allow for same host")
	}
}

func TestEgressPolicy_DenyCIDROverridesAllowHost(t *testing.T) {
	// Host matches allow by name, but its IP falls in a denied CIDR.
	resolver := fakeResolver{
		records: map[string][]net.IPAddr{
			"sneaky.example": {{IP: net.ParseIP("10.0.0.5")}},
		},
	}
	policy := EgressPolicy{
		Allow: []EgressRule{{Host: "sneaky.example"}},
		Deny:  []EgressRule{{CIDR: mustPrefix(t, "10.0.0.0/8"), IsCIDR: true}},
	}
	if err := checkEgressPolicy(context.Background(), "https://sneaky.example/hook", policy, resolver); err == nil {
		t.Fatal("expected CIDR deny to override host allow")
	}
}

func TestEgressPolicy_SubdomainWildcard(t *testing.T) {
	policy := EgressPolicy{
		Allow: []EgressRule{{Host: "example.com", Subdomains: true}},
	}
	tests := []struct {
		host    string
		allowed bool
	}{
		{"https://sub.example.com/x", true},
		{"https://deep.sub.example.com/x", true},
		{"https://example.com/x", false}, // exact match excluded for Subdomains=true
		{"https://other.com/x", false},
	}
	for _, tt := range tests {
		err := checkEgressPolicy(context.Background(), tt.host, policy, nil)
		if tt.allowed && err != nil {
			t.Errorf("expected %s to be allowed, got %v", tt.host, err)
		}
		if !tt.allowed && err == nil {
			t.Errorf("expected %s to be denied", tt.host)
		}
	}
}

func TestEgressPolicy_DenyOnlyMode(t *testing.T) {
	// With only deny rules (no allow), everything except denied is permitted.
	policy := EgressPolicy{
		Deny: []EgressRule{{Host: "blocked.example"}},
	}
	if err := checkEgressPolicy(context.Background(), "https://ok.example/hook", policy, nil); err != nil {
		t.Fatalf("expected non-denied host to pass, got %v", err)
	}
	if err := checkEgressPolicy(context.Background(), "https://blocked.example/hook", policy, nil); err == nil {
		t.Fatal("expected denied host to be rejected")
	}
}

func TestEgressPolicy_EmptyPolicyAllowsAll(t *testing.T) {
	policy := EgressPolicy{}
	if err := checkEgressPolicy(context.Background(), "https://any.host/path", policy, nil); err != nil {
		t.Fatalf("expected empty policy to allow, got %v", err)
	}
}

func TestEgressPolicy_NonHTTPScheme(t *testing.T) {
	policy := EgressPolicy{}
	if err := checkEgressPolicy(context.Background(), "ftp://example.com/file", policy, nil); err == nil {
		t.Fatal("expected ftp scheme to be denied")
	}
}

func TestEgressPolicy_EmptyHost(t *testing.T) {
	policy := EgressPolicy{}
	if err := checkEgressPolicy(context.Background(), "https:///path", policy, nil); err == nil {
		t.Fatal("expected empty host to be denied")
	}
}

func TestEgressPolicy_DNSResolverError(t *testing.T) {
	resolver := fakeResolver{err: &net.DNSError{Err: "no such host", Name: "evil.test"}}
	policy := EgressPolicy{
		Deny: []EgressRule{{IsCIDR: true, CIDR: netip.MustParsePrefix("10.0.0.0/8")}},
	}
	if err := checkEgressPolicy(context.Background(), "https://evil.test/hook", policy, resolver); err == nil {
		t.Fatal("expected DNS resolver error to deny request")
	}
}

func TestEgressPolicy_AllowCIDRRequiresEveryResolvedAddress(t *testing.T) {
	// The documented workaround for private-network delivery targets. A host
	// answering with one in-range address must not clear the allowlist while the
	// dialer is still free to pick the metadata address alongside it.
	policy := EgressPolicy{Allow: []EgressRule{{CIDR: mustPrefix(t, "10.0.0.0/8"), IsCIDR: true}}}
	resolver := fakeResolver{
		records: map[string][]net.IPAddr{
			"internal.example": {{IP: net.ParseIP("10.1.1.5")}},
			"split.example": {
				{IP: net.ParseIP("10.1.1.5")},
				{IP: net.ParseIP("169.254.169.254")},
			},
		},
	}

	if err := checkEgressPolicy(context.Background(), "https://internal.example/hook", policy, resolver); err != nil {
		t.Fatalf("expected fully in-range host to pass, got %v", err)
	}
	if err := checkEgressPolicy(context.Background(), "https://split.example/hook", policy, resolver); err == nil {
		t.Fatalf("expected host with an out-of-range address to be denied")
	}
}

func TestEgressPolicy_AllowCIDRAcceptsAddressesSpreadAcrossRules(t *testing.T) {
	// Every address is covered by some allow rule, just not all by the same one.
	policy := EgressPolicy{Allow: []EgressRule{
		{CIDR: mustPrefix(t, "10.0.0.0/8"), IsCIDR: true},
		{CIDR: mustPrefix(t, "203.0.113.0/24"), IsCIDR: true},
	}}
	resolver := fakeResolver{
		records: map[string][]net.IPAddr{
			"dual.example": {
				{IP: net.ParseIP("10.1.1.5")},
				{IP: net.ParseIP("203.0.113.9")},
			},
		},
	}

	if err := checkEgressPolicy(context.Background(), "https://dual.example/hook", policy, resolver); err != nil {
		t.Fatalf("expected host covered by the union of allow rules to pass, got %v", err)
	}
}

func TestEgressPolicy_AllowHostRuleKeepsAnyMatchSemantics(t *testing.T) {
	// A hostname rule names the one host the request carries, so it stays
	// any-match even when a CIDR rule sits beside it and matches nothing.
	policy := EgressPolicy{Allow: []EgressRule{
		{Host: "good.example"},
		{CIDR: mustPrefix(t, "10.0.0.0/8"), IsCIDR: true},
	}}
	resolver := fakeResolver{
		records: map[string][]net.IPAddr{
			"good.example": {
				{IP: net.ParseIP("93.184.216.34")},
				{IP: net.ParseIP("198.51.100.7")},
			},
		},
	}

	if err := checkEgressPolicy(context.Background(), "https://good.example/hook", policy, resolver); err != nil {
		t.Fatalf("expected allowlisted hostname to pass, got %v", err)
	}
}

func TestIsAllowedIP_NonRoutableRanges(t *testing.T) {
	denied := []string{
		"0.0.0.0",
		"0.1.2.3",
		"10.0.0.1",
		"100.64.0.1",
		"100.100.100.200", // Alibaba instance metadata
		"127.0.0.1",
		"169.254.169.254", // AWS/GCP/Azure instance metadata
		"172.16.0.1",
		"192.0.0.170",
		"192.0.0.192", // Oracle instance metadata
		"192.168.1.1",
		"198.18.0.1",
		"240.0.0.1",
		"255.255.255.254",
		"::",
		"::1",
		"::127.0.0.1",
		"::ffff:10.0.0.1",        // IPv4-mapped private
		"::ffff:169.254.169.254", // IPv4-mapped metadata
		"64:ff9b::a9fe:a9fe",     // 169.254.169.254 behind NAT64
		"fc00::1",
		"fe80::1",
		"ff02::1",
	}
	for _, s := range denied {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("test setup: %q is not an ip", s)
		}
		if isAllowedIP(ip) {
			t.Errorf("isAllowedIP(%s) = true, want false", s)
		}
	}

	allowed := []string{
		"93.184.216.34",
		"8.8.8.8",
		"192.0.2.1", // RFC5737 documentation range, used as a public stand-in
		"2606:2800:220:1:248:1893:25c8:1946",
	}
	for _, s := range allowed {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("test setup: %q is not an ip", s)
		}
		if !isAllowedIP(ip) {
			t.Errorf("isAllowedIP(%s) = false, want true", s)
		}
	}

	if isAllowedIP(nil) {
		t.Errorf("isAllowedIP(nil) = true, want false")
	}
}
