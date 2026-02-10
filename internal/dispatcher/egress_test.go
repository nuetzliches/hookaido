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
