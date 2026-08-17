package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
)

var ErrPolicyDenied = errors.New("egress policy denied")

type EgressPolicy struct {
	HTTPSOnly           bool
	Redirects           bool
	DNSRebindProtection bool

	Allow []EgressRule
	Deny  []EgressRule
}

type EgressRule struct {
	Host       string
	Subdomains bool
	CIDR       netip.Prefix
	IsCIDR     bool
}

type resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

func checkEgressPolicy(ctx context.Context, rawURL string, policy EgressPolicy, r resolver) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	return checkEgressPolicyURL(ctx, u, policy, r)
}

func checkEgressPolicyURL(ctx context.Context, u *url.URL, policy EgressPolicy, r resolver) error {
	if u == nil {
		return fmt.Errorf("%w: empty url", ErrPolicyDenied)
	}

	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return fmt.Errorf("%w: scheme %q is not allowed", ErrPolicyDenied, u.Scheme)
	}

	if policy.HTTPSOnly && strings.ToLower(u.Scheme) != "https" {
		return fmt.Errorf("%w: https_only enforced", ErrPolicyDenied)
	}

	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return fmt.Errorf("%w: empty host", ErrPolicyDenied)
	}

	needIPs := policy.DNSRebindProtection || hasCIDRRules(policy)
	ips, err := resolveHostIPs(ctx, host, needIPs, r)
	if err != nil {
		return err
	}

	if policy.DNSRebindProtection {
		for _, ip := range ips {
			if !isAllowedIP(ip) {
				return fmt.Errorf("%w: host %q resolves to disallowed ip %s", ErrPolicyDenied, host, ip.String())
			}
		}
	}

	if len(policy.Deny) > 0 && matchEgressDenyRules(host, ips, policy.Deny) {
		return fmt.Errorf("%w: host %q denied by egress policy", ErrPolicyDenied, host)
	}

	if len(policy.Allow) > 0 && !matchEgressAllowRules(host, ips, policy.Allow) {
		return fmt.Errorf("%w: host %q not in egress allowlist", ErrPolicyDenied, host)
	}

	return nil
}

func hasCIDRRules(policy EgressPolicy) bool {
	for _, r := range policy.Allow {
		if r.IsCIDR {
			return true
		}
	}
	for _, r := range policy.Deny {
		if r.IsCIDR {
			return true
		}
	}
	return false
}

func resolveHostIPs(ctx context.Context, host string, needIPs bool, r resolver) ([]net.IP, error) {
	if !needIPs {
		return nil, nil
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		return []net.IP{net.IP(addr.AsSlice())}, nil
	}
	if r == nil {
		r = net.DefaultResolver
	}
	addrs, err := r.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, a := range addrs {
		if a.IP == nil {
			continue
		}
		ips = append(ips, a.IP)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("dns lookup returned no addresses for %q", host)
	}
	return ips, nil
}

// matchEgressCIDRRules reports whether ip falls inside any CIDR rule. Host
// rules are ignored: at dial time only the address is known, and a hostname
// rule cannot be decided from it.
func matchEgressCIDRRules(ip net.IP, rules []EgressRule) bool {
	addr, ok := netipFromIP(ip)
	if !ok {
		return false
	}
	for _, r := range rules {
		if r.IsCIDR && r.CIDR.Contains(addr) {
			return true
		}
	}
	return false
}

// egressDialControl returns a net.Dialer Control hook that re-checks the
// address the connection is actually being made to.
//
// checkEgressPolicyURL resolves the host and validates the answers, but then
// hands the *hostname* to the transport, which resolves it again independently.
// Between those two lookups the answer can change — which is precisely what DNS
// rebinding is, and what dns_rebind_protection claims to stop. Control runs
// after resolution with the concrete peer address, so validating here checks
// the address that will actually be connected to rather than one that merely
// was returned earlier.
//
// Scope is deliberately narrow: rebind protection and deny rules, both of which
// a bare IP decides unambiguously. Allow rules stay at the URL level, because a
// hostname allow rule cannot be evaluated from an address alone.
func egressDialControl(policy EgressPolicy) func(network, address string, c syscall.RawConn) error {
	return func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("%w: malformed dial address %q", ErrPolicyDenied, address)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			// Control is documented to receive a resolved address; if it ever
			// does not, refusing is the safe reading.
			return fmt.Errorf("%w: dial address %q is not an ip", ErrPolicyDenied, address)
		}
		if policy.DNSRebindProtection && !isAllowedIP(ip) {
			return fmt.Errorf("%w: connection to disallowed ip %s", ErrPolicyDenied, ip)
		}
		if len(policy.Deny) > 0 && matchEgressCIDRRules(ip, policy.Deny) {
			return fmt.Errorf("%w: ip %s denied by egress policy", ErrPolicyDenied, ip)
		}
		return nil
	}
}

// NewEgressTransport returns a transport that enforces the address-level parts
// of policy at connect time. It clones http.DefaultTransport rather than
// mutating it, since that value is process-global.
func NewEgressTransport(policy EgressPolicy) *http.Transport {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		base = &http.Transport{}
	}
	tr := base.Clone()
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   egressDialControl(policy),
	}
	tr.DialContext = dialer.DialContext
	return tr
}

// matchEgressDenyRules reports whether host, or any address it resolves to, is
// covered by a denylist rule. A single match denies, which is the fail-closed
// reading for a denylist: the dialer may pick any of the resolved addresses, so
// one denied address is reason enough to refuse the target.
func matchEgressDenyRules(host string, ips []net.IP, rules []EgressRule) bool {
	for _, r := range rules {
		if r.IsCIDR {
			for _, ip := range ips {
				addr, ok := netipFromIP(ip)
				if !ok {
					continue
				}
				if r.CIDR.Contains(addr) {
					return true
				}
			}
			continue
		}
		if matchHostRule(host, r) {
			return true
		}
	}
	return false
}

// matchEgressAllowRules reports whether host is permitted by the allowlist.
//
// Hostname rules keep any-match semantics: a request carries exactly one host,
// and a rule naming it permits the target however it resolves.
//
// CIDR rules require *every* resolved address to be covered by some CIDR rule.
// Any-match is fail-open on an allowlist, unlike on a denylist: the dialer picks
// freely among the resolved addresses, so a host answering with one in-range and
// one out-of-range address would clear the allowlist and then be connected on
// the address the rule existed to exclude. Under `allow "10.0.0.0/8"` — the
// workaround documented for private-network delivery targets — a host resolving
// to 10.1.1.5 plus 169.254.169.254 used to be permitted. This mirrors the
// rebind check above, which has always required all addresses to pass.
func matchEgressAllowRules(host string, ips []net.IP, rules []EgressRule) bool {
	hasCIDR := false
	for _, r := range rules {
		if r.IsCIDR {
			hasCIDR = true
			continue
		}
		if matchHostRule(host, r) {
			return true
		}
	}
	if !hasCIDR {
		return false
	}
	if len(ips) == 0 {
		// A CIDR rule cannot be decided without addresses. resolveHostIPs
		// resolves whenever CIDR rules are present, so this is unreachable in
		// practice; refusing is the safe reading if it ever is not.
		return false
	}
	for _, ip := range ips {
		// matchEgressCIDRRules also reports false for an address that will not
		// convert, which the all-must-match requirement turns into a denial.
		if !matchEgressCIDRRules(ip, rules) {
			return false
		}
	}
	return true
}

func netipFromIP(ip net.IP) (netip.Addr, bool) {
	if ip == nil {
		return netip.Addr{}, false
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return netip.Addr{}, false
	}
	var b [16]byte
	copy(b[:], ip16)
	addr := netip.AddrFrom16(b)
	if ip.To4() != nil {
		addr = addr.Unmap()
	}
	return addr, true
}

func matchHostRule(host string, rule EgressRule) bool {
	if rule.Host == "" || host == "" {
		return false
	}
	if rule.Host == "*" {
		return true
	}
	if !rule.Subdomains {
		return host == rule.Host
	}
	if host == rule.Host {
		return false
	}
	return strings.HasSuffix(host, "."+rule.Host)
}

// nonRoutablePrefixes enumerates ranges a delivery target has no legitimate
// reason to live in.
//
// This replaces a net.IP.IsPrivate check, which covers only RFC1918 and
// fc00::/7 and therefore left several non-routable ranges reachable — including
// three that carry cloud instance metadata directly (100.100.100.200 in the
// CGNAT range, 192.0.0.192 in the IETF protocol-assignment range) or reach the
// well-known 169.254.169.254 by translation (64:ff9b::a9fe:a9fe behind NAT64,
// ::a9fe:a9fe as the deprecated IPv4-compatible form). Listing the ranges
// explicitly keeps the whole policy readable in one place instead of splitting
// it between a stdlib helper and a set of exceptions.
//
// The RFC5737 documentation ranges (192.0.2.0/24, 198.51.100.0/24,
// 203.0.113.0/24) are deliberately absent: they are never routed, but they are
// also not a metadata surface, and they serve throughout this repo's tests as
// stand-ins for public addresses.
var nonRoutablePrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),      // RFC1122 "this network"
	netip.MustParsePrefix("10.0.0.0/8"),     // RFC1918 private
	netip.MustParsePrefix("100.64.0.0/10"),  // RFC6598 CGNAT (Alibaba metadata)
	netip.MustParsePrefix("127.0.0.0/8"),    // loopback
	netip.MustParsePrefix("169.254.0.0/16"), // RFC3927 link-local (AWS/GCP/Azure metadata)
	netip.MustParsePrefix("172.16.0.0/12"),  // RFC1918 private
	netip.MustParsePrefix("192.0.0.0/24"),   // RFC6890 IETF protocol assignments (Oracle metadata)
	netip.MustParsePrefix("192.168.0.0/16"), // RFC1918 private
	netip.MustParsePrefix("198.18.0.0/15"),  // RFC2544 benchmarking
	netip.MustParsePrefix("240.0.0.0/4"),    // reserved, includes 255.255.255.255
	netip.MustParsePrefix("::/96"),          // IPv4-compatible IPv6 (::127.0.0.1)
	netip.MustParsePrefix("64:ff9b::/96"),   // RFC6052 NAT64 well-known prefix
	netip.MustParsePrefix("fc00::/7"),       // RFC4193 unique local
}

func isAllowedIP(ip net.IP) bool {
	// netipFromIP unmaps IPv4-in-IPv6, so ::ffff:169.254.169.254 is checked
	// against the IPv4 prefixes rather than slipping past them as an IPv6
	// address.
	addr, ok := netipFromIP(ip)
	if !ok {
		return false
	}
	if addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}
	for _, p := range nonRoutablePrefixes {
		if p.Contains(addr) {
			return false
		}
	}
	return addr.IsGlobalUnicast()
}
