package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
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

	if len(policy.Deny) > 0 && matchEgressRules(host, ips, policy.Deny) {
		return fmt.Errorf("%w: host %q denied by egress policy", ErrPolicyDenied, host)
	}

	if len(policy.Allow) > 0 && !matchEgressRules(host, ips, policy.Allow) {
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

func matchEgressRules(host string, ips []net.IP, rules []EgressRule) bool {
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

func isAllowedIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	if ip.IsPrivate() {
		return false
	}
	if !ip.IsGlobalUnicast() {
		return false
	}
	return true
}
