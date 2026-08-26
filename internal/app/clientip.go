package app

import (
	"net/http"
	"net/netip"
	"strings"
)

// headerXForwardedFor is the only forwarded-address header consulted, and only
// when the transport peer is inside ingress.trusted_proxies.
const headerXForwardedFor = "X-Forwarded-For"

// resolveClientIP determines the address `match remote_ip` is compared against.
//
// The default is the transport peer, because trusting X-Forwarded-For
// unconditionally would let any client name its own source address. The problem
// with peer-only is that it fails silently in the shape Hookaido is most often
// deployed in: behind a TLS-terminating reverse proxy every request arrives with
// the proxy's address, so an allowlist of the source's published egress range
// matches nothing and the route answers 404 for legitimate traffic — or, once
// someone widens the range to the proxy's subnet to make it work, matches
// everything the proxy forwards from any origin. The second outcome is the
// dangerous one: the config still reads like an origin restriction.
//
// ingress.trusted_proxies closes that without weakening the default. When the
// peer is inside one of the configured prefixes, the right-most X-Forwarded-For
// entry that is not itself trusted becomes the client address. Walking from the
// right is what makes it sound: entries to the left were appended by hops the
// operator has not vouched for, and the client can put anything it likes there.
// A request from an untrusted peer keeps its peer address and the header is
// ignored entirely.
func resolveClientIP(r *http.Request, trustedProxies []netip.Prefix) (netip.Addr, bool) {
	if r == nil {
		return netip.Addr{}, false
	}
	peer, peerOK := parseRemoteAddrIP(r.RemoteAddr)
	if len(trustedProxies) == 0 || !peerOK {
		return peer, peerOK
	}
	if !ipInPrefixes(peer, trustedProxies) {
		return peer, peerOK
	}

	forwarded := r.Header.Values(headerXForwardedFor)
	if len(forwarded) == 0 {
		return peer, peerOK
	}

	// One header may carry several comma-separated hops, and there may be
	// several headers; net/http keeps them in arrival order, so flattening
	// left-to-right reproduces the chain.
	var chain []string
	for _, value := range forwarded {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				chain = append(chain, part)
			}
		}
	}

	for i := len(chain) - 1; i >= 0; i-- {
		addr, ok := parseForwardedAddr(chain[i])
		if !ok {
			// An unparsable entry is where the trustworthy part of the chain
			// ends: everything further left was appended before it, by a hop we
			// cannot identify. Stop rather than skip past it.
			return peer, peerOK
		}
		if ipInPrefixes(addr, trustedProxies) {
			continue
		}
		return addr, true
	}

	// Every hop in the chain is a trusted proxy. There is no client address to
	// recover, so the peer stands.
	return peer, peerOK
}

// parseForwardedAddr accepts the shapes that appear in X-Forwarded-For in
// practice: a bare address, a bracketed IPv6 address, and either with a port.
func parseForwardedAddr(raw string) (netip.Addr, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return netip.Addr{}, false
	}
	if addr, err := netip.ParseAddr(raw); err == nil {
		return addr.Unmap(), true
	}
	if addrPort, err := netip.ParseAddrPort(raw); err == nil {
		return addrPort.Addr().Unmap(), true
	}
	// A bracketed IPv6 address without a port is not valid for either parser.
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		if addr, err := netip.ParseAddr(strings.Trim(raw, "[]")); err == nil {
			return addr.Unmap(), true
		}
	}
	return netip.Addr{}, false
}

func ipInPrefixes(ip netip.Addr, prefixes []netip.Prefix) bool {
	if !ip.IsValid() {
		return false
	}
	for _, pfx := range prefixes {
		if pfx.Contains(ip) {
			return true
		}
	}
	return false
}
