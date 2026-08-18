package dispatcher

import "maps"

// The comparisons below live next to the structs they compare, rather than in
// the reload path that uses them, so that adding a field to a dispatcher config
// struct and extending its comparison are the same edit. They previously sat in
// internal/app, where TargetConfig grew CustomHeaders, IsExec and ExecEnv
// without the comparison noticing: a reload that changed only a delivery header
// (commonly an auth token) or an exec target's environment reported success and
// left the running dispatcher sending the old values until restart.
//
// TestTargetConfigEqual_CoversEveryField guards the same property from the
// other side.

// RoutesEqual reports whether two dispatch route sets describe the same
// delivery behaviour, and therefore whether a reload needs to swap the
// dispatcher.
func RoutesEqual(a, b []RouteConfig) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

// Equal reports whether two route configurations are identical.
func (r RouteConfig) Equal(other RouteConfig) bool {
	if r.Route != other.Route || r.Concurrency != other.Concurrency {
		return false
	}
	if len(r.Targets) != len(other.Targets) {
		return false
	}
	for i := range r.Targets {
		if !r.Targets[i].Equal(other.Targets[i]) {
			return false
		}
	}
	return true
}

// Equal reports whether two target configurations are identical. Every field of
// TargetConfig is compared; see the note at the top of this file.
func (t TargetConfig) Equal(other TargetConfig) bool {
	if t.URL != other.URL ||
		t.Timeout != other.Timeout ||
		t.IsExec != other.IsExec {
		return false
	}
	if !t.Retry.Equal(other.Retry) {
		return false
	}
	if !t.SignHMAC.Equal(other.SignHMAC) {
		return false
	}
	if !customHeadersEqual(t.CustomHeaders, other.CustomHeaders) {
		return false
	}
	return maps.Equal(t.ExecEnv, other.ExecEnv)
}

// Equal reports whether two retry policies are identical.
func (r RetryConfig) Equal(other RetryConfig) bool {
	return r.Type == other.Type &&
		r.Max == other.Max &&
		r.Base == other.Base &&
		r.Cap == other.Cap &&
		r.Jitter == other.Jitter
}

// Equal reports whether two outbound signing configurations are identical. A
// nil receiver means "no signing", and equals only another nil.
func (h *HMACSigningConfig) Equal(other *HMACSigningConfig) bool {
	if h == nil || other == nil {
		return h == other
	}
	if h.SecretRef != other.SecretRef ||
		h.SecretSelection != other.SecretSelection ||
		h.SignatureHeader != other.SignatureHeader ||
		h.TimestampHeader != other.TimestampHeader {
		return false
	}
	if len(h.SecretVersions) != len(other.SecretVersions) {
		return false
	}
	for i := range h.SecretVersions {
		if !h.SecretVersions[i].Equal(other.SecretVersions[i]) {
			return false
		}
	}
	return true
}

// Equal reports whether two signing secret versions are identical.
func (v HMACSigningSecretVersion) Equal(other HMACSigningSecretVersion) bool {
	return v.ID == other.ID &&
		v.Ref == other.Ref &&
		v.ValidFrom.Equal(other.ValidFrom) &&
		v.ValidUntil.Equal(other.ValidUntil) &&
		v.HasUntil == other.HasUntil
}

func customHeadersEqual(a, b []CustomHeader) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}
