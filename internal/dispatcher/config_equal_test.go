package dispatcher

import (
	"reflect"
	"testing"
	"time"
)

// targetConfigFieldCount is asserted below. Bumping it is the reminder that a
// new TargetConfig field also needs a case in TargetConfig.Equal and a case in
// the table -- otherwise a reload that changes only that field is silently
// never applied, which is exactly how CustomHeaders, IsExec and ExecEnv came to
// be missing from the comparison.
const targetConfigFieldCount = 7

func TestTargetConfigEqual_CoversEveryField(t *testing.T) {
	if got := reflect.TypeOf(TargetConfig{}).NumField(); got != targetConfigFieldCount {
		t.Fatalf("TargetConfig has %d fields, the comparison covers %d: extend TargetConfig.Equal, add a case below, and update the constant",
			got, targetConfigFieldCount)
	}

	base := TargetConfig{
		URL:     "https://ci.internal/hook",
		Timeout: 10 * time.Second,
		Retry:   RetryConfig{Type: "exponential", Max: 8, Base: 2 * time.Second, Cap: 2 * time.Minute, Jitter: 0.2},
		SignHMAC: &HMACSigningConfig{
			SecretRef:       "S1",
			SecretSelection: "newest",
			SignatureHeader: "X-Signature",
			TimestampHeader: "X-Timestamp",
			SecretVersions: []HMACSigningSecretVersion{
				{ID: "v1", Ref: "raw:a", ValidFrom: time.Unix(0, 0).UTC()},
			},
		},
		CustomHeaders: []CustomHeader{{Name: "Authorization", Value: "Bearer OLD"}},
		IsExec:        false,
		ExecEnv:       map[string]string{"MODE": "prod"},
	}

	if !base.Equal(clone(base)) {
		t.Fatal("a target must equal its own copy")
	}

	tests := []struct {
		field  string
		mutate func(*TargetConfig)
	}{
		{field: "URL", mutate: func(c *TargetConfig) { c.URL = "https://ci.internal/other" }},
		{field: "Timeout", mutate: func(c *TargetConfig) { c.Timeout = 30 * time.Second }},
		{field: "Retry", mutate: func(c *TargetConfig) { c.Retry.Max = 3 }},
		{field: "SignHMAC", mutate: func(c *TargetConfig) { c.SignHMAC = nil }},
		{field: "SignHMAC.SecretVersions", mutate: func(c *TargetConfig) {
			c.SignHMAC = &HMACSigningConfig{
				SecretRef:       "S1",
				SecretSelection: "newest",
				SignatureHeader: "X-Signature",
				TimestampHeader: "X-Timestamp",
				SecretVersions: []HMACSigningSecretVersion{
					{ID: "v2", Ref: "raw:b", ValidFrom: time.Unix(0, 0).UTC()},
				},
			}
		}},
		// The token-rotation case: a header value change is a delivery change.
		{field: "CustomHeaders", mutate: func(c *TargetConfig) {
			c.CustomHeaders = []CustomHeader{{Name: "Authorization", Value: "Bearer NEW"}}
		}},
		{field: "CustomHeaders (added)", mutate: func(c *TargetConfig) {
			c.CustomHeaders = append(clone(base).CustomHeaders, CustomHeader{Name: "X-Env", Value: "prod"})
		}},
		{field: "IsExec", mutate: func(c *TargetConfig) { c.IsExec = true }},
		{field: "ExecEnv", mutate: func(c *TargetConfig) { c.ExecEnv = map[string]string{"MODE": "staging"} }},
		{field: "ExecEnv (added)", mutate: func(c *TargetConfig) {
			c.ExecEnv = map[string]string{"MODE": "prod", "EXTRA": "1"}
		}},
	}

	for _, tc := range tests {
		t.Run(tc.field, func(t *testing.T) {
			changed := clone(base)
			tc.mutate(&changed)
			if base.Equal(changed) {
				t.Fatalf("a change to %s was not detected, so a reload would never apply it", tc.field)
			}
			if changed.Equal(base) {
				t.Fatalf("comparison is not symmetric for %s", tc.field)
			}
		})
	}
}

func TestRoutesEqual(t *testing.T) {
	route := RouteConfig{
		Route:       "/x",
		Concurrency: 20,
		Targets:     []TargetConfig{{URL: "https://ci.internal/hook", Timeout: time.Second}},
	}

	if !RoutesEqual([]RouteConfig{route}, []RouteConfig{clone(route)}) {
		t.Fatal("identical route sets must compare equal")
	}
	if RoutesEqual([]RouteConfig{route}, nil) {
		t.Fatal("a different route count must not compare equal")
	}

	renamed := clone(route)
	renamed.Route = "/y"
	if RoutesEqual([]RouteConfig{route}, []RouteConfig{renamed}) {
		t.Fatal("a different route path must not compare equal")
	}

	rescaled := clone(route)
	rescaled.Concurrency = 1
	if RoutesEqual([]RouteConfig{route}, []RouteConfig{rescaled}) {
		t.Fatal("a different concurrency must not compare equal")
	}

	retargeted := clone(route)
	retargeted.Targets = []TargetConfig{{URL: "https://ci.internal/other", Timeout: time.Second}}
	if RoutesEqual([]RouteConfig{route}, []RouteConfig{retargeted}) {
		t.Fatal("a different target must not compare equal")
	}
}

func TestHMACSigningConfigEqual_Nil(t *testing.T) {
	var a, b *HMACSigningConfig
	if !a.Equal(b) {
		t.Fatal("two absent signing configs must compare equal")
	}
	if a.Equal(&HMACSigningConfig{}) {
		t.Fatal("absent and present signing configs must not compare equal")
	}
	if (&HMACSigningConfig{}).Equal(nil) {
		t.Fatal("present and absent signing configs must not compare equal")
	}
}

// clone returns a copy deep enough for the comparisons under test: the slices
// and maps are rebuilt so a mutation in one copy cannot reach the other.
func clone[T RouteConfig | TargetConfig](in T) T {
	out := in
	switch v := any(&out).(type) {
	case *TargetConfig:
		v.CustomHeaders = append([]CustomHeader(nil), v.CustomHeaders...)
		if v.ExecEnv != nil {
			env := make(map[string]string, len(v.ExecEnv))
			for k, val := range v.ExecEnv {
				env[k] = val
			}
			v.ExecEnv = env
		}
	case *RouteConfig:
		targets := make([]TargetConfig, 0, len(v.Targets))
		for _, t := range v.Targets {
			targets = append(targets, clone(t))
		}
		v.Targets = targets
	}
	return out
}
