package config

import (
	"strings"
	"testing"
)

func compileConsumerGroupConfig(t *testing.T, src string) (Compiled, ValidationResult) {
	t.Helper()
	cfg, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := Compile(cfg)
	return compiled, res
}

func mustCompileConsumerGroupConfig(t *testing.T, src string) Compiled {
	t.Helper()
	compiled, res := compileConsumerGroupConfig(t, src)
	if !res.OK {
		t.Fatalf("expected the config to compile, got errors: %v", res.Errors)
	}
	return compiled
}

func requireCompileError(t *testing.T, src string, want string) {
	t.Helper()
	_, res := compileConsumerGroupConfig(t, src)
	if res.OK {
		t.Fatalf("expected a compile error containing %q", want)
	}
	for _, e := range res.Errors {
		if strings.Contains(e, want) {
			return
		}
	}
	t.Fatalf("expected an error containing %q, got %v", want, res.Errors)
}

const twoGroupConfig = `
pull_api { auth token "raw:devtoken" }

"/webhooks/appliance" {
  pull {
    path "/appliance"
    consumer_group "integration"
    consumer_group "workstation"
  }
}
`

// Each group gets its own endpoint and its own queue target, so the ingress
// fans one inbound event out to every group rather than letting them compete
// for it.
func TestConsumerGroups_EachGroupIsAnIndependentQueue(t *testing.T) {
	compiled := mustCompileConsumerGroupConfig(t, twoGroupConfig)

	integration, ok := compiled.PullEndpoints["/appliance/integration"]
	if !ok {
		t.Fatalf("expected an endpoint per group, got %#v", compiled.PullEndpoints)
	}
	if integration.Route != "/webhooks/appliance" {
		t.Fatalf("expected route /webhooks/appliance, got %q", integration.Route)
	}
	if integration.Target != "pull:integration" {
		t.Fatalf("expected target pull:integration, got %q", integration.Target)
	}
	if integration.ConsumerGroup != "integration" {
		t.Fatalf("expected consumer group integration, got %q", integration.ConsumerGroup)
	}

	workstation := compiled.PullEndpoints["/appliance/workstation"]
	if workstation.Target != "pull:workstation" {
		t.Fatalf("expected target pull:workstation, got %q", workstation.Target)
	}

	if integration.Target == workstation.Target {
		t.Fatal("groups must not share a queue target")
	}

	targets := compiled.Routes[0].Pull.PullTargets()
	if len(targets) != 2 || targets[0] != "pull:integration" || targets[1] != "pull:workstation" {
		t.Fatalf("expected one enqueue target per group in declaration order, got %v", targets)
	}
}

// The bare path stops resolving once groups exist. Silently keeping it would
// leave an unmigrated consumer competing for a share of a queue it was meant to
// receive in full — the exact accident consumer groups exist to prevent — so it
// has to fail visibly instead.
func TestConsumerGroups_BarePathNoLongerResolves(t *testing.T) {
	compiled := mustCompileConsumerGroupConfig(t, twoGroupConfig)

	if ep, ok := compiled.PullEndpoints["/appliance"]; ok {
		t.Fatalf("expected the ungrouped path to stop resolving, got %#v", ep)
	}
	if len(compiled.PullEndpoints) != 2 {
		t.Fatalf("expected exactly the two group endpoints, got %#v", compiled.PullEndpoints)
	}
}

// A route without groups is unchanged, including the queue target. That matters
// beyond compatibility: messages already queued in a durable backend carry
// `pull`, and changing it would strand them.
func TestConsumerGroups_UngroupedRouteIsUnchanged(t *testing.T) {
	compiled := mustCompileConsumerGroupConfig(t, `
pull_api { auth token "raw:devtoken" }

"/webhooks/github" {
  pull { path "/github" }
}
`)

	ep, ok := compiled.PullEndpoints["/github"]
	if !ok {
		t.Fatalf("expected the bare path to resolve, got %#v", compiled.PullEndpoints)
	}
	if ep.Target != "pull" {
		t.Fatalf("expected the unchanged target %q, got %q", "pull", ep.Target)
	}
	if ep.ConsumerGroup != "" {
		t.Fatalf("expected no consumer group, got %q", ep.ConsumerGroup)
	}
	if got := compiled.Routes[0].Pull.PullTargets(); len(got) != 1 || got[0] != "pull" {
		t.Fatalf("expected a single pull target, got %v", got)
	}
}

func TestConsumerGroups_RejectsInvalidNames(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "duplicate",
			src:  `"/x" { pull { path "/e" consumer_group "a" consumer_group "a" } }`,
			want: `pull.consumer_group duplicate "a"`,
		},
		{
			name: "empty",
			src:  `"/x" { pull { path "/e" consumer_group "" } }`,
			want: "pull.consumer_group must not be empty",
		},
		{
			name: "path separator",
			src:  `"/x" { pull { path "/e" consumer_group "a/b" } }`,
			want: `pull.consumer_group "a/b" must match`,
		},
		{
			name: "leading punctuation",
			src:  `"/x" { pull { path "/e" consumer_group "-a" } }`,
			want: `pull.consumer_group "-a" must match`,
		},
		{
			// The operation is the last path segment, so a group named after
			// one produces URLs nobody wants to read during an outage.
			name: "reserved operation name",
			src:  `"/x" { pull { path "/e" consumer_group "dequeue" } }`,
			want: `pull.consumer_group "dequeue" is reserved`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireCompileError(t, "pull_api { auth token \"raw:t\" }\n"+tc.src, tc.want)
		})
	}
}

// A group endpoint collides with another route's pull path the same way two
// bare paths do.
func TestConsumerGroups_EndpointCollisionIsRejected(t *testing.T) {
	requireCompileError(t, `
pull_api { auth token "raw:t" }

"/webhooks/a" {
  pull { path "/appliance" consumer_group "integration" }
}

"/webhooks/b" {
  pull { path "/appliance/integration" }
}
`, `duplicate pull path "/appliance/integration"`)
}

// A rejected route must leave nothing behind in the endpoint map, or a later
// route would appear to collide with a route that was never compiled.
func TestConsumerGroups_RejectedRouteRegistersNoEndpoints(t *testing.T) {
	compiled, res := compileConsumerGroupConfig(t, `
pull_api { auth token "raw:t" }

"/webhooks/a" {
  pull { path "/shared" consumer_group "one" }
}

"/webhooks/b" {
  pull { path "/shared" consumer_group "two" consumer_group "one" }
}
`)
	if res.OK {
		t.Fatal("expected the colliding route to be rejected")
	}
	if ep, ok := compiled.PullEndpoints["/shared/two"]; ok {
		t.Fatalf("expected no endpoint from the rejected route, got %#v", ep)
	}
	if compiled.PullEndpoints["/shared/one"].Route != "/webhooks/a" {
		t.Fatalf("expected the first route to keep its endpoint, got %#v", compiled.PullEndpoints)
	}
}

// Round-trip: `config fmt` must be stable, or every edit produces a spurious
// diff.
func TestConsumerGroups_FormatRoundTrip(t *testing.T) {
	cfg, err := Parse([]byte(twoGroupConfig))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	first, err := Format(cfg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	if !strings.Contains(string(first), `consumer_group "integration"`) {
		t.Fatalf("expected consumer_group to be emitted, got:\n%s", first)
	}

	reparsed, err := Parse(first)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	second, err := Format(reparsed)
	if err != nil {
		t.Fatalf("reformat: %v", err)
	}
	if string(second) != string(first) {
		t.Fatalf("format is not stable:\nfirst:\n%s\nsecond:\n%s", first, second)
	}
}

func TestConsumerGroups_UnknownDirectiveStillRejected(t *testing.T) {
	_, err := Parse([]byte(`"/x" { pull { path "/e" consumer_groups "a" } }`))
	if err == nil {
		t.Fatal("expected a parse error for an unknown pull directive")
	}
	if !strings.Contains(err.Error(), "unknown pull directive") {
		t.Fatalf("expected an unknown-directive error, got %v", err)
	}
}
