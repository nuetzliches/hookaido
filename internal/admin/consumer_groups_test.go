package admin

import "testing"

// A grouped pull route has one target per consumer group, so publish can no
// longer auto-select one — publishing to "the route" would have to mean either
// one arbitrary group or all of them, and neither is a safe guess.
func TestResolvePublishTarget_GroupedPullRouteRequiresAnExplicitTarget(t *testing.T) {
	grouped := []string{"pull:integration", "pull:workstation"}

	if got, ok := resolvePublishTarget("", grouped); ok {
		t.Fatalf("expected no auto-selected target for a grouped route, got %q", got)
	}
	got, ok := resolvePublishTarget("pull:workstation", grouped)
	if !ok || got != "pull:workstation" {
		t.Fatalf("expected an explicit group target to resolve, got %q ok=%v", got, ok)
	}
	if got, ok := resolvePublishTarget("pull", grouped); ok {
		t.Fatalf("expected the ungrouped target to be rejected on a grouped route, got %q", got)
	}

	// An ungrouped route is unchanged: one target, still auto-selected.
	if got, ok := resolvePublishTarget("", []string{"pull"}); !ok || got != "pull" {
		t.Fatalf("expected the single pull target to auto-select, got %q ok=%v", got, ok)
	}
}

// publishRouteMode used to recognise a pull route as "exactly one target named
// pull". A grouped route has several, all of them pull targets.
func TestPublishRouteMode_RecognisesGroupedPullTargets(t *testing.T) {
	var srv *Server

	if got := srv.publishRouteMode("/webhooks/appliance", []string{"pull:integration", "pull:workstation"}); got != "pull" {
		t.Fatalf("expected a grouped pull route to be recognised as pull, got %q", got)
	}
	if got := srv.publishRouteMode("/webhooks/github", []string{"pull"}); got != "pull" {
		t.Fatalf("expected an ungrouped pull route to stay pull, got %q", got)
	}
	if got := srv.publishRouteMode("/webhooks/ci", []string{"https://ci.internal/build"}); got != "deliver" {
		t.Fatalf("expected a deliver route to stay deliver, got %q", got)
	}
	// A mixed set is not a pull route; the compiler rejects that shape anyway,
	// so the fallback must not silently call it one.
	if got := srv.publishRouteMode("/webhooks/mixed", []string{"pull:a", "https://ci.internal/build"}); got != "deliver" {
		t.Fatalf("expected a mixed target set not to be treated as pull, got %q", got)
	}
}
