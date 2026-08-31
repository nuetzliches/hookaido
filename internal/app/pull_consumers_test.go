package app

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/admin"
	"github.com/nuetzliches/hookaido/v2/internal/pullapi"
)

const pullIdentityConfig = `
pull_api { auth token "raw:globaltoken" }

"/webhooks/global" {
  pull { path "/global" }
}

"/webhooks/scoped" {
  pull { path "/scoped" auth token "raw:scopedtoken" }
}
`

func newPullIdentityState(t *testing.T) *runtimeState {
	t.Helper()
	compiled := compileForReloadTest(t, pullIdentityConfig)
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}
	return state
}

func pullStreamRequest(path, token string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "http://workers.example"+path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

func TestIdentifyPullToken_NamesTheGlobalTokenReference(t *testing.T) {
	state := newPullIdentityState(t)

	got := state.identifyPullToken(pullStreamRequest("/global/stream", "globaltoken"))
	if got != "raw:globaltoken" {
		t.Fatalf("expected the configured pull_api token reference, got %q", got)
	}
}

// A route with its own `pull { auth token }` is authorized against that set
// alone, so identification has to follow the same scoping — otherwise an
// unexpected consumer would be reported under a credential it could not have
// used to reach that route.
func TestIdentifyPullToken_UsesRouteScopedTokensForScopedRoutes(t *testing.T) {
	state := newPullIdentityState(t)

	if got := state.identifyPullToken(pullStreamRequest("/scoped/stream", "scopedtoken")); got != "raw:scopedtoken" {
		t.Fatalf("expected the route-scoped token reference, got %q", got)
	}

	// The global token authorizes nothing on this route, so it must not be
	// reported as the credential in use either.
	if got := state.identifyPullToken(pullStreamRequest("/scoped/stream", "globaltoken")); got != "" {
		t.Fatalf("expected no identity for a token the route does not accept, got %q", got)
	}
	if state.authorizePull(pullStreamRequest("/scoped/stream", "globaltoken")) {
		t.Fatal("expected the global token to be rejected on a route-scoped endpoint")
	}
}

func TestIdentifyPullToken_UnknownTokenAndEndpoint(t *testing.T) {
	state := newPullIdentityState(t)

	if got := state.identifyPullToken(pullStreamRequest("/global/stream", "nope")); got != "" {
		t.Fatalf("expected no identity for an unknown token, got %q", got)
	}
	if got := state.identifyPullToken(pullStreamRequest("/global/stream", "")); got != "" {
		t.Fatalf("expected no identity without an Authorization header, got %q", got)
	}
	// An endpoint that resolves to no route falls back to the global set,
	// which is what authorizePull does too.
	if got := state.identifyPullToken(pullStreamRequest("/unknown/stream", "globaltoken")); got != "raw:globaltoken" {
		t.Fatalf("expected the global reference for an unresolved endpoint, got %q", got)
	}
	if got := state.identifyPullToken(nil); got != "" {
		t.Fatalf("expected no identity for a nil request, got %q", got)
	}
}

func TestAdminPullConsumers_TranslatesEveryField(t *testing.T) {
	connectedAt := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	lastMessage := connectedAt.Add(90 * time.Second)

	got := adminPullConsumers([]pullapi.ConsumerConnection{{
		ID:            "con_a",
		Route:         "/webhooks/appliance",
		Endpoint:      "/appliance",
		RemoteAddr:    "10.0.0.5:41234",
		UserAgent:     "hookaido-worker/1.0",
		TokenRef:      "env.PULL_TOKEN",
		ConnectedAt:   connectedAt,
		MessagesSent:  81,
		LastMessageAt: lastMessage,
	}})

	want := admin.PullConsumer{
		ID:            "con_a",
		Route:         "/webhooks/appliance",
		Endpoint:      "/appliance",
		RemoteAddr:    "10.0.0.5:41234",
		UserAgent:     "hookaido-worker/1.0",
		TokenRef:      "env.PULL_TOKEN",
		ConnectedAt:   connectedAt,
		MessagesSent:  81,
		LastMessageAt: lastMessage,
	}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("expected %#v, got %#v", want, got)
	}

	if got := adminPullConsumers(nil); got == nil || len(got) != 0 {
		t.Fatalf("expected an empty non-nil slice, got %#v", got)
	}
}
