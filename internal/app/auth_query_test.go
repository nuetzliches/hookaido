package app

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nuetzliches/hookaido/v2/internal/config"
)

const authQueryRouteConfig = `
pull_api { auth token "raw:devtoken" }

"/webhooks/source" {
  auth query "t" "raw:url-token"
  pull { path "/source" }
}
`

func TestLoadAuth_WiresQueryAuthOntoTheSnapshot(t *testing.T) {
	compiled := compileForReloadTest(t, authQueryRouteConfig)
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "http://hooks.example/webhooks/source?t=url-token", nil)
	snap, ok := state.resolveIngressSnapshot(req, "/webhooks/source")
	if !ok {
		t.Fatal("expected the route to resolve")
	}
	if snap.QueryAuth == nil {
		t.Fatal("expected the snapshot to carry a query authenticator")
	}
	if !snap.QueryAuth.Verify(req) {
		t.Fatal("expected the configured token to verify")
	}

	wrong := httptest.NewRequest(http.MethodPost, "http://hooks.example/webhooks/source?t=nope", nil)
	if snap.QueryAuth.Verify(wrong) {
		t.Fatal("expected a wrong token to be rejected")
	}
}

// A route without `auth query` must map to a nil authenticator, which ingress
// reads as "no query auth configured".
func TestLoadAuth_RouteWithoutQueryAuthHasNoAuthenticator(t *testing.T) {
	compiled := compileForReloadTest(t, `
pull_api { auth token "raw:devtoken" }

"/webhooks/plain" {
  pull { path "/plain" }
}
`)
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "http://hooks.example/webhooks/plain", nil)
	snap, ok := state.resolveIngressSnapshot(req, "/webhooks/plain")
	if !ok {
		t.Fatal("expected the route to resolve")
	}
	if snap.QueryAuth != nil {
		t.Fatal("expected no query authenticator on a route that configures none")
	}
}

// The pool is read at verification time rather than baked in at load, which is
// what makes a rotation possible at all -- a `match query` token could only ever
// be changed by editing the config, so every rotation had a window in which the
// source was rejected.
func TestLoadAuth_QueryAuthSecretRefResolvesFromThePool(t *testing.T) {
	compiled := compileForReloadTest(t, `
secrets {
  secret "source-token" {
    value "raw:v1"
    valid_from "2020-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:devtoken" }

"/webhooks/source" {
  auth query "t" secret_ref "source-token"
  pull { path "/source" }
}
`)
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "http://hooks.example/webhooks/source?t=v1", nil)
	snap, ok := state.resolveIngressSnapshot(req, "/webhooks/source")
	if !ok {
		t.Fatal("expected the route to resolve")
	}
	if snap.QueryAuth == nil {
		t.Fatal("expected a query authenticator")
	}
	if !snap.QueryAuth.Verify(req) {
		t.Fatal("expected the pooled token version to verify")
	}
	wrong := httptest.NewRequest(http.MethodPost, "http://hooks.example/webhooks/source?t=v2", nil)
	if snap.QueryAuth.Verify(wrong) {
		t.Fatal("expected a token that is in no pool to be rejected")
	}
}

// Two pools with overlapping validity windows are the config-level way to give a
// rotation an overlap: both tokens are accepted at once.
func TestLoadAuth_QueryAuthAcceptsSeveralSecretRefsAtOnce(t *testing.T) {
	compiled := compileForReloadTest(t, `
secrets {
  secret "token-v1" {
    value "raw:v1"
    valid_from "2020-01-01T00:00:00Z"
  }
  secret "token-v2" {
    value "raw:v2"
    valid_from "2020-01-01T00:00:00Z"
  }
}

pull_api { auth token "raw:devtoken" }

"/webhooks/source" {
  auth query "t" secret_ref "token-v1"
  auth query "t" secret_ref "token-v2"
  pull { path "/source" }
}
`)
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "http://hooks.example/webhooks/source", nil)
	snap, ok := state.resolveIngressSnapshot(req, "/webhooks/source")
	if !ok {
		t.Fatal("expected the route to resolve")
	}
	for _, tok := range []string{"v1", "v2"} {
		r := httptest.NewRequest(http.MethodPost, "http://hooks.example/webhooks/source?t="+tok, nil)
		if !snap.QueryAuth.Verify(r) {
			t.Fatalf("expected %q to verify during the rotation overlap", tok)
		}
	}
	r := httptest.NewRequest(http.MethodPost, "http://hooks.example/webhooks/source?t=v3", nil)
	if snap.QueryAuth.Verify(r) {
		t.Fatal("expected an unconfigured token to be rejected")
	}
}

// A secret_ref that resolves to nothing must not degrade into "no auth". This is
// the state compilation forbids, checked at the runtime layer as well.
func TestLoadAuth_QueryAuthUnknownSecretRefFailsTheReload(t *testing.T) {
	compiled := compileForReloadTest(t, authQueryRouteConfig)
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	broken := compiled
	broken.Routes = append([]config.CompiledRoute(nil), compiled.Routes...)
	for i := range broken.Routes {
		broken.Routes[i].AuthQuery.Secrets = nil
		broken.Routes[i].AuthQuery.SecretRefs = []string{"absent"}
	}
	if err := state.loadAuth(broken); err == nil {
		t.Fatal("expected an unresolvable secret_ref to fail the reload")
	}
}

// A reload that removes `auth query` must clear the authenticator rather than
// leave the previous one in place.
func TestLoadAuth_ReloadClearsQueryAuth(t *testing.T) {
	compiled := compileForReloadTest(t, authQueryRouteConfig)
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	updated := compileForReloadTest(t, `
pull_api { auth token "raw:devtoken" }

"/webhooks/source" {
  pull { path "/source" }
}
`)
	if err := state.loadAuth(updated); err != nil {
		t.Fatalf("reload loadAuth: %v", err)
	}
	state.updateAll(updated)

	req := httptest.NewRequest(http.MethodPost, "http://hooks.example/webhooks/source", nil)
	snap, ok := state.resolveIngressSnapshot(req, "/webhooks/source")
	if !ok {
		t.Fatal("expected the route to resolve")
	}
	if snap.QueryAuth != nil {
		t.Fatal("expected the removed query auth to be cleared on reload")
	}
}

// `auth query` is live-reloadable, so a token change must not require a restart.
func TestRequiresRestartForReload_QueryAuthChangeIsLive(t *testing.T) {
	before := compileForReloadTest(t, authQueryRouteConfig)
	after := compileForReloadTest(t, `
pull_api { auth token "raw:devtoken" }

"/webhooks/source" {
  auth query "t" "raw:rotated"
  pull { path "/source" }
}
`)
	if requiresRestartForReload(after, before) {
		t.Fatal("an auth query token change must not require a restart")
	}
}
