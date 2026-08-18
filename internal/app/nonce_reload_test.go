package app

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/config"
)

func compileForNonceTest(t *testing.T, src string) config.Compiled {
	t.Helper()
	cfg, err := config.Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	compiled, res := config.Compile(cfg)
	if !res.OK {
		t.Fatalf("compile: %#v", res)
	}
	return compiled
}

func signCanonicalHMAC(ts int64, method, path, body string, secret []byte) string {
	sum := sha256.Sum256([]byte(body))
	msg := strconv.FormatInt(ts, 10) + "\n" + method + "\n" + path + "\n" + hex.EncodeToString(sum[:])
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(msg))
	return hex.EncodeToString(mac.Sum(nil))
}

// Every reload rebuilds each route's HMACAuth, and a fresh one starts with an
// empty nonce cache -- so a reload landing inside the tolerance window used to
// forget every nonce seen so far and let a captured signed request be replayed.
// Admin API managed-endpoint mutations reload too, which made the window
// reachable on demand.
func TestLoadAuth_ReloadKeepsNonceCache(t *testing.T) {
	const before = `
pull_api { auth token "raw:global" }

"/x" {
  auth hmac { secret "raw:s1" }
  pull { path "/e" }
}
`
	// A reload triggered by a change on an unrelated route: /x is untouched.
	const after = `
pull_api { auth token "raw:global" }

"/x" {
  auth hmac { secret "raw:s1" }
  pull { path "/e" }
}

"/y" {
  pull { path "/e2" }
}
`

	compiled := compileForNonceTest(t, before)
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	body := `{"x":1}`
	ts := time.Now().UTC().Unix()
	sig := signCanonicalHMAC(ts, http.MethodPost, "/x", body, []byte("s1"))
	newReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodPost, "http://example.com/x", strings.NewReader(body))
		req.Header.Set("X-Timestamp", strconv.FormatInt(ts, 10))
		req.Header.Set("X-Nonce", "n1")
		req.Header.Set("X-Signature", sig)
		return req
	}

	snap, ok := state.resolveIngressSnapshot(newReq(), "/x")
	if !ok || snap.HMACAuth == nil {
		t.Fatal("expected route /x to resolve with HMAC auth")
	}
	claim, err := snap.HMACAuth.Verify(newReq(), "/x", []byte(body))
	if err != nil {
		t.Fatalf("first request must verify: %v", err)
	}
	claim.Commit()

	updated := compileForNonceTest(t, after)
	if err := state.loadAuth(updated); err != nil {
		t.Fatalf("reload loadAuth: %v", err)
	}

	reloaded, ok := state.resolveIngressSnapshot(newReq(), "/x")
	if !ok || reloaded.HMACAuth == nil {
		t.Fatal("expected route /x to resolve after the reload")
	}
	if reloaded.HMACAuth == snap.HMACAuth {
		t.Fatal("the reload did not rebuild the authorizer, so this proves nothing")
	}
	if _, err := reloaded.HMACAuth.Verify(newReq(), "/x", []byte(body)); err == nil {
		t.Fatal("a nonce claimed before the reload was accepted again after it")
	}
}
