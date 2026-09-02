package app

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/ingress"
	"github.com/nuetzliches/hookaido/v2/internal/secrets"
)

func secretPoolFixture(t *testing.T, now time.Time) []secrets.PoolState {
	t.Helper()

	reg := secrets.NewRegistry()
	rotating, err := secrets.NewPool("rotating", true, 0, []secrets.Version{
		{ID: "old", Value: []byte("a"), ValidFrom: now.Add(-3 * time.Hour), ValidUntil: now.Add(-time.Hour)},
		{ID: "current", Value: []byte("b"), ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(30 * time.Minute)},
		{ID: "next", Value: []byte("c"), ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(2 * time.Hour)},
		{ID: "future", Value: []byte("d"), ValidFrom: now.Add(time.Hour), ValidUntil: now.Add(5 * time.Hour)},
	})
	if err != nil {
		t.Fatalf("NewPool rotating: %v", err)
	}
	// The failure mode: two versions held, neither valid now.
	lapsed, err := secrets.NewPool("lapsed", true, 0, []secrets.Version{
		{ID: "gone", Value: []byte("e"), ValidFrom: now.Add(-3 * time.Hour), ValidUntil: now.Add(-time.Hour)},
		{ID: "tomorrow", Value: []byte("f"), ValidFrom: now.Add(24 * time.Hour)},
	})
	if err != nil {
		t.Fatalf("NewPool lapsed: %v", err)
	}
	unbounded, err := secrets.NewPool("static", false, 0, []secrets.Version{
		{ID: "static", Value: []byte("g"), ValidFrom: now.Add(-time.Hour)},
	})
	if err != nil {
		t.Fatalf("NewPool static: %v", err)
	}
	for _, p := range []*secrets.Pool{rotating, lapsed, unbounded} {
		if err := reg.Register(p); err != nil {
			t.Fatalf("Register %q: %v", p.Name(), err)
		}
	}
	return reg.StatesAt(now)
}

func TestMetricsHandler_RuntimeSecretPoolGauges(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	m := newRuntimeMetrics()
	m.now = func() time.Time { return now }
	m.setSecretPoolStateSource(func(at time.Time) []secrets.PoolState {
		if !at.Equal(now) {
			t.Errorf("census taken at %s, want %s", at, now)
		}
		return secretPoolFixture(t, now)
	})

	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_runtime_secret_pool_versions{pool="rotating",state="valid"} 2`,
		`hookaido_runtime_secret_pool_versions{pool="rotating",state="pending"} 1`,
		`hookaido_runtime_secret_pool_versions{pool="rotating",state="expired"} 1`,
		// The alert `...{state="valid"} == 0` that #295 could not write.
		`hookaido_runtime_secret_pool_versions{pool="lapsed",state="valid"} 0`,
		`hookaido_runtime_secret_pool_versions{pool="lapsed",state="pending"} 1`,
		`hookaido_runtime_secret_pool_versions{pool="lapsed",state="expired"} 1`,
		// 30 minutes to the next handover, two hours to the cliff.
		`hookaido_runtime_secret_pool_next_expiry_seconds{pool="rotating"} 1800`,
		`hookaido_runtime_secret_pool_exhaustion_seconds{pool="rotating"} 7200`,
		// Already dry: no countdown left to report.
		`hookaido_runtime_secret_pool_next_expiry_seconds{pool="lapsed"} 0`,
		`hookaido_runtime_secret_pool_exhaustion_seconds{pool="lapsed"} 0`,
		// An unbounded live version never lapses, which is +Inf and not 0.
		`hookaido_runtime_secret_pool_next_expiry_seconds{pool="static"} +Inf`,
		`hookaido_runtime_secret_pool_exhaustion_seconds{pool="static"} +Inf`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestMetricsHandler_RuntimeSecretPoolGaugesAbsentWithoutSource(t *testing.T) {
	// A process with no secret registry must not emit a gauge that reads as
	// "zero pools, all healthy" -- absence is the honest signal, and the
	// dashboard guidance says to treat a missing series as not emitted.
	m := newRuntimeMetrics()
	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	if strings.Contains(rr.Body.String(), "hookaido_runtime_secret_pool_versions") {
		t.Fatalf("expected no pool gauges without a wired census:\n%s", rr.Body.String())
	}
}

func TestRuntimeMetrics_HealthDiagnosticsRuntimeSecrets(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	m := newRuntimeMetrics()
	m.now = func() time.Time { return now }
	m.setSecretPoolStateSource(func(time.Time) []secrets.PoolState {
		return secretPoolFixture(t, now)
	})

	diag := m.healthDiagnostics()
	rollup, ok := diag["runtime_secrets"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtime_secrets diagnostics object, got %T", diag["runtime_secrets"])
	}
	if got := intFromAny(rollup["pools_total"]); got != 3 {
		t.Fatalf("pools_total = %v, want 3", rollup["pools_total"])
	}
	// The one number an HTTP-only uptime checker can assert on.
	if got := intFromAny(rollup["pools_without_valid_version"]); got != 1 {
		t.Fatalf("pools_without_valid_version = %v, want 1", rollup["pools_without_valid_version"])
	}
	names, ok := rollup["pools_without_valid_version_names"].([]string)
	if !ok || len(names) != 1 || names[0] != "lapsed" {
		t.Fatalf("pools_without_valid_version_names = %#v, want [lapsed]", rollup["pools_without_valid_version_names"])
	}

	pools, ok := rollup["pools"].([]map[string]any)
	if !ok || len(pools) != 3 {
		t.Fatalf("pools = %#v, want three entries", rollup["pools"])
	}
	byName := make(map[string]map[string]any, len(pools))
	for _, entry := range pools {
		name, _ := entry["pool"].(string)
		byName[name] = entry
	}

	rotating := byName["rotating"]
	if got := intFromAny(rotating["valid"]); got != 2 {
		t.Fatalf("rotating.valid = %v, want 2", rotating["valid"])
	}
	if got, want := rotating["next_expiry_at"], now.Add(30*time.Minute).Format(time.RFC3339Nano); got != want {
		t.Fatalf("rotating.next_expiry_at = %v, want %v", got, want)
	}
	if got := rotating["exhaustion_seconds"]; got != 7200.0 {
		t.Fatalf("rotating.exhaustion_seconds = %v, want 7200", got)
	}

	lapsed := byName["lapsed"]
	if got := intFromAny(lapsed["valid"]); got != 0 {
		t.Fatalf("lapsed.valid = %v, want 0", lapsed["valid"])
	}
	// The keys stay present with a null value so a checker's JSON path does
	// not change shape between a healthy and a dry pool.
	for _, key := range []string{"next_expiry_at", "next_expiry_seconds", "exhausted_at", "exhaustion_seconds"} {
		if _, present := lapsed[key]; !present {
			t.Fatalf("lapsed is missing key %q entirely: %#v", key, lapsed)
		}
		if lapsed[key] != nil {
			t.Fatalf("lapsed.%s = %v, want null", key, lapsed[key])
		}
	}

	// And the whole payload has to survive the JSON encoding the endpoint does.
	encoded, err := json.Marshal(rollup)
	if err != nil {
		t.Fatalf("marshal runtime_secrets rollup: %v", err)
	}
	if !strings.Contains(string(encoded), `"pools_without_valid_version":1`) {
		t.Fatalf("encoded rollup lost the empty-pool count: %s", encoded)
	}
}

func TestMetricsHandler_IngressAuthRejectByRouteAndCause(t *testing.T) {
	m := newRuntimeMetrics()
	m.observeIngressAuthReject("/webhooks/source", ingress.AuthRejectNoValidSecret)
	m.observeIngressAuthReject("/webhooks/source", ingress.AuthRejectNoValidSecret)
	m.observeIngressAuthReject("/webhooks/source", ingress.AuthRejectSignatureMismatch)
	m.observeIngressAuthReject("/webhooks/other", ingress.AuthRejectTimestampOutOfWindow)
	m.observeIngressAuthReject("/webhooks/other", "something-new")
	m.observeIngressAuthReject("", "")

	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_ingress_auth_rejected_total{route="/webhooks/source",reason="no_valid_secret"} 2`,
		`hookaido_ingress_auth_rejected_total{route="/webhooks/source",reason="signature_mismatch"} 1`,
		`hookaido_ingress_auth_rejected_total{route="/webhooks/other",reason="timestamp_out_of_window"} 1`,
		`hookaido_ingress_auth_rejected_total{route="/webhooks/other",reason="other"} 1`,
		// An unattributable reject keeps an empty route rather than being
		// dropped, and an unclassified one is `unspecified`.
		`hookaido_ingress_auth_rejected_total{route="",reason="unspecified"} 1`,
		// Zero-filled baseline, so an alert on a cause that has never fired is
		// still writable.
		`hookaido_ingress_auth_rejected_total{route="",reason="replay"} 0`,
		`hookaido_ingress_auth_rejected_total{route="",reason="credentials"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}

	diag := m.healthDiagnostics()
	ingressDiag, ok := diag["ingress"].(map[string]any)
	if !ok {
		t.Fatalf("expected ingress diagnostics object, got %T", diag["ingress"])
	}
	if got := intFromAny(ingressDiag["auth_rejected_total"]); got != 6 {
		t.Fatalf("auth_rejected_total = %v, want 6", ingressDiag["auth_rejected_total"])
	}
	byReason, ok := ingressDiag["auth_rejected_by_reason"].(map[string]any)
	if !ok {
		t.Fatalf("expected auth_rejected_by_reason map, got %T", ingressDiag["auth_rejected_by_reason"])
	}
	if got := intFromAny(byReason["no_valid_secret"]); got != 2 {
		t.Fatalf("auth_rejected_by_reason.no_valid_secret = %v, want 2", byReason["no_valid_secret"])
	}
	if got := intFromAny(byReason["replay"]); got != 0 {
		t.Fatalf("auth_rejected_by_reason.replay = %v, want a zero-filled 0", byReason["replay"])
	}
}

// warnCapture collects the runtime log so a test can assert on what an operator
// would actually see.
func warnCapture(t *testing.T) (*slog.Logger, func() []map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return logger, func() []map[string]any {
		out := make([]map[string]any, 0, 4)
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("log line %q is not JSON: %v", line, err)
			}
			out = append(out, rec)
		}
		return out
	}
}

func findLogRecord(records []map[string]any, msg string) map[string]any {
	for _, rec := range records {
		if rec["msg"] == msg {
			return rec
		}
	}
	return nil
}

// The line the incident wanted: a route whose secret_ref pool holds nothing
// valid says so at startup, instead of starting green and answering 401 to
// every sender.
func TestWarnSecretPoolsWithoutValidVersion_EmptyRuntimePool(t *testing.T) {
	src := `
secrets {
  secret "rotating" {
    runtime true
    max_versions 4
  }
}

"/webhooks/source" {
  auth hmac secret_ref "rotating"
  deliver "https://internal.example.com/source" { }
}
`
	t.Setenv("HOOKAIDO_SECRET_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x2a}, 32)))
	compiled := compileForReloadTest(t, src)
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	logger, records := warnCapture(t)
	state.warnSecretPoolsWithoutValidVersion(logger, "startup")

	rec := findLogRecord(records(), "route_secret_ref_without_valid_version")
	if rec == nil {
		t.Fatalf("expected a route_secret_ref_without_valid_version warning, got %#v", records())
	}
	if rec["level"] != "WARN" {
		t.Fatalf("level = %v, want WARN", rec["level"])
	}
	if rec["route"] != "/webhooks/source" {
		t.Fatalf("route = %v, want /webhooks/source", rec["route"])
	}
	if rec["auth"] != "hmac" {
		t.Fatalf("auth = %v, want hmac", rec["auth"])
	}
	if rec["trigger"] != "startup" {
		t.Fatalf("trigger = %v, want startup", rec["trigger"])
	}
	if got := intFromAny(rec["usable_secrets"]); got != 0 {
		t.Fatalf("usable_secrets = %v, want 0", rec["usable_secrets"])
	}
	if effect, _ := rec["effect"].(string); !strings.Contains(effect, "401") {
		t.Fatalf("effect = %q, want it to name the 401 consequence", effect)
	}
}

// A pool that is populated stays quiet, and a route that keeps a static secret
// beside an empty pool is reported as a degraded rotation rather than an outage.
func TestWarnSecretPoolsWithoutValidVersion_PopulatedAndPartial(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "static.key")
	if err := os.WriteFile(keyPath, []byte("static-secret"), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	src := fmt.Sprintf(`
secrets {
  secret "empty" {
    runtime true
  }
  secret "filled" {
    value "file:%s"
    valid_from "2026-01-01T00:00:00Z"
  }
}

"/healthy" {
  auth hmac secret_ref "filled"
  deliver "https://internal.example.com/healthy" { }
}

"/degraded" {
  auth hmac "file:%s"
  auth hmac secret_ref "empty"
  deliver "https://internal.example.com/degraded" { }
}
`, filepath.ToSlash(keyPath), filepath.ToSlash(keyPath))

	t.Setenv("HOOKAIDO_SECRET_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x2a}, 32)))
	compiled := compileForReloadTest(t, src)
	state := newRuntimeState(compiled)
	state.now = func() time.Time { return time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC) }
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}

	logger, records := warnCapture(t)
	state.warnSecretPoolsWithoutValidVersion(logger, "sighup")

	got := records()
	warnings := make([]map[string]any, 0, len(got))
	for _, rec := range got {
		if rec["msg"] == "route_secret_ref_without_valid_version" {
			warnings = append(warnings, rec)
		}
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly one warning (only /degraded), got %#v", got)
	}
	if warnings[0]["route"] != "/degraded" {
		t.Fatalf("warned about %v, want /degraded", warnings[0]["route"])
	}
	if n := intFromAny(warnings[0]["usable_secrets"]); n != 1 {
		t.Fatalf("usable_secrets = %v, want 1 (the static secret still authenticates)", warnings[0]["usable_secrets"])
	}
	if effect, _ := warnings[0]["effect"].(string); strings.Contains(effect, "401") {
		t.Fatalf("effect = %q, want the degraded-rotation wording, not the outage one", effect)
	}
}

// The sweeper is the only thing that watches a running process: a pool whose
// last version lapses between two reloads becomes broken with no config change
// and no request-side signal. It must say so once, on the transition -- and not
// every five minutes for as long as the outage lasts.
func TestSecretSweeper_WarnsOnTransitionIntoNoValidVersion(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	state := &runtimeState{secretRegistry: secrets.NewRegistry(), secretStore: secrets.NewMemoryStore()}

	rotating, err := secrets.NewPool("rotating", true, 0, []secrets.Version{
		{ID: "current", Value: []byte("a"), ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour)},
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if err := state.secretRegistry.Register(rotating); err != nil {
		t.Fatalf("Register: %v", err)
	}

	logger, records := warnCapture(t)
	sweeper := newSecretSweeper(state, newRuntimeMetrics(), logger)
	sweeper.now = func() time.Time { return now }

	sweeper.sweep("startup")
	if rec := findLogRecord(records(), "runtime_secret_pool_without_valid_version"); rec != nil {
		t.Fatalf("healthy pool warned at startup: %#v", rec)
	}

	// Two hours later the only version has lapsed. Nothing reloaded; nothing
	// else in the process noticed.
	sweeper.now = func() time.Time { return now.Add(2 * time.Hour) }
	sweeper.sweep("interval")

	rec := findLogRecord(records(), "runtime_secret_pool_without_valid_version")
	if rec == nil {
		t.Fatalf("expected the lapse to be reported, got %#v", records())
	}
	if rec["level"] != "WARN" || rec["pool"] != "rotating" || rec["trigger"] != "interval" {
		t.Fatalf("warning = %#v, want WARN on pool rotating with trigger interval", rec)
	}

	// A second sweep in the same state must stay silent, or the operator learns
	// to filter the line that matters.
	before := len(records())
	sweeper.sweep("interval")
	after := records()
	if len(after) != before {
		t.Fatalf("second sweep in the same state logged again: %#v", after[before:])
	}

	// And recovery is reported, so the incident has a closing line.
	if err := rotating.Add(secrets.Version{
		ID:        "fresh",
		Value:     []byte("b"),
		ValidFrom: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("Add fresh version: %v", err)
	}
	sweeper.sweep("interval")
	if rec := findLogRecord(records(), "runtime_secret_pool_valid_version_restored"); rec == nil {
		t.Fatalf("expected a recovery line, got %#v", records())
	}
}

// A pool that comes up with nothing valid -- the deploy in #295, where the
// issuer that fills it runs on a daily schedule -- is reported by the very
// first sweep, not only once it has been seen healthy before.
func TestSecretSweeper_WarnsOnFirstSweepOfEmptyPool(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	state := &runtimeState{secretRegistry: secrets.NewRegistry(), secretStore: secrets.NewMemoryStore()}

	empty, err := secrets.NewPool("rotating", true, 0, nil)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if err := state.secretRegistry.Register(empty); err != nil {
		t.Fatalf("Register: %v", err)
	}

	logger, records := warnCapture(t)
	sweeper := newSecretSweeper(state, newRuntimeMetrics(), logger)
	sweeper.now = func() time.Time { return now }
	sweeper.sweep("startup")

	rec := findLogRecord(records(), "runtime_secret_pool_without_valid_version")
	if rec == nil {
		t.Fatalf("expected the empty pool to be reported at startup, got %#v", records())
	}
	if got := intFromAny(rec["versions"]); got != 0 {
		t.Fatalf("versions = %v, want 0", rec["versions"])
	}
	if effect, _ := rec["effect"].(string); !strings.Contains(effect, "401") {
		t.Fatalf("effect = %q, want it to name the 401 consequence", effect)
	}
}

// A reload that introduces a route pointing at an empty runtime pool used to
// log config_reloaded_ok and nothing else, while the route it just installed
// rejected every webhook.
func TestReloadConfig_WarnsWhenRouteRefsAnEmptyPool(t *testing.T) {
	t.Setenv("HOOKAIDO_SECRET_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x2a}, 32)))

	dir := t.TempDir()
	original := `
pull_api { auth token "raw:t" }
"/hooks" { pull { path "/events" } }
`
	cfgPath := writeReloadFile(t, dir, original)
	running := compileForReloadTest(t, original)
	state := newRuntimeState(running)
	if err := state.loadAuth(running); err != nil {
		t.Fatalf("initial loadAuth: %v", err)
	}

	updated := `
pull_api { auth token "raw:t" }

secrets {
  secret "rotating" {
    runtime true
  }
}

"/hooks" { pull { path "/events" } }

"/signed" {
  auth hmac secret_ref "rotating"
  pull { path "/signed-events" }
}
`
	if err := os.WriteFile(cfgPath, []byte(updated), 0644); err != nil {
		t.Fatalf("write updated config: %v", err)
	}

	logger, records := warnCapture(t)
	if _, ok := reloadConfig(cfgPath, running, state, logger, "sighup"); !ok {
		t.Fatalf("expected the reload to succeed: %#v", records())
	}

	rec := findLogRecord(records(), "route_secret_ref_without_valid_version")
	if rec == nil {
		t.Fatalf("expected a warning for /signed, got %#v", records())
	}
	if rec["route"] != "/signed" || rec["trigger"] != "sighup" {
		t.Fatalf("warning = %#v, want route /signed with trigger sighup", rec)
	}
	// The warning has to precede the success line, so a log reader sees the
	// caveat attached to the reload it belongs to.
	warnIdx, okIdx := -1, -1
	for i, r := range records() {
		switch r["msg"] {
		case "route_secret_ref_without_valid_version":
			warnIdx = i
		case "config_reloaded_ok":
			okIdx = i
		}
	}
	if okIdx < 0 {
		t.Fatalf("expected config_reloaded_ok in the log: %#v", records())
	}
	if warnIdx > okIdx {
		t.Fatalf("warning logged after config_reloaded_ok (%d > %d)", warnIdx, okIdx)
	}
}
