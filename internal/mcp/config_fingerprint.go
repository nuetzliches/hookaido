package mcp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/config"
)

// A reload used to be confirmed with waitForAdminHealth, which polls
// GET /healthz for a 200 -- the unauthenticated liveness endpoint a running
// instance answers regardless of which config it is running. Two paths reported
// success while the process still served the old config:
//
//   - `hookaido run` without --watch (the default): writing the file triggers
//     nothing, healthz answers 200 on the first poll, and config_apply returned
//     ok/applied/reloaded -- so a token revoked through config_apply was
//     reported applied while the old token kept authenticating.
//   - With --watch, a reload that fails at apply time logs config_reload_failed
//     and keeps the old config, while healthz stays 200.
//
// Because the check could not fail for those reasons, rollbackConfigFile was
// effectively dead code.
//
// The instance now reports the fingerprint of the config bytes it is running
// under /healthz?details=true, so the tools can wait for the config they wrote
// to actually be in force, and roll back when it is not.

// configFingerprint is the fingerprint the running instance reports for a given
// config file content.
func configFingerprint(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

type runningConfigIdentity struct {
	Fingerprint string
	Generation  int
}

// errConfigDiagnosticsUnavailable reports that the instance answered health
// checks but did not report a config fingerprint -- an older instance, or one
// whose admin API predates this field.
var errConfigDiagnosticsUnavailable = errors.New("running instance does not report a config fingerprint")

// fetchRunningConfigIdentity reads the config identity from the admin API's
// detailed health endpoint.
func fetchRunningConfigIdentity(compiled config.Compiled, timeout time.Duration) (runningConfigIdentity, error) {
	url, err := adminHealthURL(compiled.AdminAPI)
	if err != nil {
		return runningConfigIdentity{}, err
	}
	token, err := loadAdminHealthToken(compiled.AdminAPI.AuthTokens)
	if err != nil {
		return runningConfigIdentity{}, err
	}

	if strings.Contains(url, "?") {
		url += "&details=true"
	} else {
		url += "?details=true"
	}

	// No Proxy, for the same reason as waitForAdminHealth: this is the
	// operator's own admin endpoint and the request carries the admin token.
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: adminProxyTLSConfig(compiled.AdminAPI)},
		Timeout:   minDuration(timeout, 5*time.Second),
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return runningConfigIdentity{}, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return runningConfigIdentity{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return runningConfigIdentity{}, fmt.Errorf("health check returned status %d", resp.StatusCode)
	}

	var payload struct {
		Diagnostics struct {
			Config struct {
				Fingerprint string `json:"fingerprint"`
				Generation  int    `json:"generation"`
			} `json:"config"`
		} `json:"diagnostics"`
	}
	// A body that is not the expected JSON, or that carries no fingerprint,
	// both mean the same thing to the caller: this instance cannot say which
	// config it is running. Reporting them alike keeps an older instance from
	// looking like a failure while still refusing to claim a verified reload.
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return runningConfigIdentity{}, fmt.Errorf("%w: %v", errConfigDiagnosticsUnavailable, err)
	}
	if strings.TrimSpace(payload.Diagnostics.Config.Fingerprint) == "" {
		return runningConfigIdentity{}, errConfigDiagnosticsUnavailable
	}
	return runningConfigIdentity{
		Fingerprint: payload.Diagnostics.Config.Fingerprint,
		Generation:  payload.Diagnostics.Config.Generation,
	}, nil
}

// waitForRunningConfig polls until the instance reports it is running the given
// fingerprint, and returns an error when it does not within the timeout.
func waitForRunningConfig(compiled config.Compiled, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		identity, err := fetchRunningConfigIdentity(compiled, timeout)
		switch {
		case err == nil && identity.Fingerprint == want:
			return nil
		case err == nil:
			lastErr = fmt.Errorf("running config fingerprint is %s, want %s", shortFingerprint(identity.Fingerprint), shortFingerprint(want))
		case errors.Is(err, errConfigDiagnosticsUnavailable):
			// Nothing to wait for: this instance cannot report what it runs.
			return err
		default:
			lastErr = err
		}

		if !time.Now().Before(deadline) {
			break
		}
		sleep := minDuration(200*time.Millisecond, time.Until(deadline))
		if sleep <= 0 {
			break
		}
		time.Sleep(sleep)
	}
	if lastErr == nil {
		lastErr = errors.New("timed out")
	}
	return fmt.Errorf("the running instance did not adopt the written config (%w); it may be running without --watch, or the reload may have failed -- check the instance log for config_reload_failed", lastErr)
}

func shortFingerprint(fp string) string {
	if len(fp) <= 12 {
		return fp
	}
	return fp[:12]
}

// signalReloadBestEffort sends SIGHUP to the configured instance when it can,
// so config_apply does not depend on the operator having started the process
// with --watch (which defaults to false). It reports whether the signal was
// sent and, when it was not, why -- never an error: the config is verified by
// its fingerprint afterwards either way, and an instance running with --watch
// needs no signal at all.
func (s *Server) signalReloadBestEffort(args map[string]any) (bool, string) {
	if strings.TrimSpace(s.PIDFilePath) == "" {
		return false, "pid file path is not configured"
	}
	pidFile, err := s.resolvePIDFilePath(args)
	if err != nil {
		return false, err.Error()
	}
	pid, err := readPIDFileValue(pidFile)
	if err != nil {
		return false, err.Error()
	}
	if !isPIDRunning(pid) {
		return false, fmt.Sprintf("process %d is not running", pid)
	}
	if err := signalPID(pid, syscall.SIGHUP); err != nil {
		return false, err.Error()
	}
	return true, ""
}
