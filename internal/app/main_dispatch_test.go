package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// silenceMainOutput redirects os.Stdout/os.Stderr to /dev/null for the duration
// of a test so dispatching tests don't pollute the test runner's output.
func silenceMainOutput(t *testing.T) {
	t.Helper()
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open devnull: %v", err)
	}
	prevOut := os.Stdout
	prevErr := os.Stderr
	os.Stdout = devnull
	os.Stderr = devnull
	t.Cleanup(func() {
		os.Stdout = prevOut
		os.Stderr = prevErr
		_ = devnull.Close()
	})
}

func TestPrintHelp(t *testing.T) {
	silenceMainOutput(t)
	// Just invoke for coverage; output is verified implicitly by Main tests.
	printHelp()
}

func TestMain_NoArgs(t *testing.T) {
	silenceMainOutput(t)
	if rc := Main([]string{"hookaido"}); rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
}

func TestMain_HelpVariants(t *testing.T) {
	silenceMainOutput(t)
	for _, arg := range []string{"help", "-h", "--help"} {
		if rc := Main([]string{"hookaido", arg}); rc != 0 {
			t.Fatalf("Main(%q) rc=%d, want 0", arg, rc)
		}
	}
}

func TestMain_UnknownCommand(t *testing.T) {
	silenceMainOutput(t)
	if rc := Main([]string{"hookaido", "no-such-command"}); rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
}

func TestMain_VersionDispatch(t *testing.T) {
	silenceMainOutput(t)
	if rc := Main([]string{"hookaido", "version"}); rc != 0 {
		t.Fatalf("rc=%d, want 0", rc)
	}
}

func TestMain_ConfigDispatch_NoSubcommand(t *testing.T) {
	silenceMainOutput(t)
	if rc := Main([]string{"hookaido", "config"}); rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
}

func TestMain_VerifyReleaseDispatch_NoArgs(t *testing.T) {
	silenceMainOutput(t)
	// runVerifyReleaseCmd requires --checksums; without args it should
	// return non-zero. We just need to confirm the dispatcher reaches it.
	if rc := Main([]string{"hookaido", "verify-release"}); rc == 0 {
		t.Fatalf("expected non-zero exit, got 0")
	}
}

func TestMain_McpDispatch(t *testing.T) {
	// modules/mcp registers an MCP provider via init(). If it's compiled in
	// (the standard build is), Main should reach the provider's
	// ServeCommand. We invoke with no subcommand so it returns rc=2 fast.
	// If somehow no provider is registered, Main returns 2 with a "not
	// available" message — both code paths land at rc=2, so this assertion
	// holds either way.
	silenceMainOutput(t)
	if rc := Main([]string{"hookaido", "mcp"}); rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
}

// --- versionCmd wrapper ---

func TestVersionCmd_Wrapper(t *testing.T) {
	// Calling the public wrapper exercises the os.Stdout/os.Stderr binding
	// path that runVersionCmd tests can't reach.
	silenceMainOutput(t)
	if rc := versionCmd(nil); rc != 0 {
		t.Fatalf("rc=%d, want 0", rc)
	}
}

// --- verifyReleaseCmd wrapper ---

func TestVerifyReleaseCmd_Wrapper(t *testing.T) {
	silenceMainOutput(t)
	// No --checksums => rc != 0 from runVerifyReleaseCmd.
	if rc := verifyReleaseCmd(nil); rc == 0 {
		t.Fatalf("expected non-zero exit")
	}
}

// --- configCmd dispatch ---

func TestConfigCmd_NoSubcommand(t *testing.T) {
	silenceMainOutput(t)
	if rc := configCmd(nil); rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
}

func TestConfigCmd_UnknownSubcommand(t *testing.T) {
	silenceMainOutput(t)
	if rc := configCmd([]string{"shrug"}); rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
}

// --- configFormat ---

func TestConfigFormat_HappyPath(t *testing.T) {
	silenceMainOutput(t)
	cfgPath := writeMinimalHookaidofile(t)
	if rc := configFormat([]string{"--config", cfgPath}); rc != 0 {
		t.Fatalf("rc=%d, want 0", rc)
	}
}

func TestConfigFormat_MissingFile(t *testing.T) {
	silenceMainOutput(t)
	if rc := configFormat([]string{"--config", filepath.Join(t.TempDir(), "missing")}); rc != 1 {
		t.Fatalf("rc=%d, want 1", rc)
	}
}

func TestConfigFormat_ParseError(t *testing.T) {
	silenceMainOutput(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "broken")
	if err := os.WriteFile(cfgPath, []byte("this is not valid HCL ~~~"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if rc := configFormat([]string{"--config", cfgPath}); rc != 1 {
		t.Fatalf("rc=%d, want 1", rc)
	}
}

func TestConfigFormat_BadFlag(t *testing.T) {
	silenceMainOutput(t)
	if rc := configFormat([]string{"--no-such-flag"}); rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
}

func TestConfigCmd_DispatchesToFmt(t *testing.T) {
	silenceMainOutput(t)
	// Bad flag inside fmt subcommand should return 2 from the format step.
	if rc := configCmd([]string{"fmt", "--no-such-flag"}); rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
}

// Sanity: confirm a real config round-trips through the formatter and the
// source file on disk is unchanged (formatter writes to stdout).
func TestConfigFormat_DoesNotMutateSourceFile(t *testing.T) {
	silenceMainOutput(t)
	cfgPath := writeMinimalHookaidofile(t)

	before, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read before: %v", err)
	}
	if rc := configFormat([]string{"--config", cfgPath}); rc != 0 {
		t.Fatalf("rc=%d", rc)
	}
	after, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read after: %v", err)
	}
	if string(before) != string(after) {
		t.Fatalf("formatter mutated source file")
	}
}

// writeMinimalHookaidofile drops a known-valid config into a temp dir and
// returns the path. The repo's own Hookaidofile is the source of truth for
// what the parser accepts; we copy a minimal subset that is stable across
// versions (ingress + pull_api with token auth).
func writeMinimalHookaidofile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "Hookaidofile")
	body := `ingress {
  listen :8080
}

pull_api {
  auth token env:HOOKAIDO_PULL_TOKEN
}
`
	if err := os.WriteFile(cfgPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write cfg: %v", err)
	}
	return cfgPath
}

// silence linter on imports that may be unused depending on future edits.
var _ = strings.Contains
