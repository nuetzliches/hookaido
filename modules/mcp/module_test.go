package mcp

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestParseCSVList(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "empty",
			in:   "",
			want: nil,
		},
		{
			name: "trim_and_split",
			in:   " 127.0.0.1:2019 , https://ops.example.com/admin ",
			want: []string{"127.0.0.1:2019", "https://ops.example.com/admin"},
		},
		{
			name: "drop_empty_entries",
			in:   ",,127.0.0.1:2019,,",
			want: []string{"127.0.0.1:2019"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseCSVList(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parseCSVList(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestMcpModule_Name(t *testing.T) {
	m := &mcpModule{}
	if got := m.Name(); got != "mcp" {
		t.Fatalf("Name = %q, want mcp", got)
	}
}

// testIO returns a serveIO with closed/empty input (so the server returns
// immediately on EOF) and capture buffers for stdout and stderr.
func testIO() (serveIO, *bytes.Buffer, *bytes.Buffer) {
	stderr := &bytes.Buffer{}
	stdout := &bytes.Buffer{}
	return serveIO{
		in:         strings.NewReader(""),
		out:        stdout,
		err:        stderr,
		executable: func() (string, error) { return "/usr/bin/hookaido-test", nil },
	}, stdout, stderr
}

func TestServeCommand_MissingSubcommand(t *testing.T) {
	io, _, stderr := testIO()
	if rc := serveCommandWith(nil, io); rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
	if !strings.Contains(stderr.String(), "missing subcommand") {
		t.Fatalf("stderr=%q, want missing-subcommand message", stderr.String())
	}
}

func TestServeCommand_UnknownSubcommand(t *testing.T) {
	io, _, stderr := testIO()
	if rc := serveCommandWith([]string{"bogus"}, io); rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
	if !strings.Contains(stderr.String(), "unknown mcp subcommand: bogus") {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestServeCommand_ServeWithDefaults(t *testing.T) {
	// EOF on stdin makes server.Serve return nil; mcpServe should return 0.
	io, _, _ := testIO()
	rc := serveCommandWith([]string{"serve"}, io)
	if rc != 0 {
		t.Fatalf("rc=%d, want 0", rc)
	}
}

func TestServeCommand_InvalidFlag(t *testing.T) {
	io, _, _ := testIO()
	rc := serveCommandWith([]string{"serve", "--no-such-flag"}, io)
	if rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
}

func TestServeCommand_InvalidRole(t *testing.T) {
	io, _, stderr := testIO()
	rc := serveCommandWith([]string{"serve", "--role", "godmode"}, io)
	if rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
	// ParseRole's error message should make it to stderr.
	if stderr.Len() == 0 {
		t.Fatalf("expected role error on stderr")
	}
}

func TestServeCommand_RunBinaryDefaultsToExecutable(t *testing.T) {
	// When --run-binary isn't passed and executable() returns successfully,
	// the resolver path is exercised. EOF still drives the server to nil.
	io, _, _ := testIO()
	rc := serveCommandWith([]string{"serve",
		"--enable-runtime-control",
		"--admin-endpoint-allowlist", "127.0.0.1:2019,https://ops.example.com",
	}, io)
	if rc != 0 {
		t.Fatalf("rc=%d, want 0", rc)
	}
}

func TestDefaultServeIO_WiresOSHandles(t *testing.T) {
	io := defaultServeIO()
	if io.in == nil || io.out == nil || io.err == nil {
		t.Fatalf("defaultServeIO has nil handles: %+v", io)
	}
	if io.executable == nil {
		t.Fatalf("defaultServeIO.executable is nil")
	}
}

func TestMcpModule_ServeCommand_DispatchesToWith(t *testing.T) {
	// Exercises the production wrapper. Empty args makes serveCommandWith
	// return 2 before any IO happens, so the real os.Stderr write is just
	// a diagnostic line interleaved with test output.
	m := &mcpModule{}
	if rc := m.ServeCommand(nil); rc != 2 {
		t.Fatalf("rc=%d, want 2", rc)
	}
}

func TestServeCommand_ExecutableLookupFailureFallsThrough(t *testing.T) {
	// If executable() errors, mcpServe should still proceed (bin stays empty)
	// rather than aborting. EOF on stdin then drives a clean shutdown.
	stderr := &bytes.Buffer{}
	stdout := &bytes.Buffer{}
	io := serveIO{
		in:         strings.NewReader(""),
		out:        stdout,
		err:        stderr,
		executable: func() (string, error) { return "", errors.New("no exe") },
	}
	rc := serveCommandWith([]string{"serve"}, io)
	if rc != 0 {
		t.Fatalf("rc=%d, want 0 (executable lookup failure should not be fatal)", rc)
	}
}
