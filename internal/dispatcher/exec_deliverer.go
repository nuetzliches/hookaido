package dispatcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// ExecDeliverer delivers webhook payloads by executing a local command as a
// subprocess. The payload is piped to stdin; metadata is passed as environment
// variables. Exit codes are mapped to HTTP-like status codes for the
// dispatcher's retry/DLQ classification.
//
// Exit code mapping:
//
//	0         → 200 (success, ack)
//	75        → 503 (EX_TEMPFAIL, retriable)
//	1-125     → 500 (general failure, retriable via shouldRetry)
//	signal    → 502 (killed by signal, retriable)
//
// Startup failures (file not found, permission denied) are mapped to
// ErrPolicyDenied (not retriable, immediate DLQ).
type ExecDeliverer struct {
	Logger *slog.Logger

	// MaxStderrBytes caps how many bytes of stderr are included in log
	// output. Zero uses the default of 4096.
	MaxStderrBytes int
}

const defaultMaxStderrBytes = 4096

// ErrExecNotFound is returned when the command cannot be found or is not
// executable.
var ErrExecNotFound = fmt.Errorf("%w: command not found or not executable", ErrPolicyDenied)

func (e *ExecDeliverer) Deliver(ctx context.Context, d Delivery) Result {
	command := d.URL
	if command == "" {
		return Result{Err: fmt.Errorf("%w: empty command", ErrPolicyDenied)}
	}

	cmd := exec.CommandContext(ctx, command)

	// Build environment from delivery metadata + user-defined env vars.
	env := buildExecEnv(d)
	cmd.Env = env

	// Pipe payload to stdin.
	cmd.Stdin = bytes.NewReader(d.Body)

	// Capture stderr for logging; discard stdout.
	maxStderr := e.MaxStderrBytes
	if maxStderr <= 0 {
		maxStderr = defaultMaxStderrBytes
	}
	var stderrBuf bytes.Buffer
	cmd.Stderr = &limitWriter{w: &stderrBuf, max: maxStderr}

	err := cmd.Run()

	logger := e.Logger
	if logger == nil {
		logger = slog.Default()
	}

	if stderrBuf.Len() > 0 {
		logger.Debug("exec_stderr",
			slog.String("command", command),
			slog.String("event_id", d.ID),
			slog.String("stderr", stderrBuf.String()),
		)
	}

	if err == nil {
		return Result{StatusCode: 200}
	}

	// Context deadline exceeded → retriable (like HTTP 408).
	if ctx.Err() != nil {
		return Result{StatusCode: 408, Err: ctx.Err()}
	}

	// Extract exit code from the error.
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code := exitErr.ExitCode()

		// ExitCode() returns -1 when the process was killed by a signal
		// (on Unix). Treat as retriable.
		if code == -1 {
			return Result{StatusCode: 502, Err: fmt.Errorf("exec killed by signal: %w", err)}
		}

		switch {
		case code == 75:
			// EX_TEMPFAIL from sysexits.h → retriable.
			return Result{StatusCode: 503, Err: fmt.Errorf("exec exit %d: %w", code, err)}
		default:
			// General failure → retriable (status 500 triggers shouldRetry).
			return Result{StatusCode: 500, Err: fmt.Errorf("exec exit %d: %w", code, err)}
		}
	}

	// Startup failures: binary not found on PATH, file not found, or
	// permission denied before exec. These are permanent errors — no
	// point retrying.
	if errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
		return Result{Err: fmt.Errorf("%w: %v", ErrExecNotFound, err)}
	}

	// Other errors → retriable.
	return Result{StatusCode: 500, Err: err}
}

func buildExecEnv(d Delivery) []string {
	// Start with a minimal set — do NOT inherit the full os.Environ() to
	// prevent leaking host secrets, but include PATH so scripts can find
	// commands (jq, curl, grep, etc.).
	env := make([]string, 0, 10+len(d.Header)+len(d.ExecEnv))

	if p := os.Getenv("PATH"); p != "" {
		env = append(env, "PATH="+p)
	}

	env = append(env, "HOOKAIDO_ROUTE="+d.Route)
	env = append(env, "HOOKAIDO_EVENT_ID="+d.ID)
	env = append(env, "HOOKAIDO_ATTEMPT="+strconv.Itoa(d.Attempt))

	if ct := d.Header.Get("Content-Type"); ct != "" {
		env = append(env, "HOOKAIDO_CONTENT_TYPE="+ct)
	}

	// Pass all original request headers as HOOKAIDO_HEADER_<NAME>.
	for name, vals := range d.Header {
		key := "HOOKAIDO_HEADER_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
		env = append(env, key+"="+strings.Join(vals, ", "))
	}

	// User-defined env vars from config (may reference secrets via
	// placeholder resolution at config compile time).
	for k, v := range d.ExecEnv {
		env = append(env, k+"="+v)
	}

	return env
}

// limitWriter wraps a bytes.Buffer and stops writing after max bytes.
type limitWriter struct {
	w   *bytes.Buffer
	max int
}

func (lw *limitWriter) Write(p []byte) (int, error) {
	remaining := lw.max - lw.w.Len()
	if remaining <= 0 {
		return len(p), nil // discard silently
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	return lw.w.Write(p)
}
