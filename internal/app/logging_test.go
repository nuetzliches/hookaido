package app

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		in      string
		want    slog.Level
		wantErr bool
	}{
		{"debug", slog.LevelDebug, false},
		{"INFO", slog.LevelInfo, false},
		{"", slog.LevelInfo, false},
		{"warn", slog.LevelWarn, false},
		{"warning", slog.LevelWarn, false},
		{"  Error  ", slog.LevelError, false},
		{"trace", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseLogLevel(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOpenLogSink(t *testing.T) {
	t.Run("stderr_default", func(t *testing.T) {
		w, closer, err := openLogSink("", "")
		if err != nil {
			t.Fatalf("openLogSink: %v", err)
		}
		if w != os.Stderr || closer != nil {
			t.Fatalf("expected os.Stderr, nil closer; got %v %v", w, closer)
		}
	})
	t.Run("stdout", func(t *testing.T) {
		w, closer, err := openLogSink("STDOUT", "")
		if err != nil {
			t.Fatalf("openLogSink: %v", err)
		}
		if w != os.Stdout || closer != nil {
			t.Fatalf("expected os.Stdout, nil closer; got %v %v", w, closer)
		}
	})
	t.Run("file_requires_path", func(t *testing.T) {
		_, _, err := openLogSink("file", "  ")
		if err == nil || !strings.Contains(err.Error(), "requires path") {
			t.Fatalf("err=%v, want requires-path", err)
		}
	})
	t.Run("file_writes_to_disk", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "log.json")
		w, closer, err := openLogSink("file", path)
		if err != nil {
			t.Fatalf("openLogSink: %v", err)
		}
		defer closer.Close()
		if _, err := w.Write([]byte("hello\n")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := closer.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(body) != "hello\n" {
			t.Fatalf("file contents=%q", body)
		}
	})
	t.Run("file_open_error", func(t *testing.T) {
		// A path under a non-existent directory triggers OpenFile error.
		_, _, err := openLogSink("file", filepath.Join(t.TempDir(), "missing-dir", "log"))
		if err == nil {
			t.Fatalf("expected open error")
		}
	})
	t.Run("invalid_output", func(t *testing.T) {
		_, _, err := openLogSink("syslog", "")
		if err == nil || !strings.Contains(err.Error(), "invalid log output") {
			t.Fatalf("err=%v, want invalid-log-output", err)
		}
	})
}

func TestNewLogger(t *testing.T) {
	logger, err := newLogger("debug")
	if err != nil {
		t.Fatalf("newLogger: %v", err)
	}
	if logger == nil {
		t.Fatalf("nil logger")
	}
	if _, err := newLogger("nonsense"); err == nil {
		t.Fatalf("expected error for invalid level")
	}
}

func TestNewLoggerToSink_FileAndError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	logger, closer, err := newLoggerToSink("info", "file", path)
	if err != nil {
		t.Fatalf("newLoggerToSink: %v", err)
	}
	logger.Info("hello", slog.String("k", "v"))
	if closer == nil {
		t.Fatalf("expected closer for file sink")
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(body), `"k":"v"`) {
		t.Fatalf("log payload missing key/value: %q", body)
	}

	// Bad level surfaces from parseLogLevel before any sink work.
	if _, _, err := newLoggerToSink("nonsense", "stderr", ""); err == nil {
		t.Fatalf("expected level error")
	}
	// Bad sink surfaces after level parses.
	if _, _, err := newLoggerToSink("info", "syslog", ""); err == nil {
		t.Fatalf("expected sink error")
	}
}

func TestNewDiscardLogger(t *testing.T) {
	logger := newDiscardLogger()
	if logger == nil {
		t.Fatalf("nil logger")
	}
	// Verify it discards by writing at debug level (would otherwise fail
	// in a real handler if buffer were exhausted, etc.)
	logger.Debug("ignore me")
}

// --- statusWriter ---

func TestStatusWriter_WriteHeaderRecorded(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec}
	sw.WriteHeader(http.StatusTeapot)
	if sw.status != http.StatusTeapot {
		t.Fatalf("status=%d", sw.status)
	}
	if rec.Code != http.StatusTeapot {
		t.Fatalf("recorder code=%d", rec.Code)
	}
}

func TestStatusWriter_WriteDefaultsTo200(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec}
	n, err := sw.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("Write n=%d err=%v", n, err)
	}
	if sw.status != http.StatusOK {
		t.Fatalf("status=%d, want 200", sw.status)
	}
	if sw.bytesWritten != 5 {
		t.Fatalf("bytesWritten=%d", sw.bytesWritten)
	}
}

func TestStatusWriter_FlushPassthrough(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec}
	// httptest.Recorder implements Flusher; this should not panic.
	sw.Flush()
	// Calling Flush on a non-Flusher inner is a no-op.
	noFlush := &statusWriter{ResponseWriter: nonFlusher{}}
	noFlush.Flush()
}

type nonFlusher struct{}

func (nonFlusher) Header() http.Header       { return http.Header{} }
func (nonFlusher) Write([]byte) (int, error) { return 0, nil }
func (nonFlusher) WriteHeader(int)           {}

func TestStatusWriter_HijackUnsupported(t *testing.T) {
	sw := &statusWriter{ResponseWriter: nonFlusher{}}
	if _, _, err := sw.Hijack(); err == nil {
		t.Fatalf("expected hijack-not-supported error")
	}
}

func TestStatusWriter_HijackSupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sw := &statusWriter{ResponseWriter: w}
		conn, _, err := sw.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close()
	}))
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL)
	if err == nil {
		_ = resp.Body.Close()
	}
}

func TestStatusWriter_PushUnsupported(t *testing.T) {
	sw := &statusWriter{ResponseWriter: nonFlusher{}}
	if err := sw.Push("/foo", nil); !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("err=%v, want ErrNotSupported", err)
	}
}

// --- withAccessLog ---

func TestWithAccessLog_LogsRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))

	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ok"))
	})

	h := withAccessLog(logger, inner)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/some/path", nil)
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatalf("inner not called")
	}
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d", rec.Code)
	}
	out := buf.String()
	if !strings.Contains(out, `"path":"/some/path"`) {
		t.Fatalf("missing path in log: %s", out)
	}
	if !strings.Contains(out, `"status":202`) {
		t.Fatalf("missing status in log: %s", out)
	}
	if !strings.Contains(out, `"bytes":2`) {
		t.Fatalf("missing bytes in log: %s", out)
	}
}

func TestWithAccessLog_DefaultsToOK_WhenNoWriteHeader(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// No WriteHeader, no Write — status defaults to 200 in log line.
	})
	h := withAccessLog(logger, inner)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	if !strings.Contains(buf.String(), `"status":200`) {
		t.Fatalf("expected default 200 status in log: %s", buf.String())
	}
}

func TestWithAccessLog_NilLoggerAndHandler(t *testing.T) {
	// Passing nil should swap in slog.Default and http.NotFoundHandler
	// without panicking.
	h := withAccessLog(nil, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", rec.Code)
	}
}

// --- serveOnListener ---

func TestServeOnListener_RunsAndExitsCleanlyOnShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	}

	cancelled := make(chan struct{}, 1)
	cancel := func() { cancelled <- struct{}{} }

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	serveOnListener(logger, "test", srv, ln, cancel)

	// Quick health check to confirm the server is actually serving.
	resp, err := http.Get("http://" + ln.Addr().String())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()

	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// http.ErrServerClosed should NOT trigger cancel().
	select {
	case <-cancelled:
		t.Fatalf("cancel should not fire on clean shutdown")
	case <-time.After(150 * time.Millisecond):
	}
}

func TestServeOnListener_FailureTriggersCancel(t *testing.T) {
	// Pre-close the listener so srv.Serve returns a real error immediately.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_ = ln.Close()

	srv := &http.Server{
		Handler: http.NotFoundHandler(),
	}
	cancelled := make(chan struct{}, 1)
	cancel := func() { cancelled <- struct{}{} }

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	serveOnListener(logger, "test-fail", srv, ln, cancel)

	select {
	case <-cancelled:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatalf("cancel should have fired on serve error")
	}
	if !strings.Contains(buf.String(), "http_server_error") {
		t.Fatalf("missing http_server_error log: %s", buf.String())
	}
}

func TestServeOnListener_NilLoggerAndCancel(t *testing.T) {
	// Verify nil logger + nil cancel don't panic on serve failure.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_ = ln.Close()

	srv := &http.Server{Handler: http.NotFoundHandler()}
	serveOnListener(nil, "noctx", srv, ln, nil)
	// Give the goroutine a moment to run.
	time.Sleep(100 * time.Millisecond)
}

// silence unused-import warnings if this file is built standalone.
var _ = bufio.NewReader
