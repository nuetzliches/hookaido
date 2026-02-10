package app

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

func newLogger(level string) (*slog.Logger, error) {
	l, _, err := newLoggerToSink(level, "stderr", "")
	return l, err
}

func newLoggerWithLevel(level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})
	return slog.New(h)
}

func newLoggerToSink(level, output, path string) (*slog.Logger, io.Closer, error) {
	lvl, err := parseLogLevel(level)
	if err != nil {
		return nil, nil, err
	}
	w, closer, err := openLogSink(output, path)
	if err != nil {
		return nil, nil, err
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: lvl,
	})
	return slog.New(h), closer, nil
}

func parseLogLevel(level string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid --log-level %q (use: debug|info|warn|error)", level)
	}
}

func openLogSink(output, path string) (io.Writer, io.Closer, error) {
	switch strings.ToLower(strings.TrimSpace(output)) {
	case "", "stderr":
		return os.Stderr, nil, nil
	case "stdout":
		return os.Stdout, nil, nil
	case "file":
		p := strings.TrimSpace(path)
		if p == "" {
			return nil, nil, errors.New("log output file requires path")
		}
		f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("open log file %q: %w", p, err)
		}
		return f, f, nil
	default:
		return nil, nil, fmt.Errorf("invalid log output %q (use: stdout|stderr|file)", output)
	}
}

func newDiscardLogger() *slog.Logger {
	h := slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	return slog.New(h)
}

func withAccessLog(logger *slog.Logger, next http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if next == nil {
		next = http.NotFoundHandler()
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)

		d := time.Since(start)
		status := sw.status
		if status == 0 {
			status = http.StatusOK
		}

		logger.Info("http_request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", status),
			slog.Int("bytes", sw.bytesWritten),
			slog.Duration("duration", d),
			slog.String("remote_addr", r.RemoteAddr),
		)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status       int
	bytesWritten int
}

func (w *statusWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytesWritten += n
	return n, err
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijack not supported")
	}
	return h.Hijack()
}

func (w *statusWriter) Push(target string, opts *http.PushOptions) error {
	p, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return p.Push(target, opts)
}

func serveOnListener(logger *slog.Logger, name string, srv *http.Server, ln net.Listener, cancel func()) {
	go func() {
		err := srv.Serve(ln)
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return
		}
		if logger == nil {
			logger = slog.Default()
		}
		logger.Error("http_server_error", slog.String("name", name), slog.Any("err", err))
		if cancel != nil {
			cancel()
		}
	}()
}
