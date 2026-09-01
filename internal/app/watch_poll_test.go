package app

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPollConfig_ReloadsWhenContentChanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Hookaidofile")
	initial := []byte("# v1\n")
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reloads := make(chan struct{}, 8)
	go pollConfig(ctx, path, 20*time.Millisecond, initial, discardLogger(), func() {
		reloads <- struct{}{}
	})

	// Unchanged content must not reload, however many ticks pass.
	select {
	case <-reloads:
		t.Fatal("unchanged config triggered a reload")
	case <-time.After(700 * time.Millisecond):
	}

	if err := os.WriteFile(path, []byte("# v2\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case <-reloads:
	case <-time.After(5 * time.Second):
		t.Fatal("changed config did not trigger a reload")
	}

	// One change is one reload: the hash advances on read.
	select {
	case <-reloads:
		t.Fatal("a single change triggered more than one reload")
	case <-time.After(700 * time.Millisecond):
	}
}

// The single-file bind mount this exists for replaces the file by rename, which
// is a new inode at the same path. Content, not identity, is what the poller
// compares, so the reload has to happen.
func TestPollConfig_DetectsAtomicReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Hookaidofile")
	initial := []byte("# v1\n")
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reloads := make(chan struct{}, 8)
	go pollConfig(ctx, path, 20*time.Millisecond, initial, discardLogger(), func() {
		reloads <- struct{}{}
	})

	tmp := filepath.Join(dir, "Hookaidofile.tmp")
	if err := os.WriteFile(tmp, []byte("# v2\n"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename: %v", err)
	}

	select {
	case <-reloads:
	case <-time.After(5 * time.Second):
		t.Fatal("atomic replace did not trigger a reload")
	}
}

// Content restored to what was already loaded is not a change. This is what
// keeps an editor's save-undo-save cycle from reloading the same config twice.
func TestPollConfig_IgnoresContentRestoredToTheLoadedVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Hookaidofile")
	initial := []byte("# v1\n")
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reloads := make(chan struct{}, 8)
	go pollConfig(ctx, path, 20*time.Millisecond, initial, discardLogger(), func() {
		reloads <- struct{}{}
	})

	// Rewriting identical bytes changes mtime but not content.
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	select {
	case <-reloads:
		t.Fatal("identical content triggered a reload")
	case <-time.After(700 * time.Millisecond):
	}
}

func TestPollConfig_SurvivesAMissingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Hookaidofile")
	initial := []byte("# v1\n")
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reloads := make(chan struct{}, 8)
	go pollConfig(ctx, path, 20*time.Millisecond, initial, discardLogger(), func() {
		reloads <- struct{}{}
	})

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	time.Sleep(700 * time.Millisecond)
	select {
	case <-reloads:
		t.Fatal("a missing file must not be reported as a config change")
	default:
	}

	// The poller kept going, so the file coming back is picked up.
	if err := os.WriteFile(path, []byte("# v2\n"), 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	select {
	case <-reloads:
	case <-time.After(5 * time.Second):
		t.Fatal("poller stopped after a transient read error")
	}
}

func TestPollConfig_StopsWithContext(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Hookaidofile")
	if err := os.WriteFile(path, []byte("# v1\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pollConfig(ctx, path, 20*time.Millisecond, []byte("# v1\n"), discardLogger(), func() {})
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pollConfig did not return when its context was cancelled")
	}
}

func TestPollConfig_NoopWithoutReloadOrInterval(t *testing.T) {
	// Both return immediately rather than spinning; a hung goroutine here would
	// be invisible in production.
	done := make(chan struct{})
	go func() {
		pollConfig(context.Background(), "irrelevant", time.Second, nil, discardLogger(), nil)
		pollConfig(context.Background(), "irrelevant", 0, nil, discardLogger(), func() {})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pollConfig did not return for a no-op configuration")
	}
}

func TestConfigOnSeparateDevice_RegularFileIsNotFlagged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Hookaidofile")
	if err := os.WriteFile(path, []byte("# v1\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	separate, known := configOnSeparateDevice(path)
	if known && separate {
		t.Fatal("a file in its own temp directory must not look like a single-file mount")
	}
}

func TestConfigOnSeparateDevice_MissingPathIsUnknown(t *testing.T) {
	if _, known := configOnSeparateDevice(filepath.Join(t.TempDir(), "absent")); known {
		t.Fatal("a missing config path cannot be classified")
	}
}

// run() validates the flag pair before it touches the filesystem or binds a
// port, so these exercise the real entry point without starting a server.
func TestRun_WatchIntervalRequiresWatch(t *testing.T) {
	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"hookaido", "run", "--watch-interval", "30s"}

	var code int
	_, stderr := captureOutput(t, func() { code = run() })

	if code != 2 {
		t.Fatalf("expected exit 2, got %d", code)
	}
	if !strings.Contains(stderr, "--watch-interval requires --watch") {
		t.Fatalf("expected the error to name the missing flag, got: %s", stderr)
	}
}

func TestRun_WatchIntervalBelowMinimumIsRejected(t *testing.T) {
	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"hookaido", "run", "--watch", "--watch-interval", "100ms"}

	var code int
	_, stderr := captureOutput(t, func() { code = run() })

	if code != 2 {
		t.Fatalf("expected exit 2, got %d", code)
	}
	if !strings.Contains(stderr, "--watch-interval must be at least") {
		t.Fatalf("expected the error to state the minimum, got: %s", stderr)
	}
}

// A plain overwrite truncates and then writes, so the file is briefly empty.
// Without the settle re-read that showed up as a change -- and for a config
// being rewritten with content that differs, as two: a rejected reload of the
// half-written file followed by the real one. Hammering identical rewrites is
// what reliably catches a regression of that.
func TestPollConfig_RepeatedIdenticalRewritesAreNotChanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Hookaidofile")
	initial := []byte("# v1\nroute \"/x\" {}\n")
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reloads := make(chan struct{}, 64)
	go pollConfig(ctx, path, 20*time.Millisecond, initial, discardLogger(), func() {
		reloads <- struct{}{}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 40; i++ {
			if err := os.WriteFile(path, initial, 0o644); err != nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	<-done

	// Let the poller settle on the final state before checking.
	time.Sleep(700 * time.Millisecond)
	select {
	case <-reloads:
		t.Fatal("rewriting identical content reported a config change")
	default:
	}
}

// A change that is still being written must not be reloaded half-way. The
// settle re-read is what defers it to the next tick.
func TestPollConfig_ReportsOnlyTheSettledContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Hookaidofile")
	initial := []byte("# v1\n")
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seen := make(chan string, 64)
	go pollConfig(ctx, path, 20*time.Millisecond, initial, discardLogger(), func() {
		data, err := os.ReadFile(path)
		if err != nil {
			seen <- "read-error"
			return
		}
		seen <- string(data)
	})

	// Write the new config in pieces, slowly enough that a poll lands inside it
	// (the gap exceeds the poll interval) but fast enough that the file is still
	// changing across a settle window.
	//
	// The gap is a fraction of pollSettleDelay rather than a bare duration,
	// because the relationship is the whole assumption of the test: at a flat
	// 120ms against a 200ms settle the margin was thin enough that a loaded
	// `-race` runner could stretch one sleep past the window, at which point the
	// poller correctly reported settled partial content and the test blamed it
	// for a stall in the writer.
	const chunkGap = pollSettleDelay / 5
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, chunk := range []string{"# v2\n", "route \"/a\" {}\n", "route \"/b\" {}\n"} {
		if _, err := f.WriteString(chunk); err != nil {
			t.Fatalf("write chunk: %v", err)
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("sync: %v", err)
		}
		time.Sleep(chunkGap)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	want := "# v2\nroute \"/a\" {}\nroute \"/b\" {}\n"
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got := <-seen:
			if got == want {
				return
			}
			// Anything else means a partial state was reported as a config
			// change, which is exactly what the settle exists to prevent.
			t.Fatalf("reload observed a partial config: %q", got)
		case <-deadline:
			t.Fatal("the settled config was never reported")
		}
	}
}
