package app

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestProcessExists(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("processExists implementation differs on windows")
	}
	if !processExists(os.Getpid()) {
		t.Fatalf("processExists(self) should be true")
	}
	if processExists(0) {
		t.Fatalf("processExists(0) should be false")
	}
	if processExists(-1) {
		t.Fatalf("processExists(-1) should be false")
	}
	// PIDs above the kernel max are guaranteed-unused on Linux; we use
	// a very high value to avoid colliding with real processes.
	if processExists(2_000_000_000) {
		t.Fatalf("processExists(huge) should be false")
	}
}

func TestReadPIDFile(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing", func(t *testing.T) {
		_, err := readPIDFile(filepath.Join(dir, "missing.pid"))
		if err == nil {
			t.Fatalf("expected error for missing file")
		}
	})

	t.Run("empty", func(t *testing.T) {
		path := filepath.Join(dir, "empty.pid")
		if err := os.WriteFile(path, []byte("   \n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		_, err := readPIDFile(path)
		if err == nil || !strings.Contains(err.Error(), "is empty") {
			t.Fatalf("err=%v, want is-empty", err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		path := filepath.Join(dir, "garbage.pid")
		if err := os.WriteFile(path, []byte("not-a-number\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		_, err := readPIDFile(path)
		if err == nil || !strings.Contains(err.Error(), "invalid pid") {
			t.Fatalf("err=%v, want invalid-pid", err)
		}
	})

	t.Run("zero_or_negative", func(t *testing.T) {
		path := filepath.Join(dir, "zero.pid")
		if err := os.WriteFile(path, []byte("0\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		_, err := readPIDFile(path)
		if err == nil {
			t.Fatalf("expected error for zero pid")
		}
	})

	t.Run("valid", func(t *testing.T) {
		path := filepath.Join(dir, "ok.pid")
		if err := os.WriteFile(path, []byte("1234\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		pid, err := readPIDFile(path)
		if err != nil {
			t.Fatalf("readPIDFile: %v", err)
		}
		if pid != 1234 {
			t.Fatalf("pid=%d, want 1234", pid)
		}
	})
}

func TestWritePIDFile_AtomicReplaceAndPermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "service.pid")

	// First write creates the file.
	if err := writePIDFile(path, 4242); err != nil {
		t.Fatalf("writePIDFile: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.TrimSpace(string(body)) != "4242" {
		t.Fatalf("body=%q", body)
	}

	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("mode=%v, want 0600", info.Mode().Perm())
		}
	}

	// Second write replaces atomically.
	if err := writePIDFile(path, 9999); err != nil {
		t.Fatalf("writePIDFile second: %v", err)
	}
	body, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.TrimSpace(string(body)) != "9999" {
		t.Fatalf("body after replace=%q", body)
	}
}

func TestClaimPIDFile_EmptyPathIsNoOp(t *testing.T) {
	cleanup, err := claimPIDFile("")
	if err != nil {
		t.Fatalf("claim empty: %v", err)
	}
	if cleanup == nil {
		t.Fatalf("nil cleanup")
	}
	cleanup() // must not panic
}

func TestClaimPIDFile_HappyPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "claim.pid")
	cleanup, err := claimPIDFile(path)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer cleanup()

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.TrimSpace(string(body)) != fmt.Sprintf("%d", os.Getpid()) {
		t.Fatalf("body=%q, want self pid", body)
	}

	// cleanup() must remove the pid file when the contents still match us.
	cleanup()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("pid file should be removed after cleanup, got err=%v", err)
	}
}

func TestClaimPIDFile_StalePIDFileIsReplaced(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("processExists is unix-specific in this test scope")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "stale.pid")
	// PID well above any realistic running process; pidRunning() should
	// see it as not-running and the claim should succeed.
	if err := os.WriteFile(path, []byte("2000000000\n"), 0o600); err != nil {
		t.Fatalf("write stale: %v", err)
	}

	cleanup, err := claimPIDFile(path)
	if err != nil {
		t.Fatalf("claim with stale pid: %v", err)
	}
	defer cleanup()

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.TrimSpace(string(body)) != fmt.Sprintf("%d", os.Getpid()) {
		t.Fatalf("body=%q, want self pid (stale should have been replaced)", body)
	}
}

func TestClaimPIDFile_RunningProcessConflict(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("processExists is unix-specific in this test scope")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "conflict.pid")
	// Use our own PID — guaranteed to be running, so the claim must fail.
	if err := os.WriteFile(path, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o600); err != nil {
		t.Fatalf("write self pid: %v", err)
	}
	if _, err := claimPIDFile(path); err == nil {
		t.Fatalf("expected conflict error")
	}
}

func TestClaimPIDFile_CreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "nested", "subdir", "service.pid")
	cleanup, err := claimPIDFile(nested)
	if err != nil {
		t.Fatalf("claim nested: %v", err)
	}
	defer cleanup()
	if _, err := os.Stat(nested); err != nil {
		t.Fatalf("nested pid file should exist: %v", err)
	}
}

func TestClaimPIDFile_CleanupSkipsAfterReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "owned.pid")
	cleanup, err := claimPIDFile(path)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Simulate another instance taking over by writing a different pid.
	if err := os.WriteFile(path, []byte("4242\n"), 0o600); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	cleanup()
	// The cleanup should have left the foreign pid file alone.
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.TrimSpace(string(body)) != "4242" {
		t.Fatalf("cleanup removed file owned by another pid: %q", body)
	}
}
