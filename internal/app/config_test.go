package app

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// captureOutput runs fn while capturing stdout and stderr.
func captureOutput(t *testing.T, fn func()) (stdout, stderr string) {
	t.Helper()

	origOut := os.Stdout
	origErr := os.Stderr
	defer func() {
		os.Stdout = origOut
		os.Stderr = origErr
	}()

	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	os.Stdout = wOut
	os.Stderr = wErr

	fn()

	wOut.Close()
	wErr.Close()

	outBuf := make([]byte, 64*1024)
	n, _ := rOut.Read(outBuf)
	stdout = string(outBuf[:n])

	errBuf := make([]byte, 64*1024)
	n, _ = rErr.Read(errBuf)
	stderr = string(errBuf[:n])
	return
}

func writeConfig(t *testing.T, dir, content string) string {
	t.Helper()
	p := filepath.Join(dir, "Hookaidofile")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

const validConfig = `
ingress {
    listen :8080
}

pull_api {
    auth token "test-secret"
}

/hooks {
    pull {
        path /webhooks
    }
}
`

func TestConfigValidate_ValidJSON(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeConfig(t, dir, validConfig)

	var code int
	stdout, stderr := captureOutput(t, func() {
		code = configValidate([]string{"-config", cfgPath})
	})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d; stderr: %s", code, stderr)
	}

	var res struct {
		OK       bool     `json:"ok"`
		Errors   []string `json:"errors"`
		Warnings []string `json:"warnings"`
	}
	if err := json.Unmarshal([]byte(stdout), &res); err != nil {
		t.Fatalf("stdout is not valid JSON: %s\nraw: %s", err, stdout)
	}
	if !res.OK {
		t.Fatalf("expected ok=true, got %+v", res)
	}
}

func TestConfigValidate_ValidText(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeConfig(t, dir, validConfig)

	var code int
	stdout, _ := captureOutput(t, func() {
		code = configValidate([]string{"-config", cfgPath, "-format", "text"})
	})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d", code)
	}
	if !strings.Contains(stdout, "config ok") {
		t.Fatalf("expected 'config ok' in stdout, got: %s", stdout)
	}
}

func TestConfigValidate_ParseErrorJSON(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeConfig(t, dir, "this is not valid DSL !!!")

	var code int
	_, stderr := captureOutput(t, func() {
		code = configValidate([]string{"-config", cfgPath, "-format", "json"})
	})

	if code != 1 {
		t.Fatalf("expected exit 1, got %d", code)
	}

	var res struct {
		OK     bool     `json:"ok"`
		Errors []string `json:"errors"`
	}
	if err := json.Unmarshal([]byte(stderr), &res); err != nil {
		t.Fatalf("stderr is not valid JSON: %s\nraw: %s", err, stderr)
	}
	if res.OK {
		t.Fatal("expected ok=false for parse error")
	}
	if len(res.Errors) == 0 {
		t.Fatal("expected at least one error")
	}
}

func TestConfigValidate_ParseErrorText(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeConfig(t, dir, "this is not valid DSL !!!")

	var code int
	_, stderr := captureOutput(t, func() {
		code = configValidate([]string{"-config", cfgPath, "-format", "text"})
	})

	if code != 1 {
		t.Fatalf("expected exit 1, got %d", code)
	}
	if !strings.Contains(stderr, "config invalid") {
		t.Fatalf("expected 'config invalid' in stderr, got: %s", stderr)
	}
}

func TestConfigValidate_MissingFileJSON(t *testing.T) {
	var code int
	_, stderr := captureOutput(t, func() {
		code = configValidate([]string{"-config", "/nonexistent/path/Hookaidofile", "-format", "json"})
	})

	if code != 1 {
		t.Fatalf("expected exit 1, got %d", code)
	}

	var res struct {
		OK     bool     `json:"ok"`
		Errors []string `json:"errors"`
	}
	if err := json.Unmarshal([]byte(stderr), &res); err != nil {
		t.Fatalf("stderr is not valid JSON: %s\nraw: %s", err, stderr)
	}
	if res.OK {
		t.Fatal("expected ok=false for missing file")
	}
	if len(res.Errors) == 0 {
		t.Fatal("expected at least one error")
	}
}

func TestConfigValidate_MissingFileText(t *testing.T) {
	var code int
	_, stderr := captureOutput(t, func() {
		code = configValidate([]string{"-config", "/nonexistent/path/Hookaidofile", "-format", "text"})
	})

	if code != 1 {
		t.Fatalf("expected exit 1, got %d", code)
	}
	if !strings.Contains(stderr, "config invalid") {
		t.Fatalf("expected 'config invalid' in stderr, got: %s", stderr)
	}
}

func TestConfigValidate_CompileErrorJSON(t *testing.T) {
	dir := t.TempDir()
	// Valid parse but fails compile: route without pull or deliver
	cfgPath := writeConfig(t, dir, `
ingress {
    listen :8080
}

/hooks {
}
`)

	var code int
	_, stderr := captureOutput(t, func() {
		code = configValidate([]string{"-config", cfgPath, "-format", "json"})
	})

	if code != 1 {
		t.Fatalf("expected exit 1, got %d", code)
	}

	var res struct {
		OK     bool     `json:"ok"`
		Errors []string `json:"errors"`
	}
	if err := json.Unmarshal([]byte(stderr), &res); err != nil {
		t.Fatalf("stderr is not valid JSON: %s\nraw: %s", err, stderr)
	}
	if res.OK {
		t.Fatal("expected ok=false for compile error")
	}
	if len(res.Errors) == 0 {
		t.Fatal("expected at least one compile error")
	}
}

const diffConfigOld = `
ingress {
    listen :8080
}

pull_api {
    auth token "test-secret"
}

/hooks {
    pull {
        path /webhooks
    }
}
`

const diffConfigNew = `
ingress {
    listen :9090
}

pull_api {
    auth token "test-secret"
}

/hooks {
    pull {
        path /webhooks
    }
}
`

func writeTempFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestConfigDiff_Identical(t *testing.T) {
	dir := t.TempDir()
	a := writeTempFile(t, dir, "a.hcl", diffConfigOld)
	b := writeTempFile(t, dir, "b.hcl", diffConfigOld)

	var code int
	stdout, stderr := captureOutput(t, func() {
		code = configDiff([]string{a, b})
	})

	if code != 0 {
		t.Fatalf("expected exit 0 (identical), got %d; stderr: %s", code, stderr)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("expected empty stdout for identical configs, got: %s", stdout)
	}
}

func TestConfigDiff_Changed(t *testing.T) {
	dir := t.TempDir()
	a := writeTempFile(t, dir, "a.hcl", diffConfigOld)
	b := writeTempFile(t, dir, "b.hcl", diffConfigNew)

	var code int
	stdout, stderr := captureOutput(t, func() {
		code = configDiff([]string{a, b})
	})

	if code != 1 {
		t.Fatalf("expected exit 1 (changed), got %d; stderr: %s", code, stderr)
	}
	if !strings.Contains(stdout, "---") || !strings.Contains(stdout, "+++") {
		t.Fatalf("expected unified diff header, got: %s", stdout)
	}
	if !strings.Contains(stdout, "-") && !strings.Contains(stdout, "+") {
		t.Fatalf("expected diff lines, got: %s", stdout)
	}
	if !strings.Contains(stdout, "8080") || !strings.Contains(stdout, "9090") {
		t.Fatalf("expected port change in diff, got: %s", stdout)
	}
}

func TestConfigDiff_CustomContext(t *testing.T) {
	dir := t.TempDir()
	a := writeTempFile(t, dir, "a.hcl", diffConfigOld)
	b := writeTempFile(t, dir, "b.hcl", diffConfigNew)

	var code int
	stdout, _ := captureOutput(t, func() {
		code = configDiff([]string{"--context", "1", a, b})
	})

	if code != 1 {
		t.Fatalf("expected exit 1 (changed), got %d", code)
	}
	// With context=1, fewer context lines around changes
	if !strings.Contains(stdout, "@@") {
		t.Fatalf("expected hunk header, got: %s", stdout)
	}
}

func TestConfigDiff_ParseError(t *testing.T) {
	dir := t.TempDir()
	a := writeTempFile(t, dir, "a.hcl", "not valid DSL !!!")
	b := writeTempFile(t, dir, "b.hcl", diffConfigNew)

	var code int
	_, stderr := captureOutput(t, func() {
		code = configDiff([]string{a, b})
	})

	if code != 2 {
		t.Fatalf("expected exit 2 (error), got %d", code)
	}
	if !strings.Contains(stderr, "parsing") {
		t.Fatalf("expected parse error in stderr, got: %s", stderr)
	}
}

func TestConfigDiff_MissingFile(t *testing.T) {
	dir := t.TempDir()
	b := writeTempFile(t, dir, "b.hcl", diffConfigNew)

	var code int
	_, stderr := captureOutput(t, func() {
		code = configDiff([]string{"/nonexistent/a.hcl", b})
	})

	if code != 2 {
		t.Fatalf("expected exit 2 (error), got %d", code)
	}
	if stderr == "" {
		t.Fatal("expected error message on stderr")
	}
}

func TestConfigDiff_WrongArgCount(t *testing.T) {
	var code int
	_, stderr := captureOutput(t, func() {
		code = configDiff([]string{"only-one-arg"})
	})

	if code != 2 {
		t.Fatalf("expected exit 2 (usage error), got %d", code)
	}
	if !strings.Contains(stderr, "usage:") {
		t.Fatalf("expected usage message, got: %s", stderr)
	}
}
