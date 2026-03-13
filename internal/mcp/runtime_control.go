package mcp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func runtimeControlAuditMetadata(name string, out any, args map[string]any) map[string]any {
	switch strings.TrimSpace(name) {
	case "instance_start", "instance_stop", "instance_reload":
	default:
		return nil
	}

	meta := map[string]any{
		"operation": strings.TrimSpace(name),
	}
	if args != nil {
		if raw, ok := stringFromAny(args["pid_file"]); ok && raw != "" {
			meta["pid_file"] = raw
		}
		if raw, ok := stringFromAny(args["timeout"]); ok && raw != "" {
			meta["timeout"] = raw
		}
		if v, ok := boolFromAny(args["force"]); ok {
			meta["force"] = v
		}
	}
	if outMap, ok := out.(map[string]any); ok {
		if raw, ok := stringFromAny(outMap["pid_file"]); ok && raw != "" {
			meta["pid_file"] = raw
		}
		if raw, ok := stringFromAny(outMap["timeout"]); ok && raw != "" {
			meta["timeout"] = raw
		}
		for _, key := range []string{
			"ok",
			"started",
			"already_running",
			"stopped",
			"already_stopped",
			"reloaded",
			"signaled",
			"force",
			"forced",
		} {
			if v, ok := boolFromAny(outMap[key]); ok {
				meta[key] = v
			}
		}
		if raw := outMap["pid"]; raw != nil {
			if pid, ok := intFromAnyForError(raw); ok && pid > 0 {
				meta["pid"] = pid
			}
		}
	}
	return meta
}

func (s *Server) toolInstanceStart(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, instanceStartAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	if err := s.validateRuntimeControlSetup(); err != nil {
		return nil, err
	}
	pidFile, err := s.resolvePIDFilePath(args)
	if err != nil {
		return nil, err
	}
	timeout, err := parseDurationArg(args, "timeout", defaultStartStopTimeout)
	if err != nil {
		return nil, err
	}

	compiled, res, err := s.loadCompiledConfig()
	if err != nil {
		return nil, err
	}
	if !res.OK {
		return map[string]any{
			"ok":       false,
			"started":  false,
			"errors":   res.Errors,
			"warnings": res.Warnings,
		}, nil
	}

	if pid, running := readRunningPID(pidFile); running {
		return map[string]any{
			"ok":              true,
			"started":         false,
			"already_running": true,
			"pid":             pid,
			"pid_file":        pidFile,
		}, nil
	}
	_ = os.Remove(pidFile)

	cmdArgs := []string{
		"run",
		"--config", s.ConfigPath,
		"--db", s.DBPath,
		"--pid-file", pidFile,
	}
	if level := strings.TrimSpace(s.RunLogLevel); level != "" {
		cmdArgs = append(cmdArgs, "--log-level", level)
	}
	if strings.TrimSpace(s.RunDotenvPath) != "" {
		cmdArgs = append(cmdArgs, "--dotenv", s.RunDotenvPath)
	}
	if s.RunWatch {
		cmdArgs = append(cmdArgs, "--watch")
	}

	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = devNull.Close() }()

	cmd := exec.Command(s.RunBinaryPath, cmdArgs...)
	cmd.Stdout = devNull
	cmd.Stderr = devNull
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go func() {
		_ = cmd.Wait()
	}()

	pid, err := waitForPIDFile(pidFile, timeout)
	if err != nil {
		_ = signalPID(cmd.Process.Pid, syscall.SIGTERM)
		return map[string]any{
			"ok":          false,
			"started":     false,
			"pid_file":    pidFile,
			"errors":      []string{fmt.Sprintf("start failed: %v", err)},
			"process_pid": cmd.Process.Pid,
		}, nil
	}

	healthURL, healthErr := waitForAdminHealth(compiled, timeout)
	if healthErr != nil {
		_, _, _ = stopPID(pid, 2*time.Second, true)
		_ = removePIDFileIfMatches(pidFile, pid)
		return map[string]any{
			"ok":         false,
			"started":    false,
			"pid":        pid,
			"pid_file":   pidFile,
			"health_url": healthURL,
			"errors":     []string{healthErr.Error()},
			"warnings":   res.Warnings,
		}, nil
	}

	return map[string]any{
		"ok":         true,
		"started":    true,
		"pid":        pid,
		"pid_file":   pidFile,
		"health_url": healthURL,
		"warnings":   res.Warnings,
	}, nil
}

func (s *Server) toolInstanceStatus(args map[string]any) (any, error) {
	if err := s.validateRuntimeControlSetup(); err != nil {
		return nil, err
	}
	pidFile, err := s.resolvePIDFilePath(args)
	if err != nil {
		return nil, err
	}
	timeout, err := parseDurationArg(args, "timeout", 2*time.Second)
	if err != nil {
		return nil, err
	}

	out := map[string]any{
		"pid_file": pidFile,
		"running":  false,
		"pid":      0,
	}

	pid, running := readRunningPID(pidFile)
	if running {
		out["running"] = true
		out["pid"] = pid
	}

	compiled, res, loadErr := s.loadCompiledConfig()
	if loadErr != nil {
		out["ok"] = false
		out["config_ok"] = false
		out["errors"] = []string{loadErr.Error()}
		return out, nil
	}
	out["config_ok"] = res.OK
	out["warnings"] = res.Warnings
	if !res.OK {
		out["ok"] = false
		out["errors"] = res.Errors
		return out, nil
	}

	pullRouteCount := 0
	deliverRouteCount := 0
	managedRouteCount := 0
	for _, r := range compiled.Routes {
		if r.Pull != nil {
			pullRouteCount++
		}
		if len(r.Deliveries) > 0 {
			deliverRouteCount++
		}
		if strings.TrimSpace(r.Application) != "" && strings.TrimSpace(r.EndpointName) != "" {
			managedRouteCount++
		}
	}
	out["summary"] = map[string]any{
		"ingress_listen":                      compiled.Ingress.Listen,
		"pull_listen":                         compiled.PullAPI.Listen,
		"admin_listen":                        compiled.AdminAPI.Listen,
		"queue_backend":                       compiledQueueBackend(compiled),
		"publish_policy_direct_enabled":       compiled.Defaults.PublishPolicy.DirectEnabled,
		"publish_policy_managed_enabled":      compiled.Defaults.PublishPolicy.ManagedEnabled,
		"publish_policy_allow_pull_routes":    compiled.Defaults.PublishPolicy.AllowPullRoutes,
		"publish_policy_allow_deliver_routes": compiled.Defaults.PublishPolicy.AllowDeliverRoutes,
		"publish_policy_require_actor":        compiled.Defaults.PublishPolicy.RequireActor,
		"publish_policy_require_request_id":   compiled.Defaults.PublishPolicy.RequireRequestID,
		"publish_policy_fail_closed":          compiled.Defaults.PublishPolicy.FailClosed,
		"publish_policy_actor_allowlist": append(
			[]string(nil),
			compiled.Defaults.PublishPolicy.ActorAllowlist...,
		),
		"publish_policy_actor_prefixes": append(
			[]string(nil),
			compiled.Defaults.PublishPolicy.ActorPrefixes...,
		),
		"shared_listener":     compiled.SharedListener,
		"route_count":         len(compiled.Routes),
		"pull_route_count":    pullRouteCount,
		"deliver_route_count": deliverRouteCount,
		"managed_route_count": managedRouteCount,
	}

	health := map[string]any{
		"ok":      false,
		"url":     "",
		"error":   "",
		"checked": false,
	}
	if running {
		url, herr := waitForAdminHealth(compiled, timeout)
		health["url"] = url
		health["checked"] = true
		if herr != nil {
			health["error"] = herr.Error()
		} else {
			health["ok"] = true
		}
	}
	out["admin_health"] = health

	healthy, _ := health["ok"].(bool)
	out["ok"] = res.OK && (!running || healthy)
	if !out["ok"].(bool) {
		if herr, _ := health["error"].(string); strings.TrimSpace(herr) != "" {
			out["errors"] = []string{herr}
		} else {
			out["errors"] = []string{}
		}
	} else {
		out["errors"] = []string{}
	}
	return out, nil
}

func (s *Server) toolInstanceLogsTail(args map[string]any) (any, error) {
	if err := s.validateRuntimeControlSetup(); err != nil {
		return nil, err
	}
	pidFile, err := s.resolvePIDFilePath(args)
	if err != nil {
		return nil, err
	}
	maxLines, err := parseIntArg(args, "max_lines", 200, 1, 1000)
	if err != nil {
		return nil, err
	}
	maxBytes, err := parseIntArg(args, "max_bytes", 64*1024, 1024, 1024*1024)
	if err != nil {
		return nil, err
	}

	compiled, res, loadErr := s.loadCompiledConfig()
	if loadErr != nil {
		return map[string]any{
			"ok":       false,
			"pid_file": pidFile,
			"errors":   []string{loadErr.Error()},
		}, nil
	}
	if !res.OK {
		return map[string]any{
			"ok":       false,
			"pid_file": pidFile,
			"errors":   res.Errors,
			"warnings": res.Warnings,
		}, nil
	}
	if compiled.Observability.RuntimeLogDisabled {
		return map[string]any{
			"ok":       false,
			"pid_file": pidFile,
			"errors":   []string{"runtime_log is disabled"},
			"warnings": res.Warnings,
		}, nil
	}
	if compiled.Observability.RuntimeLogOutput != "file" || strings.TrimSpace(compiled.Observability.RuntimeLogPath) == "" {
		return map[string]any{
			"ok":       false,
			"pid_file": pidFile,
			"errors":   []string{"runtime_log output is not configured as file"},
			"warnings": res.Warnings,
		}, nil
	}

	lines, info, err := tailFile(compiled.Observability.RuntimeLogPath, maxBytes, maxLines)
	if err != nil {
		return map[string]any{
			"ok":       false,
			"pid_file": pidFile,
			"path":     compiled.Observability.RuntimeLogPath,
			"errors":   []string{err.Error()},
			"warnings": res.Warnings,
		}, nil
	}

	pid, running := readRunningPID(pidFile)
	return map[string]any{
		"ok":         true,
		"pid_file":   pidFile,
		"pid":        pid,
		"running":    running,
		"path":       compiled.Observability.RuntimeLogPath,
		"max_lines":  maxLines,
		"max_bytes":  maxBytes,
		"line_count": len(lines),
		"truncated":  info.Truncated,
		"total_size": info.TotalSize,
		"lines":      lines,
		"errors":     []string{},
		"warnings":   res.Warnings,
	}, nil
}

func (s *Server) toolInstanceStop(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, instanceStopAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	if err := s.validateRuntimeControlSetup(); err != nil {
		return nil, err
	}
	pidFile, err := s.resolvePIDFilePath(args)
	if err != nil {
		return nil, err
	}
	timeout, err := parseDurationArg(args, "timeout", defaultStartStopTimeout)
	if err != nil {
		return nil, err
	}
	force, err := parseBool(args, "force")
	if err != nil {
		return nil, err
	}

	pid, err := readPIDFileValue(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{
				"ok":              true,
				"stopped":         false,
				"already_stopped": true,
				"pid_file":        pidFile,
			}, nil
		}
		return nil, err
	}

	if !isPIDRunning(pid) {
		_ = removePIDFileIfMatches(pidFile, pid)
		return map[string]any{
			"ok":              true,
			"stopped":         false,
			"already_stopped": true,
			"pid":             pid,
			"pid_file":        pidFile,
		}, nil
	}

	stopped, forced, err := stopPID(pid, timeout, force)
	if err != nil {
		return nil, err
	}
	if stopped {
		_ = removePIDFileIfMatches(pidFile, pid)
	}

	out := map[string]any{
		"ok":       stopped,
		"stopped":  stopped,
		"forced":   forced,
		"pid":      pid,
		"pid_file": pidFile,
		"timeout":  timeout.String(),
		"errors":   []string{},
	}
	if !stopped {
		out["errors"] = []string{fmt.Sprintf("process %d did not stop within %s", pid, timeout)}
	}
	return out, nil
}

func (s *Server) toolInstanceReload(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, instanceReloadAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	if err := s.validateRuntimeControlSetup(); err != nil {
		return nil, err
	}
	pidFile, err := s.resolvePIDFilePath(args)
	if err != nil {
		return nil, err
	}
	timeout, err := parseDurationArg(args, "timeout", defaultReloadCheckTimeout)
	if err != nil {
		return nil, err
	}

	compiled, res, err := s.loadCompiledConfig()
	if err != nil {
		return nil, err
	}
	if !res.OK {
		return map[string]any{
			"ok":       false,
			"reloaded": false,
			"signaled": false,
			"errors":   res.Errors,
			"warnings": res.Warnings,
		}, nil
	}

	pid, err := readPIDFileValue(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{
				"ok":       false,
				"reloaded": false,
				"signaled": false,
				"pid_file": pidFile,
				"errors":   []string{"pid file does not exist"},
				"warnings": res.Warnings,
			}, nil
		}
		return nil, err
	}
	if !isPIDRunning(pid) {
		return map[string]any{
			"ok":       false,
			"reloaded": false,
			"signaled": false,
			"pid":      pid,
			"pid_file": pidFile,
			"errors":   []string{fmt.Sprintf("process %d is not running", pid)},
			"warnings": res.Warnings,
		}, nil
	}

	if err := signalPID(pid, syscall.SIGHUP); err != nil {
		return nil, err
	}

	healthURL, healthErr := waitForAdminHealth(compiled, timeout)
	out := map[string]any{
		"ok":         healthErr == nil,
		"reloaded":   healthErr == nil,
		"signaled":   true,
		"pid":        pid,
		"pid_file":   pidFile,
		"timeout":    timeout.String(),
		"health_url": healthURL,
		"errors":     []string{},
		"warnings":   res.Warnings,
	}
	if healthErr != nil {
		out["errors"] = []string{healthErr.Error()}
	}
	return out, nil
}

func (s *Server) validateRuntimeControlSetup() error {
	if strings.TrimSpace(s.ConfigPath) == "" {
		return errors.New("config path is not configured")
	}
	if strings.TrimSpace(s.DBPath) == "" {
		return errors.New("db path is not configured")
	}
	if strings.TrimSpace(s.PIDFilePath) == "" {
		return errors.New("pid file path is not configured")
	}
	if strings.TrimSpace(s.RunBinaryPath) == "" {
		return errors.New("run binary path is not configured")
	}
	return nil
}

func (s *Server) resolvePIDFilePath(args map[string]any) (string, error) {
	p := strings.TrimSpace(s.PIDFilePath)
	if p == "" {
		return "", errors.New("pid file path is not configured")
	}
	if raw, ok := args["pid_file"]; ok {
		argPath, ok := raw.(string)
		if !ok {
			return "", errors.New("pid_file must be a string")
		}
		argPath = strings.TrimSpace(argPath)
		if argPath != "" {
			if argPath != p {
				return "", fmt.Errorf("pid_file %q is not allowed", argPath)
			}
			p = argPath
		}
	}
	return p, nil
}

func readPIDFileValue(pidFile string) (int, error) {
	data, err := os.ReadFile(strings.TrimSpace(pidFile))
	if err != nil {
		return 0, err
	}
	raw := strings.TrimSpace(string(data))
	if raw == "" {
		return 0, fmt.Errorf("pid file %q is empty", pidFile)
	}
	pid, err := strconv.Atoi(raw)
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("pid file %q contains invalid pid %q", pidFile, raw)
	}
	return pid, nil
}

func readRunningPID(pidFile string) (int, bool) {
	pid, err := readPIDFileValue(pidFile)
	if err != nil {
		return 0, false
	}
	return pid, isPIDRunning(pid)
}

func isPIDRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	if isZombiePID(pid) {
		return false
	}
	return processExists(pid)
}

func isZombiePID(pid int) bool {
	statPath := fmt.Sprintf("/proc/%d/stat", pid)
	data, err := os.ReadFile(statPath)
	if err != nil {
		return false
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return false
	}
	return fields[2] == "Z"
}

func signalPID(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return fmt.Errorf("invalid pid %d", pid)
	}
	if err := sendSignal(pid, sig); err != nil {
		return fmt.Errorf("signal %s pid %d: %w", sig, pid, err)
	}
	return nil
}

func stopPID(pid int, timeout time.Duration, force bool) (stopped bool, forced bool, err error) {
	if !isPIDRunning(pid) {
		return true, false, nil
	}
	if err := signalPID(pid, syscall.SIGTERM); err != nil {
		return false, false, err
	}
	if waitForPIDExit(pid, timeout) {
		return true, false, nil
	}
	if !force {
		return false, false, nil
	}
	if err := signalPID(pid, syscall.SIGKILL); err != nil {
		return false, false, err
	}
	return waitForPIDExit(pid, 2*time.Second), true, nil
}

func waitForPIDExit(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if !isPIDRunning(pid) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForPIDFile(pidFile string, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		pid, err := readPIDFileValue(pidFile)
		if err == nil {
			return pid, nil
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("timeout waiting for pid file %q", pidFile)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func removePIDFileIfMatches(pidFile string, pid int) error {
	current, err := readPIDFileValue(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if current != pid {
		return nil
	}
	if err := os.Remove(pidFile); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

type tailInfo struct {
	TotalSize int64
	Truncated bool
}

func tailFile(path string, maxBytes int, maxLines int) ([]string, tailInfo, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, tailInfo{}, errors.New("empty log file path")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, tailInfo{}, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, tailInfo{}, err
	}
	if info.IsDir() {
		return nil, tailInfo{}, fmt.Errorf("log path %q is a directory", path)
	}

	size := info.Size()
	start := int64(0)
	truncated := false
	if size > int64(maxBytes) {
		start = size - int64(maxBytes)
		truncated = true
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return nil, tailInfo{}, err
	}
	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, tailInfo{}, err
	}

	if start > 0 {
		if idx := strings.IndexByte(string(buf), '\n'); idx >= 0 && idx+1 < len(buf) {
			buf = buf[idx+1:]
		}
	}

	raw := strings.ReplaceAll(string(buf), "\r\n", "\n")
	raw = strings.TrimRight(raw, "\n")
	if strings.TrimSpace(raw) == "" {
		return []string{}, tailInfo{TotalSize: size, Truncated: truncated}, nil
	}
	lines := strings.Split(raw, "\n")
	if maxLines > 0 && len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
		truncated = true
	}
	return lines, tailInfo{TotalSize: size, Truncated: truncated}, nil
}
