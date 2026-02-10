//go:build windows

package mcp

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/windows"
)

const windowsStillActiveExitCode = 259

func processExists(pid int) bool {
	if pid <= 0 {
		return false
	}

	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return false
	}
	defer windows.CloseHandle(handle)

	var exitCode uint32
	if err := windows.GetExitCodeProcess(handle, &exitCode); err != nil {
		return false
	}
	return exitCode == windowsStillActiveExitCode
}

func sendSignal(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return fmt.Errorf("invalid pid %d", pid)
	}
	if sig == 0 {
		if processExists(pid) {
			return nil
		}
		return syscall.ESRCH
	}
	if sig == syscall.SIGHUP {
		return fmt.Errorf("signal %s is not supported on Windows", sig)
	}

	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Signal(os.Kill)
}
