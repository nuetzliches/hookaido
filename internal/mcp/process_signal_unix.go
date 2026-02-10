//go:build !windows

package mcp

import (
	"errors"
	"syscall"
)

func processExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	return errors.Is(err, syscall.EPERM)
}

func sendSignal(pid int, sig syscall.Signal) error {
	return syscall.Kill(pid, sig)
}
