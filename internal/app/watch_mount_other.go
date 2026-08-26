//go:build !linux

package app

// configOnSeparateDevice is Linux-only; see watch_mount_linux.go. Elsewhere the
// check reports "unknown" and the startup hint is skipped.
func configOnSeparateDevice(string) (bool, bool) {
	return false, false
}
