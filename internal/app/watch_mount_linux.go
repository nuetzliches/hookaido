//go:build linux

package app

import (
	"os"
	"path/filepath"
	"syscall"
)

// configOnSeparateDevice reports whether the config file lives on a different
// filesystem than the directory containing it.
//
// That is the signature of a single-file bind mount -- `-v ./Hookaidofile:/app/
// Hookaidofile` or a Kubernetes `subPath` ConfigMap -- which is exactly the
// layout in which watchConfig can never fire, because the directory the watcher
// watches is the container's own and does not change when the host file is
// replaced. Reporting it at startup turns a silent non-reload into one log line.
//
// Deliberately a heuristic, and deliberately Linux-only: it is the platform
// containers run on, and st_dev is the cheap signal there. A false positive is
// possible in principle and costs one informational line; a false negative
// costs nothing beyond what the operator has today.
func configOnSeparateDevice(path string) (bool, bool) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false, false
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return false, false
	}
	fileStat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return false, false
	}
	dirStat, ok := dirInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return false, false
	}
	return fileStat.Dev != dirStat.Dev, true
}
