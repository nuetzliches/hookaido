package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"time"
)

// minWatchInterval bounds --watch-interval. A poll re-reads and hashes the
// config file, which is cheap, but a sub-second interval is far more likely to
// be a unit mistake than an intent.
const minWatchInterval = time.Second

// pollSettleDelay is how long a detected change has to hold still before it is
// reported. It mirrors watchConfig's 200 ms debounce, and for the same reason:
// a writer that truncates and then writes -- which is what a plain overwrite
// does -- is briefly observable as empty or partial content. Without the
// settle, one such write showed up as two changes: a rejected reload of a
// half-written config followed by the real one, and rewriting a config with
// identical bytes showed up as a change at all.
const pollSettleDelay = 200 * time.Millisecond

// pollConfig re-reads the config path on a fixed interval and reloads when its
// content hash changes.
//
// It exists because watchConfig cannot fire at all in the most common container
// deployment shape. watchConfig watches the parent directory and filters by
// basename, which is the right pattern for editors and atomic replace-by-rename
// -- but with the config mounted as a single file, the directory inside the
// container is not the host directory. It holds one bind-mounted entry, and
// replacing the file on the host creates a new inode that the existing mount
// does not resolve to. The container's directory genuinely did not change, so
// there is no event to receive. Kubernetes `subPath` ConfigMap mounts have the
// same property and are documented not to receive updates.
//
// The failure was silent: `watching_config` was logged, no reload happened, and
// a new route stayed 404 while the operator believed the config was live.
//
// The hash advances on every settled read, not only on a successful reload. A
// rejected reload -- invalid config, or a change that requires a restart -- is
// therefore reported once rather than on every tick; the operator's next edit
// changes the hash again and is picked up.
func pollConfig(ctx context.Context, path string, interval time.Duration, initial []byte, logger *slog.Logger, reload func()) {
	if logger == nil {
		logger = slog.Default()
	}
	if reload == nil || interval <= 0 {
		return
	}

	last := hashConfig(initial)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("polling_config", slog.String("path", path), slog.String("interval", interval.String()))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sum, ok := readSettledConfigHash(ctx, path, logger)
			if !ok || sum == last {
				continue
			}
			last = sum
			reload()
		}
	}
}

// readSettledConfigHash hashes the config file, and on a first read it re-reads
// after pollSettleDelay to confirm the content is not still being written. The
// caller only ever sees a hash that held still.
func readSettledConfigHash(ctx context.Context, path string, logger *slog.Logger) (string, bool) {
	first, err := os.ReadFile(path)
	if err != nil {
		// Transient during a replace, and permanent if the file was removed.
		// Either way the next tick retries, so this is a warning rather than a
		// reason to stop polling.
		logger.Warn("poll_config_failed", slog.Any("err", err))
		return "", false
	}
	// A plain overwrite truncates before writing, so a read can land on a
	// zero-byte file that nobody meant as a config. The settle re-read makes
	// that unlikely rather than impossible: with enough rewrites, both reads
	// eventually land in a truncation window, and an empty file that "held
	// still" was then reported as a settled change. The parser rejects it as
	// `empty config`, so the only trace was a spurious config_reload_failed
	// against a config the operator never wrote.
	//
	// Waiting for content is the honest reading of an empty file here: it is an
	// artifact of someone else's write, and a genuinely emptied Hookaidofile
	// could not be loaded either -- the running config stays in place in both
	// cases, so nothing actionable is lost by staying quiet until there is
	// something to parse.
	if len(first) == 0 {
		return "", false
	}

	timer := time.NewTimer(pollSettleDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return "", false
	case <-timer.C:
	}

	second, err := os.ReadFile(path)
	if err != nil {
		logger.Warn("poll_config_failed", slog.Any("err", err))
		return "", false
	}

	firstSum := hashConfig(first)
	secondSum := hashConfig(second)
	if firstSum != secondSum {
		// Still being written. Leave it for the next tick rather than reloading
		// a half-written config and reporting the failure.
		return "", false
	}
	return secondSum, true
}

func hashConfig(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
