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
// The hash advances on every read, not only on a successful reload. A rejected
// reload -- invalid config, or a change that requires a restart -- is therefore
// reported once rather than on every tick; the operator's next edit changes the
// hash again and is picked up.
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
			data, err := os.ReadFile(path)
			if err != nil {
				// Transient during an atomic replace, and permanent if the file
				// was removed. Either way the next tick retries, so this is a
				// warning rather than a reason to stop polling.
				logger.Warn("poll_config_failed", slog.Any("err", err))
				continue
			}
			sum := hashConfig(data)
			if sum == last {
				continue
			}
			last = sum
			reload()
		}
	}
}

func hashConfig(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
