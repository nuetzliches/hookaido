package secrets_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/secrets"
	"github.com/nuetzliches/hookaido/modules/sqlite"
)

func init() {
	extraSecretStoreFactories = append(extraSecretStoreFactories, secretStoreFactory{
		name: "sqlite",
		new: func(t *testing.T) secrets.Store {
			t.Helper()
			dbPath := filepath.Join(t.TempDir(), "secrets.db")
			s, err := sqlite.NewStore(
				dbPath,
				sqlite.WithPollInterval(5*time.Millisecond),
				sqlite.WithCheckpointInterval(0),
			)
			if err != nil {
				t.Fatalf("new sqlite store: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			return s
		},
	})
}
