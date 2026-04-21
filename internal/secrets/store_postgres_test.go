package secrets_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/secrets"
	"github.com/nuetzliches/hookaido/modules/postgres"
)

func init() {
	dsn := strings.TrimSpace(os.Getenv("HOOKAIDO_TEST_POSTGRES_DSN"))
	if dsn == "" {
		return
	}
	extraSecretStoreFactories = append(extraSecretStoreFactories, secretStoreFactory{
		name: "postgres",
		new: func(t *testing.T) secrets.Store {
			t.Helper()
			s, err := postgres.NewStore(
				dsn,
				postgres.WithPollInterval(5*time.Millisecond),
			)
			if err != nil {
				t.Fatalf("new postgres store: %v", err)
			}
			// Clear any prior test residue so each run starts fresh.
			t.Cleanup(func() { _ = s.Close() })
			if _, err := s.ListAll(); err != nil {
				t.Fatalf("postgres verify: %v", err)
			}
			// Best-effort truncate; ignore errors if table was just created.
			truncateRuntimeSecrets(t, s)
			return s
		},
	})
}

func truncateRuntimeSecrets(t *testing.T, s secrets.Store) {
	t.Helper()
	recs, err := s.ListAll()
	if err != nil {
		return
	}
	for _, r := range recs {
		_, _ = s.Delete(r.PoolName, r.ID)
	}
}
