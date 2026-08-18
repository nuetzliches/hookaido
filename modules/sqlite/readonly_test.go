package sqlite

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/queue"
)

// A read-only tool set must not write to the database it inspects. Opening
// read-write ran BEGIN IMMEDIATE migration transactions against the running
// server's database on every request, so a newer binary's `mcp serve --db`
// migrated an older server's schema forward under it -- and the older server
// then failed its downgrade guard at the next restart.
func TestReadOnlyStore_ReadsButNeverWrites(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")

	owner, err := NewStore(dbPath, WithCheckpointInterval(0))
	if err != nil {
		t.Fatalf("open owner store: %v", err)
	}
	if err := owner.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "pull", Payload: []byte("{}")}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatalf("close owner store: %v", err)
	}

	before, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}

	ro, err := NewStore(dbPath, WithReadOnly(true))
	if err != nil {
		t.Fatalf("open read-only store: %v", err)
	}
	t.Cleanup(func() { _ = ro.Close() })

	// Reads still work: this is what the read-only tool set needs.
	stats, err := ro.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ByState[queue.StateQueued] != 1 {
		t.Fatalf("queued=%d, want 1", stats.ByState[queue.StateQueued])
	}
	if _, err := ro.ListMessages(queue.MessageListRequest{Route: "/r", Limit: 10}); err != nil {
		t.Fatalf("list messages: %v", err)
	}

	// Writes do not.
	err = ro.Enqueue(queue.Envelope{ID: "evt_2", Route: "/r", Target: "pull", Payload: []byte("{}")})
	if err == nil {
		t.Fatal("a read-only store must not accept an enqueue")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "read") {
		t.Logf("enqueue rejected with: %v", err)
	}

	after, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if !after.ModTime().Equal(before.ModTime()) || after.Size() != before.Size() {
		t.Fatalf("the database file changed: size %d->%d, mtime %v->%v",
			before.Size(), after.Size(), before.ModTime(), after.ModTime())
	}
}

// A read-only store cannot create the database it is pointed at: that would
// mean writing, and an empty file would then look like a valid but empty queue.
func TestReadOnlyStore_MissingFileFails(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.db")
	if _, err := NewStore(missing, WithReadOnly(true)); err == nil {
		t.Fatal("expected opening a missing database read-only to fail")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("a read-only open must not create the database file")
	}
}

// The checkpoint loop is a background writer, so it must not run either.
func TestReadOnlyStore_NoCheckpointLoop(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	owner, err := NewStore(dbPath, WithCheckpointInterval(0))
	if err != nil {
		t.Fatalf("open owner store: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatalf("close owner store: %v", err)
	}

	ro, err := NewStore(dbPath, WithReadOnly(true), WithCheckpointInterval(time.Millisecond))
	if err != nil {
		t.Fatalf("open read-only store: %v", err)
	}
	defer func() { _ = ro.Close() }()

	if ro.checkpointStop != nil {
		t.Fatal("the checkpoint loop must not start on a read-only store")
	}
}
