package mcp

import (
	"strings"
	"testing"

	"github.com/nuetzliches/hookaido/v2/internal/queue"
)

// closableMemoryStore adapts the memory store to the closableStore the tools
// use. The memory backend enqueues a batch all-or-nothing, exactly as the
// sqlite backend does in a transaction.
type closableMemoryStore struct {
	*queue.MemoryStore
}

func (closableMemoryStore) Close() error { return nil }

func preparedItems(ids ...string) []queue.Envelope {
	out := make([]queue.Envelope, 0, len(ids))
	for _, id := range ids {
		out = append(out, queue.Envelope{ID: id, Route: "/r", Target: "pull", Payload: []byte("{}")})
	}
	return out
}

func queuedCount(t *testing.T, store queue.Store) int {
	t.Helper()
	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	return stats.ByState[queue.StateQueued]
}

// The direct-sqlite publish path enqueued item by item, so a batch that hit
// ErrQueueFull halfway left items [0,k) queued while the tool returned only an
// error -- and retrying the same batch then failed at item 0 with "already
// exists", leaving the operation permanently half-applied. The admin-proxy
// variant of the same tool treats atomicity as the contract.
func TestPublishAllOrNothing_MidBatchFailureCommitsNothing(t *testing.T) {
	memory := queue.NewMemoryStore(queue.WithQueueLimits(2, "reject"))
	store := closableMemoryStore{MemoryStore: memory}

	if _, err := publishAllOrNothing(store, preparedItems("a", "b", "c")); err == nil {
		t.Fatal("expected the over-capacity batch to fail")
	}
	if got := queuedCount(t, memory); got != 0 {
		t.Fatalf("queued=%d, want 0: the failed batch left messages behind", got)
	}

	// The same batch is retryable, which the half-applied version was not.
	if _, err := publishAllOrNothing(store, preparedItems("a", "b")); err != nil {
		t.Fatalf("retry of a smaller batch: %v", err)
	}
	if got := queuedCount(t, memory); got != 2 {
		t.Fatalf("queued=%d, want 2", got)
	}
}

func TestPublishAllOrNothing_Success(t *testing.T) {
	memory := queue.NewMemoryStore()
	store := closableMemoryStore{MemoryStore: memory}

	n, err := publishAllOrNothing(store, preparedItems("a", "b", "c"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if n != 3 {
		t.Fatalf("published=%d, want 3", n)
	}
	if got := queuedCount(t, memory); got != 3 {
		t.Fatalf("queued=%d, want 3", got)
	}
}

// A backend without atomic batch enqueue cannot be made atomic here, so the
// error has to say how far it got rather than pretend nothing happened.
func TestPublishAllOrNothing_NonBatchBackendReportsPartialState(t *testing.T) {
	memory := queue.NewMemoryStore(queue.WithQueueLimits(2, "reject"))
	store := closableMemoryStore{MemoryStore: memory}

	// publishSequentially is what a backend without BatchEnqueuer would take.
	_, err := publishSequentially(store, preparedItems("a", "b", "c"))
	if err == nil {
		t.Fatal("expected the over-capacity batch to fail")
	}
	if got := err.Error(); !strings.Contains(got, "already published") {
		t.Fatalf("error %q does not report the partial state", got)
	}
}
