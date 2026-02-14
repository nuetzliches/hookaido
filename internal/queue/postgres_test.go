package queue

import (
	"strings"
	"testing"
	"time"
)

func TestNewPostgresStore_EmptyDSN(t *testing.T) {
	_, err := NewPostgresStore("   ")
	if err == nil {
		t.Fatalf("expected error for empty dsn")
	}
	if !strings.Contains(err.Error(), "empty postgres dsn") {
		t.Fatalf("error = %v, want contains %q", err, "empty postgres dsn")
	}
}

func TestPostgresStore_RuntimeMetrics_OperationAndErrorCounters(t *testing.T) {
	store := &PostgresStore{metrics: newPostgresRuntimeMetrics()}

	store.observeStoreOperation("enqueue", time.Now().Add(-10*time.Millisecond), nil)
	store.observeStoreOperation("enqueue", time.Now().Add(-5*time.Millisecond), ErrQueueFull)
	store.observeStoreOperation("dequeue", time.Now().Add(-3*time.Millisecond), ErrLeaseNotFound)

	runtime := store.RuntimeMetrics()
	if runtime.Backend != "postgres" {
		t.Fatalf("backend = %q, want postgres", runtime.Backend)
	}

	durationByOperation := make(map[string]HistogramSnapshot)
	for _, metric := range runtime.Common.OperationDurationSeconds {
		durationByOperation[metric.Operation] = metric.DurationSeconds
	}
	if got := durationByOperation["enqueue"].Count; got != 2 {
		t.Fatalf("enqueue duration count = %d, want 2", got)
	}
	if got := durationByOperation["dequeue"].Count; got != 1 {
		t.Fatalf("dequeue duration count = %d, want 1", got)
	}

	totalByOperation := make(map[string]int64)
	for _, metric := range runtime.Common.OperationTotal {
		totalByOperation[metric.Operation] = metric.Total
	}
	if got := totalByOperation["enqueue"]; got != 2 {
		t.Fatalf("enqueue total = %d, want 2", got)
	}
	if got := totalByOperation["dequeue"]; got != 1 {
		t.Fatalf("dequeue total = %d, want 1", got)
	}

	errorsByOperationKind := make(map[string]int64)
	for _, metric := range runtime.Common.ErrorsTotal {
		key := metric.Operation + "|" + metric.Kind
		errorsByOperationKind[key] = metric.Total
	}
	if got := errorsByOperationKind["enqueue|queue_full"]; got != 1 {
		t.Fatalf("enqueue queue_full errors = %d, want 1", got)
	}
	if got := errorsByOperationKind["dequeue|lease_not_found"]; got != 1 {
		t.Fatalf("dequeue lease_not_found errors = %d, want 1", got)
	}
}

func TestPostgresStore_RuntimeMetrics_NilStore(t *testing.T) {
	var store *PostgresStore

	runtime := store.RuntimeMetrics()
	if runtime.Backend != "postgres" {
		t.Fatalf("backend = %q, want postgres", runtime.Backend)
	}
	if len(runtime.Common.OperationDurationSeconds) != 0 {
		t.Fatalf("operation durations should be empty, got %d", len(runtime.Common.OperationDurationSeconds))
	}
	if len(runtime.Common.OperationTotal) != 0 {
		t.Fatalf("operation totals should be empty, got %d", len(runtime.Common.OperationTotal))
	}
	if len(runtime.Common.ErrorsTotal) != 0 {
		t.Fatalf("error totals should be empty, got %d", len(runtime.Common.ErrorsTotal))
	}
}
