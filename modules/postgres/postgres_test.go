package postgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

func TestNewStore_EmptyDSN(t *testing.T) {
	_, err := NewStore("   ")
	if err == nil {
		t.Fatalf("expected error for empty dsn")
	}
	if !strings.Contains(err.Error(), "empty postgres dsn") {
		t.Fatalf("error = %v, want contains %q", err, "empty postgres dsn")
	}
}

func TestStore_RuntimeMetrics_OperationAndErrorCounters(t *testing.T) {
	store := &Store{metrics: newPostgresRuntimeMetrics()}

	store.observeStoreOperation("enqueue", time.Now().Add(-10*time.Millisecond), nil)
	store.observeStoreOperation("enqueue", time.Now().Add(-5*time.Millisecond), queue.ErrQueueFull)
	store.observeStoreOperation("dequeue", time.Now().Add(-3*time.Millisecond), queue.ErrLeaseNotFound)

	runtime := store.RuntimeMetrics()
	if runtime.Backend != "postgres" {
		t.Fatalf("backend = %q, want postgres", runtime.Backend)
	}

	durationByOperation := make(map[string]queue.HistogramSnapshot)
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

func TestStore_RuntimeMetrics_NilStore(t *testing.T) {
	var store *Store

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

// --- Option function tests ---

func TestWithQueueLimits(t *testing.T) {
	s := &Store{}
	WithQueueLimits(500, "  Drop_Oldest  ")(s)
	if s.maxDepth != 500 {
		t.Fatalf("maxDepth = %d, want 500", s.maxDepth)
	}
	if s.dropPolicy != "drop_oldest" {
		t.Fatalf("dropPolicy = %q, want %q", s.dropPolicy, "drop_oldest")
	}

	// zero maxDepth is valid (disables limit)
	s2 := &Store{maxDepth: 100}
	WithQueueLimits(0, "reject")(s2)
	if s2.maxDepth != 0 {
		t.Fatalf("maxDepth = %d, want 0", s2.maxDepth)
	}

	// negative maxDepth is ignored
	s3 := &Store{maxDepth: 100}
	WithQueueLimits(-1, "")(s3)
	if s3.maxDepth != 100 {
		t.Fatalf("maxDepth = %d, want 100 (unchanged)", s3.maxDepth)
	}

	// empty/whitespace dropPolicy is ignored
	s4 := &Store{dropPolicy: "reject"}
	WithQueueLimits(10, "   ")(s4)
	if s4.dropPolicy != "reject" {
		t.Fatalf("dropPolicy = %q, want %q (unchanged)", s4.dropPolicy, "reject")
	}
}

func TestWithRetention(t *testing.T) {
	s := &Store{}
	WithRetention(24*time.Hour, 5*time.Minute)(s)
	if s.retentionMaxAge != 24*time.Hour {
		t.Fatalf("retentionMaxAge = %v, want 24h", s.retentionMaxAge)
	}
	if s.pruneInterval != 5*time.Minute {
		t.Fatalf("pruneInterval = %v, want 5m", s.pruneInterval)
	}

	// zero clears values
	s2 := &Store{retentionMaxAge: time.Hour, pruneInterval: time.Minute}
	WithRetention(0, 0)(s2)
	if s2.retentionMaxAge != 0 {
		t.Fatalf("retentionMaxAge = %v, want 0", s2.retentionMaxAge)
	}
	if s2.pruneInterval != 0 {
		t.Fatalf("pruneInterval = %v, want 0", s2.pruneInterval)
	}

	// negative clears values
	s3 := &Store{retentionMaxAge: time.Hour, pruneInterval: time.Minute}
	WithRetention(-1, -1)(s3)
	if s3.retentionMaxAge != 0 {
		t.Fatalf("retentionMaxAge = %v, want 0", s3.retentionMaxAge)
	}
	if s3.pruneInterval != 0 {
		t.Fatalf("pruneInterval = %v, want 0", s3.pruneInterval)
	}
}

func TestWithDeliveredRetention(t *testing.T) {
	s := &Store{}
	WithDeliveredRetention(48 * time.Hour)(s)
	if s.deliveredRetentionMaxAge != 48*time.Hour {
		t.Fatalf("deliveredRetentionMaxAge = %v, want 48h", s.deliveredRetentionMaxAge)
	}

	// zero clears
	s2 := &Store{deliveredRetentionMaxAge: time.Hour}
	WithDeliveredRetention(0)(s2)
	if s2.deliveredRetentionMaxAge != 0 {
		t.Fatalf("deliveredRetentionMaxAge = %v, want 0", s2.deliveredRetentionMaxAge)
	}

	// negative clears
	s3 := &Store{deliveredRetentionMaxAge: time.Hour}
	WithDeliveredRetention(-1)(s3)
	if s3.deliveredRetentionMaxAge != 0 {
		t.Fatalf("deliveredRetentionMaxAge = %v, want 0", s3.deliveredRetentionMaxAge)
	}
}

func TestWithDLQRetention(t *testing.T) {
	s := &Store{}
	WithDLQRetention(72*time.Hour, 1000)(s)
	if s.dlqRetentionMaxAge != 72*time.Hour {
		t.Fatalf("dlqRetentionMaxAge = %v, want 72h", s.dlqRetentionMaxAge)
	}
	if s.dlqMaxDepth != 1000 {
		t.Fatalf("dlqMaxDepth = %d, want 1000", s.dlqMaxDepth)
	}

	// zero clears both
	s2 := &Store{dlqRetentionMaxAge: time.Hour, dlqMaxDepth: 500}
	WithDLQRetention(0, 0)(s2)
	if s2.dlqRetentionMaxAge != 0 {
		t.Fatalf("dlqRetentionMaxAge = %v, want 0", s2.dlqRetentionMaxAge)
	}
	if s2.dlqMaxDepth != 0 {
		t.Fatalf("dlqMaxDepth = %d, want 0", s2.dlqMaxDepth)
	}

	// negative clears both
	s3 := &Store{dlqRetentionMaxAge: time.Hour, dlqMaxDepth: 500}
	WithDLQRetention(-1, -1)(s3)
	if s3.dlqRetentionMaxAge != 0 {
		t.Fatalf("dlqRetentionMaxAge = %v, want 0", s3.dlqRetentionMaxAge)
	}
	if s3.dlqMaxDepth != 0 {
		t.Fatalf("dlqMaxDepth = %d, want 0", s3.dlqMaxDepth)
	}
}

func TestWithPollInterval(t *testing.T) {
	s := &Store{}
	WithPollInterval(100 * time.Millisecond)(s)
	if s.pollInterval != 100*time.Millisecond {
		t.Fatalf("pollInterval = %v, want 100ms", s.pollInterval)
	}

	// zero is ignored
	s2 := &Store{pollInterval: 50 * time.Millisecond}
	WithPollInterval(0)(s2)
	if s2.pollInterval != 50*time.Millisecond {
		t.Fatalf("pollInterval = %v, want 50ms (unchanged)", s2.pollInterval)
	}

	// negative is ignored
	s3 := &Store{pollInterval: 50 * time.Millisecond}
	WithPollInterval(-1)(s3)
	if s3.pollInterval != 50*time.Millisecond {
		t.Fatalf("pollInterval = %v, want 50ms (unchanged)", s3.pollInterval)
	}
}

func TestWithNowFunc(t *testing.T) {
	fixed := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &Store{}
	WithNowFunc(func() time.Time { return fixed })(s)
	if s.nowFn == nil {
		t.Fatal("nowFn should be set")
	}
	if got := s.nowFn(); !got.Equal(fixed) {
		t.Fatalf("nowFn() = %v, want %v", got, fixed)
	}

	// nil is ignored
	s2 := &Store{nowFn: time.Now}
	WithNowFunc(nil)(s2)
	if s2.nowFn == nil {
		t.Fatal("nowFn should remain set after nil arg")
	}
}

// --- postgresMetricErrorKind tests ---

func TestPostgresMetricErrorKind(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"queue_full", queue.ErrQueueFull, "queue_full"},
		{"duplicate", queue.ErrEnvelopeExists, "duplicate"},
		{"lease_not_found", queue.ErrLeaseNotFound, "lease_not_found"},
		{"lease_expired", queue.ErrLeaseExpired, "lease_expired"},
		{"generic", errors.New("something broke"), "other"},
		{"wrapped_queue_full", fmt.Errorf("wrap: %w", queue.ErrQueueFull), "queue_full"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := postgresMetricErrorKind(tt.err)
			if got != tt.want {
				t.Fatalf("postgresMetricErrorKind(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

// --- clampSliceCap tests ---

func TestClampSliceCap(t *testing.T) {
	tests := []struct {
		size, max, want int
	}{
		{50, 100, 50},   // normal: within max
		{200, 100, 100}, // clamped to max
		{100, 100, 100}, // equal to max
		{0, 100, 0},     // zero size
		{-1, 100, 0},    // negative size
		{50, 0, 0},      // zero max
		{50, -1, 0},     // negative max
		{0, 0, 0},       // both zero
		{-1, -1, 0},     // both negative
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("size=%d,max=%d", tt.size, tt.max), func(t *testing.T) {
			got := clampSliceCap(tt.size, tt.max)
			if got != tt.want {
				t.Fatalf("clampSliceCap(%d, %d) = %d, want %d", tt.size, tt.max, got, tt.want)
			}
		})
	}
}

// --- normalizeUniqueIDs tests ---

func TestNormalizeUniqueIDs(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		got := normalizeUniqueIDs(nil)
		if got != nil {
			t.Fatalf("normalizeUniqueIDs(nil) = %v, want nil", got)
		}
	})

	t.Run("empty", func(t *testing.T) {
		got := normalizeUniqueIDs([]string{})
		if got != nil {
			t.Fatalf("normalizeUniqueIDs([]) = %v, want nil", got)
		}
	})

	t.Run("dedup_and_trim", func(t *testing.T) {
		got := normalizeUniqueIDs([]string{"  a ", "b", " a", "c", "b "})
		want := []string{"a", "b", "c"}
		if len(got) != len(want) {
			t.Fatalf("len = %d, want %d", len(got), len(want))
		}
		for i, v := range want {
			if got[i] != v {
				t.Fatalf("got[%d] = %q, want %q", i, got[i], v)
			}
		}
	})

	t.Run("skip_empty_and_whitespace", func(t *testing.T) {
		got := normalizeUniqueIDs([]string{"", "  ", "a", ""})
		if len(got) != 1 || got[0] != "a" {
			t.Fatalf("got = %v, want [a]", got)
		}
	})
}

// --- nullIfEmpty tests ---

func TestNullIfEmpty(t *testing.T) {
	t.Run("non_empty", func(t *testing.T) {
		got := nullIfEmpty("hello")
		if got != "hello" {
			t.Fatalf("nullIfEmpty(%q) = %v, want %q", "hello", got, "hello")
		}
	})

	t.Run("empty_string", func(t *testing.T) {
		got := nullIfEmpty("")
		if got != nil {
			t.Fatalf("nullIfEmpty(%q) = %v, want nil", "", got)
		}
	})

	t.Run("whitespace_only", func(t *testing.T) {
		got := nullIfEmpty("   ")
		if got != nil {
			t.Fatalf("nullIfEmpty(%q) = %v, want nil", "   ", got)
		}
	})
}

// --- nullTime tests ---

func TestNullTime(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		got := nullTime(time.Time{})
		if got != nil {
			t.Fatalf("nullTime(zero) = %v, want nil", got)
		}
	})

	t.Run("non_zero_returns_utc", func(t *testing.T) {
		eastern, _ := time.LoadLocation("America/New_York")
		input := time.Date(2025, 6, 15, 12, 0, 0, 0, eastern)
		got := nullTime(input)
		gotTime, ok := got.(time.Time)
		if !ok {
			t.Fatalf("nullTime returned %T, want time.Time", got)
		}
		if gotTime.Location() != time.UTC {
			t.Fatalf("nullTime location = %v, want UTC", gotTime.Location())
		}
		if !gotTime.Equal(input) {
			t.Fatalf("nullTime = %v, want equal to %v", gotTime, input)
		}
	})
}

// --- nullInt tests ---

func TestNullInt(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		got := nullInt(0)
		if got != nil {
			t.Fatalf("nullInt(0) = %v, want nil", got)
		}
	})

	t.Run("positive", func(t *testing.T) {
		got := nullInt(42)
		if got != 42 {
			t.Fatalf("nullInt(42) = %v, want 42", got)
		}
	})

	t.Run("negative", func(t *testing.T) {
		got := nullInt(-1)
		if got != -1 {
			t.Fatalf("nullInt(-1) = %v, want -1", got)
		}
	})
}

// --- encodeStringMapJSON tests ---

func TestEncodeStringMapJSON(t *testing.T) {
	t.Run("nil_map", func(t *testing.T) {
		got, err := encodeStringMapJSON(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(got) != "{}" {
			t.Fatalf("encodeStringMapJSON(nil) = %s, want {}", got)
		}
	})

	t.Run("empty_map", func(t *testing.T) {
		got, err := encodeStringMapJSON(map[string]string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(got) != "{}" {
			t.Fatalf("encodeStringMapJSON({}) = %s, want {}", got)
		}
	})

	t.Run("normal_map", func(t *testing.T) {
		input := map[string]string{"Content-Type": "application/json"}
		got, err := encodeStringMapJSON(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var decoded map[string]string
		if err := json.Unmarshal(got, &decoded); err != nil {
			t.Fatalf("cannot unmarshal result: %v", err)
		}
		if decoded["Content-Type"] != "application/json" {
			t.Fatalf("decoded[Content-Type] = %q, want %q", decoded["Content-Type"], "application/json")
		}
	})
}

// --- decodeStringMapJSON tests ---

func TestDecodeStringMapJSON(t *testing.T) {
	t.Run("nil_input", func(t *testing.T) {
		got := decodeStringMapJSON(nil)
		if got != nil {
			t.Fatalf("decodeStringMapJSON(nil) = %v, want nil", got)
		}
	})

	t.Run("empty_object", func(t *testing.T) {
		got := decodeStringMapJSON([]byte("{}"))
		if got != nil {
			t.Fatalf("decodeStringMapJSON({}) = %v, want nil", got)
		}
	})

	t.Run("invalid_json", func(t *testing.T) {
		got := decodeStringMapJSON([]byte("not json"))
		if got != nil {
			t.Fatalf("decodeStringMapJSON(invalid) = %v, want nil", got)
		}
	})

	t.Run("valid_map", func(t *testing.T) {
		got := decodeStringMapJSON([]byte(`{"foo":"bar","baz":"qux"}`))
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
		if got["foo"] != "bar" {
			t.Fatalf("got[foo] = %q, want %q", got["foo"], "bar")
		}
		if got["baz"] != "qux" {
			t.Fatalf("got[baz] = %q, want %q", got["baz"], "qux")
		}
	})
}

// --- mapPostgresInsertError tests ---

func TestMapPostgresInsertError(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		got := mapPostgresInsertError(nil)
		if got != nil {
			t.Fatalf("mapPostgresInsertError(nil) = %v, want nil", got)
		}
	})

	t.Run("generic_error_passthrough", func(t *testing.T) {
		orig := errors.New("some db error")
		got := mapPostgresInsertError(orig)
		if got != orig {
			t.Fatalf("mapPostgresInsertError(generic) = %v, want %v", got, orig)
		}
	})
}

// --- Integration tests (DSN-gated) ---

func requirePostgresDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("HOOKAIDO_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("HOOKAIDO_TEST_POSTGRES_DSN not set")
	}
	return dsn
}

// cleanupStore truncates all tables so each integration test starts clean.
func cleanupStore(t *testing.T, s *Store) {
	t.Helper()
	for _, table := range []string{"queue_items", "delivery_attempts", "backlog_trend_samples"} {
		if _, err := s.db.Exec("DELETE FROM " + table); err != nil {
			t.Fatalf("cleanup %s: %v", table, err)
		}
	}
}

func TestIntegration_SchemaInit(t *testing.T) {
	dsn := requirePostgresDSN(t)

	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	expectedTables := []string{"queue_items", "delivery_attempts", "backlog_trend_samples"}
	for _, table := range expectedTables {
		var exists bool
		err := store.db.QueryRow(`
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_name = $1
			)
		`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("checking table %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("table %s does not exist after schema init", table)
		}
	}
}

func TestIntegration_EnqueueDequeueRoundTrip(t *testing.T) {
	dsn := requirePostgresDSN(t)

	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	cleanupStore(t, store)

	env := queue.Envelope{
		ID:      "roundtrip_evt_1",
		Route:   "/webhooks",
		Target:  "https://example.com/hook",
		Payload: []byte(`{"event":"test"}`),
		Headers: map[string]string{"Content-Type": "application/json", "X-Custom": "value"},
	}

	if err := store.Enqueue(env); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	resp, err := store.Dequeue(queue.DequeueRequest{
		Route:    "/webhooks",
		Target:   "https://example.com/hook",
		Batch:    1,
		LeaseTTL: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("dequeue items = %d, want 1", len(resp.Items))
	}

	got := resp.Items[0]
	if got.ID != env.ID {
		t.Fatalf("ID = %q, want %q", got.ID, env.ID)
	}
	if got.Route != env.Route {
		t.Fatalf("Route = %q, want %q", got.Route, env.Route)
	}
	if got.Target != env.Target {
		t.Fatalf("Target = %q, want %q", got.Target, env.Target)
	}
	if string(got.Payload) != string(env.Payload) {
		t.Fatalf("Payload = %q, want %q", got.Payload, env.Payload)
	}
	if got.Headers["Content-Type"] != "application/json" {
		t.Fatalf("Headers[Content-Type] = %q, want %q", got.Headers["Content-Type"], "application/json")
	}
	if got.Headers["X-Custom"] != "value" {
		t.Fatalf("Headers[X-Custom] = %q, want %q", got.Headers["X-Custom"], "value")
	}
	if got.State != queue.StateLeased {
		t.Fatalf("State = %q, want %q", got.State, queue.StateLeased)
	}
	if got.Attempt != 1 {
		t.Fatalf("Attempt = %d, want 1", got.Attempt)
	}
	if got.LeaseID == "" {
		t.Fatal("LeaseID should be set after dequeue")
	}
}

func TestIntegration_QueueLimits(t *testing.T) {
	dsn := requirePostgresDSN(t)

	store, err := NewStore(dsn, WithQueueLimits(2, "reject"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	cleanupStore(t, store)

	if err := store.Enqueue(queue.Envelope{
		ID: "qlimit_1", Route: "/limits", Target: "pull", Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("Enqueue 1: %v", err)
	}

	if err := store.Enqueue(queue.Envelope{
		ID: "qlimit_2", Route: "/limits", Target: "pull", Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("Enqueue 2: %v", err)
	}

	err = store.Enqueue(queue.Envelope{
		ID: "qlimit_3", Route: "/limits", Target: "pull", Payload: []byte("{}"),
	})
	if !errors.Is(err, queue.ErrQueueFull) {
		t.Fatalf("Enqueue 3: got %v, want ErrQueueFull", err)
	}
}

func TestIntegration_BacklogTrendCapture(t *testing.T) {
	dsn := requirePostgresDSN(t)

	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	cleanupStore(t, store)

	if err := store.Enqueue(queue.Envelope{
		ID: "trend_evt_1", Route: "/trend", Target: "pull", Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	captureTime := time.Now().UTC()
	if err := store.CaptureBacklogTrendSample(captureTime); err != nil {
		t.Fatalf("CaptureBacklogTrendSample: %v", err)
	}

	resp, err := store.ListBacklogTrend(queue.BacklogTrendListRequest{
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("ListBacklogTrend: %v", err)
	}
	if len(resp.Items) == 0 {
		t.Fatal("expected at least 1 trend sample, got 0")
	}

	// Find the sample matching our capture time (global aggregate, route="" target="").
	found := false
	for _, s := range resp.Items {
		if s.CapturedAt.Equal(captureTime) {
			found = true
			if s.Queued < 1 {
				t.Fatalf("Queued = %d, want >= 1", s.Queued)
			}
		}
	}
	if !found {
		t.Fatalf("no trend sample found at capture time %v", captureTime)
	}
}

func TestIntegration_ConcurrentDequeue(t *testing.T) {
	dsn := requirePostgresDSN(t)

	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	cleanupStore(t, store)

	if err := store.Enqueue(queue.Envelope{
		ID: "concurrent_evt_1", Route: "/concurrent", Target: "pull", Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	const goroutines = 10
	results := make(chan int, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			resp, err := store.Dequeue(queue.DequeueRequest{
				Route:    "/concurrent",
				Target:   "pull",
				Batch:    1,
				LeaseTTL: 30 * time.Second,
			})
			if err != nil {
				results <- 0
				return
			}
			results <- len(resp.Items)
		}()
	}

	wg.Wait()
	close(results)

	successCount := 0
	for n := range results {
		successCount += n
	}
	if successCount != 1 {
		t.Fatalf("exactly 1 goroutine should dequeue the item, got %d", successCount)
	}
}

func TestIntegration_RetentionPruning(t *testing.T) {
	dsn := requirePostgresDSN(t)

	// Use a very short retention and prune interval so pruning triggers.
	store, err := NewStore(dsn,
		WithRetention(1*time.Millisecond, 1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	cleanupStore(t, store)

	// Enqueue items with old received_at timestamps by using WithNowFunc.
	oldTime := time.Now().Add(-1 * time.Hour).UTC()

	// Directly insert old items using the DB.
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("prune_evt_%d", i)
		headersJSON, _ := encodeStringMapJSON(nil)
		traceJSON, _ := encodeStringMapJSON(nil)
		_, err := store.db.Exec(`
			INSERT INTO queue_items (
				id, route, target, state, received_at, attempt, next_run_at,
				payload, headers_json, trace_json, schema_version
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`,
			id, "/prune", "pull", string(queue.StateQueued), oldTime, 0, oldTime,
			[]byte("{}"), headersJSON, traceJSON, 1,
		)
		if err != nil {
			t.Fatalf("insert old item %d: %v", i, err)
		}
	}

	// Verify items exist.
	var countBefore int
	if err := store.db.QueryRow(`
		SELECT COUNT(*) FROM queue_items WHERE route = '/prune'
	`).Scan(&countBefore); err != nil {
		t.Fatalf("count before prune: %v", err)
	}
	if countBefore != 3 {
		t.Fatalf("count before prune = %d, want 3", countBefore)
	}

	// Reset lastPrune so maybePrune fires immediately, then trigger via Enqueue.
	store.mu.Lock()
	store.lastPrune = time.Time{}
	store.mu.Unlock()

	// Trigger prune by calling Enqueue (which calls maybePrune internally).
	if err := store.Enqueue(queue.Envelope{
		ID: "prune_trigger", Route: "/prune", Target: "pull", Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("trigger Enqueue: %v", err)
	}

	// The old items should be pruned; only the new trigger item should remain.
	var countAfter int
	if err := store.db.QueryRow(`
		SELECT COUNT(*) FROM queue_items WHERE route = '/prune'
	`).Scan(&countAfter); err != nil {
		t.Fatalf("count after prune: %v", err)
	}
	if countAfter != 1 {
		t.Fatalf("count after prune = %d, want 1 (only the trigger item)", countAfter)
	}
}
