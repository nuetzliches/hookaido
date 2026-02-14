package app

import (
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

func TestMetricsHandler_DefaultDiagnostics(t *testing.T) {
	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), nil)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example/metrics", nil)
	h.ServeHTTP(rr, req)

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_metrics_schema_info{schema="1.3.0"} 1`,
		"hookaido_tracing_enabled 0",
		"hookaido_tracing_init_failures_total 0",
		"hookaido_tracing_export_errors_total 0",
		"hookaido_publish_accepted_total 0",
		"hookaido_publish_rejected_total 0",
		"hookaido_publish_rejected_validation_total 0",
		"hookaido_publish_rejected_policy_total 0",
		"hookaido_publish_rejected_managed_target_mismatch_total 0",
		"hookaido_publish_rejected_managed_resolver_missing_total 0",
		"hookaido_publish_rejected_conflict_total 0",
		"hookaido_publish_rejected_queue_full_total 0",
		"hookaido_publish_rejected_store_total 0",
		"hookaido_publish_scoped_accepted_total 0",
		"hookaido_publish_scoped_rejected_total 0",
		"hookaido_ingress_accepted_total 0",
		"hookaido_ingress_rejected_total 0",
		`hookaido_ingress_rejected_by_reason_total{reason="memory_pressure",status="503"} 0`,
		`hookaido_ingress_rejected_by_reason_total{reason="queue_full",status="503"} 0`,
		"hookaido_ingress_enqueued_total 0",
		`hookaido_ingress_adaptive_backpressure_total{reason="queued_pressure"} 0`,
		"hookaido_ingress_adaptive_backpressure_applied_total 0",
		"hookaido_delivery_attempts_total 0",
		"hookaido_delivery_acked_total 0",
		"hookaido_delivery_retry_total 0",
		"hookaido_delivery_dead_total 0",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestMetricsHandler_WithDiagnostics(t *testing.T) {
	m := newRuntimeMetrics()
	m.setTracingEnabled(true)
	m.incTracingInitFailures()
	m.incTracingExportErrors()
	m.incTracingExportErrors()
	m.observePublishResult(2, 0, "", false)
	m.observePublishResult(0, 1, "invalid_body", false)
	m.observePublishResult(0, 1, "invalid_header", false)
	m.observePublishResult(0, 1, "managed_selector_required", false)
	m.observePublishResult(0, 1, "managed_target_mismatch", true)
	m.observePublishResult(0, 1, "managed_resolver_missing", false)
	m.observePublishResult(0, 1, "route_resolver_missing", false)
	m.observePublishResult(0, 1, "duplicate_id", true)
	m.observePublishResult(0, 1, "queue_full", true)
	m.observePublishResult(0, 1, "store_unavailable", false)
	m.observePublishResult(1, 0, "", true)

	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), m)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example/metrics", nil)
	h.ServeHTTP(rr, req)

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_metrics_schema_info{schema="1.3.0"} 1`,
		"hookaido_tracing_enabled 1",
		"hookaido_tracing_init_failures_total 1",
		"hookaido_tracing_export_errors_total 2",
		"hookaido_publish_accepted_total 3",
		"hookaido_publish_rejected_total 9",
		"hookaido_publish_rejected_validation_total 2",
		"hookaido_publish_rejected_policy_total 4",
		"hookaido_publish_rejected_managed_target_mismatch_total 1",
		"hookaido_publish_rejected_managed_resolver_missing_total 1",
		"hookaido_publish_rejected_conflict_total 1",
		"hookaido_publish_rejected_queue_full_total 1",
		"hookaido_publish_rejected_store_total 1",
		"hookaido_publish_scoped_accepted_total 1",
		"hookaido_publish_scoped_rejected_total 3",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestRuntimeMetrics_HealthDiagnosticsManagedCounters(t *testing.T) {
	m := newRuntimeMetrics()
	m.observePublishResult(0, 1, "managed_target_mismatch", true)
	m.observePublishResult(0, 2, "managed_resolver_missing", false)

	diag := m.healthDiagnostics()
	publish, ok := diag["publish"].(map[string]any)
	if !ok {
		t.Fatalf("expected publish diagnostics object, got %T", diag["publish"])
	}
	if got := intFromAny(publish["rejected_managed_target_mismatch_total"]); got != 1 {
		t.Fatalf("expected rejected_managed_target_mismatch_total=1, got %#v", publish["rejected_managed_target_mismatch_total"])
	}
	if got := intFromAny(publish["rejected_managed_resolver_missing_total"]); got != 2 {
		t.Fatalf("expected rejected_managed_resolver_missing_total=2, got %#v", publish["rejected_managed_resolver_missing_total"])
	}
}

func intFromAny(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func TestMetricsHandler_IngressCounters(t *testing.T) {
	m := newRuntimeMetrics()
	m.observeIngressResult(true, 2) // accepted, fanout to 2 targets
	m.observeIngressResult(true, 1) // accepted, single target
	m.observeIngressReject(http.StatusTooManyRequests, "rate_limit")
	m.observeIngressReject(http.StatusUnauthorized, "auth")
	m.observeIngressAdaptiveBackpressure("queued_pressure")
	m.observeIngressAdaptiveBackpressure("ready_lag")
	m.observeIngressAdaptiveBackpressure("")

	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		"hookaido_ingress_accepted_total 2",
		"hookaido_ingress_rejected_total 2",
		`hookaido_ingress_rejected_by_reason_total{reason="rate_limit",status="429"} 1`,
		`hookaido_ingress_rejected_by_reason_total{reason="auth",status="401"} 1`,
		"hookaido_ingress_enqueued_total 3",
		`hookaido_ingress_adaptive_backpressure_total{reason="queued_pressure"} 1`,
		`hookaido_ingress_adaptive_backpressure_total{reason="ready_lag"} 1`,
		`hookaido_ingress_adaptive_backpressure_total{reason="unspecified"} 1`,
		"hookaido_ingress_adaptive_backpressure_applied_total 3",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestMetricsHandler_IngressRejectBreakdownNormalization(t *testing.T) {
	m := newRuntimeMetrics()
	m.observeIngressReject(http.StatusServiceUnavailable, "memory_pressure")
	m.observeIngressReject(http.StatusServiceUnavailable, "queue_full")
	m.observeIngressReject(http.StatusTeapot, "unexpected_reason")

	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_ingress_rejected_by_reason_total{reason="memory_pressure",status="503"} 1`,
		`hookaido_ingress_rejected_by_reason_total{reason="queue_full",status="503"} 1`,
		`hookaido_ingress_rejected_by_reason_total{reason="other",status="other"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestMetricsHandler_DeliveryCounters(t *testing.T) {
	m := newRuntimeMetrics()
	m.observeDeliveryAttempt(queue.AttemptOutcomeAcked)
	m.observeDeliveryAttempt(queue.AttemptOutcomeAcked)
	m.observeDeliveryAttempt(queue.AttemptOutcomeRetry)
	m.observeDeliveryAttempt(queue.AttemptOutcomeDead)

	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		"hookaido_delivery_attempts_total 4",
		"hookaido_delivery_acked_total 2",
		"hookaido_delivery_retry_total 1",
		"hookaido_delivery_dead_total 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestMetricsHandler_QueueDepth(t *testing.T) {
	store := queue.NewMemoryStore()
	// Enqueue two items
	_ = store.Enqueue(queue.Envelope{ID: "e1", Route: "/test", Target: "pull", Payload: []byte(`{}`)})
	_ = store.Enqueue(queue.Envelope{ID: "e2", Route: "/test", Target: "pull", Payload: []byte(`{}`)})

	m := newRuntimeMetrics()
	m.queueStore = store

	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_queue_depth{state="queued"} 2`,
		`hookaido_queue_depth{state="leased"} 0`,
		`hookaido_queue_depth{state="dead"} 0`,
		`hookaido_queue_total 2`,
		`hookaido_queue_oldest_queued_age_seconds `,
		`hookaido_queue_ready_lag_seconds `,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestMetricsHandler_QueueLagAge(t *testing.T) {
	now := time.Date(2026, 2, 13, 12, 0, 0, 0, time.UTC)
	nowVar := now
	store := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return nowVar }))
	if err := store.Enqueue(queue.Envelope{
		ID:         "e1",
		Route:      "/test",
		Target:     "pull",
		State:      queue.StateQueued,
		ReceivedAt: nowVar.Add(-10 * time.Second),
		NextRunAt:  nowVar.Add(-5 * time.Second),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	m := newRuntimeMetrics()
	m.queueStore = store

	h := newMetricsHandler("dev", nowVar, m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_queue_total 1`,
		`hookaido_queue_oldest_queued_age_seconds 10.000000`,
		`hookaido_queue_ready_lag_seconds 5.000000`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestMetricsHandler_QueueDepthNilStore(t *testing.T) {
	// When no store is set, queue_depth metrics should not appear
	m := newRuntimeMetrics()

	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	if strings.Contains(body, "hookaido_queue_depth") {
		t.Fatalf("queue_depth should not appear when store is nil:\n%s", body)
	}
	if strings.Contains(body, "hookaido_queue_total") {
		t.Fatalf("queue_total should not appear when store is nil:\n%s", body)
	}
	if strings.Contains(body, "hookaido_queue_oldest_queued_age_seconds") {
		t.Fatalf("queue_oldest_queued_age_seconds should not appear when store is nil:\n%s", body)
	}
	if strings.Contains(body, "hookaido_queue_ready_lag_seconds") {
		t.Fatalf("queue_ready_lag_seconds should not appear when store is nil:\n%s", body)
	}
}

func TestMetricsHandler_UsesCachedQueueDepthWhenRefreshIsSlow(t *testing.T) {
	base := queue.NewMemoryStore()
	if err := base.Enqueue(queue.Envelope{ID: "evt_cache_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	blockCh := make(chan struct{})
	var statsCalls atomic.Int64
	store := &appStatsOverrideStore{
		Store: base,
		statsFn: func() (queue.Stats, error) {
			call := statsCalls.Add(1)
			if call >= 2 {
				<-blockCh
			}
			return base.Stats()
		},
	}

	m := newRuntimeMetrics()
	m.queueStore = store
	m.queueStats.ttl = 0

	h := newMetricsHandler("dev", time.Unix(100, 0).UTC(), m)

	// Warm cache.
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))
	if rr1.Code != http.StatusOK {
		t.Fatalf("warm metrics status: got %d", rr1.Code)
	}

	// Second scrape should use stale cached depth while async refresh is blocked.
	start := time.Now()
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))
	if rr2.Code != http.StatusOK {
		t.Fatalf("second metrics status: got %d", rr2.Code)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("expected cached metrics response to be fast, got %s", elapsed)
	}
	if body := rr2.Body.String(); !strings.Contains(body, `hookaido_queue_depth{state="queued"} 1`) {
		t.Fatalf("expected cached queue depth in metrics output:\n%s", body)
	}

	close(blockCh)
}

func TestMetricsHandler_SQLiteStoreRuntimeMetrics(t *testing.T) {
	now := time.Unix(500, 0).UTC()
	nowVar := now
	dbPath := filepath.Join(t.TempDir(), "hookaido.db")
	store, err := queue.NewSQLiteStore(
		dbPath,
		queue.WithSQLiteNowFunc(func() time.Time { return nowVar }),
		queue.WithSQLiteQueueLimits(10000, "reject"),
		queue.WithSQLiteCheckpointInterval(0),
	)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	resp, err := store.Dequeue(queue.DequeueRequest{Route: "/r", Target: "pull", LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 dequeued item, got %d", len(resp.Items))
	}
	if err := store.Ack(resp.Items[0].LeaseID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	m := newRuntimeMetrics()
	m.queueStore = store

	h := newMetricsHandler("dev", now, m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_store_operation_seconds_bucket{backend="sqlite",operation="write_tx",le="+Inf"}`,
		`hookaido_store_operation_seconds_count{backend="sqlite",operation="write_tx"} `,
		`hookaido_store_operation_total{backend="sqlite",operation="tx_commit"} `,
		`hookaido_store_errors_total{backend="sqlite",operation="begin_tx",kind="busy"} `,
		`hookaido_store_sqlite_write_seconds_bucket{le="+Inf"}`,
		`hookaido_store_sqlite_write_seconds_count `,
		`hookaido_store_sqlite_dequeue_seconds_bucket{le="+Inf"}`,
		`hookaido_store_sqlite_dequeue_seconds_count `,
		`hookaido_store_sqlite_checkpoint_seconds_bucket{le="+Inf"}`,
		`hookaido_store_sqlite_busy_total `,
		`hookaido_store_sqlite_retry_total `,
		`hookaido_store_sqlite_tx_commit_total `,
		`hookaido_store_sqlite_tx_rollback_total `,
		`hookaido_store_sqlite_checkpoint_total `,
		`hookaido_store_sqlite_checkpoint_errors_total `,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestMetricsHandler_MemoryStoreRuntimeMetrics(t *testing.T) {
	now := time.Unix(500, 0).UTC()
	store := queue.NewMemoryStore(
		queue.WithNowFunc(func() time.Time { return now }),
		queue.WithQueueLimits(1, "drop_oldest"),
	)
	if err := store.Enqueue(queue.Envelope{
		ID:      "evt_1",
		Route:   "/r",
		Target:  "pull",
		Payload: []byte("alpha"),
		Headers: map[string]string{"X-Test": "1"},
	}); err != nil {
		t.Fatalf("enqueue evt_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{
		ID:      "evt_2",
		Route:   "/r",
		Target:  "pull",
		Payload: []byte("beta"),
	}); err != nil {
		t.Fatalf("enqueue evt_2: %v", err)
	}

	m := newRuntimeMetrics()
	m.queueStore = store

	h := newMetricsHandler("dev", now, m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_store_operation_total{backend="memory",operation="enqueue_reject"} 0`,
		`hookaido_store_operation_total{backend="memory",operation="evict"} 1`,
		`hookaido_store_errors_total{backend="memory",operation="enqueue",kind="memory_pressure"} 0`,
		`hookaido_store_memory_items{state="queued"} 1`,
		`hookaido_store_memory_items{state="leased"} 0`,
		`hookaido_store_memory_items{state="delivered"} 0`,
		`hookaido_store_memory_items{state="dead"} 0`,
		`hookaido_store_memory_retained_bytes{state="queued"} `,
		`hookaido_store_memory_retained_bytes_total `,
		`hookaido_store_memory_evictions_total{reason="drop_oldest"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestMetricsHandler_MemoryStoreCommonErrorMetrics(t *testing.T) {
	now := time.Unix(700, 0).UTC()
	store := queue.NewMemoryStore(
		queue.WithNowFunc(func() time.Time { return now }),
		queue.WithQueueLimits(10, "reject"),
		queue.WithMemoryPressureLimits(1, 1<<30),
	)
	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); !errors.Is(err, queue.ErrMemoryPressure) {
		t.Fatalf("expected ErrMemoryPressure, got %v", err)
	}

	m := newRuntimeMetrics()
	m.queueStore = store

	h := newMetricsHandler("dev", now, m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_store_operation_total{backend="memory",operation="enqueue_reject"} 1`,
		`hookaido_store_errors_total{backend="memory",operation="enqueue",kind="memory_pressure"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestMetricsHandler_PostgresStoreRuntimeMetrics(t *testing.T) {
	now := time.Unix(900, 0).UTC()
	base := queue.NewMemoryStore(queue.WithNowFunc(func() time.Time { return now }))
	if err := base.Enqueue(queue.Envelope{ID: "evt_pg_metrics", Route: "/r", Target: "pull"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	store := &appRuntimeMetricsOverrideStore{
		Store: base,
		runtime: queue.StoreRuntimeMetrics{
			Backend: "postgres",
			Common: queue.StoreCommonRuntimeMetrics{
				OperationDurationSeconds: []queue.StoreOperationDurationRuntimeMetric{
					{
						Operation: "dequeue_tx",
						DurationSeconds: queue.HistogramSnapshot{
							Buckets: []queue.HistogramBucket{
								{Le: 0.01, Count: 1},
								{Le: math.Inf(1), Count: 2},
							},
							Count: 2,
							Sum:   0.014,
						},
					},
				},
				OperationTotal: []queue.StoreOperationCounterRuntimeMetric{
					{Operation: "tx_commit", Total: 5},
				},
				ErrorsTotal: []queue.StoreOperationErrorRuntimeMetric{
					{Operation: "begin_tx", Kind: "serialization_failure", Total: 2},
				},
			},
		},
	}

	m := newRuntimeMetrics()
	m.queueStore = store

	h := newMetricsHandler("dev", now, m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_store_operation_seconds_bucket{backend="postgres",operation="dequeue_tx",le="+Inf"} 2`,
		`hookaido_store_operation_seconds_count{backend="postgres",operation="dequeue_tx"} 2`,
		`hookaido_store_operation_total{backend="postgres",operation="tx_commit"} 5`,
		`hookaido_store_errors_total{backend="postgres",operation="begin_tx",kind="serialization_failure"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestHealthDiagnostics_IngressAndDelivery(t *testing.T) {
	m := newRuntimeMetrics()
	m.observeIngressResult(true, 3)
	m.observeIngressAdaptiveBackpressure("ready_lag")
	m.observeIngressReject(http.StatusServiceUnavailable, "memory_pressure")
	m.observeDeliveryAttempt(queue.AttemptOutcomeAcked)
	m.observeDeliveryAttempt(queue.AttemptOutcomeDead)

	diag := m.healthDiagnostics()

	ingress, ok := diag["ingress"].(map[string]any)
	if !ok {
		t.Fatalf("expected ingress diagnostics, got %T", diag["ingress"])
	}
	if got := intFromAny(ingress["accepted_total"]); got != 1 {
		t.Fatalf("ingress accepted_total=%d, want 1", got)
	}
	if got := intFromAny(ingress["enqueued_total"]); got != 3 {
		t.Fatalf("ingress enqueued_total=%d, want 3", got)
	}
	if got := intFromAny(ingress["adaptive_backpressure_applied_total"]); got != 1 {
		t.Fatalf("ingress adaptive_backpressure_applied_total=%d, want 1", got)
	}
	adaptiveByReason, ok := ingress["adaptive_backpressure_by_reason"].(map[string]any)
	if !ok {
		t.Fatalf("expected adaptive_backpressure_by_reason map, got %T", ingress["adaptive_backpressure_by_reason"])
	}
	if got := intFromAny(adaptiveByReason["ready_lag"]); got != 1 {
		t.Fatalf("ingress adaptive_backpressure_by_reason.ready_lag=%d, want 1", got)
	}
	rejectedByReason, ok := ingress["rejected_by_reason"].(map[string]any)
	if !ok {
		t.Fatalf("expected rejected_by_reason map, got %T", ingress["rejected_by_reason"])
	}
	if got := intFromAny(rejectedByReason["memory_pressure"]); got != 1 {
		t.Fatalf("ingress rejected_by_reason.memory_pressure=%d, want 1", got)
	}

	delivery, ok := diag["delivery"].(map[string]any)
	if !ok {
		t.Fatalf("expected delivery diagnostics, got %T", diag["delivery"])
	}
	if got := intFromAny(delivery["attempt_total"]); got != 2 {
		t.Fatalf("delivery attempt_total=%d, want 2", got)
	}
	if got := intFromAny(delivery["acked_total"]); got != 1 {
		t.Fatalf("delivery acked_total=%d, want 1", got)
	}
	if got := intFromAny(delivery["dead_total"]); got != 1 {
		t.Fatalf("delivery dead_total=%d, want 1", got)
	}
}

func TestHealthDiagnostics_StoreMemoryPressure(t *testing.T) {
	now := time.Unix(700, 0).UTC()
	store := queue.NewMemoryStore(
		queue.WithNowFunc(func() time.Time { return now }),
		queue.WithQueueLimits(10, "reject"),
		queue.WithMemoryPressureLimits(1, 1<<30),
	)
	if err := store.Enqueue(queue.Envelope{ID: "dead_1", Route: "/r", Target: "pull", State: queue.StateDead}); err != nil {
		t.Fatalf("enqueue dead_1: %v", err)
	}
	if err := store.Enqueue(queue.Envelope{ID: "evt_1", Route: "/r", Target: "pull"}); !errors.Is(err, queue.ErrMemoryPressure) {
		t.Fatalf("expected ErrMemoryPressure, got %v", err)
	}

	m := newRuntimeMetrics()
	m.queueStore = store

	diag := m.healthDiagnostics()
	storeDiag, ok := diag["store"].(map[string]any)
	if !ok {
		t.Fatalf("expected store diagnostics object, got %T", diag["store"])
	}
	pressure, ok := storeDiag["memory_pressure"].(map[string]any)
	if !ok {
		t.Fatalf("expected memory_pressure diagnostics object, got %T", storeDiag["memory_pressure"])
	}
	if active, _ := pressure["active"].(bool); !active {
		t.Fatalf("expected memory_pressure.active=true, got %#v", pressure["active"])
	}
	if got, _ := pressure["reason"].(string); got != "retained_items" {
		t.Fatalf("expected memory pressure reason retained_items, got %q", got)
	}
	if got := intFromAny(pressure["rejected_total"]); got != 1 {
		t.Fatalf("expected memory pressure rejected_total=1, got %d", got)
	}
}

func TestMetricsHandler_PullMetrics(t *testing.T) {
	m := newRuntimeMetrics()
	now := time.Unix(100, 0).UTC()
	m.now = func() time.Time { return now }

	m.observePullDequeue("/r1", http.StatusOK, []queue.Envelope{
		{LeaseID: "l1", LeaseUntil: now.Add(30 * time.Second)},
	})
	m.observePullDequeue("/r1", http.StatusBadRequest, nil)
	m.observePullDequeue("/r1", http.StatusServiceUnavailable, nil)
	m.observePullAck("/r1", http.StatusNoContent, "l1", false)
	m.observePullAck("/r1", http.StatusConflict, "missing-ack", false)

	m.observePullDequeue("/r1", http.StatusOK, []queue.Envelope{
		{LeaseID: "l2", LeaseUntil: now.Add(20 * time.Second)},
	})
	m.observePullNack("/r1", http.StatusNoContent, "l2", false)
	m.observePullNack("/r1", http.StatusConflict, "missing-nack", false)

	m.observePullDequeue("/r2", http.StatusOK, []queue.Envelope{
		{LeaseID: "l3", LeaseUntil: now.Add(10 * time.Second)},
	})
	m.observePullExtend("/r2", http.StatusNoContent, "l3", 15*time.Second, false)
	m.observePullExtend("/r2", http.StatusConflict, "l3", 0, true)

	h := newMetricsHandler("dev", now, m)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`hookaido_pull_dequeue_total{route="/r1",status="200"} 2`,
		`hookaido_pull_dequeue_total{route="/r1",status="204"} 0`,
		`hookaido_pull_dequeue_total{route="/r1",status="4xx"} 1`,
		`hookaido_pull_dequeue_total{route="/r1",status="5xx"} 1`,
		`hookaido_pull_acked_total{route="/r1"} 1`,
		`hookaido_pull_nacked_total{route="/r1"} 1`,
		`hookaido_pull_ack_conflict_total{route="/r1"} 1`,
		`hookaido_pull_nack_conflict_total{route="/r1"} 1`,
		`hookaido_pull_lease_active{route="/r1"} 0`,
		`hookaido_pull_lease_expired_total{route="/r1"} 0`,
		`hookaido_pull_dequeue_total{route="/r2",status="200"} 1`,
		`hookaido_pull_lease_active{route="/r2"} 0`,
		`hookaido_pull_lease_expired_total{route="/r2"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestHealthDiagnostics_PullMetrics(t *testing.T) {
	m := newRuntimeMetrics()
	now := time.Unix(200, 0).UTC()
	m.now = func() time.Time { return now }

	m.observePullDequeue("/diag", http.StatusOK, []queue.Envelope{
		{LeaseID: "lease_1", LeaseUntil: now.Add(30 * time.Second)},
	})
	m.observePullAck("/diag", http.StatusConflict, "lease_1", true)

	diag := m.healthDiagnostics()
	pull, ok := diag["pull"].(map[string]any)
	if !ok {
		t.Fatalf("expected pull diagnostics object, got %T", diag["pull"])
	}
	total, ok := pull["total"].(map[string]any)
	if !ok {
		t.Fatalf("expected pull.total object, got %T", pull["total"])
	}
	if got := intFromAny(total["ack_conflict_total"]); got != 1 {
		t.Fatalf("pull.total ack_conflict_total=%d, want 1", got)
	}
	if got := intFromAny(total["lease_expired_total"]); got != 1 {
		t.Fatalf("pull.total lease_expired_total=%d, want 1", got)
	}

	byRoute, ok := pull["by_route"].(map[string]any)
	if !ok {
		t.Fatalf("expected pull.by_route object, got %T", pull["by_route"])
	}
	diagRoute, ok := byRoute["/diag"].(map[string]any)
	if !ok {
		t.Fatalf("expected pull.by_route[/diag], got %T", byRoute["/diag"])
	}
	if got := intFromAny(diagRoute["ack_conflict_total"]); got != 1 {
		t.Fatalf("pull.by_route[/diag] ack_conflict_total=%d, want 1", got)
	}
	if got := intFromAny(diagRoute["lease_expired_total"]); got != 1 {
		t.Fatalf("pull.by_route[/diag] lease_expired_total=%d, want 1", got)
	}
}

func TestMetricsPrefixRouting(t *testing.T) {
	rm := newRuntimeMetrics()
	handler := mountPrefix("/custom/metrics", newMetricsHandler("test", time.Now(), rm))

	// Request to /custom/metrics should return Prometheus text.
	req := httptest.NewRequest("GET", "/custom/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /custom/metrics: got %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("Content-Type: got %q, want text/plain", ct)
	}
	if !strings.Contains(rec.Body.String(), "hookaido_up 1") {
		t.Fatalf("response should contain hookaido_up metric")
	}

	// Request to /metrics should 404.
	req2 := httptest.NewRequest("GET", "/metrics", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("GET /metrics: got %d, want 404", rec2.Code)
	}
}

type appStatsOverrideStore struct {
	queue.Store
	statsFn func() (queue.Stats, error)
}

func (s *appStatsOverrideStore) Stats() (queue.Stats, error) {
	if s == nil || s.statsFn == nil {
		return queue.Stats{}, nil
	}
	return s.statsFn()
}

type appRuntimeMetricsOverrideStore struct {
	queue.Store
	runtime queue.StoreRuntimeMetrics
}

func (s *appRuntimeMetricsOverrideStore) RuntimeMetrics() queue.StoreRuntimeMetrics {
	if s == nil {
		return queue.StoreRuntimeMetrics{}
	}
	return s.runtime
}
