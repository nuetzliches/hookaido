package app

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

type runtimeMetrics struct {
	tracingEnabled                             atomic.Int64
	tracingInitFailuresTotal                   atomic.Int64
	tracingExportErrorsTotal                   atomic.Int64
	publishAcceptedTotal                       atomic.Int64
	publishRejectedTotal                       atomic.Int64
	publishRejectedValidationTotal             atomic.Int64
	publishRejectedPolicyTotal                 atomic.Int64
	publishRejectedManagedTargetMismatchTotal  atomic.Int64
	publishRejectedManagedResolverMissingTotal atomic.Int64
	publishRejectedConflictTotal               atomic.Int64
	publishRejectedQueueFullTotal              atomic.Int64
	publishRejectedStoreTotal                  atomic.Int64
	publishScopedAcceptedTotal                 atomic.Int64
	publishScopedRejectedTotal                 atomic.Int64

	// Ingress counters
	ingressAcceptedTotal atomic.Int64
	ingressRejectedTotal atomic.Int64
	ingressEnqueuedTotal atomic.Int64

	// Delivery counters
	deliveryAttemptTotal atomic.Int64
	deliveryAckedTotal   atomic.Int64
	deliveryRetryTotal   atomic.Int64
	deliveryDeadTotal    atomic.Int64

	// Queue store for on-scrape stats
	queueStore queue.Store
}

func newRuntimeMetrics() *runtimeMetrics {
	return &runtimeMetrics{}
}

func (m *runtimeMetrics) setTracingEnabled(enabled bool) {
	if m == nil {
		return
	}
	if enabled {
		m.tracingEnabled.Store(1)
		return
	}
	m.tracingEnabled.Store(0)
}

func (m *runtimeMetrics) incTracingInitFailures() {
	if m == nil {
		return
	}
	m.tracingInitFailuresTotal.Add(1)
}

func (m *runtimeMetrics) incTracingExportErrors() {
	if m == nil {
		return
	}
	m.tracingExportErrorsTotal.Add(1)
}

func (m *runtimeMetrics) observeIngressResult(accepted bool, enqueued int) {
	if m == nil {
		return
	}
	if accepted {
		m.ingressAcceptedTotal.Add(1)
	} else {
		m.ingressRejectedTotal.Add(1)
	}
	if enqueued > 0 {
		m.ingressEnqueuedTotal.Add(int64(enqueued))
	}
}

func (m *runtimeMetrics) observeDeliveryAttempt(outcome queue.AttemptOutcome) {
	if m == nil {
		return
	}
	m.deliveryAttemptTotal.Add(1)
	switch outcome {
	case queue.AttemptOutcomeAcked:
		m.deliveryAckedTotal.Add(1)
	case queue.AttemptOutcomeRetry:
		m.deliveryRetryTotal.Add(1)
	case queue.AttemptOutcomeDead:
		m.deliveryDeadTotal.Add(1)
	}
}

func (m *runtimeMetrics) observePublishResult(accepted, rejected int, code string, scoped bool) {
	if m == nil {
		return
	}
	if accepted > 0 {
		m.publishAcceptedTotal.Add(int64(accepted))
		if scoped {
			m.publishScopedAcceptedTotal.Add(int64(accepted))
		}
	}
	if rejected > 0 {
		m.publishRejectedTotal.Add(int64(rejected))
		if scoped {
			m.publishScopedRejectedTotal.Add(int64(rejected))
		}
		switch publishRejectClass(code) {
		case "validation":
			m.publishRejectedValidationTotal.Add(int64(rejected))
		case "policy":
			m.publishRejectedPolicyTotal.Add(int64(rejected))
		case "conflict":
			m.publishRejectedConflictTotal.Add(int64(rejected))
		case "queue_full":
			m.publishRejectedQueueFullTotal.Add(int64(rejected))
		default:
			m.publishRejectedStoreTotal.Add(int64(rejected))
		}
		switch code {
		case "managed_target_mismatch":
			m.publishRejectedManagedTargetMismatchTotal.Add(int64(rejected))
		case "managed_resolver_missing":
			m.publishRejectedManagedResolverMissingTotal.Add(int64(rejected))
		}
	}
}

func publishRejectClass(code string) string {
	switch code {
	case "invalid_body", "invalid_received_at", "invalid_next_run_at", "invalid_payload_b64", "invalid_header", "payload_too_large", "headers_too_large", "route_mismatch", "selector_scope_mismatch", "audit_reason_required", "audit_actor_required", "audit_request_id_required":
		return "validation"
	case "audit_actor_not_allowed":
		return "policy"
	case "managed_resolver_missing", "managed_endpoint_not_found", "managed_selector_required", "managed_target_mismatch", "route_resolver_missing", "route_not_found", "target_unresolvable", "managed_endpoint_no_targets":
		return "policy"
	case "scoped_publish_required":
		return "policy"
	case "selector_scope_forbidden":
		return "policy"
	case "global_publish_disabled", "scoped_publish_disabled", "route_publish_disabled":
		return "policy"
	case "pull_route_publish_disabled", "deliver_route_publish_disabled":
		return "policy"
	case "duplicate_id":
		return "conflict"
	case "queue_full":
		return "queue_full"
	default:
		return "store"
	}
}

func (m *runtimeMetrics) healthDiagnostics() map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return map[string]any{
		"tracing": map[string]any{
			"enabled":             m.tracingEnabled.Load() == 1,
			"init_failures_total": m.tracingInitFailuresTotal.Load(),
			"export_errors_total": m.tracingExportErrorsTotal.Load(),
		},
		"publish": map[string]any{
			"accepted_total":                          m.publishAcceptedTotal.Load(),
			"rejected_total":                          m.publishRejectedTotal.Load(),
			"rejected_validation_total":               m.publishRejectedValidationTotal.Load(),
			"rejected_policy_total":                   m.publishRejectedPolicyTotal.Load(),
			"rejected_managed_target_mismatch_total":  m.publishRejectedManagedTargetMismatchTotal.Load(),
			"rejected_managed_resolver_missing_total": m.publishRejectedManagedResolverMissingTotal.Load(),
			"rejected_conflict_total":                 m.publishRejectedConflictTotal.Load(),
			"rejected_queue_full_total":               m.publishRejectedQueueFullTotal.Load(),
			"rejected_store_total":                    m.publishRejectedStoreTotal.Load(),
			"scoped_accepted_total":                   m.publishScopedAcceptedTotal.Load(),
			"scoped_rejected_total":                   m.publishScopedRejectedTotal.Load(),
		},
		"ingress": map[string]any{
			"accepted_total": m.ingressAcceptedTotal.Load(),
			"rejected_total": m.ingressRejectedTotal.Load(),
			"enqueued_total": m.ingressEnqueuedTotal.Load(),
		},
		"delivery": map[string]any{
			"attempt_total": m.deliveryAttemptTotal.Load(),
			"acked_total":   m.deliveryAckedTotal.Load(),
			"retry_total":   m.deliveryRetryTotal.Load(),
			"dead_total":    m.deliveryDeadTotal.Load(),
		},
	}
}

func newMetricsHandler(version string, start time.Time, rm *runtimeMetrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		tracingEnabled := int64(0)
		tracingInitFailuresTotal := int64(0)
		tracingExportErrorsTotal := int64(0)
		publishAcceptedTotal := int64(0)
		publishRejectedTotal := int64(0)
		publishRejectedValidationTotal := int64(0)
		publishRejectedPolicyTotal := int64(0)
		publishRejectedManagedTargetMismatchTotal := int64(0)
		publishRejectedManagedResolverMissingTotal := int64(0)
		publishRejectedConflictTotal := int64(0)
		publishRejectedQueueFullTotal := int64(0)
		publishRejectedStoreTotal := int64(0)
		publishScopedAcceptedTotal := int64(0)
		publishScopedRejectedTotal := int64(0)
		if rm != nil {
			tracingEnabled = rm.tracingEnabled.Load()
			tracingInitFailuresTotal = rm.tracingInitFailuresTotal.Load()
			tracingExportErrorsTotal = rm.tracingExportErrorsTotal.Load()
			publishAcceptedTotal = rm.publishAcceptedTotal.Load()
			publishRejectedTotal = rm.publishRejectedTotal.Load()
			publishRejectedValidationTotal = rm.publishRejectedValidationTotal.Load()
			publishRejectedPolicyTotal = rm.publishRejectedPolicyTotal.Load()
			publishRejectedManagedTargetMismatchTotal = rm.publishRejectedManagedTargetMismatchTotal.Load()
			publishRejectedManagedResolverMissingTotal = rm.publishRejectedManagedResolverMissingTotal.Load()
			publishRejectedConflictTotal = rm.publishRejectedConflictTotal.Load()
			publishRejectedQueueFullTotal = rm.publishRejectedQueueFullTotal.Load()
			publishRejectedStoreTotal = rm.publishRejectedStoreTotal.Load()
			publishScopedAcceptedTotal = rm.publishScopedAcceptedTotal.Load()
			publishScopedRejectedTotal = rm.publishScopedRejectedTotal.Load()
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "# HELP hookaido_up Whether the Hookaido process is up.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_up gauge\n")
		_, _ = fmt.Fprintf(w, "hookaido_up 1\n")
		_, _ = fmt.Fprintf(w, "# HELP hookaido_build_info Build information.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_build_info gauge\n")
		_, _ = fmt.Fprintf(w, "hookaido_build_info{version=%q} 1\n", version)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_start_time_seconds Start time since unix epoch.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_start_time_seconds gauge\n")
		_, _ = fmt.Fprintf(w, "hookaido_start_time_seconds %d\n", start.Unix())
		_, _ = fmt.Fprintf(w, "# HELP hookaido_tracing_enabled Whether tracing is enabled.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_tracing_enabled gauge\n")
		_, _ = fmt.Fprintf(w, "hookaido_tracing_enabled %d\n", tracingEnabled)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_tracing_init_failures_total Total number of tracing initialization failures.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_tracing_init_failures_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_tracing_init_failures_total %d\n", tracingInitFailuresTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_tracing_export_errors_total Total number of tracing exporter errors reported by OpenTelemetry.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_tracing_export_errors_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_tracing_export_errors_total %d\n", tracingExportErrorsTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_publish_accepted_total Total number of published queue messages accepted via Admin publish endpoints.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_publish_accepted_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_publish_accepted_total %d\n", publishAcceptedTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_publish_rejected_total Total number of Admin publish endpoint rejections.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_publish_rejected_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_publish_rejected_total %d\n", publishRejectedTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_publish_rejected_validation_total Total number of Admin publish rejections classified as validation errors.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_publish_rejected_validation_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_publish_rejected_validation_total %d\n", publishRejectedValidationTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_publish_rejected_policy_total Total number of Admin publish rejections classified as policy/selector errors.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_publish_rejected_policy_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_publish_rejected_policy_total %d\n", publishRejectedPolicyTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_publish_rejected_managed_target_mismatch_total Total number of Admin publish rejections with code managed_target_mismatch.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_publish_rejected_managed_target_mismatch_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_publish_rejected_managed_target_mismatch_total %d\n", publishRejectedManagedTargetMismatchTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_publish_rejected_managed_resolver_missing_total Total number of Admin publish rejections with code managed_resolver_missing.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_publish_rejected_managed_resolver_missing_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_publish_rejected_managed_resolver_missing_total %d\n", publishRejectedManagedResolverMissingTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_publish_rejected_conflict_total Total number of Admin publish rejections caused by duplicate IDs.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_publish_rejected_conflict_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_publish_rejected_conflict_total %d\n", publishRejectedConflictTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_publish_rejected_queue_full_total Total number of Admin publish rejections caused by queue full.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_publish_rejected_queue_full_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_publish_rejected_queue_full_total %d\n", publishRejectedQueueFullTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_publish_rejected_store_total Total number of Admin publish rejections caused by store availability/runtime errors.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_publish_rejected_store_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_publish_rejected_store_total %d\n", publishRejectedStoreTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_publish_scoped_accepted_total Total number of accepted messages via endpoint-scoped publish routes.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_publish_scoped_accepted_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_publish_scoped_accepted_total %d\n", publishScopedAcceptedTotal)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_publish_scoped_rejected_total Total number of rejections via endpoint-scoped publish routes.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_publish_scoped_rejected_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_publish_scoped_rejected_total %d\n", publishScopedRejectedTotal)

		// --- Ingress metrics ---
		ingressAccepted := int64(0)
		ingressRejected := int64(0)
		ingressEnqueued := int64(0)
		if rm != nil {
			ingressAccepted = rm.ingressAcceptedTotal.Load()
			ingressRejected = rm.ingressRejectedTotal.Load()
			ingressEnqueued = rm.ingressEnqueuedTotal.Load()
		}
		_, _ = fmt.Fprintf(w, "# HELP hookaido_ingress_accepted_total Total number of ingress requests accepted and enqueued.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_ingress_accepted_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_ingress_accepted_total %d\n", ingressAccepted)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_ingress_rejected_total Total number of ingress requests rejected (auth, rate-limit, not-found, etc).\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_ingress_rejected_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_ingress_rejected_total %d\n", ingressRejected)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_ingress_enqueued_total Total number of items enqueued via ingress (may exceed accepted if fanout targets > 1).\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_ingress_enqueued_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_ingress_enqueued_total %d\n", ingressEnqueued)

		// --- Delivery metrics ---
		deliveryAttempt := int64(0)
		deliveryAcked := int64(0)
		deliveryRetry := int64(0)
		deliveryDead := int64(0)
		if rm != nil {
			deliveryAttempt = rm.deliveryAttemptTotal.Load()
			deliveryAcked = rm.deliveryAckedTotal.Load()
			deliveryRetry = rm.deliveryRetryTotal.Load()
			deliveryDead = rm.deliveryDeadTotal.Load()
		}
		_, _ = fmt.Fprintf(w, "# HELP hookaido_delivery_attempts_total Total number of push delivery attempts.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_delivery_attempts_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_delivery_attempts_total %d\n", deliveryAttempt)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_delivery_acked_total Total number of push deliveries acknowledged (2xx).\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_delivery_acked_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_delivery_acked_total %d\n", deliveryAcked)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_delivery_retry_total Total number of push deliveries scheduled for retry.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_delivery_retry_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_delivery_retry_total %d\n", deliveryRetry)
		_, _ = fmt.Fprintf(w, "# HELP hookaido_delivery_dead_total Total number of push deliveries moved to dead letter queue.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_delivery_dead_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_delivery_dead_total %d\n", deliveryDead)

		// --- Queue depth (on-scrape from store) ---
		var queueStore queue.Store
		if rm != nil {
			queueStore = rm.queueStore
		}
		if queueStore != nil {
			if stats, err := queueStore.Stats(); err == nil {
				queued := stats.ByState[queue.StateQueued]
				leased := stats.ByState[queue.StateLeased]
				dead := stats.ByState[queue.StateDead]
				_, _ = fmt.Fprintf(w, "# HELP hookaido_queue_depth Current number of items in the queue by state.\n")
				_, _ = fmt.Fprintf(w, "# TYPE hookaido_queue_depth gauge\n")
				_, _ = fmt.Fprintf(w, "hookaido_queue_depth{state=\"queued\"} %d\n", queued)
				_, _ = fmt.Fprintf(w, "hookaido_queue_depth{state=\"leased\"} %d\n", leased)
				_, _ = fmt.Fprintf(w, "hookaido_queue_depth{state=\"dead\"} %d\n", dead)
			}
		}
	})
}
