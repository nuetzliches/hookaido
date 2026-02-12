package app

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	ingressAcceptedTotal    atomic.Int64
	ingressRejectedTotal    atomic.Int64
	ingressEnqueuedTotal    atomic.Int64
	ingressAdaptiveTotal    atomic.Int64
	ingressAdaptiveMu       sync.Mutex
	ingressAdaptiveByReason map[string]int64

	// Delivery counters
	deliveryAttemptTotal atomic.Int64
	deliveryAckedTotal   atomic.Int64
	deliveryRetryTotal   atomic.Int64
	deliveryDeadTotal    atomic.Int64

	// Queue store for on-scrape stats
	queueStore queue.Store
	queueStats struct {
		mu         sync.Mutex
		ttl        time.Duration
		cached     queue.Stats
		cachedAt   time.Time
		cachedOK   bool
		refreshing bool
	}

	pullMu      sync.Mutex
	pullByRoute map[string]*pullRouteMetrics
	pullLeases  map[string]pullLease
	now         func() time.Time
}

type pullRouteMetrics struct {
	dequeueByStatus   map[string]int64
	ackedTotal        int64
	nackedTotal       int64
	ackConflictTotal  int64
	nackConflictTotal int64
	leaseActive       int64
	leaseExpiredTotal int64
}

type pullLease struct {
	route string
	until time.Time
}

type pullRouteSnapshot struct {
	dequeueByStatus   map[string]int64
	ackedTotal        int64
	nackedTotal       int64
	ackConflictTotal  int64
	nackConflictTotal int64
	leaseActive       int64
	leaseExpiredTotal int64
}

func newRuntimeMetrics() *runtimeMetrics {
	m := &runtimeMetrics{
		pullByRoute:             make(map[string]*pullRouteMetrics),
		pullLeases:              make(map[string]pullLease),
		now:                     time.Now,
		ingressAdaptiveByReason: make(map[string]int64),
	}
	m.queueStats.ttl = time.Second
	return m
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

func (m *runtimeMetrics) observeIngressAdaptiveBackpressure(reason string) {
	if m == nil {
		return
	}
	normalized := normalizeAdaptiveBackpressureReason(reason)
	m.ingressAdaptiveTotal.Add(1)
	m.ingressAdaptiveMu.Lock()
	if m.ingressAdaptiveByReason == nil {
		m.ingressAdaptiveByReason = make(map[string]int64)
	}
	m.ingressAdaptiveByReason[normalized]++
	m.ingressAdaptiveMu.Unlock()
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

func (m *runtimeMetrics) queueStatsSnapshot() (queue.Stats, bool) {
	if m == nil || m.queueStore == nil {
		return queue.Stats{}, false
	}

	now := time.Now()
	m.queueStats.mu.Lock()
	if m.queueStats.cachedOK && (m.queueStats.ttl <= 0 || now.Sub(m.queueStats.cachedAt) <= m.queueStats.ttl) {
		stats := m.queueStats.cached
		m.queueStats.mu.Unlock()
		return stats, true
	}

	// Cold-start: no cache yet. Perform one blocking read to seed cache.
	if !m.queueStats.cachedOK {
		if m.queueStats.refreshing {
			m.queueStats.mu.Unlock()
			return queue.Stats{}, false
		}
		m.queueStats.refreshing = true
		m.queueStats.mu.Unlock()
		return m.refreshQueueStatsSync()
	}

	// Stale cache: return stale snapshot and refresh in background.
	if !m.queueStats.refreshing {
		m.queueStats.refreshing = true
		go m.refreshQueueStatsAsync()
	}
	stats := m.queueStats.cached
	m.queueStats.mu.Unlock()
	return stats, true
}

func (m *runtimeMetrics) refreshQueueStatsSync() (queue.Stats, bool) {
	if m == nil || m.queueStore == nil {
		if m != nil {
			m.queueStats.mu.Lock()
			m.queueStats.refreshing = false
			m.queueStats.mu.Unlock()
		}
		return queue.Stats{}, false
	}

	stats, err := m.queueStore.Stats()
	at := time.Now()

	m.queueStats.mu.Lock()
	defer m.queueStats.mu.Unlock()
	m.queueStats.refreshing = false
	if err == nil {
		m.queueStats.cached = stats
		m.queueStats.cachedAt = at
		m.queueStats.cachedOK = true
		return stats, true
	}
	if m.queueStats.cachedOK {
		return m.queueStats.cached, true
	}
	return queue.Stats{}, false
}

func (m *runtimeMetrics) refreshQueueStatsAsync() {
	if m == nil || m.queueStore == nil {
		if m != nil {
			m.queueStats.mu.Lock()
			m.queueStats.refreshing = false
			m.queueStats.mu.Unlock()
		}
		return
	}

	stats, err := m.queueStore.Stats()
	at := time.Now()

	m.queueStats.mu.Lock()
	defer m.queueStats.mu.Unlock()
	m.queueStats.refreshing = false
	if err != nil {
		return
	}
	m.queueStats.cached = stats
	m.queueStats.cachedAt = at
	m.queueStats.cachedOK = true
}

func (m *runtimeMetrics) observePullDequeue(route string, statusCode int, items []queue.Envelope) {
	if m == nil {
		return
	}

	route = normalizePullRoute(route)
	status := pullStatusLabel(statusCode)

	m.pullMu.Lock()
	defer m.pullMu.Unlock()

	m.expirePullLeasesLocked(m.nowLocked())

	metrics := m.pullRouteLocked(route)
	metrics.dequeueByStatus[status]++

	if statusCode != http.StatusOK || len(items) == 0 {
		return
	}
	for _, item := range items {
		if strings.TrimSpace(item.LeaseID) == "" {
			continue
		}
		m.trackPullLeaseLocked(route, item.LeaseID, item.LeaseUntil)
	}
}

func (m *runtimeMetrics) observePullAck(route string, statusCode int, leaseID string, leaseExpired bool) {
	if m == nil {
		return
	}

	route = normalizePullRoute(route)

	m.pullMu.Lock()
	defer m.pullMu.Unlock()

	m.expirePullLeasesLocked(m.nowLocked())

	metrics := m.pullRouteLocked(route)
	switch statusCode {
	case http.StatusNoContent:
		metrics.ackedTotal++
		m.clearPullLeaseLocked(leaseID)
	case http.StatusConflict:
		metrics.ackConflictTotal++
		if leaseExpired {
			metrics.leaseExpiredTotal++
		}
		m.clearPullLeaseLocked(leaseID)
	}
}

func (m *runtimeMetrics) observePullNack(route string, statusCode int, leaseID string, leaseExpired bool) {
	if m == nil {
		return
	}

	route = normalizePullRoute(route)

	m.pullMu.Lock()
	defer m.pullMu.Unlock()

	m.expirePullLeasesLocked(m.nowLocked())

	metrics := m.pullRouteLocked(route)
	switch statusCode {
	case http.StatusNoContent:
		metrics.nackedTotal++
		m.clearPullLeaseLocked(leaseID)
	case http.StatusConflict:
		metrics.nackConflictTotal++
		if leaseExpired {
			metrics.leaseExpiredTotal++
		}
		m.clearPullLeaseLocked(leaseID)
	}
}

func (m *runtimeMetrics) observePullExtend(route string, statusCode int, leaseID string, extendBy time.Duration, leaseExpired bool) {
	if m == nil {
		return
	}

	route = normalizePullRoute(route)

	m.pullMu.Lock()
	defer m.pullMu.Unlock()

	m.expirePullLeasesLocked(m.nowLocked())

	metrics := m.pullRouteLocked(route)
	switch statusCode {
	case http.StatusNoContent:
		if strings.TrimSpace(leaseID) == "" || extendBy <= 0 {
			return
		}
		lease, ok := m.pullLeases[leaseID]
		if !ok {
			return
		}
		if lease.until.IsZero() {
			return
		}
		lease.until = lease.until.Add(extendBy)
		m.pullLeases[leaseID] = lease
	case http.StatusConflict:
		if leaseExpired {
			metrics.leaseExpiredTotal++
		}
		m.clearPullLeaseLocked(leaseID)
	}
}

func (m *runtimeMetrics) pullSnapshot() map[string]pullRouteSnapshot {
	if m == nil {
		return nil
	}

	m.pullMu.Lock()
	defer m.pullMu.Unlock()

	m.expirePullLeasesLocked(m.nowLocked())
	if len(m.pullByRoute) == 0 {
		return nil
	}

	out := make(map[string]pullRouteSnapshot, len(m.pullByRoute))
	for route, metrics := range m.pullByRoute {
		if metrics == nil {
			continue
		}
		byStatus := make(map[string]int64, len(metrics.dequeueByStatus))
		for status, n := range metrics.dequeueByStatus {
			byStatus[status] = n
		}
		out[route] = pullRouteSnapshot{
			dequeueByStatus:   byStatus,
			ackedTotal:        metrics.ackedTotal,
			nackedTotal:       metrics.nackedTotal,
			ackConflictTotal:  metrics.ackConflictTotal,
			nackConflictTotal: metrics.nackConflictTotal,
			leaseActive:       metrics.leaseActive,
			leaseExpiredTotal: metrics.leaseExpiredTotal,
		}
	}
	return out
}

func (m *runtimeMetrics) pullRouteLocked(route string) *pullRouteMetrics {
	if m.pullByRoute == nil {
		m.pullByRoute = make(map[string]*pullRouteMetrics)
	}
	metrics, ok := m.pullByRoute[route]
	if !ok {
		metrics = &pullRouteMetrics{dequeueByStatus: make(map[string]int64)}
		m.pullByRoute[route] = metrics
	}
	return metrics
}

func (m *runtimeMetrics) trackPullLeaseLocked(route string, leaseID string, until time.Time) {
	if strings.TrimSpace(leaseID) == "" {
		return
	}

	if m.pullLeases == nil {
		m.pullLeases = make(map[string]pullLease)
	}

	if existing, ok := m.pullLeases[leaseID]; ok {
		if prev := m.pullByRoute[existing.route]; prev != nil && prev.leaseActive > 0 {
			prev.leaseActive--
		}
	}

	m.pullLeases[leaseID] = pullLease{route: route, until: until}
	m.pullRouteLocked(route).leaseActive++
}

func (m *runtimeMetrics) clearPullLeaseLocked(leaseID string) {
	if strings.TrimSpace(leaseID) == "" {
		return
	}
	lease, ok := m.pullLeases[leaseID]
	if !ok {
		return
	}
	delete(m.pullLeases, leaseID)
	if metrics := m.pullByRoute[lease.route]; metrics != nil && metrics.leaseActive > 0 {
		metrics.leaseActive--
	}
}

func (m *runtimeMetrics) expirePullLeasesLocked(now time.Time) {
	if len(m.pullLeases) == 0 {
		return
	}
	for leaseID, lease := range m.pullLeases {
		if lease.until.IsZero() || now.Before(lease.until) {
			continue
		}
		delete(m.pullLeases, leaseID)
		if metrics := m.pullByRoute[lease.route]; metrics != nil && metrics.leaseActive > 0 {
			metrics.leaseActive--
		}
	}
}

func (m *runtimeMetrics) nowLocked() time.Time {
	if m != nil && m.now != nil {
		return m.now()
	}
	return time.Now()
}

func normalizePullRoute(route string) string {
	route = strings.TrimSpace(route)
	if route == "" {
		return "_unknown"
	}
	return route
}

func pullStatusLabel(statusCode int) string {
	switch {
	case statusCode == http.StatusOK:
		return "200"
	case statusCode == http.StatusNoContent:
		return "204"
	case statusCode >= 400 && statusCode <= 499:
		return "4xx"
	case statusCode >= 500 && statusCode <= 599:
		return "5xx"
	default:
		return strconv.Itoa(statusCode)
	}
}

var pullStatusPreferredOrder = []string{"200", "204", "4xx", "5xx"}

var adaptiveBackpressureReasonOrder = []string{
	"queued_pressure",
	"ready_lag",
	"oldest_queued_age",
	"sustained_growth",
	"unspecified",
	"other",
}

func orderedPullStatuses(byStatus map[string]int64) []string {
	seen := make(map[string]struct{}, len(byStatus))
	out := make([]string, 0, len(byStatus)+len(pullStatusPreferredOrder))
	for _, status := range pullStatusPreferredOrder {
		out = append(out, status)
		seen[status] = struct{}{}
	}
	extra := make([]string, 0, len(byStatus))
	for status := range byStatus {
		if _, ok := seen[status]; ok {
			continue
		}
		extra = append(extra, status)
	}
	sort.Strings(extra)
	out = append(out, extra...)
	return out
}

func sortedRoutes(snapshot map[string]pullRouteSnapshot) []string {
	routes := make([]string, 0, len(snapshot))
	for route := range snapshot {
		routes = append(routes, route)
	}
	sort.Strings(routes)
	return routes
}

func normalizeAdaptiveBackpressureReason(reason string) string {
	switch strings.TrimSpace(reason) {
	case "queued_pressure":
		return "queued_pressure"
	case "ready_lag":
		return "ready_lag"
	case "oldest_queued_age":
		return "oldest_queued_age"
	case "sustained_growth":
		return "sustained_growth"
	case "":
		return "unspecified"
	default:
		return "other"
	}
}

func (m *runtimeMetrics) ingressAdaptiveSnapshot() map[string]int64 {
	out := make(map[string]int64, len(adaptiveBackpressureReasonOrder))
	for _, reason := range adaptiveBackpressureReasonOrder {
		out[reason] = 0
	}
	if m == nil {
		return out
	}
	m.ingressAdaptiveMu.Lock()
	defer m.ingressAdaptiveMu.Unlock()
	for reason, count := range m.ingressAdaptiveByReason {
		out[reason] = count
	}
	return out
}

func pullDiagnostics(snapshot map[string]pullRouteSnapshot) map[string]any {
	total := map[string]any{
		"dequeue_total": map[string]any{
			"200": int64(0),
			"204": int64(0),
			"4xx": int64(0),
			"5xx": int64(0),
		},
		"acked_total":         int64(0),
		"nacked_total":        int64(0),
		"ack_conflict_total":  int64(0),
		"nack_conflict_total": int64(0),
		"lease_active":        int64(0),
		"lease_expired_total": int64(0),
	}
	byRoute := make(map[string]any, len(snapshot))
	if len(snapshot) == 0 {
		return map[string]any{
			"total":    total,
			"by_route": byRoute,
		}
	}

	dequeueTotal := total["dequeue_total"].(map[string]any)
	for _, route := range sortedRoutes(snapshot) {
		metrics := snapshot[route]
		statuses := make(map[string]any, len(metrics.dequeueByStatus))
		for _, status := range orderedPullStatuses(metrics.dequeueByStatus) {
			value := metrics.dequeueByStatus[status]
			statuses[status] = value
			if current, ok := dequeueTotal[status].(int64); ok {
				dequeueTotal[status] = current + value
			} else if _, exists := dequeueTotal[status]; !exists {
				dequeueTotal[status] = value
			}
		}

		byRoute[route] = map[string]any{
			"dequeue_total":       statuses,
			"acked_total":         metrics.ackedTotal,
			"nacked_total":        metrics.nackedTotal,
			"ack_conflict_total":  metrics.ackConflictTotal,
			"nack_conflict_total": metrics.nackConflictTotal,
			"lease_active":        metrics.leaseActive,
			"lease_expired_total": metrics.leaseExpiredTotal,
		}
		total["acked_total"] = total["acked_total"].(int64) + metrics.ackedTotal
		total["nacked_total"] = total["nacked_total"].(int64) + metrics.nackedTotal
		total["ack_conflict_total"] = total["ack_conflict_total"].(int64) + metrics.ackConflictTotal
		total["nack_conflict_total"] = total["nack_conflict_total"].(int64) + metrics.nackConflictTotal
		total["lease_active"] = total["lease_active"].(int64) + metrics.leaseActive
		total["lease_expired_total"] = total["lease_expired_total"].(int64) + metrics.leaseExpiredTotal
	}

	return map[string]any{
		"total":    total,
		"by_route": byRoute,
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
	pullSnapshot := m.pullSnapshot()
	ingressAdaptiveByReason := m.ingressAdaptiveSnapshot()
	ingressAdaptiveByReasonAny := make(map[string]any, len(ingressAdaptiveByReason))
	for reason, count := range ingressAdaptiveByReason {
		ingressAdaptiveByReasonAny[reason] = count
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
			"accepted_total":                      m.ingressAcceptedTotal.Load(),
			"rejected_total":                      m.ingressRejectedTotal.Load(),
			"enqueued_total":                      m.ingressEnqueuedTotal.Load(),
			"adaptive_backpressure_applied_total": m.ingressAdaptiveTotal.Load(),
			"adaptive_backpressure_by_reason":     ingressAdaptiveByReasonAny,
		},
		"delivery": map[string]any{
			"attempt_total": m.deliveryAttemptTotal.Load(),
			"acked_total":   m.deliveryAckedTotal.Load(),
			"retry_total":   m.deliveryRetryTotal.Load(),
			"dead_total":    m.deliveryDeadTotal.Load(),
		},
		"pull": pullDiagnostics(pullSnapshot),
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
		ingressAdaptiveTotal := int64(0)
		ingressAdaptiveByReason := make(map[string]int64, len(adaptiveBackpressureReasonOrder))
		for _, reason := range adaptiveBackpressureReasonOrder {
			ingressAdaptiveByReason[reason] = 0
		}
		var pullSnapshot map[string]pullRouteSnapshot
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
			ingressAdaptiveTotal = rm.ingressAdaptiveTotal.Load()
			ingressAdaptiveByReason = rm.ingressAdaptiveSnapshot()
			pullSnapshot = rm.pullSnapshot()
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
		_, _ = fmt.Fprintf(w, "# HELP hookaido_ingress_adaptive_backpressure_total Total number of ingress requests rejected by adaptive backpressure, partitioned by reason.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_ingress_adaptive_backpressure_total counter\n")
		for _, reason := range adaptiveBackpressureReasonOrder {
			_, _ = fmt.Fprintf(w, "hookaido_ingress_adaptive_backpressure_total{reason=%q} %d\n", reason, ingressAdaptiveByReason[reason])
		}
		_, _ = fmt.Fprintf(w, "# HELP hookaido_ingress_adaptive_backpressure_applied_total Total number of ingress requests rejected by adaptive backpressure.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_ingress_adaptive_backpressure_applied_total counter\n")
		_, _ = fmt.Fprintf(w, "hookaido_ingress_adaptive_backpressure_applied_total %d\n", ingressAdaptiveTotal)

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

		// --- Pull metrics ---
		_, _ = fmt.Fprintf(w, "# HELP hookaido_pull_dequeue_total Total number of Pull dequeue requests by route and status class.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_pull_dequeue_total counter\n")
		_, _ = fmt.Fprintf(w, "# HELP hookaido_pull_acked_total Total number of successful Pull ack operations by route.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_pull_acked_total counter\n")
		_, _ = fmt.Fprintf(w, "# HELP hookaido_pull_nacked_total Total number of successful Pull nack/mark-dead operations by route.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_pull_nacked_total counter\n")
		_, _ = fmt.Fprintf(w, "# HELP hookaido_pull_ack_conflict_total Total number of Pull ack lease conflicts (HTTP 409) by route.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_pull_ack_conflict_total counter\n")
		_, _ = fmt.Fprintf(w, "# HELP hookaido_pull_nack_conflict_total Total number of Pull nack lease conflicts (HTTP 409) by route.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_pull_nack_conflict_total counter\n")
		_, _ = fmt.Fprintf(w, "# HELP hookaido_pull_lease_active Current number of active Pull leases tracked per route.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_pull_lease_active gauge\n")
		_, _ = fmt.Fprintf(w, "# HELP hookaido_pull_lease_expired_total Total number of Pull lease expirations observed during ack/nack/extend operations.\n")
		_, _ = fmt.Fprintf(w, "# TYPE hookaido_pull_lease_expired_total counter\n")
		for _, route := range sortedRoutes(pullSnapshot) {
			metrics := pullSnapshot[route]
			for _, status := range orderedPullStatuses(metrics.dequeueByStatus) {
				_, _ = fmt.Fprintf(
					w,
					"hookaido_pull_dequeue_total{route=%q,status=%q} %d\n",
					route,
					status,
					metrics.dequeueByStatus[status],
				)
			}
			_, _ = fmt.Fprintf(w, "hookaido_pull_acked_total{route=%q} %d\n", route, metrics.ackedTotal)
			_, _ = fmt.Fprintf(w, "hookaido_pull_nacked_total{route=%q} %d\n", route, metrics.nackedTotal)
			_, _ = fmt.Fprintf(w, "hookaido_pull_ack_conflict_total{route=%q} %d\n", route, metrics.ackConflictTotal)
			_, _ = fmt.Fprintf(w, "hookaido_pull_nack_conflict_total{route=%q} %d\n", route, metrics.nackConflictTotal)
			_, _ = fmt.Fprintf(w, "hookaido_pull_lease_active{route=%q} %d\n", route, metrics.leaseActive)
			_, _ = fmt.Fprintf(w, "hookaido_pull_lease_expired_total{route=%q} %d\n", route, metrics.leaseExpiredTotal)
		}

		// --- Queue depth (on-scrape from store) ---
		var queueStore queue.Store
		if rm != nil {
			queueStore = rm.queueStore
		}
		if queueStore != nil {
			if stats, ok := rm.queueStatsSnapshot(); ok {
				queued := stats.ByState[queue.StateQueued]
				leased := stats.ByState[queue.StateLeased]
				dead := stats.ByState[queue.StateDead]
				_, _ = fmt.Fprintf(w, "# HELP hookaido_queue_depth Current number of items in the queue by state.\n")
				_, _ = fmt.Fprintf(w, "# TYPE hookaido_queue_depth gauge\n")
				_, _ = fmt.Fprintf(w, "hookaido_queue_depth{state=\"queued\"} %d\n", queued)
				_, _ = fmt.Fprintf(w, "hookaido_queue_depth{state=\"leased\"} %d\n", leased)
				_, _ = fmt.Fprintf(w, "hookaido_queue_depth{state=\"dead\"} %d\n", dead)
			}

			if provider, ok := queueStore.(queue.RuntimeMetricsProvider); ok {
				if runtime := provider.RuntimeMetrics(); runtime.SQLite != nil {
					sqliteMetrics := runtime.SQLite
					writePrometheusHistogram(
						w,
						"hookaido_store_sqlite_write_seconds",
						"SQLite write transaction duration in seconds.",
						sqliteMetrics.WriteDurationSeconds,
					)
					writePrometheusHistogram(
						w,
						"hookaido_store_sqlite_dequeue_seconds",
						"SQLite dequeue transaction duration in seconds.",
						sqliteMetrics.DequeueDurationSeconds,
					)
					writePrometheusHistogram(
						w,
						"hookaido_store_sqlite_checkpoint_seconds",
						"SQLite WAL checkpoint duration in seconds.",
						sqliteMetrics.CheckpointDurationSeconds,
					)

					_, _ = fmt.Fprintf(w, "# HELP hookaido_store_sqlite_busy_total Total number of SQLite busy/locked errors.\n")
					_, _ = fmt.Fprintf(w, "# TYPE hookaido_store_sqlite_busy_total counter\n")
					_, _ = fmt.Fprintf(w, "hookaido_store_sqlite_busy_total %d\n", sqliteMetrics.BusyTotal)
					_, _ = fmt.Fprintf(w, "# HELP hookaido_store_sqlite_retry_total Total number of SQLite retry attempts after busy/locked errors.\n")
					_, _ = fmt.Fprintf(w, "# TYPE hookaido_store_sqlite_retry_total counter\n")
					_, _ = fmt.Fprintf(w, "hookaido_store_sqlite_retry_total %d\n", sqliteMetrics.RetryTotal)
					_, _ = fmt.Fprintf(w, "# HELP hookaido_store_sqlite_tx_commit_total Total number of committed SQLite transactions in instrumented queue paths.\n")
					_, _ = fmt.Fprintf(w, "# TYPE hookaido_store_sqlite_tx_commit_total counter\n")
					_, _ = fmt.Fprintf(w, "hookaido_store_sqlite_tx_commit_total %d\n", sqliteMetrics.TxCommitTotal)
					_, _ = fmt.Fprintf(w, "# HELP hookaido_store_sqlite_tx_rollback_total Total number of rolled-back SQLite transactions in instrumented queue paths.\n")
					_, _ = fmt.Fprintf(w, "# TYPE hookaido_store_sqlite_tx_rollback_total counter\n")
					_, _ = fmt.Fprintf(w, "hookaido_store_sqlite_tx_rollback_total %d\n", sqliteMetrics.TxRollbackTotal)
					_, _ = fmt.Fprintf(w, "# HELP hookaido_store_sqlite_checkpoint_total Total number of periodic SQLite WAL checkpoints.\n")
					_, _ = fmt.Fprintf(w, "# TYPE hookaido_store_sqlite_checkpoint_total counter\n")
					_, _ = fmt.Fprintf(w, "hookaido_store_sqlite_checkpoint_total %d\n", sqliteMetrics.CheckpointTotal)
					_, _ = fmt.Fprintf(w, "# HELP hookaido_store_sqlite_checkpoint_errors_total Total number of periodic SQLite WAL checkpoint errors.\n")
					_, _ = fmt.Fprintf(w, "# TYPE hookaido_store_sqlite_checkpoint_errors_total counter\n")
					_, _ = fmt.Fprintf(w, "hookaido_store_sqlite_checkpoint_errors_total %d\n", sqliteMetrics.CheckpointErrorTotal)
				}
			}
		}
	})
}

func writePrometheusHistogram(w http.ResponseWriter, name string, help string, snapshot queue.HistogramSnapshot) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	_, _ = fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	for _, bucket := range snapshot.Buckets {
		_, _ = fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", name, prometheusLe(bucket.Le), bucket.Count)
	}
	_, _ = fmt.Fprintf(w, "%s_sum %.9f\n", name, snapshot.Sum)
	_, _ = fmt.Fprintf(w, "%s_count %d\n", name, snapshot.Count)
}

func prometheusLe(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}
