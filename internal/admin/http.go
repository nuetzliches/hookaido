package admin

import (
	"bytes"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/httpheader"
	"github.com/nuetzliches/hookaido/internal/queue"
)

const (
	defaultListLimit          = 100
	maxListLimit              = 1000
	defaultBacklogTrendWindow = time.Hour
	defaultBacklogTrendStep   = 5 * time.Minute
	minBacklogTrendStep       = time.Minute
	maxBacklogTrendStep       = time.Hour
	maxBacklogTrendWindow     = 7 * 24 * time.Hour
	maxBacklogTrendSamples    = 20000
	healthTrendSignalSamples  = 2000

	auditReasonHeader     = "X-Hookaido-Audit-Reason"
	auditActorHeader      = "X-Hookaido-Audit-Actor"
	auditRequestIDHeader  = "X-Request-ID"
	maxAuditReasonLength  = 512
	maxAuditActorLength   = 256
	maxAuditRequestIDSize = 256
	defaultMaxBodyBytes   = 2 << 20 // 2 MiB
	defaultMaxHeaderBytes = 64 << 10

	publishCodeAuditReasonRequired         = "audit_reason_required"
	publishCodeAuditActorRequired          = "audit_actor_required"
	publishCodeAuditActorNotAllowed        = "audit_actor_not_allowed"
	publishCodeAuditRequestIDRequired      = "audit_request_id_required"
	publishCodeInvalidBody                 = "invalid_body"
	publishCodeManagedResolverMissing      = "managed_resolver_missing"
	publishCodeManagedEndpointNotFound     = "managed_endpoint_not_found"
	publishCodeRouteMismatch               = "route_mismatch"
	publishCodeManagedSelectorRequired     = "managed_selector_required"
	publishCodeRouteResolverMissing        = "route_resolver_missing"
	publishCodeManagedTargetMismatch       = "managed_target_mismatch"
	publishCodeRouteNotFound               = "route_not_found"
	publishCodeTargetUnresolvable          = "target_unresolvable"
	publishCodeInvalidReceivedAt           = "invalid_received_at"
	publishCodeInvalidNextRunAt            = "invalid_next_run_at"
	publishCodeInvalidPayload              = "invalid_payload_b64"
	publishCodeInvalidHeader               = "invalid_header"
	publishCodePayloadTooLarge             = "payload_too_large"
	publishCodeHeadersTooLarge             = "headers_too_large"
	publishCodeDuplicateID                 = "duplicate_id"
	publishCodeQueueFull                   = "queue_full"
	publishCodeStoreUnavailable            = "store_unavailable"
	publishCodeScopedSelectorMismatch      = "selector_scope_mismatch"
	publishCodeScopedSelectorForbidden     = "selector_scope_forbidden"
	publishCodeManagedEndpointNoTargets    = "managed_endpoint_no_targets"
	publishCodeScopedPublishRequired       = "scoped_publish_required"
	publishCodeGlobalPublishDisabled       = "global_publish_disabled"
	publishCodeScopedPublishDisabled       = "scoped_publish_disabled"
	publishCodePullRoutePublishDisabled    = "pull_route_publish_disabled"
	publishCodeDeliverRoutePublishDisabled = "deliver_route_publish_disabled"
	publishCodeRoutePublishDisabled        = "route_publish_disabled"

	managementCodeUnavailable      = "management_unavailable"
	managementCodeRouteNotFound    = "management_route_not_found"
	managementCodeEndpointNotFound = "management_endpoint_not_found"
	managementCodeRouteMapped      = "management_route_already_mapped"
	managementCodeRoutePublishOff  = "management_route_publish_disabled"
	managementCodeRouteTargetDrift = "management_route_target_mismatch"
	managementCodeRouteBacklog     = "management_route_backlog_active"
	managementCodeConflict         = "management_conflict"
	readCodeUnauthorized           = "unauthorized"
	readCodeMethodNotAllowed       = "method_not_allowed"
	readCodeInvalidQuery           = "invalid_query"
	readCodeNotFound               = "not_found"
	readCodeBacklogUnavailable     = "backlog_unavailable"
	readCodeManagementUnavailable  = "management_model_unavailable"
)

var defaultBacklogSummaryStates = []queue.State{
	queue.StateQueued,
	queue.StateLeased,
	queue.StateDead,
}

type Authorizer func(r *http.Request) bool

var (
	ErrManagementRouteNotFound    = errors.New("management route not found")
	ErrManagementEndpointNotFound = errors.New("management endpoint not found")
	ErrManagementConflict         = errors.New("management conflict")
	errManagedPolicyLookupMissing = errors.New("managed policy lookup is unavailable")
	errRequestBodyTooLarge        = errors.New("request body too large")
)

type managedOwnershipMismatchError struct {
	detail string
}

func (e *managedOwnershipMismatchError) Error() string {
	if e == nil || strings.TrimSpace(e.detail) == "" {
		return "managed route ownership mapping is out of sync"
	}
	return strings.TrimSpace(e.detail)
}

func BearerTokenAuthorizer(tokens [][]byte) Authorizer {
	allowed := make([][]byte, 0, len(tokens))
	for _, t := range tokens {
		if len(t) == 0 {
			continue
		}
		cp := make([]byte, len(t))
		copy(cp, t)
		allowed = append(allowed, cp)
	}

	return func(r *http.Request) bool {
		if len(allowed) == 0 {
			return true
		}

		h := r.Header.Get("Authorization")
		if h == "" {
			return false
		}
		const prefix = "Bearer "
		if !strings.HasPrefix(h, prefix) {
			return false
		}
		got := strings.TrimSpace(strings.TrimPrefix(h, prefix))
		if got == "" {
			return false
		}
		gb := []byte(got)
		for _, want := range allowed {
			if subtle.ConstantTimeCompare(gb, want) == 1 {
				return true
			}
		}
		return false
	}
}

type Server struct {
	Store                              queue.Store
	Authorize                          Authorizer
	HealthDiagnostics                  func() map[string]any
	ResolveTrendSignalConfig           func() queue.BacklogTrendSignalConfig
	ResolveManaged                     func(application, endpointName string) (route string, targets []string, ok bool)
	ManagedRouteInfoForRoute           func(route string) (application, endpointName string, managed bool, available bool)
	ManagedRouteSet                    func() (routes map[string]struct{}, available bool)
	TargetsForRoute                    func(route string) []string
	ModeForRoute                       func(route string) string
	PublishEnabledForRoute             func(route string) bool
	PublishDirectEnabledForRoute       func(route string) bool
	PublishManagedEnabledForRoute      func(route string) bool
	LimitsForRoute                     func(route string) (maxBodyBytes int64, maxHeaderBytes int)
	ManagementModel                    func() ManagementModel
	RequireManagementAuditReason       bool
	MaxBodyBytes                       int64
	MaxHeaderBytes                     int
	PublishGlobalDirectEnabled         bool
	PublishScopedManagedEnabled        bool
	PublishAllowPullRoutes             bool
	PublishAllowDeliverRoutes          bool
	PublishRequireAuditActor           bool
	PublishRequireAuditRequestID       bool
	PublishScopedManagedFailClosed     bool
	PublishScopedManagedActorAllowlist []string
	PublishScopedManagedActorPrefixes  []string
	AuditManagementMutation            func(event ManagementMutationAuditEvent)
	ObservePublishResult               func(event PublishResultEvent)
	UpsertManagedEndpoint              func(req ManagementEndpointUpsertRequest) (ManagementEndpointMutationResult, error)
	DeleteManagedEndpoint              func(req ManagementEndpointDeleteRequest) (ManagementEndpointMutationResult, error)
}

type PublishResultEvent struct {
	Accepted int
	Rejected int
	Code     string
	Scoped   bool
}

func NewServer(store queue.Store) *Server {
	return &Server{
		Store:                          store,
		RequireManagementAuditReason:   true,
		MaxBodyBytes:                   defaultMaxBodyBytes,
		MaxHeaderBytes:                 defaultMaxHeaderBytes,
		PublishGlobalDirectEnabled:     true,
		PublishScopedManagedEnabled:    true,
		PublishAllowPullRoutes:         true,
		PublishAllowDeliverRoutes:      true,
		PublishRequireAuditActor:       false,
		PublishRequireAuditRequestID:   false,
		PublishScopedManagedFailClosed: false,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Authorize != nil && !s.Authorize(r) {
		writeManagementError(w, http.StatusUnauthorized, readCodeUnauthorized, "request is not authorized")
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if strings.HasPrefix(cleanPath, "/applications/") {
		s.handleApplicationResource(w, r, cleanPath)
		return
	}

	switch cleanPath {
	case "/healthz":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleHealthz(w, r)
		return
	case "/dlq":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleDLQ(w, r)
		return
	case "/backlog/top_queued":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleBacklogTopQueued(w, r)
		return
	case "/backlog/oldest_queued":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleBacklogOldestQueued(w, r)
		return
	case "/backlog/aging_summary":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleBacklogAgingSummary(w, r)
		return
	case "/backlog/trends":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleBacklogTrends(w, r)
		return
	case "/messages":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleMessages(w, r)
		return
	case "/messages/publish":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesPublish(w, r)
		return
	case "/dlq/requeue":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleDLQRequeue(w, r)
		return
	case "/dlq/delete":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleDLQDelete(w, r)
		return
	case "/messages/cancel":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesCancel(w, r)
		return
	case "/messages/cancel_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesCancelByFilter(w, r)
		return
	case "/messages/requeue":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesRequeue(w, r)
		return
	case "/messages/resume":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesResume(w, r)
		return
	case "/messages/requeue_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesRequeueByFilter(w, r)
		return
	case "/messages/resume_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesResumeByFilter(w, r)
		return
	case "/attempts":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleAttempts(w, r)
		return
	case "/management/model":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleManagementModel(w, r)
		return
	case "/applications":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleApplications(w, r)
		return
	default:
		writeManagementError(w, http.StatusNotFound, readCodeNotFound, "resource was not found")
		return
	}
}

func (s *Server) handleApplicationResource(w http.ResponseWriter, r *http.Request, cleanPath string) {
	application, endpointName, resource, ok := parseApplicationResourcePath(cleanPath)
	if !ok {
		writeManagementError(w, http.StatusNotFound, readCodeNotFound, "resource was not found")
		return
	}
	if !config.IsValidManagementLabel(application) {
		writeManagementError(
			w,
			http.StatusBadRequest,
			publishCodeInvalidBody,
			fmt.Sprintf("application path segment must match %s", config.ManagementLabelPattern()),
		)
		return
	}
	if endpointName != "" && !config.IsValidManagementLabel(endpointName) {
		writeManagementError(
			w,
			http.StatusBadRequest,
			publishCodeInvalidBody,
			fmt.Sprintf("endpoint_name path segment must match %s", config.ManagementLabelPattern()),
		)
		return
	}

	switch resource {
	case "endpoints":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleApplicationEndpoints(w, application)
		return
	case "endpoint":
		switch r.Method {
		case http.MethodGet:
			s.handleApplicationEndpoint(w, application, endpointName)
			return
		case http.MethodPut:
			s.handleApplicationEndpointUpsert(w, r, application, endpointName)
			return
		case http.MethodDelete:
			s.handleApplicationEndpointDelete(w, r, application, endpointName)
			return
		default:
			writeMethodNotAllowed(w, "GET|PUT|DELETE")
			return
		}
	case "messages":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleApplicationEndpointMessages(w, r, application, endpointName)
		return
	case "messages_publish":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleApplicationEndpointPublish(w, r, application, endpointName)
		return
	case "messages_cancel_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleApplicationEndpointCancelByFilter(w, r, application, endpointName)
		return
	case "messages_requeue_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleApplicationEndpointRequeueByFilter(w, r, application, endpointName)
		return
	case "messages_resume_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleApplicationEndpointResumeByFilter(w, r, application, endpointName)
		return
	default:
		writeManagementError(w, http.StatusNotFound, readCodeNotFound, "resource was not found")
		return
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	detailsRaw := strings.TrimSpace(r.URL.Query().Get("details"))
	details, ok := parseBoolParam(detailsRaw)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "details must be true|false")
		return
	}
	if !details {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}

	diagnostics := map[string]any{}
	if s.HealthDiagnostics != nil {
		if v := s.HealthDiagnostics(); v != nil {
			diagnostics = v
		}
	}
	if s.Store != nil {
		diagnostics["queue"] = queueHealthDiagnostics(s.Store, s.trendSignalConfig())
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":          true,
		"time":        time.Now().UTC().Format(time.RFC3339Nano),
		"diagnostics": diagnostics,
	})
}

func queueHealthDiagnostics(store queue.Store, signalCfg queue.BacklogTrendSignalConfig) map[string]any {
	stats, err := store.Stats()
	if err != nil {
		return map[string]any{
			"ok":    false,
			"error": err.Error(),
		}
	}

	byState := make(map[string]int, len(stats.ByState))
	for state, n := range stats.ByState {
		byState[string(state)] = n
	}
	out := map[string]any{
		"ok":       true,
		"total":    stats.Total,
		"by_state": byState,
	}
	if !stats.OldestQueuedReceivedAt.IsZero() {
		out["oldest_queued_received_at"] = stats.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
		out["oldest_queued_age_seconds"] = int(stats.OldestQueuedAge / time.Second)
	}
	if !stats.EarliestQueuedNextRun.IsZero() {
		out["earliest_queued_next_run_at"] = stats.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
		out["ready_lag_seconds"] = int(stats.ReadyLag / time.Second)
	}
	if len(stats.TopQueued) > 0 {
		topQueued := make([]map[string]any, 0, len(stats.TopQueued))
		for _, b := range stats.TopQueued {
			item := map[string]any{
				"route":  b.Route,
				"target": b.Target,
				"queued": b.Queued,
			}
			if !b.OldestQueuedReceivedAt.IsZero() {
				item["oldest_queued_received_at"] = b.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
				item["oldest_queued_age_seconds"] = int(b.OldestQueuedAge / time.Second)
			}
			if !b.EarliestQueuedNextRun.IsZero() {
				item["earliest_queued_next_run_at"] = b.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
				item["ready_lag_seconds"] = int(b.ReadyLag / time.Second)
			}
			topQueued = append(topQueued, item)
		}
		out["top_queued"] = topQueued
	}
	if trendStore, ok := store.(queue.BacklogTrendStore); ok {
		now := time.Now().UTC()
		resp, err := trendStore.ListBacklogTrend(queue.BacklogTrendListRequest{
			Since: now.Add(-signalCfg.Window),
			Until: now,
			Limit: healthTrendSignalSamples,
		})
		if err != nil {
			out["trend_signals"] = map[string]any{
				"status": "error",
				"error":  err.Error(),
			}
		} else {
			signals := queue.AnalyzeBacklogTrendSignals(resp.Items, resp.Truncated, queue.BacklogTrendSignalOptions{
				Now:    now,
				Config: signalCfg,
			})
			out["trend_signals"] = signals.Map()
		}
	}
	return out
}

func backlogTopQueuedFromStats(stats queue.Stats, routeFilter, targetFilter string, limit int) backlogTopQueuedResponse {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	queuedTotal := stats.ByState[queue.StateQueued]
	sourceQueued := 0
	for _, b := range stats.TopQueued {
		sourceQueued += b.Queued
	}

	filtered := make([]queue.BacklogBucket, 0, len(stats.TopQueued))
	for _, b := range stats.TopQueued {
		if routeFilter != "" && b.Route != routeFilter {
			continue
		}
		if targetFilter != "" && b.Target != targetFilter {
			continue
		}
		filtered = append(filtered, b)
	}
	truncated := len(filtered) > limit
	if truncated {
		filtered = filtered[:limit]
	}

	items := make([]backlogTopQueuedItem, 0, len(filtered))
	for _, b := range filtered {
		item := backlogTopQueuedItem{
			Route:  b.Route,
			Target: b.Target,
			Queued: b.Queued,
		}
		if !b.OldestQueuedReceivedAt.IsZero() {
			item.OldestQueuedReceivedAt = b.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
			item.OldestQueuedAgeSeconds = int(b.OldestQueuedAge / time.Second)
		}
		if !b.EarliestQueuedNextRun.IsZero() {
			item.EarliestQueuedNextRun = b.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
			item.ReadyLagSeconds = int(b.ReadyLag / time.Second)
		}
		items = append(items, item)
	}

	out := backlogTopQueuedResponse{
		QueueTotal:    stats.Total,
		QueuedTotal:   queuedTotal,
		Limit:         limit,
		Truncated:     truncated,
		SourceBounded: sourceQueued < queuedTotal,
		Items:         items,
	}
	if !stats.OldestQueuedReceivedAt.IsZero() {
		out.OldestQueuedReceivedAt = stats.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
		out.OldestQueuedAgeSeconds = int(stats.OldestQueuedAge / time.Second)
	}
	if !stats.EarliestQueuedNextRun.IsZero() {
		out.EarliestQueuedNextRunAt = stats.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
		out.ReadyLagSeconds = int(stats.ReadyLag / time.Second)
	}
	return out
}

func backlogReferenceNow(stats queue.Stats) time.Time {
	if !stats.OldestQueuedReceivedAt.IsZero() && stats.OldestQueuedAge > 0 {
		return stats.OldestQueuedReceivedAt.Add(stats.OldestQueuedAge).UTC()
	}
	if !stats.EarliestQueuedNextRun.IsZero() && stats.ReadyLag > 0 {
		return stats.EarliestQueuedNextRun.Add(stats.ReadyLag).UTC()
	}
	return time.Now().UTC()
}

func backlogOldestQueuedFromMessages(now time.Time, stats queue.Stats, routeFilter, targetFilter string, limit int, truncated bool, items []queue.Envelope) backlogOldestQueuedResponse {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	converted := make([]backlogOldestQueuedItem, 0, len(items))
	for _, it := range items {
		item := backlogOldestQueuedItem{
			ID:      it.ID,
			Route:   it.Route,
			Target:  it.Target,
			Attempt: it.Attempt,
		}
		if !it.ReceivedAt.IsZero() {
			item.ReceivedAt = it.ReceivedAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(it.ReceivedAt) {
				item.OldestQueuedAgeSeconds = int(now.Sub(it.ReceivedAt) / time.Second)
			}
		}
		if !it.NextRunAt.IsZero() {
			item.NextRunAt = it.NextRunAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(it.NextRunAt) {
				item.ReadyLagSeconds = int(now.Sub(it.NextRunAt) / time.Second)
			}
		}
		converted = append(converted, item)
	}

	out := backlogOldestQueuedResponse{
		QueueTotal:  stats.Total,
		QueuedTotal: stats.ByState[queue.StateQueued],
		Limit:       limit,
		Returned:    len(converted),
		Truncated:   truncated,
		Selector: backlogSelector{
			Route:  routeFilter,
			Target: targetFilter,
		},
		Items: converted,
	}
	if !stats.OldestQueuedReceivedAt.IsZero() {
		out.OldestQueuedReceivedAt = stats.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
		out.OldestQueuedAgeSeconds = int(stats.OldestQueuedAge / time.Second)
	}
	if !stats.EarliestQueuedNextRun.IsZero() {
		out.EarliestQueuedNextRunAt = stats.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
		out.ReadyLagSeconds = int(stats.ReadyLag / time.Second)
	}
	return out
}

func backlogAgingSummaryFromMessages(now time.Time, stats queue.Stats, routeFilter, targetFilter string, states []queue.State, limit int, truncated bool, stateScan map[queue.State]backlogStateScanSummary, items []queue.Envelope) backlogAgingSummaryResponse {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	type agg struct {
		Route             string
		Target            string
		StateCounts       map[queue.State]int
		TotalObserved     int
		AgeSamples        []int
		AgeWindows        backlogAgeWindows
		OldestReceivedAt  time.Time
		NewestReceivedAt  time.Time
		EarliestNextRun   time.Time
		ReadyOverdueCount int
		MaxReadyLag       time.Duration
	}

	buckets := make(map[string]*agg)
	allAgeSamples := make([]int, 0, len(items))
	var allAgeWindows backlogAgeWindows
	for _, it := range items {
		key := it.Route + "\x00" + it.Target
		a := buckets[key]
		if a == nil {
			a = &agg{
				Route:            it.Route,
				Target:           it.Target,
				StateCounts:      make(map[queue.State]int),
				OldestReceivedAt: it.ReceivedAt,
				NewestReceivedAt: it.ReceivedAt,
				EarliestNextRun:  it.NextRunAt,
			}
			buckets[key] = a
		}
		a.StateCounts[it.State]++
		a.TotalObserved++
		if a.OldestReceivedAt.IsZero() || (!it.ReceivedAt.IsZero() && it.ReceivedAt.Before(a.OldestReceivedAt)) {
			a.OldestReceivedAt = it.ReceivedAt
		}
		if a.NewestReceivedAt.IsZero() || (!it.ReceivedAt.IsZero() && it.ReceivedAt.After(a.NewestReceivedAt)) {
			a.NewestReceivedAt = it.ReceivedAt
		}
		if !it.NextRunAt.IsZero() && (a.EarliestNextRun.IsZero() || it.NextRunAt.Before(a.EarliestNextRun)) {
			a.EarliestNextRun = it.NextRunAt
		}
		if it.State == queue.StateQueued && !it.NextRunAt.IsZero() && now.After(it.NextRunAt) {
			a.ReadyOverdueCount++
			lag := now.Sub(it.NextRunAt)
			if lag > a.MaxReadyLag {
				a.MaxReadyLag = lag
			}
		}
		if !it.ReceivedAt.IsZero() && !now.Before(it.ReceivedAt) {
			ageSeconds := int(now.Sub(it.ReceivedAt) / time.Second)
			a.AgeSamples = append(a.AgeSamples, ageSeconds)
			observeBacklogAgeWindow(ageSeconds, &a.AgeWindows)
			allAgeSamples = append(allAgeSamples, ageSeconds)
			observeBacklogAgeWindow(ageSeconds, &allAgeWindows)
		}
	}

	ordered := make([]*agg, 0, len(buckets))
	for _, a := range buckets {
		ordered = append(ordered, a)
	}
	sort.Slice(ordered, func(i, j int) bool {
		ai := ordered[i]
		aj := ordered[j]
		if ai.OldestReceivedAt.Equal(aj.OldestReceivedAt) {
			if ai.Route == aj.Route {
				return ai.Target < aj.Target
			}
			return ai.Route < aj.Route
		}
		if ai.OldestReceivedAt.IsZero() {
			return false
		}
		if aj.OldestReceivedAt.IsZero() {
			return true
		}
		return ai.OldestReceivedAt.Before(aj.OldestReceivedAt)
	})

	outItems := make([]backlogAgingSummaryItem, 0, len(ordered))
	for _, a := range ordered {
		item := backlogAgingSummaryItem{
			Route:          a.Route,
			Target:         a.Target,
			TotalObserved:  a.TotalObserved,
			QueuedObserved: a.StateCounts[queue.StateQueued],
			LeasedObserved: a.StateCounts[queue.StateLeased],
			DeadObserved:   a.StateCounts[queue.StateDead],
			AgeSampleCount: len(a.AgeSamples),
			AgeWindows:     a.AgeWindows,
			StateCounts: map[string]int{
				string(queue.StateQueued): a.StateCounts[queue.StateQueued],
				string(queue.StateLeased): a.StateCounts[queue.StateLeased],
				string(queue.StateDead):   a.StateCounts[queue.StateDead],
			},
			ReadyOverdueCount: a.ReadyOverdueCount,
		}
		if p, ok := backlogAgePercentilesFromSamples(a.AgeSamples); ok {
			item.AgePercentilesSeconds = &p
		}
		if !a.OldestReceivedAt.IsZero() {
			item.OldestQueuedReceivedAt = a.OldestReceivedAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(a.OldestReceivedAt) {
				item.OldestQueuedAgeSeconds = int(now.Sub(a.OldestReceivedAt) / time.Second)
			}
		}
		if !a.NewestReceivedAt.IsZero() {
			item.NewestQueuedReceivedAt = a.NewestReceivedAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(a.NewestReceivedAt) {
				item.NewestQueuedAgeSeconds = int(now.Sub(a.NewestReceivedAt) / time.Second)
			}
		}
		if !a.EarliestNextRun.IsZero() {
			item.EarliestQueuedNextRunAt = a.EarliestNextRun.UTC().Format(time.RFC3339Nano)
		}
		if a.MaxReadyLag > 0 {
			item.MaxReadyLagSeconds = int(a.MaxReadyLag / time.Second)
		}
		outItems = append(outItems, item)
	}

	outStateScan := make(map[string]backlogStateScanSummary, len(states))
	scannedTotal := 0
	for _, st := range states {
		scan := stateScan[st]
		outStateScan[string(st)] = scan
		scannedTotal += scan.Scanned
	}

	sourceBounded := false
	if routeFilter == "" && targetFilter == "" {
		for _, st := range states {
			if stats.ByState[st] > stateScan[st].Scanned {
				sourceBounded = true
				break
			}
		}
	}
	if truncated {
		sourceBounded = true
	}

	stateNames := make([]string, 0, len(states))
	for _, st := range states {
		stateNames = append(stateNames, string(st))
	}

	out := backlogAgingSummaryResponse{
		QueueTotal:     stats.Total,
		QueuedTotal:    stats.ByState[queue.StateQueued],
		Limit:          limit,
		States:         stateNames,
		AgeSampleCount: len(allAgeSamples),
		AgeWindows:     allAgeWindows,
		Scanned:        scannedTotal,
		BucketCount:    len(outItems),
		Truncated:      truncated,
		SourceBounded:  sourceBounded,
		StateScan:      outStateScan,
		Selector: backlogSelector{
			Route:  routeFilter,
			Target: targetFilter,
		},
		Items: outItems,
	}
	if p, ok := backlogAgePercentilesFromSamples(allAgeSamples); ok {
		out.AgePercentilesSeconds = &p
	}
	if !stats.OldestQueuedReceivedAt.IsZero() {
		out.OldestQueuedReceivedAt = stats.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
		out.OldestQueuedAgeSeconds = int(stats.OldestQueuedAge / time.Second)
	}
	if !stats.EarliestQueuedNextRun.IsZero() {
		out.EarliestQueuedNextRunAt = stats.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
		out.ReadyLagSeconds = int(stats.ReadyLag / time.Second)
	}
	return out
}

func backlogTrendsFromSamples(since, until time.Time, window, step time.Duration, routeFilter, targetFilter string, truncated bool, samples []queue.BacklogTrendSample, signalCfg queue.BacklogTrendSignalConfig) backlogTrendsResponse {
	if step <= 0 {
		step = defaultBacklogTrendStep
	}
	if window <= 0 {
		window = defaultBacklogTrendWindow
	}
	if !until.After(since) {
		until = since.Add(step)
		window = until.Sub(since)
	}

	bucketCount := int(window / step)
	if window%step != 0 {
		bucketCount++
	}
	if bucketCount <= 0 {
		bucketCount = 1
	}

	points := make([]backlogTrendPoint, 0, bucketCount)
	for i := 0; i < bucketCount; i++ {
		bucketStart := since.Add(time.Duration(i) * step)
		bucketEnd := bucketStart.Add(step)
		if bucketEnd.After(until) {
			bucketEnd = until
		}
		points = append(points, backlogTrendPoint{
			BucketStart: bucketStart.UTC().Format(time.RFC3339Nano),
			BucketEnd:   bucketEnd.UTC().Format(time.RFC3339Nano),
		})
	}

	latestTotal := 0
	maxTotal := 0
	latestCapturedAt := time.Time{}
	sampleCount := 0
	nonEmptyPoints := 0

	for _, sample := range samples {
		at := sample.CapturedAt.UTC()
		if at.Before(since) || !at.Before(until) {
			continue
		}
		idx := int(at.Sub(since) / step)
		if idx < 0 || idx >= len(points) {
			continue
		}
		p := &points[idx]
		p.SampleCount++
		if p.SampleCount == 1 {
			nonEmptyPoints++
		}
		if at.After(p.lastCapturedAt) {
			p.lastCapturedAt = at
			p.LastCapturedAt = at.Format(time.RFC3339Nano)
			p.QueuedLast = sample.Queued
			p.LeasedLast = sample.Leased
			p.DeadLast = sample.Dead
			p.TotalLast = sample.Queued + sample.Leased + sample.Dead
		}
		if sample.Queued > p.QueuedMax {
			p.QueuedMax = sample.Queued
		}
		if sample.Leased > p.LeasedMax {
			p.LeasedMax = sample.Leased
		}
		if sample.Dead > p.DeadMax {
			p.DeadMax = sample.Dead
		}
		total := sample.Queued + sample.Leased + sample.Dead
		if total > p.TotalMax {
			p.TotalMax = total
		}
		if total > maxTotal {
			maxTotal = total
		}
		if at.After(latestCapturedAt) {
			latestCapturedAt = at
			latestTotal = total
		}
		sampleCount++
	}

	for i := range points {
		points[i].lastCapturedAt = time.Time{}
	}

	out := backlogTrendsResponse{
		Window:             window.String(),
		Step:               step.String(),
		Since:              since.UTC().Format(time.RFC3339Nano),
		Until:              until.UTC().Format(time.RFC3339Nano),
		SampleCount:        sampleCount,
		PointCount:         len(points),
		NonEmptyPointCount: nonEmptyPoints,
		Truncated:          truncated,
		LatestTotal:        latestTotal,
		MaxTotal:           maxTotal,
		Selector: backlogSelector{
			Route:  routeFilter,
			Target: targetFilter,
		},
		Signals: queue.AnalyzeBacklogTrendSignals(samples, truncated, queue.BacklogTrendSignalOptions{
			Now:    until,
			Window: window,
			Config: signalCfg,
		}).Map(),
		Items: points,
	}
	if !latestCapturedAt.IsZero() {
		out.LatestCapturedAt = latestCapturedAt.Format(time.RFC3339Nano)
	}
	return out
}

type dlqResponse struct {
	Items []dlqItem `json:"items"`
}

type backlogTopQueuedResponse struct {
	QueueTotal              int                    `json:"queue_total"`
	QueuedTotal             int                    `json:"queued_total"`
	Limit                   int                    `json:"limit"`
	Truncated               bool                   `json:"truncated"`
	SourceBounded           bool                   `json:"source_bounded"`
	OldestQueuedReceivedAt  string                 `json:"oldest_queued_received_at,omitempty"`
	OldestQueuedAgeSeconds  int                    `json:"oldest_queued_age_seconds,omitempty"`
	EarliestQueuedNextRunAt string                 `json:"earliest_queued_next_run_at,omitempty"`
	ReadyLagSeconds         int                    `json:"ready_lag_seconds,omitempty"`
	Items                   []backlogTopQueuedItem `json:"items"`
}

type backlogTopQueuedItem struct {
	Route                  string `json:"route"`
	Target                 string `json:"target"`
	Queued                 int    `json:"queued"`
	OldestQueuedReceivedAt string `json:"oldest_queued_received_at,omitempty"`
	OldestQueuedAgeSeconds int    `json:"oldest_queued_age_seconds,omitempty"`
	EarliestQueuedNextRun  string `json:"earliest_queued_next_run_at,omitempty"`
	ReadyLagSeconds        int    `json:"ready_lag_seconds,omitempty"`
}

type backlogOldestQueuedResponse struct {
	QueueTotal              int                       `json:"queue_total"`
	QueuedTotal             int                       `json:"queued_total"`
	Limit                   int                       `json:"limit"`
	Returned                int                       `json:"returned"`
	Truncated               bool                      `json:"truncated"`
	OldestQueuedReceivedAt  string                    `json:"oldest_queued_received_at,omitempty"`
	OldestQueuedAgeSeconds  int                       `json:"oldest_queued_age_seconds,omitempty"`
	EarliestQueuedNextRunAt string                    `json:"earliest_queued_next_run_at,omitempty"`
	ReadyLagSeconds         int                       `json:"ready_lag_seconds,omitempty"`
	Selector                backlogSelector           `json:"selector"`
	Items                   []backlogOldestQueuedItem `json:"items"`
}

type backlogSelector struct {
	Route  string `json:"route,omitempty"`
	Target string `json:"target,omitempty"`
}

type backlogOldestQueuedItem struct {
	ID                     string `json:"id"`
	Route                  string `json:"route"`
	Target                 string `json:"target"`
	ReceivedAt             string `json:"received_at,omitempty"`
	Attempt                int    `json:"attempt"`
	NextRunAt              string `json:"next_run_at,omitempty"`
	OldestQueuedAgeSeconds int    `json:"oldest_queued_age_seconds,omitempty"`
	ReadyLagSeconds        int    `json:"ready_lag_seconds,omitempty"`
}

type backlogTrendsResponse struct {
	Window             string              `json:"window"`
	Step               string              `json:"step"`
	Since              string              `json:"since"`
	Until              string              `json:"until"`
	SampleCount        int                 `json:"sample_count"`
	PointCount         int                 `json:"point_count"`
	NonEmptyPointCount int                 `json:"non_empty_point_count"`
	Truncated          bool                `json:"truncated"`
	LatestCapturedAt   string              `json:"latest_captured_at,omitempty"`
	LatestTotal        int                 `json:"latest_total"`
	MaxTotal           int                 `json:"max_total"`
	Selector           backlogSelector     `json:"selector"`
	Signals            map[string]any      `json:"signals"`
	Items              []backlogTrendPoint `json:"items"`
}

type backlogTrendPoint struct {
	BucketStart    string `json:"bucket_start"`
	BucketEnd      string `json:"bucket_end"`
	SampleCount    int    `json:"sample_count"`
	LastCapturedAt string `json:"last_captured_at,omitempty"`
	QueuedLast     int    `json:"queued_last"`
	LeasedLast     int    `json:"leased_last"`
	DeadLast       int    `json:"dead_last"`
	TotalLast      int    `json:"total_last"`
	QueuedMax      int    `json:"queued_max"`
	LeasedMax      int    `json:"leased_max"`
	DeadMax        int    `json:"dead_max"`
	TotalMax       int    `json:"total_max"`

	lastCapturedAt time.Time
}

type backlogAgingSummaryResponse struct {
	QueueTotal              int                                `json:"queue_total"`
	QueuedTotal             int                                `json:"queued_total"`
	Limit                   int                                `json:"limit"`
	States                  []string                           `json:"states"`
	AgeSampleCount          int                                `json:"age_sample_count"`
	AgeWindows              backlogAgeWindows                  `json:"age_windows"`
	AgePercentilesSeconds   *backlogAgePercentiles             `json:"age_percentiles_seconds,omitempty"`
	Scanned                 int                                `json:"scanned"`
	BucketCount             int                                `json:"bucket_count"`
	Truncated               bool                               `json:"truncated"`
	SourceBounded           bool                               `json:"source_bounded"`
	StateScan               map[string]backlogStateScanSummary `json:"state_scan"`
	OldestQueuedReceivedAt  string                             `json:"oldest_queued_received_at,omitempty"`
	OldestQueuedAgeSeconds  int                                `json:"oldest_queued_age_seconds,omitempty"`
	EarliestQueuedNextRunAt string                             `json:"earliest_queued_next_run_at,omitempty"`
	ReadyLagSeconds         int                                `json:"ready_lag_seconds,omitempty"`
	Selector                backlogSelector                    `json:"selector"`
	Items                   []backlogAgingSummaryItem          `json:"items"`
}

type backlogStateScanSummary struct {
	Scanned   int  `json:"scanned"`
	Truncated bool `json:"truncated"`
}

type backlogAgePercentiles struct {
	P50 int `json:"p50"`
	P90 int `json:"p90"`
	P99 int `json:"p99"`
}

type backlogAgeWindows struct {
	LE5M      int `json:"le_5m"`
	GT5MLE15M int `json:"gt_5m_le_15m"`
	GT15MLE1H int `json:"gt_15m_le_1h"`
	GT1HLE6H  int `json:"gt_1h_le_6h"`
	GT6H      int `json:"gt_6h"`
}

type backlogAgingSummaryItem struct {
	Route                   string                 `json:"route"`
	Target                  string                 `json:"target"`
	TotalObserved           int                    `json:"total_observed"`
	QueuedObserved          int                    `json:"queued_observed"`
	LeasedObserved          int                    `json:"leased_observed"`
	DeadObserved            int                    `json:"dead_observed"`
	AgeSampleCount          int                    `json:"age_sample_count"`
	AgeWindows              backlogAgeWindows      `json:"age_windows"`
	AgePercentilesSeconds   *backlogAgePercentiles `json:"age_percentiles_seconds,omitempty"`
	StateCounts             map[string]int         `json:"state_counts"`
	OldestQueuedReceivedAt  string                 `json:"oldest_queued_received_at,omitempty"`
	OldestQueuedAgeSeconds  int                    `json:"oldest_queued_age_seconds,omitempty"`
	NewestQueuedReceivedAt  string                 `json:"newest_queued_received_at,omitempty"`
	NewestQueuedAgeSeconds  int                    `json:"newest_queued_age_seconds,omitempty"`
	EarliestQueuedNextRunAt string                 `json:"earliest_queued_next_run_at,omitempty"`
	ReadyOverdueCount       int                    `json:"ready_overdue_count"`
	MaxReadyLagSeconds      int                    `json:"max_ready_lag_seconds,omitempty"`
}

func observeBacklogAgeWindow(ageSeconds int, windows *backlogAgeWindows) {
	switch {
	case ageSeconds <= int((5*time.Minute)/time.Second):
		windows.LE5M++
	case ageSeconds <= int((15*time.Minute)/time.Second):
		windows.GT5MLE15M++
	case ageSeconds <= int(time.Hour/time.Second):
		windows.GT15MLE1H++
	case ageSeconds <= int((6*time.Hour)/time.Second):
		windows.GT1HLE6H++
	default:
		windows.GT6H++
	}
}

func backlogAgePercentilesFromSamples(samples []int) (backlogAgePercentiles, bool) {
	if len(samples) == 0 {
		return backlogAgePercentiles{}, false
	}
	ordered := make([]int, len(samples))
	copy(ordered, samples)
	sort.Ints(ordered)
	return backlogAgePercentiles{
		P50: backlogAgePercentileNearestRank(ordered, 50),
		P90: backlogAgePercentileNearestRank(ordered, 90),
		P99: backlogAgePercentileNearestRank(ordered, 99),
	}, true
}

func backlogAgePercentileNearestRank(ordered []int, percentile int) int {
	if len(ordered) == 0 {
		return 0
	}
	if percentile <= 0 {
		return ordered[0]
	}
	if percentile >= 100 {
		return ordered[len(ordered)-1]
	}
	rank := (percentile*len(ordered) + 100 - 1) / 100
	if rank <= 0 {
		rank = 1
	}
	if rank > len(ordered) {
		rank = len(ordered)
	}
	return ordered[rank-1]
}

type dlqItem struct {
	ID         string            `json:"id"`
	Route      string            `json:"route"`
	Target     string            `json:"target"`
	ReceivedAt time.Time         `json:"received_at"`
	Attempt    int               `json:"attempt"`
	DeadReason string            `json:"dead_reason,omitempty"`
	PayloadB64 string            `json:"payload_b64,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	Trace      map[string]string `json:"trace,omitempty"`
}

func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	q := r.URL.Query()
	limit := defaultListLimit
	if v := strings.TrimSpace(q.Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	before, ok := parseTimeParam(strings.TrimSpace(q.Get("before")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "before must be RFC3339")
		return
	}

	includePayload, ok := parseBoolParam(strings.TrimSpace(q.Get("include_payload")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "include_payload must be true|false")
		return
	}
	includeHeaders, ok := parseBoolParam(strings.TrimSpace(q.Get("include_headers")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "include_headers must be true|false")
		return
	}
	includeTrace, ok := parseBoolParam(strings.TrimSpace(q.Get("include_trace")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "include_trace must be true|false")
		return
	}
	routeFilter, ok := parseOptionalRoutePath(q.Get("route"))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}

	resp, err := s.Store.ListDead(queue.DeadListRequest{
		Route:          routeFilter,
		Limit:          limit,
		Before:         before,
		IncludePayload: includePayload,
		IncludeHeaders: includeHeaders,
		IncludeTrace:   includeTrace,
	})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	out := dlqResponse{Items: make([]dlqItem, 0, len(resp.Items))}
	for _, it := range resp.Items {
		item := dlqItem{
			ID:         it.ID,
			Route:      it.Route,
			Target:     it.Target,
			ReceivedAt: it.ReceivedAt,
			Attempt:    it.Attempt,
			DeadReason: it.DeadReason,
		}
		if includePayload && len(it.Payload) > 0 {
			item.PayloadB64 = base64.StdEncoding.EncodeToString(it.Payload)
		}
		if includeHeaders && len(it.Headers) > 0 {
			item.Headers = it.Headers
		}
		if includeTrace && len(it.Trace) > 0 {
			item.Trace = it.Trace
		}
		out.Items = append(out.Items, item)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleBacklogTopQueued(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	limit := defaultListLimit
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	stats, err := s.Store.Stats()
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	routeFilter, ok := parseOptionalRoutePath(r.URL.Query().Get("route"))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}
	targetFilter := strings.TrimSpace(r.URL.Query().Get("target"))
	out := backlogTopQueuedFromStats(stats, routeFilter, targetFilter, limit)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleBacklogOldestQueued(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	limit := defaultListLimit
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	stats, err := s.Store.Stats()
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	routeFilter, ok := parseOptionalRoutePath(r.URL.Query().Get("route"))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}
	targetFilter := strings.TrimSpace(r.URL.Query().Get("target"))

	probeLimit := limit
	if limit < maxListLimit {
		probeLimit = limit + 1
	}

	resp, err := s.Store.ListMessages(queue.MessageListRequest{
		Route:  routeFilter,
		Target: targetFilter,
		State:  queue.StateQueued,
		Order:  queue.MessageOrderAsc,
		Limit:  probeLimit,
	})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	items := resp.Items
	truncated := false
	if limit < maxListLimit && len(items) > limit {
		truncated = true
		items = items[:limit]
	}
	if limit == maxListLimit && routeFilter == "" && targetFilter == "" && stats.ByState[queue.StateQueued] > limit {
		truncated = true
	}

	out := backlogOldestQueuedFromMessages(backlogReferenceNow(stats), stats, routeFilter, targetFilter, limit, truncated, items)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleBacklogAgingSummary(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	limit := defaultListLimit
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	stats, err := s.Store.Stats()
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	routeFilter, ok := parseOptionalRoutePath(r.URL.Query().Get("route"))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}
	targetFilter := strings.TrimSpace(r.URL.Query().Get("target"))
	states, ok := parseBacklogSummaryStates(strings.TrimSpace(r.URL.Query().Get("states")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "states must contain only queued|leased|dead")
		return
	}

	probeLimit := limit
	if limit < maxListLimit {
		probeLimit = limit + 1
	}

	truncated := false
	stateScan := make(map[queue.State]backlogStateScanSummary, len(states))
	items := make([]queue.Envelope, 0, len(states)*limit)
	for _, state := range states {
		resp, err := s.Store.ListMessages(queue.MessageListRequest{
			Route:  routeFilter,
			Target: targetFilter,
			State:  state,
			Order:  queue.MessageOrderAsc,
			Limit:  probeLimit,
		})
		if err != nil {
			writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
			return
		}
		stateItems := resp.Items
		stateTruncated := false
		if limit < maxListLimit && len(stateItems) > limit {
			stateTruncated = true
			stateItems = stateItems[:limit]
		}
		if limit == maxListLimit && routeFilter == "" && targetFilter == "" && stats.ByState[state] > limit {
			stateTruncated = true
		}
		if stateTruncated {
			truncated = true
		}
		stateScan[state] = backlogStateScanSummary{
			Scanned:   len(stateItems),
			Truncated: stateTruncated,
		}
		items = append(items, stateItems...)
	}

	out := backlogAgingSummaryFromMessages(backlogReferenceNow(stats), stats, routeFilter, targetFilter, states, limit, truncated, stateScan, items)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleBacklogTrends(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}
	trendStore, ok := s.Store.(queue.BacklogTrendStore)
	if !ok {
		writeManagementError(w, http.StatusServiceUnavailable, readCodeBacklogUnavailable, "backlog trend store is not supported by queue backend")
		return
	}

	window := defaultBacklogTrendWindow
	if raw := strings.TrimSpace(r.URL.Query().Get("window")); raw != "" {
		d, valid := parseDurationParam(raw)
		if !valid || d <= 0 || d > maxBacklogTrendWindow {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "window must be a duration between 1m and 168h")
			return
		}
		window = d
	}

	step := defaultBacklogTrendStep
	if raw := strings.TrimSpace(r.URL.Query().Get("step")); raw != "" {
		d, valid := parseDurationParam(raw)
		if !valid || d < minBacklogTrendStep || d > maxBacklogTrendStep {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "step must be a duration between 1m and 1h")
			return
		}
		step = d
	}
	if step > window {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "step must be less than or equal to window")
		return
	}

	until, valid := parseTimeParam(strings.TrimSpace(r.URL.Query().Get("until")))
	if !valid {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "until must be RFC3339")
		return
	}
	if until.IsZero() {
		until = time.Now().UTC()
	}
	since := until.Add(-window)
	bucketCount := int(window / step)
	if window%step != 0 {
		bucketCount++
	}
	if bucketCount <= 0 || bucketCount > maxListLimit {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "window/step produce an invalid bucket count")
		return
	}

	routeFilter, ok := parseOptionalRoutePath(r.URL.Query().Get("route"))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}
	targetFilter := strings.TrimSpace(r.URL.Query().Get("target"))
	resp, err := trendStore.ListBacklogTrend(queue.BacklogTrendListRequest{
		Route:  routeFilter,
		Target: targetFilter,
		Since:  since,
		Until:  until,
		Limit:  maxBacklogTrendSamples,
	})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	out := backlogTrendsFromSamples(since, until, window, step, routeFilter, targetFilter, resp.Truncated, resp.Items, s.trendSignalConfig())
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) trendSignalConfig() queue.BacklogTrendSignalConfig {
	if s != nil && s.ResolveTrendSignalConfig != nil {
		return s.ResolveTrendSignalConfig()
	}
	return queue.DefaultBacklogTrendSignalConfig()
}

type dlqManageRequest struct {
	IDs []string `json:"ids"`
}

type dlqRequeueResponse struct {
	Requeued int              `json:"requeued"`
	Audit    *managementAudit `json:"audit,omitempty"`
}

type dlqDeleteResponse struct {
	Deleted int              `json:"deleted"`
	Audit   *managementAudit `json:"audit,omitempty"`
}

type messagesResponse struct {
	Items []messageItem `json:"items"`
}

type messageItem struct {
	ID         string            `json:"id"`
	Route      string            `json:"route"`
	Target     string            `json:"target"`
	State      queue.State       `json:"state"`
	ReceivedAt time.Time         `json:"received_at"`
	Attempt    int               `json:"attempt"`
	NextRunAt  time.Time         `json:"next_run_at"`
	DeadReason string            `json:"dead_reason,omitempty"`
	PayloadB64 string            `json:"payload_b64,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	Trace      map[string]string `json:"trace,omitempty"`
}

type messagesCancelResponse struct {
	Canceled int              `json:"canceled"`
	Audit    *managementAudit `json:"audit,omitempty"`
}

type messagesRequeueResponse struct {
	Requeued int              `json:"requeued"`
	Audit    *managementAudit `json:"audit,omitempty"`
}

type messagesResumeResponse struct {
	Resumed int              `json:"resumed"`
	Audit   *managementAudit `json:"audit,omitempty"`
}

type messagesPublishItem struct {
	ID           string            `json:"id"`
	Route        string            `json:"route"`
	Target       string            `json:"target"`
	Application  string            `json:"application,omitempty"`
	EndpointName string            `json:"endpoint_name,omitempty"`
	PayloadB64   string            `json:"payload_b64,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	Trace        map[string]string `json:"trace,omitempty"`
	ReceivedAt   string            `json:"received_at,omitempty"`
	NextRunAt    string            `json:"next_run_at,omitempty"`
}

type messagesPublishRequest struct {
	Items []messagesPublishItem `json:"items"`
}

type messagesPublishResponse struct {
	Published int              `json:"published"`
	Scope     *managementScope `json:"scope,omitempty"`
	Audit     *managementAudit `json:"audit,omitempty"`
}

type publishErrorResponse struct {
	Code      string `json:"code"`
	Detail    string `json:"detail"`
	ItemIndex *int   `json:"item_index,omitempty"`
}

type messagesManageFilterRequest struct {
	Route        string `json:"route"`
	Application  string `json:"application"`
	EndpointName string `json:"endpoint_name"`
	Target       string `json:"target"`
	State        string `json:"state"`
	Before       string `json:"before"`
	Limit        int    `json:"limit"`
	PreviewOnly  bool   `json:"preview_only"`
}

type messagesManageFilterResponse struct {
	Matched     int              `json:"matched"`
	Canceled    int              `json:"canceled,omitempty"`
	Requeued    int              `json:"requeued,omitempty"`
	Resumed     int              `json:"resumed,omitempty"`
	PreviewOnly bool             `json:"preview_only"`
	Scope       *managementScope `json:"scope,omitempty"`
	Audit       *managementAudit `json:"audit,omitempty"`
}

type ManagementModel struct {
	RouteCount       int                     `json:"route_count"`
	ApplicationCount int                     `json:"application_count"`
	EndpointCount    int                     `json:"endpoint_count"`
	Applications     []ManagementApplication `json:"applications"`
}

type ManagementApplication struct {
	Name          string               `json:"name"`
	EndpointCount int                  `json:"endpoint_count"`
	Endpoints     []ManagementEndpoint `json:"endpoints"`
}

type ManagementEndpoint struct {
	Name          string                          `json:"name"`
	Route         string                          `json:"route"`
	Mode          string                          `json:"mode"`
	Targets       []string                        `json:"targets"`
	PublishPolicy ManagementEndpointPublishPolicy `json:"publish_policy"`
}

type ManagementEndpointPublishPolicy struct {
	Enabled        bool `json:"enabled"`
	DirectEnabled  bool `json:"direct_enabled"`
	ManagedEnabled bool `json:"managed_enabled"`
}

type applicationsResponse struct {
	ApplicationCount int               `json:"application_count"`
	EndpointCount    int               `json:"endpoint_count"`
	Items            []applicationItem `json:"items"`
}

type applicationItem struct {
	Name          string `json:"name"`
	EndpointCount int    `json:"endpoint_count"`
}

type applicationEndpointsResponse struct {
	Application   string               `json:"application"`
	EndpointCount int                  `json:"endpoint_count"`
	Items         []ManagementEndpoint `json:"items"`
}

type applicationEndpointResponse struct {
	Application   string                          `json:"application"`
	EndpointName  string                          `json:"endpoint_name"`
	Route         string                          `json:"route"`
	Mode          string                          `json:"mode"`
	Targets       []string                        `json:"targets"`
	PublishPolicy ManagementEndpointPublishPolicy `json:"publish_policy"`
}

type applicationEndpointMutationResponse struct {
	Applied  bool                         `json:"applied"`
	Action   string                       `json:"action"`
	Endpoint *applicationEndpointResponse `json:"endpoint,omitempty"`
	Scope    *managementScope             `json:"scope,omitempty"`
	Audit    *managementAudit             `json:"audit,omitempty"`
}

type managementScope struct {
	Application  string `json:"application"`
	EndpointName string `json:"endpoint_name"`
	Route        string `json:"route"`
}

type managementAudit struct {
	Reason    string `json:"reason,omitempty"`
	Actor     string `json:"actor,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

type ManagementMutationAuditEvent struct {
	At           time.Time
	Operation    string
	Application  string
	EndpointName string
	Route        string
	Target       string
	State        string
	Limit        int
	PreviewOnly  bool
	Matched      int
	Changed      int
	Reason       string
	Actor        string
	RequestID    string
}

type ManagementEndpointUpsertRequest struct {
	Application  string
	EndpointName string
	Route        string
	Reason       string
	Actor        string
	RequestID    string
}

type ManagementEndpointDeleteRequest struct {
	Application  string
	EndpointName string
	Reason       string
	Actor        string
	RequestID    string
}

type ManagementEndpointMutationResult struct {
	Applied bool
	Action  string
	Route   string

	// PostWriteValidate is called after the config file is written but before
	// reload. If non-nil and it returns an error, the file write is rolled back.
	// Used to re-check invariants (e.g. backlog emptiness) after config mutation.
	PostWriteValidate func() error `json:"-"`
}

type managementEndpointUpsertPayload struct {
	Route string `json:"route"`
}

func (s *Server) handleDLQRequeue(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "invalid or missing audit headers")
		return
	}

	ids, ok := parseManageIDs(r)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, fmt.Sprintf("ids must contain between 1 and %d non-empty entries", maxListLimit))
		return
	}
	scopedManagedMutation, err := s.scopedManagedMutationByIDs(ids, map[queue.State]struct{}{
		queue.StateDead: {},
	})
	if err != nil {
		s.writeScopedManagedIDMutationError(w, err)
		return
	}
	if scopedManagedMutation {
		if code, detail := s.mutationAuditPolicyError(audit, true, "mutation"); code != "" {
			writeManagementError(w, http.StatusBadRequest, code, detail)
			return
		}
	}

	resp, err := s.Store.RequeueDead(queue.DeadRequeueRequest{IDs: ids})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(dlqRequeueResponse{
		Requeued: resp.Requeued,
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	})
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:        time.Now().UTC(),
		Operation: "dlq_requeue",
		State:     string(queue.StateDead),
		Limit:     len(ids),
		Matched:   len(ids),
		Changed:   resp.Requeued,
		Reason:    audit.Reason,
		Actor:     audit.Actor,
		RequestID: audit.RequestID,
	})
}

func (s *Server) handleDLQDelete(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "invalid or missing audit headers")
		return
	}

	ids, ok := parseManageIDs(r)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, fmt.Sprintf("ids must contain between 1 and %d non-empty entries", maxListLimit))
		return
	}
	scopedManagedMutation, err := s.scopedManagedMutationByIDs(ids, map[queue.State]struct{}{
		queue.StateDead: {},
	})
	if err != nil {
		s.writeScopedManagedIDMutationError(w, err)
		return
	}
	if scopedManagedMutation {
		if code, detail := s.mutationAuditPolicyError(audit, true, "mutation"); code != "" {
			writeManagementError(w, http.StatusBadRequest, code, detail)
			return
		}
	}

	resp, err := s.Store.DeleteDead(queue.DeadDeleteRequest{IDs: ids})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(dlqDeleteResponse{
		Deleted: resp.Deleted,
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	})
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:        time.Now().UTC(),
		Operation: "dlq_delete",
		State:     string(queue.StateDead),
		Limit:     len(ids),
		Matched:   len(ids),
		Changed:   resp.Deleted,
		Reason:    audit.Reason,
		Actor:     audit.Actor,
		RequestID: audit.RequestID,
	})
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	q := r.URL.Query()
	limit := defaultListLimit
	if v := strings.TrimSpace(q.Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	before, ok := parseTimeParam(strings.TrimSpace(q.Get("before")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "before must be RFC3339")
		return
	}

	includePayload, ok := parseBoolParam(strings.TrimSpace(q.Get("include_payload")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "include_payload must be true|false")
		return
	}
	includeHeaders, ok := parseBoolParam(strings.TrimSpace(q.Get("include_headers")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "include_headers must be true|false")
		return
	}
	includeTrace, ok := parseBoolParam(strings.TrimSpace(q.Get("include_trace")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "include_trace must be true|false")
		return
	}

	state, ok := parseQueueState(strings.TrimSpace(q.Get("state")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "state must be one of queued|leased|delivered|dead|canceled")
		return
	}

	routeHint := strings.TrimSpace(q.Get("route"))
	application := strings.TrimSpace(q.Get("application"))
	endpointName := strings.TrimSpace(q.Get("endpoint_name"))
	if detail, ok := validateManagedSelectorLabels(application, endpointName); !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, detail)
		return
	}
	if application != "" && routeHint != "" {
		writeManagementError(w, http.StatusBadRequest, publishCodeScopedSelectorMismatch, "route is not allowed when application and endpoint_name are set")
		return
	}
	routeHint, ok = parseOptionalRoutePath(routeHint)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}
	route, status, code, detail, ok := s.resolveManagedRoute(routeHint, application, endpointName)
	if !ok {
		if code != "" {
			writeManagementError(w, status, code, detail)
			return
		}
		writeManagedSelectorResolveError(w, status)
		return
	}

	resp, err := s.Store.ListMessages(queue.MessageListRequest{
		Route:          route,
		Target:         strings.TrimSpace(q.Get("target")),
		State:          state,
		Limit:          limit,
		Before:         before,
		IncludePayload: includePayload,
		IncludeHeaders: includeHeaders,
		IncludeTrace:   includeTrace,
	})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	out := messagesResponse{Items: make([]messageItem, 0, len(resp.Items))}
	for _, it := range resp.Items {
		item := messageItem{
			ID:         it.ID,
			Route:      it.Route,
			Target:     it.Target,
			State:      it.State,
			ReceivedAt: it.ReceivedAt,
			Attempt:    it.Attempt,
			NextRunAt:  it.NextRunAt,
			DeadReason: it.DeadReason,
		}
		if includePayload && len(it.Payload) > 0 {
			item.PayloadB64 = base64.StdEncoding.EncodeToString(it.Payload)
		}
		if includeHeaders && len(it.Headers) > 0 {
			item.Headers = it.Headers
		}
		if includeTrace && len(it.Trace) > 0 {
			item.Trace = it.Trace
		}
		out.Items = append(out.Items, item)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleMessagesPublish(w http.ResponseWriter, r *http.Request) {
	if !s.PublishGlobalDirectEnabled {
		s.writePublishError(w, http.StatusForbidden, publishCodeGlobalPublishDisabled, "global direct publish path is disabled by defaults.publish_policy.direct", -1, false)
		return
	}
	if s.Store == nil {
		s.writePublishError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable", -1, false)
		return
	}
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		s.writePublishError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "X-Hookaido-Audit-Reason is required", -1, false)
		return
	}
	if code, detail := s.mutationAuditPolicyError(audit, false, "publish"); code != "" {
		s.writePublishError(w, http.StatusBadRequest, code, detail, -1, false)
		return
	}

	items, parseErr := parsePublishItems(r)
	if parseErr != nil {
		s.writePublishError(w, http.StatusBadRequest, parseErr.Code, parseErr.Detail, parseErr.ItemIndex, false)
		return
	}
	if idx := firstManagedPublishItemIndex(items); idx >= 0 {
		s.writePublishError(
			w,
			http.StatusBadRequest,
			publishCodeScopedPublishRequired,
			"managed publish must use /applications/{application}/endpoints/{endpoint_name}/messages/publish",
			idx,
			false,
		)
		return
	}

	managedRoutes, managedRoutesAvailable := s.managedRouteSet()
	prepared := make([]queue.Envelope, 0, len(items))
	for idx, item := range items {
		route := strings.TrimSpace(item.Route)
		target := strings.TrimSpace(item.Target)
		application := strings.TrimSpace(item.Application)
		endpointName := strings.TrimSpace(item.EndpointName)

		if (application == "") != (endpointName == "") {
			s.writePublishError(w, http.StatusBadRequest, publishCodeInvalidBody, "item.application and item.endpoint_name must be set together", idx, false)
			return
		}

		if application != "" {
			s.writePublishError(
				w,
				http.StatusBadRequest,
				publishCodeScopedPublishRequired,
				"managed publish must use /applications/{application}/endpoints/{endpoint_name}/messages/publish",
				idx,
				false,
			)
			return
		}
		if route == "" {
			s.writePublishError(w, http.StatusBadRequest, publishCodeInvalidBody, "item.route is required when managed selector is not used", idx, false)
			return
		}
		if !strings.HasPrefix(route, "/") {
			s.writePublishError(w, http.StatusBadRequest, publishCodeInvalidBody, "item.route must start with '/'", idx, false)
			return
		}
		routeOwnership := s.lookupManagementEndpointByRouteOwnershipStatus(route)
		if routeOwnership.SourceMismatch {
			s.writePublishError(w, http.StatusServiceUnavailable, publishCodeManagedTargetMismatch, routeOwnership.SourceMismatchInfo, idx, false)
			return
		}
		managedByRouteOwnership := routeOwnership.Managed
		if managedRoutesAvailable {
			if _, managed := managedRoutes[route]; managed {
				managedByRouteOwnership = true
			}
		} else if !routeOwnership.Available {
			if s.PublishScopedManagedFailClosed {
				s.writePublishError(w, http.StatusServiceUnavailable, publishCodeManagedResolverMissing, "management model is unavailable for managed-route publish policy evaluation", idx, false)
				return
			}
		}
		if managedByRouteOwnership {
			s.writePublishError(w, http.StatusBadRequest, publishCodeManagedSelectorRequired, fmt.Sprintf("route %q is managed by application/endpoint; publish must use endpoint-scoped publish path", route), idx, false)
			return
		}
		var routeTargets []string
		if s.TargetsForRoute != nil {
			routeTargets = normalizePublishTargets(s.TargetsForRoute(route))
			if len(routeTargets) == 0 {
				s.writePublishError(w, http.StatusBadRequest, publishCodeRouteNotFound, fmt.Sprintf("route %q has no publishable targets", route), idx, false)
				return
			}
			if code, detail := s.publishRoutePolicyError(route, routeTargets, false); code != "" {
				s.writePublishError(w, http.StatusForbidden, code, detail, idx, false)
				return
			}
			var ok bool
			target, ok = resolvePublishTarget(target, routeTargets)
			if !ok {
				s.writePublishError(w, http.StatusBadRequest, publishCodeTargetUnresolvable, publishTargetUnresolvableDetail(route, strings.TrimSpace(item.Target), routeTargets), idx, false)
				return
			}
		} else {
			s.writePublishError(w, http.StatusServiceUnavailable, publishCodeRouteResolverMissing, "route target resolution is not configured for global publish path", idx, false)
			return
		}
		if target == "" {
			s.writePublishError(w, http.StatusBadRequest, publishCodeTargetUnresolvable, fmt.Sprintf("item.target is required for route %q", route), idx, false)
			return
		}

		env, code, codeDetail := publishEnvelopeFromItem(
			item,
			route,
			target,
			s.publishMaxBodyBytes(route),
			s.publishMaxHeaderBytes(route),
		)
		if code != "" {
			switch code {
			case publishCodeInvalidReceivedAt:
				s.writePublishError(w, http.StatusBadRequest, code, "item.received_at must be RFC3339", idx, false)
			case publishCodeInvalidNextRunAt:
				s.writePublishError(w, http.StatusBadRequest, code, "item.next_run_at must be RFC3339", idx, false)
			case publishCodeInvalidHeader:
				if strings.TrimSpace(codeDetail) == "" {
					codeDetail = "item.headers contains an invalid HTTP header name or value"
				}
				s.writePublishError(w, http.StatusBadRequest, code, codeDetail, idx, false)
			case publishCodePayloadTooLarge:
				if strings.TrimSpace(codeDetail) == "" {
					codeDetail = "item payload exceeds max_body for route"
				}
				s.writePublishError(w, http.StatusRequestEntityTooLarge, code, codeDetail, idx, false)
			case publishCodeHeadersTooLarge:
				if strings.TrimSpace(codeDetail) == "" {
					codeDetail = "item headers exceed max_headers for route"
				}
				s.writePublishError(w, http.StatusRequestEntityTooLarge, code, codeDetail, idx, false)
			default:
				s.writePublishError(w, http.StatusBadRequest, code, "item.payload_b64 must be valid base64", idx, false)
			}
			return
		}

		prepared = append(prepared, env)
	}
	if dupIdx, dupID, err := firstExistingMessageIDIndex(s.Store, publishEnvelopeIDs(prepared)); err != nil {
		s.writePublishError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable", -1, false)
		return
	} else if dupIdx >= 0 {
		s.writePublishError(w, http.StatusConflict, publishCodeDuplicateID, fmt.Sprintf("item.id %q already exists", dupID), dupIdx, false)
		return
	}

	published := 0
	auditRoute := ""
	auditTarget := ""
	singleRoute := true
	singleTarget := true

	if batcher, ok := s.Store.(queue.BatchEnqueuer); ok {
		n, err := batcher.EnqueueBatch(prepared)
		if err != nil {
			if errors.Is(err, queue.ErrEnvelopeExists) {
				s.writePublishError(w, http.StatusConflict, publishCodeDuplicateID, "batch contains a duplicate item.id", -1, false)
				return
			}
			if errors.Is(err, queue.ErrQueueFull) {
				s.writePublishError(w, http.StatusServiceUnavailable, publishCodeQueueFull, "queue is full", -1, false)
				return
			}
			s.writePublishError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable", -1, false)
			return
		}
		published = n
		for _, env := range prepared {
			if auditRoute == "" {
				auditRoute = env.Route
			} else if env.Route != auditRoute {
				singleRoute = false
			}
			if auditTarget == "" {
				auditTarget = env.Target
			} else if env.Target != auditTarget {
				singleTarget = false
			}
		}
	} else {
		for idx, env := range prepared {
			if err := s.Store.Enqueue(env); err != nil {
				if errors.Is(err, queue.ErrEnvelopeExists) {
					s.writePublishError(w, http.StatusConflict, publishCodeDuplicateID, fmt.Sprintf("item.id %q already exists", env.ID), idx, false)
					return
				}
				if errors.Is(err, queue.ErrQueueFull) {
					s.writePublishError(w, http.StatusServiceUnavailable, publishCodeQueueFull, "queue is full", idx, false)
					return
				}
				s.writePublishError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable", idx, false)
				return
			}
			if published == 0 {
				auditRoute = env.Route
				auditTarget = env.Target
			} else {
				if env.Route != auditRoute {
					singleRoute = false
				}
				if env.Target != auditTarget {
					singleTarget = false
				}
			}
			published++
		}
	}

	if !singleRoute {
		auditRoute = ""
	}
	if !singleTarget {
		auditTarget = ""
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(messagesPublishResponse{
		Published: published,
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	})
	s.observePublishAccepted(published, false)
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:        time.Now().UTC(),
		Operation: "publish_messages",
		Route:     auditRoute,
		Target:    auditTarget,
		State:     string(queue.StateQueued),
		Limit:     len(items),
		Matched:   len(items),
		Changed:   published,
		Reason:    audit.Reason,
		Actor:     audit.Actor,
		RequestID: audit.RequestID,
	})
}

func (s *Server) handleMessagesCancel(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "invalid or missing audit headers")
		return
	}

	ids, ok := parseManageIDs(r)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, fmt.Sprintf("ids must contain between 1 and %d non-empty entries", maxListLimit))
		return
	}
	scopedManagedMutation, err := s.scopedManagedMutationByIDs(ids, map[queue.State]struct{}{
		queue.StateQueued: {},
		queue.StateLeased: {},
		queue.StateDead:   {},
	})
	if err != nil {
		s.writeScopedManagedIDMutationError(w, err)
		return
	}
	if scopedManagedMutation {
		if code, detail := s.mutationAuditPolicyError(audit, true, "mutation"); code != "" {
			writeManagementError(w, http.StatusBadRequest, code, detail)
			return
		}
	}

	resp, err := s.Store.CancelMessages(queue.MessageCancelRequest{IDs: ids})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(messagesCancelResponse{
		Canceled: resp.Canceled,
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	})
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:        time.Now().UTC(),
		Operation: "cancel_messages",
		State:     string(queue.StateCanceled),
		Limit:     len(ids),
		Matched:   len(ids),
		Changed:   resp.Canceled,
		Reason:    audit.Reason,
		Actor:     audit.Actor,
		RequestID: audit.RequestID,
	})
}

func (s *Server) handleMessagesRequeue(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "invalid or missing audit headers")
		return
	}

	ids, ok := parseManageIDs(r)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, fmt.Sprintf("ids must contain between 1 and %d non-empty entries", maxListLimit))
		return
	}
	scopedManagedMutation, err := s.scopedManagedMutationByIDs(ids, map[queue.State]struct{}{
		queue.StateDead:     {},
		queue.StateCanceled: {},
	})
	if err != nil {
		s.writeScopedManagedIDMutationError(w, err)
		return
	}
	if scopedManagedMutation {
		if code, detail := s.mutationAuditPolicyError(audit, true, "mutation"); code != "" {
			writeManagementError(w, http.StatusBadRequest, code, detail)
			return
		}
	}

	resp, err := s.Store.RequeueMessages(queue.MessageRequeueRequest{IDs: ids})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(messagesRequeueResponse{
		Requeued: resp.Requeued,
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	})
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:        time.Now().UTC(),
		Operation: "requeue_messages",
		State:     string(queue.StateQueued),
		Limit:     len(ids),
		Matched:   len(ids),
		Changed:   resp.Requeued,
		Reason:    audit.Reason,
		Actor:     audit.Actor,
		RequestID: audit.RequestID,
	})
}

func (s *Server) handleMessagesResume(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "invalid or missing audit headers")
		return
	}

	ids, ok := parseManageIDs(r)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, fmt.Sprintf("ids must contain between 1 and %d non-empty entries", maxListLimit))
		return
	}
	scopedManagedMutation, err := s.scopedManagedMutationByIDs(ids, map[queue.State]struct{}{
		queue.StateCanceled: {},
	})
	if err != nil {
		s.writeScopedManagedIDMutationError(w, err)
		return
	}
	if scopedManagedMutation {
		if code, detail := s.mutationAuditPolicyError(audit, true, "mutation"); code != "" {
			writeManagementError(w, http.StatusBadRequest, code, detail)
			return
		}
	}

	resp, err := s.Store.ResumeMessages(queue.MessageResumeRequest{IDs: ids})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(messagesResumeResponse{
		Resumed: resp.Resumed,
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	})
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:        time.Now().UTC(),
		Operation: "resume_messages",
		State:     string(queue.StateQueued),
		Limit:     len(ids),
		Matched:   len(ids),
		Changed:   resp.Resumed,
		Reason:    audit.Reason,
		Actor:     audit.Actor,
		RequestID: audit.RequestID,
	})
}

func (s *Server) handleMessagesCancelByFilter(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	req, application, endpointName, ok := parseMessageManageFilter(r, map[queue.State]struct{}{
		queue.StateQueued: {},
		queue.StateLeased: {},
		queue.StateDead:   {},
	})
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, "request body is invalid for cancel_by_filter")
		return
	}
	if detail, ok := validateManagedSelectorLabels(application, endpointName); !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, detail)
		return
	}
	if application != "" && strings.TrimSpace(req.Route) != "" {
		writeManagementError(w, http.StatusBadRequest, publishCodeScopedSelectorMismatch, "route is not allowed when application and endpoint_name are set")
		return
	}
	routeHint, routeOK := parseOptionalRoutePath(req.Route)
	if !routeOK {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, "route must start with '/'")
		return
	}
	req.Route = routeHint
	resolvedRoute, status, code, detail, ok := s.resolveManagedRoute(req.Route, application, endpointName)
	if !ok {
		if code != "" {
			writeManagementError(w, status, code, detail)
			return
		}
		writeManagedSelectorResolveError(w, status)
		return
	}
	req.Route = resolvedRoute
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "X-Hookaido-Audit-Reason is required")
		return
	}
	scopedManagedMutation, err := s.filterMutationTouchesManagedRoute(req.Route, application, endpointName)
	if err != nil {
		s.writeScopedManagedFilterMutationError(w, err)
		return
	}
	if application == "" && scopedManagedMutation {
		writeManagementError(w, http.StatusBadRequest, publishCodeManagedSelectorRequired, managedSelectorRequiredDetail(req.Route))
		return
	}
	if scopedManagedMutation {
		if code, detail := s.mutationAuditPolicyError(audit, true, "mutation"); code != "" {
			writeManagementError(w, http.StatusBadRequest, code, detail)
			return
		}
	}

	resp, err := s.Store.CancelMessagesByFilter(req)
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	out := messagesManageFilterResponse{
		Matched:     resp.Matched,
		Canceled:    resp.Canceled,
		PreviewOnly: resp.PreviewOnly,
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	}
	if application != "" && endpointName != "" {
		out.Scope = &managementScope{
			Application:  application,
			EndpointName: endpointName,
			Route:        req.Route,
		}
	}
	_ = json.NewEncoder(w).Encode(out)
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:           time.Now().UTC(),
		Operation:    "cancel_by_filter",
		Application:  application,
		EndpointName: endpointName,
		Route:        req.Route,
		Target:       req.Target,
		State:        string(req.State),
		Limit:        req.Limit,
		PreviewOnly:  req.PreviewOnly,
		Matched:      resp.Matched,
		Changed:      resp.Canceled,
		Reason:       audit.Reason,
		Actor:        audit.Actor,
		RequestID:    audit.RequestID,
	})
}

func (s *Server) handleMessagesRequeueByFilter(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	req, application, endpointName, ok := parseMessageManageFilter(r, map[queue.State]struct{}{
		queue.StateDead:     {},
		queue.StateCanceled: {},
	})
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, "request body is invalid for requeue_by_filter")
		return
	}
	if detail, ok := validateManagedSelectorLabels(application, endpointName); !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, detail)
		return
	}
	if application != "" && strings.TrimSpace(req.Route) != "" {
		writeManagementError(w, http.StatusBadRequest, publishCodeScopedSelectorMismatch, "route is not allowed when application and endpoint_name are set")
		return
	}
	routeHint, routeOK := parseOptionalRoutePath(req.Route)
	if !routeOK {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, "route must start with '/'")
		return
	}
	req.Route = routeHint
	resolvedRoute, status, code, detail, ok := s.resolveManagedRoute(req.Route, application, endpointName)
	if !ok {
		if code != "" {
			writeManagementError(w, status, code, detail)
			return
		}
		writeManagedSelectorResolveError(w, status)
		return
	}
	req.Route = resolvedRoute
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "X-Hookaido-Audit-Reason is required")
		return
	}
	scopedManagedMutation, err := s.filterMutationTouchesManagedRoute(req.Route, application, endpointName)
	if err != nil {
		s.writeScopedManagedFilterMutationError(w, err)
		return
	}
	if application == "" && scopedManagedMutation {
		writeManagementError(w, http.StatusBadRequest, publishCodeManagedSelectorRequired, managedSelectorRequiredDetail(req.Route))
		return
	}
	if scopedManagedMutation {
		if code, detail := s.mutationAuditPolicyError(audit, true, "mutation"); code != "" {
			writeManagementError(w, http.StatusBadRequest, code, detail)
			return
		}
	}

	resp, err := s.Store.RequeueMessagesByFilter(req)
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	out := messagesManageFilterResponse{
		Matched:     resp.Matched,
		Requeued:    resp.Requeued,
		PreviewOnly: resp.PreviewOnly,
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	}
	if application != "" && endpointName != "" {
		out.Scope = &managementScope{
			Application:  application,
			EndpointName: endpointName,
			Route:        req.Route,
		}
	}
	_ = json.NewEncoder(w).Encode(out)
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:           time.Now().UTC(),
		Operation:    "requeue_by_filter",
		Application:  application,
		EndpointName: endpointName,
		Route:        req.Route,
		Target:       req.Target,
		State:        string(req.State),
		Limit:        req.Limit,
		PreviewOnly:  req.PreviewOnly,
		Matched:      resp.Matched,
		Changed:      resp.Requeued,
		Reason:       audit.Reason,
		Actor:        audit.Actor,
		RequestID:    audit.RequestID,
	})
}

func (s *Server) handleMessagesResumeByFilter(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	req, application, endpointName, ok := parseMessageManageFilter(r, map[queue.State]struct{}{
		queue.StateCanceled: {},
	})
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, "request body is invalid for resume_by_filter")
		return
	}
	if detail, ok := validateManagedSelectorLabels(application, endpointName); !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, detail)
		return
	}
	if application != "" && strings.TrimSpace(req.Route) != "" {
		writeManagementError(w, http.StatusBadRequest, publishCodeScopedSelectorMismatch, "route is not allowed when application and endpoint_name are set")
		return
	}
	routeHint, routeOK := parseOptionalRoutePath(req.Route)
	if !routeOK {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, "route must start with '/'")
		return
	}
	req.Route = routeHint
	resolvedRoute, status, code, detail, ok := s.resolveManagedRoute(req.Route, application, endpointName)
	if !ok {
		if code != "" {
			writeManagementError(w, status, code, detail)
			return
		}
		writeManagedSelectorResolveError(w, status)
		return
	}
	req.Route = resolvedRoute
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "X-Hookaido-Audit-Reason is required")
		return
	}
	scopedManagedMutation, err := s.filterMutationTouchesManagedRoute(req.Route, application, endpointName)
	if err != nil {
		s.writeScopedManagedFilterMutationError(w, err)
		return
	}
	if application == "" && scopedManagedMutation {
		writeManagementError(w, http.StatusBadRequest, publishCodeManagedSelectorRequired, managedSelectorRequiredDetail(req.Route))
		return
	}
	if scopedManagedMutation {
		if code, detail := s.mutationAuditPolicyError(audit, true, "mutation"); code != "" {
			writeManagementError(w, http.StatusBadRequest, code, detail)
			return
		}
	}

	resp, err := s.Store.ResumeMessagesByFilter(req)
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	out := messagesManageFilterResponse{
		Matched:     resp.Matched,
		Resumed:     resp.Resumed,
		PreviewOnly: resp.PreviewOnly,
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	}
	if application != "" && endpointName != "" {
		out.Scope = &managementScope{
			Application:  application,
			EndpointName: endpointName,
			Route:        req.Route,
		}
	}
	_ = json.NewEncoder(w).Encode(out)
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:           time.Now().UTC(),
		Operation:    "resume_by_filter",
		Application:  application,
		EndpointName: endpointName,
		Route:        req.Route,
		Target:       req.Target,
		State:        string(req.State),
		Limit:        req.Limit,
		PreviewOnly:  req.PreviewOnly,
		Matched:      resp.Matched,
		Changed:      resp.Resumed,
		Reason:       audit.Reason,
		Actor:        audit.Actor,
		RequestID:    audit.RequestID,
	})
}

type attemptsResponse struct {
	Items []attemptItem `json:"items"`
}

type attemptItem struct {
	ID         string               `json:"id"`
	EventID    string               `json:"event_id"`
	Route      string               `json:"route"`
	Target     string               `json:"target"`
	Attempt    int                  `json:"attempt"`
	StatusCode int                  `json:"status_code,omitempty"`
	Error      string               `json:"error,omitempty"`
	Outcome    queue.AttemptOutcome `json:"outcome"`
	DeadReason string               `json:"dead_reason,omitempty"`
	CreatedAt  time.Time            `json:"created_at"`
}

func (s *Server) handleAttempts(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	q := r.URL.Query()
	limit := defaultListLimit
	if v := strings.TrimSpace(q.Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	before, ok := parseTimeParam(strings.TrimSpace(q.Get("before")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "before must be RFC3339")
		return
	}

	outcome, ok := parseAttemptOutcome(strings.TrimSpace(q.Get("outcome")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "outcome must be one of acked|retry|dead")
		return
	}
	routeHint := strings.TrimSpace(q.Get("route"))
	application := strings.TrimSpace(q.Get("application"))
	endpointName := strings.TrimSpace(q.Get("endpoint_name"))
	if detail, ok := validateManagedSelectorLabels(application, endpointName); !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, detail)
		return
	}
	if application != "" && routeHint != "" {
		writeManagementError(w, http.StatusBadRequest, publishCodeScopedSelectorMismatch, "route is not allowed when application and endpoint_name are set")
		return
	}
	routeHint, ok = parseOptionalRoutePath(routeHint)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}
	route, status, code, detail, ok := s.resolveManagedRoute(routeHint, application, endpointName)
	if !ok {
		if code != "" {
			writeManagementError(w, status, code, detail)
			return
		}
		writeManagedSelectorResolveError(w, status)
		return
	}

	resp, err := s.Store.ListAttempts(queue.AttemptListRequest{
		Route:   route,
		Target:  strings.TrimSpace(q.Get("target")),
		EventID: strings.TrimSpace(q.Get("event_id")),
		Outcome: outcome,
		Limit:   limit,
		Before:  before,
	})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	out := attemptsResponse{Items: make([]attemptItem, 0, len(resp.Items))}
	for _, it := range resp.Items {
		out.Items = append(out.Items, attemptItem{
			ID:         it.ID,
			EventID:    it.EventID,
			Route:      it.Route,
			Target:     it.Target,
			Attempt:    it.Attempt,
			StatusCode: it.StatusCode,
			Error:      it.Error,
			Outcome:    it.Outcome,
			DeadReason: it.DeadReason,
			CreatedAt:  it.CreatedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleManagementModel(w http.ResponseWriter, r *http.Request) {
	if s.ManagementModel == nil {
		writeManagementError(w, http.StatusServiceUnavailable, readCodeManagementUnavailable, "management model is not configured")
		return
	}
	out := s.ManagementModel()
	if out.Applications == nil {
		out.Applications = []ManagementApplication{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) resolveManagedRoute(route, application, endpointName string) (string, int, string, string, bool) {
	route = strings.TrimSpace(route)
	application = strings.TrimSpace(application)
	endpointName = strings.TrimSpace(endpointName)
	if (application == "") != (endpointName == "") {
		return "", http.StatusBadRequest, "", "", false
	}
	if application == "" {
		if route != "" {
			routeOwnership := s.lookupManagementEndpointByRouteOwnershipStatus(route)
			if routeOwnership.SourceMismatch {
				return "", http.StatusServiceUnavailable, publishCodeManagedTargetMismatch, routeOwnership.SourceMismatchInfo, false
			}
		}
		return route, 0, "", "", true
	}
	resolvedRoute, _, status, code, detail, ok := s.resolveManagedEndpointAlignedScope(application, endpointName)
	if !ok {
		return "", status, code, detail, false
	}
	if route != "" && route != resolvedRoute {
		return "", http.StatusBadRequest, "", "", false
	}
	return resolvedRoute, 0, "", "", true
}

func (s *Server) handleApplications(w http.ResponseWriter, r *http.Request) {
	if s.ManagementModel == nil {
		writeManagementError(w, http.StatusServiceUnavailable, readCodeManagementUnavailable, "management model is not configured")
		return
	}
	model := s.ManagementModel()
	items := make([]applicationItem, 0, len(model.Applications))
	endpointCount := 0
	for _, app := range model.Applications {
		count := app.EndpointCount
		if count == 0 && len(app.Endpoints) > 0 {
			count = len(app.Endpoints)
		}
		endpointCount += count
		items = append(items, applicationItem{
			Name:          app.Name,
			EndpointCount: count,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(applicationsResponse{
		ApplicationCount: len(items),
		EndpointCount:    endpointCount,
		Items:            items,
	})
}

func (s *Server) handleApplicationEndpoints(w http.ResponseWriter, application string) {
	if s.ManagementModel == nil {
		writeManagementError(w, http.StatusServiceUnavailable, readCodeManagementUnavailable, "management model is not configured")
		return
	}

	model := s.ManagementModel()
	for _, app := range model.Applications {
		if app.Name != application {
			continue
		}
		items := app.Endpoints
		if items == nil {
			items = []ManagementEndpoint{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(applicationEndpointsResponse{
			Application:   app.Name,
			EndpointCount: len(items),
			Items:         items,
		})
		return
	}
	writeManagementError(w, http.StatusNotFound, readCodeNotFound, "application not found")
}

func (s *Server) handleApplicationEndpoint(w http.ResponseWriter, application, endpointName string) {
	if s.ManagementModel == nil {
		writeManagementError(w, http.StatusServiceUnavailable, readCodeManagementUnavailable, "management model is not configured")
		return
	}

	endpoint, ok := s.lookupManagementEndpoint(application, endpointName)
	if !ok {
		writeManagementError(w, http.StatusNotFound, managementCodeEndpointNotFound, "management endpoint not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(applicationEndpointResponse{
		Application:   application,
		EndpointName:  endpointName,
		Route:         endpoint.Route,
		Mode:          endpoint.Mode,
		Targets:       endpoint.Targets,
		PublishPolicy: endpoint.PublishPolicy,
	})
}

func (s *Server) handleApplicationEndpointUpsert(w http.ResponseWriter, r *http.Request, application, endpointName string) {
	if s.UpsertManagedEndpoint == nil {
		writeManagementError(w, http.StatusServiceUnavailable, managementCodeUnavailable, "management endpoint mutation is not configured")
		return
	}

	route, detail, ok := parseManagementEndpointUpsert(r)
	if !ok {
		if strings.TrimSpace(detail) == "" {
			detail = "request body must be valid JSON with route"
		}
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, detail)
		return
	}
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "invalid or missing audit headers")
		return
	}
	if code, detail := s.mutationAuditPolicyError(audit, true, "endpoint mapping mutation"); code != "" {
		writeManagementError(w, http.StatusBadRequest, code, detail)
		return
	}

	result, err := s.UpsertManagedEndpoint(ManagementEndpointUpsertRequest{
		Application:  application,
		EndpointName: endpointName,
		Route:        route,
		Reason:       audit.Reason,
		Actor:        audit.Actor,
		RequestID:    audit.RequestID,
	})
	if err != nil {
		status, code, detail := managementEndpointMutationError(err)
		writeManagementError(w, status, code, detail)
		return
	}

	resolvedRoute := strings.TrimSpace(result.Route)
	if resolvedRoute == "" {
		resolvedRoute = route
	}
	action := strings.TrimSpace(result.Action)
	if action == "" {
		if result.Applied {
			action = "updated"
		} else {
			action = "noop"
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(applicationEndpointMutationResponse{
		Applied: result.Applied,
		Action:  action,
		Endpoint: s.managementEndpointProjection(
			application,
			endpointName,
			resolvedRoute,
			action != "deleted",
		),
		Scope: &managementScope{
			Application:  application,
			EndpointName: endpointName,
			Route:        resolvedRoute,
		},
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	})

	changed := 0
	if result.Applied {
		changed = 1
	}
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:           time.Now().UTC(),
		Operation:    "upsert_endpoint",
		Application:  application,
		EndpointName: endpointName,
		Route:        resolvedRoute,
		Matched:      1,
		Changed:      changed,
		Reason:       audit.Reason,
		Actor:        audit.Actor,
		RequestID:    audit.RequestID,
	})
}

func (s *Server) handleApplicationEndpointDelete(w http.ResponseWriter, r *http.Request, application, endpointName string) {
	if s.DeleteManagedEndpoint == nil {
		writeManagementError(w, http.StatusServiceUnavailable, managementCodeUnavailable, "management endpoint mutation is not configured")
		return
	}

	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "invalid or missing audit headers")
		return
	}
	if code, detail := s.mutationAuditPolicyError(audit, true, "endpoint mapping mutation"); code != "" {
		writeManagementError(w, http.StatusBadRequest, code, detail)
		return
	}

	result, err := s.DeleteManagedEndpoint(ManagementEndpointDeleteRequest{
		Application:  application,
		EndpointName: endpointName,
		Reason:       audit.Reason,
		Actor:        audit.Actor,
		RequestID:    audit.RequestID,
	})
	if err != nil {
		status, code, detail := managementEndpointMutationError(err)
		writeManagementError(w, status, code, detail)
		return
	}

	resolvedRoute := strings.TrimSpace(result.Route)
	action := strings.TrimSpace(result.Action)
	if action == "" {
		if result.Applied {
			action = "deleted"
		} else {
			action = "noop"
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(applicationEndpointMutationResponse{
		Applied: result.Applied,
		Action:  action,
		Endpoint: s.managementEndpointProjection(
			application,
			endpointName,
			resolvedRoute,
			false,
		),
		Scope: &managementScope{
			Application:  application,
			EndpointName: endpointName,
			Route:        resolvedRoute,
		},
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	})

	changed := 0
	if result.Applied {
		changed = 1
	}
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:           time.Now().UTC(),
		Operation:    "delete_endpoint",
		Application:  application,
		EndpointName: endpointName,
		Route:        resolvedRoute,
		Matched:      1,
		Changed:      changed,
		Reason:       audit.Reason,
		Actor:        audit.Actor,
		RequestID:    audit.RequestID,
	})
}

func (s *Server) handleApplicationEndpointMessages(w http.ResponseWriter, r *http.Request, application, endpointName string) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}
	route, status, code, detail, ok := s.resolveManagedPathRoute(application, endpointName)
	if !ok {
		if code != "" {
			writeManagementError(w, status, code, detail)
			return
		}
		writeManagementPathResolveError(w, status)
		return
	}

	q := r.URL.Query()
	if hasScopedManagedSelectorHints(
		strings.TrimSpace(q.Get("route")),
		strings.TrimSpace(q.Get("application")),
		strings.TrimSpace(q.Get("endpoint_name")),
	) {
		writeManagementError(w, http.StatusBadRequest, publishCodeScopedSelectorForbidden, "selector hints are not allowed on endpoint-scoped messages path")
		return
	}

	limit := defaultListLimit
	if v := strings.TrimSpace(q.Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	before, ok := parseTimeParam(strings.TrimSpace(q.Get("before")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "before must be RFC3339")
		return
	}

	includePayload, ok := parseBoolParam(strings.TrimSpace(q.Get("include_payload")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "include_payload must be true|false")
		return
	}
	includeHeaders, ok := parseBoolParam(strings.TrimSpace(q.Get("include_headers")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "include_headers must be true|false")
		return
	}
	includeTrace, ok := parseBoolParam(strings.TrimSpace(q.Get("include_trace")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "include_trace must be true|false")
		return
	}

	state, ok := parseQueueState(strings.TrimSpace(q.Get("state")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "state must be one of queued|leased|delivered|dead|canceled")
		return
	}

	resp, err := s.Store.ListMessages(queue.MessageListRequest{
		Route:          route,
		Target:         strings.TrimSpace(q.Get("target")),
		State:          state,
		Limit:          limit,
		Before:         before,
		IncludePayload: includePayload,
		IncludeHeaders: includeHeaders,
		IncludeTrace:   includeTrace,
	})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	out := messagesResponse{Items: make([]messageItem, 0, len(resp.Items))}
	for _, it := range resp.Items {
		item := messageItem{
			ID:         it.ID,
			Route:      it.Route,
			Target:     it.Target,
			State:      it.State,
			ReceivedAt: it.ReceivedAt,
			Attempt:    it.Attempt,
			NextRunAt:  it.NextRunAt,
			DeadReason: it.DeadReason,
		}
		if includePayload && len(it.Payload) > 0 {
			item.PayloadB64 = base64.StdEncoding.EncodeToString(it.Payload)
		}
		if includeHeaders && len(it.Headers) > 0 {
			item.Headers = it.Headers
		}
		if includeTrace && len(it.Trace) > 0 {
			item.Trace = it.Trace
		}
		out.Items = append(out.Items, item)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleApplicationEndpointPublish(w http.ResponseWriter, r *http.Request, application, endpointName string) {
	if !s.PublishScopedManagedEnabled {
		s.writePublishError(w, http.StatusForbidden, publishCodeScopedPublishDisabled, "endpoint-scoped publish path is disabled by defaults.publish_policy.managed", -1, true)
		return
	}
	if s.Store == nil {
		s.writePublishError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable", -1, true)
		return
	}
	route, targets, status, code, detail, ok := s.resolveManagedEndpointPublishScope(application, endpointName)
	if !ok {
		if code != "" {
			s.writePublishError(w, status, code, detail, -1, true)
			return
		}
		switch status {
		case http.StatusNotFound:
			s.writePublishError(w, http.StatusNotFound, publishCodeManagedEndpointNotFound, "managed endpoint not found", -1, true)
		default:
			s.writePublishError(w, http.StatusServiceUnavailable, publishCodeManagedResolverMissing, "managed endpoint resolution is not configured", -1, true)
		}
		return
	}
	if len(targets) == 0 {
		s.writePublishError(w, http.StatusBadRequest, publishCodeManagedEndpointNoTargets, "managed endpoint has no publishable targets", -1, true)
		return
	}

	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		s.writePublishError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "X-Hookaido-Audit-Reason is required", -1, true)
		return
	}
	if code, detail := s.mutationAuditPolicyError(audit, true, "publish"); code != "" {
		s.writePublishError(w, http.StatusBadRequest, code, detail, -1, true)
		return
	}
	if code, detail := s.publishRoutePolicyError(route, targets, true); code != "" {
		s.writePublishError(w, http.StatusForbidden, code, detail, -1, true)
		return
	}

	items, parseErr := parseScopedPublishItems(r)
	if parseErr != nil {
		s.writePublishError(w, http.StatusBadRequest, parseErr.Code, parseErr.Detail, parseErr.ItemIndex, true)
		return
	}

	prepared := make([]queue.Envelope, 0, len(items))
	for idx, item := range items {
		if publishItemHasSelectorHints(item) {
			s.writePublishError(w, http.StatusBadRequest, publishCodeScopedSelectorForbidden, "item selector hints are not allowed on endpoint-scoped publish", idx, true)
			return
		}
		if !validateScopedManagedSelector(
			route,
			application,
			endpointName,
			strings.TrimSpace(item.Route),
			strings.TrimSpace(item.Application),
			strings.TrimSpace(item.EndpointName),
		) {
			s.writePublishError(w, http.StatusBadRequest, publishCodeScopedSelectorMismatch, "item selector does not match endpoint scope", idx, true)
			return
		}

		target, ok := resolvePublishTarget(strings.TrimSpace(item.Target), targets)
		if !ok {
			s.writePublishError(w, http.StatusBadRequest, publishCodeTargetUnresolvable, publishTargetUnresolvableDetail(route, strings.TrimSpace(item.Target), targets), idx, true)
			return
		}
		env, code, codeDetail := publishEnvelopeFromItem(
			item,
			route,
			target,
			s.publishMaxBodyBytes(route),
			s.publishMaxHeaderBytes(route),
		)
		if code != "" {
			switch code {
			case publishCodeInvalidReceivedAt:
				s.writePublishError(w, http.StatusBadRequest, code, "item.received_at must be RFC3339", idx, true)
			case publishCodeInvalidNextRunAt:
				s.writePublishError(w, http.StatusBadRequest, code, "item.next_run_at must be RFC3339", idx, true)
			case publishCodeInvalidHeader:
				if strings.TrimSpace(codeDetail) == "" {
					codeDetail = "item.headers contains an invalid HTTP header name or value"
				}
				s.writePublishError(w, http.StatusBadRequest, code, codeDetail, idx, true)
			case publishCodePayloadTooLarge:
				if strings.TrimSpace(codeDetail) == "" {
					codeDetail = "item payload exceeds max_body for route"
				}
				s.writePublishError(w, http.StatusRequestEntityTooLarge, code, codeDetail, idx, true)
			case publishCodeHeadersTooLarge:
				if strings.TrimSpace(codeDetail) == "" {
					codeDetail = "item headers exceed max_headers for route"
				}
				s.writePublishError(w, http.StatusRequestEntityTooLarge, code, codeDetail, idx, true)
			default:
				s.writePublishError(w, http.StatusBadRequest, code, "item.payload_b64 must be valid base64", idx, true)
			}
			return
		}

		prepared = append(prepared, env)
	}
	if dupIdx, dupID, err := firstExistingMessageIDIndex(s.Store, publishEnvelopeIDs(prepared)); err != nil {
		s.writePublishError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable", -1, true)
		return
	} else if dupIdx >= 0 {
		s.writePublishError(w, http.StatusConflict, publishCodeDuplicateID, fmt.Sprintf("item.id %q already exists", dupID), dupIdx, true)
		return
	}

	published := 0
	auditTarget := ""
	singleTarget := true

	if batcher, ok := s.Store.(queue.BatchEnqueuer); ok {
		n, err := batcher.EnqueueBatch(prepared)
		if err != nil {
			if errors.Is(err, queue.ErrEnvelopeExists) {
				s.writePublishError(w, http.StatusConflict, publishCodeDuplicateID, "batch contains a duplicate item.id", -1, true)
				return
			}
			if errors.Is(err, queue.ErrQueueFull) {
				s.writePublishError(w, http.StatusServiceUnavailable, publishCodeQueueFull, "queue is full", -1, true)
				return
			}
			s.writePublishError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable", -1, true)
			return
		}
		published = n
		for _, env := range prepared {
			if auditTarget == "" {
				auditTarget = env.Target
			} else if env.Target != auditTarget {
				singleTarget = false
			}
		}
	} else {
		for idx, env := range prepared {
			if err := s.Store.Enqueue(env); err != nil {
				if errors.Is(err, queue.ErrEnvelopeExists) {
					s.writePublishError(w, http.StatusConflict, publishCodeDuplicateID, fmt.Sprintf("item.id %q already exists", env.ID), idx, true)
					return
				}
				if errors.Is(err, queue.ErrQueueFull) {
					s.writePublishError(w, http.StatusServiceUnavailable, publishCodeQueueFull, "queue is full", idx, true)
					return
				}
				s.writePublishError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable", idx, true)
				return
			}

			if published == 0 {
				auditTarget = env.Target
			} else if env.Target != auditTarget {
				singleTarget = false
			}
			published++
		}
	}
	if !singleTarget {
		auditTarget = ""
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(messagesPublishResponse{
		Published: published,
		Scope: &managementScope{
			Application:  application,
			EndpointName: endpointName,
			Route:        route,
		},
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	})
	s.observePublishAccepted(published, true)
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:           time.Now().UTC(),
		Operation:    "publish_messages",
		Application:  application,
		EndpointName: endpointName,
		Route:        route,
		Target:       auditTarget,
		State:        string(queue.StateQueued),
		Limit:        len(items),
		Matched:      len(items),
		Changed:      published,
		Reason:       audit.Reason,
		Actor:        audit.Actor,
		RequestID:    audit.RequestID,
	})
}

func (s *Server) handleApplicationEndpointCancelByFilter(w http.ResponseWriter, r *http.Request, application, endpointName string) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}
	route, status, code, detail, ok := s.resolveManagedPathRoute(application, endpointName)
	if !ok {
		if code != "" {
			writeManagementError(w, status, code, detail)
			return
		}
		writeManagementPathResolveError(w, status)
		return
	}

	req, app, ep, ok := parseMessageManageFilter(r, map[queue.State]struct{}{
		queue.StateQueued: {},
		queue.StateLeased: {},
		queue.StateDead:   {},
	})
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, "request body is invalid for cancel_by_filter")
		return
	}
	if hasScopedManagedSelectorHints(req.Route, app, ep) {
		writeManagementError(w, http.StatusBadRequest, publishCodeScopedSelectorForbidden, "selector hints are not allowed on endpoint-scoped filter path")
		return
	}
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "X-Hookaido-Audit-Reason is required")
		return
	}
	if code, detail := s.mutationAuditPolicyError(audit, true, "mutation"); code != "" {
		writeManagementError(w, http.StatusBadRequest, code, detail)
		return
	}
	req.Route = route

	resp, err := s.Store.CancelMessagesByFilter(req)
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	out := messagesManageFilterResponse{
		Matched:     resp.Matched,
		Canceled:    resp.Canceled,
		PreviewOnly: resp.PreviewOnly,
		Scope: &managementScope{
			Application:  application,
			EndpointName: endpointName,
			Route:        route,
		},
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	}
	_ = json.NewEncoder(w).Encode(out)
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:           time.Now().UTC(),
		Operation:    "cancel_by_filter",
		Application:  application,
		EndpointName: endpointName,
		Route:        route,
		Target:       req.Target,
		State:        string(req.State),
		Limit:        req.Limit,
		PreviewOnly:  req.PreviewOnly,
		Matched:      resp.Matched,
		Changed:      resp.Canceled,
		Reason:       audit.Reason,
		Actor:        audit.Actor,
		RequestID:    audit.RequestID,
	})
}

func (s *Server) handleApplicationEndpointRequeueByFilter(w http.ResponseWriter, r *http.Request, application, endpointName string) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}
	route, status, code, detail, ok := s.resolveManagedPathRoute(application, endpointName)
	if !ok {
		if code != "" {
			writeManagementError(w, status, code, detail)
			return
		}
		writeManagementPathResolveError(w, status)
		return
	}

	req, app, ep, ok := parseMessageManageFilter(r, map[queue.State]struct{}{
		queue.StateDead:     {},
		queue.StateCanceled: {},
	})
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, "request body is invalid for requeue_by_filter")
		return
	}
	if hasScopedManagedSelectorHints(req.Route, app, ep) {
		writeManagementError(w, http.StatusBadRequest, publishCodeScopedSelectorForbidden, "selector hints are not allowed on endpoint-scoped filter path")
		return
	}
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "X-Hookaido-Audit-Reason is required")
		return
	}
	if code, detail := s.mutationAuditPolicyError(audit, true, "mutation"); code != "" {
		writeManagementError(w, http.StatusBadRequest, code, detail)
		return
	}
	req.Route = route

	resp, err := s.Store.RequeueMessagesByFilter(req)
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	out := messagesManageFilterResponse{
		Matched:     resp.Matched,
		Requeued:    resp.Requeued,
		PreviewOnly: resp.PreviewOnly,
		Scope: &managementScope{
			Application:  application,
			EndpointName: endpointName,
			Route:        route,
		},
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	}
	_ = json.NewEncoder(w).Encode(out)
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:           time.Now().UTC(),
		Operation:    "requeue_by_filter",
		Application:  application,
		EndpointName: endpointName,
		Route:        route,
		Target:       req.Target,
		State:        string(req.State),
		Limit:        req.Limit,
		PreviewOnly:  req.PreviewOnly,
		Matched:      resp.Matched,
		Changed:      resp.Requeued,
		Reason:       audit.Reason,
		Actor:        audit.Actor,
		RequestID:    audit.RequestID,
	})
}

func (s *Server) handleApplicationEndpointResumeByFilter(w http.ResponseWriter, r *http.Request, application, endpointName string) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}
	route, status, code, detail, ok := s.resolveManagedPathRoute(application, endpointName)
	if !ok {
		if code != "" {
			writeManagementError(w, status, code, detail)
			return
		}
		writeManagementPathResolveError(w, status)
		return
	}

	req, app, ep, ok := parseMessageManageFilter(r, map[queue.State]struct{}{
		queue.StateCanceled: {},
	})
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, "request body is invalid for resume_by_filter")
		return
	}
	if hasScopedManagedSelectorHints(req.Route, app, ep) {
		writeManagementError(w, http.StatusBadRequest, publishCodeScopedSelectorForbidden, "selector hints are not allowed on endpoint-scoped filter path")
		return
	}
	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "X-Hookaido-Audit-Reason is required")
		return
	}
	if code, detail := s.mutationAuditPolicyError(audit, true, "mutation"); code != "" {
		writeManagementError(w, http.StatusBadRequest, code, detail)
		return
	}
	req.Route = route

	resp, err := s.Store.ResumeMessagesByFilter(req)
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	out := messagesManageFilterResponse{
		Matched:     resp.Matched,
		Resumed:     resp.Resumed,
		PreviewOnly: resp.PreviewOnly,
		Scope: &managementScope{
			Application:  application,
			EndpointName: endpointName,
			Route:        route,
		},
		Audit: &managementAudit{
			Reason:    audit.Reason,
			Actor:     audit.Actor,
			RequestID: audit.RequestID,
		},
	}
	_ = json.NewEncoder(w).Encode(out)
	s.emitManagementMutationAudit(ManagementMutationAuditEvent{
		At:           time.Now().UTC(),
		Operation:    "resume_by_filter",
		Application:  application,
		EndpointName: endpointName,
		Route:        route,
		Target:       req.Target,
		State:        string(req.State),
		Limit:        req.Limit,
		PreviewOnly:  req.PreviewOnly,
		Matched:      resp.Matched,
		Changed:      resp.Resumed,
		Reason:       audit.Reason,
		Actor:        audit.Actor,
		RequestID:    audit.RequestID,
	})
}

func (s *Server) lookupManagementEndpoint(application, endpointName string) (ManagementEndpoint, bool) {
	if s.ManagementModel == nil {
		return ManagementEndpoint{}, false
	}
	model := s.ManagementModel()
	for _, app := range model.Applications {
		if app.Name != application {
			continue
		}
		for _, endpoint := range app.Endpoints {
			if endpoint.Name == endpointName {
				return endpoint, true
			}
		}
		return ManagementEndpoint{}, false
	}
	return ManagementEndpoint{}, false
}

func (s *Server) lookupManagementEndpointByRoute(route string) (string, string, bool) {
	application, endpointName, managed, _ := s.lookupManagementEndpointByRouteStatus(route)
	return application, endpointName, managed
}

type managedRouteOwnershipStatus struct {
	Application        string
	EndpointName       string
	Managed            bool
	Available          bool
	SourceMismatch     bool
	SourceMismatchInfo string
}

func (s *Server) lookupManagementEndpointByRouteOwnershipStatus(route string) managedRouteOwnershipStatus {
	route = strings.TrimSpace(route)
	if route == "" {
		return managedRouteOwnershipStatus{
			Available: true,
		}
	}

	modelApp, modelEndpoint, modelManaged, modelAvailable := s.lookupManagementEndpointByRouteStatusFromModel(route)
	policyApp, policyEndpoint, policyManaged, policyAvailable := s.lookupManagementEndpointByRouteStatusFromPolicy(route)

	out := managedRouteOwnershipStatus{}
	switch {
	case modelAvailable:
		out.Application = modelApp
		out.EndpointName = modelEndpoint
		out.Managed = modelManaged
		out.Available = true
	case policyAvailable:
		out.Application = policyApp
		out.EndpointName = policyEndpoint
		out.Managed = policyManaged
		out.Available = true
	default:
		return out
	}

	if modelAvailable && policyAvailable {
		mismatch := modelManaged != policyManaged
		if !mismatch && modelManaged && (modelApp != policyApp || modelEndpoint != policyEndpoint) {
			mismatch = true
		}
		if mismatch {
			out.SourceMismatch = true
			out.SourceMismatchInfo = managedOwnershipSourceMismatchDetail(
				route,
				modelApp,
				modelEndpoint,
				modelManaged,
				policyApp,
				policyEndpoint,
				policyManaged,
			)
		}
	}

	return out
}

func (s *Server) lookupManagementEndpointByRouteStatusFromModel(route string) (string, string, bool, bool) {
	if s == nil || s.ManagementModel == nil {
		return "", "", false, false
	}
	model := s.ManagementModel()
	for _, app := range model.Applications {
		for _, endpoint := range app.Endpoints {
			if strings.TrimSpace(endpoint.Route) == route {
				return strings.TrimSpace(app.Name), strings.TrimSpace(endpoint.Name), true, true
			}
		}
	}
	return "", "", false, true
}

func (s *Server) lookupManagementEndpointByRouteStatusFromPolicy(route string) (string, string, bool, bool) {
	if s == nil || s.ManagedRouteInfoForRoute == nil {
		return "", "", false, false
	}
	application, endpointName, managed, available := s.ManagedRouteInfoForRoute(route)
	if !available {
		return "", "", false, false
	}
	application = strings.TrimSpace(application)
	endpointName = strings.TrimSpace(endpointName)
	if !managed {
		return "", "", false, true
	}
	return application, endpointName, true, true
}

func (s *Server) lookupManagementEndpointByRouteStatus(route string) (string, string, bool, bool) {
	status := s.lookupManagementEndpointByRouteOwnershipStatus(route)
	return status.Application, status.EndpointName, status.Managed, status.Available
}

func (s *Server) scopedManagedMutationByIDs(ids []string, allowedStates map[queue.State]struct{}) (bool, error) {
	if s == nil || s.Store == nil || len(ids) == 0 {
		return false, nil
	}
	managedRoutes, managedRoutesAvailable := s.managedRouteSet()
	lookup, err := s.Store.LookupMessages(queue.MessageLookupRequest{IDs: ids})
	if err != nil {
		return false, err
	}
	touchesManaged := false
	missingPolicyContext := false
	for _, item := range lookup.Items {
		if len(allowedStates) > 0 {
			if _, ok := allowedStates[item.State]; !ok {
				continue
			}
		}
		route := strings.TrimSpace(item.Route)
		if route == "" {
			continue
		}
		ownership := s.lookupManagementEndpointByRouteOwnershipStatus(route)
		if ownership.SourceMismatch {
			return false, &managedOwnershipMismatchError{detail: ownership.SourceMismatchInfo}
		}
		managed := ownership.Available && ownership.Managed
		if managedRoutesAvailable {
			if _, inSet := managedRoutes[route]; inSet {
				managed = true
			}
		} else if !ownership.Available {
			missingPolicyContext = true
		}
		if managed {
			touchesManaged = true
		}
	}
	if !managedRoutesAvailable && !touchesManaged && missingPolicyContext && s.PublishScopedManagedFailClosed && s.publishScopedManagedIDPolicyEnabled() {
		return false, errManagedPolicyLookupMissing
	}
	return touchesManaged, nil
}

func (s *Server) managedRouteSet() (map[string]struct{}, bool) {
	if s == nil {
		return nil, false
	}

	modelRoutes, modelAvailable := s.managedRouteSetFromModel()
	policyRoutes, policyAvailable := s.managedRouteSetFromPolicy()
	switch {
	case modelAvailable && policyAvailable:
		out := make(map[string]struct{}, len(modelRoutes)+len(policyRoutes))
		for route := range modelRoutes {
			out[route] = struct{}{}
		}
		for route := range policyRoutes {
			out[route] = struct{}{}
		}
		return out, true
	case modelAvailable:
		return modelRoutes, true
	case policyAvailable:
		return policyRoutes, true
	default:
		return nil, false
	}
}

func (s *Server) managedRouteSetFromPolicy() (map[string]struct{}, bool) {
	if s == nil || s.ManagedRouteSet == nil {
		return nil, false
	}
	routes, available := s.ManagedRouteSet()
	if !available {
		return nil, false
	}
	out := make(map[string]struct{}, len(routes))
	for rawRoute := range routes {
		route := strings.TrimSpace(rawRoute)
		if route == "" {
			continue
		}
		out[route] = struct{}{}
	}
	return out, true
}

func (s *Server) managedRouteSetFromModel() (map[string]struct{}, bool) {
	if s == nil || s.ManagementModel == nil {
		return nil, false
	}
	model := s.ManagementModel()
	out := make(map[string]struct{}, model.EndpointCount)
	for _, app := range model.Applications {
		for _, endpoint := range app.Endpoints {
			route := strings.TrimSpace(endpoint.Route)
			if route == "" {
				continue
			}
			out[route] = struct{}{}
		}
	}
	return out, true
}

func (s *Server) writeScopedManagedIDMutationError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, errManagedPolicyLookupMissing) {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeManagedResolverMissing, "management model is unavailable for scoped id mutation policy evaluation")
		return
	}
	var ownershipErr *managedOwnershipMismatchError
	if errors.As(err, &ownershipErr) {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeManagedTargetMismatch, ownershipErr.Error())
		return
	}
	writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
}

func (s *Server) writeScopedManagedFilterMutationError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, errManagedPolicyLookupMissing) {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeManagedResolverMissing, "management model is unavailable for route selector policy evaluation")
		return
	}
	var ownershipErr *managedOwnershipMismatchError
	if errors.As(err, &ownershipErr) {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeManagedTargetMismatch, ownershipErr.Error())
		return
	}
	writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
}

func (s *Server) resolveManagedEndpointScope(application, endpointName string) (string, []string, int, bool) {
	application = strings.TrimSpace(application)
	endpointName = strings.TrimSpace(endpointName)
	if application == "" || endpointName == "" {
		return "", nil, http.StatusBadRequest, false
	}

	if s != nil && s.ManagementModel != nil {
		endpoint, ok := s.lookupManagementEndpoint(application, endpointName)
		if !ok {
			return "", nil, http.StatusNotFound, false
		}
		route := strings.TrimSpace(endpoint.Route)
		if route == "" {
			return "", nil, http.StatusNotFound, false
		}
		return route, normalizePublishTargets(endpoint.Targets), 0, true
	}

	if s == nil || s.ResolveManaged == nil {
		return "", nil, http.StatusServiceUnavailable, false
	}
	route, targets, ok := s.ResolveManaged(application, endpointName)
	if !ok || strings.TrimSpace(route) == "" {
		return "", nil, http.StatusNotFound, false
	}
	return strings.TrimSpace(route), normalizePublishTargets(targets), 0, true
}

func (s *Server) resolveManagedEndpointOwnedScope(application, endpointName string) (string, []string, int, string, string, bool) {
	route, targets, status, ok := s.resolveManagedEndpointScope(application, endpointName)
	if !ok {
		return "", nil, status, "", "", false
	}

	expectedApplication := strings.TrimSpace(application)
	expectedEndpointName := strings.TrimSpace(endpointName)
	if expectedApplication == "" || expectedEndpointName == "" {
		return route, targets, 0, "", "", true
	}

	routeOwnership := s.lookupManagementEndpointByRouteOwnershipStatus(route)
	if routeOwnership.SourceMismatch {
		return "", nil, http.StatusServiceUnavailable, publishCodeManagedTargetMismatch, routeOwnership.SourceMismatchInfo, false
	}

	routeApplication := routeOwnership.Application
	routeEndpointName := routeOwnership.EndpointName
	managed := routeOwnership.Managed
	available := routeOwnership.Available
	if available && (!managed || routeApplication != expectedApplication || routeEndpointName != expectedEndpointName) {
		return "", nil, http.StatusServiceUnavailable, publishCodeManagedTargetMismatch,
			managedOwnershipMismatchDetail(
				expectedApplication,
				expectedEndpointName,
				route,
				routeApplication,
				routeEndpointName,
				managed,
			), false
	}

	return route, targets, 0, "", "", true
}

func (s *Server) resolveManagedEndpointPublishScope(application, endpointName string) (string, []string, int, string, string, bool) {
	return s.resolveManagedEndpointAlignedScope(application, endpointName)
}

func (s *Server) resolveManagedEndpointAlignedScope(application, endpointName string) (string, []string, int, string, string, bool) {
	route, targets, status, code, detail, ok := s.resolveManagedEndpointOwnedScope(application, endpointName)
	if !ok {
		return "", nil, status, code, detail, false
	}
	if s == nil || s.TargetsForRoute == nil {
		return route, targets, 0, "", "", true
	}

	routeTargets := normalizePublishTargets(s.TargetsForRoute(route))
	if publishTargetSetsEqual(targets, routeTargets) {
		return route, routeTargets, 0, "", "", true
	}

	return "", nil, http.StatusServiceUnavailable, publishCodeManagedTargetMismatch,
		managedTargetMismatchDetail(application, endpointName, route, targets, routeTargets), false
}

func publishTargetSetsEqual(left, right []string) bool {
	left = normalizePublishTargets(left)
	right = normalizePublishTargets(right)
	if len(left) != len(right) {
		return false
	}
	if len(left) == 0 {
		return true
	}

	set := make(map[string]struct{}, len(left))
	for _, target := range left {
		set[target] = struct{}{}
	}
	for _, target := range right {
		if _, ok := set[target]; !ok {
			return false
		}
	}
	return true
}

func managedTargetMismatchDetail(application, endpointName, route string, managedTargets, routeTargets []string) string {
	return fmt.Sprintf(
		"managed endpoint (%q, %q) targets for route %q are out of sync (managed=%s, route_resolver=%s)",
		strings.TrimSpace(application),
		strings.TrimSpace(endpointName),
		strings.TrimSpace(route),
		formatAllowedTargetsForError(managedTargets),
		formatAllowedTargetsForError(routeTargets),
	)
}

func managedOwnershipMismatchDetail(application, endpointName, route, routeApplication, routeEndpointName string, managed bool) string {
	application = strings.TrimSpace(application)
	endpointName = strings.TrimSpace(endpointName)
	route = strings.TrimSpace(route)
	routeApplication = strings.TrimSpace(routeApplication)
	routeEndpointName = strings.TrimSpace(routeEndpointName)
	if !managed {
		return fmt.Sprintf(
			"managed endpoint (%q, %q) resolved route %q is not managed by route ownership policy",
			application,
			endpointName,
			route,
		)
	}
	return fmt.Sprintf(
		"managed endpoint (%q, %q) resolved route %q is owned by (%q, %q) in route ownership policy",
		application,
		endpointName,
		route,
		routeApplication,
		routeEndpointName,
	)
}

func managedOwnershipSourceMismatchDetail(route, modelApplication, modelEndpointName string, modelManaged bool, policyApplication, policyEndpointName string, policyManaged bool) string {
	route = strings.TrimSpace(route)
	modelApplication = strings.TrimSpace(modelApplication)
	modelEndpointName = strings.TrimSpace(modelEndpointName)
	policyApplication = strings.TrimSpace(policyApplication)
	policyEndpointName = strings.TrimSpace(policyEndpointName)

	modelOwner := "unmanaged"
	if modelManaged {
		modelOwner = fmt.Sprintf("(%q, %q)", modelApplication, modelEndpointName)
	}
	policyOwner := "unmanaged"
	if policyManaged {
		policyOwner = fmt.Sprintf("(%q, %q)", policyApplication, policyEndpointName)
	}
	return fmt.Sprintf(
		"route %q ownership sources are out of sync (management_model=%s, route_policy=%s)",
		route,
		modelOwner,
		policyOwner,
	)
}

func normalizePublishTargets(targets []string) []string {
	if len(targets) == 0 {
		return nil
	}
	out := make([]string, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, raw := range targets {
		target := strings.TrimSpace(raw)
		if target == "" {
			continue
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	return out
}

func (s *Server) resolveManagedPathRoute(application, endpointName string) (string, int, string, string, bool) {
	route, _, status, code, detail, ok := s.resolveManagedEndpointAlignedScope(application, endpointName)
	if !ok {
		return "", status, code, detail, false
	}
	return route, 0, "", "", true
}

func validateScopedManagedSelector(route, application, endpointName, routeHint, applicationHint, endpointHint string) bool {
	routeHint = strings.TrimSpace(routeHint)
	applicationHint = strings.TrimSpace(applicationHint)
	endpointHint = strings.TrimSpace(endpointHint)
	if routeHint != "" && routeHint != route {
		return false
	}
	if (applicationHint == "") != (endpointHint == "") {
		return false
	}
	if applicationHint != "" && (applicationHint != application || endpointHint != endpointName) {
		return false
	}
	return true
}

func validateManagedSelectorLabels(application, endpointName string) (string, bool) {
	application = strings.TrimSpace(application)
	endpointName = strings.TrimSpace(endpointName)
	if application != "" && !config.IsValidManagementLabel(application) {
		return fmt.Sprintf("application must match %s", config.ManagementLabelPattern()), false
	}
	if endpointName != "" && !config.IsValidManagementLabel(endpointName) {
		return fmt.Sprintf("endpoint_name must match %s", config.ManagementLabelPattern()), false
	}
	return "", true
}

func publishItemHasSelectorHints(item messagesPublishItem) bool {
	return strings.TrimSpace(item.Route) != "" ||
		strings.TrimSpace(item.Application) != "" ||
		strings.TrimSpace(item.EndpointName) != ""
}

func hasScopedManagedSelectorHints(routeHint, applicationHint, endpointHint string) bool {
	return strings.TrimSpace(routeHint) != "" ||
		strings.TrimSpace(applicationHint) != "" ||
		strings.TrimSpace(endpointHint) != ""
}

func managedSelectorRequiredDetail(route string) string {
	route = strings.TrimSpace(route)
	if route != "" {
		return fmt.Sprintf("route %q is managed by application/endpoint; use application+endpoint_name selector", route)
	}
	return "managed routes are present; use application+endpoint_name selector"
}

func (s *Server) filterMutationTouchesManagedRoute(route, application, endpointName string) (bool, error) {
	route = strings.TrimSpace(route)
	application = strings.TrimSpace(application)
	endpointName = strings.TrimSpace(endpointName)
	if application != "" && endpointName != "" {
		return true, nil
	}
	var routeOwnership managedRouteOwnershipStatus
	if route != "" {
		routeOwnership = s.lookupManagementEndpointByRouteOwnershipStatus(route)
		if routeOwnership.SourceMismatch {
			return false, &managedOwnershipMismatchError{detail: routeOwnership.SourceMismatchInfo}
		}
	}

	managedRoutes, available := s.managedRouteSet()
	if route != "" {
		managedByRouteOwnership := routeOwnership.Available && routeOwnership.Managed
		if available {
			if _, managed := managedRoutes[route]; managed {
				managedByRouteOwnership = true
			}
			return managedByRouteOwnership, nil
		}
		if routeOwnership.Available {
			return managedByRouteOwnership, nil
		}
		if s.PublishScopedManagedFailClosed {
			return false, errManagedPolicyLookupMissing
		}
		return false, nil
	}
	if !available {
		if s.PublishScopedManagedFailClosed {
			return false, errManagedPolicyLookupMissing
		}
		return false, nil
	}
	if len(managedRoutes) == 0 {
		return false, nil
	}
	return true, nil
}

func parseManagementAudit(r *http.Request, requireReason bool) (managementAudit, bool) {
	if r == nil {
		return managementAudit{}, !requireReason
	}

	reason := strings.TrimSpace(r.Header.Get(auditReasonHeader))
	actor := strings.TrimSpace(r.Header.Get(auditActorHeader))
	requestID := strings.TrimSpace(r.Header.Get(auditRequestIDHeader))
	if requireReason && reason == "" {
		return managementAudit{}, false
	}
	if len(reason) > maxAuditReasonLength || len(actor) > maxAuditActorLength || len(requestID) > maxAuditRequestIDSize {
		return managementAudit{}, false
	}

	return managementAudit{
		Reason:    reason,
		Actor:     actor,
		RequestID: requestID,
	}, true
}

func parseManagementEndpointUpsert(r *http.Request) (string, string, bool) {
	if r == nil {
		return "", "request body is missing", false
	}
	var payload managementEndpointUpsertPayload
	if err := decodeJSONBodyStrict(r, &payload); err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			return "", fmt.Sprintf("request body exceeds %d bytes", defaultMaxBodyBytes), false
		}
		return "", "request body must be valid JSON with route", false
	}
	route, ok := parseOptionalRoutePath(payload.Route)
	if !ok {
		return "", "route must start with '/'", false
	}
	if route == "" {
		return "", "route is required", false
	}
	return route, "", true
}

func decodeJSONBodyStrict(r *http.Request, out any) error {
	if r == nil || r.Body == nil {
		return errors.New("request body is missing")
	}
	maxBytes := int64(defaultMaxBodyBytes)
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBytes+1))
	if err != nil {
		return err
	}
	if int64(len(body)) > maxBytes {
		return errRequestBodyTooLarge
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain a single JSON document")
		}
		return err
	}
	return nil
}

func (s *Server) emitManagementMutationAudit(event ManagementMutationAuditEvent) {
	if s.AuditManagementMutation != nil {
		s.AuditManagementMutation(event)
	}
}

func (s *Server) observePublishAccepted(count int, scoped bool) {
	if s == nil || s.ObservePublishResult == nil || count <= 0 {
		return
	}
	s.ObservePublishResult(PublishResultEvent{
		Accepted: count,
		Scoped:   scoped,
	})
}

func (s *Server) observePublishRejected(code string, scoped bool) {
	if s == nil || s.ObservePublishResult == nil {
		return
	}
	s.ObservePublishResult(PublishResultEvent{
		Rejected: 1,
		Code:     strings.TrimSpace(code),
		Scoped:   scoped,
	})
}

func (s *Server) publishMaxBodyBytes(route string) int64 {
	maxBody := s.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = defaultMaxBodyBytes
	}
	if s != nil && s.LimitsForRoute != nil {
		if routeMaxBody, _ := s.LimitsForRoute(strings.TrimSpace(route)); routeMaxBody > 0 {
			maxBody = routeMaxBody
		}
	}
	return maxBody
}

func (s *Server) publishMaxHeaderBytes(route string) int {
	maxHeaders := s.MaxHeaderBytes
	if maxHeaders <= 0 {
		maxHeaders = defaultMaxHeaderBytes
	}
	if s != nil && s.LimitsForRoute != nil {
		if _, routeMaxHeaders := s.LimitsForRoute(strings.TrimSpace(route)); routeMaxHeaders > 0 {
			maxHeaders = routeMaxHeaders
		}
	}
	return maxHeaders
}

func (s *Server) publishRouteMode(route string, targets []string) string {
	if s != nil && s.ModeForRoute != nil {
		mode := strings.ToLower(strings.TrimSpace(s.ModeForRoute(strings.TrimSpace(route))))
		if mode == "pull" || mode == "deliver" {
			return mode
		}
	}
	if len(targets) == 1 && strings.TrimSpace(targets[0]) == "pull" {
		return "pull"
	}
	if len(targets) > 0 {
		return "deliver"
	}
	return ""
}

func (s *Server) mutationAuditPolicyError(audit managementAudit, scoped bool, scopedOperation string) (code, detail string) {
	if s == nil {
		return "", ""
	}
	actor := strings.TrimSpace(audit.Actor)
	if s.PublishRequireAuditActor && actor == "" {
		return publishCodeAuditActorRequired, "X-Hookaido-Audit-Actor is required by defaults.publish_policy.require_actor"
	}
	if s.PublishRequireAuditRequestID && strings.TrimSpace(audit.RequestID) == "" {
		return publishCodeAuditRequestIDRequired, "X-Request-ID is required by defaults.publish_policy.require_request_id"
	}
	if scoped && s.publishScopedManagedActorPolicyEnabled() {
		if actor == "" {
			return publishCodeAuditActorRequired, "X-Hookaido-Audit-Actor is required by defaults.publish_policy.actor_allow/actor_prefix"
		}
		if !s.publishScopedManagedActorAllowed(actor) {
			scopedOperation = strings.TrimSpace(scopedOperation)
			if scopedOperation == "" {
				scopedOperation = "mutation"
			}
			return publishCodeAuditActorNotAllowed, fmt.Sprintf(
				"X-Hookaido-Audit-Actor %q is not allowed for endpoint-scoped managed %s; %s",
				actor,
				scopedOperation,
				formatScopedManagedActorPolicyDetail(s.PublishScopedManagedActorAllowlist, s.PublishScopedManagedActorPrefixes),
			)
		}
	}
	return "", ""
}

func (s *Server) publishScopedManagedActorPolicyEnabled() bool {
	if s == nil {
		return false
	}
	return len(s.PublishScopedManagedActorAllowlist) > 0 || len(s.PublishScopedManagedActorPrefixes) > 0
}

func (s *Server) publishScopedManagedIDPolicyEnabled() bool {
	if s == nil {
		return false
	}
	return s.PublishRequireAuditActor ||
		s.PublishRequireAuditRequestID ||
		s.publishScopedManagedActorPolicyEnabled()
}

func (s *Server) publishScopedManagedActorAllowed(actor string) bool {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return false
	}
	if s == nil || !s.publishScopedManagedActorPolicyEnabled() {
		return true
	}
	for _, allowed := range s.PublishScopedManagedActorAllowlist {
		if actor == strings.TrimSpace(allowed) {
			return true
		}
	}
	for _, prefix := range s.PublishScopedManagedActorPrefixes {
		if strings.HasPrefix(actor, strings.TrimSpace(prefix)) {
			return true
		}
	}
	return false
}

func formatScopedManagedActorPolicyDetail(allowlist, prefixes []string) string {
	parts := make([]string, 0, 2)
	if len(allowlist) > 0 {
		quoted := make([]string, 0, len(allowlist))
		for _, actor := range allowlist {
			actor = strings.TrimSpace(actor)
			if actor == "" {
				continue
			}
			quoted = append(quoted, strconv.Quote(actor))
		}
		if len(quoted) > 0 {
			parts = append(parts, "allowed actors: "+strings.Join(quoted, ", "))
		}
	}
	if len(prefixes) > 0 {
		quoted := make([]string, 0, len(prefixes))
		for _, prefix := range prefixes {
			prefix = strings.TrimSpace(prefix)
			if prefix == "" {
				continue
			}
			quoted = append(quoted, strconv.Quote(prefix))
		}
		if len(quoted) > 0 {
			parts = append(parts, "allowed actor prefixes: "+strings.Join(quoted, ", "))
		}
	}
	if len(parts) == 0 {
		return "no scoped actor policy configured"
	}
	return strings.Join(parts, "; ")
}

func (s *Server) publishEnabledForRoute(route string) bool {
	if s == nil || s.PublishEnabledForRoute == nil {
		return true
	}
	return s.PublishEnabledForRoute(strings.TrimSpace(route))
}

func (s *Server) publishDirectEnabledForRoute(route string) bool {
	if s == nil || s.PublishDirectEnabledForRoute == nil {
		return true
	}
	return s.PublishDirectEnabledForRoute(strings.TrimSpace(route))
}

func (s *Server) publishManagedEnabledForRoute(route string) bool {
	if s == nil || s.PublishManagedEnabledForRoute == nil {
		return true
	}
	return s.PublishManagedEnabledForRoute(strings.TrimSpace(route))
}

func (s *Server) publishRoutePolicyError(route string, targets []string, scoped bool) (code, detail string) {
	if !s.publishEnabledForRoute(route) {
		return publishCodeRoutePublishDisabled, fmt.Sprintf("publish is disabled for route %q by route.publish", strings.TrimSpace(route))
	}
	if scoped {
		if !s.publishManagedEnabledForRoute(route) {
			return publishCodeRoutePublishDisabled, fmt.Sprintf("managed publish is disabled for route %q by route.publish.managed", strings.TrimSpace(route))
		}
	} else {
		if !s.publishDirectEnabledForRoute(route) {
			return publishCodeRoutePublishDisabled, fmt.Sprintf("direct publish is disabled for route %q by route.publish.direct", strings.TrimSpace(route))
		}
	}
	mode := s.publishRouteMode(route, targets)
	switch mode {
	case "pull":
		if !s.PublishAllowPullRoutes {
			return publishCodePullRoutePublishDisabled, "publish to pull routes is disabled by defaults.publish_policy.allow_pull_routes"
		}
	case "deliver":
		if !s.PublishAllowDeliverRoutes {
			return publishCodeDeliverRoutePublishDisabled, "publish to deliver routes is disabled by defaults.publish_policy.allow_deliver_routes"
		}
	}
	return "", ""
}

func (s *Server) writePublishError(w http.ResponseWriter, status int, code, detail string, itemIndex int, scoped bool) {
	s.observePublishRejected(code, scoped)
	if w == nil {
		return
	}
	code = strings.TrimSpace(code)
	if code == "" {
		code = publishCodeInvalidBody
	}
	detail = strings.TrimSpace(detail)
	if detail == "" {
		detail = code
	}
	resp := publishErrorResponse{
		Code:   code,
		Detail: detail,
	}
	if itemIndex >= 0 {
		idx := itemIndex
		resp.ItemIndex = &idx
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func writeManagementError(w http.ResponseWriter, status int, code, detail string) {
	if w == nil {
		return
	}
	code = strings.TrimSpace(code)
	if code == "" {
		code = publishCodeInvalidBody
	}
	detail = strings.TrimSpace(detail)
	if detail == "" {
		detail = http.StatusText(status)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(publishErrorResponse{
		Code:   code,
		Detail: detail,
	})
}

func writeMethodNotAllowed(w http.ResponseWriter, expected string) {
	expected = strings.TrimSpace(expected)
	detail := "method is not allowed"
	if expected != "" {
		detail = fmt.Sprintf("method must be %s", expected)
	}
	writeManagementError(w, http.StatusMethodNotAllowed, readCodeMethodNotAllowed, detail)
}

func writeManagementPathResolveError(w http.ResponseWriter, status int) {
	switch status {
	case http.StatusNotFound:
		writeManagementError(w, http.StatusNotFound, publishCodeManagedEndpointNotFound, "managed endpoint not found")
	default:
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeManagedResolverMissing, "managed endpoint resolution is not configured")
	}
}

func writeManagedSelectorResolveError(w http.ResponseWriter, status int) {
	switch status {
	case http.StatusServiceUnavailable:
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeManagedResolverMissing, "managed endpoint resolution is not configured")
	case http.StatusNotFound:
		writeManagementError(w, http.StatusNotFound, publishCodeManagedEndpointNotFound, "managed endpoint not found")
	default:
		writeManagementError(w, http.StatusBadRequest, publishCodeScopedSelectorMismatch, "route/managed selector is invalid or mismatched")
	}
}

func (s *Server) managementEndpointProjection(application, endpointName, route string, preferCurrent bool) *applicationEndpointResponse {
	if preferCurrent && s.ManagementModel != nil {
		if endpoint, ok := s.lookupManagementEndpoint(application, endpointName); ok {
			return &applicationEndpointResponse{
				Application:   application,
				EndpointName:  endpointName,
				Route:         endpoint.Route,
				Mode:          endpoint.Mode,
				Targets:       endpoint.Targets,
				PublishPolicy: endpoint.PublishPolicy,
			}
		}
	}

	route = strings.TrimSpace(route)
	if route == "" {
		return nil
	}
	return &applicationEndpointResponse{
		Application:  application,
		EndpointName: endpointName,
		Route:        route,
		PublishPolicy: ManagementEndpointPublishPolicy{
			Enabled:        true,
			DirectEnabled:  true,
			ManagedEnabled: true,
		},
	}
}

func managementEndpointMutationError(err error) (status int, code, detail string) {
	typedCode, typedDetail, hasTyped := ExtractManagementMutationError(err)
	switch {
	case errors.Is(err, ErrManagementRouteNotFound):
		detail = managementEndpointMutationDetail(err, "management route not found")
		if hasTyped && strings.TrimSpace(typedDetail) != "" {
			detail = strings.TrimSpace(typedDetail)
		}
		return http.StatusNotFound, managementCodeRouteNotFound, detail
	case errors.Is(err, ErrManagementEndpointNotFound):
		detail = managementEndpointMutationDetail(err, "management endpoint not found")
		if hasTyped && strings.TrimSpace(typedDetail) != "" {
			detail = strings.TrimSpace(typedDetail)
		}
		return http.StatusNotFound, managementCodeEndpointNotFound, detail
	case errors.Is(err, ErrManagementConflict):
		detail = managementEndpointMutationDetail(err, "management endpoint mutation conflict")
		if hasTyped && strings.TrimSpace(typedDetail) != "" {
			detail = strings.TrimSpace(typedDetail)
		}
		return http.StatusConflict, managementEndpointConflictCode(typedCode, detail), detail
	default:
		return http.StatusServiceUnavailable, managementCodeUnavailable, "management endpoint mutation failed"
	}
}

func managementEndpointConflictCode(typedCode, detail string) string {
	switch strings.TrimSpace(typedCode) {
	case managementCodeRouteMapped, managementCodeRoutePublishOff, managementCodeRouteTargetDrift, managementCodeRouteBacklog, managementCodeConflict:
		return strings.TrimSpace(typedCode)
	}

	lower := strings.ToLower(strings.TrimSpace(detail))
	switch {
	case strings.Contains(lower, "is already mapped to"):
		return managementCodeRouteMapped
	case strings.Contains(lower, "managed publish is disabled for route"):
		return managementCodeRoutePublishOff
	case strings.Contains(lower, "managed endpoint target profile mismatch"):
		return managementCodeRouteTargetDrift
	case strings.Contains(lower, "has active backlog"):
		return managementCodeRouteBacklog
	default:
		return managementCodeConflict
	}
}

func managementEndpointMutationDetail(err error, fallback string) string {
	fallback = strings.TrimSpace(fallback)
	if err == nil {
		return fallback
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return fallback
	}

	known := []string{
		ErrManagementRouteNotFound.Error(),
		ErrManagementEndpointNotFound.Error(),
		ErrManagementConflict.Error(),
	}
	for _, base := range known {
		if msg == base {
			return fallback
		}
		prefix := base + ":"
		if !strings.HasPrefix(msg, prefix) {
			continue
		}
		detail := strings.TrimSpace(strings.TrimPrefix(msg, prefix))
		if detail != "" {
			return detail
		}
		return fallback
	}
	return msg
}

func parseApplicationResourcePath(cleanPath string) (string, string, string, bool) {
	if !strings.HasPrefix(cleanPath, "/applications/") {
		return "", "", "", false
	}
	trimmed := strings.TrimPrefix(cleanPath, "/applications/")
	trimmed = strings.Trim(trimmed, "/")
	if trimmed == "" {
		return "", "", "", false
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 || parts[1] != "endpoints" {
		return "", "", "", false
	}

	application, ok := decodePathSegment(parts[0])
	if !ok {
		return "", "", "", false
	}
	if len(parts) == 2 {
		return application, "", "endpoints", true
	}

	endpointName, ok := decodePathSegment(parts[2])
	if !ok {
		return "", "", "", false
	}
	if len(parts) == 3 {
		return application, endpointName, "endpoint", true
	}
	if len(parts) == 4 && parts[3] == "messages" {
		return application, endpointName, "messages", true
	}
	if len(parts) == 5 && parts[3] == "messages" {
		switch parts[4] {
		case "publish":
			return application, endpointName, "messages_publish", true
		case "cancel_by_filter":
			return application, endpointName, "messages_cancel_by_filter", true
		case "requeue_by_filter":
			return application, endpointName, "messages_requeue_by_filter", true
		case "resume_by_filter":
			return application, endpointName, "messages_resume_by_filter", true
		}
	}
	return "", "", "", false
}

func decodePathSegment(raw string) (string, bool) {
	v, err := url.PathUnescape(raw)
	if err != nil {
		return "", false
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	return v, true
}

func parseBoolParam(raw string) (bool, bool) {
	if raw == "" {
		return false, true
	}
	switch strings.ToLower(raw) {
	case "1", "true", "on", "yes":
		return true, true
	case "0", "false", "off", "no":
		return false, true
	default:
		return false, false
	}
}

func parseTimeParam(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, true
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC(), true
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC(), true
	}
	return time.Time{}, false
}

func parseOptionalRoutePath(raw string) (string, bool) {
	route := strings.TrimSpace(raw)
	if route == "" {
		return "", true
	}
	if !strings.HasPrefix(route, "/") {
		return "", false
	}
	return route, true
}

func parseDurationParam(raw string) (time.Duration, bool) {
	if raw == "" {
		return 0, true
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, false
	}
	return d, true
}

func parseAttemptOutcome(raw string) (queue.AttemptOutcome, bool) {
	if raw == "" {
		return "", true
	}
	switch queue.AttemptOutcome(strings.ToLower(raw)) {
	case queue.AttemptOutcomeAcked, queue.AttemptOutcomeRetry, queue.AttemptOutcomeDead:
		return queue.AttemptOutcome(strings.ToLower(raw)), true
	default:
		return "", false
	}
}

func parseQueueState(raw string) (queue.State, bool) {
	if raw == "" {
		return "", true
	}
	switch queue.State(strings.ToLower(raw)) {
	case queue.StateQueued, queue.StateLeased, queue.StateDelivered, queue.StateDead, queue.StateCanceled:
		return queue.State(strings.ToLower(raw)), true
	default:
		return "", false
	}
}

func parseBacklogSummaryStates(raw string) ([]queue.State, bool) {
	if strings.TrimSpace(raw) == "" {
		out := make([]queue.State, len(defaultBacklogSummaryStates))
		copy(out, defaultBacklogSummaryStates)
		return out, true
	}

	parts := strings.Split(raw, ",")
	out := make([]queue.State, 0, len(parts))
	seen := make(map[queue.State]struct{}, len(parts))
	for _, part := range parts {
		state, ok := parseQueueState(strings.TrimSpace(part))
		if !ok || state == "" {
			return nil, false
		}
		switch state {
		case queue.StateQueued, queue.StateLeased, queue.StateDead:
		default:
			return nil, false
		}
		if _, exists := seen[state]; exists {
			continue
		}
		seen[state] = struct{}{}
		out = append(out, state)
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

func parseManageIDs(r *http.Request) ([]string, bool) {
	var req dlqManageRequest
	if err := decodeJSONBodyStrict(r, &req); err != nil {
		return nil, false
	}
	if len(req.IDs) == 0 || len(req.IDs) > maxListLimit {
		return nil, false
	}

	seen := make(map[string]struct{}, len(req.IDs))
	ids := make([]string, 0, len(req.IDs))
	for _, raw := range req.IDs {
		id := strings.TrimSpace(raw)
		if id == "" {
			return nil, false
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, false
	}
	return ids, true
}

type publishParseError struct {
	Code      string
	Detail    string
	ItemIndex int
}

func parsePublishItems(r *http.Request) ([]messagesPublishItem, *publishParseError) {
	return parsePublishItemsWithSelectorRequirement(r, true)
}

func parseScopedPublishItems(r *http.Request) ([]messagesPublishItem, *publishParseError) {
	return parsePublishItemsWithSelectorRequirement(r, false)
}

func parsePublishItemsWithSelectorRequirement(r *http.Request, requireSelector bool) ([]messagesPublishItem, *publishParseError) {
	var req messagesPublishRequest
	if err := decodeJSONBodyStrict(r, &req); err != nil {
		detail := "request body must be valid JSON with items"
		if errors.Is(err, errRequestBodyTooLarge) {
			detail = fmt.Sprintf("request body exceeds %d bytes", defaultMaxBodyBytes)
		}
		return nil, &publishParseError{
			Code:      publishCodeInvalidBody,
			Detail:    detail,
			ItemIndex: -1,
		}
	}
	if len(req.Items) == 0 || len(req.Items) > maxListLimit {
		return nil, &publishParseError{
			Code:      publishCodeInvalidBody,
			Detail:    fmt.Sprintf("items must contain between 1 and %d entries", maxListLimit),
			ItemIndex: -1,
		}
	}

	seen := make(map[string]struct{}, len(req.Items))
	items := make([]messagesPublishItem, 0, len(req.Items))
	for idx, raw := range req.Items {
		id := strings.TrimSpace(raw.ID)
		if id == "" {
			return nil, &publishParseError{
				Code:      publishCodeInvalidBody,
				Detail:    "item.id is required",
				ItemIndex: idx,
			}
		}
		route := strings.TrimSpace(raw.Route)
		target := strings.TrimSpace(raw.Target)
		application := strings.TrimSpace(raw.Application)
		endpointName := strings.TrimSpace(raw.EndpointName)
		if requireSelector && route == "" && application == "" && endpointName == "" {
			return nil, &publishParseError{
				Code:      publishCodeInvalidBody,
				Detail:    "item must provide route or application+endpoint_name",
				ItemIndex: idx,
			}
		}
		if route != "" && !strings.HasPrefix(route, "/") {
			return nil, &publishParseError{
				Code:      publishCodeInvalidBody,
				Detail:    "item.route must start with '/'",
				ItemIndex: idx,
			}
		}
		if requireSelector && route == "" && application == "" && target != "" {
			// Target without route/managed selector is invalid.
			return nil, &publishParseError{
				Code:      publishCodeInvalidBody,
				Detail:    "item.target requires route or application+endpoint_name",
				ItemIndex: idx,
			}
		}
		if (application == "") != (endpointName == "") {
			return nil, &publishParseError{
				Code:      publishCodeInvalidBody,
				Detail:    "item.application and item.endpoint_name must be set together",
				ItemIndex: idx,
			}
		}
		if application != "" && !config.IsValidManagementLabel(application) {
			return nil, &publishParseError{
				Code:      publishCodeInvalidBody,
				Detail:    fmt.Sprintf("item.application must match %s", config.ManagementLabelPattern()),
				ItemIndex: idx,
			}
		}
		if endpointName != "" && !config.IsValidManagementLabel(endpointName) {
			return nil, &publishParseError{
				Code:      publishCodeInvalidBody,
				Detail:    fmt.Sprintf("item.endpoint_name must match %s", config.ManagementLabelPattern()),
				ItemIndex: idx,
			}
		}
		if _, ok := seen[id]; ok {
			return nil, &publishParseError{
				Code:      publishCodeInvalidBody,
				Detail:    fmt.Sprintf("duplicate item.id %q in request", id),
				ItemIndex: idx,
			}
		}
		seen[id] = struct{}{}
		items = append(items, raw)
	}
	return items, nil
}

func publishEnvelopeFromItem(item messagesPublishItem, route, target string, maxBodyBytes int64, maxHeaderBytes int) (queue.Envelope, string, string) {
	if maxBodyBytes <= 0 {
		maxBodyBytes = defaultMaxBodyBytes
	}
	if maxHeaderBytes <= 0 {
		maxHeaderBytes = defaultMaxHeaderBytes
	}

	receivedAt, ok := parseTimeParam(strings.TrimSpace(item.ReceivedAt))
	if !ok {
		return queue.Envelope{}, publishCodeInvalidReceivedAt, ""
	}
	nextRunAt, ok := parseTimeParam(strings.TrimSpace(item.NextRunAt))
	if !ok {
		return queue.Envelope{}, publishCodeInvalidNextRunAt, ""
	}

	payload := []byte{}
	if strings.TrimSpace(item.PayloadB64) != "" {
		decoded, err := base64.StdEncoding.DecodeString(item.PayloadB64)
		if err != nil {
			return queue.Envelope{}, publishCodeInvalidPayload, ""
		}
		if int64(len(decoded)) > maxBodyBytes {
			return queue.Envelope{}, publishCodePayloadTooLarge, fmt.Sprintf(
				"decoded payload (%d bytes) exceeds max_body (%d bytes) for route %q",
				len(decoded),
				maxBodyBytes,
				route,
			)
		}
		payload = decoded
	}
	if err := httpheader.ValidateMap(item.Headers); err != nil {
		return queue.Envelope{}, publishCodeInvalidHeader, err.Error()
	}

	headerBytes := publishHeadersBytes(item.Headers)
	if headerBytes > maxHeaderBytes {
		return queue.Envelope{}, publishCodeHeadersTooLarge, fmt.Sprintf(
			"headers (%d bytes) exceed max_headers (%d bytes) for route %q",
			headerBytes,
			maxHeaderBytes,
			route,
		)
	}

	return queue.Envelope{
		ID:         strings.TrimSpace(item.ID),
		Route:      route,
		Target:     target,
		State:      queue.StateQueued,
		ReceivedAt: receivedAt,
		NextRunAt:  nextRunAt,
		Payload:    payload,
		Headers:    item.Headers,
		Trace:      item.Trace,
	}, "", ""
}

func publishHeadersBytes(headers map[string]string) int {
	total := 0
	for k, v := range headers {
		total += len(k) + len(v)
	}
	return total
}

func firstManagedPublishItemIndex(items []messagesPublishItem) int {
	for i, item := range items {
		if strings.TrimSpace(item.Application) != "" || strings.TrimSpace(item.EndpointName) != "" {
			return i
		}
	}
	return -1
}

func resolvePublishTarget(target string, allowedTargets []string) (string, bool) {
	target = strings.TrimSpace(target)
	if len(allowedTargets) == 0 {
		return "", false
	}

	if target == "" {
		if len(allowedTargets) == 1 {
			return allowedTargets[0], true
		}
		return "", false
	}

	for _, candidate := range allowedTargets {
		if target == strings.TrimSpace(candidate) {
			return target, true
		}
	}
	return "", false
}

func publishTargetUnresolvableDetail(route, target string, allowedTargets []string) string {
	route = strings.TrimSpace(route)
	target = strings.TrimSpace(target)
	allowed := formatAllowedTargetsForError(allowedTargets)
	if target == "" {
		return fmt.Sprintf("item.target is required for route %q; allowed targets: %s", route, allowed)
	}
	return fmt.Sprintf("item.target %q is not allowed for route %q; allowed targets: %s", target, route, allowed)
}

func publishEnvelopeIDs(envelopes []queue.Envelope) []string {
	ids := make([]string, 0, len(envelopes))
	for _, env := range envelopes {
		id := strings.TrimSpace(env.ID)
		if id == "" {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

func firstExistingMessageIDIndex(store queue.Store, ids []string) (int, string, error) {
	if store == nil || len(ids) == 0 {
		return -1, "", nil
	}
	indexByID := make(map[string]int, len(ids))
	for i, rawID := range ids {
		id := strings.TrimSpace(rawID)
		if id == "" {
			continue
		}
		indexByID[id] = i
	}
	if len(indexByID) == 0 {
		return -1, "", nil
	}

	lookup, err := store.LookupMessages(queue.MessageLookupRequest{IDs: ids})
	if err != nil {
		return -1, "", err
	}
	bestIdx := len(ids) + 1
	bestID := ""
	for _, item := range lookup.Items {
		idx, ok := indexByID[strings.TrimSpace(item.ID)]
		if !ok {
			continue
		}
		if idx < bestIdx {
			bestIdx = idx
			bestID = strings.TrimSpace(item.ID)
		}
	}
	if bestID == "" {
		return -1, "", nil
	}
	return bestIdx, bestID, nil
}

func formatAllowedTargetsForError(allowedTargets []string) string {
	seen := make(map[string]struct{}, len(allowedTargets))
	ordered := make([]string, 0, len(allowedTargets))
	for _, raw := range allowedTargets {
		target := strings.TrimSpace(raw)
		if target == "" {
			continue
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		ordered = append(ordered, target)
	}
	if len(ordered) == 0 {
		return "(none)"
	}
	parts := make([]string, 0, len(ordered))
	for _, target := range ordered {
		parts = append(parts, fmt.Sprintf("%q", target))
	}
	return strings.Join(parts, ", ")
}

func parseMessageManageFilter(r *http.Request, allowedStates map[queue.State]struct{}) (queue.MessageManageFilterRequest, string, string, bool) {
	var req messagesManageFilterRequest
	if err := decodeJSONBodyStrict(r, &req); err != nil {
		return queue.MessageManageFilterRequest{}, "", "", false
	}

	limit := req.Limit
	if limit == 0 {
		limit = defaultListLimit
	}
	if limit < 0 {
		return queue.MessageManageFilterRequest{}, "", "", false
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	state, ok := parseQueueState(strings.TrimSpace(req.State))
	if !ok {
		return queue.MessageManageFilterRequest{}, "", "", false
	}
	if state != "" {
		if _, exists := allowedStates[state]; !exists {
			return queue.MessageManageFilterRequest{}, "", "", false
		}
	}

	before, ok := parseTimeParam(strings.TrimSpace(req.Before))
	if !ok {
		return queue.MessageManageFilterRequest{}, "", "", false
	}

	return queue.MessageManageFilterRequest{
		Route:       strings.TrimSpace(req.Route),
		Target:      strings.TrimSpace(req.Target),
		State:       state,
		Limit:       limit,
		Before:      before,
		PreviewOnly: req.PreviewOnly,
	}, strings.TrimSpace(req.Application), strings.TrimSpace(req.EndpointName), true
}
