package admin

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/backlog"
	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/httpheader"
	"github.com/nuetzliches/hookaido/internal/queue"
)

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
		diagnostics["queue"] = s.queueHealthDiagnosticsSnapshot()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":          true,
		"time":        time.Now().UTC().Format(time.RFC3339Nano),
		"diagnostics": diagnostics,
	})
}

func (s *Server) queueHealthDiagnosticsSnapshot() map[string]any {
	if s == nil || s.Store == nil {
		return map[string]any{
			"ok":    false,
			"error": "queue store is not configured",
		}
	}

	now := time.Now()
	s.queueHealth.mu.Lock()
	if s.queueHealth.cached != nil && (s.queueHealth.ttl <= 0 || now.Sub(s.queueHealth.cachedAt) <= s.queueHealth.ttl) {
		diag := s.queueHealth.cached
		s.queueHealth.mu.Unlock()
		return diag
	}

	// Cold-start: build queue diagnostics synchronously once.
	if s.queueHealth.cached == nil {
		if s.queueHealth.refreshing {
			s.queueHealth.mu.Unlock()
			return map[string]any{
				"ok":     false,
				"status": "warming",
			}
		}
		s.queueHealth.refreshing = true
		s.queueHealth.mu.Unlock()
		return s.refreshQueueHealthDiagnosticsSync()
	}

	// Stale cache: return stale diagnostics immediately and refresh async.
	if !s.queueHealth.refreshing {
		s.queueHealth.refreshing = true
		go s.refreshQueueHealthDiagnosticsAsync()
	}
	diag := s.queueHealth.cached
	s.queueHealth.mu.Unlock()
	return diag
}

func (s *Server) refreshQueueHealthDiagnosticsSync() map[string]any {
	diag := queueHealthDiagnostics(s.Store, s.trendSignalConfig())

	s.queueHealth.mu.Lock()
	s.queueHealth.cached = diag
	s.queueHealth.cachedAt = time.Now()
	s.queueHealth.refreshing = false
	s.queueHealth.mu.Unlock()
	return diag
}

func (s *Server) refreshQueueHealthDiagnosticsAsync() {
	diag := queueHealthDiagnostics(s.Store, s.trendSignalConfig())

	s.queueHealth.mu.Lock()
	s.queueHealth.cached = diag
	s.queueHealth.cachedAt = time.Now()
	s.queueHealth.refreshing = false
	s.queueHealth.mu.Unlock()
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
	if s == nil {
		return defaultMaxBodyBytes
	}
	maxBody := s.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = defaultMaxBodyBytes
	}
	if s.LimitsForRoute != nil {
		if routeMaxBody, _ := s.LimitsForRoute(strings.TrimSpace(route)); routeMaxBody > 0 {
			maxBody = routeMaxBody
		}
	}
	return maxBody
}

func (s *Server) publishMaxHeaderBytes(route string) int {
	if s == nil {
		return defaultMaxHeaderBytes
	}
	maxHeaders := s.MaxHeaderBytes
	if maxHeaders <= 0 {
		maxHeaders = defaultMaxHeaderBytes
	}
	if s.LimitsForRoute != nil {
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
		out := make([]queue.State, len(backlog.DefaultSummaryStates))
		copy(out, backlog.DefaultSummaryStates)
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
	if len(req.IDs) == 0 || len(req.IDs) > backlog.MaxListLimit {
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
	if len(req.Items) == 0 || len(req.Items) > backlog.MaxListLimit {
		return nil, &publishParseError{
			Code:      publishCodeInvalidBody,
			Detail:    fmt.Sprintf("items must contain between 1 and %d entries", backlog.MaxListLimit),
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
	if limit > backlog.MaxListLimit {
		limit = backlog.MaxListLimit
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
