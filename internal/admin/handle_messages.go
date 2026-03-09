package admin

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/backlog"
	"github.com/nuetzliches/hookaido/internal/queue"
)

type dlqResponse struct {
	Items []dlqItem `json:"items"`
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
	if limit > backlog.MaxListLimit {
		limit = backlog.MaxListLimit
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
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, fmt.Sprintf("ids must contain between 1 and %d non-empty entries", backlog.MaxListLimit))
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
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, fmt.Sprintf("ids must contain between 1 and %d non-empty entries", backlog.MaxListLimit))
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
	if limit > backlog.MaxListLimit {
		limit = backlog.MaxListLimit
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
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, fmt.Sprintf("ids must contain between 1 and %d non-empty entries", backlog.MaxListLimit))
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
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, fmt.Sprintf("ids must contain between 1 and %d non-empty entries", backlog.MaxListLimit))
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
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, fmt.Sprintf("ids must contain between 1 and %d non-empty entries", backlog.MaxListLimit))
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
	if limit > backlog.MaxListLimit {
		limit = backlog.MaxListLimit
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
