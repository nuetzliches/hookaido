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
	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/queue"
)

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
