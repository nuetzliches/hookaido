package mcp

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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

func (s *Server) toolMessagesList(args map[string]any) (any, error) {
	req, application, endpointName, err := parseMessageListArgs(args)
	if err != nil {
		return nil, err
	}

	if compiled, useAdmin := s.queueToolsUseAdminProxy(); useAdmin {
		query := url.Values{}
		endpointPath := "/messages"
		if application != "" {
			if _, err := resolveManagedRouteFilterWithCompiled(req.Route, application, endpointName, compiled); err != nil {
				return nil, err
			}
			endpointPath = managedEndpointMessagesPath(application, endpointName)
		} else if req.Route != "" {
			query.Set("route", req.Route)
		}
		if req.Target != "" {
			query.Set("target", req.Target)
		}
		if req.State != "" {
			query.Set("state", string(req.State))
		}
		if !req.Before.IsZero() {
			query.Set("before", req.Before.UTC().Format(time.RFC3339Nano))
		}
		if req.IncludePayload {
			query.Set("include_payload", "true")
		}
		if req.IncludeHeaders {
			query.Set("include_headers", "true")
		}
		if req.IncludeTrace {
			query.Set("include_trace", "true")
		}
		query.Set("limit", strconv.Itoa(req.Limit))
		return s.callAdminJSON(compiled, http.MethodGet, endpointPath, query, nil, nil, defaultAdminProxyTimeout)
	}

	resolvedRoute, err := s.resolveManagedRouteFilter(req.Route, application, endpointName)
	if err != nil {
		return nil, err
	}
	req.Route = resolvedRoute

	store, err := s.openQueueStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	resp, err := store.ListMessages(req)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(resp.Items))
	for _, it := range resp.Items {
		item := map[string]any{
			"id":          it.ID,
			"route":       it.Route,
			"target":      it.Target,
			"state":       it.State,
			"received_at": it.ReceivedAt.Format(time.RFC3339Nano),
			"attempt":     it.Attempt,
			"next_run_at": it.NextRunAt.Format(time.RFC3339Nano),
		}
		if strings.TrimSpace(it.DeadReason) != "" {
			item["dead_reason"] = it.DeadReason
		}
		if len(it.Payload) > 0 {
			item["payload_b64"] = base64.StdEncoding.EncodeToString(it.Payload)
		}
		if len(it.Headers) > 0 {
			item["headers"] = it.Headers
		}
		if len(it.Trace) > 0 {
			item["trace"] = it.Trace
		}
		items = append(items, item)
	}

	return map[string]any{"items": items}, nil
}

func (s *Server) toolAttemptsList(args map[string]any) (any, error) {
	req, application, endpointName, err := parseAttemptListArgs(args)
	if err != nil {
		return nil, err
	}

	if compiled, useAdmin := s.queueToolsUseAdminProxy(); useAdmin {
		query := url.Values{}
		if application != "" {
			query.Set("application", application)
			query.Set("endpoint_name", endpointName)
		} else if req.Route != "" {
			query.Set("route", req.Route)
		}
		if req.Target != "" {
			query.Set("target", req.Target)
		}
		if req.EventID != "" {
			query.Set("event_id", req.EventID)
		}
		if req.Outcome != "" {
			query.Set("outcome", string(req.Outcome))
		}
		if !req.Before.IsZero() {
			query.Set("before", req.Before.UTC().Format(time.RFC3339Nano))
		}
		query.Set("limit", strconv.Itoa(req.Limit))
		return s.callAdminJSON(compiled, http.MethodGet, "/attempts", query, nil, nil, defaultAdminProxyTimeout)
	}
	resolvedRoute, err := s.resolveManagedRouteFilter(req.Route, application, endpointName)
	if err != nil {
		return nil, err
	}
	req.Route = resolvedRoute

	store, err := s.openQueueStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	resp, err := store.ListAttempts(req)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(resp.Items))
	for _, it := range resp.Items {
		item := map[string]any{
			"id":         it.ID,
			"event_id":   it.EventID,
			"route":      it.Route,
			"target":     it.Target,
			"attempt":    it.Attempt,
			"outcome":    it.Outcome,
			"created_at": it.CreatedAt.Format(time.RFC3339Nano),
		}
		if it.StatusCode > 0 {
			item["status_code"] = it.StatusCode
		}
		if strings.TrimSpace(it.Error) != "" {
			item["error"] = it.Error
		}
		if strings.TrimSpace(it.DeadReason) != "" {
			item["dead_reason"] = it.DeadReason
		}
		items = append(items, item)
	}

	return map[string]any{"items": items}, nil
}

func (s *Server) toolMessagesCancel(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, idMutationAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	audit, err := parseMutationAuditArgs(args, s.auditPrincipal())
	if err != nil {
		return nil, err
	}
	ids, err := parseIDs(args)
	if err != nil {
		return nil, err
	}

	ctx, err := s.resolveIDMutationPolicyContext()
	if err != nil {
		return nil, err
	}
	if ctx.useAdminProxy {
		out, err := s.callAdminJSON(ctx.compiled, http.MethodPost, "/messages/cancel", nil, map[string]any{"ids": ids}, mutationAuditHeaders(audit), defaultAdminProxyTimeout)
		if err != nil {
			return nil, err
		}
		return withAuditPrincipal(out, s.auditPrincipal()), nil
	}

	store, err := s.openQueueStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()
	if err := validateScopedManagedAuditPolicyForIDMutation(store, ids, map[queue.State]struct{}{
		queue.StateQueued: {},
		queue.StateLeased: {},
		queue.StateDead:   {},
	}, audit, ctx.compiled, ctx.compiledAvailable); err != nil {
		return nil, err
	}

	resp, err := store.CancelMessages(queue.MessageCancelRequest{IDs: ids})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"canceled": resp.Canceled,
		"audit":    mutationAuditMap(audit, s.auditPrincipal()),
	}, nil
}

func (s *Server) toolMessagesRequeue(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, idMutationAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	audit, err := parseMutationAuditArgs(args, s.auditPrincipal())
	if err != nil {
		return nil, err
	}
	ids, err := parseIDs(args)
	if err != nil {
		return nil, err
	}

	ctx, err := s.resolveIDMutationPolicyContext()
	if err != nil {
		return nil, err
	}
	if ctx.useAdminProxy {
		out, err := s.callAdminJSON(ctx.compiled, http.MethodPost, "/messages/requeue", nil, map[string]any{"ids": ids}, mutationAuditHeaders(audit), defaultAdminProxyTimeout)
		if err != nil {
			return nil, err
		}
		return withAuditPrincipal(out, s.auditPrincipal()), nil
	}

	store, err := s.openQueueStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()
	if err := validateScopedManagedAuditPolicyForIDMutation(store, ids, map[queue.State]struct{}{
		queue.StateDead:     {},
		queue.StateCanceled: {},
	}, audit, ctx.compiled, ctx.compiledAvailable); err != nil {
		return nil, err
	}

	resp, err := store.RequeueMessages(queue.MessageRequeueRequest{IDs: ids})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"requeued": resp.Requeued,
		"audit":    mutationAuditMap(audit, s.auditPrincipal()),
	}, nil
}

func (s *Server) toolMessagesResume(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, idMutationAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	audit, err := parseMutationAuditArgs(args, s.auditPrincipal())
	if err != nil {
		return nil, err
	}
	ids, err := parseIDs(args)
	if err != nil {
		return nil, err
	}

	ctx, err := s.resolveIDMutationPolicyContext()
	if err != nil {
		return nil, err
	}
	if ctx.useAdminProxy {
		out, err := s.callAdminJSON(ctx.compiled, http.MethodPost, "/messages/resume", nil, map[string]any{"ids": ids}, mutationAuditHeaders(audit), defaultAdminProxyTimeout)
		if err != nil {
			return nil, err
		}
		return withAuditPrincipal(out, s.auditPrincipal()), nil
	}

	store, err := s.openQueueStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()
	if err := validateScopedManagedAuditPolicyForIDMutation(store, ids, map[queue.State]struct{}{
		queue.StateCanceled: {},
	}, audit, ctx.compiled, ctx.compiledAvailable); err != nil {
		return nil, err
	}

	resp, err := store.ResumeMessages(queue.MessageResumeRequest{IDs: ids})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"resumed": resp.Resumed,
		"audit":   mutationAuditMap(audit, s.auditPrincipal()),
	}, nil
}

func (s *Server) toolMessagesPublish(args map[string]any) (any, error) {
	audit, err := parseMutationAuditArgs(args, s.auditPrincipal())
	if err != nil {
		return nil, err
	}

	items, err := parseMessagesPublishArgs(args)
	if err != nil {
		return nil, err
	}

	if compiled, useAdmin := s.queueToolsUseAdminProxy(); useAdmin {
		if err := validatePublishAuditPolicyForMutation(audit, items, compiled); err != nil {
			return nil, err
		}
		return s.toolMessagesPublishViaAdmin(compiled, items, audit)
	}

	compiled, res, err := s.loadCompiledConfig()
	if err != nil {
		return nil, err
	}
	if !res.OK {
		return nil, fmt.Errorf("config compile failed: %s", strings.Join(res.Errors, "; "))
	}
	if err := validatePublishAuditPolicyForMutation(audit, items, compiled); err != nil {
		return nil, err
	}

	store, err := s.openQueueStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	prepared := make([]queue.Envelope, 0, len(items))
	for i, item := range items {
		env, err := resolveManagedPublishItem(item, compiled)
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}
		prepared = append(prepared, env)
	}
	if dupIdx, dupID, err := firstExistingMessageIDIndex(store, publishEnvelopeIDs(prepared)); err != nil {
		return nil, err
	} else if dupIdx >= 0 {
		return nil, fmt.Errorf("items[%d].id %q already exists", dupIdx, dupID)
	}

	published := 0
	for i, env := range prepared {
		if err := store.Enqueue(env); err != nil {
			switch {
			case errors.Is(err, queue.ErrEnvelopeExists):
				return nil, fmt.Errorf("items[%d].id %q already exists", i, env.ID)
			case errors.Is(err, queue.ErrQueueFull):
				return nil, fmt.Errorf("items[%d]: queue full", i)
			default:
				return nil, fmt.Errorf("items[%d]: %w", i, err)
			}
		}
		published++
	}
	return map[string]any{
		"published": published,
		"audit":     mutationAuditMap(audit, s.auditPrincipal()),
	}, nil
}

type adminPublishBatch struct {
	EndpointPath string
	Items        []publishItem
	Scoped       bool
}

func (s *Server) toolMessagesPublishViaAdmin(compiled config.Compiled, items []publishItem, audit mutationAuditArgs) (map[string]any, error) {
	batches, err := buildAdminPublishBatches(items, compiled)
	if err != nil {
		return nil, err
	}
	if len(batches) == 0 {
		return nil, errors.New("items must not be empty")
	}

	headers := mutationAuditHeaders(audit)
	if len(batches) == 1 {
		payloadItems := publishItemsToAdminPayload(batches[0].Items)
		if batches[0].Scoped {
			payloadItems = scopedPublishItemsToAdminPayload(batches[0].Items)
		}
		out, err := s.callAdminJSON(
			compiled,
			http.MethodPost,
			batches[0].EndpointPath,
			nil,
			map[string]any{"items": payloadItems},
			headers,
			defaultAdminProxyTimeout,
		)
		if err != nil {
			s.rollbackPublishedIDsViaAdmin(compiled, likelyCommittedPublishIDsFromFailedBatch(err, batches[0].Items), headers)
			return nil, err
		}
		return withAuditPrincipal(out, s.auditPrincipal()), nil
	}

	totalPublished := 0
	publishOffset := 0
	publishedIDs := make([]string, 0, len(items))
	for _, batch := range batches {
		payloadItems := publishItemsToAdminPayload(batch.Items)
		if batch.Scoped {
			payloadItems = scopedPublishItemsToAdminPayload(batch.Items)
		}
		resp, err := s.callAdminJSON(
			compiled,
			http.MethodPost,
			batch.EndpointPath,
			nil,
			map[string]any{"items": payloadItems},
			headers,
			defaultAdminProxyTimeout,
		)
		if err != nil {
			rollbackIDs := append([]string{}, publishedIDs...)
			rollbackIDs = append(rollbackIDs, likelyCommittedPublishIDsFromFailedBatch(err, batch.Items)...)
			s.rollbackPublishedIDsViaAdmin(compiled, rollbackIDs, headers)
			return nil, withPublishBatchItemIndexOffset(err, publishOffset)
		}
		published, ok := intFromAnyForError(resp["published"])
		if !ok || published < 0 {
			return nil, fmt.Errorf("admin POST %s returned invalid published count", batch.EndpointPath)
		}
		totalPublished += published
		publishedIDs = append(publishedIDs, publishBatchItemIDs(batch.Items)...)
		publishOffset += len(batch.Items)
	}
	return map[string]any{
		"published": totalPublished,
		"audit":     mutationAuditMap(audit, s.auditPrincipal()),
	}, nil
}

func likelyCommittedPublishIDsFromFailedBatch(err error, items []publishItem) []string {
	if err == nil || len(items) == 0 {
		return nil
	}

	var adminErr *adminProxyHTTPError
	if !errors.As(err, &adminErr) {
		return nil
	}
	info := extractAdminProxyErrorInfo(adminErr.payload)
	if info.ItemIndex == nil || *info.ItemIndex <= 0 {
		return nil
	}

	prefix := *info.ItemIndex
	if prefix > len(items) {
		prefix = len(items)
	}
	return publishBatchItemIDs(items[:prefix])
}

func (s *Server) rollbackPublishedIDsViaAdmin(compiled config.Compiled, ids []string, headers map[string]string) {
	if len(ids) == 0 {
		return
	}
	s.rollbackAttemptsTotal.Add(1)
	s.rollbackIDsTotal.Add(int64(len(ids)))
	// Best-effort compensation for already-successful publish batches.
	_, err := s.callAdminJSON(
		compiled,
		http.MethodPost,
		"/messages/cancel",
		nil,
		map[string]any{"ids": ids},
		headers,
		defaultAdminProxyTimeout,
	)
	if err != nil {
		s.rollbackFailedTotal.Add(1)
		return
	}
	s.rollbackSucceededTotal.Add(1)
}

func (s *Server) adminProxyPublishRollbackCounters() map[string]any {
	if s == nil {
		return map[string]any{
			"rollback_attempts_total":  int64(0),
			"rollback_succeeded_total": int64(0),
			"rollback_failed_total":    int64(0),
			"rollback_ids_total":       int64(0),
		}
	}
	return map[string]any{
		"rollback_attempts_total":  s.rollbackAttemptsTotal.Load(),
		"rollback_succeeded_total": s.rollbackSucceededTotal.Load(),
		"rollback_failed_total":    s.rollbackFailedTotal.Load(),
		"rollback_ids_total":       s.rollbackIDsTotal.Load(),
	}
}

func publishBatchItemIDs(items []publishItem) []string {
	if len(items) == 0 {
		return nil
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return ids
}

func buildAdminPublishBatches(items []publishItem, compiled config.Compiled) ([]adminPublishBatch, error) {
	if len(items) == 0 {
		return nil, nil
	}

	batches := make([]adminPublishBatch, 0, len(items))
	for i, item := range items {
		// Preflight full publish item resolution (policy + selector + target + route limits)
		// before issuing the first Admin proxy request, so late-item validation failures
		// do not cause partial cross-batch publish side effects.
		if _, err := resolveManagedPublishItem(item, compiled); err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}

		endpointPath := "/messages/publish"
		scoped := false
		application := strings.TrimSpace(item.Application)
		endpointName := strings.TrimSpace(item.EndpointName)
		if application != "" || endpointName != "" {
			endpointPath = managedEndpointPublishPath(application, endpointName)
			scoped = true
		}

		if n := len(batches); n > 0 && batches[n-1].EndpointPath == endpointPath && batches[n-1].Scoped == scoped {
			batches[n-1].Items = append(batches[n-1].Items, item)
			continue
		}
		batches = append(batches, adminPublishBatch{
			EndpointPath: endpointPath,
			Items:        []publishItem{item},
			Scoped:       scoped,
		})
	}
	return batches, nil
}

func managedEndpointPublishPath(application, endpointName string) string {
	return managedEndpointMessageActionPath(application, endpointName, "publish")
}

func managedEndpointMessagesPath(application, endpointName string) string {
	return fmt.Sprintf(
		"/applications/%s/endpoints/%s/messages",
		url.PathEscape(strings.TrimSpace(application)),
		url.PathEscape(strings.TrimSpace(endpointName)),
	)
}

func managedEndpointMessageActionPath(application, endpointName, action string) string {
	return fmt.Sprintf(
		"/applications/%s/endpoints/%s/messages/%s",
		url.PathEscape(strings.TrimSpace(application)),
		url.PathEscape(strings.TrimSpace(endpointName)),
		strings.TrimSpace(action),
	)
}

func withPublishBatchItemIndexOffset(err error, offset int) error {
	if err == nil || offset <= 0 {
		return err
	}

	var adminErr *adminProxyHTTPError
	if !errors.As(err, &adminErr) {
		return err
	}
	shiftedPayload, ok := shiftedAdminProxyItemIndexPayload(adminErr.payload, offset)
	if !ok {
		return err
	}
	return &adminProxyHTTPError{
		method:       adminErr.method,
		endpointPath: adminErr.endpointPath,
		statusCode:   adminErr.statusCode,
		payload:      shiftedPayload,
	}
}

func shiftedAdminProxyItemIndexPayload(payload []byte, offset int) ([]byte, bool) {
	if len(payload) == 0 || offset <= 0 {
		return nil, false
	}
	var obj map[string]any
	if err := json.Unmarshal(payload, &obj); err != nil {
		return nil, false
	}
	raw, ok := obj["item_index"]
	if !ok {
		return nil, false
	}
	index, ok := intFromAnyForError(raw)
	if !ok || index < 0 {
		return nil, false
	}
	obj["item_index"] = index + offset
	shifted, err := json.Marshal(obj)
	if err != nil {
		return nil, false
	}
	return shifted, true
}

func (s *Server) toolMessagesCancelByFilter(args map[string]any) (any, error) {
	audit, err := parseMutationAuditArgs(args, s.auditPrincipal())
	if err != nil {
		return nil, err
	}
	req, application, endpointName, err := parseMessageManageFilterArgs(args, map[queue.State]struct{}{
		queue.StateQueued: {},
		queue.StateLeased: {},
		queue.StateDead:   {},
	})
	if err != nil {
		return nil, err
	}

	if compiled, useAdmin := s.queueToolsUseAdminProxy(); useAdmin {
		endpointPath := "/messages/cancel_by_filter"
		payload := messageManageFilterPayload(req, application, endpointName)
		if err := validateScopedManagedAuditPolicyForFilterMutation(audit, req.Route, application, endpointName, compiled); err != nil {
			return nil, err
		}
		if application != "" {
			if _, err := resolveManagedRouteFilterWithCompiled(req.Route, application, endpointName, compiled); err != nil {
				return nil, err
			}
			endpointPath = managedEndpointMessageActionPath(application, endpointName, "cancel_by_filter")
			payload = scopedMessageManageFilterPayload(req)
		}
		out, err := s.callAdminJSON(
			compiled,
			http.MethodPost,
			endpointPath,
			nil,
			payload,
			mutationAuditHeaders(audit),
			defaultAdminProxyTimeout,
		)
		if err != nil {
			return nil, err
		}
		return withAuditPrincipal(out, s.auditPrincipal()), nil
	}

	var resolvedRoute string
	if application != "" {
		compiled, res, err := s.loadCompiledConfig()
		if err != nil {
			return nil, err
		}
		if !res.OK {
			return nil, fmt.Errorf("config compile failed: %s", strings.Join(res.Errors, "; "))
		}
		if err := validateScopedManagedAuditPolicyForFilterMutation(audit, req.Route, application, endpointName, compiled); err != nil {
			return nil, err
		}
		resolvedRoute, err = resolveManagedRouteFilterWithCompiled(req.Route, application, endpointName, compiled)
		if err != nil {
			return nil, err
		}
	} else {
		if err := s.validateRouteScopedManagedAuditPolicyForFilterMutation(audit, req.Route); err != nil {
			return nil, err
		}
		var err error
		resolvedRoute, err = s.resolveManagedRouteFilter(req.Route, application, endpointName)
		if err != nil {
			return nil, err
		}
	}
	req.Route = resolvedRoute

	store, err := s.openQueueStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	resp, err := store.CancelMessagesByFilter(req)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"canceled":     resp.Canceled,
		"matched":      resp.Matched,
		"preview_only": resp.PreviewOnly,
		"audit":        mutationAuditMap(audit, s.auditPrincipal()),
	}, nil
}

func (s *Server) toolMessagesRequeueByFilter(args map[string]any) (any, error) {
	audit, err := parseMutationAuditArgs(args, s.auditPrincipal())
	if err != nil {
		return nil, err
	}
	req, application, endpointName, err := parseMessageManageFilterArgs(args, map[queue.State]struct{}{
		queue.StateDead:     {},
		queue.StateCanceled: {},
	})
	if err != nil {
		return nil, err
	}

	if compiled, useAdmin := s.queueToolsUseAdminProxy(); useAdmin {
		endpointPath := "/messages/requeue_by_filter"
		payload := messageManageFilterPayload(req, application, endpointName)
		if err := validateScopedManagedAuditPolicyForFilterMutation(audit, req.Route, application, endpointName, compiled); err != nil {
			return nil, err
		}
		if application != "" {
			if _, err := resolveManagedRouteFilterWithCompiled(req.Route, application, endpointName, compiled); err != nil {
				return nil, err
			}
			endpointPath = managedEndpointMessageActionPath(application, endpointName, "requeue_by_filter")
			payload = scopedMessageManageFilterPayload(req)
		}
		out, err := s.callAdminJSON(
			compiled,
			http.MethodPost,
			endpointPath,
			nil,
			payload,
			mutationAuditHeaders(audit),
			defaultAdminProxyTimeout,
		)
		if err != nil {
			return nil, err
		}
		return withAuditPrincipal(out, s.auditPrincipal()), nil
	}

	var resolvedRoute string
	if application != "" {
		compiled, res, err := s.loadCompiledConfig()
		if err != nil {
			return nil, err
		}
		if !res.OK {
			return nil, fmt.Errorf("config compile failed: %s", strings.Join(res.Errors, "; "))
		}
		if err := validateScopedManagedAuditPolicyForFilterMutation(audit, req.Route, application, endpointName, compiled); err != nil {
			return nil, err
		}
		resolvedRoute, err = resolveManagedRouteFilterWithCompiled(req.Route, application, endpointName, compiled)
		if err != nil {
			return nil, err
		}
	} else {
		if err := s.validateRouteScopedManagedAuditPolicyForFilterMutation(audit, req.Route); err != nil {
			return nil, err
		}
		var err error
		resolvedRoute, err = s.resolveManagedRouteFilter(req.Route, application, endpointName)
		if err != nil {
			return nil, err
		}
	}
	req.Route = resolvedRoute

	store, err := s.openQueueStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	resp, err := store.RequeueMessagesByFilter(req)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"requeued":     resp.Requeued,
		"matched":      resp.Matched,
		"preview_only": resp.PreviewOnly,
		"audit":        mutationAuditMap(audit, s.auditPrincipal()),
	}, nil
}

func (s *Server) toolMessagesResumeByFilter(args map[string]any) (any, error) {
	audit, err := parseMutationAuditArgs(args, s.auditPrincipal())
	if err != nil {
		return nil, err
	}
	req, application, endpointName, err := parseMessageManageFilterArgs(args, map[queue.State]struct{}{
		queue.StateCanceled: {},
	})
	if err != nil {
		return nil, err
	}

	if compiled, useAdmin := s.queueToolsUseAdminProxy(); useAdmin {
		endpointPath := "/messages/resume_by_filter"
		payload := messageManageFilterPayload(req, application, endpointName)
		if err := validateScopedManagedAuditPolicyForFilterMutation(audit, req.Route, application, endpointName, compiled); err != nil {
			return nil, err
		}
		if application != "" {
			if _, err := resolveManagedRouteFilterWithCompiled(req.Route, application, endpointName, compiled); err != nil {
				return nil, err
			}
			endpointPath = managedEndpointMessageActionPath(application, endpointName, "resume_by_filter")
			payload = scopedMessageManageFilterPayload(req)
		}
		out, err := s.callAdminJSON(
			compiled,
			http.MethodPost,
			endpointPath,
			nil,
			payload,
			mutationAuditHeaders(audit),
			defaultAdminProxyTimeout,
		)
		if err != nil {
			return nil, err
		}
		return withAuditPrincipal(out, s.auditPrincipal()), nil
	}

	var resolvedRoute string
	if application != "" {
		compiled, res, err := s.loadCompiledConfig()
		if err != nil {
			return nil, err
		}
		if !res.OK {
			return nil, fmt.Errorf("config compile failed: %s", strings.Join(res.Errors, "; "))
		}
		if err := validateScopedManagedAuditPolicyForFilterMutation(audit, req.Route, application, endpointName, compiled); err != nil {
			return nil, err
		}
		resolvedRoute, err = resolveManagedRouteFilterWithCompiled(req.Route, application, endpointName, compiled)
		if err != nil {
			return nil, err
		}
	} else {
		if err := s.validateRouteScopedManagedAuditPolicyForFilterMutation(audit, req.Route); err != nil {
			return nil, err
		}
		var err error
		resolvedRoute, err = s.resolveManagedRouteFilter(req.Route, application, endpointName)
		if err != nil {
			return nil, err
		}
	}
	req.Route = resolvedRoute

	store, err := s.openQueueStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	resp, err := store.ResumeMessagesByFilter(req)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"resumed":      resp.Resumed,
		"matched":      resp.Matched,
		"preview_only": resp.PreviewOnly,
		"audit":        mutationAuditMap(audit, s.auditPrincipal()),
	}, nil
}

func parseMessageListArgs(args map[string]any) (queue.MessageListRequest, string, string, error) {
	before, err := parseOptionalTime(args, "before")
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	state, err := parseOptionalState(args, "state")
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	limit, err := parseLimit(args, backlog.MaxListLimit)
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	includePayload, err := parseBool(args, "include_payload")
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	includeHeaders, err := parseBool(args, "include_headers")
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	includeTrace, err := parseBool(args, "include_trace")
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}

	route, err := parseString(args, "route")
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	if err := validateOptionalRoutePath(route); err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	application, err := parseString(args, "application")
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	endpointName, err := parseString(args, "endpoint_name")
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	if (application == "") != (endpointName == "") {
		return queue.MessageListRequest{}, "", "", errors.New("application and endpoint_name must be set together")
	}
	if err := validateManagedSelectorLabels(application, endpointName); err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	if application != "" && route != "" {
		return queue.MessageListRequest{}, "", "", errors.New("route is not allowed when application and endpoint_name are set")
	}

	target, err := parseString(args, "target")
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}

	return queue.MessageListRequest{
		Route:          route,
		Target:         target,
		State:          state,
		Limit:          limit,
		Before:         before,
		IncludePayload: includePayload,
		IncludeHeaders: includeHeaders,
		IncludeTrace:   includeTrace,
	}, application, endpointName, nil
}

func parseMessageManageFilterArgs(args map[string]any, allowed map[queue.State]struct{}) (queue.MessageManageFilterRequest, string, string, error) {
	if err := validateAllowedKeys(args, messageManageFilterAllowedKeys, "arguments"); err != nil {
		return queue.MessageManageFilterRequest{}, "", "", err
	}

	before, err := parseOptionalTime(args, "before")
	if err != nil {
		return queue.MessageManageFilterRequest{}, "", "", err
	}
	state, err := parseOptionalState(args, "state")
	if err != nil {
		return queue.MessageManageFilterRequest{}, "", "", err
	}
	if state != "" {
		if _, ok := allowed[state]; !ok {
			return queue.MessageManageFilterRequest{}, "", "", fmt.Errorf("invalid state %q", state)
		}
	}

	limit, err := parseLimit(args, backlog.MaxListLimit)
	if err != nil {
		return queue.MessageManageFilterRequest{}, "", "", err
	}
	previewOnly, err := parseBool(args, "preview_only")
	if err != nil {
		return queue.MessageManageFilterRequest{}, "", "", err
	}

	route, err := parseString(args, "route")
	if err != nil {
		return queue.MessageManageFilterRequest{}, "", "", err
	}
	if err := validateOptionalRoutePath(route); err != nil {
		return queue.MessageManageFilterRequest{}, "", "", err
	}
	application, err := parseString(args, "application")
	if err != nil {
		return queue.MessageManageFilterRequest{}, "", "", err
	}
	endpointName, err := parseString(args, "endpoint_name")
	if err != nil {
		return queue.MessageManageFilterRequest{}, "", "", err
	}
	if (application == "") != (endpointName == "") {
		return queue.MessageManageFilterRequest{}, "", "", errors.New("application and endpoint_name must be set together")
	}
	if err := validateManagedSelectorLabels(application, endpointName); err != nil {
		return queue.MessageManageFilterRequest{}, "", "", err
	}
	if application != "" && route != "" {
		return queue.MessageManageFilterRequest{}, "", "", errors.New("route is not allowed when application and endpoint_name are set")
	}
	target, err := parseString(args, "target")
	if err != nil {
		return queue.MessageManageFilterRequest{}, "", "", err
	}

	return queue.MessageManageFilterRequest{
		Route:       route,
		Target:      target,
		State:       state,
		Limit:       limit,
		Before:      before,
		PreviewOnly: previewOnly,
	}, application, endpointName, nil
}

func parseAttemptListArgs(args map[string]any) (queue.AttemptListRequest, string, string, error) {
	before, err := parseOptionalTime(args, "before")
	if err != nil {
		return queue.AttemptListRequest{}, "", "", err
	}
	limit, err := parseLimit(args, backlog.MaxListLimit)
	if err != nil {
		return queue.AttemptListRequest{}, "", "", err
	}
	outcomeRaw, err := parseString(args, "outcome")
	if err != nil {
		return queue.AttemptListRequest{}, "", "", err
	}
	outcome := queue.AttemptOutcome(strings.ToLower(strings.TrimSpace(outcomeRaw)))
	if outcome != "" && outcome != queue.AttemptOutcomeAcked && outcome != queue.AttemptOutcomeRetry && outcome != queue.AttemptOutcomeDead {
		return queue.AttemptListRequest{}, "", "", fmt.Errorf("invalid outcome %q", outcomeRaw)
	}
	route, err := parseString(args, "route")
	if err != nil {
		return queue.AttemptListRequest{}, "", "", err
	}
	if err := validateOptionalRoutePath(route); err != nil {
		return queue.AttemptListRequest{}, "", "", err
	}
	application, err := parseString(args, "application")
	if err != nil {
		return queue.AttemptListRequest{}, "", "", err
	}
	endpointName, err := parseString(args, "endpoint_name")
	if err != nil {
		return queue.AttemptListRequest{}, "", "", err
	}
	if (application == "") != (endpointName == "") {
		return queue.AttemptListRequest{}, "", "", errors.New("application and endpoint_name must be set together")
	}
	if err := validateManagedSelectorLabels(application, endpointName); err != nil {
		return queue.AttemptListRequest{}, "", "", err
	}
	if application != "" && route != "" {
		return queue.AttemptListRequest{}, "", "", errors.New("route is not allowed when application and endpoint_name are set")
	}
	target, err := parseString(args, "target")
	if err != nil {
		return queue.AttemptListRequest{}, "", "", err
	}
	eventID, err := parseString(args, "event_id")
	if err != nil {
		return queue.AttemptListRequest{}, "", "", err
	}
	return queue.AttemptListRequest{
		Route:   route,
		Target:  target,
		EventID: eventID,
		Outcome: outcome,
		Limit:   limit,
		Before:  before,
	}, application, endpointName, nil
}

func parseIDs(args map[string]any) ([]string, error) {
	raw, ok := args["ids"]
	if !ok {
		return nil, errors.New("ids is required")
	}

	var in []any
	switch v := raw.(type) {
	case []any:
		in = v
	case []string:
		in = make([]any, 0, len(v))
		for _, s := range v {
			in = append(in, s)
		}
	default:
		return nil, errors.New("ids must be an array of strings")
	}

	if len(in) == 0 || len(in) > backlog.MaxListLimit {
		return nil, fmt.Errorf("ids must contain between 1 and %d entries", backlog.MaxListLimit)
	}

	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for i, rawID := range in {
		id, ok := rawID.(string)
		if !ok {
			return nil, fmt.Errorf("ids[%d] must be a string", i)
		}
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, fmt.Errorf("ids[%d] must not be empty", i)
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) == 0 {
		return nil, errors.New("ids must not be empty")
	}
	return out, nil
}

type publishItem struct {
	ID           string
	Route        string
	Target       string
	Application  string
	EndpointName string
	Payload      []byte
	ReceivedAt   time.Time
	NextRunAt    time.Time
	Headers      map[string]string
	Trace        map[string]string
}

func parseMessagesPublishArgs(args map[string]any) ([]publishItem, error) {
	if err := validateAllowedKeys(args, publishToolAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	raw, ok := args["items"]
	if !ok {
		return nil, errors.New("items is required")
	}

	var in []any
	switch v := raw.(type) {
	case []any:
		in = v
	default:
		return nil, errors.New("items must be an array")
	}
	if len(in) == 0 || len(in) > backlog.MaxListLimit {
		return nil, fmt.Errorf("items must contain between 1 and %d entries", backlog.MaxListLimit)
	}

	seenIDs := make(map[string]struct{}, len(in))
	out := make([]publishItem, 0, len(in))
	for i, rawItem := range in {
		item, ok := rawItem.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("items[%d] must be an object", i)
		}
		if err := validateAllowedKeys(item, publishItemAllowedKeys, fmt.Sprintf("items[%d]", i)); err != nil {
			return nil, err
		}

		id, err := parseRequiredString(item, "id")
		if err != nil {
			return nil, fmt.Errorf("items[%d].%s", i, err.Error())
		}
		if _, dup := seenIDs[id]; dup {
			return nil, fmt.Errorf("items[%d].id %q is duplicated", i, id)
		}
		seenIDs[id] = struct{}{}

		route, err := parseString(item, "route")
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}
		target, err := parseString(item, "target")
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}
		application, err := parseString(item, "application")
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}
		endpointName, err := parseString(item, "endpoint_name")
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}
		if route == "" && application == "" && endpointName == "" {
			return nil, fmt.Errorf("items[%d]: provide route or application+endpoint_name", i)
		}
		if route != "" && !strings.HasPrefix(route, "/") {
			return nil, fmt.Errorf("items[%d].route must start with '/'", i)
		}
		if (application == "") != (endpointName == "") {
			return nil, fmt.Errorf("items[%d]: application and endpoint_name must be set together", i)
		}
		if err := validateManagedSelectorLabels(application, endpointName); err != nil {
			return nil, fmt.Errorf("items[%d].%w", i, err)
		}
		if application != "" && route != "" {
			return nil, fmt.Errorf("items[%d].route is not allowed when application and endpoint_name are set", i)
		}

		payloadB64, err := parseString(item, "payload_b64")
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}
		payload := []byte{}
		if payloadB64 != "" {
			decoded, err := base64.StdEncoding.DecodeString(payloadB64)
			if err != nil {
				return nil, fmt.Errorf("items[%d].payload_b64 must be base64: %w", i, err)
			}
			payload = decoded
		}

		receivedAt, err := parseOptionalTime(item, "received_at")
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}
		nextRunAt, err := parseOptionalTime(item, "next_run_at")
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}
		headers, err := parseStringMap(item, "headers")
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}
		if err := httpheader.ValidateMap(headers); err != nil {
			return nil, fmt.Errorf("items[%d].headers: %w", i, err)
		}
		trace, err := parseStringMap(item, "trace")
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}

		out = append(out, publishItem{
			ID:           id,
			Route:        route,
			Target:       target,
			Application:  application,
			EndpointName: endpointName,
			Payload:      payload,
			ReceivedAt:   receivedAt,
			NextRunAt:    nextRunAt,
			Headers:      headers,
			Trace:        trace,
		})
	}
	if err := validateMessagesPublishSelectorMode(out); err != nil {
		return nil, err
	}
	return out, nil
}

func validateMessagesPublishSelectorMode(items []publishItem) error {
	mode := ""
	for _, item := range items {
		itemMode := "route"
		if strings.TrimSpace(item.Application) != "" || strings.TrimSpace(item.EndpointName) != "" {
			itemMode = "managed"
		}
		if mode == "" {
			mode = itemMode
			continue
		}
		if mode != itemMode {
			return errors.New("items must use a single selector mode per request: either route or application+endpoint_name")
		}
	}
	return nil
}

func publishItemsToAdminPayload(items []publishItem) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		record := map[string]any{
			"id": item.ID,
		}
		if item.Route != "" {
			record["route"] = item.Route
		}
		if item.Target != "" {
			record["target"] = item.Target
		}
		if item.Application != "" {
			record["application"] = item.Application
			record["endpoint_name"] = item.EndpointName
		}
		if len(item.Payload) > 0 {
			record["payload_b64"] = base64.StdEncoding.EncodeToString(item.Payload)
		}
		if !item.ReceivedAt.IsZero() {
			record["received_at"] = item.ReceivedAt.UTC().Format(time.RFC3339Nano)
		}
		if !item.NextRunAt.IsZero() {
			record["next_run_at"] = item.NextRunAt.UTC().Format(time.RFC3339Nano)
		}
		if len(item.Headers) > 0 {
			record["headers"] = item.Headers
		}
		if len(item.Trace) > 0 {
			record["trace"] = item.Trace
		}
		out = append(out, record)
	}
	return out
}

func scopedPublishItemsToAdminPayload(items []publishItem) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		record := map[string]any{
			"id": item.ID,
		}
		if item.Target != "" {
			record["target"] = item.Target
		}
		if len(item.Payload) > 0 {
			record["payload_b64"] = base64.StdEncoding.EncodeToString(item.Payload)
		}
		if !item.ReceivedAt.IsZero() {
			record["received_at"] = item.ReceivedAt.UTC().Format(time.RFC3339Nano)
		}
		if !item.NextRunAt.IsZero() {
			record["next_run_at"] = item.NextRunAt.UTC().Format(time.RFC3339Nano)
		}
		if len(item.Headers) > 0 {
			record["headers"] = item.Headers
		}
		if len(item.Trace) > 0 {
			record["trace"] = item.Trace
		}
		out = append(out, record)
	}
	return out
}

func messageManageFilterPayload(req queue.MessageManageFilterRequest, application, endpointName string) map[string]any {
	out := map[string]any{
		"limit":        req.Limit,
		"preview_only": req.PreviewOnly,
	}
	if req.Route != "" {
		out["route"] = req.Route
	}
	if application != "" {
		out["application"] = application
		out["endpoint_name"] = endpointName
	}
	if req.Target != "" {
		out["target"] = req.Target
	}
	if req.State != "" {
		out["state"] = string(req.State)
	}
	if !req.Before.IsZero() {
		out["before"] = req.Before.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func scopedMessageManageFilterPayload(req queue.MessageManageFilterRequest) map[string]any {
	out := map[string]any{
		"limit":        req.Limit,
		"preview_only": req.PreviewOnly,
	}
	if req.Target != "" {
		out["target"] = req.Target
	}
	if req.State != "" {
		out["state"] = string(req.State)
	}
	if !req.Before.IsZero() {
		out["before"] = req.Before.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func resolveManagedPublishItem(item publishItem, compiled config.Compiled) (queue.Envelope, error) {
	routePath := strings.TrimSpace(item.Route)
	target := strings.TrimSpace(item.Target)
	application := strings.TrimSpace(item.Application)
	endpointName := strings.TrimSpace(item.EndpointName)

	if err := validatePublishPolicyForItem(item, compiled); err != nil {
		return queue.Envelope{}, err
	}

	var routeCfg *config.CompiledRoute
	if application != "" || endpointName != "" {
		r, err := findCompiledRouteByManagement(compiled, application, endpointName)
		if err != nil {
			return queue.Envelope{}, err
		}
		routeCfg = r
		if routePath != "" {
			return queue.Envelope{}, errors.New("route is not allowed when application and endpoint_name are provided")
		}
		routePath = routeCfg.Path
	}

	if routePath == "" {
		return queue.Envelope{}, errors.New("route is required when application/endpoint_name is not provided")
	}
	if routeCfg == nil {
		r, ok := findCompiledRouteByPath(compiled, routePath)
		if !ok {
			return queue.Envelope{}, fmt.Errorf("route %q not found in compiled config", routePath)
		}
		routeCfg = r
	}
	if application == "" && endpointName == "" {
		managedApplication := strings.TrimSpace(routeCfg.Application)
		managedEndpointName := strings.TrimSpace(routeCfg.EndpointName)
		if managedApplication != "" && managedEndpointName != "" {
			return queue.Envelope{}, fmt.Errorf("route %q is managed by (%q, %q); provide application and endpoint_name", routePath, managedApplication, managedEndpointName)
		}
	}

	allowedTargets := compiledRouteTargets(*routeCfg)
	if len(allowedTargets) == 0 {
		return queue.Envelope{}, fmt.Errorf("route %q has no publishable targets", routePath)
	}
	if target == "" {
		if len(allowedTargets) != 1 {
			return queue.Envelope{}, fmt.Errorf("item.target is required for route %q; allowed targets: %s", routePath, formatAllowedTargetsForError(allowedTargets))
		}
		target = allowedTargets[0]
	} else if !stringInSlice(target, allowedTargets) {
		return queue.Envelope{}, fmt.Errorf("item.target %q is not allowed for route %q; allowed targets: %s", target, routePath, formatAllowedTargetsForError(allowedTargets))
	}
	if routeCfg.MaxBodyBytes > 0 && int64(len(item.Payload)) > routeCfg.MaxBodyBytes {
		return queue.Envelope{}, fmt.Errorf(
			"decoded payload (%d bytes) exceeds max_body (%d bytes) for route %q",
			len(item.Payload),
			routeCfg.MaxBodyBytes,
			routePath,
		)
	}
	if routeCfg.MaxHeaderBytes > 0 {
		headerBytes := publishHeadersBytes(item.Headers)
		if headerBytes > routeCfg.MaxHeaderBytes {
			return queue.Envelope{}, fmt.Errorf(
				"headers (%d bytes) exceed max_headers (%d bytes) for route %q",
				headerBytes,
				routeCfg.MaxHeaderBytes,
				routePath,
			)
		}
	}

	return queue.Envelope{
		ID:         item.ID,
		Route:      routePath,
		Target:     target,
		State:      queue.StateQueued,
		ReceivedAt: item.ReceivedAt,
		NextRunAt:  item.NextRunAt,
		Payload:    item.Payload,
		Headers:    item.Headers,
		Trace:      item.Trace,
	}, nil
}

func publishHeadersBytes(headers map[string]string) int {
	total := 0
	for k, v := range headers {
		total += len(k) + len(v)
	}
	return total
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

func validatePublishPolicyForItem(item publishItem, compiled config.Compiled) error {
	application := strings.TrimSpace(item.Application)
	endpointName := strings.TrimSpace(item.EndpointName)
	routePath := strings.TrimSpace(item.Route)
	var routeCfg *config.CompiledRoute
	if application != "" || endpointName != "" {
		if !compiled.Defaults.PublishPolicy.ManagedEnabled {
			return errors.New("managed publish is disabled by defaults.publish_policy.managed")
		}
		r, err := findCompiledRouteByManagement(compiled, application, endpointName)
		if err != nil {
			return err
		}
		routeCfg = r
	} else {
		if !compiled.Defaults.PublishPolicy.DirectEnabled {
			return errors.New("direct publish is disabled by defaults.publish_policy.direct")
		}
		if routePath != "" {
			if r, ok := findCompiledRouteByPath(compiled, routePath); ok {
				routeCfg = r
			}
		}
	}

	if routeCfg != nil {
		if !routeCfg.Publish {
			return fmt.Errorf("publish is disabled for route %q by route.publish", routeCfg.Path)
		}
		if application != "" || endpointName != "" {
			if !routeCfg.PublishManaged {
				return fmt.Errorf("managed publish is disabled for route %q by route.publish.managed", routeCfg.Path)
			}
		} else {
			if !routeCfg.PublishDirect {
				return fmt.Errorf("direct publish is disabled for route %q by route.publish.direct", routeCfg.Path)
			}
		}
		if routeCfg.Pull != nil && !compiled.Defaults.PublishPolicy.AllowPullRoutes {
			return errors.New("publish to pull routes is disabled by defaults.publish_policy.allow_pull_routes")
		}
		if routeCfg.Pull == nil && !compiled.Defaults.PublishPolicy.AllowDeliverRoutes {
			return errors.New("publish to deliver routes is disabled by defaults.publish_policy.allow_deliver_routes")
		}
	}
	return nil
}

func validatePublishAuditPolicyForMutation(audit mutationAuditArgs, items []publishItem, compiled config.Compiled) error {
	if compiled.Defaults.PublishPolicy.RequireActor && strings.TrimSpace(audit.Actor) == "" {
		return errors.New("X-Hookaido-Audit-Actor is required by defaults.publish_policy.require_actor")
	}
	if compiled.Defaults.PublishPolicy.RequireRequestID && strings.TrimSpace(audit.RequestID) == "" {
		return errors.New("X-Request-ID is required by defaults.publish_policy.require_request_id")
	}
	if !publishScopedManagedActorPolicyEnabled(compiled) {
		return nil
	}
	if !publishItemsRequireScopedManagedIdentity(items) {
		return nil
	}
	actor := strings.TrimSpace(audit.Actor)
	if actor == "" {
		return errors.New("X-Hookaido-Audit-Actor is required by defaults.publish_policy.actor_allow/actor_prefix")
	}
	if !publishScopedManagedActorAllowed(actor, compiled) {
		return fmt.Errorf(
			"X-Hookaido-Audit-Actor %q is not allowed for endpoint-scoped managed publish; %s",
			actor,
			formatScopedManagedActorPolicyDetail(
				compiled.Defaults.PublishPolicy.ActorAllowlist,
				compiled.Defaults.PublishPolicy.ActorPrefixes,
			),
		)
	}
	return nil
}

func validateScopedManagedAuditPolicyForFilterMutation(audit mutationAuditArgs, route, application, endpointName string, compiled config.Compiled) error {
	route = strings.TrimSpace(route)
	application = strings.TrimSpace(application)
	endpointName = strings.TrimSpace(endpointName)
	if application != "" || endpointName != "" {
		return validateScopedManagedAuditPolicyForManagedMutation(audit, compiled)
	}
	managedRoutes := compiledManagedRouteSet(compiled)
	if route == "" {
		if len(managedRoutes) == 0 {
			return nil
		}
		return errors.New("managed routes are present; use application+endpoint_name selector")
	}
	if len(managedRoutes) == 0 {
		return nil
	}
	if _, ok := managedRoutes[route]; !ok {
		return nil
	}
	return fmt.Errorf("route %q is managed by application/endpoint; use application+endpoint_name", route)
}

func validateScopedManagedAuditPolicyForIDMutation(store queue.Store, ids []string, allowedStates map[queue.State]struct{}, audit mutationAuditArgs, compiled config.Compiled, compiledAvailable bool) error {
	if store == nil || len(ids) == 0 || !compiledAvailable {
		return nil
	}
	managedRoutes := compiledManagedRouteSet(compiled)
	if len(managedRoutes) == 0 {
		return nil
	}

	touchesManagedRoute, err := queue.IDMutationTouchesManagedRoute(store, ids, allowedStates, managedRoutes)
	if err != nil {
		return err
	}
	if touchesManagedRoute {
		return validateScopedManagedAuditPolicyForManagedMutation(audit, compiled)
	}
	return nil
}

func validateScopedManagedAuditPolicyForManagedMutation(audit mutationAuditArgs, compiled config.Compiled) error {
	if compiled.Defaults.PublishPolicy.RequireActor && strings.TrimSpace(audit.Actor) == "" {
		return errors.New("X-Hookaido-Audit-Actor is required by defaults.publish_policy.require_actor")
	}
	if compiled.Defaults.PublishPolicy.RequireRequestID && strings.TrimSpace(audit.RequestID) == "" {
		return errors.New("X-Request-ID is required by defaults.publish_policy.require_request_id")
	}
	if !publishScopedManagedActorPolicyEnabled(compiled) {
		return nil
	}
	actor := strings.TrimSpace(audit.Actor)
	if actor == "" {
		return errors.New("X-Hookaido-Audit-Actor is required by defaults.publish_policy.actor_allow/actor_prefix")
	}
	if !publishScopedManagedActorAllowed(actor, compiled) {
		return fmt.Errorf(
			"X-Hookaido-Audit-Actor %q is not allowed for endpoint-scoped managed mutation; %s",
			actor,
			formatScopedManagedActorPolicyDetail(
				compiled.Defaults.PublishPolicy.ActorAllowlist,
				compiled.Defaults.PublishPolicy.ActorPrefixes,
			),
		)
	}
	return nil
}

func compiledManagedRouteSet(compiled config.Compiled) map[string]struct{} {
	out := make(map[string]struct{})
	for _, r := range compiled.Routes {
		if strings.TrimSpace(r.Application) == "" || strings.TrimSpace(r.EndpointName) == "" {
			continue
		}
		route := strings.TrimSpace(r.Path)
		if route == "" {
			continue
		}
		out[route] = struct{}{}
	}
	return out
}

func publishItemsRequireScopedManagedIdentity(items []publishItem) bool {
	for _, item := range items {
		if strings.TrimSpace(item.Application) != "" || strings.TrimSpace(item.EndpointName) != "" {
			return true
		}
	}
	return false
}

func publishScopedManagedActorPolicyEnabled(compiled config.Compiled) bool {
	return len(compiled.Defaults.PublishPolicy.ActorAllowlist) > 0 ||
		len(compiled.Defaults.PublishPolicy.ActorPrefixes) > 0
}

func publishScopedManagedActorAllowed(actor string, compiled config.Compiled) bool {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return false
	}
	if !publishScopedManagedActorPolicyEnabled(compiled) {
		return true
	}
	for _, allowed := range compiled.Defaults.PublishPolicy.ActorAllowlist {
		if actor == strings.TrimSpace(allowed) {
			return true
		}
	}
	for _, prefix := range compiled.Defaults.PublishPolicy.ActorPrefixes {
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

func (s *Server) resolveManagedRouteFilter(route, application, endpointName string) (string, error) {
	route = strings.TrimSpace(route)
	application = strings.TrimSpace(application)
	endpointName = strings.TrimSpace(endpointName)
	if (application == "") != (endpointName == "") {
		return "", errors.New("application and endpoint_name must be provided together")
	}
	if err := validateManagedSelectorLabels(application, endpointName); err != nil {
		return "", err
	}
	if application == "" {
		return route, nil
	}
	if strings.TrimSpace(s.ConfigPath) == "" {
		return "", errors.New("config path is not configured")
	}
	compiled, res, err := s.loadCompiledConfig()
	if err != nil {
		return "", err
	}
	if !res.OK {
		return "", fmt.Errorf("config compile failed: %s", strings.Join(res.Errors, "; "))
	}
	return resolveManagedRouteFilterWithCompiled(route, application, endpointName, compiled)
}

func resolveManagedRouteFilterWithCompiled(route, application, endpointName string, compiled config.Compiled) (string, error) {
	route = strings.TrimSpace(route)
	application = strings.TrimSpace(application)
	endpointName = strings.TrimSpace(endpointName)
	if (application == "") != (endpointName == "") {
		return "", errors.New("application and endpoint_name must be provided together")
	}
	if err := validateManagedSelectorLabels(application, endpointName); err != nil {
		return "", err
	}
	if application == "" {
		return route, nil
	}
	routeCfg, err := findCompiledRouteByManagement(compiled, application, endpointName)
	if err != nil {
		return "", err
	}
	if route != "" && route != routeCfg.Path {
		return "", fmt.Errorf("route %q does not match management endpoint (%q, %q) route %q", route, application, endpointName, routeCfg.Path)
	}
	return routeCfg.Path, nil
}

func findCompiledRouteByPath(compiled config.Compiled, routePath string) (*config.CompiledRoute, bool) {
	for i := range compiled.Routes {
		if compiled.Routes[i].Path == routePath {
			return &compiled.Routes[i], true
		}
	}
	return nil, false
}

func findCompiledRouteByManagement(compiled config.Compiled, application string, endpointName string) (*config.CompiledRoute, error) {
	if application == "" || endpointName == "" {
		return nil, errors.New("application and endpoint_name must be provided together")
	}
	if err := validateManagedSelectorLabels(application, endpointName); err != nil {
		return nil, err
	}
	match := -1
	for i := range compiled.Routes {
		r := &compiled.Routes[i]
		if r.Application != application || r.EndpointName != endpointName {
			continue
		}
		if match >= 0 {
			return nil, fmt.Errorf("management endpoint (%q, %q) resolves to multiple routes", application, endpointName)
		}
		match = i
	}
	if match < 0 {
		return nil, fmt.Errorf("management endpoint (%q, %q) not found", application, endpointName)
	}
	return &compiled.Routes[match], nil
}
