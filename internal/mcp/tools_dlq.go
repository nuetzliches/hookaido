package mcp

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/backlog"
	"github.com/nuetzliches/hookaido/internal/queue"
)

func (s *Server) toolDLQList(args map[string]any) (any, error) {
	req, err := parseDeadListArgs(args)
	if err != nil {
		return nil, err
	}

	if compiled, useAdmin := s.queueToolsUseAdminProxy(); useAdmin {
		query := url.Values{}
		if req.Route != "" {
			query.Set("route", req.Route)
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
		return s.callAdminJSON(compiled, http.MethodGet, "/dlq", query, nil, nil, defaultAdminProxyTimeout)
	}

	store, err := s.openQueueStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	resp, err := store.ListDead(req)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(resp.Items))
	for _, it := range resp.Items {
		item := map[string]any{
			"id":          it.ID,
			"route":       it.Route,
			"target":      it.Target,
			"received_at": it.ReceivedAt.Format(time.RFC3339Nano),
			"attempt":     it.Attempt,
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

func (s *Server) toolDLQRequeue(args map[string]any) (any, error) {
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
		out, err := s.callAdminJSON(ctx.compiled, http.MethodPost, "/dlq/requeue", nil, map[string]any{"ids": ids}, mutationAuditHeaders(audit), defaultAdminProxyTimeout)
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
		queue.StateDead: {},
	}, audit, ctx.compiled, ctx.compiledAvailable); err != nil {
		return nil, err
	}

	resp, err := store.RequeueDead(queue.DeadRequeueRequest{IDs: ids})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"requeued": resp.Requeued,
		"audit":    mutationAuditMap(audit, s.auditPrincipal()),
	}, nil
}

func (s *Server) toolDLQDelete(args map[string]any) (any, error) {
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
		out, err := s.callAdminJSON(ctx.compiled, http.MethodPost, "/dlq/delete", nil, map[string]any{"ids": ids}, mutationAuditHeaders(audit), defaultAdminProxyTimeout)
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
		queue.StateDead: {},
	}, audit, ctx.compiled, ctx.compiledAvailable); err != nil {
		return nil, err
	}

	resp, err := store.DeleteDead(queue.DeadDeleteRequest{IDs: ids})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"deleted": resp.Deleted,
		"audit":   mutationAuditMap(audit, s.auditPrincipal()),
	}, nil
}

func parseDeadListArgs(args map[string]any) (queue.DeadListRequest, error) {
	before, err := parseOptionalTime(args, "before")
	if err != nil {
		return queue.DeadListRequest{}, err
	}
	limit, err := parseLimit(args, backlog.MaxListLimit)
	if err != nil {
		return queue.DeadListRequest{}, err
	}
	includePayload, err := parseBool(args, "include_payload")
	if err != nil {
		return queue.DeadListRequest{}, err
	}
	includeHeaders, err := parseBool(args, "include_headers")
	if err != nil {
		return queue.DeadListRequest{}, err
	}
	includeTrace, err := parseBool(args, "include_trace")
	if err != nil {
		return queue.DeadListRequest{}, err
	}
	route, err := parseString(args, "route")
	if err != nil {
		return queue.DeadListRequest{}, err
	}
	if err := validateOptionalRoutePath(route); err != nil {
		return queue.DeadListRequest{}, err
	}
	return queue.DeadListRequest{
		Route:          route,
		Limit:          limit,
		Before:         before,
		IncludePayload: includePayload,
		IncludeHeaders: includeHeaders,
		IncludeTrace:   includeTrace,
	}, nil
}
