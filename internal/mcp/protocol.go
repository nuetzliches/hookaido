package mcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nuetzliches/hookaido/internal/backlog"
	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/hookaido"
	"github.com/nuetzliches/hookaido/internal/queue"
	"github.com/nuetzliches/hookaido/internal/secrets"
)

const (
	protocolVersion           = "2024-11-05"
	maxAuditReasonLength      = 512
	maxAuditActorLength       = 256
	maxAuditRequestIDLength   = 256
	defaultStartStopTimeout   = 10 * time.Second
	defaultReloadCheckTimeout = 5 * time.Second
	defaultAdminProxyTimeout  = 5 * time.Second
	adminProxyRetryMaxGET     = 3
	adminProxyRetryBackoff    = 100 * time.Millisecond
	healthTrendSignalSamples  = 2000

	adminAuditReasonHeader    = "X-Hookaido-Audit-Reason"
	adminAuditActorHeader     = "X-Hookaido-Audit-Actor"
	adminAuditRequestIDHeader = "X-Request-ID"
)

func managementLabelSchema() map[string]any {
	return map[string]any{
		"type":    "string",
		"pattern": config.ManagementLabelPattern(),
	}
}

func routePathSchema() map[string]any {
	return map[string]any{
		"type":    "string",
		"pattern": "^/.*$",
	}
}

var (
	configApplyAllowedKeys = keySet(
		"path",
		"content",
		"mode",
		"reload_timeout",
	)
	managementEndpointUpsertAllowedKeys = keySet(
		"path",
		"application",
		"endpoint_name",
		"route",
		"reason",
		"actor",
		"request_id",
		"mode",
		"reload_timeout",
	)
	managementEndpointDeleteAllowedKeys = keySet(
		"path",
		"application",
		"endpoint_name",
		"reason",
		"actor",
		"request_id",
		"mode",
		"reload_timeout",
	)
	idMutationAllowedKeys = keySet(
		"reason",
		"actor",
		"request_id",
		"ids",
	)
	instanceStartAllowedKeys = keySet(
		"pid_file",
		"timeout",
	)
	instanceStopAllowedKeys = keySet(
		"pid_file",
		"timeout",
		"force",
	)
	instanceReloadAllowedKeys = keySet(
		"pid_file",
		"timeout",
	)
	publishToolAllowedKeys = keySet(
		"reason",
		"actor",
		"request_id",
		"items",
	)
	publishItemAllowedKeys = keySet(
		"id",
		"route",
		"target",
		"application",
		"endpoint_name",
		"payload_b64",
		"received_at",
		"next_run_at",
		"headers",
		"trace",
	)
	messageManageFilterAllowedKeys = keySet(
		"reason",
		"actor",
		"request_id",
		"route",
		"application",
		"endpoint_name",
		"target",
		"state",
		"before",
		"limit",
		"preview_only",
	)
)

type Role string

const (
	RoleRead    Role = "read"
	RoleOperate Role = "operate"
	RoleAdmin   Role = "admin"
)

func ParseRole(raw string) (Role, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", string(RoleRead):
		return RoleRead, nil
	case string(RoleOperate):
		return RoleOperate, nil
	case string(RoleAdmin):
		return RoleAdmin, nil
	default:
		return "", fmt.Errorf("invalid MCP role %q (supported: read, operate, admin)", strings.TrimSpace(raw))
	}
}

type Server struct {
	ConfigPath                  string
	DBPath                      string
	PIDFilePath                 string
	RunBinaryPath               string
	RunWatch                    bool
	RunLogLevel                 string
	RunDotenvPath               string
	AdminProxyEndpointAllowlist []string
	In                          io.Reader
	Out                         io.Writer

	MutationsEnabled       bool
	RuntimeControlEnabled  bool
	Role                   Role
	Principal              string
	AuditWriter            io.Writer
	rollbackAttemptsTotal  atomic.Int64
	rollbackSucceededTotal atomic.Int64
	rollbackFailedTotal    atomic.Int64
	rollbackIDsTotal       atomic.Int64
}

type Option func(*Server)

func WithMutationsEnabled(enabled bool) Option {
	return func(s *Server) {
		s.MutationsEnabled = enabled
	}
}

func WithRuntimeControlEnabled(enabled bool) Option {
	return func(s *Server) {
		s.RuntimeControlEnabled = enabled
	}
}

func WithRole(role Role) Option {
	return func(s *Server) {
		if parsed, err := ParseRole(string(role)); err == nil {
			s.Role = parsed
			return
		}
		s.Role = RoleRead
	}
}

func WithPrincipal(principal string) Option {
	return func(s *Server) {
		s.Principal = strings.TrimSpace(principal)
	}
}

func WithAuditWriter(w io.Writer) Option {
	return func(s *Server) {
		s.AuditWriter = w
	}
}

func WithRuntimeControlPIDFile(path string) Option {
	return func(s *Server) {
		s.PIDFilePath = strings.TrimSpace(path)
	}
}

func WithRuntimeControlRunBinary(path string) Option {
	return func(s *Server) {
		s.RunBinaryPath = strings.TrimSpace(path)
	}
}

func WithRuntimeControlRunWatch(enabled bool) Option {
	return func(s *Server) {
		s.RunWatch = enabled
	}
}

func WithRuntimeControlRunLogLevel(level string) Option {
	return func(s *Server) {
		s.RunLogLevel = strings.TrimSpace(level)
	}
}

func WithRuntimeControlRunDotenv(path string) Option {
	return func(s *Server) {
		s.RunDotenvPath = strings.TrimSpace(path)
	}
}

func WithAdminProxyEndpointAllowlist(entries []string) Option {
	return func(s *Server) {
		if len(entries) == 0 {
			s.AdminProxyEndpointAllowlist = nil
			return
		}
		normalized := make([]string, 0, len(entries))
		seen := make(map[string]struct{}, len(entries))
		for _, raw := range entries {
			entry := strings.TrimSpace(raw)
			if entry == "" {
				continue
			}
			if _, ok := seen[entry]; ok {
				continue
			}
			seen[entry] = struct{}{}
			normalized = append(normalized, entry)
		}
		s.AdminProxyEndpointAllowlist = normalized
	}
}

func NewServer(in io.Reader, out io.Writer, configPath, dbPath string, opts ...Option) *Server {
	runBinary := ""
	if exe, err := os.Executable(); err == nil {
		runBinary = strings.TrimSpace(exe)
	}
	s := &Server{
		ConfigPath:    strings.TrimSpace(configPath),
		DBPath:        strings.TrimSpace(dbPath),
		RunBinaryPath: runBinary,
		RunWatch:      true,
		RunLogLevel:   "info",
		Role:          RoleRead,
		AuditWriter:   io.Discard,
		In:            in,
		Out:           out,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) Serve(ctx context.Context) error {
	if s == nil {
		return errors.New("nil mcp server")
	}
	if s.In == nil {
		return errors.New("nil input reader")
	}
	if s.Out == nil {
		return errors.New("nil output writer")
	}

	r := bufio.NewReader(s.In)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		payload, err := readFrame(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			resp := rpcResponse{
				JSONRPC: "2.0",
				Error:   &rpcError{Code: -32700, Message: "parse error"},
			}
			_ = writeFrame(s.Out, resp)
			continue
		}

		var req rpcRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			resp := rpcResponse{
				JSONRPC: "2.0",
				Error:   &rpcError{Code: -32700, Message: "parse error"},
			}
			_ = writeFrame(s.Out, resp)
			continue
		}

		resp := s.handleRequest(req)
		if resp == nil {
			continue
		}
		if err := writeFrame(s.Out, resp); err != nil {
			return err
		}
	}
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *rpcError `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type initializeResult struct {
	ProtocolVersion string         `json:"protocolVersion"`
	Capabilities    map[string]any `json:"capabilities"`
	ServerInfo      serverInfo     `json:"serverInfo"`
}

type serverInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type toolsListResult struct {
	Tools []toolDescriptor `json:"tools"`
}

type toolDescriptor struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

type toolsCallParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
}

type toolsCallResult struct {
	Content           []toolContent `json:"content"`
	StructuredContent any           `json:"structuredContent,omitempty"`
	IsError           bool          `json:"isError,omitempty"`
}

type toolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func (s *Server) handleRequest(req rpcRequest) *rpcResponse {
	if req.JSONRPC != "2.0" {
		return s.errorResponse(req.ID, -32600, "invalid request")
	}

	switch req.Method {
	case "initialize":
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: initializeResult{
				ProtocolVersion: protocolVersion,
				Capabilities: map[string]any{
					"tools": map[string]any{},
				},
				ServerInfo: serverInfo{
					Name:    "hookaido",
					Version: "0.0.0-dev",
				},
			},
		}
	case "notifications/initialized":
		return nil
	case "ping":
		if req.ID == nil {
			return nil
		}
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  map[string]any{},
		}
	case "tools/list":
		if req.ID == nil {
			return nil
		}
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  toolsListResult{Tools: s.toolDescriptors()},
		}
	case "tools/call":
		if req.ID == nil {
			return nil
		}
		var params toolsCallParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return s.errorResponse(req.ID, -32602, "invalid params")
		}
		if strings.TrimSpace(params.Name) == "" {
			return s.errorResponse(req.ID, -32602, "invalid params: missing tool name")
		}
		if params.Arguments == nil {
			params.Arguments = map[string]any{}
		}

		result := s.callTool(params.Name, params.Arguments)
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  result,
		}
	default:
		if req.ID == nil {
			return nil
		}
		return s.errorResponse(req.ID, -32601, "method not found")
	}
}

func (s *Server) errorResponse(id any, code int, msg string) *rpcResponse {
	return &rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &rpcError{
			Code:    code,
			Message: msg,
		},
	}
}

func (s *Server) effectiveRole() Role {
	if s == nil {
		return RoleRead
	}
	role, err := ParseRole(string(s.Role))
	if err != nil {
		return RoleRead
	}
	return role
}

func roleRank(role Role) int {
	switch role {
	case RoleAdmin:
		return 3
	case RoleOperate:
		return 2
	default:
		return 1
	}
}

func (s *Server) roleAllows(required Role) bool {
	return roleRank(s.effectiveRole()) >= roleRank(required)
}

func requiredRoleForTool(name string) (Role, bool) {
	switch name {
	case "config_parse", "config_validate", "config_compile", "config_fmt_preview", "config_diff", "admin_health", "management_model",
		"backlog_top_queued", "backlog_oldest_queued", "backlog_aging_summary", "backlog_trends",
		"messages_list", "attempts_list", "dlq_list":
		return RoleRead, true
	case "dlq_requeue", "dlq_delete", "messages_cancel", "messages_requeue", "messages_resume",
		"messages_publish", "messages_cancel_by_filter", "messages_requeue_by_filter", "messages_resume_by_filter",
		"instance_status", "instance_logs_tail":
		return RoleOperate, true
	case "config_apply", "management_endpoint_upsert", "management_endpoint_delete",
		"instance_start", "instance_stop", "instance_reload":
		return RoleAdmin, true
	default:
		return "", false
	}
}

func toolRequiresMutationsFlag(name string) bool {
	switch name {
	case "config_apply", "management_endpoint_upsert", "management_endpoint_delete",
		"dlq_requeue", "dlq_delete", "messages_cancel", "messages_requeue", "messages_resume",
		"messages_publish", "messages_cancel_by_filter", "messages_requeue_by_filter", "messages_resume_by_filter":
		return true
	default:
		return false
	}
}

func toolRequiresRuntimeControlFlag(name string) bool {
	switch name {
	case "instance_start", "instance_status", "instance_logs_tail", "instance_stop", "instance_reload":
		return true
	default:
		return false
	}
}

func toolIsMutating(name string) bool {
	switch name {
	case "config_apply", "management_endpoint_upsert", "management_endpoint_delete",
		"dlq_requeue", "dlq_delete", "messages_cancel", "messages_requeue", "messages_resume",
		"messages_publish", "messages_cancel_by_filter", "messages_requeue_by_filter", "messages_resume_by_filter",
		"instance_start", "instance_stop", "instance_reload":
		return true
	default:
		return false
	}
}

func (s *Server) auditPrincipal() string {
	return strings.TrimSpace(s.Principal)
}

func (s *Server) toolAccessError(name string) error {
	requiredRole, ok := requiredRoleForTool(name)
	if !ok {
		return fmt.Errorf("unknown tool %q", name)
	}
	if toolRequiresMutationsFlag(name) && !s.MutationsEnabled {
		return fmt.Errorf("tool %q is disabled (start server with --enable-mutations)", name)
	}
	if toolRequiresRuntimeControlFlag(name) && !s.RuntimeControlEnabled {
		return fmt.Errorf("tool %q is disabled (start server with --enable-runtime-control)", name)
	}
	if !s.roleAllows(requiredRole) {
		return fmt.Errorf("tool %q is not permitted for role %q (requires role %q)", name, s.effectiveRole(), requiredRole)
	}
	if toolIsMutating(name) && s.auditPrincipal() == "" {
		return fmt.Errorf("tool %q requires configured MCP principal (--principal)", name)
	}
	return nil
}

func (s *Server) callTool(name string, args map[string]any) toolsCallResult {
	var (
		out any
		err error
	)
	var meta map[string]any
	if configMeta := configMutationAuditMetadata(name, nil, args); len(configMeta) > 0 {
		meta = map[string]any{
			"config_mutation": configMeta,
		}
	}
	if runtimeMeta := runtimeControlAuditMetadata(name, nil, args); len(runtimeMeta) > 0 {
		if meta == nil {
			meta = map[string]any{}
		}
		meta["runtime_control"] = runtimeMeta
	}
	if idMeta := idMutationAuditMetadata(name, nil, args); len(idMeta) > 0 {
		if meta == nil {
			meta = map[string]any{}
		}
		meta["id_mutation"] = idMeta
	}
	if filterMeta := filterMutationAuditMetadata(name, nil, args); len(filterMeta) > 0 {
		if meta == nil {
			meta = map[string]any{}
		}
		meta["filter_mutation"] = filterMeta
	}
	var rollbackBefore rollbackCounterSnapshot
	includeRollbackMeta := name == "messages_publish"
	if includeRollbackMeta {
		rollbackBefore = s.rollbackCounterSnapshot()
	}
	started := time.Now()
	if accessErr := s.toolAccessError(name); accessErr != nil {
		s.emitMutationAuditEvent(name, args, started, "denied", accessErr, meta)
		return toolErrorf("%v", accessErr)
	}

	switch name {
	case "config_parse":
		out, err = s.toolConfigParse(args)
	case "config_validate":
		out, err = s.toolConfigValidate(args)
	case "config_compile":
		out, err = s.toolConfigCompile(args)
	case "config_fmt_preview":
		out, err = s.toolConfigFmtPreview(args)
	case "config_diff":
		out, err = s.toolConfigDiff(args)
	case "config_apply":
		out, err = s.toolConfigApply(args)
	case "admin_health":
		out, err = s.toolAdminHealth(args)
	case "management_model":
		out, err = s.toolManagementModel(args)
	case "management_endpoint_upsert":
		out, err = s.toolManagementEndpointUpsert(args)
	case "management_endpoint_delete":
		out, err = s.toolManagementEndpointDelete(args)
	case "backlog_top_queued":
		out, err = s.toolBacklogTopQueued(args)
	case "backlog_oldest_queued":
		out, err = s.toolBacklogOldestQueued(args)
	case "backlog_aging_summary":
		out, err = s.toolBacklogAgingSummary(args)
	case "backlog_trends":
		out, err = s.toolBacklogTrends(args)
	case "messages_list":
		out, err = s.toolMessagesList(args)
	case "attempts_list":
		out, err = s.toolAttemptsList(args)
	case "dlq_list":
		out, err = s.toolDLQList(args)
	case "dlq_requeue":
		out, err = s.toolDLQRequeue(args)
	case "dlq_delete":
		out, err = s.toolDLQDelete(args)
	case "messages_cancel":
		out, err = s.toolMessagesCancel(args)
	case "messages_requeue":
		out, err = s.toolMessagesRequeue(args)
	case "messages_resume":
		out, err = s.toolMessagesResume(args)
	case "messages_publish":
		out, err = s.toolMessagesPublish(args)
	case "messages_cancel_by_filter":
		out, err = s.toolMessagesCancelByFilter(args)
	case "messages_requeue_by_filter":
		out, err = s.toolMessagesRequeueByFilter(args)
	case "messages_resume_by_filter":
		out, err = s.toolMessagesResumeByFilter(args)
	case "instance_start":
		out, err = s.toolInstanceStart(args)
	case "instance_status":
		out, err = s.toolInstanceStatus(args)
	case "instance_logs_tail":
		out, err = s.toolInstanceLogsTail(args)
	case "instance_stop":
		out, err = s.toolInstanceStop(args)
	case "instance_reload":
		out, err = s.toolInstanceReload(args)
	default:
		return toolErrorf("unknown tool %q", name)
	}

	if includeRollbackMeta {
		if meta == nil {
			meta = map[string]any{}
		}
		meta["admin_proxy_publish"] = rollbackAuditMetadata(rollbackBefore, s.rollbackCounterSnapshot())
	}
	if idMeta := idMutationAuditMetadata(name, out, args); len(idMeta) > 0 {
		if meta == nil {
			meta = map[string]any{}
		}
		meta["id_mutation"] = idMeta
	}
	if filterMeta := filterMutationAuditMetadata(name, out, args); len(filterMeta) > 0 {
		if meta == nil {
			meta = map[string]any{}
		}
		meta["filter_mutation"] = filterMeta
	}
	if configMeta := configMutationAuditMetadata(name, out, args); len(configMeta) > 0 {
		if meta == nil {
			meta = map[string]any{}
		}
		meta["config_mutation"] = configMeta
	}
	if runtimeMeta := runtimeControlAuditMetadata(name, out, args); len(runtimeMeta) > 0 {
		if meta == nil {
			meta = map[string]any{}
		}
		meta["runtime_control"] = runtimeMeta
	}
	if err != nil {
		s.emitMutationAuditEvent(name, args, started, "error", err, meta)
		return toolErrorf("%v", err)
	}
	s.emitMutationAuditEvent(name, args, started, "success", nil, meta)
	return toolSuccess(out)
}

func (s *Server) emitMutationAuditEvent(name string, args map[string]any, started time.Time, result string, callErr error, meta map[string]any) {
	if !toolIsMutating(name) {
		return
	}
	if s == nil || s.AuditWriter == nil {
		return
	}

	event := map[string]any{
		"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
		"principal":   s.auditPrincipal(),
		"role":        s.effectiveRole(),
		"tool":        name,
		"input_hash":  toolInputHash(args),
		"result":      result,
		"duration_ms": time.Since(started).Milliseconds(),
	}
	if callErr != nil {
		event["error"] = callErr.Error()
	}
	if len(meta) > 0 {
		event["metadata"] = meta
	}
	_ = json.NewEncoder(s.AuditWriter).Encode(event)
}

type rollbackCounterSnapshot struct {
	AttemptsTotal  int64
	SucceededTotal int64
	FailedTotal    int64
	IDsTotal       int64
}

func (s *Server) rollbackCounterSnapshot() rollbackCounterSnapshot {
	if s == nil {
		return rollbackCounterSnapshot{}
	}
	return rollbackCounterSnapshot{
		AttemptsTotal:  s.rollbackAttemptsTotal.Load(),
		SucceededTotal: s.rollbackSucceededTotal.Load(),
		FailedTotal:    s.rollbackFailedTotal.Load(),
		IDsTotal:       s.rollbackIDsTotal.Load(),
	}
}

func rollbackAuditMetadata(before, after rollbackCounterSnapshot) map[string]any {
	out := map[string]any{
		"rollback_attempts_total":  after.AttemptsTotal,
		"rollback_succeeded_total": after.SucceededTotal,
		"rollback_failed_total":    after.FailedTotal,
		"rollback_ids_total":       after.IDsTotal,
	}
	if delta := after.AttemptsTotal - before.AttemptsTotal; delta >= 0 {
		out["rollback_attempts"] = delta
	}
	if delta := after.SucceededTotal - before.SucceededTotal; delta >= 0 {
		out["rollback_succeeded"] = delta
	}
	if delta := after.FailedTotal - before.FailedTotal; delta >= 0 {
		out["rollback_failed"] = delta
	}
	if delta := after.IDsTotal - before.IDsTotal; delta >= 0 {
		out["rollback_ids"] = delta
	}
	return out
}

func idMutationAuditMetadata(name string, out any, args map[string]any) map[string]any {
	operation, changedField, ok := idMutationAuditDescriptor(name)
	if !ok {
		return nil
	}

	meta := map[string]any{
		"operation":     operation,
		"changed_field": changedField,
	}
	if requested, unique, ok := idsCountFromArgs(args); ok {
		meta["ids_requested"] = requested
		meta["ids_unique"] = unique
	}
	if outMap, ok := out.(map[string]any); ok {
		if raw, ok := outMap[changedField]; ok {
			if changed, ok := intFromAnyForError(raw); ok && changed >= 0 {
				meta["changed"] = changed
			}
		}
	}
	return meta
}

func idMutationAuditDescriptor(name string) (operation, changedField string, ok bool) {
	switch strings.TrimSpace(name) {
	case "dlq_requeue":
		return "dlq_requeue", "requeued", true
	case "dlq_delete":
		return "dlq_delete", "deleted", true
	case "messages_cancel":
		return "messages_cancel", "canceled", true
	case "messages_requeue":
		return "messages_requeue", "requeued", true
	case "messages_resume":
		return "messages_resume", "resumed", true
	default:
		return "", "", false
	}
}

func idsCountFromArgs(args map[string]any) (requested, unique int, ok bool) {
	if args == nil {
		return 0, 0, false
	}
	raw, ok := args["ids"]
	if !ok {
		return 0, 0, false
	}

	seen := map[string]struct{}{}
	switch v := raw.(type) {
	case []any:
		requested = len(v)
		for _, item := range v {
			id, ok := item.(string)
			if !ok {
				continue
			}
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			seen[id] = struct{}{}
		}
		return requested, len(seen), true
	case []string:
		requested = len(v)
		for _, id := range v {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			seen[id] = struct{}{}
		}
		return requested, len(seen), true
	default:
		return 0, 0, false
	}
}

func filterMutationAuditMetadata(name string, out any, args map[string]any) map[string]any {
	operation, changedField, ok := filterMutationAuditDescriptor(name)
	if !ok {
		return nil
	}

	meta := map[string]any{
		"operation":     operation,
		"changed_field": changedField,
	}
	if args != nil {
		if preview, ok := boolFromAny(args["preview_only"]); ok {
			meta["preview_only"] = preview
		}
	}
	if outMap, ok := out.(map[string]any); ok {
		if raw, ok := outMap["matched"]; ok {
			if matched, ok := intFromAnyForError(raw); ok && matched >= 0 {
				meta["matched"] = matched
			}
		}
		if raw, ok := outMap[changedField]; ok {
			if changed, ok := intFromAnyForError(raw); ok && changed >= 0 {
				meta["changed"] = changed
			}
		}
		if preview, ok := boolFromAny(outMap["preview_only"]); ok {
			meta["preview_only"] = preview
		}
	}
	return meta
}

func filterMutationAuditDescriptor(name string) (operation, changedField string, ok bool) {
	switch strings.TrimSpace(name) {
	case "messages_cancel_by_filter":
		return "cancel_by_filter", "canceled", true
	case "messages_requeue_by_filter":
		return "requeue_by_filter", "requeued", true
	case "messages_resume_by_filter":
		return "resume_by_filter", "resumed", true
	default:
		return "", "", false
	}
}

func configMutationAuditMetadata(name string, out any, args map[string]any) map[string]any {
	switch strings.TrimSpace(name) {
	case "config_apply":
		meta := map[string]any{
			"operation": "config_apply",
		}
		mode := "preview_only"
		if args != nil {
			if raw, ok := stringFromAny(args["mode"]); ok && raw != "" {
				mode = strings.ToLower(raw)
			}
			if raw, ok := stringFromAny(args["path"]); ok && raw != "" {
				meta["path"] = raw
			}
		}
		meta["mode"] = mode
		if outMap, ok := out.(map[string]any); ok {
			if raw, ok := stringFromAny(outMap["mode"]); ok && raw != "" {
				meta["mode"] = raw
			}
			if raw, ok := stringFromAny(outMap["path"]); ok && raw != "" {
				meta["path"] = raw
			}
			if v, ok := boolFromAny(outMap["ok"]); ok {
				meta["ok"] = v
			}
			if v, ok := boolFromAny(outMap["applied"]); ok {
				meta["applied"] = v
			}
			if v, ok := boolFromAny(outMap["reloaded"]); ok {
				meta["reloaded"] = v
			}
			if v, ok := boolFromAny(outMap["rolled_back"]); ok {
				meta["rolled_back"] = v
			}
			if v, ok := boolFromAny(outMap["parse_only"]); ok {
				meta["parse_only"] = v
			}
		}
		return meta
	case "management_endpoint_upsert", "management_endpoint_delete":
		meta := map[string]any{
			"operation": strings.TrimSpace(name),
		}
		mode := "write_only"
		if args != nil {
			if raw, ok := stringFromAny(args["mode"]); ok && raw != "" {
				mode = strings.ToLower(raw)
			}
			if raw, ok := stringFromAny(args["application"]); ok && raw != "" {
				meta["application"] = raw
			}
			if raw, ok := stringFromAny(args["endpoint_name"]); ok && raw != "" {
				meta["endpoint_name"] = raw
			}
			if raw, ok := stringFromAny(args["route"]); ok && raw != "" {
				meta["route"] = raw
			}
		}
		meta["mode"] = mode
		if outMap, ok := out.(map[string]any); ok {
			if raw, ok := stringFromAny(outMap["mode"]); ok && raw != "" {
				meta["mode"] = raw
			}
			if raw, ok := stringFromAny(outMap["application"]); ok && raw != "" {
				meta["application"] = raw
			}
			if raw, ok := stringFromAny(outMap["endpoint_name"]); ok && raw != "" {
				meta["endpoint_name"] = raw
			}
			if raw, ok := stringFromAny(outMap["route"]); ok && raw != "" {
				meta["route"] = raw
			}
			if raw, ok := stringFromAny(outMap["action"]); ok && raw != "" {
				meta["action"] = raw
			}
			if v, ok := boolFromAny(outMap["ok"]); ok {
				meta["ok"] = v
			}
			if v, ok := boolFromAny(outMap["mutates_config"]); ok {
				meta["mutates_config"] = v
			}
			if applyOut, ok := outMap["config_apply"].(map[string]any); ok {
				if v, ok := boolFromAny(applyOut["applied"]); ok {
					meta["applied"] = v
				}
				if v, ok := boolFromAny(applyOut["reloaded"]); ok {
					meta["reloaded"] = v
				}
				if v, ok := boolFromAny(applyOut["rolled_back"]); ok {
					meta["rolled_back"] = v
				}
			}
		}
		return meta
	default:
		return nil
	}
}

func runtimeControlAuditMetadata(name string, out any, args map[string]any) map[string]any {
	switch strings.TrimSpace(name) {
	case "instance_start", "instance_stop", "instance_reload":
	default:
		return nil
	}

	meta := map[string]any{
		"operation": strings.TrimSpace(name),
	}
	if args != nil {
		if raw, ok := stringFromAny(args["pid_file"]); ok && raw != "" {
			meta["pid_file"] = raw
		}
		if raw, ok := stringFromAny(args["timeout"]); ok && raw != "" {
			meta["timeout"] = raw
		}
		if v, ok := boolFromAny(args["force"]); ok {
			meta["force"] = v
		}
	}
	if outMap, ok := out.(map[string]any); ok {
		if raw, ok := stringFromAny(outMap["pid_file"]); ok && raw != "" {
			meta["pid_file"] = raw
		}
		if raw, ok := stringFromAny(outMap["timeout"]); ok && raw != "" {
			meta["timeout"] = raw
		}
		for _, key := range []string{
			"ok",
			"started",
			"already_running",
			"stopped",
			"already_stopped",
			"reloaded",
			"signaled",
			"force",
			"forced",
		} {
			if v, ok := boolFromAny(outMap[key]); ok {
				meta[key] = v
			}
		}
		if raw := outMap["pid"]; raw != nil {
			if pid, ok := intFromAnyForError(raw); ok && pid > 0 {
				meta["pid"] = pid
			}
		}
	}
	return meta
}

func stringFromAny(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x), true
	default:
		return "", false
	}
}

func boolFromAny(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(x))
		if err == nil {
			return b, true
		}
	case int:
		if x == 0 || x == 1 {
			return x == 1, true
		}
	case int8:
		if x == 0 || x == 1 {
			return x == 1, true
		}
	case int16:
		if x == 0 || x == 1 {
			return x == 1, true
		}
	case int32:
		if x == 0 || x == 1 {
			return x == 1, true
		}
	case int64:
		if x == 0 || x == 1 {
			return x == 1, true
		}
	case uint:
		if x == 0 || x == 1 {
			return x == 1, true
		}
	case uint8:
		if x == 0 || x == 1 {
			return x == 1, true
		}
	case uint16:
		if x == 0 || x == 1 {
			return x == 1, true
		}
	case uint32:
		if x == 0 || x == 1 {
			return x == 1, true
		}
	case uint64:
		if x == 0 || x == 1 {
			return x == 1, true
		}
	}
	return false, false
}

func toolInputHash(args map[string]any) string {
	if args == nil {
		args = map[string]any{}
	}
	raw, err := json.Marshal(args)
	if err != nil {
		raw = []byte("{}")
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum)
}

func toolSuccess(out any) toolsCallResult {
	text := formatToolText(out)
	return toolsCallResult{
		Content: []toolContent{
			{Type: "text", Text: text},
		},
		StructuredContent: out,
	}
}

func toolErrorf(format string, args ...any) toolsCallResult {
	msg := fmt.Sprintf(format, args...)
	return toolsCallResult{
		Content: []toolContent{
			{Type: "text", Text: msg},
		},
		IsError: true,
	}
}

func formatToolText(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "{}"
	}
	return string(b)
}

func (s *Server) toolDescriptors() []toolDescriptor {
	tools := []toolDescriptor{
		{
			Name:        "config_parse",
			Description: "Parse Hookaidofile and return structured AST or parser errors",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{"type": "string"},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "config_validate",
			Description: "Validate Hookaidofile and return errors/warnings",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":           map[string]any{"type": "string"},
					"strict_secrets": map[string]any{"type": "boolean"},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "config_compile",
			Description: "Compile Hookaidofile and return runtime summary plus validation result",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{"type": "string"},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "config_fmt_preview",
			Description: "Format Hookaidofile without writing changes",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{"type": "string"},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "config_diff",
			Description: "Compute normalized unified diff between current and candidate Hookaidofile content",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{
						"type":        "string",
						"description": "Must match configured --config path",
					},
					"content": map[string]any{
						"type":        "string",
						"description": "Candidate Hookaidofile content",
					},
					"context": map[string]any{
						"type":        "integer",
						"minimum":     0,
						"maximum":     20,
						"description": "Unified diff context lines (default 3)",
					},
				},
				"required":             []string{"content"},
				"additionalProperties": false,
			},
		},
		{
			Name:        "admin_health",
			Description: "Read-only health snapshot (local config/db/queue checks plus optional admin API diagnostics probe)",
			InputSchema: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{},
				"additionalProperties": false,
			},
		},
		{
			Name:        "management_model",
			Description: "Read management model projection (application/endpoint to route+targets) from compiled config",
			InputSchema: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{},
				"additionalProperties": false,
			},
		},
		{
			Name:        "backlog_top_queued",
			Description: "Read bounded top queued backlog buckets from queue stats with optional route/target filters",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"route":  routePathSchema(),
					"target": map[string]any{"type": "string"},
					"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": backlog.MaxListLimit},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "backlog_oldest_queued",
			Description: "Read oldest queued messages ordered by received_at with optional route/target filters",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"route":  routePathSchema(),
					"target": map[string]any{"type": "string"},
					"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": backlog.MaxListLimit},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "backlog_aging_summary",
			Description: "Read route/target backlog aging summary with state scan metadata, age windows, and age percentiles",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"route":  routePathSchema(),
					"target": map[string]any{"type": "string"},
					"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": backlog.MaxListLimit},
					"states": map[string]any{
						"type":  "array",
						"items": map[string]any{"type": "string", "enum": []string{"queued", "leased", "dead"}},
					},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "backlog_trends",
			Description: "Read persisted backlog trend rollups across queued/leased/dead over a window",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"route":  routePathSchema(),
					"target": map[string]any{"type": "string"},
					"window": map[string]any{"type": "string", "description": "Duration window (default 1h, max 168h)"},
					"step":   map[string]any{"type": "string", "description": "Rollup step (default 5m, min 1m, max 1h)"},
					"until":  map[string]any{"type": "string", "description": "RFC3339 timestamp upper bound (default now)"},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "messages_list",
			Description: "List queue messages from the active backend with optional filters",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"route":           routePathSchema(),
					"application":     managementLabelSchema(),
					"endpoint_name":   managementLabelSchema(),
					"target":          map[string]any{"type": "string"},
					"state":           map[string]any{"type": "string", "enum": []string{"queued", "leased", "delivered", "dead", "canceled"}},
					"limit":           map[string]any{"type": "integer", "minimum": 1, "maximum": backlog.MaxListLimit},
					"before":          map[string]any{"type": "string", "description": "RFC3339 timestamp"},
					"include_payload": map[string]any{"type": "boolean"},
					"include_headers": map[string]any{"type": "boolean"},
					"include_trace":   map[string]any{"type": "boolean"},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "attempts_list",
			Description: "List delivery attempts from the active backend with optional filters",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"route":         routePathSchema(),
					"application":   managementLabelSchema(),
					"endpoint_name": managementLabelSchema(),
					"target":        map[string]any{"type": "string"},
					"event_id":      map[string]any{"type": "string"},
					"outcome":       map[string]any{"type": "string", "enum": []string{"acked", "retry", "dead"}},
					"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": backlog.MaxListLimit},
					"before":        map[string]any{"type": "string", "description": "RFC3339 timestamp"},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "dlq_list",
			Description: "List dead-letter queue items from the active backend",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"route":           routePathSchema(),
					"limit":           map[string]any{"type": "integer", "minimum": 1, "maximum": backlog.MaxListLimit},
					"before":          map[string]any{"type": "string", "description": "RFC3339 timestamp"},
					"include_payload": map[string]any{"type": "boolean"},
					"include_headers": map[string]any{"type": "boolean"},
					"include_trace":   map[string]any{"type": "boolean"},
				},
				"additionalProperties": false,
			},
		},
	}
	if s.MutationsEnabled {
		tools = append(tools,
			toolDescriptor{
				Name:        "config_apply",
				Description: "Validate+compile candidate config and optionally apply atomically (with optional reload health check)",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"path": map[string]any{
							"type":        "string",
							"description": "Must match configured --config path",
						},
						"content": map[string]any{
							"type":        "string",
							"description": "Candidate Hookaidofile content",
						},
						"mode": map[string]any{
							"type": "string",
							"enum": []string{"preview_only", "write_only", "write_and_reload"},
						},
						"reload_timeout": map[string]any{
							"type":        "string",
							"description": "Duration for write_and_reload health check (default 5s)",
						},
					},
					"required":             []string{"content"},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "management_endpoint_upsert",
				Description: "Upsert application/endpoint mapping onto an existing route in Hookaidofile",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"path": map[string]any{
							"type":        "string",
							"description": "Must match configured --config path",
						},
						"application":   managementLabelSchema(),
						"endpoint_name": managementLabelSchema(),
						"route":         routePathSchema(),
						"reason": map[string]any{
							"type":        "string",
							"description": "Required audit reason (maps to X-Hookaido-Audit-Reason semantics)",
						},
						"actor": map[string]any{
							"type": "string",
						},
						"request_id": map[string]any{
							"type": "string",
						},
						"mode": map[string]any{
							"type": "string",
							"enum": []string{"write_only", "write_and_reload", "preview_only"},
						},
						"reload_timeout": map[string]any{
							"type":        "string",
							"description": "Duration for write_and_reload health check (default 5s)",
						},
					},
					"required":             []string{"application", "endpoint_name", "route", "reason"},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "management_endpoint_delete",
				Description: "Delete application/endpoint mapping labels from Hookaidofile",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"path": map[string]any{
							"type":        "string",
							"description": "Must match configured --config path",
						},
						"application":   managementLabelSchema(),
						"endpoint_name": managementLabelSchema(),
						"reason": map[string]any{
							"type":        "string",
							"description": "Required audit reason (maps to X-Hookaido-Audit-Reason semantics)",
						},
						"actor": map[string]any{
							"type": "string",
						},
						"request_id": map[string]any{
							"type": "string",
						},
						"mode": map[string]any{
							"type": "string",
							"enum": []string{"write_only", "write_and_reload", "preview_only"},
						},
						"reload_timeout": map[string]any{
							"type":        "string",
							"description": "Duration for write_and_reload health check (default 5s)",
						},
					},
					"required":             []string{"application", "endpoint_name", "reason"},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "dlq_requeue",
				Description: "Requeue dead-letter queue items by ID",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reason": map[string]any{
							"type":        "string",
							"description": "Required audit reason (maps to X-Hookaido-Audit-Reason semantics)",
						},
						"actor": map[string]any{
							"type": "string",
						},
						"request_id": map[string]any{
							"type": "string",
						},
						"ids": map[string]any{
							"type":     "array",
							"items":    map[string]any{"type": "string"},
							"minItems": 1,
							"maxItems": backlog.MaxListLimit,
						},
					},
					"required":             []string{"ids", "reason"},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "dlq_delete",
				Description: "Delete dead-letter queue items by ID",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reason": map[string]any{
							"type":        "string",
							"description": "Required audit reason (maps to X-Hookaido-Audit-Reason semantics)",
						},
						"actor": map[string]any{
							"type": "string",
						},
						"request_id": map[string]any{
							"type": "string",
						},
						"ids": map[string]any{
							"type":     "array",
							"items":    map[string]any{"type": "string"},
							"minItems": 1,
							"maxItems": backlog.MaxListLimit,
						},
					},
					"required":             []string{"ids", "reason"},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "messages_cancel",
				Description: "Cancel queue messages by ID (queued|leased|dead -> canceled)",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reason": map[string]any{
							"type":        "string",
							"description": "Required audit reason (maps to X-Hookaido-Audit-Reason semantics)",
						},
						"actor": map[string]any{
							"type": "string",
						},
						"request_id": map[string]any{
							"type": "string",
						},
						"ids": map[string]any{
							"type":     "array",
							"items":    map[string]any{"type": "string"},
							"minItems": 1,
							"maxItems": backlog.MaxListLimit,
						},
					},
					"required":             []string{"ids", "reason"},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "messages_requeue",
				Description: "Requeue queue messages by ID (dead|canceled -> queued)",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reason": map[string]any{
							"type":        "string",
							"description": "Required audit reason (maps to X-Hookaido-Audit-Reason semantics)",
						},
						"actor": map[string]any{
							"type": "string",
						},
						"request_id": map[string]any{
							"type": "string",
						},
						"ids": map[string]any{
							"type":     "array",
							"items":    map[string]any{"type": "string"},
							"minItems": 1,
							"maxItems": backlog.MaxListLimit,
						},
					},
					"required":             []string{"ids", "reason"},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "messages_resume",
				Description: "Resume canceled queue messages by ID (canceled -> queued)",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reason": map[string]any{
							"type":        "string",
							"description": "Required audit reason (maps to X-Hookaido-Audit-Reason semantics)",
						},
						"actor": map[string]any{
							"type": "string",
						},
						"request_id": map[string]any{
							"type": "string",
						},
						"ids": map[string]any{
							"type":     "array",
							"items":    map[string]any{"type": "string"},
							"minItems": 1,
							"maxItems": backlog.MaxListLimit,
						},
					},
					"required":             []string{"ids", "reason"},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "messages_publish",
				Description: "Publish queued messages (route/target or application/endpoint_name resolution; managed selectors are endpoint-scoped in admin-proxy mode; payload as base64)",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reason": map[string]any{
							"type":        "string",
							"description": "Required audit reason (maps to X-Hookaido-Audit-Reason semantics)",
						},
						"actor": map[string]any{
							"type": "string",
						},
						"request_id": map[string]any{
							"type": "string",
						},
						"items": map[string]any{
							"type":     "array",
							"minItems": 1,
							"maxItems": backlog.MaxListLimit,
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"id":            map[string]any{"type": "string"},
									"route":         routePathSchema(),
									"target":        map[string]any{"type": "string"},
									"application":   managementLabelSchema(),
									"endpoint_name": managementLabelSchema(),
									"payload_b64":   map[string]any{"type": "string"},
									"received_at":   map[string]any{"type": "string", "description": "RFC3339 timestamp"},
									"next_run_at":   map[string]any{"type": "string", "description": "RFC3339 timestamp"},
									"headers": map[string]any{
										"type":                 "object",
										"additionalProperties": map[string]any{"type": "string"},
									},
									"trace": map[string]any{
										"type":                 "object",
										"additionalProperties": map[string]any{"type": "string"},
									},
								},
								"required":             []string{"id"},
								"additionalProperties": false,
							},
						},
					},
					"required":             []string{"items", "reason"},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "messages_cancel_by_filter",
				Description: "Cancel queue messages by filter (default states queued|leased|dead)",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reason":        map[string]any{"type": "string", "description": "Required audit reason (maps to X-Hookaido-Audit-Reason semantics)"},
						"actor":         map[string]any{"type": "string"},
						"request_id":    map[string]any{"type": "string"},
						"route":         routePathSchema(),
						"application":   managementLabelSchema(),
						"endpoint_name": managementLabelSchema(),
						"target":        map[string]any{"type": "string"},
						"state":         map[string]any{"type": "string", "enum": []string{"queued", "leased", "dead"}},
						"before":        map[string]any{"type": "string", "description": "RFC3339 timestamp"},
						"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": backlog.MaxListLimit},
						"preview_only": map[string]any{
							"type":        "boolean",
							"description": "When true, return matched count without mutating",
						},
					},
					"required":             []string{"reason"},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "messages_requeue_by_filter",
				Description: "Requeue queue messages by filter (default states dead|canceled)",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reason":        map[string]any{"type": "string", "description": "Required audit reason (maps to X-Hookaido-Audit-Reason semantics)"},
						"actor":         map[string]any{"type": "string"},
						"request_id":    map[string]any{"type": "string"},
						"route":         routePathSchema(),
						"application":   managementLabelSchema(),
						"endpoint_name": managementLabelSchema(),
						"target":        map[string]any{"type": "string"},
						"state":         map[string]any{"type": "string", "enum": []string{"dead", "canceled"}},
						"before":        map[string]any{"type": "string", "description": "RFC3339 timestamp"},
						"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": backlog.MaxListLimit},
						"preview_only": map[string]any{
							"type":        "boolean",
							"description": "When true, return matched count without mutating",
						},
					},
					"required":             []string{"reason"},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "messages_resume_by_filter",
				Description: "Resume queue messages by filter (default state canceled)",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reason":        map[string]any{"type": "string", "description": "Required audit reason (maps to X-Hookaido-Audit-Reason semantics)"},
						"actor":         map[string]any{"type": "string"},
						"request_id":    map[string]any{"type": "string"},
						"route":         routePathSchema(),
						"application":   managementLabelSchema(),
						"endpoint_name": managementLabelSchema(),
						"target":        map[string]any{"type": "string"},
						"state":         map[string]any{"type": "string", "enum": []string{"canceled"}},
						"before":        map[string]any{"type": "string", "description": "RFC3339 timestamp"},
						"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": backlog.MaxListLimit},
						"preview_only": map[string]any{
							"type":        "boolean",
							"description": "When true, return matched count without mutating",
						},
					},
					"required":             []string{"reason"},
					"additionalProperties": false,
				},
			},
		)
	}
	if s.RuntimeControlEnabled {
		tools = append(tools,
			toolDescriptor{
				Name:        "instance_status",
				Description: "Read current hookaido run status from pid/config and optional admin health probe",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"pid_file": map[string]any{
							"type":        "string",
							"description": "Must match configured pid file path",
						},
						"timeout": map[string]any{
							"type":        "string",
							"description": "Health probe timeout when process is running (default 2s)",
						},
					},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "instance_logs_tail",
				Description: "Tail runtime log file configured via observability.runtime_log output file",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"pid_file": map[string]any{
							"type":        "string",
							"description": "Must match configured pid file path",
						},
						"max_lines": map[string]any{
							"type":        "integer",
							"minimum":     1,
							"maximum":     1000,
							"description": "Maximum number of lines (default 200)",
						},
						"max_bytes": map[string]any{
							"type":        "integer",
							"minimum":     1024,
							"maximum":     1048576,
							"description": "Maximum bytes read from file tail (default 65536)",
						},
					},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "instance_start",
				Description: "Start hookaido run process and verify admin health",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"pid_file": map[string]any{
							"type":        "string",
							"description": "Must match configured pid file path",
						},
						"timeout": map[string]any{
							"type":        "string",
							"description": "Max wait for start + health check (default 10s)",
						},
					},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "instance_stop",
				Description: "Stop hookaido run process using pid file",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"pid_file": map[string]any{
							"type":        "string",
							"description": "Must match configured pid file path",
						},
						"timeout": map[string]any{
							"type":        "string",
							"description": "Graceful stop timeout (default 10s)",
						},
						"force": map[string]any{
							"type":        "boolean",
							"description": "Send SIGKILL when graceful timeout expires",
						},
					},
					"additionalProperties": false,
				},
			},
			toolDescriptor{
				Name:        "instance_reload",
				Description: "Signal hookaido run process (SIGHUP) and verify admin health",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"pid_file": map[string]any{
							"type":        "string",
							"description": "Must match configured pid file path",
						},
						"timeout": map[string]any{
							"type":        "string",
							"description": "Health verification timeout (default 5s)",
						},
					},
					"additionalProperties": false,
				},
			},
		)
	}
	filtered := make([]toolDescriptor, 0, len(tools))
	for _, tool := range tools {
		if s.toolAccessError(tool.Name) == nil {
			filtered = append(filtered, tool)
		}
	}
	return filtered
}

func (s *Server) toolAdminHealth(args map[string]any) (any, error) {
	if len(args) > 0 {
		return nil, errors.New("admin_health does not accept arguments")
	}

	configReadable := false
	if p := strings.TrimSpace(s.ConfigPath); p != "" {
		if data, err := os.ReadFile(p); err == nil {
			if _, err := config.Parse(data); err == nil {
				configReadable = true
			}
		}
	}

	dbExists := false
	dbReadable := false
	if p := strings.TrimSpace(s.DBPath); p != "" {
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			dbExists = true
			f, err := os.Open(p)
			if err == nil {
				dbReadable = true
				_ = f.Close()
			}
		}
	}

	compiled := config.Compiled{}
	compiledOK := false
	queueBackend := "unknown"
	adminProbe := map[string]any{
		"checked":     false,
		"url":         "",
		"ok":          false,
		"status_code": 0,
		"error":       "",
		"details":     map[string]any{},
	}
	signalCfg := queue.DefaultBacklogTrendSignalConfig()
	if loaded, res, err := s.loadCompiledConfig(); err == nil && res.OK {
		compiled = loaded
		compiledOK = true
		queueBackend = compiledQueueBackend(compiled)
		signalCfg = queueTrendSignalConfigFromCompiled(compiled.Defaults.TrendSignals)
		url, urlErr := adminEndpointURL(compiled.AdminAPI, "/healthz")
		if urlErr != nil {
			adminProbe["checked"] = true
			adminProbe["error"] = urlErr.Error()
		} else {
			adminProbe["url"] = url + "?details=1"
			adminProbe["checked"] = true
			if err := s.validateAdminProxyEndpointURL(url); err != nil {
				adminProbe["error"] = err.Error()
			} else {
				url, statusCode, details, herr := probeAdminHealthDetails(compiled, 2*time.Second)
				adminProbe["url"] = url
				adminProbe["status_code"] = statusCode
				if herr != nil {
					adminProbe["error"] = herr.Error()
				} else {
					adminProbe["ok"] = true
					if details != nil {
						adminProbe["details"] = details
					}
				}
			}
		}
	}
	if queueBackend == "unknown" {
		queueBackend = "sqlite"
	}

	useAdminProxy := queueBackendUsesAdminProxy(queueBackend)
	localDBRequired := !useAdminProxy
	localOK := configReadable && (!localDBRequired || (dbExists && dbReadable))
	localQueue := map[string]any{
		"backend":                     queueBackend,
		"source":                      "sqlite_local",
		"checked":                     false,
		"ok":                          false,
		"total":                       0,
		"by_state":                    map[string]int{},
		"oldest_queued_received_at":   "",
		"oldest_queued_age_seconds":   0,
		"earliest_queued_next_run_at": "",
		"ready_lag_seconds":           0,
		"top_queued":                  []map[string]any{},
		"trend_signals":               map[string]any{},
		"error":                       "",
	}
	if useAdminProxy {
		localQueue["source"] = "admin_api"
		if checked, _ := adminProbe["checked"].(bool); checked {
			if adminOK, _ := adminProbe["ok"].(bool); adminOK {
				if details, ok := adminProbe["details"].(map[string]any); ok {
					if diagnostics, ok := details["diagnostics"].(map[string]any); ok {
						if queueDiag, ok := diagnostics["queue"].(map[string]any); ok {
							proxyQueue := make(map[string]any)
							for k, v := range queueDiag {
								proxyQueue[k] = v
							}
							proxyQueue["backend"] = queueBackend
							proxyQueue["source"] = "admin_api"
							if _, ok := proxyQueue["checked"]; !ok {
								proxyQueue["checked"] = true
							}
							localQueue = proxyQueue
						} else {
							localQueue["error"] = "admin health details missing diagnostics.queue"
						}
					} else {
						localQueue["error"] = "admin health details missing diagnostics"
					}
				} else {
					localQueue["error"] = "admin health details missing"
				}
			} else {
				if msg, _ := adminProbe["error"].(string); strings.TrimSpace(msg) != "" {
					localQueue["error"] = msg
				} else {
					localQueue["error"] = "admin API probe failed"
				}
			}
		} else {
			localQueue["error"] = "admin API probe unavailable"
		}
	} else if dbExists && dbReadable {
		localQueue["checked"] = true
		store, err := s.openQueueStore()
		if err != nil {
			localQueue["error"] = err.Error()
		} else {
			defer func() { _ = store.Close() }()
			stats, err := store.Stats()
			if err != nil {
				localQueue["error"] = err.Error()
			} else {
				byState := make(map[string]int, len(stats.ByState))
				for state, n := range stats.ByState {
					byState[string(state)] = n
				}
				localQueue["ok"] = true
				localQueue["total"] = stats.Total
				localQueue["by_state"] = byState
				if !stats.OldestQueuedReceivedAt.IsZero() {
					localQueue["oldest_queued_received_at"] = stats.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
					localQueue["oldest_queued_age_seconds"] = int(stats.OldestQueuedAge / time.Second)
				}
				if !stats.EarliestQueuedNextRun.IsZero() {
					localQueue["earliest_queued_next_run_at"] = stats.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
					localQueue["ready_lag_seconds"] = int(stats.ReadyLag / time.Second)
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
					localQueue["top_queued"] = topQueued
				}
				if trendStore, ok := any(store).(queue.BacklogTrendStore); ok {
					now := time.Now().UTC()
					trendResp, trendErr := trendStore.ListBacklogTrend(queue.BacklogTrendListRequest{
						Since: now.Add(-signalCfg.Window),
						Until: now,
						Limit: healthTrendSignalSamples,
					})
					if trendErr != nil {
						localQueue["trend_signals"] = map[string]any{
							"status": "error",
							"error":  trendErr.Error(),
						}
					} else {
						signals := queue.AnalyzeBacklogTrendSignals(trendResp.Items, trendResp.Truncated, queue.BacklogTrendSignalOptions{
							Now:    now,
							Config: signalCfg,
						})
						localQueue["trend_signals"] = signals.Map()
					}
				}
			}
		}
	}
	// Build local tracing config surface.
	tracingSection := map[string]any{
		"enabled":   false,
		"collector": "",
	}
	if compiledOK {
		tracingSection["enabled"] = compiled.Observability.TracingEnabled
		tracingSection["collector"] = compiled.Observability.TracingCollector
	}
	// Propagate runtime tracing diagnostics from admin API probe when available.
	if adminOK, _ := adminProbe["ok"].(bool); adminOK {
		if details, ok := adminProbe["details"].(map[string]any); ok {
			if diagnostics, ok := details["diagnostics"].(map[string]any); ok {
				if tracingDiag, ok := diagnostics["tracing"].(map[string]any); ok {
					for k, v := range tracingDiag {
						tracingSection[k] = v
					}
				}
			}
		}
	}

	out := map[string]any{
		"ok":              localOK,
		"config_path":     s.ConfigPath,
		"db_path":         s.DBPath,
		"queue_backend":   queueBackend,
		"config_compiled": compiledOK,
		"config_readable": configReadable,
		"db_exists":       dbExists,
		"db_readable":     dbReadable,
		"queue":           localQueue,
		"tracing":         tracingSection,
		"admin_api":       adminProbe,
		"mcp": map[string]any{
			"admin_proxy_publish": s.adminProxyPublishRollbackCounters(),
		},
		"time": time.Now().UTC().Format(time.RFC3339Nano),
	}
	if checked, _ := localQueue["checked"].(bool); checked {
		queueOK, _ := localQueue["ok"].(bool)
		out["ok_with_queue"] = localOK && queueOK
	}
	if checked, _ := adminProbe["checked"].(bool); checked {
		adminOK, _ := adminProbe["ok"].(bool)
		out["ok_with_admin"] = localOK && adminOK
	}
	return out, nil
}

func (s *Server) toolManagementModel(args map[string]any) (any, error) {
	if len(args) > 0 {
		return nil, errors.New("management_model does not accept arguments")
	}

	compiled, res, err := s.loadCompiledConfig()
	if err != nil {
		return nil, err
	}

	out := map[string]any{
		"ok":           res.OK,
		"errors":       res.Errors,
		"warnings":     res.Warnings,
		"route_count":  len(compiled.Routes),
		"applications": []map[string]any{},
	}
	if !res.OK {
		return out, nil
	}

	type endpointProjection struct {
		name   string
		record map[string]any
	}
	type appProjection struct {
		name      string
		endpoints []endpointProjection
	}

	apps := make(map[string]*appProjection)
	for _, r := range compiled.Routes {
		app := strings.TrimSpace(r.Application)
		endpointName := strings.TrimSpace(r.EndpointName)
		if app == "" || endpointName == "" {
			continue
		}
		ap := apps[app]
		if ap == nil {
			ap = &appProjection{name: app}
			apps[app] = ap
		}
		mode := "deliver"
		if r.Pull != nil {
			mode = "pull"
		}
		ap.endpoints = append(ap.endpoints, endpointProjection{
			name: endpointName,
			record: map[string]any{
				"name":    endpointName,
				"route":   r.Path,
				"mode":    mode,
				"targets": compiledRouteTargets(r),
				"publish_policy": map[string]any{
					"enabled":         r.Publish,
					"direct_enabled":  r.PublishDirect,
					"managed_enabled": r.PublishManaged,
				},
			},
		})
	}

	appNames := make([]string, 0, len(apps))
	for name := range apps {
		appNames = append(appNames, name)
	}
	sort.Strings(appNames)

	applications := make([]map[string]any, 0, len(appNames))
	endpointCount := 0
	for _, appName := range appNames {
		ap := apps[appName]
		sort.Slice(ap.endpoints, func(i, j int) bool {
			if ap.endpoints[i].name != ap.endpoints[j].name {
				return ap.endpoints[i].name < ap.endpoints[j].name
			}
			ri, _ := ap.endpoints[i].record["route"].(string)
			rj, _ := ap.endpoints[j].record["route"].(string)
			return ri < rj
		})
		endpoints := make([]map[string]any, 0, len(ap.endpoints))
		for _, ep := range ap.endpoints {
			endpoints = append(endpoints, ep.record)
		}
		endpointCount += len(endpoints)
		applications = append(applications, map[string]any{
			"name":       appName,
			"path_count": len(endpoints),
			"endpoints":  endpoints,
		})
	}
	out["application_count"] = len(applications)
	out["path_count"] = endpointCount
	out["applications"] = applications
	return out, nil
}

func (s *Server) toolInstanceStart(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, instanceStartAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	if err := s.validateRuntimeControlSetup(); err != nil {
		return nil, err
	}
	pidFile, err := s.resolvePIDFilePath(args)
	if err != nil {
		return nil, err
	}
	timeout, err := parseDurationArg(args, "timeout", defaultStartStopTimeout)
	if err != nil {
		return nil, err
	}

	compiled, res, err := s.loadCompiledConfig()
	if err != nil {
		return nil, err
	}
	if !res.OK {
		return map[string]any{
			"ok":       false,
			"started":  false,
			"errors":   res.Errors,
			"warnings": res.Warnings,
		}, nil
	}

	if pid, running := readRunningPID(pidFile); running {
		return map[string]any{
			"ok":              true,
			"started":         false,
			"already_running": true,
			"pid":             pid,
			"pid_file":        pidFile,
		}, nil
	}
	_ = os.Remove(pidFile)

	cmdArgs := []string{
		"run",
		"--config", s.ConfigPath,
		"--db", s.DBPath,
		"--pid-file", pidFile,
	}
	if level := strings.TrimSpace(s.RunLogLevel); level != "" {
		cmdArgs = append(cmdArgs, "--log-level", level)
	}
	if strings.TrimSpace(s.RunDotenvPath) != "" {
		cmdArgs = append(cmdArgs, "--dotenv", s.RunDotenvPath)
	}
	if s.RunWatch {
		cmdArgs = append(cmdArgs, "--watch")
	}

	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = devNull.Close() }()

	cmd := exec.Command(s.RunBinaryPath, cmdArgs...)
	cmd.Stdout = devNull
	cmd.Stderr = devNull
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go func() {
		_ = cmd.Wait()
	}()

	pid, err := waitForPIDFile(pidFile, timeout)
	if err != nil {
		_ = signalPID(cmd.Process.Pid, syscall.SIGTERM)
		return map[string]any{
			"ok":          false,
			"started":     false,
			"pid_file":    pidFile,
			"errors":      []string{fmt.Sprintf("start failed: %v", err)},
			"process_pid": cmd.Process.Pid,
		}, nil
	}

	healthURL, healthErr := waitForAdminHealth(compiled, timeout)
	if healthErr != nil {
		_, _, _ = stopPID(pid, 2*time.Second, true)
		_ = removePIDFileIfMatches(pidFile, pid)
		return map[string]any{
			"ok":         false,
			"started":    false,
			"pid":        pid,
			"pid_file":   pidFile,
			"health_url": healthURL,
			"errors":     []string{healthErr.Error()},
			"warnings":   res.Warnings,
		}, nil
	}

	return map[string]any{
		"ok":         true,
		"started":    true,
		"pid":        pid,
		"pid_file":   pidFile,
		"health_url": healthURL,
		"warnings":   res.Warnings,
	}, nil
}

func (s *Server) toolInstanceStatus(args map[string]any) (any, error) {
	if err := s.validateRuntimeControlSetup(); err != nil {
		return nil, err
	}
	pidFile, err := s.resolvePIDFilePath(args)
	if err != nil {
		return nil, err
	}
	timeout, err := parseDurationArg(args, "timeout", 2*time.Second)
	if err != nil {
		return nil, err
	}

	out := map[string]any{
		"pid_file": pidFile,
		"running":  false,
		"pid":      0,
	}

	pid, running := readRunningPID(pidFile)
	if running {
		out["running"] = true
		out["pid"] = pid
	}

	compiled, res, loadErr := s.loadCompiledConfig()
	if loadErr != nil {
		out["ok"] = false
		out["config_ok"] = false
		out["errors"] = []string{loadErr.Error()}
		return out, nil
	}
	out["config_ok"] = res.OK
	out["warnings"] = res.Warnings
	if !res.OK {
		out["ok"] = false
		out["errors"] = res.Errors
		return out, nil
	}

	pullRouteCount := 0
	deliverRouteCount := 0
	managedRouteCount := 0
	for _, r := range compiled.Routes {
		if r.Pull != nil {
			pullRouteCount++
		}
		if len(r.Deliveries) > 0 {
			deliverRouteCount++
		}
		if strings.TrimSpace(r.Application) != "" && strings.TrimSpace(r.EndpointName) != "" {
			managedRouteCount++
		}
	}
	out["summary"] = map[string]any{
		"ingress_listen":                      compiled.Ingress.Listen,
		"pull_listen":                         compiled.PullAPI.Listen,
		"admin_listen":                        compiled.AdminAPI.Listen,
		"queue_backend":                       compiledQueueBackend(compiled),
		"publish_policy_direct_enabled":       compiled.Defaults.PublishPolicy.DirectEnabled,
		"publish_policy_managed_enabled":      compiled.Defaults.PublishPolicy.ManagedEnabled,
		"publish_policy_allow_pull_routes":    compiled.Defaults.PublishPolicy.AllowPullRoutes,
		"publish_policy_allow_deliver_routes": compiled.Defaults.PublishPolicy.AllowDeliverRoutes,
		"publish_policy_require_actor":        compiled.Defaults.PublishPolicy.RequireActor,
		"publish_policy_require_request_id":   compiled.Defaults.PublishPolicy.RequireRequestID,
		"publish_policy_fail_closed":          compiled.Defaults.PublishPolicy.FailClosed,
		"publish_policy_actor_allowlist": append(
			[]string(nil),
			compiled.Defaults.PublishPolicy.ActorAllowlist...,
		),
		"publish_policy_actor_prefixes": append(
			[]string(nil),
			compiled.Defaults.PublishPolicy.ActorPrefixes...,
		),
		"shared_listener":     compiled.SharedListener,
		"route_count":         len(compiled.Routes),
		"pull_route_count":    pullRouteCount,
		"deliver_route_count": deliverRouteCount,
		"managed_route_count": managedRouteCount,
	}

	health := map[string]any{
		"ok":      false,
		"url":     "",
		"error":   "",
		"checked": false,
	}
	if running {
		url, herr := waitForAdminHealth(compiled, timeout)
		health["url"] = url
		health["checked"] = true
		if herr != nil {
			health["error"] = herr.Error()
		} else {
			health["ok"] = true
		}
	}
	out["admin_health"] = health

	healthy, _ := health["ok"].(bool)
	out["ok"] = res.OK && (!running || healthy)
	if !out["ok"].(bool) {
		if herr, _ := health["error"].(string); strings.TrimSpace(herr) != "" {
			out["errors"] = []string{herr}
		} else {
			out["errors"] = []string{}
		}
	} else {
		out["errors"] = []string{}
	}
	return out, nil
}

func (s *Server) toolInstanceLogsTail(args map[string]any) (any, error) {
	if err := s.validateRuntimeControlSetup(); err != nil {
		return nil, err
	}
	pidFile, err := s.resolvePIDFilePath(args)
	if err != nil {
		return nil, err
	}
	maxLines, err := parseIntArg(args, "max_lines", 200, 1, 1000)
	if err != nil {
		return nil, err
	}
	maxBytes, err := parseIntArg(args, "max_bytes", 64*1024, 1024, 1024*1024)
	if err != nil {
		return nil, err
	}

	compiled, res, loadErr := s.loadCompiledConfig()
	if loadErr != nil {
		return map[string]any{
			"ok":       false,
			"pid_file": pidFile,
			"errors":   []string{loadErr.Error()},
		}, nil
	}
	if !res.OK {
		return map[string]any{
			"ok":       false,
			"pid_file": pidFile,
			"errors":   res.Errors,
			"warnings": res.Warnings,
		}, nil
	}
	if compiled.Observability.RuntimeLogDisabled {
		return map[string]any{
			"ok":       false,
			"pid_file": pidFile,
			"errors":   []string{"runtime_log is disabled"},
			"warnings": res.Warnings,
		}, nil
	}
	if compiled.Observability.RuntimeLogOutput != "file" || strings.TrimSpace(compiled.Observability.RuntimeLogPath) == "" {
		return map[string]any{
			"ok":       false,
			"pid_file": pidFile,
			"errors":   []string{"runtime_log output is not configured as file"},
			"warnings": res.Warnings,
		}, nil
	}

	lines, info, err := tailFile(compiled.Observability.RuntimeLogPath, maxBytes, maxLines)
	if err != nil {
		return map[string]any{
			"ok":       false,
			"pid_file": pidFile,
			"path":     compiled.Observability.RuntimeLogPath,
			"errors":   []string{err.Error()},
			"warnings": res.Warnings,
		}, nil
	}

	pid, running := readRunningPID(pidFile)
	return map[string]any{
		"ok":         true,
		"pid_file":   pidFile,
		"pid":        pid,
		"running":    running,
		"path":       compiled.Observability.RuntimeLogPath,
		"max_lines":  maxLines,
		"max_bytes":  maxBytes,
		"line_count": len(lines),
		"truncated":  info.Truncated,
		"total_size": info.TotalSize,
		"lines":      lines,
		"errors":     []string{},
		"warnings":   res.Warnings,
	}, nil
}

func (s *Server) toolInstanceStop(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, instanceStopAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	if err := s.validateRuntimeControlSetup(); err != nil {
		return nil, err
	}
	pidFile, err := s.resolvePIDFilePath(args)
	if err != nil {
		return nil, err
	}
	timeout, err := parseDurationArg(args, "timeout", defaultStartStopTimeout)
	if err != nil {
		return nil, err
	}
	force, err := parseBool(args, "force")
	if err != nil {
		return nil, err
	}

	pid, err := readPIDFileValue(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{
				"ok":              true,
				"stopped":         false,
				"already_stopped": true,
				"pid_file":        pidFile,
			}, nil
		}
		return nil, err
	}

	if !isPIDRunning(pid) {
		_ = removePIDFileIfMatches(pidFile, pid)
		return map[string]any{
			"ok":              true,
			"stopped":         false,
			"already_stopped": true,
			"pid":             pid,
			"pid_file":        pidFile,
		}, nil
	}

	stopped, forced, err := stopPID(pid, timeout, force)
	if err != nil {
		return nil, err
	}
	if stopped {
		_ = removePIDFileIfMatches(pidFile, pid)
	}

	out := map[string]any{
		"ok":       stopped,
		"stopped":  stopped,
		"forced":   forced,
		"pid":      pid,
		"pid_file": pidFile,
		"timeout":  timeout.String(),
		"errors":   []string{},
	}
	if !stopped {
		out["errors"] = []string{fmt.Sprintf("process %d did not stop within %s", pid, timeout)}
	}
	return out, nil
}

func (s *Server) toolInstanceReload(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, instanceReloadAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	if err := s.validateRuntimeControlSetup(); err != nil {
		return nil, err
	}
	pidFile, err := s.resolvePIDFilePath(args)
	if err != nil {
		return nil, err
	}
	timeout, err := parseDurationArg(args, "timeout", defaultReloadCheckTimeout)
	if err != nil {
		return nil, err
	}

	compiled, res, err := s.loadCompiledConfig()
	if err != nil {
		return nil, err
	}
	if !res.OK {
		return map[string]any{
			"ok":       false,
			"reloaded": false,
			"signaled": false,
			"errors":   res.Errors,
			"warnings": res.Warnings,
		}, nil
	}

	pid, err := readPIDFileValue(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{
				"ok":       false,
				"reloaded": false,
				"signaled": false,
				"pid_file": pidFile,
				"errors":   []string{"pid file does not exist"},
				"warnings": res.Warnings,
			}, nil
		}
		return nil, err
	}
	if !isPIDRunning(pid) {
		return map[string]any{
			"ok":       false,
			"reloaded": false,
			"signaled": false,
			"pid":      pid,
			"pid_file": pidFile,
			"errors":   []string{fmt.Sprintf("process %d is not running", pid)},
			"warnings": res.Warnings,
		}, nil
	}

	if err := signalPID(pid, syscall.SIGHUP); err != nil {
		return nil, err
	}

	healthURL, healthErr := waitForAdminHealth(compiled, timeout)
	out := map[string]any{
		"ok":         healthErr == nil,
		"reloaded":   healthErr == nil,
		"signaled":   true,
		"pid":        pid,
		"pid_file":   pidFile,
		"timeout":    timeout.String(),
		"health_url": healthURL,
		"errors":     []string{},
		"warnings":   res.Warnings,
	}
	if healthErr != nil {
		out["errors"] = []string{healthErr.Error()}
	}
	return out, nil
}

func (s *Server) resolveConfigPath(args map[string]any) (string, error) {
	p := strings.TrimSpace(s.ConfigPath)
	if raw, ok := args["path"]; ok {
		argPath, ok := raw.(string)
		if !ok {
			return "", errors.New("path must be a string")
		}
		argPath = strings.TrimSpace(argPath)
		if argPath != "" {
			if p == "" {
				return "", errors.New("config path is not configured")
			}
			if argPath != p {
				return "", fmt.Errorf("path %q is not allowed", argPath)
			}
			p = argPath
		}
	}
	if p == "" {
		return "", errors.New("config path is not configured")
	}
	return p, nil
}

// closableStore combines queue.Store with a Close method for per-request
// store lifecycle management.
type closableStore interface {
	queue.Store
	Close() error
}

func (s *Server) openQueueStore() (closableStore, error) {
	p := strings.TrimSpace(s.DBPath)
	if p == "" {
		return nil, errors.New("db path is not configured")
	}
	info, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("db file %q does not exist", p)
		}
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("db path %q is a directory", p)
	}
	b, ok := hookaido.LookupQueueBackend("sqlite")
	if !ok {
		return nil, errors.New("sqlite queue backend is not available (not compiled in)")
	}
	raw, _, err := b.OpenStore(hookaido.QueueBackendConfig{DSN: p})
	if err != nil {
		return nil, err
	}
	cs, ok := raw.(closableStore)
	if !ok {
		return nil, errors.New("queue backend does not support close")
	}
	return cs, nil
}

type idMutationPolicyContext struct {
	compiled          config.Compiled
	compiledAvailable bool
	useAdminProxy     bool
}

func (s *Server) resolveIDMutationPolicyContext() (idMutationPolicyContext, error) {
	var out idMutationPolicyContext
	if strings.TrimSpace(s.ConfigPath) == "" {
		return out, nil
	}

	compiled, res, err := s.loadCompiledConfig()
	if err == nil && res.OK {
		out.compiled = compiled
		out.compiledAvailable = true
		out.useAdminProxy = queueBackendUsesAdminProxy(compiledQueueBackend(compiled))
		return out, nil
	}

	if enabled, known := s.scopedManagedFailClosedFromRawConfig(); known && enabled {
		if err != nil {
			return out, fmt.Errorf("cannot evaluate scoped managed id mutation policy: %w", err)
		}
		return out, fmt.Errorf("cannot evaluate scoped managed id mutation policy: config compile failed: %s", strings.Join(res.Errors, "; "))
	}
	return out, nil
}

func (s *Server) queueToolsUseAdminProxy() (config.Compiled, bool) {
	if strings.TrimSpace(s.ConfigPath) == "" {
		return config.Compiled{}, false
	}
	compiled, res, err := s.loadCompiledConfig()
	if err != nil || !res.OK {
		return config.Compiled{}, false
	}
	return compiled, queueBackendUsesAdminProxy(compiledQueueBackend(compiled))
}

func (s *Server) validateRouteScopedManagedAuditPolicyForFilterMutation(audit mutationAuditArgs, route string) error {
	route = strings.TrimSpace(route)
	if strings.TrimSpace(s.ConfigPath) == "" {
		return nil
	}

	compiled, res, err := s.loadCompiledConfig()
	if err == nil && res.OK {
		return validateScopedManagedAuditPolicyForFilterMutation(audit, route, "", "", compiled)
	}

	if enabled, known := s.scopedManagedFailClosedFromRawConfig(); known && enabled {
		if err != nil {
			return fmt.Errorf("cannot evaluate scoped managed filter mutation policy: %w", err)
		}
		return fmt.Errorf("cannot evaluate scoped managed filter mutation policy: config compile failed: %s", strings.Join(res.Errors, "; "))
	}
	return nil
}

func (s *Server) scopedManagedFailClosedFromRawConfig() (bool, bool) {
	p := strings.TrimSpace(s.ConfigPath)
	if p == "" {
		return false, false
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return false, false
	}
	cfg, err := config.Parse(data)
	if err != nil || cfg == nil || cfg.Defaults == nil || cfg.Defaults.PublishPolicy == nil {
		return false, false
	}
	policy := cfg.Defaults.PublishPolicy
	if !policy.FailClosedSet {
		return false, true
	}
	val, ok := parseConfigBoolToken(policy.FailClosed)
	if !ok {
		return false, false
	}
	return val, true
}

func parseConfigBoolToken(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "on", "yes":
		return true, true
	case "0", "false", "off", "no":
		return false, true
	default:
		return false, false
	}
}

func (s *Server) callAdminJSON(
	compiled config.Compiled,
	method string,
	endpointPath string,
	query url.Values,
	body any,
	headers map[string]string,
	timeout time.Duration,
) (map[string]any, error) {
	if timeout <= 0 {
		timeout = defaultAdminProxyTimeout
	}

	endpointURL, err := adminEndpointURL(compiled.AdminAPI, endpointPath)
	if err != nil {
		return nil, err
	}
	if err := s.validateAdminProxyEndpointURL(endpointURL); err != nil {
		return nil, err
	}
	if len(query) > 0 {
		u, err := url.Parse(endpointURL)
		if err != nil {
			return nil, err
		}
		u.RawQuery = query.Encode()
		endpointURL = u.String()
	}

	token, err := loadAdminHealthToken(compiled.AdminAPI.AuthTokens)
	if err != nil {
		return nil, err
	}

	var bodyBytes []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyBytes = b
	}

	client := adminProxyClient(compiled.AdminAPI, timeout)
	maxAttempts := 1
	if strings.EqualFold(method, http.MethodGet) {
		maxAttempts = adminProxyRetryMaxGET
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var reqBody io.Reader
		if len(bodyBytes) > 0 {
			reqBody = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequest(method, endpointURL, reqBody)
		if err != nil {
			return nil, err
		}
		if reqBody != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		for k, v := range headers {
			k = strings.TrimSpace(k)
			v = strings.TrimSpace(v)
			if k == "" || v == "" {
				continue
			}
			req.Header.Set(k, v)
		}

		resp, err := client.Do(req)
		if err != nil {
			if shouldRetryAdminProxyCall(attempt, maxAttempts, 0, err) {
				time.Sleep(adminProxyRetryDelay(attempt))
				continue
			}
			return nil, fmt.Errorf("admin %s %s request failed: %w", method, endpointPath, err)
		}

		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		trimmed := bytes.TrimSpace(payload)
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			if shouldRetryAdminProxyCall(attempt, maxAttempts, resp.StatusCode, nil) {
				time.Sleep(adminProxyRetryDelay(attempt))
				continue
			}
			return nil, &adminProxyHTTPError{
				method:       method,
				endpointPath: endpointPath,
				statusCode:   resp.StatusCode,
				payload:      trimmed,
			}
		}

		if len(trimmed) == 0 {
			return map[string]any{}, nil
		}

		var out any
		if err := json.Unmarshal(trimmed, &out); err != nil {
			return nil, fmt.Errorf("decode admin response: %w", err)
		}
		obj, ok := out.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("admin %s %s returned non-object JSON", method, endpointPath)
		}
		return obj, nil
	}

	return nil, fmt.Errorf("admin %s %s request failed after %d attempts", method, endpointPath, maxAttempts)
}

type adminProxyHTTPError struct {
	method       string
	endpointPath string
	statusCode   int
	payload      []byte
}

func (e *adminProxyHTTPError) Error() string {
	if e == nil {
		return ""
	}
	return adminProxyStatusError(e.method, e.endpointPath, e.statusCode, e.payload).Error()
}

func shouldRetryAdminProxyCall(attempt, maxAttempts, statusCode int, err error) bool {
	if attempt >= maxAttempts {
		return false
	}
	if err != nil {
		return true
	}
	switch statusCode {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func adminProxyRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	delay := time.Duration(attempt) * adminProxyRetryBackoff
	if delay > 2*time.Second {
		return 2 * time.Second
	}
	return delay
}

func adminProxyStatusError(method, endpointPath string, statusCode int, payload []byte) error {
	verb := strings.ToUpper(strings.TrimSpace(method))
	if verb == "" {
		verb = "REQUEST"
	}
	base := fmt.Sprintf("admin %s %s returned status %d", verb, endpointPath, statusCode)
	switch statusCode {
	case http.StatusBadRequest:
		base = fmt.Sprintf("admin rejected %s %s as invalid (status 400)", verb, endpointPath)
	case http.StatusUnauthorized:
		base = fmt.Sprintf("admin authentication failed for %s %s (status 401)", verb, endpointPath)
	case http.StatusForbidden:
		base = fmt.Sprintf("admin authorization denied for %s %s (status 403)", verb, endpointPath)
	case http.StatusNotFound:
		base = fmt.Sprintf("admin endpoint not found for %s %s (status 404; check admin_api.prefix/listen and running instance)", verb, endpointPath)
	case http.StatusConflict:
		base = fmt.Sprintf("admin reported conflict for %s %s (status 409)", verb, endpointPath)
	case http.StatusTooManyRequests:
		base = fmt.Sprintf("admin rate-limited %s %s (status 429)", verb, endpointPath)
	case http.StatusServiceUnavailable:
		base = fmt.Sprintf("admin is unavailable for %s %s (status 503)", verb, endpointPath)
	}

	info := extractAdminProxyErrorInfo(payload)
	info = normalizeAdminProxyErrorInfo(info)
	if info.Code != "" || info.Detail != "" || info.ItemIndex != nil {
		parts := make([]string, 0, 3)
		if info.Code != "" {
			parts = append(parts, "code="+info.Code)
		}
		if info.ItemIndex != nil {
			parts = append(parts, fmt.Sprintf("item_index=%d", *info.ItemIndex))
		}
		if info.Detail != "" {
			parts = append(parts, "detail="+info.Detail)
		}
		return errors.New(base + ": " + strings.Join(parts, ", "))
	}
	return errors.New(base)
}

type adminProxyErrorInfo struct {
	Code      string
	Detail    string
	ItemIndex *int
}

func normalizeAdminProxyErrorInfo(in adminProxyErrorInfo) adminProxyErrorInfo {
	out := in
	out.Code = strings.TrimSpace(out.Code)
	out.Detail = strings.TrimSpace(out.Detail)
	if out.Code != "" && out.Detail == "" {
		out.Detail = adminProxyErrorDetailFallback(out.Code)
	}
	return out
}

func adminProxyErrorDetailFallback(code string) string {
	switch strings.TrimSpace(code) {
	case "invalid_body":
		return "request body is invalid"
	case "invalid_query":
		return "request query is invalid"
	case "audit_reason_required":
		return "X-Hookaido-Audit-Reason is required"
	case "audit_actor_required":
		return "X-Hookaido-Audit-Actor is required by defaults.publish_policy.require_actor"
	case "audit_actor_not_allowed":
		return "X-Hookaido-Audit-Actor is not allowed for endpoint-scoped managed mutation"
	case "audit_request_id_required":
		return "X-Request-ID is required by defaults.publish_policy.require_request_id"
	case "store_unavailable":
		return "queue store is unavailable"
	case "queue_full":
		return "queue is full"
	case "duplicate_id":
		return "item.id already exists"
	case "route_not_found":
		return "route has no publishable targets or is not configured"
	case "target_unresolvable":
		return "item.target is required or not allowed for route"
	case "invalid_received_at":
		return "item.received_at must be RFC3339"
	case "invalid_next_run_at":
		return "item.next_run_at must be RFC3339"
	case "invalid_payload_b64":
		return "item.payload_b64 must be valid base64"
	case "invalid_header":
		return "item.headers contains invalid HTTP header name/value"
	case "payload_too_large":
		return "decoded payload exceeds max_body for route"
	case "headers_too_large":
		return "headers exceed max_headers for route"
	case "managed_selector_required":
		return "route is managed by application/endpoint; use application+endpoint_name"
	case "route_resolver_missing":
		return "route target resolution is not configured for global publish path"
	case "scoped_publish_required":
		return "managed publish must use endpoint-scoped publish path"
	case "managed_resolver_missing":
		return "managed endpoint resolution is not configured"
	case "managed_endpoint_not_found":
		return "managed endpoint not found"
	case "managed_target_mismatch":
		return "managed endpoint target/ownership mapping is out of sync with route policy resolution"
	case "managed_endpoint_no_targets":
		return "managed endpoint has no publishable targets"
	case "selector_scope_mismatch":
		return "route/managed selector is invalid or mismatched"
	case "selector_scope_forbidden":
		return "selector hints are not allowed on scoped path"
	case "global_publish_disabled":
		return "global direct publish path is disabled by defaults.publish_policy.direct"
	case "scoped_publish_disabled":
		return "endpoint-scoped publish path is disabled by defaults.publish_policy.managed"
	case "route_publish_disabled":
		return "publish is disabled for route by route-level publish policy"
	case "pull_route_publish_disabled":
		return "publish to pull routes is disabled by defaults.publish_policy.allow_pull_routes"
	case "deliver_route_publish_disabled":
		return "publish to deliver routes is disabled by defaults.publish_policy.allow_deliver_routes"
	case "management_unavailable":
		return "management endpoint mutation failed"
	case "management_route_not_found":
		return "management route not found"
	case "management_endpoint_not_found":
		return "management endpoint not found"
	case "management_route_already_mapped":
		return "management endpoint target route is already mapped to another endpoint"
	case "management_route_publish_disabled":
		return "management endpoint target route has managed publish disabled"
	case "management_route_target_mismatch":
		return "management endpoint target route has mismatched publish target profile"
	case "management_route_backlog_active":
		return "management endpoint current route has active queued/leased backlog"
	case "management_conflict":
		return "management endpoint mutation conflict"
	case "not_found":
		return "resource not found"
	case "backlog_unavailable":
		return "backlog trend store is not supported by queue backend"
	case "management_model_unavailable":
		return "management model is not configured"
	default:
		return ""
	}
}

func extractAdminProxyErrorInfo(payload []byte) adminProxyErrorInfo {
	out := adminProxyErrorInfo{}
	if len(payload) == 0 {
		return out
	}
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return out
	}

	var obj map[string]any
	if err := json.Unmarshal(trimmed, &obj); err == nil {
		if code, ok := obj["code"].(string); ok {
			out.Code = strings.TrimSpace(code)
		}
		if raw, ok := obj["item_index"]; ok {
			if idx, ok := intFromAnyForError(raw); ok && idx >= 0 {
				out.ItemIndex = &idx
			}
		}
		for _, key := range []string{"error", "message", "detail"} {
			if msg := extractErrorFieldString(obj[key]); msg != "" {
				out.Detail = truncateErrorBody(msg)
				return out
			}
		}
		if raw, ok := obj["errors"]; ok {
			switch v := raw.(type) {
			case []any:
				for _, item := range v {
					if msg := extractErrorFieldString(item); msg != "" {
						out.Detail = truncateErrorBody(msg)
						return out
					}
				}
			case []string:
				for _, msg := range v {
					msg = strings.TrimSpace(msg)
					if msg != "" {
						out.Detail = truncateErrorBody(msg)
						return out
					}
				}
			}
		}
		// If the payload is a structured object with a code but no textual detail,
		// keep detail empty so the code-based fallback can be applied upstream.
		if out.Code != "" {
			return out
		}
	}

	var text string
	if err := json.Unmarshal(trimmed, &text); err == nil {
		text = strings.TrimSpace(text)
		if text != "" {
			out.Detail = truncateErrorBody(text)
			return out
		}
	}
	out.Detail = truncateErrorBody(string(trimmed))
	return out
}

func intFromAnyForError(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int8:
		return int(n), true
	case int16:
		return int(n), true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case uint:
		return int(n), true
	case uint8:
		return int(n), true
	case uint16:
		return int(n), true
	case uint32:
		return int(n), true
	case uint64:
		if n > uint64(^uint(0)>>1) {
			return 0, false
		}
		return int(n), true
	case float64:
		if n == float64(int(n)) {
			return int(n), true
		}
	case string:
		n = strings.TrimSpace(n)
		if n == "" {
			return 0, false
		}
		i, err := strconv.Atoi(n)
		if err == nil {
			return i, true
		}
	}
	return 0, false
}

func extractErrorFieldString(v any) string {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case map[string]any:
		for _, key := range []string{"message", "error", "detail"} {
			if msg := extractErrorFieldString(x[key]); msg != "" {
				return msg
			}
		}
	}
	return ""
}

func (s *Server) validateAdminProxyEndpointURL(endpointURL string) error {
	if s == nil || len(s.AdminProxyEndpointAllowlist) == 0 {
		return nil
	}
	target, err := url.Parse(endpointURL)
	if err != nil {
		return fmt.Errorf("invalid admin endpoint URL %q: %w", endpointURL, err)
	}
	for _, entry := range s.AdminProxyEndpointAllowlist {
		ok, err := adminProxyAllowlistEntryMatches(entry, target)
		if err != nil {
			return fmt.Errorf("invalid admin endpoint allowlist entry %q: %w", entry, err)
		}
		if ok {
			return nil
		}
	}
	return fmt.Errorf("admin endpoint %q is not allowed by admin endpoint allowlist", endpointURL)
}

func adminProxyAllowlistEntryMatches(entry string, target *url.URL) (bool, error) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return false, errors.New("entry is empty")
	}
	if target == nil {
		return false, errors.New("target URL is nil")
	}

	targetHost := strings.TrimSpace(target.Host)
	targetScheme := strings.ToLower(strings.TrimSpace(target.Scheme))
	targetPath := cleanURLPath(target.Path)
	if targetHost == "" || targetScheme == "" {
		return false, errors.New("target URL missing scheme or host")
	}

	if strings.Contains(entry, "://") {
		u, err := url.Parse(entry)
		if err != nil {
			return false, err
		}
		scheme := strings.ToLower(strings.TrimSpace(u.Scheme))
		if scheme != "http" && scheme != "https" {
			return false, fmt.Errorf("unsupported scheme %q", u.Scheme)
		}
		host := strings.TrimSpace(u.Host)
		if host == "" {
			return false, errors.New("missing host")
		}
		if !strings.EqualFold(scheme, targetScheme) {
			return false, nil
		}
		if !strings.EqualFold(host, targetHost) {
			return false, nil
		}
		allowedPath := cleanURLPath(u.Path)
		if allowedPath == "/" {
			return true, nil
		}
		return pathHasSegmentPrefix(targetPath, allowedPath), nil
	}

	if strings.Contains(entry, "/") || strings.Contains(entry, "?") || strings.Contains(entry, "#") {
		return false, errors.New("host:port entries must not contain path/query/fragment")
	}
	return strings.EqualFold(entry, targetHost), nil
}

func cleanURLPath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = path.Clean(p)
	if p == "." {
		return "/"
	}
	return p
}

func pathHasSegmentPrefix(pathValue, prefix string) bool {
	pathValue = cleanURLPath(pathValue)
	prefix = cleanURLPath(prefix)
	if prefix == "/" {
		return true
	}
	if pathValue == prefix {
		return true
	}
	return strings.HasPrefix(pathValue, prefix+"/")
}

func adminEndpointURL(api config.APIConfig, endpointPath string) (string, error) {
	listen := strings.TrimSpace(api.Listen)
	if listen == "" {
		return "", errors.New("admin_api.listen is empty")
	}

	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("admin_api.listen %q must be host:port: %w", listen, err)
	}
	switch strings.TrimSpace(host) {
	case "", "0.0.0.0", "::", "[::]":
		host = "127.0.0.1"
	}
	hostPort := net.JoinHostPort(host, port)

	scheme := "http"
	if api.TLS.Enabled {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s%s", scheme, hostPort, joinURLPath(api.Prefix, endpointPath)), nil
}

func adminProxyClient(api config.APIConfig, timeout time.Duration) *http.Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout: timeout,
		}).DialContext,
	}
	if api.TLS.Enabled {
		tr.TLSClientConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, // Local operator channel, trust is managed by deployment.
		}
	}
	return &http.Client{
		Transport: tr,
		Timeout:   timeout,
	}
}

func truncateErrorBody(s string) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
	if len(s) <= 256 {
		return s
	}
	return s[:256] + "..."
}

func (s *Server) validateRuntimeControlSetup() error {
	if strings.TrimSpace(s.ConfigPath) == "" {
		return errors.New("config path is not configured")
	}
	if strings.TrimSpace(s.DBPath) == "" {
		return errors.New("db path is not configured")
	}
	if strings.TrimSpace(s.PIDFilePath) == "" {
		return errors.New("pid file path is not configured")
	}
	if strings.TrimSpace(s.RunBinaryPath) == "" {
		return errors.New("run binary path is not configured")
	}
	return nil
}

func (s *Server) resolvePIDFilePath(args map[string]any) (string, error) {
	p := strings.TrimSpace(s.PIDFilePath)
	if p == "" {
		return "", errors.New("pid file path is not configured")
	}
	if raw, ok := args["pid_file"]; ok {
		argPath, ok := raw.(string)
		if !ok {
			return "", errors.New("pid_file must be a string")
		}
		argPath = strings.TrimSpace(argPath)
		if argPath != "" {
			if argPath != p {
				return "", fmt.Errorf("pid_file %q is not allowed", argPath)
			}
			p = argPath
		}
	}
	return p, nil
}

func (s *Server) loadCompiledConfig() (config.Compiled, config.ValidationResult, error) {
	data, err := os.ReadFile(strings.TrimSpace(s.ConfigPath))
	if err != nil {
		return config.Compiled{}, config.ValidationResult{}, err
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return config.Compiled{}, config.ValidationResult{
			Errors: []string{err.Error()},
		}, nil
	}
	compiled, res := config.Compile(cfg)
	return compiled, res, nil
}

func (s *Server) resolveTrendSignalConfig() queue.BacklogTrendSignalConfig {
	compiled, res, err := s.loadCompiledConfig()
	if err != nil || !res.OK {
		return queue.DefaultBacklogTrendSignalConfig()
	}
	return queueTrendSignalConfigFromCompiled(compiled.Defaults.TrendSignals)
}

func queueBackendUsesAdminProxy(backend string) bool {
	switch strings.ToLower(strings.TrimSpace(backend)) {
	case "", "sqlite", "mixed":
		return false
	default:
		return true
	}
}

func compiledQueueBackend(compiled config.Compiled) string {
	backend := ""
	for _, rt := range compiled.Routes {
		b := strings.ToLower(strings.TrimSpace(rt.QueueBackend))
		if b == "" {
			b = "sqlite"
		}
		if backend == "" {
			backend = b
			continue
		}
		if b != backend {
			return "mixed"
		}
	}
	if backend == "" {
		return "sqlite"
	}
	return backend
}

func queueTrendSignalConfigFromCompiled(in config.TrendSignalsConfig) queue.BacklogTrendSignalConfig {
	return queue.BacklogTrendSignalConfig{
		Window:                         in.Window,
		ExpectedCaptureInterval:        in.ExpectedCaptureInterval,
		StaleGraceFactor:               in.StaleGraceFactor,
		SustainedGrowthConsecutive:     in.SustainedGrowthConsecutive,
		SustainedGrowthMinSamples:      in.SustainedGrowthMinSamples,
		SustainedGrowthMinDelta:        in.SustainedGrowthMinDelta,
		RecentSurgeMinTotal:            in.RecentSurgeMinTotal,
		RecentSurgeMinDelta:            in.RecentSurgeMinDelta,
		RecentSurgePercent:             in.RecentSurgePercent,
		DeadShareHighMinTotal:          in.DeadShareHighMinTotal,
		DeadShareHighPercent:           in.DeadShareHighPercent,
		QueuedPressureMinTotal:         in.QueuedPressureMinTotal,
		QueuedPressurePercent:          in.QueuedPressurePercent,
		QueuedPressureLeasedMultiplier: in.QueuedPressureLeasedMultiplier,
	}
}

func parseReloadTimeout(args map[string]any, defaultTimeout time.Duration) (time.Duration, error) {
	return parseDurationArg(args, "reload_timeout", defaultTimeout)
}

func parseDurationArg(args map[string]any, key string, defaultValue time.Duration) (time.Duration, error) {
	raw, ok := args[key]
	if !ok {
		return defaultValue, nil
	}
	s, ok := raw.(string)
	if !ok {
		return 0, fmt.Errorf("%s must be a duration string", key)
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return defaultValue, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", key, s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be > 0", key)
	}
	return d, nil
}

func parseIntArg(args map[string]any, key string, defaultValue int, minValue int, maxValue int) (int, error) {
	raw, ok := args[key]
	if !ok {
		return defaultValue, nil
	}
	switch v := raw.(type) {
	case float64:
		n := int(v)
		if float64(n) != v {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		if n < minValue || n > maxValue {
			return 0, fmt.Errorf("%s must be between %d and %d", key, minValue, maxValue)
		}
		return n, nil
	case int:
		if v < minValue || v > maxValue {
			return 0, fmt.Errorf("%s must be between %d and %d", key, minValue, maxValue)
		}
		return v, nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		if n < minValue || n > maxValue {
			return 0, fmt.Errorf("%s must be between %d and %d", key, minValue, maxValue)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("%s must be an integer", key)
	}
}

func waitForAdminHealth(compiled config.Compiled, timeout time.Duration) (string, error) {
	url, err := adminHealthURL(compiled.AdminAPI)
	if err != nil {
		return "", err
	}

	token, err := loadAdminHealthToken(compiled.AdminAPI.AuthTokens)
	if err != nil {
		return url, err
	}

	perRequestTimeout := minDuration(timeout, time.Second)
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout: perRequestTimeout,
		}).DialContext,
	}
	if compiled.AdminAPI.TLS.Enabled {
		tr.TLSClientConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, // Local liveness check; cert trust can be self-managed.
		}
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   perRequestTimeout,
	}

	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return url, err
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}

		resp, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return url, nil
			}
			lastErr = fmt.Errorf("health check returned status %d", resp.StatusCode)
		} else {
			lastErr = err
		}

		if time.Now().After(deadline) {
			break
		}
		sleep := minDuration(200*time.Millisecond, time.Until(deadline))
		if sleep <= 0 {
			break
		}
		time.Sleep(sleep)
	}
	if lastErr == nil {
		lastErr = errors.New("timed out waiting for admin health")
	}
	return url, fmt.Errorf("reload health check failed: %w", lastErr)
}

func probeAdminHealthDetails(compiled config.Compiled, timeout time.Duration) (string, int, map[string]any, error) {
	url, err := adminHealthURL(compiled.AdminAPI)
	if err != nil {
		return "", 0, nil, err
	}

	token, err := loadAdminHealthToken(compiled.AdminAPI.AuthTokens)
	if err != nil {
		return url, 0, nil, err
	}

	detailsURL := url + "?details=1"
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout: timeout,
		}).DialContext,
	}
	if compiled.AdminAPI.TLS.Enabled {
		tr.TLSClientConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, // Local diagnostics probe.
		}
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   timeout,
	}
	req, err := http.NewRequest(http.MethodGet, detailsURL, nil)
	if err != nil {
		return detailsURL, 0, nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return detailsURL, 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return detailsURL, resp.StatusCode, nil, fmt.Errorf("health check returned status %d", resp.StatusCode)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return detailsURL, resp.StatusCode, map[string]any{}, nil
	}

	var details map[string]any
	if err := json.Unmarshal(body, &details); err != nil {
		return detailsURL, resp.StatusCode, nil, fmt.Errorf("decode admin health details: %w", err)
	}
	return detailsURL, resp.StatusCode, details, nil
}

func adminHealthURL(api config.APIConfig) (string, error) {
	listen := strings.TrimSpace(api.Listen)
	if listen == "" {
		return "", errors.New("admin_api.listen is empty")
	}

	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("admin_api.listen %q must be host:port: %w", listen, err)
	}
	switch strings.TrimSpace(host) {
	case "", "0.0.0.0", "::", "[::]":
		host = "127.0.0.1"
	}
	hostPort := net.JoinHostPort(host, port)

	scheme := "http"
	if api.TLS.Enabled {
		scheme = "https"
	}
	healthPath := joinURLPath(api.Prefix, "/healthz")
	return fmt.Sprintf("%s://%s%s", scheme, hostPort, healthPath), nil
}

func joinURLPath(prefix, suffix string) string {
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = path.Clean(p)
	if p == "." {
		p = "/"
	}

	s := "/" + strings.TrimPrefix(strings.TrimSpace(suffix), "/")
	if p == "/" {
		return path.Clean(s)
	}
	return path.Clean(p + s)
}

func loadAdminHealthToken(refs []string) (string, error) {
	if len(refs) == 0 {
		return "", nil
	}
	token := ""
	for i, ref := range refs {
		b, err := secrets.LoadRef(ref)
		if err != nil {
			return "", fmt.Errorf("admin_api auth token[%d] %q: %w", i, ref, err)
		}
		if token == "" {
			token = string(b)
		}
	}
	return token, nil
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 {
		return b
	}
	if b <= 0 {
		return a
	}
	if a < b {
		return a
	}
	return b
}

func readPIDFileValue(pidFile string) (int, error) {
	data, err := os.ReadFile(strings.TrimSpace(pidFile))
	if err != nil {
		return 0, err
	}
	raw := strings.TrimSpace(string(data))
	if raw == "" {
		return 0, fmt.Errorf("pid file %q is empty", pidFile)
	}
	pid, err := strconv.Atoi(raw)
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("pid file %q contains invalid pid %q", pidFile, raw)
	}
	return pid, nil
}

func readRunningPID(pidFile string) (int, bool) {
	pid, err := readPIDFileValue(pidFile)
	if err != nil {
		return 0, false
	}
	return pid, isPIDRunning(pid)
}

func isPIDRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	if isZombiePID(pid) {
		return false
	}
	return processExists(pid)
}

func isZombiePID(pid int) bool {
	statPath := fmt.Sprintf("/proc/%d/stat", pid)
	data, err := os.ReadFile(statPath)
	if err != nil {
		return false
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return false
	}
	return fields[2] == "Z"
}

func signalPID(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return fmt.Errorf("invalid pid %d", pid)
	}
	if err := sendSignal(pid, sig); err != nil {
		return fmt.Errorf("signal %s pid %d: %w", sig, pid, err)
	}
	return nil
}

func stopPID(pid int, timeout time.Duration, force bool) (stopped bool, forced bool, err error) {
	if !isPIDRunning(pid) {
		return true, false, nil
	}
	if err := signalPID(pid, syscall.SIGTERM); err != nil {
		return false, false, err
	}
	if waitForPIDExit(pid, timeout) {
		return true, false, nil
	}
	if !force {
		return false, false, nil
	}
	if err := signalPID(pid, syscall.SIGKILL); err != nil {
		return false, false, err
	}
	return waitForPIDExit(pid, 2*time.Second), true, nil
}

func waitForPIDExit(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if !isPIDRunning(pid) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForPIDFile(pidFile string, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		pid, err := readPIDFileValue(pidFile)
		if err == nil {
			return pid, nil
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("timeout waiting for pid file %q", pidFile)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func removePIDFileIfMatches(pidFile string, pid int) error {
	current, err := readPIDFileValue(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if current != pid {
		return nil
	}
	if err := os.Remove(pidFile); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

type tailInfo struct {
	TotalSize int64
	Truncated bool
}

func tailFile(path string, maxBytes int, maxLines int) ([]string, tailInfo, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, tailInfo{}, errors.New("empty log file path")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, tailInfo{}, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, tailInfo{}, err
	}
	if info.IsDir() {
		return nil, tailInfo{}, fmt.Errorf("log path %q is a directory", path)
	}

	size := info.Size()
	start := int64(0)
	truncated := false
	if size > int64(maxBytes) {
		start = size - int64(maxBytes)
		truncated = true
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return nil, tailInfo{}, err
	}
	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, tailInfo{}, err
	}

	if start > 0 {
		if idx := strings.IndexByte(string(buf), '\n'); idx >= 0 && idx+1 < len(buf) {
			buf = buf[idx+1:]
		}
	}

	raw := strings.ReplaceAll(string(buf), "\r\n", "\n")
	raw = strings.TrimRight(raw, "\n")
	if strings.TrimSpace(raw) == "" {
		return []string{}, tailInfo{TotalSize: size, Truncated: truncated}, nil
	}
	lines := strings.Split(raw, "\n")
	if maxLines > 0 && len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
		truncated = true
	}
	return lines, tailInfo{TotalSize: size, Truncated: truncated}, nil
}

type mutationAuditArgs struct {
	Reason    string
	Actor     string
	RequestID string
}

func parseMutationAuditArgs(args map[string]any, principal string) (mutationAuditArgs, error) {
	reason, err := parseRequiredString(args, "reason")
	if err != nil {
		return mutationAuditArgs{}, err
	}
	actor, err := parseString(args, "actor")
	if err != nil {
		return mutationAuditArgs{}, err
	}
	requestID, err := parseString(args, "request_id")
	if err != nil {
		return mutationAuditArgs{}, err
	}
	resolvedActor := strings.TrimSpace(actor)
	if resolvedActor == "" {
		resolvedActor = strings.TrimSpace(principal)
	}
	if err := validateMutationAuditFields(reason, resolvedActor, requestID); err != nil {
		return mutationAuditArgs{}, err
	}
	actor, err = bindAuditActorToPrincipal(actor, principal)
	if err != nil {
		return mutationAuditArgs{}, err
	}
	return mutationAuditArgs{
		Reason:    reason,
		Actor:     actor,
		RequestID: requestID,
	}, nil
}

func bindAuditActorToPrincipal(actor, principal string) (string, error) {
	actor = strings.TrimSpace(actor)
	principal = strings.TrimSpace(principal)
	if actor == "" {
		actor = principal
	}
	if actor != "" && principal != "" && actor != principal {
		return "", fmt.Errorf("actor %q must match configured MCP principal %q", actor, principal)
	}
	return actor, nil
}

func validateMutationAuditFields(reason, actor, requestID string) error {
	if len(reason) > maxAuditReasonLength {
		return fmt.Errorf("reason must be at most %d chars", maxAuditReasonLength)
	}
	if len(actor) > maxAuditActorLength {
		return fmt.Errorf("actor must be at most %d chars", maxAuditActorLength)
	}
	if len(requestID) > maxAuditRequestIDLength {
		return fmt.Errorf("request_id must be at most %d chars", maxAuditRequestIDLength)
	}
	return nil
}

func mutationAuditHeaders(audit mutationAuditArgs) map[string]string {
	headers := map[string]string{
		adminAuditReasonHeader: audit.Reason,
	}
	if audit.Actor != "" {
		headers[adminAuditActorHeader] = audit.Actor
	}
	if audit.RequestID != "" {
		headers[adminAuditRequestIDHeader] = audit.RequestID
	}
	return headers
}

func mutationAuditMap(audit mutationAuditArgs, principal string) map[string]any {
	out := map[string]any{
		"reason": audit.Reason,
	}
	if audit.Actor != "" {
		out["actor"] = audit.Actor
	}
	if audit.RequestID != "" {
		out["request_id"] = audit.RequestID
	}
	if strings.TrimSpace(principal) != "" {
		out["principal"] = strings.TrimSpace(principal)
	}
	return out
}

func withAuditPrincipal(out map[string]any, principal string) map[string]any {
	if out == nil {
		return nil
	}
	principal = strings.TrimSpace(principal)
	if principal == "" {
		return out
	}
	rawAudit, ok := out["audit"]
	if !ok {
		return out
	}
	audit, ok := rawAudit.(map[string]any)
	if !ok {
		return out
	}
	audit["principal"] = principal
	out["audit"] = audit
	return out
}

func parseRequiredString(args map[string]any, key string) (string, error) {
	v, err := parseString(args, key)
	if err != nil {
		return "", err
	}
	if v == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return v, nil
}

func validateOptionalRoutePath(route string) error {
	route = strings.TrimSpace(route)
	if route == "" {
		return nil
	}
	if !strings.HasPrefix(route, "/") {
		return errors.New("route must start with '/'")
	}
	return nil
}

func validateManagedSelectorLabels(application, endpointName string) error {
	application = strings.TrimSpace(application)
	endpointName = strings.TrimSpace(endpointName)
	if application != "" && !config.IsValidManagementLabel(application) {
		return fmt.Errorf("application must match %s", config.ManagementLabelPattern())
	}
	if endpointName != "" && !config.IsValidManagementLabel(endpointName) {
		return fmt.Errorf("endpoint_name must match %s", config.ManagementLabelPattern())
	}
	return nil
}

func keySet(keys ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		out[key] = struct{}{}
	}
	return out
}

func validateAllowedKeys(args map[string]any, allowed map[string]struct{}, scope string) error {
	if len(args) == 0 || len(allowed) == 0 {
		return nil
	}

	unknown := make([]string, 0)
	for key := range args {
		if _, ok := allowed[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	if len(unknown) == 1 {
		return fmt.Errorf("%s contains unknown key %q", scope, unknown[0])
	}
	return fmt.Errorf("%s contains unknown keys: %s", scope, strings.Join(unknown, ", "))
}

func parseStringMap(args map[string]any, key string) (map[string]string, error) {
	raw, ok := args[key]
	if !ok || raw == nil {
		return nil, nil
	}
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object of string values", key)
	}
	out := make(map[string]string, len(obj))
	for k, rv := range obj {
		s, ok := rv.(string)
		if !ok {
			return nil, fmt.Errorf("%s.%s must be a string", key, k)
		}
		out[k] = s
	}
	return out, nil
}

func parseString(args map[string]any, key string) (string, error) {
	raw, ok := args[key]
	if !ok {
		return "", nil
	}
	v, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	return strings.TrimSpace(v), nil
}

func parseBool(args map[string]any, key string) (bool, error) {
	raw, ok := args[key]
	if !ok {
		return false, nil
	}
	v, ok := raw.(bool)
	if !ok {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return v, nil
}

func parseOptionalTime(args map[string]any, key string) (time.Time, error) {
	raw, ok := args[key]
	if !ok {
		return time.Time{}, nil
	}
	v, ok := raw.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("%s must be an RFC3339 string", key)
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be RFC3339", key)
	}
	return t.UTC(), nil
}

func parseOptionalState(args map[string]any, key string) (queue.State, error) {
	raw, ok := args[key]
	if !ok {
		return "", nil
	}
	v, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return "", nil
	}
	st := queue.State(v)
	switch st {
	case queue.StateQueued, queue.StateLeased, queue.StateDelivered, queue.StateDead, queue.StateCanceled:
		return st, nil
	default:
		return "", fmt.Errorf("invalid state %q", v)
	}
}

func parseLimit(args map[string]any, max int) (int, error) {
	raw, ok := args["limit"]
	if !ok {
		return 100, nil
	}
	switch v := raw.(type) {
	case float64:
		n := int(v)
		if float64(n) != v {
			return 0, errors.New("limit must be an integer")
		}
		if n <= 0 || n > max {
			return 0, fmt.Errorf("limit must be between 1 and %d", max)
		}
		return n, nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0, errors.New("limit must be an integer")
		}
		if n <= 0 || n > max {
			return 0, fmt.Errorf("limit must be between 1 and %d", max)
		}
		return n, nil
	default:
		return 0, errors.New("limit must be an integer")
	}
}

func readFrame(r *bufio.Reader) ([]byte, error) {
	contentLength := -1
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		colon := strings.IndexByte(line, ':')
		if colon <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:colon])
		val := strings.TrimSpace(line[colon+1:])
		if strings.EqualFold(key, "Content-Length") {
			n, err := strconv.Atoi(val)
			if err != nil || n < 0 {
				return nil, errors.New("invalid content length")
			}
			contentLength = n
		}
	}
	if contentLength < 0 {
		return nil, errors.New("missing content length")
	}
	payload := make([]byte, contentLength)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func writeFrame(w io.Writer, msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Content-Length: %d\r\n\r\n", len(payload)); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}
