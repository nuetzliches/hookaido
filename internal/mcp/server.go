package mcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
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
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"hookaido/internal/config"
	"hookaido/internal/httpheader"
	"hookaido/internal/queue"
	"hookaido/internal/secrets"
)

const (
	protocolVersion           = "2024-11-05"
	maxListLimit              = 1000
	maxAuditReasonLength      = 512
	maxAuditActorLength       = 256
	maxAuditRequestIDLength   = 256
	defaultStartStopTimeout   = 10 * time.Second
	defaultReloadCheckTimeout = 5 * time.Second
	defaultAdminProxyTimeout  = 5 * time.Second
	adminProxyRetryMaxGET     = 3
	adminProxyRetryBackoff    = 100 * time.Millisecond
	defaultBacklogTrendWindow = time.Hour
	defaultBacklogTrendStep   = 5 * time.Minute
	minBacklogTrendStep       = time.Minute
	maxBacklogTrendStep       = time.Hour
	maxBacklogTrendWindow     = 7 * 24 * time.Hour
	maxBacklogTrendSamples    = 20000
	healthTrendSignalSamples  = 2000

	adminAuditReasonHeader    = "X-Hookaido-Audit-Reason"
	adminAuditActorHeader     = "X-Hookaido-Audit-Actor"
	adminAuditRequestIDHeader = "X-Request-ID"
)

var defaultBacklogSummaryStates = []queue.State{
	queue.StateQueued,
	queue.StateLeased,
	queue.StateDead,
}

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
					"path": map[string]any{"type": "string"},
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
					"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": maxListLimit},
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
					"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": maxListLimit},
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
					"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": maxListLimit},
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
			Description: "List queue messages from sqlite backend with optional filters",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"route":           routePathSchema(),
					"application":     managementLabelSchema(),
					"endpoint_name":   managementLabelSchema(),
					"target":          map[string]any{"type": "string"},
					"state":           map[string]any{"type": "string", "enum": []string{"queued", "leased", "delivered", "dead", "canceled"}},
					"limit":           map[string]any{"type": "integer", "minimum": 1, "maximum": maxListLimit},
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
			Description: "List delivery attempts from sqlite backend with optional filters",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"route":         routePathSchema(),
					"application":   managementLabelSchema(),
					"endpoint_name": managementLabelSchema(),
					"target":        map[string]any{"type": "string"},
					"event_id":      map[string]any{"type": "string"},
					"outcome":       map[string]any{"type": "string", "enum": []string{"acked", "retry", "dead"}},
					"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": maxListLimit},
					"before":        map[string]any{"type": "string", "description": "RFC3339 timestamp"},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "dlq_list",
			Description: "List dead-letter queue items from sqlite backend",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"route":           routePathSchema(),
					"limit":           map[string]any{"type": "integer", "minimum": 1, "maximum": maxListLimit},
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
							"maxItems": maxListLimit,
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
							"maxItems": maxListLimit,
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
							"maxItems": maxListLimit,
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
							"maxItems": maxListLimit,
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
							"maxItems": maxListLimit,
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
							"maxItems": maxListLimit,
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
						"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": maxListLimit},
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
						"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": maxListLimit},
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
						"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": maxListLimit},
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

func (s *Server) toolConfigParse(args map[string]any) (any, error) {
	p, err := s.resolveConfigPath(args)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return map[string]any{
			"ok":         false,
			"path":       p,
			"errors":     []string{err.Error()},
			"warnings":   []string{},
			"parse_only": true,
		}, nil
	}

	astRaw, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	ast := map[string]any{}
	if err := json.Unmarshal(astRaw, &ast); err != nil {
		return nil, err
	}

	return map[string]any{
		"ok":       true,
		"path":     p,
		"errors":   []string{},
		"warnings": []string{},
		"ast":      ast,
	}, nil
}

func (s *Server) toolConfigValidate(args map[string]any) (any, error) {
	p, err := s.resolveConfigPath(args)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return map[string]any{
			"ok":         false,
			"path":       p,
			"errors":     []string{err.Error()},
			"warnings":   []string{},
			"parse_only": true,
		}, nil
	}
	res := config.ValidateWithResult(cfg)
	return map[string]any{
		"ok":       res.OK,
		"path":     p,
		"errors":   res.Errors,
		"warnings": res.Warnings,
	}, nil
}

func (s *Server) toolConfigCompile(args map[string]any) (any, error) {
	p, err := s.resolveConfigPath(args)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return map[string]any{
			"ok":         false,
			"path":       p,
			"errors":     []string{err.Error()},
			"warnings":   []string{},
			"parse_only": true,
		}, nil
	}
	compiled, res := config.Compile(cfg)

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

	return map[string]any{
		"ok":       res.OK,
		"path":     p,
		"errors":   res.Errors,
		"warnings": res.Warnings,
		"summary": map[string]any{
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
			"path_count":          len(compiled.PathToRoute),
		},
	}, nil
}

func (s *Server) toolConfigFmtPreview(args map[string]any) (any, error) {
	p, err := s.resolveConfigPath(args)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return nil, err
	}
	formatted, err := config.Format(cfg)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"path":      p,
		"formatted": string(formatted),
	}, nil
}

func (s *Server) toolConfigDiff(args map[string]any) (any, error) {
	p, err := s.resolveConfigPath(args)
	if err != nil {
		return nil, err
	}
	contentRaw, ok := args["content"]
	if !ok {
		return nil, errors.New("content is required")
	}
	content, ok := contentRaw.(string)
	if !ok {
		return nil, errors.New("content must be a string")
	}
	contextLines, err := parseIntArg(args, "context", 3, 0, 20)
	if err != nil {
		return nil, err
	}

	currentData, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	currentCfg, err := config.Parse(currentData)
	if err != nil {
		return map[string]any{
			"ok":                 false,
			"path":               p,
			"changed":            false,
			"context":            contextLines,
			"errors":             []string{fmt.Sprintf("current config parse error: %v", err)},
			"warnings":           []string{},
			"current_parse_only": true,
		}, nil
	}
	currentFormatted, err := config.Format(currentCfg)
	if err != nil {
		return nil, err
	}
	_, currentRes := config.Compile(currentCfg)
	if !currentRes.OK {
		return map[string]any{
			"ok":              false,
			"path":            p,
			"changed":         false,
			"context":         contextLines,
			"errors":          currentRes.Errors,
			"warnings":        currentRes.Warnings,
			"current_invalid": true,
		}, nil
	}

	candidateCfg, err := config.Parse([]byte(content))
	if err != nil {
		return map[string]any{
			"ok":                   false,
			"path":                 p,
			"changed":              false,
			"context":              contextLines,
			"errors":               []string{err.Error()},
			"warnings":             []string{},
			"candidate_parse_only": true,
		}, nil
	}
	candidateFormatted, err := config.Format(candidateCfg)
	if err != nil {
		return nil, err
	}
	_, candidateRes := config.Compile(candidateCfg)
	if !candidateRes.OK {
		return map[string]any{
			"ok":       false,
			"path":     p,
			"changed":  false,
			"context":  contextLines,
			"errors":   candidateRes.Errors,
			"warnings": candidateRes.Warnings,
		}, nil
	}

	oldLines := config.NormalizedDiffLines(string(currentFormatted))
	newLines := config.NormalizedDiffLines(string(candidateFormatted))
	ops := config.LineDiffOps(oldLines, newLines)
	diff := config.UnifiedDiff(ops, contextLines, "current", "candidate")

	return map[string]any{
		"ok":                  true,
		"path":                p,
		"context":             contextLines,
		"changed":             diff != "",
		"unified_diff":        diff,
		"current_formatted":   string(currentFormatted),
		"candidate_formatted": string(candidateFormatted),
		"errors":              []string{},
		"warnings":            candidateRes.Warnings,
		"current_warnings":    currentRes.Warnings,
	}, nil
}

func (s *Server) toolConfigApply(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, configApplyAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	p, err := s.resolveConfigPath(args)
	if err != nil {
		return nil, err
	}
	contentRaw, ok := args["content"]
	if !ok {
		return nil, errors.New("content is required")
	}
	content, ok := contentRaw.(string)
	if !ok {
		return nil, errors.New("content must be a string")
	}

	mode := "preview_only"
	if rawMode, ok := args["mode"]; ok {
		m, ok := rawMode.(string)
		if !ok {
			return nil, errors.New("mode must be a string")
		}
		mode = strings.ToLower(strings.TrimSpace(m))
		if mode == "" {
			mode = "preview_only"
		}
	}
	switch mode {
	case "preview_only", "write_only", "write_and_reload":
	default:
		return nil, fmt.Errorf("unsupported mode %q", mode)
	}
	reloadTimeout := 5 * time.Second
	if mode == "write_and_reload" {
		d, err := parseReloadTimeout(args, reloadTimeout)
		if err != nil {
			return nil, err
		}
		reloadTimeout = d
	}

	cfg, err := config.Parse([]byte(content))
	if err != nil {
		return map[string]any{
			"ok":         false,
			"path":       p,
			"mode":       mode,
			"applied":    false,
			"errors":     []string{err.Error()},
			"warnings":   []string{},
			"parse_only": true,
		}, nil
	}

	compiled, res := config.Compile(cfg)
	if !res.OK {
		return map[string]any{
			"ok":       false,
			"path":     p,
			"mode":     mode,
			"applied":  false,
			"errors":   res.Errors,
			"warnings": res.Warnings,
		}, nil
	}

	applied := false
	reloaded := false
	rolledBack := false
	healthURL := ""
	reloadErr := ""

	if mode == "write_only" {
		if err := writeFileAtomic(p, []byte(content)); err != nil {
			return nil, err
		}
		applied = true
	}

	if mode == "write_and_reload" {
		previous, existed, err := readExistingFile(p)
		if err != nil {
			return nil, err
		}

		if err := writeFileAtomic(p, []byte(content)); err != nil {
			return nil, err
		}

		healthURL, err = waitForAdminHealth(compiled, reloadTimeout)
		if err != nil {
			reloadErr = err.Error()
			if rbErr := rollbackConfigFile(p, existed, previous); rbErr != nil {
				return nil, fmt.Errorf("reload verification failed (%v), rollback failed: %w", err, rbErr)
			}
			rolledBack = true
		} else {
			applied = true
			reloaded = true
		}
	}

	out := map[string]any{
		"ok":       reloadErr == "",
		"path":     p,
		"mode":     mode,
		"applied":  applied,
		"errors":   []string{},
		"warnings": res.Warnings,
	}
	if mode == "write_and_reload" {
		out["reload_timeout"] = reloadTimeout.String()
		out["health_url"] = healthURL
		out["reloaded"] = reloaded
		out["rolled_back"] = rolledBack
		out["reload_error"] = reloadErr
		if reloadErr != "" {
			out["errors"] = []string{reloadErr}
		}
	}
	return out, nil
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

	localDBRequired := queueBackend != "memory"
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
	if queueBackend == "memory" {
		localQueue["source"] = "admin_api"
		if checked, _ := adminProbe["checked"].(bool); checked {
			if adminOK, _ := adminProbe["ok"].(bool); adminOK {
				if details, ok := adminProbe["details"].(map[string]any); ok {
					if diagnostics, ok := details["diagnostics"].(map[string]any); ok {
						if queueDiag, ok := diagnostics["queue"].(map[string]any); ok {
							proxyQueue := make(map[string]any, len(queueDiag)+2)
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
		store, err := s.openSQLiteStore()
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

func (s *Server) toolBacklogTopQueued(args map[string]any) (any, error) {
	route, err := parseString(args, "route")
	if err != nil {
		return nil, err
	}
	if err := validateOptionalRoutePath(route); err != nil {
		return nil, err
	}
	target, err := parseString(args, "target")
	if err != nil {
		return nil, err
	}
	limit, err := parseLimit(args, maxListLimit)
	if err != nil {
		return nil, err
	}

	if compiled, useAdmin := s.queueToolsUseAdminProxy(); useAdmin {
		query := url.Values{}
		if route != "" {
			query.Set("route", route)
		}
		if target != "" {
			query.Set("target", target)
		}
		query.Set("limit", strconv.Itoa(limit))
		return s.callAdminJSON(compiled, http.MethodGet, "/backlog/top_queued", query, nil, nil, defaultAdminProxyTimeout)
	}

	store, err := s.openSQLiteStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	stats, err := store.Stats()
	if err != nil {
		return nil, err
	}
	return backlogTopQueuedMap(stats, route, target, limit), nil
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

func (s *Server) toolBacklogOldestQueued(args map[string]any) (any, error) {
	route, err := parseString(args, "route")
	if err != nil {
		return nil, err
	}
	if err := validateOptionalRoutePath(route); err != nil {
		return nil, err
	}
	target, err := parseString(args, "target")
	if err != nil {
		return nil, err
	}
	limit, err := parseLimit(args, maxListLimit)
	if err != nil {
		return nil, err
	}

	if compiled, useAdmin := s.queueToolsUseAdminProxy(); useAdmin {
		query := url.Values{}
		if route != "" {
			query.Set("route", route)
		}
		if target != "" {
			query.Set("target", target)
		}
		query.Set("limit", strconv.Itoa(limit))
		return s.callAdminJSON(compiled, http.MethodGet, "/backlog/oldest_queued", query, nil, nil, defaultAdminProxyTimeout)
	}

	store, err := s.openSQLiteStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	stats, err := store.Stats()
	if err != nil {
		return nil, err
	}

	probeLimit := limit
	if limit < maxListLimit {
		probeLimit = limit + 1
	}
	resp, err := store.ListMessages(queue.MessageListRequest{
		Route:  route,
		Target: target,
		State:  queue.StateQueued,
		Order:  queue.MessageOrderAsc,
		Limit:  probeLimit,
	})
	if err != nil {
		return nil, err
	}

	items := resp.Items
	truncated := false
	if limit < maxListLimit && len(items) > limit {
		truncated = true
		items = items[:limit]
	}
	if limit == maxListLimit && route == "" && target == "" && stats.ByState[queue.StateQueued] > limit {
		truncated = true
	}

	return backlogOldestQueuedMap(backlogReferenceNow(stats), stats, route, target, limit, truncated, items), nil
}

func (s *Server) toolBacklogAgingSummary(args map[string]any) (any, error) {
	route, err := parseString(args, "route")
	if err != nil {
		return nil, err
	}
	if err := validateOptionalRoutePath(route); err != nil {
		return nil, err
	}
	target, err := parseString(args, "target")
	if err != nil {
		return nil, err
	}
	limit, err := parseLimit(args, maxListLimit)
	if err != nil {
		return nil, err
	}
	states, err := parseBacklogSummaryStatesArg(args, "states")
	if err != nil {
		return nil, err
	}

	if compiled, useAdmin := s.queueToolsUseAdminProxy(); useAdmin {
		query := url.Values{}
		if route != "" {
			query.Set("route", route)
		}
		if target != "" {
			query.Set("target", target)
		}
		query.Set("limit", strconv.Itoa(limit))
		if len(states) > 0 {
			parts := make([]string, 0, len(states))
			for _, state := range states {
				parts = append(parts, string(state))
			}
			query.Set("states", strings.Join(parts, ","))
		}
		return s.callAdminJSON(compiled, http.MethodGet, "/backlog/aging_summary", query, nil, nil, defaultAdminProxyTimeout)
	}

	store, err := s.openSQLiteStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	stats, err := store.Stats()
	if err != nil {
		return nil, err
	}

	probeLimit := limit
	if limit < maxListLimit {
		probeLimit = limit + 1
	}
	truncated := false
	stateScan := make(map[queue.State]backlogStateScanSummary, len(states))
	items := make([]queue.Envelope, 0, len(states)*limit)
	for _, state := range states {
		resp, err := store.ListMessages(queue.MessageListRequest{
			Route:  route,
			Target: target,
			State:  state,
			Order:  queue.MessageOrderAsc,
			Limit:  probeLimit,
		})
		if err != nil {
			return nil, err
		}
		stateItems := resp.Items
		stateTruncated := false
		if limit < maxListLimit && len(stateItems) > limit {
			stateTruncated = true
			stateItems = stateItems[:limit]
		}
		if limit == maxListLimit && route == "" && target == "" && stats.ByState[state] > limit {
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

	return backlogAgingSummaryMap(backlogReferenceNow(stats), stats, route, target, states, limit, truncated, stateScan, items), nil
}

func (s *Server) toolBacklogTrends(args map[string]any) (any, error) {
	route, err := parseString(args, "route")
	if err != nil {
		return nil, err
	}
	if err := validateOptionalRoutePath(route); err != nil {
		return nil, err
	}
	target, err := parseString(args, "target")
	if err != nil {
		return nil, err
	}
	window, err := parseDurationArg(args, "window", defaultBacklogTrendWindow)
	if err != nil {
		return nil, err
	}
	if window <= 0 || window > maxBacklogTrendWindow {
		return nil, fmt.Errorf("window must be > 0 and <= %s", maxBacklogTrendWindow)
	}
	step, err := parseDurationArg(args, "step", defaultBacklogTrendStep)
	if err != nil {
		return nil, err
	}
	if step < minBacklogTrendStep || step > maxBacklogTrendStep {
		return nil, fmt.Errorf("step must be between %s and %s", minBacklogTrendStep, maxBacklogTrendStep)
	}
	if step > window {
		return nil, errors.New("step must be <= window")
	}
	until, err := parseOptionalTime(args, "until")
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("window/step yields %d buckets; max %d", bucketCount, maxListLimit)
	}

	if compiled, useAdmin := s.queueToolsUseAdminProxy(); useAdmin {
		query := url.Values{}
		if route != "" {
			query.Set("route", route)
		}
		if target != "" {
			query.Set("target", target)
		}
		query.Set("window", window.String())
		query.Set("step", step.String())
		if !until.IsZero() {
			query.Set("until", until.UTC().Format(time.RFC3339Nano))
		}
		return s.callAdminJSON(compiled, http.MethodGet, "/backlog/trends", query, nil, nil, defaultAdminProxyTimeout)
	}

	signalCfg := s.resolveTrendSignalConfig()

	store, err := s.openSQLiteStore()
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	trendStore, ok := any(store).(queue.BacklogTrendStore)
	if !ok {
		return nil, errors.New("backlog trend snapshots are unavailable for this store")
	}
	resp, err := trendStore.ListBacklogTrend(queue.BacklogTrendListRequest{
		Route:  route,
		Target: target,
		Since:  since,
		Until:  until,
		Limit:  maxBacklogTrendSamples,
	})
	if err != nil {
		return nil, err
	}
	return backlogTrendsMap(since, until, window, step, route, target, resp.Truncated, resp.Items, signalCfg), nil
}

func (s *Server) toolManagementEndpointUpsert(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, managementEndpointUpsertAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	configPath, err := s.resolveConfigPath(args)
	if err != nil {
		return nil, err
	}

	mutationReq, err := parseManagementEndpointMutationArgs(args, true, s.auditPrincipal())
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return nil, err
	}
	compiled, res := config.Compile(cfg)
	if !res.OK {
		return nil, fmt.Errorf("config compile failed: %s", strings.Join(res.Errors, "; "))
	}
	if err := validateScopedManagedAuditPolicyForManagedMutation(mutationAuditArgs{
		Reason:    mutationReq.Reason,
		Actor:     mutationReq.Actor,
		RequestID: mutationReq.RequestID,
	}, compiled); err != nil {
		return nil, err
	}
	if err := s.enforceManagementEndpointBacklogGuardrailForUpsert(compiled, mutationReq.Application, mutationReq.EndpointName, mutationReq.Route); err != nil {
		return nil, localManagementEndpointMutationError("upsert", err)
	}

	result, err := applyManagementEndpointUpsert(cfg, compiled, mutationReq.Application, mutationReq.EndpointName, mutationReq.Route)
	if err != nil {
		return nil, localManagementEndpointMutationError("upsert", err)
	}

	out := map[string]any{
		"ok":             true,
		"action":         result.Action,
		"application":    mutationReq.Application,
		"endpoint_name":  mutationReq.EndpointName,
		"route":          result.Route,
		"mode":           mutationReq.Mode,
		"mutates_config": result.MutatesConfig,
		"audit": map[string]any{
			"reason":     mutationReq.Reason,
			"actor":      mutationReq.Actor,
			"request_id": mutationReq.RequestID,
			"principal":  s.auditPrincipal(),
		},
	}
	if !result.MutatesConfig {
		return out, nil
	}

	formatted, err := config.Format(cfg)
	if err != nil {
		return nil, err
	}

	applyArgs := map[string]any{
		"path":    configPath,
		"content": string(formatted),
		"mode":    mutationReq.Mode,
	}
	if mutationReq.ReloadTimeout != "" {
		applyArgs["reload_timeout"] = mutationReq.ReloadTimeout
	}
	applyOut, err := s.toolConfigApply(applyArgs)
	if err != nil {
		return nil, err
	}
	if applyMap, ok := applyOut.(map[string]any); ok {
		if okVal, ok := applyMap["ok"].(bool); ok {
			out["ok"] = okVal
		}
	}
	out["config_apply"] = applyOut
	return out, nil
}

func (s *Server) toolManagementEndpointDelete(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, managementEndpointDeleteAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	configPath, err := s.resolveConfigPath(args)
	if err != nil {
		return nil, err
	}

	mutationReq, err := parseManagementEndpointMutationArgs(args, false, s.auditPrincipal())
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return nil, err
	}
	compiled, res := config.Compile(cfg)
	if !res.OK {
		return nil, fmt.Errorf("config compile failed: %s", strings.Join(res.Errors, "; "))
	}
	if err := validateScopedManagedAuditPolicyForManagedMutation(mutationAuditArgs{
		Reason:    mutationReq.Reason,
		Actor:     mutationReq.Actor,
		RequestID: mutationReq.RequestID,
	}, compiled); err != nil {
		return nil, err
	}
	if err := s.enforceManagementEndpointBacklogGuardrailForDelete(compiled, mutationReq.Application, mutationReq.EndpointName); err != nil {
		return nil, localManagementEndpointMutationError("delete", err)
	}

	result, err := applyManagementEndpointDelete(cfg, compiled, mutationReq.Application, mutationReq.EndpointName)
	if err != nil {
		return nil, localManagementEndpointMutationError("delete", err)
	}

	out := map[string]any{
		"ok":             true,
		"action":         result.Action,
		"application":    mutationReq.Application,
		"endpoint_name":  mutationReq.EndpointName,
		"route":          result.Route,
		"mode":           mutationReq.Mode,
		"mutates_config": result.MutatesConfig,
		"audit": map[string]any{
			"reason":     mutationReq.Reason,
			"actor":      mutationReq.Actor,
			"request_id": mutationReq.RequestID,
			"principal":  s.auditPrincipal(),
		},
	}
	if !result.MutatesConfig {
		return out, nil
	}

	formatted, err := config.Format(cfg)
	if err != nil {
		return nil, err
	}

	applyArgs := map[string]any{
		"path":    configPath,
		"content": string(formatted),
		"mode":    mutationReq.Mode,
	}
	if mutationReq.ReloadTimeout != "" {
		applyArgs["reload_timeout"] = mutationReq.ReloadTimeout
	}
	applyOut, err := s.toolConfigApply(applyArgs)
	if err != nil {
		return nil, err
	}
	if applyMap, ok := applyOut.(map[string]any); ok {
		if okVal, ok := applyMap["ok"].(bool); ok {
			out["ok"] = okVal
		}
	}
	out["config_apply"] = applyOut
	return out, nil
}

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

	store, err := s.openSQLiteStore()
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

	store, err := s.openSQLiteStore()
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

	store, err := s.openSQLiteStore()
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

	store, err := s.openSQLiteStore()
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

	store, err := s.openSQLiteStore()
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

	store, err := s.openSQLiteStore()
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

	store, err := s.openSQLiteStore()
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

	store, err := s.openSQLiteStore()
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

	store, err := s.openSQLiteStore()
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

	store, err := s.openSQLiteStore()
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

	store, err := s.openSQLiteStore()
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

	store, err := s.openSQLiteStore()
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

func (s *Server) openSQLiteStore() (*queue.SQLiteStore, error) {
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
	return queue.NewSQLiteStore(p)
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
		out.useAdminProxy = compiledQueueBackend(compiled) == "memory"
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
	return compiled, compiledQueueBackend(compiled) == "memory"
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

func readExistingFile(p string) ([]byte, bool, error) {
	b, err := os.ReadFile(p)
	if err == nil {
		return b, true, nil
	}
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	return nil, false, err
}

func rollbackConfigFile(p string, existed bool, previous []byte) error {
	if existed {
		return writeFileAtomic(p, previous)
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(filepath.Dir(p))
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

func parseMessageListArgs(args map[string]any) (queue.MessageListRequest, string, string, error) {
	before, err := parseOptionalTime(args, "before")
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	state, err := parseOptionalState(args, "state")
	if err != nil {
		return queue.MessageListRequest{}, "", "", err
	}
	limit, err := parseLimit(args, maxListLimit)
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

type managementEndpointMutationRequest struct {
	Application   string
	EndpointName  string
	Route         string
	Reason        string
	Actor         string
	RequestID     string
	Mode          string
	ReloadTimeout string
}

type managementEndpointMutationResult struct {
	Action        string
	Route         string
	MutatesConfig bool
}

type mutationAuditArgs struct {
	Reason    string
	Actor     string
	RequestID string
}

func parseManagementEndpointMutationArgs(args map[string]any, requireRoute bool, principal string) (managementEndpointMutationRequest, error) {
	application, err := parseRequiredString(args, "application")
	if err != nil {
		return managementEndpointMutationRequest{}, err
	}
	endpointName, err := parseRequiredString(args, "endpoint_name")
	if err != nil {
		return managementEndpointMutationRequest{}, err
	}
	if err := validateManagedSelectorLabels(application, endpointName); err != nil {
		return managementEndpointMutationRequest{}, err
	}
	route := ""
	if requireRoute {
		route, err = parseRequiredString(args, "route")
		if err != nil {
			return managementEndpointMutationRequest{}, err
		}
		if err := validateOptionalRoutePath(route); err != nil {
			return managementEndpointMutationRequest{}, err
		}
	}
	reason, err := parseRequiredString(args, "reason")
	if err != nil {
		return managementEndpointMutationRequest{}, err
	}
	actor, err := parseString(args, "actor")
	if err != nil {
		return managementEndpointMutationRequest{}, err
	}
	requestID, err := parseString(args, "request_id")
	if err != nil {
		return managementEndpointMutationRequest{}, err
	}
	mode, err := parseString(args, "mode")
	if err != nil {
		return managementEndpointMutationRequest{}, err
	}
	if mode == "" {
		mode = "write_only"
	}
	switch mode {
	case "preview_only", "write_only", "write_and_reload":
	default:
		return managementEndpointMutationRequest{}, fmt.Errorf("unsupported mode %q", mode)
	}
	reloadTimeout, err := parseString(args, "reload_timeout")
	if err != nil {
		return managementEndpointMutationRequest{}, err
	}
	resolvedActor := strings.TrimSpace(actor)
	if resolvedActor == "" {
		resolvedActor = strings.TrimSpace(principal)
	}
	if err := validateMutationAuditFields(reason, resolvedActor, requestID); err != nil {
		return managementEndpointMutationRequest{}, err
	}
	actor, err = bindAuditActorToPrincipal(actor, principal)
	if err != nil {
		return managementEndpointMutationRequest{}, err
	}

	return managementEndpointMutationRequest{
		Application:   application,
		EndpointName:  endpointName,
		Route:         route,
		Reason:        reason,
		Actor:         actor,
		RequestID:     requestID,
		Mode:          mode,
		ReloadTimeout: reloadTimeout,
	}, nil
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

type managementEndpointMutationError struct {
	code   string
	detail string
}

func (e *managementEndpointMutationError) Error() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.detail)
}

func newManagementEndpointMutationError(code, detail string, args ...any) error {
	if len(args) > 0 {
		detail = fmt.Sprintf(detail, args...)
	}
	return &managementEndpointMutationError{
		code:   strings.TrimSpace(code),
		detail: strings.TrimSpace(detail),
	}
}

func localManagementEndpointMutationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var typed *managementEndpointMutationError
	if !errors.As(err, &typed) || typed == nil {
		return err
	}

	code := strings.TrimSpace(typed.code)
	if code == "" {
		return err
	}

	detail := strings.TrimSpace(typed.detail)
	if detail == "" {
		detail = adminProxyErrorDetailFallback(code)
	}
	if detail == "" {
		detail = strings.TrimSpace(err.Error())
	}
	if detail == "" {
		detail = "management endpoint mutation failed"
	}

	op := strings.TrimSpace(operation)
	if op == "" {
		op = "mutation"
	}
	return fmt.Errorf("management endpoint %s failed: code=%s, detail=%s", op, code, detail)
}

func (s *Server) enforceManagementEndpointBacklogGuardrailForUpsert(compiled config.Compiled, application, endpointName, targetRoute string) error {
	targetRoute = strings.TrimSpace(targetRoute)
	_, currentRoute, hasCurrent := compiledManagedEndpointRoute(compiled, application, endpointName)
	currentRoute = strings.TrimSpace(currentRoute)
	if !hasCurrent || currentRoute == "" || currentRoute == targetRoute {
		return nil
	}
	return s.enforceManagementEndpointBacklogGuardrailForRoute(compiled, "move", currentRoute)
}

func (s *Server) enforceManagementEndpointBacklogGuardrailForDelete(compiled config.Compiled, application, endpointName string) error {
	_, currentRoute, hasCurrent := compiledManagedEndpointRoute(compiled, application, endpointName)
	currentRoute = strings.TrimSpace(currentRoute)
	if !hasCurrent || currentRoute == "" {
		return nil
	}
	return s.enforceManagementEndpointBacklogGuardrailForRoute(compiled, "delete", currentRoute)
}

func (s *Server) enforceManagementEndpointBacklogGuardrailForRoute(compiled config.Compiled, operation, route string) error {
	queued, leased, evaluated, err := s.managementRouteHasActiveBacklog(compiled, route)
	if err != nil {
		return err
	}
	if !evaluated || (!queued && !leased) {
		return nil
	}
	return managementRouteBacklogActiveMutationError(operation, route, queued, leased)
}

func (s *Server) managementRouteHasActiveBacklog(compiled config.Compiled, route string) (queued, leased, evaluated bool, err error) {
	route = strings.TrimSpace(route)
	if route == "" {
		return false, false, false, nil
	}

	switch compiledQueueBackend(compiled) {
	case "sqlite":
		store, err := s.openSQLiteStore()
		if err != nil {
			return false, false, false, nil
		}
		defer func() { _ = store.Close() }()
		queued, leased, err = managementRouteHasActiveBacklogInStore(store, route)
		if err != nil {
			return false, false, true, err
		}
		return queued, leased, true, nil
	case "memory":
		return s.managementRouteHasActiveBacklogViaAdmin(compiled, route)
	default:
		return false, false, false, nil
	}
}

func (s *Server) managementRouteHasActiveBacklogViaAdmin(compiled config.Compiled, route string) (queued, leased bool, evaluated bool, err error) {
	checkState := func(state queue.State) (hasBacklog bool, stateEvaluated bool, stateErr error) {
		query := url.Values{
			"route": {route},
			"state": {string(state)},
			"limit": {"1"},
		}
		out, callErr := s.callAdminJSON(compiled, http.MethodGet, "/messages", query, nil, nil, defaultAdminProxyTimeout)
		if callErr != nil {
			return false, false, nil
		}
		hasBacklog, stateErr = managementRouteStateHasBacklogViaAdminResponse(out)
		if stateErr != nil {
			return false, true, stateErr
		}
		return hasBacklog, true, nil
	}

	queuedHasBacklog, queuedEvaluated, err := checkState(queue.StateQueued)
	if err != nil {
		return false, false, true, err
	}
	leasedHasBacklog, leasedEvaluated, err := checkState(queue.StateLeased)
	if err != nil {
		return false, false, true, err
	}

	switch {
	case queuedEvaluated && queuedHasBacklog:
		return true, leasedHasBacklog, true, nil
	case leasedEvaluated && leasedHasBacklog:
		return queuedHasBacklog, true, true, nil
	case queuedEvaluated && leasedEvaluated:
		return false, false, true, nil
	default:
		return false, false, false, nil
	}
}

func managementRouteStateHasBacklogViaAdminResponse(resp map[string]any) (bool, error) {
	if resp == nil {
		return false, errors.New("admin backlog check response is empty")
	}
	rawItems, ok := resp["items"]
	if !ok {
		return false, errors.New("admin backlog check response is missing items")
	}
	switch items := rawItems.(type) {
	case []any:
		return len(items) > 0, nil
	case []map[string]any:
		return len(items) > 0, nil
	default:
		return false, fmt.Errorf("admin backlog check response items has unexpected type %T", rawItems)
	}
}

func managementRouteHasActiveBacklogInStore(store queue.Store, route string) (queued, leased bool, err error) {
	if store == nil {
		return false, false, nil
	}
	route = strings.TrimSpace(route)
	if route == "" {
		return false, false, nil
	}

	queuedResp, err := store.ListMessages(queue.MessageListRequest{
		Route: route,
		State: queue.StateQueued,
		Limit: 1,
	})
	if err != nil {
		return false, false, err
	}
	queued = len(queuedResp.Items) > 0

	leasedResp, err := store.ListMessages(queue.MessageListRequest{
		Route: route,
		State: queue.StateLeased,
		Limit: 1,
	})
	if err != nil {
		return false, false, err
	}
	leased = len(leasedResp.Items) > 0
	return queued, leased, nil
}

func managementRouteBacklogActiveMutationError(operation, route string, queued, leased bool) error {
	operation = strings.TrimSpace(operation)
	if operation == "" {
		operation = "mutate"
	}
	route = strings.TrimSpace(route)
	states := make([]string, 0, 2)
	if queued {
		states = append(states, string(queue.StateQueued))
	}
	if leased {
		states = append(states, string(queue.StateLeased))
	}
	detail := fmt.Sprintf(
		"cannot %s management endpoint while route %q has active backlog (%s); drain queued/leased messages first",
		operation,
		route,
		strings.Join(states, ", "),
	)
	return newManagementEndpointMutationError("management_route_backlog_active", detail)
}

func applyManagementEndpointUpsert(cfg *config.Config, compiled config.Compiled, application, endpointName, route string) (managementEndpointMutationResult, error) {
	if cfg == nil {
		return managementEndpointMutationResult{}, errors.New("nil config")
	}

	targetIdx, ok := compiledRouteIndexByPath(compiled, route)
	if !ok || targetIdx < 0 || targetIdx >= len(cfg.Routes) {
		return managementEndpointMutationResult{}, newManagementEndpointMutationError("management_route_not_found", "route %q not found in compiled config", route)
	}

	target := compiled.Routes[targetIdx]
	targetApp := strings.TrimSpace(target.Application)
	targetEndpoint := strings.TrimSpace(target.EndpointName)
	if (targetApp != "" || targetEndpoint != "") && (targetApp != application || targetEndpoint != endpointName) {
		return managementEndpointMutationResult{}, newManagementEndpointMutationError("management_route_already_mapped", "route %q is already mapped to (%q, %q)", route, targetApp, targetEndpoint)
	}

	currentIdx, _, hasCurrent := compiledManagedEndpointRoute(compiled, application, endpointName)
	if hasCurrent && currentIdx == targetIdx {
		return managementEndpointMutationResult{
			Action:        "noop",
			Route:         route,
			MutatesConfig: false,
		}, nil
	}
	if !target.Publish {
		return managementEndpointMutationResult{}, newManagementEndpointMutationError("management_route_publish_disabled", "managed publish is disabled for route %q by route.publish", route)
	}
	if !target.PublishManaged {
		return managementEndpointMutationResult{}, newManagementEndpointMutationError("management_route_publish_disabled", "managed publish is disabled for route %q by route.publish.managed", route)
	}
	if hasCurrent && currentIdx >= 0 && currentIdx < len(compiled.Routes) {
		current := compiled.Routes[currentIdx]
		if !managedEndpointRouteProfileCompatible(current, target) {
			return managementEndpointMutationResult{}, newManagementEndpointMutationError(
				"management_route_target_mismatch",
				managedEndpointTargetProfileMismatchDetail(current, target),
			)
		}
	}

	if hasCurrent && currentIdx >= 0 && currentIdx < len(cfg.Routes) {
		clearRouteManagedEndpointLabel(&cfg.Routes[currentIdx])
	}
	setRouteManagedEndpointLabel(&cfg.Routes[targetIdx], application, endpointName)

	action := "created"
	if hasCurrent {
		action = "updated"
	}
	return managementEndpointMutationResult{
		Action:        action,
		Route:         route,
		MutatesConfig: true,
	}, nil
}

func applyManagementEndpointDelete(cfg *config.Config, compiled config.Compiled, application, endpointName string) (managementEndpointMutationResult, error) {
	if cfg == nil {
		return managementEndpointMutationResult{}, errors.New("nil config")
	}

	currentIdx, route, ok := compiledManagedEndpointRoute(compiled, application, endpointName)
	if !ok || currentIdx < 0 || currentIdx >= len(cfg.Routes) {
		return managementEndpointMutationResult{}, newManagementEndpointMutationError("management_endpoint_not_found", "management endpoint (%q, %q) not found", application, endpointName)
	}

	clearRouteManagedEndpointLabel(&cfg.Routes[currentIdx])
	return managementEndpointMutationResult{
		Action:        "deleted",
		Route:         route,
		MutatesConfig: true,
	}, nil
}

func compiledRouteIndexByPath(compiled config.Compiled, route string) (int, bool) {
	route = strings.TrimSpace(route)
	if route == "" {
		return -1, false
	}
	for i := range compiled.Routes {
		if compiled.Routes[i].Path == route {
			return i, true
		}
	}
	return -1, false
}

func compiledManagedEndpointRoute(compiled config.Compiled, application, endpointName string) (int, string, bool) {
	application = strings.TrimSpace(application)
	endpointName = strings.TrimSpace(endpointName)
	if application == "" || endpointName == "" {
		return -1, "", false
	}
	for i := range compiled.Routes {
		r := compiled.Routes[i]
		if strings.TrimSpace(r.Application) == application && strings.TrimSpace(r.EndpointName) == endpointName {
			return i, r.Path, true
		}
	}
	return -1, "", false
}

func managedEndpointRouteProfileCompatible(current, target config.CompiledRoute) bool {
	if managedEndpointRouteMode(current) != managedEndpointRouteMode(target) {
		return false
	}
	return managedEndpointTargetSetEqual(compiledRouteTargets(current), compiledRouteTargets(target))
}

func managedEndpointRouteMode(route config.CompiledRoute) string {
	if route.Pull != nil {
		return "pull"
	}
	if len(route.Deliveries) > 0 {
		return "deliver"
	}
	return ""
}

func managedEndpointTargetSetEqual(left, right []string) bool {
	left = normalizePublishTargets(left)
	right = normalizePublishTargets(right)
	if len(left) != len(right) {
		return false
	}
	if len(left) == 0 {
		return true
	}

	seen := make(map[string]struct{}, len(left))
	for _, target := range left {
		seen[target] = struct{}{}
	}
	for _, target := range right {
		if _, ok := seen[target]; !ok {
			return false
		}
	}
	return true
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

func managedEndpointTargetProfileMismatchDetail(current, target config.CompiledRoute) string {
	return fmt.Sprintf(
		"managed endpoint target profile mismatch between current route %q (mode=%s, targets=%s) and target route %q (mode=%s, targets=%s)",
		strings.TrimSpace(current.Path),
		managedEndpointRouteMode(current),
		formatAllowedTargetsForError(compiledRouteTargets(current)),
		strings.TrimSpace(target.Path),
		managedEndpointRouteMode(target),
		formatAllowedTargetsForError(compiledRouteTargets(target)),
	)
}

func setRouteManagedEndpointLabel(route *config.Route, application, endpointName string) {
	if route == nil {
		return
	}
	route.Application = application
	route.ApplicationQuoted = true
	route.ApplicationSet = true
	route.EndpointName = endpointName
	route.EndpointNameQuoted = true
	route.EndpointNameSet = true
}

func clearRouteManagedEndpointLabel(route *config.Route) {
	if route == nil {
		return
	}
	route.Application = ""
	route.ApplicationQuoted = false
	route.ApplicationSet = false
	route.EndpointName = ""
	route.EndpointNameQuoted = false
	route.EndpointNameSet = false
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

	limit, err := parseLimit(args, maxListLimit)
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
	limit, err := parseLimit(args, maxListLimit)
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

func parseDeadListArgs(args map[string]any) (queue.DeadListRequest, error) {
	before, err := parseOptionalTime(args, "before")
	if err != nil {
		return queue.DeadListRequest{}, err
	}
	limit, err := parseLimit(args, maxListLimit)
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

	if len(in) == 0 || len(in) > maxListLimit {
		return nil, fmt.Errorf("ids must contain between 1 and %d entries", maxListLimit)
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
	if len(in) == 0 || len(in) > maxListLimit {
		return nil, fmt.Errorf("items must contain between 1 and %d entries", maxListLimit)
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

func backlogTopQueuedMap(stats queue.Stats, routeFilter, targetFilter string, limit int) map[string]any {
	if limit <= 0 {
		limit = 100
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

	items := make([]map[string]any, 0, len(filtered))
	for _, b := range filtered {
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
		items = append(items, item)
	}

	out := map[string]any{
		"queue_total":     stats.Total,
		"queued_total":    queuedTotal,
		"limit":           limit,
		"truncated":       truncated,
		"source_bounded":  sourceQueued < queuedTotal,
		"items":           items,
		"selector":        map[string]any{"route": routeFilter, "target": targetFilter},
		"source":          "queue_stats.top_queued",
		"source_bucket_n": len(stats.TopQueued),
	}
	if !stats.OldestQueuedReceivedAt.IsZero() {
		out["oldest_queued_received_at"] = stats.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
		out["oldest_queued_age_seconds"] = int(stats.OldestQueuedAge / time.Second)
	}
	if !stats.EarliestQueuedNextRun.IsZero() {
		out["earliest_queued_next_run_at"] = stats.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
		out["ready_lag_seconds"] = int(stats.ReadyLag / time.Second)
	}
	return out
}

func backlogOldestQueuedMap(now time.Time, stats queue.Stats, routeFilter, targetFilter string, limit int, truncated bool, items []queue.Envelope) map[string]any {
	if limit <= 0 {
		limit = 100
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	outItems := make([]map[string]any, 0, len(items))
	for _, it := range items {
		item := map[string]any{
			"id":      it.ID,
			"route":   it.Route,
			"target":  it.Target,
			"attempt": it.Attempt,
		}
		if !it.ReceivedAt.IsZero() {
			item["received_at"] = it.ReceivedAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(it.ReceivedAt) {
				item["oldest_queued_age_seconds"] = int(now.Sub(it.ReceivedAt) / time.Second)
			}
		}
		if !it.NextRunAt.IsZero() {
			item["next_run_at"] = it.NextRunAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(it.NextRunAt) {
				item["ready_lag_seconds"] = int(now.Sub(it.NextRunAt) / time.Second)
			}
		}
		outItems = append(outItems, item)
	}

	out := map[string]any{
		"queue_total":  stats.Total,
		"queued_total": stats.ByState[queue.StateQueued],
		"limit":        limit,
		"returned":     len(outItems),
		"truncated":    truncated,
		"selector":     map[string]any{"route": routeFilter, "target": targetFilter},
		"source":       "queue_items",
		"state":        string(queue.StateQueued),
		"order":        "received_at_asc",
		"items":        outItems,
	}
	if !stats.OldestQueuedReceivedAt.IsZero() {
		out["oldest_queued_received_at"] = stats.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
		out["oldest_queued_age_seconds"] = int(stats.OldestQueuedAge / time.Second)
	}
	if !stats.EarliestQueuedNextRun.IsZero() {
		out["earliest_queued_next_run_at"] = stats.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
		out["ready_lag_seconds"] = int(stats.ReadyLag / time.Second)
	}
	return out
}

type backlogStateScanSummary struct {
	Scanned   int
	Truncated bool
}

type backlogAgePercentiles struct {
	P50 int
	P90 int
	P99 int
}

type backlogAgeWindows struct {
	LE5M      int
	GT5MLE15M int
	GT15MLE1H int
	GT1HLE6H  int
	GT6H      int
}

func backlogAgingSummaryMap(now time.Time, stats queue.Stats, routeFilter, targetFilter string, states []queue.State, limit int, truncated bool, stateScan map[queue.State]backlogStateScanSummary, items []queue.Envelope) map[string]any {
	if limit <= 0 {
		limit = 100
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
		EarliestNextRunAt time.Time
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
				Route:             it.Route,
				Target:            it.Target,
				StateCounts:       make(map[queue.State]int),
				OldestReceivedAt:  it.ReceivedAt,
				NewestReceivedAt:  it.ReceivedAt,
				EarliestNextRunAt: it.NextRunAt,
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
		if !it.NextRunAt.IsZero() && (a.EarliestNextRunAt.IsZero() || it.NextRunAt.Before(a.EarliestNextRunAt)) {
			a.EarliestNextRunAt = it.NextRunAt
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

	outItems := make([]map[string]any, 0, len(ordered))
	for _, a := range ordered {
		item := map[string]any{
			"route":            a.Route,
			"target":           a.Target,
			"total_observed":   a.TotalObserved,
			"queued_observed":  a.StateCounts[queue.StateQueued],
			"leased_observed":  a.StateCounts[queue.StateLeased],
			"dead_observed":    a.StateCounts[queue.StateDead],
			"age_sample_count": len(a.AgeSamples),
			"age_windows":      backlogAgeWindowsMap(a.AgeWindows),
			"state_counts": map[string]any{
				string(queue.StateQueued): a.StateCounts[queue.StateQueued],
				string(queue.StateLeased): a.StateCounts[queue.StateLeased],
				string(queue.StateDead):   a.StateCounts[queue.StateDead],
			},
			"ready_overdue_count": a.ReadyOverdueCount,
		}
		if p, ok := backlogAgePercentilesFromSamples(a.AgeSamples); ok {
			item["age_percentiles_seconds"] = map[string]any{
				"p50": p.P50,
				"p90": p.P90,
				"p99": p.P99,
			}
		}
		if !a.OldestReceivedAt.IsZero() {
			item["oldest_queued_received_at"] = a.OldestReceivedAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(a.OldestReceivedAt) {
				item["oldest_queued_age_seconds"] = int(now.Sub(a.OldestReceivedAt) / time.Second)
			}
		}
		if !a.NewestReceivedAt.IsZero() {
			item["newest_queued_received_at"] = a.NewestReceivedAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(a.NewestReceivedAt) {
				item["newest_queued_age_seconds"] = int(now.Sub(a.NewestReceivedAt) / time.Second)
			}
		}
		if !a.EarliestNextRunAt.IsZero() {
			item["earliest_queued_next_run_at"] = a.EarliestNextRunAt.UTC().Format(time.RFC3339Nano)
		}
		if a.MaxReadyLag > 0 {
			item["max_ready_lag_seconds"] = int(a.MaxReadyLag / time.Second)
		}
		outItems = append(outItems, item)
	}

	stateNames := make([]string, 0, len(states))
	outStateScan := make(map[string]any, len(states))
	scannedTotal := 0
	for _, st := range states {
		stateNames = append(stateNames, string(st))
		scan := stateScan[st]
		outStateScan[string(st)] = map[string]any{
			"scanned":   scan.Scanned,
			"truncated": scan.Truncated,
		}
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

	out := map[string]any{
		"queue_total":      stats.Total,
		"queued_total":     stats.ByState[queue.StateQueued],
		"limit":            limit,
		"states":           stateNames,
		"age_sample_count": len(allAgeSamples),
		"age_windows":      backlogAgeWindowsMap(allAgeWindows),
		"scanned":          scannedTotal,
		"bucket_count":     len(outItems),
		"truncated":        truncated,
		"source_bounded":   sourceBounded,
		"state_scan":       outStateScan,
		"selector":         map[string]any{"route": routeFilter, "target": targetFilter},
		"source":           "queue_items",
		"order":            "received_at_asc",
		"items":            outItems,
	}
	if p, ok := backlogAgePercentilesFromSamples(allAgeSamples); ok {
		out["age_percentiles_seconds"] = map[string]any{
			"p50": p.P50,
			"p90": p.P90,
			"p99": p.P99,
		}
	}
	if !stats.OldestQueuedReceivedAt.IsZero() {
		out["oldest_queued_received_at"] = stats.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
		out["oldest_queued_age_seconds"] = int(stats.OldestQueuedAge / time.Second)
	}
	if !stats.EarliestQueuedNextRun.IsZero() {
		out["earliest_queued_next_run_at"] = stats.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
		out["ready_lag_seconds"] = int(stats.ReadyLag / time.Second)
	}
	return out
}

func backlogTrendsMap(since, until time.Time, window, step time.Duration, routeFilter, targetFilter string, truncated bool, samples []queue.BacklogTrendSample, signalCfg queue.BacklogTrendSignalConfig) map[string]any {
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

	type trendPoint struct {
		BucketStart    time.Time
		BucketEnd      time.Time
		SampleCount    int
		LastCapturedAt time.Time
		QueuedLast     int
		LeasedLast     int
		DeadLast       int
		TotalLast      int
		QueuedMax      int
		LeasedMax      int
		DeadMax        int
		TotalMax       int
	}

	points := make([]trendPoint, 0, bucketCount)
	for i := 0; i < bucketCount; i++ {
		bucketStart := since.Add(time.Duration(i) * step)
		bucketEnd := bucketStart.Add(step)
		if bucketEnd.After(until) {
			bucketEnd = until
		}
		points = append(points, trendPoint{
			BucketStart: bucketStart,
			BucketEnd:   bucketEnd,
		})
	}

	latestTotal := 0
	maxTotal := 0
	latestCapturedAt := time.Time{}
	sampleCount := 0
	nonEmptyPointCount := 0

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
			nonEmptyPointCount++
		}
		if at.After(p.LastCapturedAt) {
			p.LastCapturedAt = at
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

	outPoints := make([]map[string]any, 0, len(points))
	for _, p := range points {
		item := map[string]any{
			"bucket_start": p.BucketStart.UTC().Format(time.RFC3339Nano),
			"bucket_end":   p.BucketEnd.UTC().Format(time.RFC3339Nano),
			"sample_count": p.SampleCount,
			"queued_last":  p.QueuedLast,
			"leased_last":  p.LeasedLast,
			"dead_last":    p.DeadLast,
			"total_last":   p.TotalLast,
			"queued_max":   p.QueuedMax,
			"leased_max":   p.LeasedMax,
			"dead_max":     p.DeadMax,
			"total_max":    p.TotalMax,
		}
		if !p.LastCapturedAt.IsZero() {
			item["last_captured_at"] = p.LastCapturedAt.UTC().Format(time.RFC3339Nano)
		}
		outPoints = append(outPoints, item)
	}

	out := map[string]any{
		"window":                window.String(),
		"step":                  step.String(),
		"since":                 since.UTC().Format(time.RFC3339Nano),
		"until":                 until.UTC().Format(time.RFC3339Nano),
		"sample_count":          sampleCount,
		"point_count":           len(outPoints),
		"non_empty_point_count": nonEmptyPointCount,
		"truncated":             truncated,
		"latest_total":          latestTotal,
		"max_total":             maxTotal,
		"selector": map[string]any{
			"route":  routeFilter,
			"target": targetFilter,
		},
		"signals": queue.AnalyzeBacklogTrendSignals(samples, truncated, queue.BacklogTrendSignalOptions{
			Now:    until,
			Window: window,
			Config: signalCfg,
		}).Map(),
		"items": outPoints,
	}
	if !latestCapturedAt.IsZero() {
		out["latest_captured_at"] = latestCapturedAt.UTC().Format(time.RFC3339Nano)
	}
	return out
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

func backlogAgeWindowsMap(w backlogAgeWindows) map[string]any {
	return map[string]any{
		"le_5m":        w.LE5M,
		"gt_5m_le_15m": w.GT5MLE15M,
		"gt_15m_le_1h": w.GT15MLE1H,
		"gt_1h_le_6h":  w.GT1HLE6H,
		"gt_6h":        w.GT6H,
	}
}

func compiledRouteTargets(r config.CompiledRoute) []string {
	if r.Pull != nil {
		return []string{"pull"}
	}
	if len(r.Deliveries) == 0 {
		return nil
	}
	targets := make([]string, 0, len(r.Deliveries))
	for _, d := range r.Deliveries {
		if strings.TrimSpace(d.URL) == "" {
			continue
		}
		targets = append(targets, d.URL)
	}
	return targets
}

func stringInSlice(s string, list []string) bool {
	for _, v := range list {
		if s == v {
			return true
		}
	}
	return false
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

func parseBacklogSummaryStatesArg(args map[string]any, key string) ([]queue.State, error) {
	raw, ok := args[key]
	if !ok || raw == nil {
		out := make([]queue.State, len(defaultBacklogSummaryStates))
		copy(out, defaultBacklogSummaryStates)
		return out, nil
	}

	tokens := make([]string, 0)
	switch v := raw.(type) {
	case string:
		tokens = append(tokens, strings.Split(v, ",")...)
	case []any:
		for i, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%s[%d] must be a string", key, i)
			}
			tokens = append(tokens, s)
		}
	default:
		return nil, fmt.Errorf("%s must be an array of states", key)
	}

	out := make([]queue.State, 0, len(tokens))
	seen := make(map[queue.State]struct{}, len(tokens))
	for _, tok := range tokens {
		state := queue.State(strings.ToLower(strings.TrimSpace(tok)))
		switch state {
		case queue.StateQueued, queue.StateLeased, queue.StateDead:
		default:
			return nil, fmt.Errorf("invalid state %q", tok)
		}
		if _, exists := seen[state]; exists {
			continue
		}
		seen[state] = struct{}{}
		out = append(out, state)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s must include at least one state", key)
	}
	return out, nil
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

func writeFileAtomic(path string, data []byte) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return errors.New("empty path")
	}

	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	mode := os.FileMode(0o600)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	} else if !os.IsNotExist(err) {
		return err
	}

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	keepTemp := false
	defer func() {
		_ = tmp.Close()
		if !keepTemp {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := tmp.Chmod(mode); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	keepTemp = true

	if err := syncDir(dir); err != nil {
		return err
	}
	return nil
}

func syncDir(dir string) error {
	if dir == "" {
		dir = "."
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		if runtime.GOOS == "windows" {
			// Windows does not support fsync on directory handles.
			return nil
		}
		return err
	}
	return nil
}
