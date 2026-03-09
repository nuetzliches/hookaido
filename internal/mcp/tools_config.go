package mcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/queue"
)

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
	strictSecrets, err := parseBool(args, "strict_secrets")
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
	res := config.ValidateWithResultOptions(cfg, config.ValidationOptions{
		SecretPreflight: strictSecrets,
	})
	return map[string]any{
		"ok":             res.OK,
		"path":           p,
		"errors":         res.Errors,
		"warnings":       res.Warnings,
		"strict_secrets": strictSecrets,
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

	switch backend := compiledQueueBackend(compiled); {
	case backend == "sqlite":
		store, err := s.openQueueStore()
		if err != nil {
			return false, false, false, nil
		}
		defer func() { _ = store.Close() }()
		queued, leased, err = managementRouteHasActiveBacklogInStore(store, route)
		if err != nil {
			return false, false, true, err
		}
		return queued, leased, true, nil
	case queueBackendUsesAdminProxy(backend):
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
