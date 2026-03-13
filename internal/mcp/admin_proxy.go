package mcp

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/queue"
	"github.com/nuetzliches/hookaido/internal/secrets"
)

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
