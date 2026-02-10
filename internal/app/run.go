package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/nuetzliches/hookaido/internal/admin"
	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/dispatcher"
	"github.com/nuetzliches/hookaido/internal/ingress"
	"github.com/nuetzliches/hookaido/internal/pullapi"
	"github.com/nuetzliches/hookaido/internal/queue"
	"github.com/nuetzliches/hookaido/internal/router"
	"github.com/nuetzliches/hookaido/internal/secrets"
)

const backlogTrendCaptureInterval = time.Minute

func run() int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "./Hookaidofile", "path to config file")
	dbPath := fs.String("db", "./hookaido.db", "path to sqlite queue db file")
	pidFile := fs.String("pid-file", "", "write process PID to file (for runtime control)")
	logLevel := fs.String("log-level", "info", "log level (debug|info|warn|error)")
	dotenvPath := fs.String("dotenv", "", "load environment variables from file (dev only)")
	watch := fs.Bool("watch", false, "watch config file for reload")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return 2
	}

	baseLogger, err := newLogger(*logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	slog.SetDefault(baseLogger)

	releasePIDFile, err := claimPIDFile(strings.TrimSpace(*pidFile))
	if err != nil {
		baseLogger.Error("pid_file_failed", slog.Any("err", err))
		return 1
	}
	defer releasePIDFile()

	if strings.TrimSpace(*dotenvPath) != "" {
		if err := loadDotenv(strings.TrimSpace(*dotenvPath)); err != nil {
			baseLogger.Error("dotenv_failed", slog.Any("err", err))
			return 1
		}
	}

	data, err := os.ReadFile(*configPath)
	if err != nil {
		baseLogger.Error("read_config_failed", slog.Any("err", err))
		return 1
	}

	cfg, err := config.Parse(data)
	if err != nil {
		baseLogger.Error("parse_config_failed", slog.Any("err", err))
		return 1
	}

	compiled, res := config.Compile(cfg)
	if !res.OK {
		baseLogger.Error("compile_config_failed", slog.String("error", config.FormatValidationText(res)))
		return 1
	}

	for _, w := range res.Warnings {
		baseLogger.Warn("config_warning", slog.String("warning", w))
	}
	baseLogger.Info("config_ok")

	runtimeLogger := baseLogger
	var runtimeLogCloser io.Closer
	if compiled.Observability.RuntimeLogDisabled {
		runtimeLogger = newDiscardLogger()
	} else if compiled.Observability.RuntimeLogSet {
		level := strings.TrimSpace(*logLevel)
		if compiled.Observability.RuntimeLogLevel != "" {
			level = compiled.Observability.RuntimeLogLevel
		}
		l, closer, err := newLoggerToSink(level, compiled.Observability.RuntimeLogOutput, compiled.Observability.RuntimeLogPath)
		if err != nil {
			baseLogger.Error("runtime_log_failed", slog.Any("err", err))
			return 1
		}
		runtimeLogger = l
		runtimeLogCloser = closer
	}
	if runtimeLogCloser != nil {
		defer func() { _ = runtimeLogCloser.Close() }()
	}
	slog.SetDefault(runtimeLogger)

	appMetrics := newRuntimeMetrics()

	var shutdownTracing func(context.Context) error
	if compiled.Observability.TracingEnabled {
		var err error
		shutdownTracing, err = initTracing(context.Background(), compiled.Observability, func(err error) {
			appMetrics.incTracingExportErrors()
			runtimeLogger.Error("tracing_export_failed", slog.Any("err", err))
		})
		if err != nil {
			appMetrics.incTracingInitFailures()
			runtimeLogger.Error("tracing_init_failed", slog.Any("err", err))
			return 1
		}
		appMetrics.setTracingEnabled(true)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownTracing(ctx)
		}()
		runtimeLogger.Info("tracing_enabled")
	}

	var accessLogger *slog.Logger
	var accessLogCloser io.Closer
	if compiled.Observability.AccessLogEnabled {
		l, closer, err := newLoggerToSink("info", compiled.Observability.AccessLogOutput, compiled.Observability.AccessLogPath)
		if err != nil {
			runtimeLogger.Error("access_log_failed", slog.Any("err", err))
			return 1
		}
		accessLogger = l
		accessLogCloser = closer
	}
	if accessLogCloser != nil {
		defer func() { _ = accessLogCloser.Close() }()
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, stop := signal.NotifyContext(rootCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		runtimeLogger.Error("load_auth_failed", slog.Any("err", err))
		return 1
	}

	running := compiled
	var store queue.Store
	var reloadMu sync.Mutex
	reloadNow := func(trigger string) {
		reloadMu.Lock()
		defer reloadMu.Unlock()

		updated, ok := reloadConfig(*configPath, running, state, runtimeLogger, trigger)
		if ok {
			running = updated
		}
	}

	upsertManagedEndpoint := func(req admin.ManagementEndpointUpsertRequest) (admin.ManagementEndpointMutationResult, error) {
		reloadMu.Lock()
		defer reloadMu.Unlock()

		result, updated, err := mutateManagedEndpointConfig(*configPath, running, state, runtimeLogger, func(cfg *config.Config, compiled config.Compiled) (admin.ManagementEndpointMutationResult, error) {
			return applyManagedEndpointUpsert(cfg, compiled, req, store)
		}, "admin_management_upsert")
		if err != nil {
			return admin.ManagementEndpointMutationResult{}, err
		}
		running = updated
		return result, nil
	}

	deleteManagedEndpoint := func(req admin.ManagementEndpointDeleteRequest) (admin.ManagementEndpointMutationResult, error) {
		reloadMu.Lock()
		defer reloadMu.Unlock()

		result, updated, err := mutateManagedEndpointConfig(*configPath, running, state, runtimeLogger, func(cfg *config.Config, compiled config.Compiled) (admin.ManagementEndpointMutationResult, error) {
			return applyManagedEndpointDelete(cfg, compiled, req, store)
		}, "admin_management_delete")
		if err != nil {
			return admin.ManagementEndpointMutationResult{}, err
		}
		running = updated
		return result, nil
	}

	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hupCh:
				reloadNow("signal_sighup")
			}
		}
	}()
	store, queueBackend, closeStore, err := newQueueStore(compiled, *dbPath)
	if err != nil {
		runtimeLogger.Error("open_queue_failed", slog.Any("err", err))
		return 1
	}
	defer func() { _ = closeStore() }()
	runtimeLogger.Info("queue_backend_selected", slog.String("backend", queueBackend))
	appMetrics.queueStore = store
	if trendStore, ok := any(store).(queue.BacklogTrendStore); ok {
		startBacklogTrendCapture(ctx, trendStore, runtimeLogger)
	}

	servers, err := startServers(store, compiled, state, runtimeLogger, accessLogger, appMetrics, upsertManagedEndpoint, deleteManagedEndpoint, cancel)
	if err != nil {
		runtimeLogger.Error("start_servers_failed", slog.Any("err", err))
		return 1
	}
	if compiled.HasDeliverRoutes {
		routes := buildDispatchRoutes(compiled)
		policy := dispatcher.EgressPolicy{
			HTTPSOnly:           compiled.Defaults.EgressPolicy.HTTPSOnly,
			Redirects:           compiled.Defaults.EgressPolicy.Redirects,
			DNSRebindProtection: compiled.Defaults.EgressPolicy.DNSRebindProtection,
			Allow:               mapEgressRules(compiled.Defaults.EgressPolicy.Allow),
			Deny:                mapEgressRules(compiled.Defaults.EgressPolicy.Deny),
		}
		client := tracingHTTPClient(compiled.Observability.TracingEnabled)
		push := dispatcher.PushDispatcher{
			Store:     store,
			Deliverer: dispatcher.NewHTTPDeliverer(client, policy),
			Routes:    routes,
			Logger:    runtimeLogger,
			ObserveAttempt: func(outcome queue.AttemptOutcome) {
				appMetrics.observeDeliveryAttempt(outcome)
			},
		}
		push.Start()
		defer func() {
			// Drain in-flight deliveries before exit. The timeout should
			// cover the longest possible delivery (timeout + retry delay).
			const drainTimeout = 15 * time.Second
			if ok := push.Drain(drainTimeout); !ok {
				runtimeLogger.Warn("dispatcher_drain_timeout", slog.Duration("timeout", drainTimeout))
			} else {
				runtimeLogger.Info("dispatcher_drained")
			}
		}()
		runtimeLogger.Info("dispatcher_started", slog.Int("routes", len(routes)))
	}
	if *watch {
		go watchConfig(ctx, *configPath, runtimeLogger, func() {
			reloadNow("watch")
		})
	}

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	for _, s := range servers {
		_ = s.Shutdown(shutdownCtx)
	}

	return 0
}

type runtimeState struct {
	mu                 sync.RWMutex
	routes             []config.CompiledRoute
	pathToRoute        map[string]string
	trendSignals       config.TrendSignalsConfig
	pullAuthorize      pullapi.Authorizer
	adminAuthorize     admin.Authorizer
	pullByRoute        map[string]pullapi.Authorizer
	basicByRoute       map[string]*ingress.BasicAuth
	forwardByRoute     map[string]*ingress.ForwardAuth
	hmacByRoute        map[string]*ingress.HMACAuth
	ingressGlobalLimit *tokenBucketLimiter
	ingressRouteLimits map[string]*tokenBucketLimiter
	now                func() time.Time
}

func newRuntimeState(compiled config.Compiled) *runtimeState {
	s := &runtimeState{
		routes:             compiled.Routes,
		pathToRoute:        compiled.PathToRoute,
		trendSignals:       compiled.Defaults.TrendSignals,
		pullByRoute:        make(map[string]pullapi.Authorizer),
		basicByRoute:       make(map[string]*ingress.BasicAuth),
		forwardByRoute:     make(map[string]*ingress.ForwardAuth),
		hmacByRoute:        make(map[string]*ingress.HMACAuth),
		ingressRouteLimits: make(map[string]*tokenBucketLimiter),
		now:                time.Now,
	}
	s.configureIngressRateLimits(compiled)
	return s
}

func (s *runtimeState) updateAll(compiled config.Compiled) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.routes = compiled.Routes
	s.pathToRoute = compiled.PathToRoute
	s.trendSignals = compiled.Defaults.TrendSignals
	s.configureIngressRateLimits(compiled)
}

func (s *runtimeState) configureIngressRateLimits(compiled config.Compiled) {
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}

	if compiled.Ingress.RateLimit.Enabled {
		s.ingressGlobalLimit = newTokenBucketLimiter(compiled.Ingress.RateLimit.RPS, compiled.Ingress.RateLimit.Burst, now)
	} else {
		s.ingressGlobalLimit = nil
	}

	routeLimits := make(map[string]*tokenBucketLimiter)
	for _, rt := range compiled.Routes {
		if !rt.RateLimit.Enabled {
			continue
		}
		routeLimits[rt.Path] = newTokenBucketLimiter(rt.RateLimit.RPS, rt.RateLimit.Burst, now)
	}
	s.ingressRouteLimits = routeLimits
}

func (s *runtimeState) allowIngress(route string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	if s.now != nil {
		now = s.now()
	}

	if limiter, ok := s.ingressRouteLimits[route]; ok {
		return limiter.AllowAt(now)
	}
	if s.ingressGlobalLimit != nil {
		return s.ingressGlobalLimit.AllowAt(now)
	}
	return true
}

func (s *runtimeState) resolveIngress(r *http.Request, requestPath string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var reqHost string
	if r != nil {
		reqHost = normalizeHost(r.Host)
	}

	var queryValues map[string][]string
	if r != nil {
		queryValues = r.URL.Query()
	}
	var remoteIP netip.Addr
	var remoteIPOK bool
	if r != nil {
		remoteIP, remoteIPOK = parseRemoteAddrIP(r.RemoteAddr)
	}

	for _, rt := range s.routes {
		if !router.MatchPath(requestPath, rt.Path) {
			continue
		}
		if !matchHosts(reqHost, rt.Match.Hosts) {
			continue
		}
		if r != nil && !matchHeaders(r.Header, rt.Match.Headers, rt.Match.HeaderExists) {
			continue
		}
		if !matchQuery(queryValues, rt.Match.Query, rt.Match.QueryExists) {
			continue
		}
		if !matchRemoteIPs(remoteIP, remoteIPOK, rt.Match.RemoteIPs) {
			continue
		}
		if r != nil && !matchMethods(r.Method, rt.Match.Methods) {
			continue
		}
		return rt.Path, true
	}
	return "", false
}

func (s *runtimeState) allowedMethodsFor(r *http.Request, requestPath string) []string {
	if r == nil {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	reqHost := normalizeHost(r.Host)
	queryValues := r.URL.Query()
	remoteIP, remoteIPOK := parseRemoteAddrIP(r.RemoteAddr)

	seen := make(map[string]struct{})
	var out []string

	for _, rt := range s.routes {
		if !router.MatchPath(requestPath, rt.Path) {
			continue
		}
		if !matchHosts(reqHost, rt.Match.Hosts) {
			continue
		}
		if !matchHeaders(r.Header, rt.Match.Headers, rt.Match.HeaderExists) {
			continue
		}
		if !matchQuery(queryValues, rt.Match.Query, rt.Match.QueryExists) {
			continue
		}
		if !matchRemoteIPs(remoteIP, remoteIPOK, rt.Match.RemoteIPs) {
			continue
		}

		methods := rt.Match.Methods
		if len(methods) == 0 {
			methods = []string{http.MethodPost}
		}
		for _, m := range methods {
			if _, ok := seen[m]; ok {
				continue
			}
			seen[m] = struct{}{}
			out = append(out, m)
		}
	}

	return out
}

func (s *runtimeState) resolvePull(endpoint string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	route, ok := s.pathToRoute[endpoint]
	return route, ok
}

func (s *runtimeState) authorizePull(r *http.Request) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	auth := s.pullAuthorize
	if r != nil && len(s.pullByRoute) > 0 {
		if endpoint := pullEndpointFromRequest(r); endpoint != "" {
			if route, ok := s.pathToRoute[endpoint]; ok {
				if ra, ok := s.pullByRoute[route]; ok && ra != nil {
					auth = ra
				}
			}
		}
	}
	if auth == nil {
		return true
	}
	return auth(r)
}

func (s *runtimeState) authorizeAdmin(r *http.Request) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	auth := s.adminAuthorize
	if auth == nil {
		return true
	}
	return auth(r)
}

func (s *runtimeState) hmacAuthFor(route string) *ingress.HMACAuth {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hmacByRoute[route]
}

func (s *runtimeState) basicAuthFor(route string) *ingress.BasicAuth {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.basicByRoute[route]
}

func (s *runtimeState) forwardAuthFor(route string) *ingress.ForwardAuth {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.forwardByRoute[route]
}

func (s *runtimeState) limitsFor(route string) (int64, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rt := range s.routes {
		if rt.Path == route {
			return rt.MaxBodyBytes, rt.MaxHeaderBytes
		}
	}
	return 0, 0
}

func (s *runtimeState) targetsFor(route string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rt := range s.routes {
		if rt.Path != route {
			continue
		}
		if len(rt.Deliveries) == 0 {
			return nil
		}
		targets := make([]string, 0, len(rt.Deliveries))
		for _, d := range rt.Deliveries {
			targets = append(targets, d.URL)
		}
		return targets
	}
	return nil
}

func (s *runtimeState) targetsForRoute(route string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rt := range s.routes {
		if rt.Path != route {
			continue
		}
		return compiledRouteTargets(rt)
	}
	return nil
}

func (s *runtimeState) modeForRoute(route string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rt := range s.routes {
		if rt.Path != route {
			continue
		}
		if rt.Pull != nil {
			return "pull"
		}
		if len(rt.Deliveries) > 0 {
			return "deliver"
		}
		return ""
	}
	return ""
}

func (s *runtimeState) publishEnabledForRoute(route string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rt := range s.routes {
		if rt.Path != route {
			continue
		}
		return rt.Publish
	}
	return true
}

func (s *runtimeState) publishDirectEnabledForRoute(route string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rt := range s.routes {
		if rt.Path != route {
			continue
		}
		return rt.PublishDirect
	}
	return true
}

func (s *runtimeState) publishManagedEnabledForRoute(route string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rt := range s.routes {
		if rt.Path != route {
			continue
		}
		return rt.PublishManaged
	}
	return true
}

func (s *runtimeState) managedRouteInfoForRoute(route string) (string, string, bool, bool) {
	route = strings.TrimSpace(route)
	if route == "" {
		return "", "", false, true
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rt := range s.routes {
		if rt.Path != route {
			continue
		}
		application := strings.TrimSpace(rt.Application)
		endpointName := strings.TrimSpace(rt.EndpointName)
		if application != "" && endpointName != "" {
			return application, endpointName, true, true
		}
		return "", "", false, true
	}
	return "", "", false, true
}

func (s *runtimeState) managedRouteSetForPolicy() (map[string]struct{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]struct{}, len(s.routes))
	for _, rt := range s.routes {
		application := strings.TrimSpace(rt.Application)
		endpointName := strings.TrimSpace(rt.EndpointName)
		if application == "" || endpointName == "" {
			continue
		}
		route := strings.TrimSpace(rt.Path)
		if route == "" {
			continue
		}
		out[route] = struct{}{}
	}
	return out, true
}

func (s *runtimeState) managementModel() admin.ManagementModel {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := admin.ManagementModel{
		RouteCount:   len(s.routes),
		Applications: []admin.ManagementApplication{},
	}

	apps := make(map[string][]admin.ManagementEndpoint)
	for _, rt := range s.routes {
		app := strings.TrimSpace(rt.Application)
		endpointName := strings.TrimSpace(rt.EndpointName)
		if app == "" || endpointName == "" {
			continue
		}

		mode := "deliver"
		if rt.Pull != nil {
			mode = "pull"
		}
		apps[app] = append(apps[app], admin.ManagementEndpoint{
			Name:    endpointName,
			Route:   rt.Path,
			Mode:    mode,
			Targets: compiledRouteTargets(rt),
			PublishPolicy: admin.ManagementEndpointPublishPolicy{
				Enabled:        rt.Publish,
				DirectEnabled:  rt.PublishDirect,
				ManagedEnabled: rt.PublishManaged,
			},
		})
	}

	appNames := make([]string, 0, len(apps))
	for app := range apps {
		appNames = append(appNames, app)
	}
	sort.Strings(appNames)

	for _, appName := range appNames {
		endpoints := apps[appName]
		sort.Slice(endpoints, func(i, j int) bool {
			if endpoints[i].Name != endpoints[j].Name {
				return endpoints[i].Name < endpoints[j].Name
			}
			return endpoints[i].Route < endpoints[j].Route
		})
		out.EndpointCount += len(endpoints)
		out.Applications = append(out.Applications, admin.ManagementApplication{
			Name:          appName,
			EndpointCount: len(endpoints),
			Endpoints:     endpoints,
		})
	}
	out.ApplicationCount = len(out.Applications)
	return out
}

func (s *runtimeState) resolveManagedEndpoint(application, endpointName string) (string, []string, bool) {
	app := strings.TrimSpace(application)
	ep := strings.TrimSpace(endpointName)
	if app == "" || ep == "" {
		return "", nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rt := range s.routes {
		if strings.TrimSpace(rt.Application) != app || strings.TrimSpace(rt.EndpointName) != ep {
			continue
		}
		return rt.Path, compiledRouteTargets(rt), true
	}
	return "", nil, false
}

func (s *runtimeState) trendSignalsConfig() config.TrendSignalsConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trendSignals
}

func compiledRouteTargets(rt config.CompiledRoute) []string {
	if rt.Pull != nil {
		return []string{"pull"}
	}
	if len(rt.Deliveries) == 0 {
		return nil
	}
	targets := make([]string, 0, len(rt.Deliveries))
	for _, d := range rt.Deliveries {
		target := strings.TrimSpace(d.URL)
		if target == "" {
			continue
		}
		targets = append(targets, target)
	}
	return targets
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

func (s *runtimeState) loadAuth(compiled config.Compiled) error {
	tokens := make([][]byte, 0, len(compiled.PullAPI.AuthTokens))
	for _, ref := range compiled.PullAPI.AuthTokens {
		b, err := secrets.LoadRef(ref)
		if err != nil {
			return fmt.Errorf("pull_api auth token %q: %w", ref, err)
		}
		tokens = append(tokens, b)
	}

	adminTokens := make([][]byte, 0, len(compiled.AdminAPI.AuthTokens))
	for _, ref := range compiled.AdminAPI.AuthTokens {
		b, err := secrets.LoadRef(ref)
		if err != nil {
			return fmt.Errorf("admin_api auth token %q: %w", ref, err)
		}
		adminTokens = append(adminTokens, b)
	}

	secretVersions := make(map[string]secrets.Version, len(compiled.Secrets))
	for id, sc := range compiled.Secrets {
		b, err := secrets.LoadRef(sc.ValueRef)
		if err != nil {
			return fmt.Errorf("secret %q value %q: %w", id, sc.ValueRef, err)
		}
		secretVersions[id] = secrets.Version{
			ID:         id,
			Value:      b,
			ValidFrom:  sc.ValidFrom,
			ValidUntil: sc.ValidUntil,
		}
	}

	pullByRoute := make(map[string]pullapi.Authorizer)
	for _, rt := range compiled.Routes {
		if rt.Pull == nil || len(rt.Pull.AuthTokens) == 0 {
			continue
		}
		routeTokens := make([][]byte, 0, len(rt.Pull.AuthTokens))
		for _, ref := range rt.Pull.AuthTokens {
			b, err := secrets.LoadRef(ref)
			if err != nil {
				return fmt.Errorf("route %q pull auth token %q: %w", rt.Path, ref, err)
			}
			routeTokens = append(routeTokens, b)
		}
		pullByRoute[rt.Path] = pullapi.BearerTokenAuthorizer(routeTokens)
	}

	hmacByRoute := make(map[string]*ingress.HMACAuth, len(compiled.Routes))
	basicByRoute := make(map[string]*ingress.BasicAuth, len(compiled.Routes))
	forwardByRoute := make(map[string]*ingress.ForwardAuth, len(compiled.Routes))
	for _, rt := range compiled.Routes {
		if len(rt.AuthBasic) > 0 {
			basicByRoute[rt.Path] = ingress.NewBasicAuth(rt.AuthBasic)
		} else {
			basicByRoute[rt.Path] = nil
		}
		if rt.AuthForward.Enabled {
			forward := ingress.NewForwardAuth(rt.AuthForward.URL)
			if rt.AuthForward.Timeout > 0 {
				forward.Timeout = rt.AuthForward.Timeout
			}
			if len(rt.AuthForward.CopyHeaders) > 0 {
				forward.CopyHeaders = append([]string(nil), rt.AuthForward.CopyHeaders...)
			}
			forward.BodyLimitBytes = rt.AuthForward.BodyLimitBytes
			forwardByRoute[rt.Path] = forward
		} else {
			forwardByRoute[rt.Path] = nil
		}

		if len(rt.AuthHMACSecrets) == 0 {
			if len(rt.AuthHMACSecretRefs) == 0 {
				hmacByRoute[rt.Path] = nil
				continue
			}
		}
		var secs [][]byte
		for _, ref := range rt.AuthHMACSecrets {
			b, err := secrets.LoadRef(ref)
			if err != nil {
				return fmt.Errorf("route %q auth hmac secret %q: %w", rt.Path, ref, err)
			}
			secs = append(secs, b)
		}

		var versions []secrets.Version
		seenRefs := make(map[string]struct{})
		for _, ref := range rt.AuthHMACSecretRefs {
			if _, ok := seenRefs[ref]; ok {
				continue
			}
			seenRefs[ref] = struct{}{}
			v, ok := secretVersions[ref]
			if !ok {
				return fmt.Errorf("route %q auth hmac secret_ref %q not found", rt.Path, ref)
			}
			versions = append(versions, v)
		}

		auth := ingress.NewHMACAuth(secs)
		if strings.TrimSpace(rt.AuthHMACSignatureHeader) != "" {
			auth.SignatureHeader = rt.AuthHMACSignatureHeader
		}
		if strings.TrimSpace(rt.AuthHMACTimestampHeader) != "" {
			auth.TimestampHeader = rt.AuthHMACTimestampHeader
		}
		if strings.TrimSpace(rt.AuthHMACNonceHeader) != "" {
			auth.NonceHeader = rt.AuthHMACNonceHeader
		}
		if rt.AuthHMACTolerance > 0 {
			auth.Tolerance = rt.AuthHMACTolerance
		}
		if len(versions) > 0 {
			set := secrets.Set{Versions: versions}
			if err := set.Validate(); err != nil {
				return fmt.Errorf("route %q auth hmac secret_ref invalid: %w", rt.Path, err)
			}
			auth.SelectSecrets = func(at time.Time) [][]byte {
				valid := set.ValidAt(at)
				out := make([][]byte, 0, len(valid)+len(secs))
				for _, v := range valid {
					out = append(out, v.Value)
				}
				if len(secs) > 0 {
					out = append(out, secs...)
				}
				return out
			}
		}
		hmacByRoute[rt.Path] = auth
	}

	s.mu.Lock()
	s.pullAuthorize = pullapi.BearerTokenAuthorizer(tokens)
	s.adminAuthorize = admin.BearerTokenAuthorizer(adminTokens)
	s.pullByRoute = pullByRoute
	s.basicByRoute = basicByRoute
	s.forwardByRoute = forwardByRoute
	s.hmacByRoute = hmacByRoute
	s.mu.Unlock()
	return nil
}

func startBacklogTrendCapture(ctx context.Context, trendStore queue.BacklogTrendStore, logger *slog.Logger) {
	if trendStore == nil {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}

	capture := func(trigger string) {
		if err := trendStore.CaptureBacklogTrendSample(time.Now().UTC()); err != nil {
			logger.Warn("backlog_trend_capture_failed", slog.Any("err", err), slog.String("trigger", trigger))
			return
		}
		logger.Debug("backlog_trend_captured", slog.String("trigger", trigger))
	}

	capture("startup")
	go func() {
		ticker := time.NewTicker(backlogTrendCaptureInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				capture("interval")
			}
		}
	}()
}

func watchConfig(ctx context.Context, path string, logger *slog.Logger, reload func()) {
	if logger == nil {
		logger = slog.Default()
	}
	if reload == nil {
		return
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Warn("watch_disabled", slog.Any("err", err))
		return
	}
	defer w.Close()

	dir := filepath.Dir(path)
	base := filepath.Base(path)
	if err := w.Add(dir); err != nil {
		logger.Warn("watch_disabled", slog.Any("err", err))
		return
	}

	logger.Info("watching_config", slog.String("path", path))

	// Debounce to coalesce bursty editor/atomic-write events.
	var timer *time.Timer
	var timerCh <-chan time.Time
	schedule := func() {
		if timer == nil {
			timer = time.NewTimer(200 * time.Millisecond)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(200 * time.Millisecond)
		}
		timerCh = timer.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-w.Events:
			if !ok {
				return
			}
			if filepath.Base(ev.Name) != base {
				continue
			}
			if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) == 0 {
				continue
			}
			schedule()
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			logger.Warn("watch_error", slog.Any("err", err))
		case <-timerCh:
			timerCh = nil
			reload()
		}
	}
}

func reloadConfig(path string, running config.Compiled, state *runtimeState, logger *slog.Logger, trigger string) (config.Compiled, bool) {
	if logger == nil {
		logger = slog.Default()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		logger.Error("config_reload_failed", slog.Any("err", err), slog.String("trigger", trigger))
		return running, false
	}
	cfg, err := config.Parse(data)
	if err != nil {
		logger.Error("config_reload_failed", slog.Any("err", err), slog.String("trigger", trigger))
		return running, false
	}
	compiled, res := config.Compile(cfg)
	if !res.OK {
		logger.Error("config_reload_failed", slog.String("error", config.FormatValidationText(res)), slog.String("trigger", trigger))
		return running, false
	}

	// Route updates are applied live where safe.
	// Listener/prefix and dispatcher-affecting changes still require a restart.
	if requiresRestartForReload(compiled, running) {
		logger.Info("config_reloaded_restart_required", slog.String("trigger", trigger))
		return running, false
	}

	if err := state.loadAuth(compiled); err != nil {
		logger.Error("config_reload_failed", slog.Any("err", err), slog.String("trigger", trigger))
		return running, false
	}
	state.updateAll(compiled)

	logger.Info("config_reloaded_ok", slog.String("trigger", trigger))
	return compiled, true
}

type managedEndpointConfigMutation func(cfg *config.Config, compiled config.Compiled) (admin.ManagementEndpointMutationResult, error)

func mutateManagedEndpointConfig(
	path string,
	running config.Compiled,
	state *runtimeState,
	logger *slog.Logger,
	mutation managedEndpointConfigMutation,
	trigger string,
) (admin.ManagementEndpointMutationResult, config.Compiled, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if mutation == nil {
		return admin.ManagementEndpointMutationResult{}, running, errors.New("missing management mutation handler")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return admin.ManagementEndpointMutationResult{}, running, err
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return admin.ManagementEndpointMutationResult{}, running, err
	}

	compiled, res := config.Compile(cfg)
	if !res.OK {
		return admin.ManagementEndpointMutationResult{}, running, errors.New(config.FormatValidationText(res))
	}

	result, err := mutation(cfg, compiled)
	if err != nil {
		return admin.ManagementEndpointMutationResult{}, running, err
	}
	if !result.Applied {
		return result, running, nil
	}

	formatted, err := config.Format(cfg)
	if err != nil {
		return admin.ManagementEndpointMutationResult{}, running, err
	}

	candidateCfg, err := config.Parse(formatted)
	if err != nil {
		return admin.ManagementEndpointMutationResult{}, running, err
	}
	if _, res := config.Compile(candidateCfg); !res.OK {
		return admin.ManagementEndpointMutationResult{}, running, errors.New(config.FormatValidationText(res))
	}

	if err := writeFileAtomic(path, formatted); err != nil {
		return admin.ManagementEndpointMutationResult{}, running, err
	}

	// Re-check invariants after file write but before reload (TOCTOU guard).
	if result.PostWriteValidate != nil {
		if err := result.PostWriteValidate(); err != nil {
			if rbErr := writeFileAtomic(path, data); rbErr != nil {
				return admin.ManagementEndpointMutationResult{}, running, fmt.Errorf("post-write validation failed (%v), rollback failed: %w", err, rbErr)
			}
			return admin.ManagementEndpointMutationResult{}, running, err
		}
	}

	updated, ok := reloadConfig(path, running, state, logger, trigger)
	if !ok {
		if rbErr := writeFileAtomic(path, data); rbErr != nil {
			return admin.ManagementEndpointMutationResult{}, running, fmt.Errorf("config mutation reload failed, rollback failed: %w", rbErr)
		}
		return admin.ManagementEndpointMutationResult{}, running, errors.New("config mutation reload failed")
	}

	return result, updated, nil
}

func applyManagedEndpointUpsert(cfg *config.Config, compiled config.Compiled, req admin.ManagementEndpointUpsertRequest, store queue.Store) (admin.ManagementEndpointMutationResult, error) {
	if cfg == nil {
		return admin.ManagementEndpointMutationResult{}, errors.New("nil config")
	}

	application := strings.TrimSpace(req.Application)
	endpointName := strings.TrimSpace(req.EndpointName)
	route := strings.TrimSpace(req.Route)
	if application == "" || endpointName == "" || route == "" {
		return admin.ManagementEndpointMutationResult{}, errors.New("application, endpoint_name and route are required")
	}

	targetIdx, ok := compiledRouteIndexByPath(compiled, route)
	if !ok || targetIdx < 0 || targetIdx >= len(cfg.Routes) {
		return admin.ManagementEndpointMutationResult{}, admin.ErrManagementRouteNotFound
	}

	target := compiled.Routes[targetIdx]
	targetApp := strings.TrimSpace(target.Application)
	targetEndpoint := strings.TrimSpace(target.EndpointName)
	if (targetApp != "" || targetEndpoint != "") && (targetApp != application || targetEndpoint != endpointName) {
		detail := fmt.Sprintf("route %q is already mapped to (%q, %q)", route, targetApp, targetEndpoint)
		return admin.ManagementEndpointMutationResult{}, fmt.Errorf(
			"%w",
			admin.NewManagementMutationError(
				admin.ErrManagementConflict,
				admin.ManagementMutationCodeRouteAlreadyMapped,
				detail,
			),
		)
	}

	currentIdx, _, hasCurrent := compiledManagedEndpointRoute(compiled, application, endpointName)
	if hasCurrent && currentIdx == targetIdx {
		return admin.ManagementEndpointMutationResult{
			Applied: false,
			Action:  "noop",
			Route:   route,
		}, nil
	}
	if hasCurrent && currentIdx >= 0 && currentIdx < len(compiled.Routes) {
		currentRoute := strings.TrimSpace(compiled.Routes[currentIdx].Path)
		queued, leased, err := managementRouteHasActiveBacklog(store, currentRoute)
		if err != nil {
			return admin.ManagementEndpointMutationResult{}, err
		}
		if queued || leased {
			return admin.ManagementEndpointMutationResult{}, managementRouteActiveBacklogError("move", currentRoute, queued, leased)
		}
	}
	if !target.Publish {
		detail := fmt.Sprintf("managed publish is disabled for route %q by route.publish", route)
		return admin.ManagementEndpointMutationResult{}, fmt.Errorf(
			"%w",
			admin.NewManagementMutationError(
				admin.ErrManagementConflict,
				admin.ManagementMutationCodeRoutePublishDisabled,
				detail,
			),
		)
	}
	if hasCurrent && currentIdx >= 0 && currentIdx < len(compiled.Routes) {
		current := compiled.Routes[currentIdx]
		if !managedEndpointRouteProfileCompatible(current, target) {
			detail := managedEndpointTargetProfileMismatchDetail(current, target)
			return admin.ManagementEndpointMutationResult{}, fmt.Errorf(
				"%w",
				admin.NewManagementMutationError(
					admin.ErrManagementConflict,
					admin.ManagementMutationCodeRouteTargetMismatch,
					detail,
				),
			)
		}
	}
	if !target.PublishManaged {
		detail := fmt.Sprintf("managed publish is disabled for route %q by route.publish.managed", route)
		return admin.ManagementEndpointMutationResult{}, fmt.Errorf(
			"%w",
			admin.NewManagementMutationError(
				admin.ErrManagementConflict,
				admin.ManagementMutationCodeRoutePublishDisabled,
				detail,
			),
		)
	}

	if hasCurrent && currentIdx >= 0 && currentIdx < len(cfg.Routes) {
		clearRouteManagedEndpoint(&cfg.Routes[currentIdx])
	}
	setRouteManagedEndpoint(&cfg.Routes[targetIdx], application, endpointName)

	action := "created"
	if hasCurrent {
		action = "updated"
	}

	// Re-check that no backlog appeared on the old route between the initial
	// check and the config file write (TOCTOU guard against concurrent ingress).
	var postWriteValidate func() error
	if hasCurrent && currentIdx >= 0 && currentIdx < len(compiled.Routes) {
		currentRoute := strings.TrimSpace(compiled.Routes[currentIdx].Path)
		postWriteValidate = func() error {
			queued, leased, err := managementRouteHasActiveBacklog(store, currentRoute)
			if err != nil {
				return err
			}
			if queued || leased {
				return managementRouteActiveBacklogError("move", currentRoute, queued, leased)
			}
			return nil
		}
	}

	return admin.ManagementEndpointMutationResult{
		Applied:           true,
		Action:            action,
		Route:             route,
		PostWriteValidate: postWriteValidate,
	}, nil
}

func applyManagedEndpointDelete(cfg *config.Config, compiled config.Compiled, req admin.ManagementEndpointDeleteRequest, store queue.Store) (admin.ManagementEndpointMutationResult, error) {
	if cfg == nil {
		return admin.ManagementEndpointMutationResult{}, errors.New("nil config")
	}

	application := strings.TrimSpace(req.Application)
	endpointName := strings.TrimSpace(req.EndpointName)
	if application == "" || endpointName == "" {
		return admin.ManagementEndpointMutationResult{}, errors.New("application and endpoint_name are required")
	}

	currentIdx, route, ok := compiledManagedEndpointRoute(compiled, application, endpointName)
	if !ok || currentIdx < 0 || currentIdx >= len(cfg.Routes) {
		return admin.ManagementEndpointMutationResult{}, admin.ErrManagementEndpointNotFound
	}
	queued, leased, err := managementRouteHasActiveBacklog(store, route)
	if err != nil {
		return admin.ManagementEndpointMutationResult{}, err
	}
	if queued || leased {
		return admin.ManagementEndpointMutationResult{}, managementRouteActiveBacklogError("delete", route, queued, leased)
	}

	clearRouteManagedEndpoint(&cfg.Routes[currentIdx])

	// Re-check backlog on the deleted route between initial check and file write.
	postWriteValidate := func() error {
		q, l, err := managementRouteHasActiveBacklog(store, route)
		if err != nil {
			return err
		}
		if q || l {
			return managementRouteActiveBacklogError("delete", route, q, l)
		}
		return nil
	}

	return admin.ManagementEndpointMutationResult{
		Applied:           true,
		Action:            "deleted",
		Route:             route,
		PostWriteValidate: postWriteValidate,
	}, nil
}

func managementRouteHasActiveBacklog(store queue.Store, route string) (queued, leased bool, err error) {
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

func managementRouteActiveBacklogError(operation, route string, queued, leased bool) error {
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
	return fmt.Errorf(
		"%w",
		admin.NewManagementMutationError(
			admin.ErrManagementConflict,
			admin.ManagementMutationCodeRouteBacklogActive,
			detail,
		),
	)
}

func compiledRouteIndexByPath(compiled config.Compiled, route string) (int, bool) {
	route = strings.TrimSpace(route)
	if route == "" {
		return -1, false
	}
	for i, rt := range compiled.Routes {
		if rt.Path == route {
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

	for i, rt := range compiled.Routes {
		if strings.TrimSpace(rt.Application) != application || strings.TrimSpace(rt.EndpointName) != endpointName {
			continue
		}
		return i, rt.Path, true
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
	left = normalizeTargetSet(left)
	right = normalizeTargetSet(right)
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

func normalizeTargetSet(targets []string) []string {
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

func formatAllowedTargetsForError(allowedTargets []string) string {
	normalized := normalizeTargetSet(allowedTargets)
	if len(normalized) == 0 {
		return "(none)"
	}
	quoted := make([]string, 0, len(normalized))
	for _, target := range normalized {
		quoted = append(quoted, strconv.Quote(target))
	}
	return strings.Join(quoted, ", ")
}

func setRouteManagedEndpoint(route *config.Route, application, endpointName string) {
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

func clearRouteManagedEndpoint(route *config.Route) {
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

func claimPIDFile(pidFile string) (func(), error) {
	pidFile = strings.TrimSpace(pidFile)
	if pidFile == "" {
		return func() {}, nil
	}

	if err := os.MkdirAll(filepath.Dir(pidFile), 0o755); err != nil {
		return nil, err
	}

	if pid, err := readPIDFile(pidFile); err == nil && pid > 0 {
		if pidRunning(pid) {
			return nil, fmt.Errorf("pid file %q points to running process %d", pidFile, pid)
		}
	}

	pid := os.Getpid()
	if err := writePIDFile(pidFile, pid); err != nil {
		return nil, err
	}

	return func() {
		cur, err := readPIDFile(pidFile)
		if err != nil {
			return
		}
		if cur == pid {
			_ = os.Remove(pidFile)
		}
	}, nil
}

func writePIDFile(pidFile string, pid int) error {
	tmp, err := os.CreateTemp(filepath.Dir(pidFile), "."+filepath.Base(pidFile)+".tmp-*")
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

	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(tmp, "%d\n", pid); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, pidFile); err != nil {
		return err
	}
	keepTemp = true
	return nil
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

	return syncDir(dir)
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

func readPIDFile(pidFile string) (int, error) {
	b, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, err
	}
	raw := strings.TrimSpace(string(b))
	if raw == "" {
		return 0, fmt.Errorf("pid file %q is empty", pidFile)
	}
	pid, err := strconv.Atoi(raw)
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("pid file %q contains invalid pid %q", pidFile, raw)
	}
	return pid, nil
}

func pidRunning(pid int) bool {
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

func startServers(
	store queue.Store,
	compiled config.Compiled,
	state *runtimeState,
	runtimeLogger *slog.Logger,
	accessLogger *slog.Logger,
	appMetrics *runtimeMetrics,
	upsertManagedEndpoint func(req admin.ManagementEndpointUpsertRequest) (admin.ManagementEndpointMutationResult, error),
	deleteManagedEndpoint func(req admin.ManagementEndpointDeleteRequest) (admin.ManagementEndpointMutationResult, error),
	cancel context.CancelFunc,
) ([]*http.Server, error) {
	if runtimeLogger == nil {
		runtimeLogger = slog.Default()
	}

	ing := ingress.NewServer(store)
	ing.ResolveRoute = state.resolveIngress
	ing.AllowedMethodsFor = state.allowedMethodsFor
	ing.AllowRequestFor = state.allowIngress
	ing.BasicAuthFor = state.basicAuthFor
	ing.ForwardAuthFor = state.forwardAuthFor
	ing.HMACAuthFor = state.hmacAuthFor
	ing.LimitsFor = state.limitsFor
	ing.TargetsFor = state.targetsFor
	ing.MaxBodyBytes = compiled.Defaults.MaxBodyBytes
	ing.MaxHeaderBytes = compiled.Defaults.MaxHeaderBytes
	ing.ObserveResult = func(accepted bool, enqueued int) {
		appMetrics.observeIngressResult(accepted, enqueued)
	}

	pullHandler := pullapi.NewServer(store)
	pullHandler.ResolveRoute = state.resolvePull
	pullHandler.Authorize = state.authorizePull
	if compiled.PullAPI.MaxBatch > 0 {
		pullHandler.MaxBatch = compiled.PullAPI.MaxBatch
	}
	if compiled.PullAPI.DefaultLeaseTTL > 0 {
		pullHandler.DefaultLeaseTTL = compiled.PullAPI.DefaultLeaseTTL
	}
	pullHandler.MaxLeaseTTL = compiled.PullAPI.MaxLeaseTTL
	pullHandler.DefaultMaxWait = compiled.PullAPI.DefaultMaxWait
	pullHandler.MaxWait = compiled.PullAPI.MaxWait

	adminH := admin.NewServer(store)
	adminH.Authorize = state.authorizeAdmin
	adminH.ResolveTrendSignalConfig = func() queue.BacklogTrendSignalConfig {
		return queueTrendSignalConfigFromCompiled(state.trendSignalsConfig())
	}
	adminH.ResolveManaged = state.resolveManagedEndpoint
	adminH.ManagedRouteInfoForRoute = state.managedRouteInfoForRoute
	adminH.ManagedRouteSet = state.managedRouteSetForPolicy
	adminH.TargetsForRoute = state.targetsForRoute
	adminH.ModeForRoute = state.modeForRoute
	adminH.PublishEnabledForRoute = state.publishEnabledForRoute
	adminH.PublishDirectEnabledForRoute = state.publishDirectEnabledForRoute
	adminH.PublishManagedEnabledForRoute = state.publishManagedEnabledForRoute
	adminH.LimitsForRoute = state.limitsFor
	adminH.MaxBodyBytes = compiled.Defaults.MaxBodyBytes
	adminH.MaxHeaderBytes = compiled.Defaults.MaxHeaderBytes
	adminH.PublishGlobalDirectEnabled = compiled.Defaults.PublishPolicy.DirectEnabled
	adminH.PublishScopedManagedEnabled = compiled.Defaults.PublishPolicy.ManagedEnabled
	adminH.PublishAllowPullRoutes = compiled.Defaults.PublishPolicy.AllowPullRoutes
	adminH.PublishAllowDeliverRoutes = compiled.Defaults.PublishPolicy.AllowDeliverRoutes
	adminH.PublishRequireAuditActor = compiled.Defaults.PublishPolicy.RequireActor
	adminH.PublishRequireAuditRequestID = compiled.Defaults.PublishPolicy.RequireRequestID
	adminH.PublishScopedManagedFailClosed = compiled.Defaults.PublishPolicy.FailClosed
	adminH.PublishScopedManagedActorAllowlist = append([]string(nil), compiled.Defaults.PublishPolicy.ActorAllowlist...)
	adminH.PublishScopedManagedActorPrefixes = append([]string(nil), compiled.Defaults.PublishPolicy.ActorPrefixes...)
	adminH.ManagementModel = state.managementModel
	adminH.UpsertManagedEndpoint = upsertManagedEndpoint
	adminH.DeleteManagedEndpoint = deleteManagedEndpoint
	adminH.AuditManagementMutation = func(event admin.ManagementMutationAuditEvent) {
		runtimeLogger.Info(
			"admin_management_mutation",
			slog.String("operation", event.Operation),
			slog.String("application", event.Application),
			slog.String("endpoint_name", event.EndpointName),
			slog.String("route", event.Route),
			slog.String("target", event.Target),
			slog.String("state", event.State),
			slog.Int("limit", event.Limit),
			slog.Bool("preview_only", event.PreviewOnly),
			slog.Int("matched", event.Matched),
			slog.Int("changed", event.Changed),
			slog.String("reason", event.Reason),
			slog.String("actor", event.Actor),
			slog.String("request_id", event.RequestID),
			slog.Time("at", event.At),
		)
	}
	adminH.ObservePublishResult = func(event admin.PublishResultEvent) {
		if appMetrics == nil {
			return
		}
		appMetrics.observePublishResult(event.Accepted, event.Rejected, event.Code, event.Scoped)
	}
	if appMetrics != nil {
		adminH.HealthDiagnostics = appMetrics.healthDiagnostics
	}

	ingressLn, err := listenWithTLS(compiled.Ingress.Listen, compiled.Ingress.TLS)
	if err != nil {
		return nil, fmt.Errorf("ingress listen %q: %w", compiled.Ingress.Listen, err)
	}
	ingressHandler := http.Handler(ing)
	ingressHandler = wrapTracingHandler(compiled.Observability.TracingEnabled, "ingress", ingressHandler)
	if accessLogger != nil {
		ingressHandler = withAccessLog(accessLogger.With(slog.String("component", "ingress")), ingressHandler)
	}
	ingressSrv := &http.Server{
		Addr:    compiled.Ingress.Listen,
		Handler: ingressHandler,
	}

	type serverEntry struct {
		name string
		srv  *http.Server
		ln   net.Listener
	}
	var entries []serverEntry
	entries = append(entries, serverEntry{name: "ingress", srv: ingressSrv, ln: ingressLn})

	closeEntries := func() {
		for _, e := range entries {
			_ = e.ln.Close()
		}
	}

	if compiled.SharedListener {
		sharedLn, err := listenWithTLS(compiled.PullAPI.Listen, compiled.PullAPI.TLS)
		if err != nil {
			closeEntries()
			return nil, fmt.Errorf("pull/admin listen %q: %w", compiled.PullAPI.Listen, err)
		}
		if compiled.HasPullRoutes {
			pullMounted := mountPrefix(compiled.PullAPI.Prefix, pullHandler)
			adminMounted := mountPrefix(compiled.AdminAPI.Prefix, adminH)
			pullMounted = wrapTracingHandler(compiled.Observability.TracingEnabled, "pull_api", pullMounted)
			adminMounted = wrapTracingHandler(compiled.Observability.TracingEnabled, "admin_api", adminMounted)
			if accessLogger != nil {
				pullMounted = withAccessLog(accessLogger.With(slog.String("component", "pull_api")), pullMounted)
				adminMounted = withAccessLog(accessLogger.With(slog.String("component", "admin_api")), adminMounted)
			}
			shared := &http.Server{
				Addr: compiled.PullAPI.Listen,
				Handler: sharedPrefixMux(
					compiled.PullAPI.Prefix, pullMounted,
					compiled.AdminAPI.Prefix, adminMounted,
				),
			}
			entries = append(entries, serverEntry{name: "pull+admin", srv: shared, ln: sharedLn})
		} else {
			adminMounted := mountPrefix(compiled.AdminAPI.Prefix, adminH)
			adminMounted = wrapTracingHandler(compiled.Observability.TracingEnabled, "admin_api", adminMounted)
			if accessLogger != nil {
				adminMounted = withAccessLog(accessLogger.With(slog.String("component", "admin_api")), adminMounted)
			}
			adminSrv := &http.Server{
				Addr:    compiled.AdminAPI.Listen,
				Handler: adminMounted,
			}
			entries = append(entries, serverEntry{name: "admin_api", srv: adminSrv, ln: sharedLn})
		}
	} else {
		if compiled.HasPullRoutes {
			pullLn, err := listenWithTLS(compiled.PullAPI.Listen, compiled.PullAPI.TLS)
			if err != nil {
				closeEntries()
				return nil, fmt.Errorf("pull_api listen %q: %w", compiled.PullAPI.Listen, err)
			}
			pullSrv := &http.Server{
				Addr:    compiled.PullAPI.Listen,
				Handler: mountPrefix(compiled.PullAPI.Prefix, pullHandler),
			}
			pullSrv.Handler = wrapTracingHandler(compiled.Observability.TracingEnabled, "pull_api", pullSrv.Handler)
			if accessLogger != nil {
				pullSrv.Handler = withAccessLog(accessLogger.With(slog.String("component", "pull_api")), pullSrv.Handler)
			}
			entries = append(entries, serverEntry{name: "pull_api", srv: pullSrv, ln: pullLn})
		}

		adminLn, err := listenWithTLS(compiled.AdminAPI.Listen, compiled.AdminAPI.TLS)
		if err != nil {
			closeEntries()
			return nil, fmt.Errorf("admin_api listen %q: %w", compiled.AdminAPI.Listen, err)
		}
		adminSrv := &http.Server{
			Addr:    compiled.AdminAPI.Listen,
			Handler: mountPrefix(compiled.AdminAPI.Prefix, adminH),
		}
		adminSrv.Handler = wrapTracingHandler(compiled.Observability.TracingEnabled, "admin_api", adminSrv.Handler)
		if accessLogger != nil {
			adminSrv.Handler = withAccessLog(accessLogger.With(slog.String("component", "admin_api")), adminSrv.Handler)
		}
		entries = append(entries, serverEntry{name: "admin_api", srv: adminSrv, ln: adminLn})
	}

	servers := make([]*http.Server, 0, len(entries))
	for _, e := range entries {
		servers = append(servers, e.srv)
		serveOnListener(runtimeLogger, e.name, e.srv, e.ln, cancel)
	}

	if compiled.Observability.Metrics.Enabled {
		metricsLn, err := net.Listen("tcp", compiled.Observability.Metrics.Listen)
		if err != nil {
			closeEntries()
			return nil, fmt.Errorf("metrics listen %q: %w", compiled.Observability.Metrics.Listen, err)
		}
		metricsHandler := mountPrefix(compiled.Observability.Metrics.Prefix, newMetricsHandler(version, time.Now(), appMetrics))
		metricsHandler = wrapTracingHandler(compiled.Observability.TracingEnabled, "metrics", metricsHandler)
		if accessLogger != nil {
			metricsHandler = withAccessLog(accessLogger.With(slog.String("component", "metrics")), metricsHandler)
		}
		metricsSrv := &http.Server{
			Addr:    compiled.Observability.Metrics.Listen,
			Handler: metricsHandler,
		}
		servers = append(servers, metricsSrv)
		serveOnListener(runtimeLogger, "metrics", metricsSrv, metricsLn, cancel)
	}

	runtimeLogger.Info("ingress_listening", slog.String("addr", compiled.Ingress.Listen))
	if compiled.SharedListener {
		if compiled.HasPullRoutes {
			runtimeLogger.Info("pull_admin_listening",
				slog.String("addr", compiled.PullAPI.Listen),
				slog.String("pull_prefix", compiled.PullAPI.Prefix),
				slog.String("admin_prefix", compiled.AdminAPI.Prefix),
			)
		} else {
			runtimeLogger.Info("admin_api_listening",
				slog.String("addr", compiled.AdminAPI.Listen),
				slog.String("prefix", compiled.AdminAPI.Prefix),
			)
		}
	} else {
		if compiled.HasPullRoutes {
			runtimeLogger.Info("pull_api_listening", slog.String("addr", compiled.PullAPI.Listen), slog.String("prefix", compiled.PullAPI.Prefix))
		}
		runtimeLogger.Info("admin_api_listening", slog.String("addr", compiled.AdminAPI.Listen), slog.String("prefix", compiled.AdminAPI.Prefix))
	}
	if compiled.Observability.Metrics.Enabled {
		runtimeLogger.Info("metrics_listening",
			slog.String("addr", compiled.Observability.Metrics.Listen),
			slog.String("prefix", compiled.Observability.Metrics.Prefix),
		)
	}

	return servers, nil
}

func mountPrefix(prefix string, next http.Handler) http.Handler {
	if prefix == "" {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !hasPathPrefix(r.URL.Path, prefix) {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		r2 := r.Clone(r.Context())
		r2.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
		if r2.URL.Path == "" {
			r2.URL.Path = "/"
		}
		if !strings.HasPrefix(r2.URL.Path, "/") {
			r2.URL.Path = "/" + r2.URL.Path
		}
		next.ServeHTTP(w, r2)
	})
}

func sharedPrefixMux(pullPrefix string, pull http.Handler, adminPrefix string, admin http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hasPathPrefix(r.URL.Path, pullPrefix) {
			pull.ServeHTTP(w, r)
			return
		}
		if hasPathPrefix(r.URL.Path, adminPrefix) {
			admin.ServeHTTP(w, r)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
}

func hasPathPrefix(p, prefix string) bool {
	if prefix == "" {
		return true
	}
	if p == prefix {
		return true
	}
	if strings.HasPrefix(p, prefix+"/") {
		return true
	}
	return false
}

func listenWithTLS(addr string, tlsCfg config.TLSConfig) (net.Listener, error) {
	if !tlsCfg.Enabled {
		return net.Listen("tcp", addr)
	}
	cfg, err := buildTLSConfig(tlsCfg)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return tls.NewListener(ln, cfg), nil
}

func newQueueStore(compiled config.Compiled, dbPath string) (queue.Store, string, func() error, error) {
	backend := queueBackendForCompiled(compiled)
	switch backend {
	case "sqlite":
		store, err := queue.NewSQLiteStore(
			dbPath,
			queue.WithSQLiteQueueLimits(compiled.QueueLimits.MaxDepth, compiled.QueueLimits.DropPolicy),
			queue.WithSQLiteRetention(compiled.QueueRetention.MaxAge, compiled.QueueRetention.PruneInterval),
			queue.WithSQLiteDeliveredRetention(compiled.DeliveredRetention.MaxAge),
			queue.WithSQLiteDLQRetention(compiled.DLQRetention.MaxAge, compiled.DLQRetention.MaxDepth),
		)
		if err != nil {
			return nil, backend, nil, err
		}
		return store, backend, store.Close, nil
	case "memory":
		store := queue.NewMemoryStore(
			queue.WithQueueLimits(compiled.QueueLimits.MaxDepth, compiled.QueueLimits.DropPolicy),
			queue.WithQueueRetention(compiled.QueueRetention.MaxAge, compiled.QueueRetention.PruneInterval),
			queue.WithDeliveredRetention(compiled.DeliveredRetention.MaxAge),
			queue.WithDLQRetention(compiled.DLQRetention.MaxAge, compiled.DLQRetention.MaxDepth),
		)
		return store, backend, func() error { return nil }, nil
	case "mixed":
		return nil, backend, nil, errors.New("mixed route queue backends are not supported yet")
	default:
		return nil, backend, nil, fmt.Errorf("unsupported queue backend %q", backend)
	}
}

func queueBackendForCompiled(compiled config.Compiled) string {
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

func buildTLSConfig(tlsCfg config.TLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsCfg.ClientAuth,
	}
	if tlsCfg.ClientCA != "" {
		b, err := os.ReadFile(tlsCfg.ClientCA)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("failed to parse client_ca %q", tlsCfg.ClientCA)
		}
		cfg.ClientCAs = pool
	}
	return cfg, nil
}

func normalizeHost(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	host = strings.ToLower(host)
	host = strings.TrimSuffix(host, ".")

	if strings.HasPrefix(host, "[") {
		if h, _, err := net.SplitHostPort(host); err == nil {
			return strings.Trim(h, "[]")
		}
		return strings.Trim(host, "[]")
	}
	if strings.Count(host, ":") > 1 {
		return strings.Trim(host, "[]")
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

func mapEgressRules(rules []config.EgressRule) []dispatcher.EgressRule {
	if len(rules) == 0 {
		return nil
	}
	out := make([]dispatcher.EgressRule, 0, len(rules))
	for _, r := range rules {
		out = append(out, dispatcher.EgressRule{
			Host:       r.Host,
			Subdomains: r.Subdomains,
			CIDR:       r.CIDR,
			IsCIDR:     r.IsCIDR,
		})
	}
	return out
}

func pullEndpointFromRequest(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}
	cleanPath := path.Clean(r.URL.Path)
	op := path.Base(cleanPath)
	endpoint := strings.TrimSuffix(cleanPath, "/"+op)
	if endpoint == "" {
		endpoint = "/"
	}
	return endpoint
}

func matchHosts(requestHost string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	if requestHost == "" {
		return false
	}
	for _, h := range allowed {
		if h == "*" {
			return true
		}
		if requestHost == h {
			return true
		}
		if strings.HasPrefix(h, "*.") {
			suffix := strings.TrimPrefix(h, "*.")
			if suffix == "" || requestHost == suffix {
				continue
			}
			if strings.HasSuffix(requestHost, "."+suffix) {
				return true
			}
		}
	}
	return false
}

func matchMethods(method string, allowed []string) bool {
	if method == "" {
		return false
	}
	if len(allowed) == 0 {
		return method == http.MethodPost
	}
	for _, m := range allowed {
		if method == m {
			return true
		}
	}
	return false
}

func matchHeaders(h http.Header, expected []config.HeaderMatchConfig, required []string) bool {
	if len(expected) == 0 && len(required) == 0 {
		return true
	}
	for _, name := range required {
		if len(h.Values(name)) == 0 {
			return false
		}
	}
	for _, m := range expected {
		values := h.Values(m.Name)
		if len(values) == 0 {
			return false
		}
		if !matchHeaderValues(values, m.Value) {
			return false
		}
	}
	return true
}

func matchHeaderValues(values []string, expected string) bool {
	for _, v := range values {
		if v == expected {
			return true
		}
		for _, part := range strings.Split(v, ",") {
			if strings.TrimSpace(part) == expected {
				return true
			}
		}
	}
	return false
}

func matchQuery(values map[string][]string, expected []config.QueryMatchConfig, required []string) bool {
	if len(expected) == 0 && len(required) == 0 {
		return true
	}
	if values == nil {
		return false
	}
	for _, name := range required {
		v, ok := values[name]
		if !ok || len(v) == 0 {
			return false
		}
	}
	for _, m := range expected {
		v, ok := values[m.Name]
		if !ok {
			return false
		}
		if !matchQueryValues(v, m.Value) {
			return false
		}
	}
	return true
}

func matchQueryValues(values []string, expected string) bool {
	for _, v := range values {
		if v == expected {
			return true
		}
	}
	return false
}

func parseRemoteAddrIP(remoteAddr string) (netip.Addr, bool) {
	raw := strings.TrimSpace(remoteAddr)
	if raw == "" {
		return netip.Addr{}, false
	}

	if host, _, err := net.SplitHostPort(raw); err == nil {
		raw = host
	}
	raw = strings.Trim(raw, "[]")
	if raw == "" {
		return netip.Addr{}, false
	}

	ip, err := netip.ParseAddr(raw)
	if err != nil {
		return netip.Addr{}, false
	}
	return ip.Unmap(), true
}

func matchRemoteIPs(remoteIP netip.Addr, remoteIPOK bool, allowed []netip.Prefix) bool {
	if len(allowed) == 0 {
		return true
	}
	if !remoteIPOK {
		return false
	}
	for _, pfx := range allowed {
		if pfx.Contains(remoteIP) {
			return true
		}
	}
	return false
}

func observabilityEqual(a, b config.ObservabilityConfig) bool {
	if a.AccessLogEnabled != b.AccessLogEnabled ||
		a.AccessLogOutput != b.AccessLogOutput ||
		a.AccessLogPath != b.AccessLogPath {
		return false
	}
	if a.RuntimeLogLevel != b.RuntimeLogLevel ||
		a.RuntimeLogDisabled != b.RuntimeLogDisabled ||
		a.RuntimeLogOutput != b.RuntimeLogOutput ||
		a.RuntimeLogPath != b.RuntimeLogPath ||
		a.RuntimeLogSet != b.RuntimeLogSet {
		return false
	}
	if a.TracingEnabled != b.TracingEnabled ||
		a.TracingCollector != b.TracingCollector ||
		a.TracingURLPath != b.TracingURLPath ||
		a.TracingCompression != b.TracingCompression ||
		a.TracingInsecure != b.TracingInsecure ||
		a.TracingTimeout != b.TracingTimeout ||
		a.TracingTimeoutSet != b.TracingTimeoutSet ||
		a.TracingProxyURL != b.TracingProxyURL ||
		a.TracingTLSCAFile != b.TracingTLSCAFile ||
		a.TracingTLSCertFile != b.TracingTLSCertFile ||
		a.TracingTLSKeyFile != b.TracingTLSKeyFile ||
		a.TracingTLSServerName != b.TracingTLSServerName ||
		a.TracingTLSInsecureSkipVerify != b.TracingTLSInsecureSkipVerify ||
		!tracingRetryEqual(a.TracingRetry, b.TracingRetry) ||
		!tracingHeadersEqual(a.TracingHeaders, b.TracingHeaders) {
		return false
	}
	if a.Metrics.Enabled != b.Metrics.Enabled {
		return false
	}
	if !a.Metrics.Enabled {
		return true
	}
	return a.Metrics.Listen == b.Metrics.Listen && a.Metrics.Prefix == b.Metrics.Prefix
}

func tracingHeadersEqual(a, b []config.TracingHeaderConfig) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}

func tracingRetryEqual(a, b *config.TracingRetryConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Enabled == b.Enabled &&
		a.InitialInterval == b.InitialInterval &&
		a.MaxInterval == b.MaxInterval &&
		a.MaxElapsedTime == b.MaxElapsedTime
}

func queueLimitsEqual(a, b config.QueueLimitsConfig) bool {
	return a.MaxDepth == b.MaxDepth && a.DropPolicy == b.DropPolicy
}

func queueRetentionEqual(a, b config.QueueRetentionConfig) bool {
	if a.Enabled != b.Enabled {
		return false
	}
	return a.MaxAge == b.MaxAge && a.PruneInterval == b.PruneInterval
}

func deliveredRetentionEqual(a, b config.DeliveredRetentionConfig) bool {
	if a.Enabled != b.Enabled {
		return false
	}
	return a.MaxAge == b.MaxAge
}

func dlqRetentionEqual(a, b config.DLQRetentionConfig) bool {
	if a.Enabled != b.Enabled {
		return false
	}
	return a.MaxAge == b.MaxAge && a.MaxDepth == b.MaxDepth
}

func requiresRestartForReload(compiled, running config.Compiled) bool {
	if compiled.SharedListener != running.SharedListener ||
		compiled.HasPullRoutes != running.HasPullRoutes ||
		compiled.HasDeliverRoutes != running.HasDeliverRoutes ||
		queueBackendForCompiled(compiled) != queueBackendForCompiled(running) ||
		compiled.Ingress.Listen != running.Ingress.Listen ||
		compiled.Ingress.TLS != running.Ingress.TLS ||
		compiled.PullAPI.Prefix != running.PullAPI.Prefix ||
		compiled.AdminAPI.Prefix != running.AdminAPI.Prefix ||
		compiled.PullAPI.Listen != running.PullAPI.Listen ||
		compiled.AdminAPI.Listen != running.AdminAPI.Listen ||
		compiled.PullAPI.MaxBatch != running.PullAPI.MaxBatch ||
		compiled.PullAPI.DefaultLeaseTTL != running.PullAPI.DefaultLeaseTTL ||
		compiled.PullAPI.MaxLeaseTTL != running.PullAPI.MaxLeaseTTL ||
		compiled.PullAPI.DefaultMaxWait != running.PullAPI.DefaultMaxWait ||
		compiled.PullAPI.MaxWait != running.PullAPI.MaxWait ||
		compiled.PullAPI.TLS != running.PullAPI.TLS ||
		compiled.AdminAPI.TLS != running.AdminAPI.TLS ||
		compiled.Defaults.MaxBodyBytes != running.Defaults.MaxBodyBytes ||
		compiled.Defaults.MaxHeaderBytes != running.Defaults.MaxHeaderBytes ||
		!publishPolicyEqual(compiled.Defaults.PublishPolicy, running.Defaults.PublishPolicy) ||
		!observabilityEqual(compiled.Observability, running.Observability) ||
		!queueLimitsEqual(compiled.QueueLimits, running.QueueLimits) ||
		!queueRetentionEqual(compiled.QueueRetention, running.QueueRetention) ||
		!deliveredRetentionEqual(compiled.DeliveredRetention, running.DeliveredRetention) ||
		!dlqRetentionEqual(compiled.DLQRetention, running.DLQRetention) {
		return true
	}

	if !dispatcherConfigEqual(compiled, running) {
		return true
	}

	return false
}

func dispatcherConfigEqual(a, b config.Compiled) bool {
	if a.HasDeliverRoutes != b.HasDeliverRoutes {
		return false
	}
	if !a.HasDeliverRoutes {
		return true
	}
	if !egressPolicyEqual(a.Defaults.EgressPolicy, b.Defaults.EgressPolicy) {
		return false
	}
	return dispatchRoutesEqual(buildDispatchRoutes(a), buildDispatchRoutes(b))
}

func egressPolicyEqual(a, b config.EgressPolicyConfig) bool {
	if a.HTTPSOnly != b.HTTPSOnly ||
		a.Redirects != b.Redirects ||
		a.DNSRebindProtection != b.DNSRebindProtection {
		return false
	}
	if !egressRulesEqual(a.Allow, b.Allow) {
		return false
	}
	if !egressRulesEqual(a.Deny, b.Deny) {
		return false
	}
	return true
}

func egressRulesEqual(a, b []config.EgressRule) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Host != b[i].Host ||
			a[i].Subdomains != b[i].Subdomains ||
			a[i].IsCIDR != b[i].IsCIDR ||
			a[i].CIDR != b[i].CIDR {
			return false
		}
	}
	return true
}

func dispatchRoutesEqual(a, b []dispatcher.RouteConfig) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Route != b[i].Route || a[i].Concurrency != b[i].Concurrency {
			return false
		}
		if !dispatchTargetsEqual(a[i].Targets, b[i].Targets) {
			return false
		}
	}
	return true
}

func dispatchTargetsEqual(a, b []dispatcher.TargetConfig) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].URL != b[i].URL ||
			a[i].Timeout != b[i].Timeout ||
			!retryConfigEqual(a[i].Retry, b[i].Retry) ||
			!hmacSigningConfigEqual(a[i].SignHMAC, b[i].SignHMAC) {
			return false
		}
	}
	return true
}

func hmacSigningConfigEqual(a, b *dispatcher.HMACSigningConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.SecretRef == b.SecretRef &&
		hmacSigningSecretVersionsEqual(a.SecretVersions, b.SecretVersions) &&
		a.SecretSelection == b.SecretSelection &&
		a.SignatureHeader == b.SignatureHeader &&
		a.TimestampHeader == b.TimestampHeader
}

func hmacSigningSecretVersionsEqual(a, b []dispatcher.HMACSigningSecretVersion) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID ||
			a[i].Ref != b[i].Ref ||
			!a[i].ValidFrom.Equal(b[i].ValidFrom) ||
			!a[i].ValidUntil.Equal(b[i].ValidUntil) ||
			a[i].HasUntil != b[i].HasUntil {
			return false
		}
	}
	return true
}

func retryConfigEqual(a, b dispatcher.RetryConfig) bool {
	return a.Type == b.Type &&
		a.Max == b.Max &&
		a.Base == b.Base &&
		a.Cap == b.Cap &&
		a.Jitter == b.Jitter
}

func buildDispatchRoutes(compiled config.Compiled) []dispatcher.RouteConfig {
	routes := make([]dispatcher.RouteConfig, 0, len(compiled.Routes))
	for _, rt := range compiled.Routes {
		if len(rt.Deliveries) == 0 {
			continue
		}
		targets := make([]dispatcher.TargetConfig, 0, len(rt.Deliveries))
		for _, d := range rt.Deliveries {
			var signing *dispatcher.HMACSigningConfig
			if d.SigningHMAC.Enabled {
				secretVersions := make([]dispatcher.HMACSigningSecretVersion, 0, len(d.SigningHMAC.SecretVersions))
				for _, sv := range d.SigningHMAC.SecretVersions {
					secretVersions = append(secretVersions, dispatcher.HMACSigningSecretVersion{
						ID:         sv.ID,
						Ref:        sv.ValueRef,
						ValidFrom:  sv.ValidFrom,
						ValidUntil: sv.ValidUntil,
						HasUntil:   sv.HasUntil,
					})
				}
				signing = &dispatcher.HMACSigningConfig{
					SecretRef:       d.SigningHMAC.SecretRef,
					SecretVersions:  secretVersions,
					SecretSelection: d.SigningHMAC.SecretSelection,
					SignatureHeader: d.SigningHMAC.SignatureHeader,
					TimestampHeader: d.SigningHMAC.TimestampHeader,
				}
			}
			targets = append(targets, dispatcher.TargetConfig{
				URL:     d.URL,
				Timeout: d.Timeout,
				Retry: dispatcher.RetryConfig{
					Type:   d.Retry.Type,
					Max:    d.Retry.Max,
					Base:   d.Retry.Base,
					Cap:    d.Retry.Cap,
					Jitter: d.Retry.Jitter,
				},
				SignHMAC: signing,
			})
		}
		routes = append(routes, dispatcher.RouteConfig{
			Route:       rt.Path,
			Targets:     targets,
			Concurrency: rt.DeliverConcurrency,
		})
	}
	return routes
}

func publishPolicyEqual(a, b config.PublishPolicyConfig) bool {
	return a.DirectEnabled == b.DirectEnabled &&
		a.ManagedEnabled == b.ManagedEnabled &&
		a.AllowPullRoutes == b.AllowPullRoutes &&
		a.AllowDeliverRoutes == b.AllowDeliverRoutes &&
		a.RequireActor == b.RequireActor &&
		a.RequireRequestID == b.RequireRequestID &&
		a.FailClosed == b.FailClosed &&
		slicesEqual(a.ActorAllowlist, b.ActorAllowlist) &&
		slicesEqual(a.ActorPrefixes, b.ActorPrefixes)
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
