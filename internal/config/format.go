package config

import (
	"bytes"
	"fmt"
	"strings"
)

func format(cfg *Config) ([]byte, error) {
	var b bytes.Buffer

	for _, c := range cfg.Preamble {
		line := strings.TrimRight(c, "\r\n")
		if line == "" {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}

	hasBody := cfg.Ingress != nil || cfg.Defaults != nil || cfg.Vars != nil || cfg.Secrets != nil || cfg.PullAPI != nil || cfg.AdminAPI != nil || cfg.Observability != nil || cfg.QueueRetention != nil || cfg.DeliveredRetention != nil || cfg.DLQRetention != nil || cfg.QueueLimits != nil || len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0
	if len(cfg.Preamble) > 0 && hasBody {
		b.WriteByte('\n')
	}

	if cfg.Ingress != nil {
		writeIngressBlock(&b, cfg.Ingress)
		if cfg.PullAPI != nil || cfg.AdminAPI != nil || cfg.Observability != nil || cfg.QueueRetention != nil || cfg.DeliveredRetention != nil || cfg.DLQRetention != nil || cfg.QueueLimits != nil || cfg.Defaults != nil || cfg.Vars != nil || cfg.Secrets != nil || len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	if cfg.PullAPI != nil {
		writeAPIBlock(&b, "pull_api", cfg.PullAPI)
		if cfg.AdminAPI != nil || cfg.Observability != nil || cfg.QueueRetention != nil || cfg.DeliveredRetention != nil || cfg.DLQRetention != nil || cfg.QueueLimits != nil || cfg.Defaults != nil || cfg.Vars != nil || cfg.Secrets != nil || len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	if cfg.AdminAPI != nil {
		writeAPIBlock(&b, "admin_api", cfg.AdminAPI)
		if cfg.Observability != nil || cfg.QueueRetention != nil || cfg.DeliveredRetention != nil || cfg.DLQRetention != nil || cfg.QueueLimits != nil || cfg.Defaults != nil || cfg.Vars != nil || cfg.Secrets != nil || len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	if cfg.Observability != nil {
		writeObservabilityBlock(&b, cfg.Observability)
		if cfg.QueueRetention != nil || cfg.DeliveredRetention != nil || cfg.DLQRetention != nil || cfg.QueueLimits != nil || cfg.Defaults != nil || cfg.Vars != nil || cfg.Secrets != nil || len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	if cfg.QueueRetention != nil {
		writeQueueRetentionBlock(&b, cfg.QueueRetention)
		if cfg.DeliveredRetention != nil || cfg.DLQRetention != nil || cfg.QueueLimits != nil || cfg.Defaults != nil || cfg.Vars != nil || cfg.Secrets != nil || len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	if cfg.DeliveredRetention != nil {
		writeDeliveredRetentionBlock(&b, cfg.DeliveredRetention)
		if cfg.DLQRetention != nil || cfg.QueueLimits != nil || cfg.Defaults != nil || cfg.Vars != nil || cfg.Secrets != nil || len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	if cfg.DLQRetention != nil {
		writeDLQRetentionBlock(&b, cfg.DLQRetention)
		if cfg.QueueLimits != nil || cfg.Defaults != nil || cfg.Vars != nil || cfg.Secrets != nil || len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	if cfg.QueueLimits != nil {
		writeQueueLimitsBlock(&b, cfg.QueueLimits)
		if cfg.Defaults != nil || cfg.Vars != nil || cfg.Secrets != nil || len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	if cfg.Defaults != nil {
		writeDefaultsBlock(&b, cfg.Defaults)
		if cfg.Vars != nil || cfg.Secrets != nil || len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	if cfg.Vars != nil {
		writeVarsBlock(&b, cfg.Vars)
		if cfg.Secrets != nil || len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	if cfg.Secrets != nil {
		writeSecretsBlock(&b, cfg.Secrets)
		if len(cfg.NamedMatchers) > 0 || len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	if len(cfg.NamedMatchers) > 0 {
		for i, m := range cfg.NamedMatchers {
			writeNamedMatcherBlock(&b, m)
			if i != len(cfg.NamedMatchers)-1 {
				b.WriteByte('\n')
			}
		}
		if len(cfg.Routes) > 0 {
			b.WriteByte('\n')
		}
	}

	// Group consecutive routes by channel type for formatting.
	type routeGroup struct {
		ct     ChannelType
		routes []Route
	}
	var groups []routeGroup
	for _, r := range cfg.Routes {
		ct := r.ChannelType
		if ct != "" && len(groups) > 0 && groups[len(groups)-1].ct == ct {
			groups[len(groups)-1].routes = append(groups[len(groups)-1].routes, r)
		} else {
			groups = append(groups, routeGroup{ct: ct, routes: []Route{r}})
		}
	}

	for i, g := range groups {
		switch {
		case g.ct == "":
			// Bare routes (implicit inbound).
			for j, r := range g.routes {
				writeRouteBlock(&b, r)
				if j != len(g.routes)-1 {
					b.WriteByte('\n')
				}
			}
		case len(g.routes) == 1:
			// Single-route shorthand: channel_type /path { ... }
			fmt.Fprintf(&b, "%s ", string(g.ct))
			writeRouteBlock(&b, g.routes[0])
		default:
			// Multi-route wrapper: channel_type { ... }
			fmt.Fprintf(&b, "%s {\n", string(g.ct))
			for j, r := range g.routes {
				writeRouteBlockIndented(&b, r, "  ")
				if j != len(g.routes)-1 {
					b.WriteByte('\n')
				}
			}
			b.WriteString("}\n")
		}
		if i != len(groups)-1 {
			b.WriteByte('\n')
		}
	}

	return b.Bytes(), nil
}

func writeIngressBlock(b *bytes.Buffer, ing *IngressBlock) {
	b.WriteString("ingress {\n")
	if ing.Listen != "" {
		fmt.Fprintf(b, "  listen %s\n", formatValue(ing.Listen, ing.ListenQuoted))
	}
	if ing.TLS != nil {
		writeTLSBlock(b, ing.TLS)
	}
	if ing.RateLimit != nil {
		writeRateLimitBlock(b, "  ", ing.RateLimit)
	}
	b.WriteString("}\n")
}

func writeDefaultsBlock(b *bytes.Buffer, def *DefaultsBlock) {
	b.WriteString("defaults {\n")
	if def.MaxBodySet {
		fmt.Fprintf(b, "  max_body %s\n", formatValue(def.MaxBody, def.MaxBodyQuoted))
	}
	if def.MaxHeadersSet {
		fmt.Fprintf(b, "  max_headers %s\n", formatValue(def.MaxHeaders, def.MaxHeadersQuoted))
	}
	if def.Egress != nil {
		writeEgressBlock(b, def.Egress)
	}
	if def.PublishPolicy != nil {
		writePublishPolicyBlock(b, def.PublishPolicy)
	}
	if def.Deliver != nil {
		writeDefaultsDeliverBlock(b, def.Deliver)
	}
	if def.TrendSignals != nil {
		writeTrendSignalsBlock(b, def.TrendSignals)
	}
	b.WriteString("}\n")
}

func writeVarsBlock(b *bytes.Buffer, vars *VarsBlock) {
	b.WriteString("vars {\n")
	for _, item := range vars.Items {
		if strings.TrimSpace(item.Name) == "" {
			continue
		}
		fmt.Fprintf(b, "  %s %s\n", formatValue(item.Name, item.NameQuoted), formatValue(item.Value, item.ValueQuoted))
	}
	b.WriteString("}\n")
}

func writeSecretsBlock(b *bytes.Buffer, s *SecretsBlock) {
	b.WriteString("secrets {\n")
	for _, secret := range s.Items {
		if strings.TrimSpace(secret.ID) == "" {
			continue
		}
		fmt.Fprintf(b, "  secret %s {\n", formatValue(secret.ID, secret.IDQuoted))
		if secret.ValueSet {
			fmt.Fprintf(b, "    value %s\n", formatValue(secret.Value, secret.ValueQuoted))
		}
		if secret.ValidFromSet {
			fmt.Fprintf(b, "    valid_from %s\n", formatValue(secret.ValidFrom, secret.ValidFromQuoted))
		}
		if secret.ValidUntilSet {
			fmt.Fprintf(b, "    valid_until %s\n", formatValue(secret.ValidUntil, secret.ValidUntilQuoted))
		}
		b.WriteString("  }\n")
	}
	b.WriteString("}\n")
}

func writeDefaultsDeliverBlock(b *bytes.Buffer, def *DeliverBlock) {
	b.WriteString("  deliver {\n")
	if def.Retry != nil {
		fmt.Fprintf(b, "    %s\n", formatRetryDirective(def.Retry))
	}
	if def.TimeoutSet {
		fmt.Fprintf(b, "    timeout %s\n", formatValue(def.Timeout, def.TimeoutQuoted))
	}
	if def.ConcurrencySet {
		fmt.Fprintf(b, "    concurrency %s\n", formatValue(def.Concurrency, def.ConcurrencyQuoted))
	}
	b.WriteString("  }\n")
}

func writeTrendSignalsBlock(b *bytes.Buffer, block *TrendSignalsBlock) {
	b.WriteString("  trend_signals {\n")
	if block.WindowSet {
		fmt.Fprintf(b, "    window %s\n", formatValue(block.Window, block.WindowQuoted))
	}
	if block.ExpectedCaptureIntervalSet {
		fmt.Fprintf(b, "    expected_capture_interval %s\n", formatValue(block.ExpectedCaptureInterval, block.ExpectedCaptureIntervalQuoted))
	}
	if block.StaleGraceFactorSet {
		fmt.Fprintf(b, "    stale_grace_factor %s\n", formatValue(block.StaleGraceFactor, block.StaleGraceFactorQuoted))
	}
	if block.SustainedGrowthConsecutiveSet {
		fmt.Fprintf(b, "    sustained_growth_consecutive %s\n", formatValue(block.SustainedGrowthConsecutive, block.SustainedGrowthConsecutiveQuoted))
	}
	if block.SustainedGrowthMinSamplesSet {
		fmt.Fprintf(b, "    sustained_growth_min_samples %s\n", formatValue(block.SustainedGrowthMinSamples, block.SustainedGrowthMinSamplesQuoted))
	}
	if block.SustainedGrowthMinDeltaSet {
		fmt.Fprintf(b, "    sustained_growth_min_delta %s\n", formatValue(block.SustainedGrowthMinDelta, block.SustainedGrowthMinDeltaQuoted))
	}
	if block.RecentSurgeMinTotalSet {
		fmt.Fprintf(b, "    recent_surge_min_total %s\n", formatValue(block.RecentSurgeMinTotal, block.RecentSurgeMinTotalQuoted))
	}
	if block.RecentSurgeMinDeltaSet {
		fmt.Fprintf(b, "    recent_surge_min_delta %s\n", formatValue(block.RecentSurgeMinDelta, block.RecentSurgeMinDeltaQuoted))
	}
	if block.RecentSurgePercentSet {
		fmt.Fprintf(b, "    recent_surge_percent %s\n", formatValue(block.RecentSurgePercent, block.RecentSurgePercentQuoted))
	}
	if block.DeadShareHighMinTotalSet {
		fmt.Fprintf(b, "    dead_share_high_min_total %s\n", formatValue(block.DeadShareHighMinTotal, block.DeadShareHighMinTotalQuoted))
	}
	if block.DeadShareHighPercentSet {
		fmt.Fprintf(b, "    dead_share_high_percent %s\n", formatValue(block.DeadShareHighPercent, block.DeadShareHighPercentQuoted))
	}
	if block.QueuedPressureMinTotalSet {
		fmt.Fprintf(b, "    queued_pressure_min_total %s\n", formatValue(block.QueuedPressureMinTotal, block.QueuedPressureMinTotalQuoted))
	}
	if block.QueuedPressurePercentSet {
		fmt.Fprintf(b, "    queued_pressure_percent %s\n", formatValue(block.QueuedPressurePercent, block.QueuedPressurePercentQuoted))
	}
	if block.QueuedPressureLeasedMultiplierSet {
		fmt.Fprintf(b, "    queued_pressure_leased_multiplier %s\n", formatValue(block.QueuedPressureLeasedMultiplier, block.QueuedPressureLeasedMultiplierQuoted))
	}
	b.WriteString("  }\n")
}

func writeAPIBlock(b *bytes.Buffer, name string, api *APIBlock) {
	fmt.Fprintf(b, "%s {\n", name)
	if api.Listen != "" {
		fmt.Fprintf(b, "  listen %s\n", formatValue(api.Listen, api.ListenQuoted))
	}
	if api.Prefix != "" {
		fmt.Fprintf(b, "  prefix %s\n", formatValue(api.Prefix, api.PrefixQuoted))
	}
	if name == "pull_api" {
		if api.MaxBatchSet {
			fmt.Fprintf(b, "  max_batch %s\n", formatValue(api.MaxBatch, api.MaxBatchQuoted))
		}
		if api.DefaultLeaseTTLSet {
			fmt.Fprintf(b, "  default_lease_ttl %s\n", formatValue(api.DefaultLeaseTTL, api.DefaultLeaseTTLQuoted))
		}
		if api.MaxLeaseTTLSet {
			fmt.Fprintf(b, "  max_lease_ttl %s\n", formatValue(api.MaxLeaseTTL, api.MaxLeaseTTLQuoted))
		}
		if api.DefaultMaxWaitSet {
			fmt.Fprintf(b, "  default_max_wait %s\n", formatValue(api.DefaultMaxWait, api.DefaultMaxWaitQuoted))
		}
		if api.MaxWaitSet {
			fmt.Fprintf(b, "  max_wait %s\n", formatValue(api.MaxWait, api.MaxWaitQuoted))
		}
	}
	if api.TLS != nil {
		writeTLSBlock(b, api.TLS)
	}
	for i, t := range api.AuthTokens {
		if strings.TrimSpace(t) == "" {
			continue
		}
		fmt.Fprintf(b, "  auth token %s\n", formatValue(t, quotedAt(api.AuthTokensQuoted, i)))
	}
	b.WriteString("}\n")
}

func writeObservabilityBlock(b *bytes.Buffer, obs *ObservabilityBlock) {
	b.WriteString("observability {\n")
	if obs.AccessLogBlock != nil {
		writeAccessLogDirectiveBlock(b, obs.AccessLogBlock)
	} else if obs.AccessLogSet {
		fmt.Fprintf(b, "  access_log %s\n", formatValue(obs.AccessLog, obs.AccessLogQuoted))
	}
	if obs.RuntimeLogBlock != nil {
		writeRuntimeLogDirectiveBlock(b, obs.RuntimeLogBlock)
	} else if obs.RuntimeLogSet {
		fmt.Fprintf(b, "  runtime_log %s\n", formatValue(obs.RuntimeLog, obs.RuntimeLogQuoted))
	}
	if obs.Metrics != nil {
		if obs.Metrics.shorthand {
			fmt.Fprintf(b, "  metrics %s\n", formatValue(obs.Metrics.Enabled, obs.Metrics.EnabledQuoted))
		} else {
			b.WriteString("  metrics {\n")
			if obs.Metrics.EnabledSet {
				fmt.Fprintf(b, "    enabled %s\n", formatValue(obs.Metrics.Enabled, obs.Metrics.EnabledQuoted))
			}
			if obs.Metrics.ListenSet {
				fmt.Fprintf(b, "    listen %s\n", formatValue(obs.Metrics.Listen, obs.Metrics.ListenQuoted))
			}
			if obs.Metrics.PrefixSet {
				fmt.Fprintf(b, "    prefix %s\n", formatValue(obs.Metrics.Prefix, obs.Metrics.PrefixQuoted))
			}
			b.WriteString("  }\n")
		}
	}
	if obs.Tracing != nil {
		if obs.Tracing.shorthand {
			fmt.Fprintf(b, "  tracing %s\n", formatValue(obs.Tracing.Enabled, obs.Tracing.EnabledQuoted))
		} else {
			writeTracingBlock(b, obs.Tracing)
		}
	}
	b.WriteString("}\n")
}

func writeAccessLogDirectiveBlock(b *bytes.Buffer, block *AccessLogBlock) {
	b.WriteString("  access_log {\n")
	if block.EnabledSet {
		fmt.Fprintf(b, "    enabled %s\n", formatValue(block.Enabled, block.EnabledQuoted))
	}
	if block.OutputSet {
		fmt.Fprintf(b, "    output %s\n", formatValue(block.Output, block.OutputQuoted))
	}
	if block.PathSet {
		fmt.Fprintf(b, "    path %s\n", formatValue(block.Path, block.PathQuoted))
	}
	if block.FormatSet {
		fmt.Fprintf(b, "    format %s\n", formatValue(block.Format, block.FormatQuoted))
	}
	b.WriteString("  }\n")
}

func writeRuntimeLogDirectiveBlock(b *bytes.Buffer, block *RuntimeLogBlock) {
	b.WriteString("  runtime_log {\n")
	if block.LevelSet {
		fmt.Fprintf(b, "    level %s\n", formatValue(block.Level, block.LevelQuoted))
	}
	if block.OutputSet {
		fmt.Fprintf(b, "    output %s\n", formatValue(block.Output, block.OutputQuoted))
	}
	if block.PathSet {
		fmt.Fprintf(b, "    path %s\n", formatValue(block.Path, block.PathQuoted))
	}
	if block.FormatSet {
		fmt.Fprintf(b, "    format %s\n", formatValue(block.Format, block.FormatQuoted))
	}
	b.WriteString("  }\n")
}

func writeTLSBlock(b *bytes.Buffer, t *TLSBlock) {
	b.WriteString("  tls {\n")
	if t.CertFileSet {
		fmt.Fprintf(b, "    cert_file %s\n", formatValue(t.CertFile, t.CertFileQuoted))
	}
	if t.KeyFileSet {
		fmt.Fprintf(b, "    key_file %s\n", formatValue(t.KeyFile, t.KeyFileQuoted))
	}
	if t.ClientCASet {
		fmt.Fprintf(b, "    client_ca %s\n", formatValue(t.ClientCA, t.ClientCAQuoted))
	}
	if t.ClientAuthSet {
		fmt.Fprintf(b, "    client_auth %s\n", formatValue(t.ClientAuth, t.ClientAuthQuoted))
	}
	b.WriteString("  }\n")
}

func writeEgressBlock(b *bytes.Buffer, eg *EgressBlock) {
	b.WriteString("  egress {\n")
	for i, v := range eg.Allow {
		if strings.TrimSpace(v) == "" {
			continue
		}
		fmt.Fprintf(b, "    allow %s\n", formatValue(v, quotedAt(eg.AllowQuoted, i)))
	}
	for i, v := range eg.Deny {
		if strings.TrimSpace(v) == "" {
			continue
		}
		fmt.Fprintf(b, "    deny %s\n", formatValue(v, quotedAt(eg.DenyQuoted, i)))
	}
	if eg.HTTPSOnlySet {
		fmt.Fprintf(b, "    https_only %s\n", formatValue(eg.HTTPSOnly, eg.HTTPSOnlyQuoted))
	}
	if eg.RedirectsSet {
		fmt.Fprintf(b, "    redirects %s\n", formatValue(eg.Redirects, eg.RedirectsQuoted))
	}
	if eg.DNSRebindProtectionSet {
		fmt.Fprintf(b, "    dns_rebind_protection %s\n", formatValue(eg.DNSRebindProtection, eg.DNSRebindProtectionQuoted))
	}
	b.WriteString("  }\n")
}

func writePublishPolicyBlock(b *bytes.Buffer, p *PublishPolicyBlock) {
	b.WriteString("  publish_policy {\n")
	if p.DirectSet {
		fmt.Fprintf(b, "    direct %s\n", formatValue(p.Direct, p.DirectQuoted))
	}
	if p.ManagedSet {
		fmt.Fprintf(b, "    managed %s\n", formatValue(p.Managed, p.ManagedQuoted))
	}
	if p.AllowPullRoutesSet {
		fmt.Fprintf(b, "    allow_pull_routes %s\n", formatValue(p.AllowPullRoutes, p.AllowPullRoutesQuoted))
	}
	if p.AllowDeliverRoutesSet {
		fmt.Fprintf(b, "    allow_deliver_routes %s\n", formatValue(p.AllowDeliverRoutes, p.AllowDeliverRoutesQuoted))
	}
	if p.RequireActorSet {
		fmt.Fprintf(b, "    require_actor %s\n", formatValue(p.RequireActor, p.RequireActorQuoted))
	}
	if p.RequireRequestIDSet {
		fmt.Fprintf(b, "    require_request_id %s\n", formatValue(p.RequireRequestID, p.RequireRequestIDQuoted))
	}
	if p.FailClosedSet {
		fmt.Fprintf(b, "    fail_closed %s\n", formatValue(p.FailClosed, p.FailClosedQuoted))
	}
	for i := range p.ActorAllow {
		quoted := false
		if i < len(p.ActorAllowQuoted) {
			quoted = p.ActorAllowQuoted[i]
		}
		fmt.Fprintf(b, "    actor_allow %s\n", formatValue(p.ActorAllow[i], quoted))
	}
	for i := range p.ActorPrefix {
		quoted := false
		if i < len(p.ActorPrefixQuoted) {
			quoted = p.ActorPrefixQuoted[i]
		}
		fmt.Fprintf(b, "    actor_prefix %s\n", formatValue(p.ActorPrefix[i], quoted))
	}
	b.WriteString("  }\n")
}

func writeQueueLimitsBlock(b *bytes.Buffer, q *QueueLimitsBlock) {
	b.WriteString("queue_limits {\n")
	if q.MaxDepthSet {
		fmt.Fprintf(b, "  max_depth %s\n", formatValue(q.MaxDepth, q.MaxDepthQuoted))
	}
	if q.DropPolicySet {
		fmt.Fprintf(b, "  drop_policy %s\n", formatValue(q.DropPolicy, q.DropPolicyQuoted))
	}
	b.WriteString("}\n")
}

func writeTracingBlock(b *bytes.Buffer, t *TracingBlock) {
	b.WriteString("  tracing {\n")
	if t.EnabledSet {
		fmt.Fprintf(b, "    enabled %s\n", formatValue(t.Enabled, t.EnabledQuoted))
	}
	if t.CollectorSet {
		fmt.Fprintf(b, "    collector %s\n", formatValue(t.Collector, t.CollectorQuoted))
	}
	if t.URLPathSet {
		fmt.Fprintf(b, "    url_path %s\n", formatValue(t.URLPath, t.URLPathQuoted))
	}
	if t.TimeoutSet {
		fmt.Fprintf(b, "    timeout %s\n", formatValue(t.Timeout, t.TimeoutQuoted))
	}
	if t.CompressionSet {
		fmt.Fprintf(b, "    compression %s\n", formatValue(t.Compression, t.CompressionQuoted))
	}
	if t.InsecureSet {
		fmt.Fprintf(b, "    insecure %s\n", formatValue(t.Insecure, t.InsecureQuoted))
	}
	if t.ProxyURLSet {
		fmt.Fprintf(b, "    proxy_url %s\n", formatValue(t.ProxyURL, t.ProxyURLQuoted))
	}
	if t.TLS != nil {
		writeTracingTLSBlock(b, t.TLS)
	}
	if t.Retry != nil {
		writeTracingRetryBlock(b, t.Retry)
	}
	for _, h := range t.Headers {
		if strings.TrimSpace(h.Name) == "" {
			continue
		}
		fmt.Fprintf(b, "    header %s %s\n",
			formatValue(h.Name, h.NameQuoted),
			formatValue(h.Value, h.ValueQuoted),
		)
	}
	b.WriteString("  }\n")
}

func writeTracingTLSBlock(b *bytes.Buffer, t *TracingTLSBlock) {
	b.WriteString("    tls {\n")
	if t.CAFileSet {
		fmt.Fprintf(b, "      ca_file %s\n", formatValue(t.CAFile, t.CAFileQuoted))
	}
	if t.CertFileSet {
		fmt.Fprintf(b, "      cert_file %s\n", formatValue(t.CertFile, t.CertFileQuoted))
	}
	if t.KeyFileSet {
		fmt.Fprintf(b, "      key_file %s\n", formatValue(t.KeyFile, t.KeyFileQuoted))
	}
	if t.ServerNameSet {
		fmt.Fprintf(b, "      server_name %s\n", formatValue(t.ServerName, t.ServerNameQuoted))
	}
	if t.InsecureSkipVerifySet {
		fmt.Fprintf(b, "      insecure_skip_verify %s\n", formatValue(t.InsecureSkipVerify, t.InsecureSkipVerifyQuoted))
	}
	b.WriteString("    }\n")
}

func writeTracingRetryBlock(b *bytes.Buffer, r *TracingRetryBlock) {
	b.WriteString("    retry {\n")
	if r.EnabledSet {
		fmt.Fprintf(b, "      enabled %s\n", formatValue(r.Enabled, r.EnabledQuoted))
	}
	if r.InitialIntervalSet {
		fmt.Fprintf(b, "      initial_interval %s\n", formatValue(r.InitialInterval, r.InitialIntervalQuoted))
	}
	if r.MaxIntervalSet {
		fmt.Fprintf(b, "      max_interval %s\n", formatValue(r.MaxInterval, r.MaxIntervalQuoted))
	}
	if r.MaxElapsedTimeSet {
		fmt.Fprintf(b, "      max_elapsed_time %s\n", formatValue(r.MaxElapsedTime, r.MaxElapsedTimeQuoted))
	}
	b.WriteString("    }\n")
}

func writeQueueRetentionBlock(b *bytes.Buffer, q *QueueRetentionBlock) {
	b.WriteString("queue_retention {\n")
	if q.MaxAgeSet {
		fmt.Fprintf(b, "  max_age %s\n", formatValue(q.MaxAge, q.MaxAgeQuoted))
	}
	if q.PruneIntervalSet {
		fmt.Fprintf(b, "  prune_interval %s\n", formatValue(q.PruneInterval, q.PruneIntervalQuoted))
	}
	b.WriteString("}\n")
}

func writeDeliveredRetentionBlock(b *bytes.Buffer, q *DeliveredRetentionBlock) {
	b.WriteString("delivered_retention {\n")
	if q.MaxAgeSet {
		fmt.Fprintf(b, "  max_age %s\n", formatValue(q.MaxAge, q.MaxAgeQuoted))
	}
	b.WriteString("}\n")
}

func writeDLQRetentionBlock(b *bytes.Buffer, q *DLQRetentionBlock) {
	b.WriteString("dlq_retention {\n")
	if q.MaxAgeSet {
		fmt.Fprintf(b, "  max_age %s\n", formatValue(q.MaxAge, q.MaxAgeQuoted))
	}
	if q.MaxDepthSet {
		fmt.Fprintf(b, "  max_depth %s\n", formatValue(q.MaxDepth, q.MaxDepthQuoted))
	}
	b.WriteString("}\n")
}

func writeRouteBlock(b *bytes.Buffer, r Route) {
	fmt.Fprintf(b, "%s {\n", formatRoutePath(r.Path, r.PathQuoted))
	if r.ApplicationSet {
		fmt.Fprintf(b, "  application %s\n", formatValue(r.Application, r.ApplicationQuoted))
	}
	if r.EndpointNameSet {
		fmt.Fprintf(b, "  endpoint_name %s\n", formatValue(r.EndpointName, r.EndpointNameQuoted))
	}
	for _, ref := range r.MatchRefs {
		if strings.TrimSpace(ref) == "" {
			continue
		}
		fmt.Fprintf(b, "  match @%s\n", ref)
	}
	if r.Match != nil {
		writeMatchBlock(b, r.Match)
	}
	if r.RateLimit != nil {
		writeRateLimitBlock(b, "  ", r.RateLimit)
	}
	for _, basic := range r.AuthBasic {
		fmt.Fprintf(b, "  auth basic %s %s\n",
			formatValue(basic.User, basic.UserQuoted),
			formatValue(basic.Pass, basic.PassQuoted),
		)
	}
	if r.AuthForward != nil && strings.TrimSpace(r.AuthForward.URL) != "" {
		if shouldWriteRouteAuthForwardBlock(*r.AuthForward) {
			writeRouteAuthForwardBlock(b, *r.AuthForward)
		} else {
			fmt.Fprintf(b, "  auth forward %s\n", formatValue(r.AuthForward.URL, r.AuthForward.URLQuoted))
		}
	}
	if shouldWriteRouteAuthHMACBlock(r) {
		writeRouteAuthHMACBlock(b, r)
	} else {
		for i, s := range r.AuthHMACSecrets {
			if strings.TrimSpace(s) == "" {
				continue
			}
			if isAuthHMACRef(r, i) {
				fmt.Fprintf(b, "  auth hmac secret_ref %s\n", formatValue(s, quotedAt(r.AuthHMACSecretsQuoted, i)))
			} else {
				fmt.Fprintf(b, "  auth hmac %s\n", formatValue(s, quotedAt(r.AuthHMACSecretsQuoted, i)))
			}
		}
	}
	if r.MaxBodySet {
		fmt.Fprintf(b, "  max_body %s\n", formatValue(r.MaxBody, r.MaxBodyQuoted))
	}
	if r.MaxHeadersSet {
		fmt.Fprintf(b, "  max_headers %s\n", formatValue(r.MaxHeaders, r.MaxHeadersQuoted))
	}
	if r.Publish != nil {
		writePublishBlock(b, r.Publish)
	}
	if r.Queue != nil {
		writeQueueBlock(b, r.Queue)
	}
	if r.DeliverConcurrencySet {
		fmt.Fprintf(b, "  deliver_concurrency %s\n", formatValue(r.DeliverConcurrency, r.DeliverConcurrencyQuoted))
	}
	if r.Pull != nil {
		b.WriteString("  pull {\n")
		if r.Pull.Path != "" {
			fmt.Fprintf(b, "    path %s\n", formatValue(r.Pull.Path, r.Pull.PathQuoted))
		}
		for i, t := range r.Pull.AuthTokens {
			if strings.TrimSpace(t) == "" {
				continue
			}
			fmt.Fprintf(b, "    auth token %s\n", formatValue(t, quotedAt(r.Pull.AuthTokensQuoted, i)))
		}
		b.WriteString("  }\n")
	}
	for _, d := range r.Deliveries {
		writeDeliverBlock(b, d)
	}
	b.WriteString("}\n")
}

// writeRouteBlockIndented writes a route block with every line prefixed by indent.
func writeRouteBlockIndented(b *bytes.Buffer, r Route, indent string) {
	var sub bytes.Buffer
	writeRouteBlock(&sub, r)
	raw := strings.TrimRight(sub.String(), "\n")
	for _, line := range strings.Split(raw, "\n") {
		b.WriteString(indent)
		b.WriteString(line)
		b.WriteByte('\n')
	}
}

func writeMatchBlock(b *bytes.Buffer, m *MatchBlock) {
	b.WriteString("  match {\n")
	writeMatchBody(b, "    ", m)
	b.WriteString("  }\n")
}

func writeRateLimitBlock(b *bytes.Buffer, indent string, rl *RateLimitBlock) {
	if rl == nil {
		return
	}
	fmt.Fprintf(b, "%srate_limit {\n", indent)
	inner := indent + "  "
	if rl.RPSSet {
		fmt.Fprintf(b, "%srps %s\n", inner, formatValue(rl.RPS, rl.RPSQuoted))
	}
	if rl.BurstSet {
		fmt.Fprintf(b, "%sburst %s\n", inner, formatValue(rl.Burst, rl.BurstQuoted))
	}
	fmt.Fprintf(b, "%s}\n", indent)
}

func writeNamedMatcherBlock(b *bytes.Buffer, m NamedMatcher) {
	fmt.Fprintf(b, "@%s {\n", m.Name)
	if m.Match != nil {
		writeMatchBody(b, "  ", m.Match)
	}
	b.WriteString("}\n")
}

func writeMatchBody(b *bytes.Buffer, indent string, m *MatchBlock) {
	for i, method := range m.Methods {
		if strings.TrimSpace(method) == "" {
			continue
		}
		fmt.Fprintf(b, "%smethod %s\n", indent, formatValue(method, quotedAt(m.MethodsQuoted, i)))
	}
	for i, h := range m.Hosts {
		if strings.TrimSpace(h) == "" {
			continue
		}
		fmt.Fprintf(b, "%shost %s\n", indent, formatValue(h, quotedAt(m.HostsQuoted, i)))
	}
	for _, h := range m.Headers {
		if strings.TrimSpace(h.Name) == "" {
			continue
		}
		fmt.Fprintf(b, "%sheader %s %s\n",
			indent,
			formatValue(h.Name, h.NameQuoted),
			formatValue(h.Value, h.ValueQuoted),
		)
	}
	for i, name := range m.HeaderExists {
		if strings.TrimSpace(name) == "" {
			continue
		}
		fmt.Fprintf(b, "%sheader_exists %s\n", indent, formatValue(name, quotedAt(m.HeaderExistsQuoted, i)))
	}
	for _, q := range m.Query {
		if strings.TrimSpace(q.Name) == "" {
			continue
		}
		fmt.Fprintf(b, "%squery %s %s\n",
			indent,
			formatValue(q.Name, q.NameQuoted),
			formatValue(q.Value, q.ValueQuoted),
		)
	}
	for i, raw := range m.RemoteIPs {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		fmt.Fprintf(b, "%sremote_ip %s\n", indent, formatValue(raw, quotedAt(m.RemoteIPsQuoted, i)))
	}
	for i, name := range m.QueryExists {
		if strings.TrimSpace(name) == "" {
			continue
		}
		fmt.Fprintf(b, "%squery_exists %s\n", indent, formatValue(name, quotedAt(m.QueryExistsQuoted, i)))
	}
}

func writePublishBlock(b *bytes.Buffer, p *PublishBlock) {
	if p.shorthand {
		fmt.Fprintf(b, "  publish %s\n", formatValue(p.Enabled, p.EnabledQuoted))
		return
	}
	if p.dotNotation {
		if p.DirectSet {
			fmt.Fprintf(b, "  publish.direct %s\n", formatValue(p.Direct, p.DirectQuoted))
		}
		if p.ManagedSet {
			fmt.Fprintf(b, "  publish.managed %s\n", formatValue(p.Managed, p.ManagedQuoted))
		}
		return
	}
	b.WriteString("  publish {\n")
	if p.EnabledSet {
		fmt.Fprintf(b, "    enabled %s\n", formatValue(p.Enabled, p.EnabledQuoted))
	}
	if p.DirectSet {
		fmt.Fprintf(b, "    direct %s\n", formatValue(p.Direct, p.DirectQuoted))
	}
	if p.ManagedSet {
		fmt.Fprintf(b, "    managed %s\n", formatValue(p.Managed, p.ManagedQuoted))
	}
	b.WriteString("  }\n")
}

func writeQueueBlock(b *bytes.Buffer, q *QueueBlock) {
	if q.shorthand {
		fmt.Fprintf(b, "  queue %s\n", formatValue(q.Backend, q.BackendQuoted))
		return
	}
	b.WriteString("  queue {\n")
	if q.BackendSet {
		fmt.Fprintf(b, "    backend %s\n", formatValue(q.Backend, q.BackendQuoted))
	}
	b.WriteString("  }\n")
}

func writeDeliverBlock(b *bytes.Buffer, d Deliver) {
	fmt.Fprintf(b, "  deliver %s {\n", formatValue(d.URL, d.URLQuoted))
	if d.Retry != nil {
		fmt.Fprintf(b, "    %s\n", formatRetryDirective(d.Retry))
	}
	if d.TimeoutSet {
		fmt.Fprintf(b, "    timeout %s\n", formatValue(d.Timeout, d.TimeoutQuoted))
	}
	if len(d.SignHMACSecretRefs) > 0 {
		for i, ref := range d.SignHMACSecretRefs {
			quoted := false
			if i < len(d.SignHMACSecretRefsQuoted) {
				quoted = d.SignHMACSecretRefsQuoted[i]
			}
			fmt.Fprintf(b, "    sign hmac secret_ref %s\n", formatValue(ref, quoted))
		}
	} else if d.SignHMACSecretSet {
		fmt.Fprintf(b, "    sign hmac %s\n", formatValue(d.SignHMACSecret, d.SignHMACSecretQuoted))
	}
	if d.SignHMACSignatureHeaderSet {
		fmt.Fprintf(b, "    sign signature_header %s\n", formatValue(d.SignHMACSignatureHeader, d.SignHMACSignatureHeaderQuoted))
	}
	if d.SignHMACTimestampHeaderSet {
		fmt.Fprintf(b, "    sign timestamp_header %s\n", formatValue(d.SignHMACTimestampHeader, d.SignHMACTimestampHeaderQuoted))
	}
	if d.SignHMACSecretSelectionSet {
		fmt.Fprintf(b, "    sign secret_selection %s\n", formatValue(d.SignHMACSecretSelection, d.SignHMACSecretSelectionQuoted))
	}
	b.WriteString("  }\n")
}

func formatRetryDirective(r *RetryBlock) string {
	var out strings.Builder
	out.WriteString("retry ")
	out.WriteString(formatValue(r.Type, r.TypeQuoted))
	if r.MaxSet {
		out.WriteString(" max ")
		out.WriteString(formatValue(r.Max, r.MaxQuoted))
	}
	if r.BaseSet {
		out.WriteString(" base ")
		out.WriteString(formatValue(r.Base, r.BaseQuoted))
	}
	if r.CapSet {
		out.WriteString(" cap ")
		out.WriteString(formatValue(r.Cap, r.CapQuoted))
	}
	if r.JitterSet {
		out.WriteString(" jitter ")
		out.WriteString(formatValue(r.Jitter, r.JitterQuoted))
	}
	return out.String()
}

func formatRoutePath(path string, quoted bool) string {
	if quoted {
		return quoteString(path)
	}
	if isUnquotedPathSafe(path) {
		return path
	}
	return quoteString(path)
}

func formatValue(val string, quoted bool) string {
	if quoted {
		return quoteString(val)
	}
	if isUnquotedValueSafe(val) {
		return val
	}
	return quoteString(val)
}

func isUnquotedPathSafe(path string) bool {
	if path == "" || !strings.HasPrefix(path, "/") {
		return false
	}
	for _, r := range path {
		switch r {
		case ' ', '\t', '\n', '\r', '{', '}', '"', '#':
			return false
		}
	}
	return true
}

func isUnquotedValueSafe(val string) bool {
	if val == "" {
		return false
	}
	if strings.HasPrefix(val, "{") && strings.HasSuffix(val, "}") && !strings.ContainsAny(val, " \t\r\n") {
		return true
	}
	for _, r := range val {
		switch r {
		case ' ', '\t', '\n', '\r', '{', '}', '"', '#':
			return false
		}
	}
	return true
}

func quotedAt(flags []bool, idx int) bool {
	if idx < 0 || idx >= len(flags) {
		return false
	}
	return flags[idx]
}

func isAuthHMACRef(r Route, idx int) bool {
	if idx < 0 || idx >= len(r.AuthHMACSecretIsRef) {
		return false
	}
	return r.AuthHMACSecretIsRef[idx]
}

func shouldWriteRouteAuthHMACBlock(r Route) bool {
	if r.AuthHMACBlockSet {
		return true
	}
	return hasRouteAuthHMACOptions(r)
}

func shouldWriteRouteAuthForwardBlock(b ForwardAuthBlock) bool {
	return b.BlockSet || b.TimeoutSet || len(b.CopyHeaders) > 0 || b.BodyLimitSet
}

func hasRouteAuthHMACOptions(r Route) bool {
	return r.AuthHMACSignatureHeaderSet || r.AuthHMACTimestampHeaderSet || r.AuthHMACNonceHeaderSet || r.AuthHMACToleranceSet
}

func writeRouteAuthHMACBlock(b *bytes.Buffer, r Route) {
	b.WriteString("  auth hmac {\n")
	for i, s := range r.AuthHMACSecrets {
		if strings.TrimSpace(s) == "" {
			continue
		}
		if isAuthHMACRef(r, i) {
			fmt.Fprintf(b, "    secret_ref %s\n", formatValue(s, quotedAt(r.AuthHMACSecretsQuoted, i)))
		} else {
			fmt.Fprintf(b, "    secret %s\n", formatValue(s, quotedAt(r.AuthHMACSecretsQuoted, i)))
		}
	}
	if r.AuthHMACSignatureHeaderSet {
		fmt.Fprintf(b, "    signature_header %s\n", formatValue(r.AuthHMACSignatureHeader, r.AuthHMACSignatureHeaderQuoted))
	}
	if r.AuthHMACTimestampHeaderSet {
		fmt.Fprintf(b, "    timestamp_header %s\n", formatValue(r.AuthHMACTimestampHeader, r.AuthHMACTimestampHeaderQuoted))
	}
	if r.AuthHMACNonceHeaderSet {
		fmt.Fprintf(b, "    nonce_header %s\n", formatValue(r.AuthHMACNonceHeader, r.AuthHMACNonceHeaderQuoted))
	}
	if r.AuthHMACToleranceSet {
		fmt.Fprintf(b, "    tolerance %s\n", formatValue(r.AuthHMACTolerance, r.AuthHMACToleranceQuoted))
	}
	b.WriteString("  }\n")
}

func writeRouteAuthForwardBlock(b *bytes.Buffer, f ForwardAuthBlock) {
	b.WriteString("  auth forward ")
	b.WriteString(formatValue(f.URL, f.URLQuoted))
	b.WriteString(" {\n")
	if f.TimeoutSet {
		fmt.Fprintf(b, "    timeout %s\n", formatValue(f.Timeout, f.TimeoutQuoted))
	}
	for i, name := range f.CopyHeaders {
		if strings.TrimSpace(name) == "" {
			continue
		}
		fmt.Fprintf(b, "    copy_headers %s\n", formatValue(name, quotedAt(f.CopyHeadersQuoted, i)))
	}
	if f.BodyLimitSet {
		fmt.Fprintf(b, "    body_limit %s\n", formatValue(f.BodyLimit, f.BodyLimitQuoted))
	}
	b.WriteString("  }\n")
}

func quoteString(s string) string {
	var out strings.Builder
	out.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			out.WriteString(`\\`)
		case '"':
			out.WriteString(`\"`)
		case '\n':
			out.WriteString(`\n`)
		case '\t':
			out.WriteString(`\t`)
		case '\r':
			out.WriteString(`\r`)
		default:
			out.WriteRune(r)
		}
	}
	out.WriteByte('"')
	return out.String()
}
