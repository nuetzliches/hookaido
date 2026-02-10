package queue

import (
	"math"
	"sort"
	"time"
)

const (
	defaultBacklogTrendSignalWindow           = 15 * time.Minute
	defaultBacklogTrendSignalCaptureInterval  = time.Minute
	defaultBacklogTrendSignalStaleGraceFactor = 3
	defaultSustainedGrowthConsecutive         = 3
	defaultSustainedGrowthMinSamples          = 5
	defaultSustainedGrowthMinDelta            = 10
	defaultRecentSurgeMinTotal                = 20
	defaultRecentSurgeMinDelta                = 10
	defaultRecentSurgePercent                 = 50
	defaultDeadShareHighMinTotal              = 10
	defaultDeadShareHighPercent               = 20
	defaultQueuedPressureMinTotal             = 20
	defaultQueuedPressurePercent              = 75
	defaultQueuedPressureLeasedMultiplier     = 2
)

type BacklogTrendSignalConfig struct {
	Window                  time.Duration
	ExpectedCaptureInterval time.Duration
	StaleGraceFactor        int

	SustainedGrowthConsecutive int
	SustainedGrowthMinSamples  int
	SustainedGrowthMinDelta    int

	RecentSurgeMinTotal int
	RecentSurgeMinDelta int
	RecentSurgePercent  int

	DeadShareHighMinTotal int
	DeadShareHighPercent  int

	QueuedPressureMinTotal         int
	QueuedPressurePercent          int
	QueuedPressureLeasedMultiplier int
}

func DefaultBacklogTrendSignalConfig() BacklogTrendSignalConfig {
	return BacklogTrendSignalConfig{
		Window:                         defaultBacklogTrendSignalWindow,
		ExpectedCaptureInterval:        defaultBacklogTrendSignalCaptureInterval,
		StaleGraceFactor:               defaultBacklogTrendSignalStaleGraceFactor,
		SustainedGrowthConsecutive:     defaultSustainedGrowthConsecutive,
		SustainedGrowthMinSamples:      defaultSustainedGrowthMinSamples,
		SustainedGrowthMinDelta:        defaultSustainedGrowthMinDelta,
		RecentSurgeMinTotal:            defaultRecentSurgeMinTotal,
		RecentSurgeMinDelta:            defaultRecentSurgeMinDelta,
		RecentSurgePercent:             defaultRecentSurgePercent,
		DeadShareHighMinTotal:          defaultDeadShareHighMinTotal,
		DeadShareHighPercent:           defaultDeadShareHighPercent,
		QueuedPressureMinTotal:         defaultQueuedPressureMinTotal,
		QueuedPressurePercent:          defaultQueuedPressurePercent,
		QueuedPressureLeasedMultiplier: defaultQueuedPressureLeasedMultiplier,
	}
}

type BacklogTrendSignalOptions struct {
	Now                     time.Time
	Window                  time.Duration
	ExpectedCaptureInterval time.Duration
	Config                  BacklogTrendSignalConfig
}

type BacklogTrendSignals struct {
	Status                  string
	Window                  time.Duration
	ExpectedCaptureInterval time.Duration
	Since                   time.Time
	Until                   time.Time
	SampleCount             int
	Truncated               bool

	LatestCapturedAt time.Time
	LatestQueued     int
	LatestLeased     int
	LatestDead       int
	LatestTotal      int

	BaselineTotal        int
	DeltaTotal           int
	DeltaPercent         int
	GrowthRatePerMinute  float64
	ConsecutiveIncreases int

	SustainedGrowth bool
	RecentSurge     bool

	DeadSharePercent   int
	DeadShareHigh      bool
	QueuedSharePercent int
	QueuedPressure     bool

	FreshnessSeconds int
	SamplingStale    bool
	ActiveAlerts     []string
}

type BacklogTrendOperatorAction struct {
	ID              string
	Severity        string
	AlertRoutingKey string
	Summary         string
	Playbook        string
	Alerts          []string
	MCPTools        []string
	AdminEndpoints  []string
}

func AnalyzeBacklogTrendSignals(samples []BacklogTrendSample, truncated bool, opts BacklogTrendSignalOptions) BacklogTrendSignals {
	cfg := normalizeBacklogTrendSignalConfig(opts.Config)

	window := opts.Window
	if window <= 0 {
		window = cfg.Window
	}

	until := opts.Now.UTC()
	if until.IsZero() {
		until = time.Now().UTC()
	}
	since := until.Add(-window)

	expectedCaptureInterval := opts.ExpectedCaptureInterval
	if expectedCaptureInterval <= 0 {
		expectedCaptureInterval = cfg.ExpectedCaptureInterval
	}
	staleThreshold := expectedCaptureInterval * time.Duration(cfg.StaleGraceFactor)

	filtered := make([]BacklogTrendSample, 0, len(samples))
	for _, sample := range samples {
		at := sample.CapturedAt.UTC()
		if at.Before(since) || at.After(until) {
			continue
		}
		filtered = append(filtered, BacklogTrendSample{
			CapturedAt: at,
			Queued:     sample.Queued,
			Leased:     sample.Leased,
			Dead:       sample.Dead,
		})
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CapturedAt.Before(filtered[j].CapturedAt)
	})

	out := BacklogTrendSignals{
		Status:                  "ok",
		Window:                  window,
		ExpectedCaptureInterval: expectedCaptureInterval,
		Since:                   since,
		Until:                   until,
		SampleCount:             len(filtered),
		Truncated:               truncated,
		ActiveAlerts:            make([]string, 0, 5),
	}
	if len(filtered) == 0 {
		out.SamplingStale = true
		out.FreshnessSeconds = int(window / time.Second)
		out.ActiveAlerts = append(out.ActiveAlerts, "sampling_stale")
		out.Status = "warn"
		return out
	}

	latest := filtered[len(filtered)-1]
	latestTotal := backlogTrendTotal(latest)

	out.LatestCapturedAt = latest.CapturedAt
	out.LatestQueued = latest.Queued
	out.LatestLeased = latest.Leased
	out.LatestDead = latest.Dead
	out.LatestTotal = latestTotal
	out.DeadSharePercent = percentRounded(latest.Dead, latestTotal)
	out.QueuedSharePercent = percentRounded(latest.Queued, latestTotal)

	history := filtered[:len(filtered)-1]
	if len(history) == 0 {
		out.BaselineTotal = latestTotal
	} else {
		sum := 0
		for _, sample := range history {
			sum += backlogTrendTotal(sample)
		}
		out.BaselineTotal = int(math.Round(float64(sum) / float64(len(history))))
	}

	out.DeltaTotal = out.LatestTotal - out.BaselineTotal
	switch {
	case out.BaselineTotal > 0:
		out.DeltaPercent = int(math.Round(float64(out.DeltaTotal) * 100 / float64(out.BaselineTotal)))
	case out.LatestTotal > 0:
		out.DeltaPercent = 100
	}

	if len(filtered) >= 2 {
		earliest := filtered[0]
		earliestTotal := backlogTrendTotal(earliest)
		elapsed := latest.CapturedAt.Sub(earliest.CapturedAt)
		if elapsed > 0 {
			rate := float64(latestTotal-earliestTotal) / elapsed.Minutes()
			out.GrowthRatePerMinute = roundTenth(rate)
		}
	}

	for i := len(filtered) - 1; i > 0; i-- {
		cur := backlogTrendTotal(filtered[i])
		prev := backlogTrendTotal(filtered[i-1])
		if cur > prev {
			out.ConsecutiveIncreases++
			continue
		}
		break
	}

	out.SustainedGrowth = out.ConsecutiveIncreases >= cfg.SustainedGrowthConsecutive || (len(filtered) >= cfg.SustainedGrowthMinSamples && out.DeltaTotal >= cfg.SustainedGrowthMinDelta && out.DeltaTotal > 0)
	if out.BaselineTotal <= 0 {
		out.RecentSurge = out.LatestTotal >= cfg.RecentSurgeMinTotal
	} else {
		surgeThreshold := out.BaselineTotal + maxInt(cfg.RecentSurgeMinDelta, int(math.Ceil(float64(out.BaselineTotal)*float64(cfg.RecentSurgePercent)/100.0)))
		if surgeThreshold < cfg.RecentSurgeMinTotal {
			surgeThreshold = cfg.RecentSurgeMinTotal
		}
		out.RecentSurge = out.LatestTotal >= surgeThreshold
	}

	out.DeadShareHigh = out.LatestTotal >= cfg.DeadShareHighMinTotal && out.DeadSharePercent >= cfg.DeadShareHighPercent
	out.QueuedPressure = out.LatestTotal >= cfg.QueuedPressureMinTotal && out.QueuedSharePercent >= cfg.QueuedPressurePercent && out.LatestQueued >= out.LatestLeased*cfg.QueuedPressureLeasedMultiplier

	if !until.Before(out.LatestCapturedAt) {
		out.FreshnessSeconds = int(until.Sub(out.LatestCapturedAt) / time.Second)
		out.SamplingStale = until.Sub(out.LatestCapturedAt) > staleThreshold
	}

	if out.SamplingStale {
		out.ActiveAlerts = append(out.ActiveAlerts, "sampling_stale")
	}
	if out.SustainedGrowth {
		out.ActiveAlerts = append(out.ActiveAlerts, "sustained_growth")
	}
	if out.RecentSurge {
		out.ActiveAlerts = append(out.ActiveAlerts, "recent_surge")
	}
	if out.DeadShareHigh {
		out.ActiveAlerts = append(out.ActiveAlerts, "dead_share_high")
	}
	if out.QueuedPressure {
		out.ActiveAlerts = append(out.ActiveAlerts, "queued_pressure")
	}
	if len(out.ActiveAlerts) > 0 {
		out.Status = "warn"
	}

	return out
}

func (s BacklogTrendSignals) Map() map[string]any {
	operatorActions := s.OperatorActions()
	operatorActionMaps := make([]map[string]any, 0, len(operatorActions))
	for _, action := range operatorActions {
		operatorActionMaps = append(operatorActionMaps, action.Map())
	}

	out := map[string]any{
		"status":                    s.Status,
		"window":                    s.Window.String(),
		"expected_capture_interval": s.ExpectedCaptureInterval.String(),
		"since":                     s.Since.UTC().Format(time.RFC3339Nano),
		"until":                     s.Until.UTC().Format(time.RFC3339Nano),
		"sample_count":              s.SampleCount,
		"truncated":                 s.Truncated,
		"latest_total":              s.LatestTotal,
		"latest_queued":             s.LatestQueued,
		"latest_leased":             s.LatestLeased,
		"latest_dead":               s.LatestDead,
		"baseline_total":            s.BaselineTotal,
		"delta_total":               s.DeltaTotal,
		"delta_percent":             s.DeltaPercent,
		"growth_rate_per_minute":    s.GrowthRatePerMinute,
		"consecutive_increases":     s.ConsecutiveIncreases,
		"sustained_growth":          s.SustainedGrowth,
		"recent_surge":              s.RecentSurge,
		"dead_share_percent":        s.DeadSharePercent,
		"dead_share_high":           s.DeadShareHigh,
		"queued_share_percent":      s.QueuedSharePercent,
		"queued_pressure":           s.QueuedPressure,
		"freshness_seconds":         s.FreshnessSeconds,
		"sampling_stale":            s.SamplingStale,
		"active_alerts":             s.ActiveAlerts,
		"operator_actions":          operatorActionMaps,
	}
	if !s.LatestCapturedAt.IsZero() {
		out["latest_captured_at"] = s.LatestCapturedAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func (s BacklogTrendSignals) OperatorActions() []BacklogTrendOperatorAction {
	actions := make([]BacklogTrendOperatorAction, 0, 4)

	if s.SamplingStale {
		actions = append(actions, BacklogTrendOperatorAction{
			ID:              "sampling_stale_recover",
			Severity:        "critical",
			AlertRoutingKey: "hookaido.backlog.sampling_stale",
			Summary:         "Backlog trend sampling is stale; diagnostics are likely incomplete.",
			Playbook:        "Check runtime process health and sampler cadence, then restore snapshot capture before relying on trend-based decisions.",
			Alerts:          []string{"sampling_stale"},
			MCPTools:        []string{"admin_health", "instance_status", "instance_reload"},
			AdminEndpoints:  []string{"/healthz?details=1", "/backlog/trends"},
		})
	}

	if s.QueuedPressure {
		actions = append(actions, BacklogTrendOperatorAction{
			ID:              "queued_pressure_relieve",
			Severity:        "critical",
			AlertRoutingKey: "hookaido.backlog.queued_pressure",
			Summary:         "Queued backlog pressure is high relative to leased work.",
			Playbook:        "Identify the hottest route/target backlog, verify worker/dispatcher throughput, and apply scoped lifecycle controls with preview before mutating.",
			Alerts:          []string{"queued_pressure"},
			MCPTools:        []string{"backlog_top_queued", "backlog_oldest_queued", "messages_list", "attempts_list", "messages_cancel_by_filter"},
			AdminEndpoints:  []string{"/backlog/top_queued", "/backlog/oldest_queued", "/messages", "/attempts", "/messages/cancel_by_filter"},
		})
	}

	if s.DeadShareHigh {
		actions = append(actions, BacklogTrendOperatorAction{
			ID:              "dead_share_drain",
			Severity:        "warn",
			AlertRoutingKey: "hookaido.backlog.dead_share_high",
			Summary:         "Dead-letter share is elevated in current backlog.",
			Playbook:        "Inspect recent delivery errors and DLQ cohorts, fix destination/policy issues, then requeue only validated dead-letter subsets.",
			Alerts:          []string{"dead_share_high"},
			MCPTools:        []string{"attempts_list", "dlq_list", "dlq_requeue"},
			AdminEndpoints:  []string{"/attempts", "/dlq", "/dlq/requeue"},
		})
	}

	if s.SustainedGrowth || s.RecentSurge {
		severity := "warn"
		summary := "Sustained backlog growth detected."
		playbook := "Inspect growth hotspots and backlog age profile, then increase drain capacity or apply scoped backlog controls."
		alertRoutingKey := "hookaido.backlog.sustained_growth"
		alerts := make([]string, 0, 2)
		if s.SustainedGrowth {
			alerts = append(alerts, "sustained_growth")
		}
		if s.RecentSurge {
			severity = "critical"
			summary = "Recent backlog surge detected."
			playbook = "Prioritize immediate hotspot triage and delivery-path checks; use bounded lifecycle controls to prevent uncontrolled backlog expansion."
			alertRoutingKey = "hookaido.backlog.recent_surge"
			alerts = append(alerts, "recent_surge")
		}
		actions = append(actions, BacklogTrendOperatorAction{
			ID:              "backlog_growth_triage",
			Severity:        severity,
			AlertRoutingKey: alertRoutingKey,
			Summary:         summary,
			Playbook:        playbook,
			Alerts:          alerts,
			MCPTools:        []string{"backlog_top_queued", "backlog_aging_summary", "backlog_trends", "messages_list"},
			AdminEndpoints:  []string{"/backlog/top_queued", "/backlog/aging_summary", "/backlog/trends", "/messages"},
		})
	}

	return actions
}

func (a BacklogTrendOperatorAction) Map() map[string]any {
	alerts := make([]string, 0, len(a.Alerts))
	alerts = append(alerts, a.Alerts...)
	mcpTools := make([]string, 0, len(a.MCPTools))
	mcpTools = append(mcpTools, a.MCPTools...)
	adminEndpoints := make([]string, 0, len(a.AdminEndpoints))
	adminEndpoints = append(adminEndpoints, a.AdminEndpoints...)

	return map[string]any{
		"id":                a.ID,
		"severity":          a.Severity,
		"alert_routing_key": a.AlertRoutingKey,
		"summary":           a.Summary,
		"playbook":          a.Playbook,
		"alerts":            alerts,
		"mcp_tools":         mcpTools,
		"admin_endpoints":   adminEndpoints,
	}
}

func backlogTrendTotal(sample BacklogTrendSample) int {
	return sample.Queued + sample.Leased + sample.Dead
}

func percentRounded(part, total int) int {
	if part <= 0 || total <= 0 {
		return 0
	}
	return int(math.Round(float64(part) * 100 / float64(total)))
}

func roundTenth(v float64) float64 {
	return math.Round(v*10) / 10
}

func maxInt(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

func normalizeBacklogTrendSignalConfig(cfg BacklogTrendSignalConfig) BacklogTrendSignalConfig {
	out := DefaultBacklogTrendSignalConfig()

	if cfg.Window > 0 {
		out.Window = cfg.Window
	}
	if cfg.ExpectedCaptureInterval > 0 {
		out.ExpectedCaptureInterval = cfg.ExpectedCaptureInterval
	}
	if cfg.StaleGraceFactor > 0 {
		out.StaleGraceFactor = cfg.StaleGraceFactor
	}
	if cfg.SustainedGrowthConsecutive > 0 {
		out.SustainedGrowthConsecutive = cfg.SustainedGrowthConsecutive
	}
	if cfg.SustainedGrowthMinSamples > 0 {
		out.SustainedGrowthMinSamples = cfg.SustainedGrowthMinSamples
	}
	if cfg.SustainedGrowthMinDelta > 0 {
		out.SustainedGrowthMinDelta = cfg.SustainedGrowthMinDelta
	}
	if cfg.RecentSurgeMinTotal > 0 {
		out.RecentSurgeMinTotal = cfg.RecentSurgeMinTotal
	}
	if cfg.RecentSurgeMinDelta > 0 {
		out.RecentSurgeMinDelta = cfg.RecentSurgeMinDelta
	}
	if cfg.RecentSurgePercent > 0 {
		out.RecentSurgePercent = cfg.RecentSurgePercent
	}
	if cfg.DeadShareHighMinTotal > 0 {
		out.DeadShareHighMinTotal = cfg.DeadShareHighMinTotal
	}
	if cfg.DeadShareHighPercent > 0 {
		out.DeadShareHighPercent = cfg.DeadShareHighPercent
	}
	if cfg.QueuedPressureMinTotal > 0 {
		out.QueuedPressureMinTotal = cfg.QueuedPressureMinTotal
	}
	if cfg.QueuedPressurePercent > 0 {
		out.QueuedPressurePercent = cfg.QueuedPressurePercent
	}
	if cfg.QueuedPressureLeasedMultiplier > 0 {
		out.QueuedPressureLeasedMultiplier = cfg.QueuedPressureLeasedMultiplier
	}

	return out
}
