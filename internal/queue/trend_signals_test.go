package queue

import (
	"testing"
	"time"
)

func TestAnalyzeBacklogTrendSignalsGrowthAndPressure(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 5, 0, 0, time.UTC)
	samples := []BacklogTrendSample{
		{CapturedAt: now.Add(-4 * time.Minute), Queued: 8, Leased: 2, Dead: 0},  // total=10
		{CapturedAt: now.Add(-3 * time.Minute), Queued: 11, Leased: 3, Dead: 0}, // total=14
		{CapturedAt: now.Add(-2 * time.Minute), Queued: 16, Leased: 4, Dead: 0}, // total=20
		{CapturedAt: now.Add(-1 * time.Minute), Queued: 24, Leased: 5, Dead: 1}, // total=30
		{CapturedAt: now, Queued: 36, Leased: 6, Dead: 3},                       // total=45
	}

	signals := AnalyzeBacklogTrendSignals(samples, false, BacklogTrendSignalOptions{
		Now:                     now,
		Window:                  10 * time.Minute,
		ExpectedCaptureInterval: time.Minute,
	})

	if signals.SampleCount != 5 {
		t.Fatalf("expected sample_count=5, got %d", signals.SampleCount)
	}
	if signals.LatestTotal != 45 || signals.BaselineTotal != 19 {
		t.Fatalf("unexpected totals: %+v", signals)
	}
	if signals.DeltaTotal != 26 || signals.DeltaPercent != 137 {
		t.Fatalf("unexpected delta: %+v", signals)
	}
	if signals.ConsecutiveIncreases != 4 {
		t.Fatalf("expected consecutive_increases=4, got %d", signals.ConsecutiveIncreases)
	}
	if !signals.SustainedGrowth {
		t.Fatalf("expected sustained_growth=true")
	}
	if !signals.RecentSurge {
		t.Fatalf("expected recent_surge=true")
	}
	if !signals.QueuedPressure {
		t.Fatalf("expected queued_pressure=true")
	}
	if signals.SamplingStale {
		t.Fatalf("expected sampling_stale=false")
	}
	if signals.Status != "warn" {
		t.Fatalf("expected status=warn, got %q", signals.Status)
	}
	if !sliceContains(signals.ActiveAlerts, "sustained_growth") || !sliceContains(signals.ActiveAlerts, "recent_surge") || !sliceContains(signals.ActiveAlerts, "queued_pressure") {
		t.Fatalf("expected active alerts for growth/surge/pressure, got %#v", signals.ActiveAlerts)
	}
	actionIDs := operatorActionIDs(signals.OperatorActions())
	if !sliceContains(actionIDs, "backlog_growth_triage") || !sliceContains(actionIDs, "queued_pressure_relieve") {
		t.Fatalf("expected growth and pressure operator actions, got %#v", actionIDs)
	}
	m := signals.Map()
	if _, ok := m["operator_actions"]; !ok {
		t.Fatalf("expected operator_actions in signal map")
	}
}

func TestAnalyzeBacklogTrendSignalsStaleAndDeadShare(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	samples := []BacklogTrendSample{
		{CapturedAt: now.Add(-10 * time.Minute), Queued: 8, Leased: 6, Dead: 6}, // total=20, dead=30%
	}

	signals := AnalyzeBacklogTrendSignals(samples, false, BacklogTrendSignalOptions{
		Now:                     now,
		Window:                  15 * time.Minute,
		ExpectedCaptureInterval: time.Minute,
	})
	if signals.SampleCount != 1 {
		t.Fatalf("expected sample_count=1, got %d", signals.SampleCount)
	}
	if !signals.SamplingStale {
		t.Fatalf("expected sampling_stale=true")
	}
	if signals.FreshnessSeconds != 600 {
		t.Fatalf("expected freshness_seconds=600, got %d", signals.FreshnessSeconds)
	}
	if !signals.DeadShareHigh || signals.DeadSharePercent != 30 {
		t.Fatalf("expected dead_share_high with 30%%, got %+v", signals)
	}
	if signals.Status != "warn" {
		t.Fatalf("expected status=warn, got %q", signals.Status)
	}
	if !sliceContains(signals.ActiveAlerts, "sampling_stale") || !sliceContains(signals.ActiveAlerts, "dead_share_high") {
		t.Fatalf("expected sampling_stale and dead_share_high alerts, got %#v", signals.ActiveAlerts)
	}
	actionIDs := operatorActionIDs(signals.OperatorActions())
	if !sliceContains(actionIDs, "sampling_stale_recover") || !sliceContains(actionIDs, "dead_share_drain") {
		t.Fatalf("expected stale and dead-share operator actions, got %#v", actionIDs)
	}
}

func TestAnalyzeBacklogTrendSignalsNoSamples(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	signals := AnalyzeBacklogTrendSignals(nil, true, BacklogTrendSignalOptions{
		Now:                     now,
		Window:                  20 * time.Minute,
		ExpectedCaptureInterval: time.Minute,
	})
	if signals.SampleCount != 0 {
		t.Fatalf("expected sample_count=0, got %d", signals.SampleCount)
	}
	if !signals.Truncated {
		t.Fatalf("expected truncated=true")
	}
	if !signals.SamplingStale {
		t.Fatalf("expected sampling_stale=true for empty samples")
	}
	if signals.Status != "warn" {
		t.Fatalf("expected status=warn, got %q", signals.Status)
	}
	if !sliceContains(signals.ActiveAlerts, "sampling_stale") {
		t.Fatalf("expected sampling_stale alert, got %#v", signals.ActiveAlerts)
	}
	actionIDs := operatorActionIDs(signals.OperatorActions())
	if !sliceContains(actionIDs, "sampling_stale_recover") {
		t.Fatalf("expected sampling_stale_recover action, got %#v", actionIDs)
	}
}

func TestAnalyzeBacklogTrendSignalsCustomConfig(t *testing.T) {
	now := time.Date(2026, 2, 7, 12, 5, 0, 0, time.UTC)
	samples := []BacklogTrendSample{
		{CapturedAt: now.Add(-2 * time.Minute), Queued: 10, Leased: 5, Dead: 0}, // total=15
		{CapturedAt: now.Add(-1 * time.Minute), Queued: 12, Leased: 6, Dead: 0}, // total=18
		{CapturedAt: now, Queued: 14, Leased: 7, Dead: 0},                       // total=21
	}

	signals := AnalyzeBacklogTrendSignals(samples, false, BacklogTrendSignalOptions{
		Now: now,
		Config: BacklogTrendSignalConfig{
			SustainedGrowthConsecutive:     5,
			SustainedGrowthMinSamples:      10,
			SustainedGrowthMinDelta:        100,
			RecentSurgeMinTotal:            100,
			RecentSurgeMinDelta:            100,
			RecentSurgePercent:             300,
			DeadShareHighMinTotal:          100,
			DeadShareHighPercent:           90,
			QueuedPressureMinTotal:         100,
			QueuedPressurePercent:          95,
			QueuedPressureLeasedMultiplier: 10,
		},
	})

	if signals.SustainedGrowth || signals.RecentSurge || signals.DeadShareHigh || signals.QueuedPressure {
		t.Fatalf("expected all custom threshold alerts off, got %+v", signals)
	}
	if signals.Status != "ok" {
		t.Fatalf("expected status=ok, got %q", signals.Status)
	}
	if len(signals.OperatorActions()) != 0 {
		t.Fatalf("expected no operator actions, got %#v", signals.OperatorActions())
	}
}

func sliceContains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func operatorActionIDs(actions []BacklogTrendOperatorAction) []string {
	out := make([]string, 0, len(actions))
	for _, action := range actions {
		out = append(out, action.ID)
	}
	return out
}
