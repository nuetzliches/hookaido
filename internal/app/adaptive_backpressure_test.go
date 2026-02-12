package app

import (
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/queue"
)

type adaptiveTestStore struct {
	queue.Store
	statsFn func() (queue.Stats, error)
	trendFn func(req queue.BacklogTrendListRequest) (queue.BacklogTrendListResponse, error)
}

func (s *adaptiveTestStore) Stats() (queue.Stats, error) {
	if s != nil && s.statsFn != nil {
		return s.statsFn()
	}
	return s.Store.Stats()
}

func (s *adaptiveTestStore) CaptureBacklogTrendSample(time.Time) error {
	return nil
}

func (s *adaptiveTestStore) ListBacklogTrend(req queue.BacklogTrendListRequest) (queue.BacklogTrendListResponse, error) {
	if s != nil && s.trendFn != nil {
		return s.trendFn(req)
	}
	return queue.BacklogTrendListResponse{}, nil
}

func TestAdaptiveAdmissionController_QueuedPressure(t *testing.T) {
	c := newAdaptiveAdmissionController(
		config.AdaptiveBackpressureConfig{
			Enabled:         true,
			MinTotal:        1,
			QueuedPercent:   70,
			ReadyLag:        time.Hour,
			OldestQueuedAge: time.Hour,
			SustainedGrowth: false,
		},
		config.TrendSignalsConfig{},
	)
	c.setStore(&adaptiveTestStore{
		Store: queue.NewMemoryStore(),
		statsFn: func() (queue.Stats, error) {
			return queue.Stats{
				Total: 10,
				ByState: map[queue.State]int{
					queue.StateQueued: 8,
					queue.StateLeased: 2,
				},
			}, nil
		},
	})

	apply, reason := c.evaluate()
	if !apply {
		t.Fatal("expected adaptive backpressure to apply")
	}
	if reason != "queued_pressure" {
		t.Fatalf("expected reason queued_pressure, got %q", reason)
	}
}

func TestAdaptiveAdmissionController_ReadyLag(t *testing.T) {
	c := newAdaptiveAdmissionController(
		config.AdaptiveBackpressureConfig{
			Enabled:         true,
			MinTotal:        1,
			QueuedPercent:   90,
			ReadyLag:        time.Minute,
			OldestQueuedAge: time.Hour,
			SustainedGrowth: false,
		},
		config.TrendSignalsConfig{},
	)
	c.setStore(&adaptiveTestStore{
		Store: queue.NewMemoryStore(),
		statsFn: func() (queue.Stats, error) {
			return queue.Stats{
				Total:    10,
				ReadyLag: 2 * time.Minute,
				ByState: map[queue.State]int{
					queue.StateQueued: 2,
					queue.StateLeased: 8,
				},
			}, nil
		},
	})

	apply, reason := c.evaluate()
	if !apply {
		t.Fatal("expected adaptive backpressure to apply")
	}
	if reason != "ready_lag" {
		t.Fatalf("expected reason ready_lag, got %q", reason)
	}
}

func TestAdaptiveAdmissionController_SustainedGrowth(t *testing.T) {
	now := time.Date(2026, 2, 12, 20, 0, 0, 0, time.UTC)

	c := newAdaptiveAdmissionController(
		config.AdaptiveBackpressureConfig{
			Enabled:         true,
			MinTotal:        1,
			QueuedPercent:   95,
			ReadyLag:        time.Hour,
			OldestQueuedAge: time.Hour,
			SustainedGrowth: true,
		},
		config.TrendSignalsConfig{
			Window:                         15 * time.Minute,
			ExpectedCaptureInterval:        time.Minute,
			StaleGraceFactor:               3,
			SustainedGrowthConsecutive:     3,
			SustainedGrowthMinSamples:      5,
			SustainedGrowthMinDelta:        10,
			RecentSurgeMinTotal:            20,
			RecentSurgeMinDelta:            10,
			RecentSurgePercent:             50,
			DeadShareHighMinTotal:          10,
			DeadShareHighPercent:           20,
			QueuedPressureMinTotal:         20,
			QueuedPressurePercent:          75,
			QueuedPressureLeasedMultiplier: 2,
		},
	)
	c.now = func() time.Time { return now }
	c.setStore(&adaptiveTestStore{
		Store: queue.NewMemoryStore(),
		statsFn: func() (queue.Stats, error) {
			return queue.Stats{
				Total: 100,
				ByState: map[queue.State]int{
					queue.StateQueued: 10,
					queue.StateLeased: 90,
				},
			}, nil
		},
		trendFn: func(req queue.BacklogTrendListRequest) (queue.BacklogTrendListResponse, error) {
			samples := []queue.BacklogTrendSample{
				{CapturedAt: now.Add(-4 * time.Minute), Queued: 10},
				{CapturedAt: now.Add(-3 * time.Minute), Queued: 20},
				{CapturedAt: now.Add(-2 * time.Minute), Queued: 30},
				{CapturedAt: now.Add(-1 * time.Minute), Queued: 40},
				{CapturedAt: now, Queued: 50},
			}
			return queue.BacklogTrendListResponse{Items: samples}, nil
		},
	})

	apply, reason := c.evaluate()
	if !apply {
		t.Fatal("expected adaptive backpressure to apply")
	}
	if reason != "sustained_growth" {
		t.Fatalf("expected reason sustained_growth, got %q", reason)
	}
}
