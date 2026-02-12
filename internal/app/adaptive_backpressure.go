package app

import (
	"math"
	"sync"
	"time"

	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/queue"
)

const (
	defaultAdaptiveStatsCacheTTL        = 250 * time.Millisecond
	defaultAdaptiveTrendSignalsCacheTTL = time.Second
	adaptiveTrendSignalSamples          = 512
)

type adaptiveAdmissionController struct {
	mu       sync.RWMutex
	cfg      config.AdaptiveBackpressureConfig
	trendCfg config.TrendSignalsConfig
	store    queue.Store
	now      func() time.Time

	stats struct {
		mu         sync.Mutex
		ttl        time.Duration
		cached     queue.Stats
		cachedAt   time.Time
		cachedOK   bool
		refreshing bool
	}

	trend struct {
		mu         sync.Mutex
		ttl        time.Duration
		cached     queue.BacklogTrendSignals
		cachedAt   time.Time
		cachedOK   bool
		refreshing bool
	}
}

func newAdaptiveAdmissionController(cfg config.AdaptiveBackpressureConfig, trendCfg config.TrendSignalsConfig) *adaptiveAdmissionController {
	c := &adaptiveAdmissionController{
		cfg:      cfg,
		trendCfg: trendCfg,
		now:      time.Now,
	}
	c.stats.ttl = defaultAdaptiveStatsCacheTTL
	c.trend.ttl = defaultAdaptiveTrendSignalsCacheTTL
	return c
}

func (c *adaptiveAdmissionController) setStore(store queue.Store) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.store = store
	c.mu.Unlock()

	c.stats.mu.Lock()
	c.stats.cached = queue.Stats{}
	c.stats.cachedAt = time.Time{}
	c.stats.cachedOK = false
	c.stats.refreshing = false
	c.stats.mu.Unlock()

	c.trend.mu.Lock()
	c.trend.cached = queue.BacklogTrendSignals{}
	c.trend.cachedAt = time.Time{}
	c.trend.cachedOK = false
	c.trend.refreshing = false
	c.trend.mu.Unlock()
}

func (c *adaptiveAdmissionController) updateConfig(cfg config.AdaptiveBackpressureConfig, trendCfg config.TrendSignalsConfig) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.cfg = cfg
	c.trendCfg = trendCfg
	c.mu.Unlock()

	// Trend interpretation depends on trend config; force quick recompute.
	c.trend.mu.Lock()
	c.trend.cached = queue.BacklogTrendSignals{}
	c.trend.cachedAt = time.Time{}
	c.trend.cachedOK = false
	c.trend.refreshing = false
	c.trend.mu.Unlock()
}

func (c *adaptiveAdmissionController) evaluate() (apply bool, reason string) {
	if c == nil {
		return false, ""
	}

	cfg, _, _ := c.snapshot()
	if !cfg.Enabled {
		return false, ""
	}

	stats, ok := c.statsSnapshot()
	if !ok {
		// Fail open when pressure signals are unavailable.
		return false, ""
	}
	activeTotal := adaptiveActiveTotal(stats)
	if activeTotal < cfg.MinTotal {
		return false, ""
	}

	queuedPercent := adaptiveQueuedPercent(stats, activeTotal)
	if queuedPercent >= cfg.QueuedPercent {
		return true, "queued_pressure"
	}
	if cfg.ReadyLag > 0 && stats.ReadyLag >= cfg.ReadyLag {
		return true, "ready_lag"
	}
	if cfg.OldestQueuedAge > 0 && stats.OldestQueuedAge >= cfg.OldestQueuedAge {
		return true, "oldest_queued_age"
	}
	if cfg.SustainedGrowth && c.sustainedGrowthActive() {
		return true, "sustained_growth"
	}
	return false, ""
}

func adaptiveActiveTotal(stats queue.Stats) int {
	return stats.ByState[queue.StateQueued] + stats.ByState[queue.StateLeased]
}

func adaptiveQueuedPercent(stats queue.Stats, activeTotal int) int {
	if activeTotal <= 0 {
		return 0
	}
	queued := stats.ByState[queue.StateQueued]
	return int(math.Round(float64(queued) * 100 / float64(activeTotal)))
}

func (c *adaptiveAdmissionController) snapshot() (config.AdaptiveBackpressureConfig, config.TrendSignalsConfig, queue.Store) {
	c.mu.RLock()
	cfg := c.cfg
	trendCfg := c.trendCfg
	store := c.store
	c.mu.RUnlock()
	return cfg, trendCfg, store
}

func (c *adaptiveAdmissionController) nowUTC() time.Time {
	c.mu.RLock()
	nowFn := c.now
	c.mu.RUnlock()
	now := time.Now()
	if nowFn != nil {
		now = nowFn()
	}
	return now.UTC()
}

func (c *adaptiveAdmissionController) statsSnapshot() (queue.Stats, bool) {
	if c == nil {
		return queue.Stats{}, false
	}
	_, _, store := c.snapshot()
	if store == nil {
		return queue.Stats{}, false
	}

	now := c.nowUTC()
	c.stats.mu.Lock()
	if c.stats.cachedOK && (c.stats.ttl <= 0 || now.Sub(c.stats.cachedAt) <= c.stats.ttl) {
		stats := c.stats.cached
		c.stats.mu.Unlock()
		return stats, true
	}

	// Cold-start fetch happens once synchronously.
	if !c.stats.cachedOK {
		if c.stats.refreshing {
			c.stats.mu.Unlock()
			return queue.Stats{}, false
		}
		c.stats.refreshing = true
		c.stats.mu.Unlock()
		return c.refreshStatsSync(store)
	}

	// Stale cache: return cached snapshot and refresh in the background.
	if !c.stats.refreshing {
		c.stats.refreshing = true
		go c.refreshStatsAsync(store)
	}
	stats := c.stats.cached
	c.stats.mu.Unlock()
	return stats, true
}

func (c *adaptiveAdmissionController) refreshStatsSync(store queue.Store) (queue.Stats, bool) {
	stats, err := store.Stats()
	at := c.nowUTC()

	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	c.stats.refreshing = false
	if err == nil {
		c.stats.cached = stats
		c.stats.cachedAt = at
		c.stats.cachedOK = true
		return stats, true
	}
	if c.stats.cachedOK {
		return c.stats.cached, true
	}
	return queue.Stats{}, false
}

func (c *adaptiveAdmissionController) refreshStatsAsync(store queue.Store) {
	stats, err := store.Stats()
	at := c.nowUTC()

	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	c.stats.refreshing = false
	if err != nil {
		return
	}
	c.stats.cached = stats
	c.stats.cachedAt = at
	c.stats.cachedOK = true
}

func (c *adaptiveAdmissionController) sustainedGrowthActive() bool {
	signals, ok := c.trendSnapshot()
	if !ok {
		return false
	}
	return signals.SustainedGrowth
}

func (c *adaptiveAdmissionController) trendSnapshot() (queue.BacklogTrendSignals, bool) {
	if c == nil {
		return queue.BacklogTrendSignals{}, false
	}
	_, trendCfg, store := c.snapshot()
	trendStore, ok := store.(queue.BacklogTrendStore)
	if !ok || trendStore == nil {
		return queue.BacklogTrendSignals{}, false
	}

	now := c.nowUTC()
	c.trend.mu.Lock()
	if c.trend.cachedOK && (c.trend.ttl <= 0 || now.Sub(c.trend.cachedAt) <= c.trend.ttl) {
		signals := c.trend.cached
		c.trend.mu.Unlock()
		return signals, true
	}

	if !c.trend.cachedOK {
		if c.trend.refreshing {
			c.trend.mu.Unlock()
			return queue.BacklogTrendSignals{}, false
		}
		c.trend.refreshing = true
		c.trend.mu.Unlock()
		return c.refreshTrendSync(trendStore, trendCfg, now)
	}

	if !c.trend.refreshing {
		c.trend.refreshing = true
		go c.refreshTrendAsync(trendStore, trendCfg, now)
	}
	signals := c.trend.cached
	c.trend.mu.Unlock()
	return signals, true
}

func (c *adaptiveAdmissionController) refreshTrendSync(trendStore queue.BacklogTrendStore, trendCfg config.TrendSignalsConfig, now time.Time) (queue.BacklogTrendSignals, bool) {
	signals, err := computeTrendSignals(trendStore, trendCfg, now)

	c.trend.mu.Lock()
	defer c.trend.mu.Unlock()
	c.trend.refreshing = false
	if err == nil {
		c.trend.cached = signals
		c.trend.cachedAt = now
		c.trend.cachedOK = true
		return signals, true
	}
	if c.trend.cachedOK {
		return c.trend.cached, true
	}
	return queue.BacklogTrendSignals{}, false
}

func (c *adaptiveAdmissionController) refreshTrendAsync(trendStore queue.BacklogTrendStore, trendCfg config.TrendSignalsConfig, now time.Time) {
	signals, err := computeTrendSignals(trendStore, trendCfg, now)

	c.trend.mu.Lock()
	defer c.trend.mu.Unlock()
	c.trend.refreshing = false
	if err != nil {
		return
	}
	c.trend.cached = signals
	c.trend.cachedAt = now
	c.trend.cachedOK = true
}

func computeTrendSignals(trendStore queue.BacklogTrendStore, trendCfg config.TrendSignalsConfig, now time.Time) (queue.BacklogTrendSignals, error) {
	resp, err := trendStore.ListBacklogTrend(queue.BacklogTrendListRequest{
		Since: now.Add(-trendCfg.Window),
		Until: now,
		Limit: adaptiveTrendSignalSamples,
	})
	if err != nil {
		return queue.BacklogTrendSignals{}, err
	}
	signals := queue.AnalyzeBacklogTrendSignals(resp.Items, resp.Truncated, queue.BacklogTrendSignalOptions{
		Now:    now,
		Config: queueTrendSignalConfigFromCompiled(trendCfg),
	})
	return signals, nil
}
