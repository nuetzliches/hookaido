package admin

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/backlog"
	"github.com/nuetzliches/hookaido/internal/queue"
)

func backlogTopQueuedFromStats(stats queue.Stats, routeFilter, targetFilter string, limit int) backlogTopQueuedResponse {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > backlog.MaxListLimit {
		limit = backlog.MaxListLimit
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

	items := make([]backlogTopQueuedItem, 0, len(filtered))
	for _, b := range filtered {
		item := backlogTopQueuedItem{
			Route:  b.Route,
			Target: b.Target,
			Queued: b.Queued,
		}
		if !b.OldestQueuedReceivedAt.IsZero() {
			item.OldestQueuedReceivedAt = b.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
			item.OldestQueuedAgeSeconds = int(b.OldestQueuedAge / time.Second)
		}
		if !b.EarliestQueuedNextRun.IsZero() {
			item.EarliestQueuedNextRun = b.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
			item.ReadyLagSeconds = int(b.ReadyLag / time.Second)
		}
		items = append(items, item)
	}

	out := backlogTopQueuedResponse{
		QueueTotal:    stats.Total,
		QueuedTotal:   queuedTotal,
		Limit:         limit,
		Truncated:     truncated,
		SourceBounded: sourceQueued < queuedTotal,
		Items:         items,
	}
	if !stats.OldestQueuedReceivedAt.IsZero() {
		out.OldestQueuedReceivedAt = stats.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
		out.OldestQueuedAgeSeconds = int(stats.OldestQueuedAge / time.Second)
	}
	if !stats.EarliestQueuedNextRun.IsZero() {
		out.EarliestQueuedNextRunAt = stats.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
		out.ReadyLagSeconds = int(stats.ReadyLag / time.Second)
	}
	return out
}

func backlogOldestQueuedFromMessages(now time.Time, stats queue.Stats, routeFilter, targetFilter string, limit int, truncated bool, items []queue.Envelope) backlogOldestQueuedResponse {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > backlog.MaxListLimit {
		limit = backlog.MaxListLimit
	}

	converted := make([]backlogOldestQueuedItem, 0, len(items))
	for _, it := range items {
		item := backlogOldestQueuedItem{
			ID:      it.ID,
			Route:   it.Route,
			Target:  it.Target,
			Attempt: it.Attempt,
		}
		if !it.ReceivedAt.IsZero() {
			item.ReceivedAt = it.ReceivedAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(it.ReceivedAt) {
				item.OldestQueuedAgeSeconds = int(now.Sub(it.ReceivedAt) / time.Second)
			}
		}
		if !it.NextRunAt.IsZero() {
			item.NextRunAt = it.NextRunAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(it.NextRunAt) {
				item.ReadyLagSeconds = int(now.Sub(it.NextRunAt) / time.Second)
			}
		}
		converted = append(converted, item)
	}

	out := backlogOldestQueuedResponse{
		QueueTotal:  stats.Total,
		QueuedTotal: stats.ByState[queue.StateQueued],
		Limit:       limit,
		Returned:    len(converted),
		Truncated:   truncated,
		Selector: backlogSelector{
			Route:  routeFilter,
			Target: targetFilter,
		},
		Items: converted,
	}
	if !stats.OldestQueuedReceivedAt.IsZero() {
		out.OldestQueuedReceivedAt = stats.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
		out.OldestQueuedAgeSeconds = int(stats.OldestQueuedAge / time.Second)
	}
	if !stats.EarliestQueuedNextRun.IsZero() {
		out.EarliestQueuedNextRunAt = stats.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
		out.ReadyLagSeconds = int(stats.ReadyLag / time.Second)
	}
	return out
}

func backlogAgingSummaryFromMessages(now time.Time, stats queue.Stats, routeFilter, targetFilter string, states []queue.State, limit int, truncated bool, stateScan map[queue.State]backlog.StateScanSummary, items []queue.Envelope) backlogAgingSummaryResponse {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > backlog.MaxListLimit {
		limit = backlog.MaxListLimit
	}

	type agg struct {
		Route             string
		Target            string
		StateCounts       map[queue.State]int
		TotalObserved     int
		AgeSamples        []int
		AgeWindows        backlog.AgeWindows
		OldestReceivedAt  time.Time
		NewestReceivedAt  time.Time
		EarliestNextRun   time.Time
		ReadyOverdueCount int
		MaxReadyLag       time.Duration
	}

	buckets := make(map[string]*agg)
	allAgeSamples := make([]int, 0, len(items))
	var allAgeWindows backlog.AgeWindows
	for _, it := range items {
		key := it.Route + "\x00" + it.Target
		a := buckets[key]
		if a == nil {
			a = &agg{
				Route:            it.Route,
				Target:           it.Target,
				StateCounts:      make(map[queue.State]int),
				OldestReceivedAt: it.ReceivedAt,
				NewestReceivedAt: it.ReceivedAt,
				EarliestNextRun:  it.NextRunAt,
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
		if !it.NextRunAt.IsZero() && (a.EarliestNextRun.IsZero() || it.NextRunAt.Before(a.EarliestNextRun)) {
			a.EarliestNextRun = it.NextRunAt
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
			backlog.ObserveAgeWindow(ageSeconds, &a.AgeWindows)
			allAgeSamples = append(allAgeSamples, ageSeconds)
			backlog.ObserveAgeWindow(ageSeconds, &allAgeWindows)
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

	outItems := make([]backlogAgingSummaryItem, 0, len(ordered))
	for _, a := range ordered {
		item := backlogAgingSummaryItem{
			Route:          a.Route,
			Target:         a.Target,
			TotalObserved:  a.TotalObserved,
			QueuedObserved: a.StateCounts[queue.StateQueued],
			LeasedObserved: a.StateCounts[queue.StateLeased],
			DeadObserved:   a.StateCounts[queue.StateDead],
			AgeSampleCount: len(a.AgeSamples),
			AgeWindows:     a.AgeWindows,
			StateCounts: map[string]int{
				string(queue.StateQueued): a.StateCounts[queue.StateQueued],
				string(queue.StateLeased): a.StateCounts[queue.StateLeased],
				string(queue.StateDead):   a.StateCounts[queue.StateDead],
			},
			ReadyOverdueCount: a.ReadyOverdueCount,
		}
		if p, ok := backlog.AgePercentilesFromSamples(a.AgeSamples); ok {
			item.AgePercentilesSeconds = &p
		}
		if !a.OldestReceivedAt.IsZero() {
			item.OldestQueuedReceivedAt = a.OldestReceivedAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(a.OldestReceivedAt) {
				item.OldestQueuedAgeSeconds = int(now.Sub(a.OldestReceivedAt) / time.Second)
			}
		}
		if !a.NewestReceivedAt.IsZero() {
			item.NewestQueuedReceivedAt = a.NewestReceivedAt.UTC().Format(time.RFC3339Nano)
			if !now.Before(a.NewestReceivedAt) {
				item.NewestQueuedAgeSeconds = int(now.Sub(a.NewestReceivedAt) / time.Second)
			}
		}
		if !a.EarliestNextRun.IsZero() {
			item.EarliestQueuedNextRunAt = a.EarliestNextRun.UTC().Format(time.RFC3339Nano)
		}
		if a.MaxReadyLag > 0 {
			item.MaxReadyLagSeconds = int(a.MaxReadyLag / time.Second)
		}
		outItems = append(outItems, item)
	}

	outStateScan := make(map[string]backlog.StateScanSummary, len(states))
	scannedTotal := 0
	for _, st := range states {
		scan := stateScan[st]
		outStateScan[string(st)] = scan
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

	stateNames := make([]string, 0, len(states))
	for _, st := range states {
		stateNames = append(stateNames, string(st))
	}

	out := backlogAgingSummaryResponse{
		QueueTotal:     stats.Total,
		QueuedTotal:    stats.ByState[queue.StateQueued],
		Limit:          limit,
		States:         stateNames,
		AgeSampleCount: len(allAgeSamples),
		AgeWindows:     allAgeWindows,
		Scanned:        scannedTotal,
		BucketCount:    len(outItems),
		Truncated:      truncated,
		SourceBounded:  sourceBounded,
		StateScan:      outStateScan,
		Selector: backlogSelector{
			Route:  routeFilter,
			Target: targetFilter,
		},
		Items: outItems,
	}
	if p, ok := backlog.AgePercentilesFromSamples(allAgeSamples); ok {
		out.AgePercentilesSeconds = &p
	}
	if !stats.OldestQueuedReceivedAt.IsZero() {
		out.OldestQueuedReceivedAt = stats.OldestQueuedReceivedAt.UTC().Format(time.RFC3339Nano)
		out.OldestQueuedAgeSeconds = int(stats.OldestQueuedAge / time.Second)
	}
	if !stats.EarliestQueuedNextRun.IsZero() {
		out.EarliestQueuedNextRunAt = stats.EarliestQueuedNextRun.UTC().Format(time.RFC3339Nano)
		out.ReadyLagSeconds = int(stats.ReadyLag / time.Second)
	}
	return out
}

func backlogTrendsFromSamples(since, until time.Time, window, step time.Duration, routeFilter, targetFilter string, truncated bool, samples []queue.BacklogTrendSample, signalCfg queue.BacklogTrendSignalConfig) backlogTrendsResponse {
	if step <= 0 {
		step = backlog.DefaultTrendStep
	}
	if window <= 0 {
		window = backlog.DefaultTrendWindow
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

	points := make([]backlogTrendPoint, 0, bucketCount)
	for i := 0; i < bucketCount; i++ {
		bucketStart := since.Add(time.Duration(i) * step)
		bucketEnd := bucketStart.Add(step)
		if bucketEnd.After(until) {
			bucketEnd = until
		}
		points = append(points, backlogTrendPoint{
			BucketStart: bucketStart.UTC().Format(time.RFC3339Nano),
			BucketEnd:   bucketEnd.UTC().Format(time.RFC3339Nano),
		})
	}

	latestTotal := 0
	maxTotal := 0
	latestCapturedAt := time.Time{}
	sampleCount := 0
	nonEmptyPoints := 0

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
			nonEmptyPoints++
		}
		if at.After(p.lastCapturedAt) {
			p.lastCapturedAt = at
			p.LastCapturedAt = at.Format(time.RFC3339Nano)
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

	for i := range points {
		points[i].lastCapturedAt = time.Time{}
	}

	out := backlogTrendsResponse{
		Window:             window.String(),
		Step:               step.String(),
		Since:              since.UTC().Format(time.RFC3339Nano),
		Until:              until.UTC().Format(time.RFC3339Nano),
		SampleCount:        sampleCount,
		PointCount:         len(points),
		NonEmptyPointCount: nonEmptyPoints,
		Truncated:          truncated,
		LatestTotal:        latestTotal,
		MaxTotal:           maxTotal,
		Selector: backlogSelector{
			Route:  routeFilter,
			Target: targetFilter,
		},
		Signals: queue.AnalyzeBacklogTrendSignals(samples, truncated, queue.BacklogTrendSignalOptions{
			Now:    until,
			Window: window,
			Config: signalCfg,
		}).Map(),
		Items: points,
	}
	if !latestCapturedAt.IsZero() {
		out.LatestCapturedAt = latestCapturedAt.Format(time.RFC3339Nano)
	}
	return out
}

type backlogTopQueuedResponse struct {
	QueueTotal              int                    `json:"queue_total"`
	QueuedTotal             int                    `json:"queued_total"`
	Limit                   int                    `json:"limit"`
	Truncated               bool                   `json:"truncated"`
	SourceBounded           bool                   `json:"source_bounded"`
	OldestQueuedReceivedAt  string                 `json:"oldest_queued_received_at,omitempty"`
	OldestQueuedAgeSeconds  int                    `json:"oldest_queued_age_seconds,omitempty"`
	EarliestQueuedNextRunAt string                 `json:"earliest_queued_next_run_at,omitempty"`
	ReadyLagSeconds         int                    `json:"ready_lag_seconds,omitempty"`
	Items                   []backlogTopQueuedItem `json:"items"`
}

type backlogTopQueuedItem struct {
	Route                  string `json:"route"`
	Target                 string `json:"target"`
	Queued                 int    `json:"queued"`
	OldestQueuedReceivedAt string `json:"oldest_queued_received_at,omitempty"`
	OldestQueuedAgeSeconds int    `json:"oldest_queued_age_seconds,omitempty"`
	EarliestQueuedNextRun  string `json:"earliest_queued_next_run_at,omitempty"`
	ReadyLagSeconds        int    `json:"ready_lag_seconds,omitempty"`
}

type backlogOldestQueuedResponse struct {
	QueueTotal              int                       `json:"queue_total"`
	QueuedTotal             int                       `json:"queued_total"`
	Limit                   int                       `json:"limit"`
	Returned                int                       `json:"returned"`
	Truncated               bool                      `json:"truncated"`
	OldestQueuedReceivedAt  string                    `json:"oldest_queued_received_at,omitempty"`
	OldestQueuedAgeSeconds  int                       `json:"oldest_queued_age_seconds,omitempty"`
	EarliestQueuedNextRunAt string                    `json:"earliest_queued_next_run_at,omitempty"`
	ReadyLagSeconds         int                       `json:"ready_lag_seconds,omitempty"`
	Selector                backlogSelector           `json:"selector"`
	Items                   []backlogOldestQueuedItem `json:"items"`
}

type backlogSelector struct {
	Route  string `json:"route,omitempty"`
	Target string `json:"target,omitempty"`
}

type backlogOldestQueuedItem struct {
	ID                     string `json:"id"`
	Route                  string `json:"route"`
	Target                 string `json:"target"`
	ReceivedAt             string `json:"received_at,omitempty"`
	Attempt                int    `json:"attempt"`
	NextRunAt              string `json:"next_run_at,omitempty"`
	OldestQueuedAgeSeconds int    `json:"oldest_queued_age_seconds,omitempty"`
	ReadyLagSeconds        int    `json:"ready_lag_seconds,omitempty"`
}

type backlogTrendsResponse struct {
	Window             string              `json:"window"`
	Step               string              `json:"step"`
	Since              string              `json:"since"`
	Until              string              `json:"until"`
	SampleCount        int                 `json:"sample_count"`
	PointCount         int                 `json:"point_count"`
	NonEmptyPointCount int                 `json:"non_empty_point_count"`
	Truncated          bool                `json:"truncated"`
	LatestCapturedAt   string              `json:"latest_captured_at,omitempty"`
	LatestTotal        int                 `json:"latest_total"`
	MaxTotal           int                 `json:"max_total"`
	Selector           backlogSelector     `json:"selector"`
	Signals            map[string]any      `json:"signals"`
	Items              []backlogTrendPoint `json:"items"`
}

type backlogTrendPoint struct {
	BucketStart    string `json:"bucket_start"`
	BucketEnd      string `json:"bucket_end"`
	SampleCount    int    `json:"sample_count"`
	LastCapturedAt string `json:"last_captured_at,omitempty"`
	QueuedLast     int    `json:"queued_last"`
	LeasedLast     int    `json:"leased_last"`
	DeadLast       int    `json:"dead_last"`
	TotalLast      int    `json:"total_last"`
	QueuedMax      int    `json:"queued_max"`
	LeasedMax      int    `json:"leased_max"`
	DeadMax        int    `json:"dead_max"`
	TotalMax       int    `json:"total_max"`

	lastCapturedAt time.Time
}

type backlogAgingSummaryResponse struct {
	QueueTotal              int                                 `json:"queue_total"`
	QueuedTotal             int                                 `json:"queued_total"`
	Limit                   int                                 `json:"limit"`
	States                  []string                            `json:"states"`
	AgeSampleCount          int                                 `json:"age_sample_count"`
	AgeWindows              backlog.AgeWindows                  `json:"age_windows"`
	AgePercentilesSeconds   *backlog.AgePercentiles             `json:"age_percentiles_seconds,omitempty"`
	Scanned                 int                                 `json:"scanned"`
	BucketCount             int                                 `json:"bucket_count"`
	Truncated               bool                                `json:"truncated"`
	SourceBounded           bool                                `json:"source_bounded"`
	StateScan               map[string]backlog.StateScanSummary `json:"state_scan"`
	OldestQueuedReceivedAt  string                              `json:"oldest_queued_received_at,omitempty"`
	OldestQueuedAgeSeconds  int                                 `json:"oldest_queued_age_seconds,omitempty"`
	EarliestQueuedNextRunAt string                              `json:"earliest_queued_next_run_at,omitempty"`
	ReadyLagSeconds         int                                 `json:"ready_lag_seconds,omitempty"`
	Selector                backlogSelector                     `json:"selector"`
	Items                   []backlogAgingSummaryItem           `json:"items"`
}

type backlogAgingSummaryItem struct {
	Route                   string                  `json:"route"`
	Target                  string                  `json:"target"`
	TotalObserved           int                     `json:"total_observed"`
	QueuedObserved          int                     `json:"queued_observed"`
	LeasedObserved          int                     `json:"leased_observed"`
	DeadObserved            int                     `json:"dead_observed"`
	AgeSampleCount          int                     `json:"age_sample_count"`
	AgeWindows              backlog.AgeWindows      `json:"age_windows"`
	AgePercentilesSeconds   *backlog.AgePercentiles `json:"age_percentiles_seconds,omitempty"`
	StateCounts             map[string]int          `json:"state_counts"`
	OldestQueuedReceivedAt  string                  `json:"oldest_queued_received_at,omitempty"`
	OldestQueuedAgeSeconds  int                     `json:"oldest_queued_age_seconds,omitempty"`
	NewestQueuedReceivedAt  string                  `json:"newest_queued_received_at,omitempty"`
	NewestQueuedAgeSeconds  int                     `json:"newest_queued_age_seconds,omitempty"`
	EarliestQueuedNextRunAt string                  `json:"earliest_queued_next_run_at,omitempty"`
	ReadyOverdueCount       int                     `json:"ready_overdue_count"`
	MaxReadyLagSeconds      int                     `json:"max_ready_lag_seconds,omitempty"`
}

func (s *Server) handleBacklogTopQueued(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	limit := defaultListLimit
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > backlog.MaxListLimit {
		limit = backlog.MaxListLimit
	}

	stats, err := s.Store.Stats()
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	routeFilter, ok := parseOptionalRoutePath(r.URL.Query().Get("route"))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}
	targetFilter := strings.TrimSpace(r.URL.Query().Get("target"))
	out := backlogTopQueuedFromStats(stats, routeFilter, targetFilter, limit)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleBacklogOldestQueued(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	limit := defaultListLimit
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > backlog.MaxListLimit {
		limit = backlog.MaxListLimit
	}

	stats, err := s.Store.Stats()
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	routeFilter, ok := parseOptionalRoutePath(r.URL.Query().Get("route"))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}
	targetFilter := strings.TrimSpace(r.URL.Query().Get("target"))

	probeLimit := limit
	if limit < backlog.MaxListLimit {
		probeLimit = limit + 1
	}

	resp, err := s.Store.ListMessages(queue.MessageListRequest{
		Route:  routeFilter,
		Target: targetFilter,
		State:  queue.StateQueued,
		Order:  queue.MessageOrderAsc,
		Limit:  probeLimit,
	})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	items := resp.Items
	truncated := false
	if limit < backlog.MaxListLimit && len(items) > limit {
		truncated = true
		items = items[:limit]
	}
	if limit == backlog.MaxListLimit && routeFilter == "" && targetFilter == "" && stats.ByState[queue.StateQueued] > limit {
		truncated = true
	}

	out := backlogOldestQueuedFromMessages(backlog.ReferenceNow(stats), stats, routeFilter, targetFilter, limit, truncated, items)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleBacklogAgingSummary(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	limit := defaultListLimit
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > backlog.MaxListLimit {
		limit = backlog.MaxListLimit
	}

	stats, err := s.Store.Stats()
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	routeFilter, ok := parseOptionalRoutePath(r.URL.Query().Get("route"))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}
	targetFilter := strings.TrimSpace(r.URL.Query().Get("target"))
	states, ok := parseBacklogSummaryStates(strings.TrimSpace(r.URL.Query().Get("states")))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "states must contain only queued|leased|dead")
		return
	}

	probeLimit := limit
	if limit < backlog.MaxListLimit {
		probeLimit = limit + 1
	}

	truncated := false
	stateScan := make(map[queue.State]backlog.StateScanSummary, len(states))
	items := make([]queue.Envelope, 0, len(states)*limit)
	for _, state := range states {
		resp, err := s.Store.ListMessages(queue.MessageListRequest{
			Route:  routeFilter,
			Target: targetFilter,
			State:  state,
			Order:  queue.MessageOrderAsc,
			Limit:  probeLimit,
		})
		if err != nil {
			writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
			return
		}
		stateItems := resp.Items
		stateTruncated := false
		if limit < backlog.MaxListLimit && len(stateItems) > limit {
			stateTruncated = true
			stateItems = stateItems[:limit]
		}
		if limit == backlog.MaxListLimit && routeFilter == "" && targetFilter == "" && stats.ByState[state] > limit {
			stateTruncated = true
		}
		if stateTruncated {
			truncated = true
		}
		stateScan[state] = backlog.StateScanSummary{
			Scanned:   len(stateItems),
			Truncated: stateTruncated,
		}
		items = append(items, stateItems...)
	}

	out := backlogAgingSummaryFromMessages(backlog.ReferenceNow(stats), stats, routeFilter, targetFilter, states, limit, truncated, stateScan, items)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleBacklogTrends(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}
	trendStore, ok := s.Store.(queue.BacklogTrendStore)
	if !ok {
		writeManagementError(w, http.StatusServiceUnavailable, readCodeBacklogUnavailable, "backlog trend store is not supported by queue backend")
		return
	}

	window := backlog.DefaultTrendWindow
	if raw := strings.TrimSpace(r.URL.Query().Get("window")); raw != "" {
		d, valid := parseDurationParam(raw)
		if !valid || d <= 0 || d > backlog.MaxTrendWindow {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "window must be a duration between 1m and 168h")
			return
		}
		window = d
	}

	step := backlog.DefaultTrendStep
	if raw := strings.TrimSpace(r.URL.Query().Get("step")); raw != "" {
		d, valid := parseDurationParam(raw)
		if !valid || d < backlog.MinTrendStep || d > backlog.MaxTrendStep {
			writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "step must be a duration between 1m and 1h")
			return
		}
		step = d
	}
	if step > window {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "step must be less than or equal to window")
		return
	}

	until, valid := parseTimeParam(strings.TrimSpace(r.URL.Query().Get("until")))
	if !valid {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "until must be RFC3339")
		return
	}
	if until.IsZero() {
		until = time.Now().UTC()
	}
	since := until.Add(-window)
	bucketCount := int(window / step)
	if window%step != 0 {
		bucketCount++
	}
	if bucketCount <= 0 || bucketCount > backlog.MaxListLimit {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "window/step produce an invalid bucket count")
		return
	}

	routeFilter, ok := parseOptionalRoutePath(r.URL.Query().Get("route"))
	if !ok {
		writeManagementError(w, http.StatusBadRequest, readCodeInvalidQuery, "route must start with '/'")
		return
	}
	targetFilter := strings.TrimSpace(r.URL.Query().Get("target"))
	resp, err := trendStore.ListBacklogTrend(queue.BacklogTrendListRequest{
		Route:  routeFilter,
		Target: targetFilter,
		Since:  since,
		Until:  until,
		Limit:  backlog.MaxTrendSamples,
	})
	if err != nil {
		writeManagementError(w, http.StatusServiceUnavailable, publishCodeStoreUnavailable, "queue store is unavailable")
		return
	}

	out := backlogTrendsFromSamples(since, until, window, step, routeFilter, targetFilter, resp.Truncated, resp.Items, s.trendSignalConfig())
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) trendSignalConfig() queue.BacklogTrendSignalConfig {
	if s != nil && s.ResolveTrendSignalConfig != nil {
		return s.ResolveTrendSignalConfig()
	}
	return queue.DefaultBacklogTrendSignalConfig()
}
