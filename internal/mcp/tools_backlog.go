package mcp

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/backlog"
	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/queue"
)

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
	limit, err := parseLimit(args, backlog.MaxListLimit)
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
	limit, err := parseLimit(args, backlog.MaxListLimit)
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
	if limit < backlog.MaxListLimit {
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
	if limit < backlog.MaxListLimit && len(items) > limit {
		truncated = true
		items = items[:limit]
	}
	if limit == backlog.MaxListLimit && route == "" && target == "" && stats.ByState[queue.StateQueued] > limit {
		truncated = true
	}

	return backlogOldestQueuedMap(backlog.ReferenceNow(stats), stats, route, target, limit, truncated, items), nil
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
	limit, err := parseLimit(args, backlog.MaxListLimit)
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
	if limit < backlog.MaxListLimit {
		probeLimit = limit + 1
	}
	truncated := false
	stateScan := make(map[queue.State]backlog.StateScanSummary, len(states))
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
		if limit < backlog.MaxListLimit && len(stateItems) > limit {
			stateTruncated = true
			stateItems = stateItems[:limit]
		}
		if limit == backlog.MaxListLimit && route == "" && target == "" && stats.ByState[state] > limit {
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

	return backlogAgingSummaryMap(backlog.ReferenceNow(stats), stats, route, target, states, limit, truncated, stateScan, items), nil
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
	window, err := parseDurationArg(args, "window", backlog.DefaultTrendWindow)
	if err != nil {
		return nil, err
	}
	if window <= 0 || window > backlog.MaxTrendWindow {
		return nil, fmt.Errorf("window must be > 0 and <= %s", backlog.MaxTrendWindow)
	}
	step, err := parseDurationArg(args, "step", backlog.DefaultTrendStep)
	if err != nil {
		return nil, err
	}
	if step < backlog.MinTrendStep || step > backlog.MaxTrendStep {
		return nil, fmt.Errorf("step must be between %s and %s", backlog.MinTrendStep, backlog.MaxTrendStep)
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
	if bucketCount <= 0 || bucketCount > backlog.MaxListLimit {
		return nil, fmt.Errorf("window/step yields %d buckets; max %d", bucketCount, backlog.MaxListLimit)
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
		Limit:  backlog.MaxTrendSamples,
	})
	if err != nil {
		return nil, err
	}
	return backlogTrendsMap(since, until, window, step, route, target, resp.Truncated, resp.Items, signalCfg), nil
}

func backlogTopQueuedMap(stats queue.Stats, routeFilter, targetFilter string, limit int) map[string]any {
	if limit <= 0 {
		limit = 100
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
	if limit > backlog.MaxListLimit {
		limit = backlog.MaxListLimit
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

func backlogAgingSummaryMap(now time.Time, stats queue.Stats, routeFilter, targetFilter string, states []queue.State, limit int, truncated bool, stateScan map[queue.State]backlog.StateScanSummary, items []queue.Envelope) map[string]any {
	if limit <= 0 {
		limit = 100
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
		EarliestNextRunAt time.Time
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
			"age_windows":      ageWindowsToMap(a.AgeWindows),
			"state_counts": map[string]any{
				string(queue.StateQueued): a.StateCounts[queue.StateQueued],
				string(queue.StateLeased): a.StateCounts[queue.StateLeased],
				string(queue.StateDead):   a.StateCounts[queue.StateDead],
			},
			"ready_overdue_count": a.ReadyOverdueCount,
		}
		if p, ok := backlog.AgePercentilesFromSamples(a.AgeSamples); ok {
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
		"age_windows":      ageWindowsToMap(allAgeWindows),
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
	if p, ok := backlog.AgePercentilesFromSamples(allAgeSamples); ok {
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

func ageWindowsToMap(w backlog.AgeWindows) map[string]any {
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

func parseBacklogSummaryStatesArg(args map[string]any, key string) ([]queue.State, error) {
	raw, ok := args[key]
	if !ok || raw == nil {
		out := make([]queue.State, len(backlog.DefaultSummaryStates))
		copy(out, backlog.DefaultSummaryStates)
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
