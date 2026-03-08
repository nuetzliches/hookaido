// Package backlog provides shared backlog analytics types, constants, and
// algorithms used by both the Admin API and MCP server.
package backlog

import (
	"sort"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

// Shared constants for backlog analytics queries.
const (
	// MaxListLimit is the upper bound for list/query limit parameters.
	MaxListLimit = 1000

	// DefaultTrendWindow is the default time window for backlog trend queries.
	DefaultTrendWindow = time.Hour

	// DefaultTrendStep is the default bucket step for backlog trend queries.
	DefaultTrendStep = 5 * time.Minute

	// MinTrendStep is the minimum allowed trend bucket step.
	MinTrendStep = time.Minute

	// MaxTrendStep is the maximum allowed trend bucket step.
	MaxTrendStep = time.Hour

	// MaxTrendWindow is the maximum allowed trend query window.
	MaxTrendWindow = 7 * 24 * time.Hour

	// MaxTrendSamples is the maximum number of trend samples returned per query.
	MaxTrendSamples = 20000
)

// DefaultSummaryStates is the default set of queue states included in
// backlog summary queries when no explicit states are specified.
var DefaultSummaryStates = []queue.State{
	queue.StateQueued,
	queue.StateLeased,
	queue.StateDead,
}

// StateScanSummary reports how many items were scanned for a given queue state
// and whether the scan was truncated by the query limit.
type StateScanSummary struct {
	Scanned   int  `json:"scanned"`
	Truncated bool `json:"truncated"`
}

// AgePercentiles holds P50/P90/P99 age percentiles in seconds, computed from
// a sample of queued item ages.
type AgePercentiles struct {
	P50 int `json:"p50"`
	P90 int `json:"p90"`
	P99 int `json:"p99"`
}

// AgeWindows counts items by age bucket: ≤5m, 5m–15m, 15m–1h, 1h–6h, >6h.
type AgeWindows struct {
	LE5M      int `json:"le_5m"`
	GT5MLE15M int `json:"gt_5m_le_15m"`
	GT15MLE1H int `json:"gt_15m_le_1h"`
	GT1HLE6H  int `json:"gt_1h_le_6h"`
	GT6H      int `json:"gt_6h"`
}

// ReferenceNow derives a stable "now" timestamp from queue stats, falling back
// to time.Now().UTC() when no age/lag data is available. Using stats-derived
// timestamps ensures consistent age/lag calculations across a single response.
func ReferenceNow(stats queue.Stats) time.Time {
	if !stats.OldestQueuedReceivedAt.IsZero() && stats.OldestQueuedAge > 0 {
		return stats.OldestQueuedReceivedAt.Add(stats.OldestQueuedAge).UTC()
	}
	if !stats.EarliestQueuedNextRun.IsZero() && stats.ReadyLag > 0 {
		return stats.EarliestQueuedNextRun.Add(stats.ReadyLag).UTC()
	}
	return time.Now().UTC()
}

// ObserveAgeWindow increments the appropriate age bucket in windows based on
// the given age in seconds.
func ObserveAgeWindow(ageSeconds int, windows *AgeWindows) {
	switch {
	case ageSeconds <= int((5*time.Minute)/time.Second):
		windows.LE5M++
	case ageSeconds <= int((15*time.Minute)/time.Second):
		windows.GT5MLE15M++
	case ageSeconds <= int(time.Hour/time.Second):
		windows.GT15MLE1H++
	case ageSeconds <= int((6*time.Hour)/time.Second):
		windows.GT1HLE6H++
	default:
		windows.GT6H++
	}
}

// AgePercentilesFromSamples computes P50/P90/P99 from a slice of age-in-seconds
// samples. Returns false when samples is empty.
func AgePercentilesFromSamples(samples []int) (AgePercentiles, bool) {
	if len(samples) == 0 {
		return AgePercentiles{}, false
	}
	ordered := make([]int, len(samples))
	copy(ordered, samples)
	sort.Ints(ordered)
	return AgePercentiles{
		P50: AgePercentileNearestRank(ordered, 50),
		P90: AgePercentileNearestRank(ordered, 90),
		P99: AgePercentileNearestRank(ordered, 99),
	}, true
}

// AgePercentileNearestRank returns the value at the given percentile using
// nearest-rank interpolation. The input slice must be sorted in ascending order.
func AgePercentileNearestRank(ordered []int, percentile int) int {
	if len(ordered) == 0 {
		return 0
	}
	if percentile <= 0 {
		return ordered[0]
	}
	if percentile >= 100 {
		return ordered[len(ordered)-1]
	}
	rank := (percentile*len(ordered) + 100 - 1) / 100
	if rank <= 0 {
		rank = 1
	}
	if rank > len(ordered) {
		rank = len(ordered)
	}
	return ordered[rank-1]
}
