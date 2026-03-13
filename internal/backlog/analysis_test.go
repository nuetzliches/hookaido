package backlog

import (
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

// ---------------------------------------------------------------------------
// AgePercentileNearestRank
// ---------------------------------------------------------------------------

func TestAgePercentileNearestRank_EmptySlice(t *testing.T) {
	if got := AgePercentileNearestRank(nil, 50); got != 0 {
		t.Fatalf("empty slice: want 0, got %d", got)
	}
}

func TestAgePercentileNearestRank_SingleElement(t *testing.T) {
	ordered := []int{42}
	for _, p := range []int{0, 50, 99, 100} {
		if got := AgePercentileNearestRank(ordered, p); got != 42 {
			t.Errorf("p%d: want 42, got %d", p, got)
		}
	}
}

func TestAgePercentileNearestRank_Boundary(t *testing.T) {
	ordered := []int{10, 20, 30, 40, 50}
	if got := AgePercentileNearestRank(ordered, 0); got != ordered[0] {
		t.Errorf("p0: want %d, got %d", ordered[0], got)
	}
	if got := AgePercentileNearestRank(ordered, 100); got != ordered[len(ordered)-1] {
		t.Errorf("p100: want %d, got %d", ordered[len(ordered)-1], got)
	}
}

func TestAgePercentileNearestRank_TenElements(t *testing.T) {
	ordered := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	tests := []struct {
		percentile int
		want       int
	}{
		{50, 5},
		{90, 9},
		{99, 10},
	}
	for _, tt := range tests {
		got := AgePercentileNearestRank(ordered, tt.percentile)
		if got != tt.want {
			t.Errorf("p%d: want %d, got %d", tt.percentile, tt.want, got)
		}
	}
}

// ---------------------------------------------------------------------------
// AgePercentilesFromSamples
// ---------------------------------------------------------------------------

func TestAgePercentilesFromSamples_Empty(t *testing.T) {
	_, ok := AgePercentilesFromSamples(nil)
	if ok {
		t.Fatal("expected ok=false for empty samples")
	}
}

func TestAgePercentilesFromSamples_UnsortedInput(t *testing.T) {
	samples := []int{90, 10, 50, 30, 70, 20, 80, 40, 60, 100}
	p, ok := AgePercentilesFromSamples(samples)
	if !ok {
		t.Fatal("expected ok=true")
	}
	// After internal sort: [10,20,30,40,50,60,70,80,90,100]
	// p50 → rank ceil(50*10/100) = 5 → ordered[4]=50
	// p90 → rank ceil(90*10/100) = 9 → ordered[8]=90
	// p99 → rank ceil(99*10/100) = 10 → ordered[9]=100
	if p.P50 != 50 {
		t.Errorf("P50: want 50, got %d", p.P50)
	}
	if p.P90 != 90 {
		t.Errorf("P90: want 90, got %d", p.P90)
	}
	if p.P99 != 100 {
		t.Errorf("P99: want 100, got %d", p.P99)
	}
}

func TestAgePercentilesFromSamples_DoesNotMutateInput(t *testing.T) {
	samples := []int{5, 3, 1, 4, 2}
	original := make([]int, len(samples))
	copy(original, samples)

	AgePercentilesFromSamples(samples)

	for i := range samples {
		if samples[i] != original[i] {
			t.Fatalf("input mutated at index %d: want %d, got %d", i, original[i], samples[i])
		}
	}
}

// ---------------------------------------------------------------------------
// ObserveAgeWindow
// ---------------------------------------------------------------------------

func TestObserveAgeWindow_AllBuckets(t *testing.T) {
	tests := []struct {
		name       string
		ageSeconds int
		field      string
	}{
		{"LE5M_60s", 60, "LE5M"},
		{"LE5M_300s", 300, "LE5M"},
		{"GT5MLE15M_600s", 600, "GT5MLE15M"},
		{"GT5MLE15M_900s", 900, "GT5MLE15M"},
		{"GT15MLE1H_1800s", 1800, "GT15MLE1H"},
		{"GT15MLE1H_3600s", 3600, "GT15MLE1H"},
		{"GT1HLE6H_7200s", 7200, "GT1HLE6H"},
		{"GT1HLE6H_21600s", 21600, "GT1HLE6H"},
		{"GT6H_86400s", 86400, "GT6H"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var w AgeWindows
			ObserveAgeWindow(tt.ageSeconds, &w)

			got := map[string]int{
				"LE5M":      w.LE5M,
				"GT5MLE15M": w.GT5MLE15M,
				"GT15MLE1H": w.GT15MLE1H,
				"GT1HLE6H":  w.GT1HLE6H,
				"GT6H":      w.GT6H,
			}

			for field, count := range got {
				if field == tt.field {
					if count != 1 {
						t.Errorf("expected %s=1, got %d", field, count)
					}
				} else {
					if count != 0 {
						t.Errorf("expected %s=0, got %d", field, count)
					}
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ReferenceNow
// ---------------------------------------------------------------------------

func TestReferenceNow_FromOldestQueued(t *testing.T) {
	received := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	age := 30 * time.Second
	stats := queue.Stats{
		OldestQueuedReceivedAt: received,
		OldestQueuedAge:        age,
	}
	got := ReferenceNow(stats)
	want := received.Add(age).UTC()
	if !got.Equal(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestReferenceNow_FromReadyLag(t *testing.T) {
	nextRun := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	lag := 5 * time.Second
	stats := queue.Stats{
		EarliestQueuedNextRun: nextRun,
		ReadyLag:              lag,
	}
	got := ReferenceNow(stats)
	want := nextRun.Add(lag).UTC()
	if !got.Equal(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestReferenceNow_FallbackToNow(t *testing.T) {
	before := time.Now().UTC()
	got := ReferenceNow(queue.Stats{})
	after := time.Now().UTC()
	if got.Before(before) || got.After(after) {
		t.Fatalf("expected between %v and %v, got %v", before, after, got)
	}
}

func TestReferenceNow_OldestQueuedPrecedence(t *testing.T) {
	received := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	age := 30 * time.Second
	nextRun := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	lag := 5 * time.Second

	stats := queue.Stats{
		OldestQueuedReceivedAt: received,
		OldestQueuedAge:        age,
		EarliestQueuedNextRun:  nextRun,
		ReadyLag:               lag,
	}
	got := ReferenceNow(stats)
	want := received.Add(age).UTC()
	if !got.Equal(want) {
		t.Fatalf("OldestQueued should take precedence: want %v, got %v", want, got)
	}
}
