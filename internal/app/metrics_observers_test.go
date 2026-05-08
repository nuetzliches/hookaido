package app

import (
	"testing"
	"time"
)

func TestObserveSecretGCPruned_AccumulatesPerPool(t *testing.T) {
	m := newRuntimeMetrics()
	m.observeSecretGCPruned("stripe", 3)
	m.observeSecretGCPruned("stripe", 2)
	m.observeSecretGCPruned("cituro", 5)

	// No-op cases.
	m.observeSecretGCPruned("", 1)     // empty pool
	m.observeSecretGCPruned("noop", 0) // zero count
	m.observeSecretGCPruned("noop", -1)
	var nilMetrics *runtimeMetrics
	nilMetrics.observeSecretGCPruned("nil", 1) // nil receiver guard

	snap := m.secretGCPrunedSnapshot()
	if snap["stripe"] != 5 {
		t.Fatalf("stripe=%d, want 5", snap["stripe"])
	}
	if snap["cituro"] != 5 {
		t.Fatalf("cituro=%d, want 5", snap["cituro"])
	}
	if _, ok := snap["noop"]; ok {
		t.Fatalf("zero/negative observations should not create pool entry")
	}
	if _, ok := snap[""]; ok {
		t.Fatalf("empty-pool observation should not create entry")
	}
}

func TestObservePullSSE_ConnectAndDisconnect(t *testing.T) {
	m := newRuntimeMetrics()

	// nil-safe.
	var nilMetrics *runtimeMetrics
	nilMetrics.observePullSSEConnect("/r")
	nilMetrics.observePullSSEDisconnect("/r", 200, 0, 0)

	// Two concurrent connections, then both disconnect.
	m.observePullSSEConnect("/r")
	m.observePullSSEConnect("/r")

	snap := m.pullSnapshot()
	if snap == nil {
		t.Fatalf("snapshot should not be nil after observations")
	}
	r := snap[normalizePullRoute("/r")]
	if r.sseConnectionsTotal != 2 {
		t.Fatalf("connectionsTotal=%d, want 2", r.sseConnectionsTotal)
	}

	m.observePullSSEDisconnect("/r", 200, 5, 100*time.Millisecond)
	m.observePullSSEDisconnect("/r", 500, 7, 250*time.Millisecond)

	// One more disconnect when no active connections — counter shouldn't go
	// negative.
	m.observePullSSEDisconnect("/r", 200, 1, 0)

	snap = m.pullSnapshot()
	r = snap[normalizePullRoute("/r")]
	if r.sseMessagesSentTotal != 5+7+1 {
		t.Fatalf("messagesSentTotal=%d, want 13", r.sseMessagesSentTotal)
	}
}
