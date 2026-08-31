package app

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/nuetzliches/hookaido/v2/internal/ingress"
	"github.com/nuetzliches/hookaido/v2/internal/pullapi"
	"github.com/nuetzliches/hookaido/v2/internal/queue"
)

const consumerGroupConfig = `
pull_api { auth token "raw:devtoken" }

"/webhooks/appliance" {
  pull {
    path "/appliance"
    consumer_group "integration"
    consumer_group "workstation"
  }
}

"/webhooks/github" {
  pull { path "/github" }
}
`

func newConsumerGroupState(t *testing.T) *runtimeState {
	t.Helper()
	compiled := compileForReloadTest(t, consumerGroupConfig)
	state := newRuntimeState(compiled)
	if err := state.loadAuth(compiled); err != nil {
		t.Fatalf("loadAuth: %v", err)
	}
	return state
}

// This is the whole point of the feature: one inbound event has to land in
// every group's queue, not be split between them.
func TestConsumerGroups_IngressFansOutToEveryGroup(t *testing.T) {
	state := newConsumerGroupState(t)
	store := queue.NewMemoryStore()

	srv := ingress.NewServer(store)
	srv.ResolveRequest = state.resolveIngressSnapshot

	req := httptest.NewRequest(http.MethodPost, "http://hooks.example/webhooks/appliance", strings.NewReader(`{"event":1}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", rr.Code, rr.Body.String())
	}

	items, err := store.ListMessages(queue.MessageListRequest{Route: "/webhooks/appliance", Limit: 10})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	targets := make([]string, 0, len(items.Items))
	for _, it := range items.Items {
		targets = append(targets, it.Target)
	}
	sort.Strings(targets)

	if len(targets) != 2 || targets[0] != "pull:integration" || targets[1] != "pull:workstation" {
		t.Fatalf("expected one enqueued copy per group, got %v", targets)
	}
}

// An ungrouped route must keep enqueueing exactly one copy on the unchanged
// `pull` target — messages already queued in a durable backend carry it.
func TestConsumerGroups_UngroupedRouteStillEnqueuesOnce(t *testing.T) {
	state := newConsumerGroupState(t)
	store := queue.NewMemoryStore()

	srv := ingress.NewServer(store)
	srv.ResolveRequest = state.resolveIngressSnapshot

	req := httptest.NewRequest(http.MethodPost, "http://hooks.example/webhooks/github", strings.NewReader(`{"event":1}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", rr.Code, rr.Body.String())
	}

	items, err := store.ListMessages(queue.MessageListRequest{Route: "/webhooks/github", Limit: 10})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(items.Items) != 1 {
		t.Fatalf("expected exactly one enqueued copy, got %d", len(items.Items))
	}
	if got := items.Items[0].Target; got != "pull" {
		t.Fatalf("expected the unchanged target %q, got %q", "pull", got)
	}
}

func TestConsumerGroups_ResolvePullReturnsThePerGroupQueue(t *testing.T) {
	state := newConsumerGroupState(t)

	q, ok := state.resolvePull("/appliance/integration")
	if !ok {
		t.Fatal("expected the group endpoint to resolve")
	}
	want := pullapi.Queue{Route: "/webhooks/appliance", Target: "pull:integration", ConsumerGroup: "integration"}
	if q != want {
		t.Fatalf("expected %#v, got %#v", want, q)
	}

	// The bare path of a grouped route must not resolve: a consumer left on the
	// old URL has to see a 404 rather than silently share one group's queue.
	if q, ok := state.resolvePull("/appliance"); ok {
		t.Fatalf("expected the bare path of a grouped route to 404, got %#v", q)
	}

	ungrouped, ok := state.resolvePull("/github")
	if !ok {
		t.Fatal("expected the ungrouped endpoint to resolve")
	}
	if ungrouped.Target != "pull" || ungrouped.ConsumerGroup != "" {
		t.Fatalf("expected the unchanged ungrouped queue, got %#v", ungrouped)
	}
}

// Groups must not collapse into one metric series: with two groups configured,
// two attached consumers on one route is the expected state, and an unexpected
// third has to stay visible as one group having two.
func TestConsumerGroups_MetricsSeparateGroups(t *testing.T) {
	m := newRuntimeMetrics()

	integration := pullMetricKey(pullapi.Queue{Route: "/webhooks/appliance", ConsumerGroup: "integration"})
	workstation := pullMetricKey(pullapi.Queue{Route: "/webhooks/appliance", ConsumerGroup: "workstation"})

	m.observePullSSEConnect(integration)
	m.observePullSSEConnect(workstation)
	m.observePullSSEConnect(workstation)

	snap := m.pullSnapshot()
	if got := snap[integration].sseConnectionActive; got != 1 {
		t.Fatalf("expected 1 active connection for integration, got %d", got)
	}
	if got := snap[workstation].sseConnectionActive; got != 2 {
		t.Fatalf("expected 2 active connections for workstation, got %d", got)
	}
}
