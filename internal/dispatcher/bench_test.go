package dispatcher

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/ingress"
	"github.com/nuetzliches/hookaido/internal/queue"
)

const (
	pushBenchRoute       = "/webhooks/push"
	pushBenchIngressPath = "/hooks/push/events"
	pushBenchTargetURL   = "https://target.example/internal/hook"
	pushBenchFastTarget  = "https://fast-target.example/internal/hook"
	pushBenchSlowTarget  = "https://slow-target.example/internal/hook"
	pushBenchMaxDepth    = 512
)

func BenchmarkPushIngressDrainSaturation(b *testing.B) {
	for _, backend := range []string{"memory", "sqlite"} {
		b.Run(backend, func(b *testing.B) {
			store, closeStore := newPushBenchStore(b, backend)
			defer closeStore()

			ingressSrv := ingress.NewServer(store)
			ingressSrv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
				if requestPath == pushBenchIngressPath {
					return pushBenchRoute, true
				}
				return "", false
			}
			ingressSrv.TargetsFor = func(route string) []string {
				if route == pushBenchRoute {
					return []string{pushBenchTargetURL}
				}
				return nil
			}
			rejectStats := &pushBenchRejectStats{}
			ingressSrv.ObserveReject = rejectStats.Observe

			deliverer := &pushBenchDeliverer{
				delay: 2 * time.Millisecond,
			}
			dispatcher := PushDispatcher{
				Store:     store,
				Deliverer: deliverer,
				Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
				MaxWait:   2 * time.Millisecond,
				Routes: []RouteConfig{
					{
						Route: pushBenchRoute,
						Targets: []TargetConfig{
							{
								URL:     pushBenchTargetURL,
								Timeout: 5 * time.Second,
								Retry: RetryConfig{
									Max: 1,
								},
							},
						},
						Concurrency: 8,
					},
				},
			}
			dispatcher.Start()
			defer func() {
				if ok := dispatcher.Drain(10 * time.Second); !ok {
					b.Fatalf("dispatcher drain timeout")
				}
			}()

			var ingressRejects atomic.Int64
			latencyNanos := make([]int64, b.N)
			var latencyCount atomic.Int64
			payload := []byte(`{"ok":true}`)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					start := time.Now()
					rr := httptest.NewRecorder()
					req := httptest.NewRequest(http.MethodPost, "http://example"+pushBenchIngressPath, bytes.NewReader(payload))
					req.Header.Set("Content-Type", "application/json")
					ingressSrv.ServeHTTP(rr, req)
					if rr.Code != http.StatusAccepted {
						ingressRejects.Add(1)
					}
					idx := int(latencyCount.Add(1)) - 1
					if idx < len(latencyNanos) {
						latencyNanos[idx] = time.Since(start).Nanoseconds()
					}
				}
			})
			b.StopTimer()

			samples := int(latencyCount.Load())
			if samples > len(latencyNanos) {
				samples = len(latencyNanos)
			}
			p95 := pushBenchPercentileNanos(latencyNanos[:samples], 95)
			p99 := pushBenchPercentileNanos(latencyNanos[:samples], 99)
			b.ReportMetric(float64(p95)/float64(time.Millisecond), "p95_ms")
			b.ReportMetric(float64(p99)/float64(time.Millisecond), "p99_ms")
			b.ReportMetric(float64(ingressRejects.Load()), "ingress_rejects")
			b.ReportMetric(float64(rejectStats.queueFull.Load()), "ingress_rejects_queue_full")
			b.ReportMetric(float64(rejectStats.adaptiveBackpressure.Load()), "ingress_rejects_adaptive_backpressure")
			b.ReportMetric(float64(rejectStats.memoryPressure.Load()), "ingress_rejects_memory_pressure")
			b.ReportMetric(float64(rejectStats.other.Load()), "ingress_rejects_other")
			b.ReportMetric(float64(deliverer.completed.Load()), "deliveries")
		})
	}
}

func BenchmarkPushIngressDrainSkewedTargets(b *testing.B) {
	for _, backend := range []string{"memory", "sqlite"} {
		b.Run(backend, func(b *testing.B) {
			store, closeStore := newPushBenchStore(b, backend)
			defer closeStore()

			ingressSrv := ingress.NewServer(store)
			ingressSrv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
				if requestPath == pushBenchIngressPath {
					return pushBenchRoute, true
				}
				return "", false
			}
			ingressSrv.TargetsFor = func(route string) []string {
				if route == pushBenchRoute {
					return []string{pushBenchFastTarget, pushBenchSlowTarget}
				}
				return nil
			}
			rejectStats := &pushBenchRejectStats{}
			ingressSrv.ObserveReject = rejectStats.Observe

			deliverer := &pushBenchSkewedDeliverer{
				delays: map[string]time.Duration{
					pushBenchFastTarget: 500 * time.Microsecond,
					pushBenchSlowTarget: 8 * time.Millisecond,
				},
			}
			dispatcher := PushDispatcher{
				Store:     store,
				Deliverer: deliverer,
				Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
				MaxWait:   2 * time.Millisecond,
				Routes: []RouteConfig{
					{
						Route: pushBenchRoute,
						Targets: []TargetConfig{
							{
								URL:     pushBenchFastTarget,
								Timeout: 5 * time.Second,
								Retry: RetryConfig{
									Max: 1,
								},
							},
							{
								URL:     pushBenchSlowTarget,
								Timeout: 5 * time.Second,
								Retry: RetryConfig{
									Max: 1,
								},
							},
						},
						Concurrency: 8,
					},
				},
			}
			dispatcher.Start()
			defer func() {
				if ok := dispatcher.Drain(10 * time.Second); !ok {
					b.Fatalf("dispatcher drain timeout")
				}
			}()

			var ingressRejects atomic.Int64
			latencyNanos := make([]int64, b.N)
			var latencyCount atomic.Int64
			payload := []byte(`{"ok":true}`)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					start := time.Now()
					rr := httptest.NewRecorder()
					req := httptest.NewRequest(http.MethodPost, "http://example"+pushBenchIngressPath, bytes.NewReader(payload))
					req.Header.Set("Content-Type", "application/json")
					ingressSrv.ServeHTTP(rr, req)
					if rr.Code != http.StatusAccepted {
						ingressRejects.Add(1)
					}
					idx := int(latencyCount.Add(1)) - 1
					if idx < len(latencyNanos) {
						latencyNanos[idx] = time.Since(start).Nanoseconds()
					}
				}
			})
			b.StopTimer()

			samples := int(latencyCount.Load())
			if samples > len(latencyNanos) {
				samples = len(latencyNanos)
			}
			p95 := pushBenchPercentileNanos(latencyNanos[:samples], 95)
			p99 := pushBenchPercentileNanos(latencyNanos[:samples], 99)
			b.ReportMetric(float64(p95)/float64(time.Millisecond), "p95_ms")
			b.ReportMetric(float64(p99)/float64(time.Millisecond), "p99_ms")
			b.ReportMetric(float64(ingressRejects.Load()), "ingress_rejects")
			b.ReportMetric(float64(rejectStats.queueFull.Load()), "ingress_rejects_queue_full")
			b.ReportMetric(float64(rejectStats.adaptiveBackpressure.Load()), "ingress_rejects_adaptive_backpressure")
			b.ReportMetric(float64(rejectStats.memoryPressure.Load()), "ingress_rejects_memory_pressure")
			b.ReportMetric(float64(rejectStats.other.Load()), "ingress_rejects_other")
			b.ReportMetric(float64(deliverer.fastCompleted.Load()), "deliveries_fast")
			b.ReportMetric(float64(deliverer.slowCompleted.Load()), "deliveries_slow")
		})
	}
}

type pushBenchRejectStats struct {
	queueFull            atomic.Int64
	adaptiveBackpressure atomic.Int64
	memoryPressure       atomic.Int64
	other                atomic.Int64
}

func (s *pushBenchRejectStats) Observe(_ string, _ int, reason string) {
	switch reason {
	case "queue_full":
		s.queueFull.Add(1)
	case "adaptive_backpressure":
		s.adaptiveBackpressure.Add(1)
	case "memory_pressure":
		s.memoryPressure.Add(1)
	default:
		s.other.Add(1)
	}
}

func pushBenchPercentileNanos(values []int64, percentile int) int64 {
	if len(values) == 0 {
		return 0
	}

	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	if percentile <= 0 {
		return sorted[0]
	}
	if percentile >= 100 {
		return sorted[len(sorted)-1]
	}

	rank := (percentile*len(sorted) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func newPushBenchStore(b *testing.B, backend string) (queue.Store, func()) {
	b.Helper()

	switch backend {
	case "memory":
		s := queue.NewMemoryStore(
			queue.WithQueueLimits(pushBenchMaxDepth, "reject"),
		)
		return s, func() {}
	case "sqlite":
		dbPath := filepath.Join(b.TempDir(), "hookaido-push-bench.db")
		s, err := queue.NewSQLiteStore(
			dbPath,
			queue.WithSQLiteQueueLimits(pushBenchMaxDepth, "reject"),
			queue.WithSQLiteCheckpointInterval(0),
			queue.WithSQLitePollInterval(2*time.Millisecond),
		)
		if err != nil {
			b.Fatalf("new sqlite store: %v", err)
		}
		return s, func() {
			_ = s.Close()
		}
	default:
		b.Fatalf("unknown backend: %q", backend)
		return nil, func() {}
	}
}

type pushBenchDeliverer struct {
	delay     time.Duration
	completed atomic.Int64
}

func (d *pushBenchDeliverer) Deliver(ctx context.Context, _ Delivery) Result {
	if d.delay > 0 {
		timer := time.NewTimer(d.delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return Result{Err: ctx.Err()}
		case <-timer.C:
		}
	}
	d.completed.Add(1)
	return Result{StatusCode: http.StatusNoContent}
}

type pushBenchSkewedDeliverer struct {
	delays map[string]time.Duration

	fastCompleted atomic.Int64
	slowCompleted atomic.Int64
}

func (d *pushBenchSkewedDeliverer) Deliver(ctx context.Context, in Delivery) Result {
	if delay := d.delays[in.Target]; delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return Result{Err: ctx.Err()}
		case <-timer.C:
		}
	}

	switch in.Target {
	case pushBenchFastTarget:
		d.fastCompleted.Add(1)
	case pushBenchSlowTarget:
		d.slowCompleted.Add(1)
	}
	return Result{StatusCode: http.StatusNoContent}
}
