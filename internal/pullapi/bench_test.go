package pullapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/ingress"
	"github.com/nuetzliches/hookaido/internal/queue"
)

const (
	pullBenchEndpointRoute = "/pull/github"
	pullBenchInternalRoute = "/webhooks/github"
	mixedBenchIngressPath  = "/hooks/github/events"
	pullBenchRefillMemory  = 8192
	pullBenchRefillSQLite  = 512
)

func BenchmarkPullDequeueAckSingle(b *testing.B) {
	for _, backend := range []string{"memory", "sqlite"} {
		b.Run(backend, func(b *testing.B) {
			store, closeFn := newPullBenchStore(b, backend)
			defer closeFn()

			refillChunk := pullBenchRefillChunk(backend, 1)
			nextSeedID := seedPullBenchStoreRange(b, store, 0, refillChunk)
			queued := refillChunk
			srv := newPullBenchServer(store)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if queued < 1 {
					b.StopTimer()
					nextSeedID = seedPullBenchStoreRange(b, store, nextSeedID, refillChunk)
					queued += refillChunk
					b.StartTimer()
				}
				leaseIDs := pullBenchDequeue(b, srv, 1)
				pullBenchAckSingle(b, srv, leaseIDs[0])
				queued--
			}
		})
	}
}

func BenchmarkPullDequeueAckBatch15(b *testing.B) {
	benchmarkPullDequeueAckBatch(b, 15)
}

func BenchmarkPullDequeueAckBatch32(b *testing.B) {
	benchmarkPullDequeueAckBatch(b, 32)
}

func BenchmarkPullDequeueNackSingle(b *testing.B) {
	for _, backend := range []string{"memory", "sqlite"} {
		b.Run(backend, func(b *testing.B) {
			store, closeFn := newPullBenchStore(b, backend)
			defer closeFn()

			seedPullBenchStore(b, store, 1)
			srv := newPullBenchServer(store)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leaseIDs := pullBenchDequeue(b, srv, 1)
				pullBenchNackSingle(b, srv, leaseIDs[0])
			}
		})
	}
}

func BenchmarkPullDequeueExtendSingle(b *testing.B) {
	for _, backend := range []string{"memory", "sqlite"} {
		b.Run(backend, func(b *testing.B) {
			store, closeFn := newPullBenchStore(b, backend)
			defer closeFn()

			seedPullBenchStore(b, store, 1)
			srv := newPullBenchServer(store)

			leaseIDs := pullBenchDequeue(b, srv, 1)
			leaseID := leaseIDs[0]

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pullBenchExtendSingle(b, srv, leaseID)
			}
		})
	}
}

func BenchmarkPullAckRetryParallel(b *testing.B) {
	for _, backend := range []string{"memory", "sqlite"} {
		b.Run(backend, func(b *testing.B) {
			store, closeFn := newPullBenchStore(b, backend)
			defer closeFn()

			seedPullBenchStore(b, store, 1)
			srv := newPullBenchServer(store)
			leaseIDs := pullBenchDequeue(b, srv, 1)
			leaseID := leaseIDs[0]
			pullBenchAckSingle(b, srv, leaseID)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					pullBenchAckSingle(b, srv, leaseID)
				}
			})
		})
	}
}

func BenchmarkPullNackRetryParallel(b *testing.B) {
	for _, backend := range []string{"memory", "sqlite"} {
		b.Run(backend, func(b *testing.B) {
			store, closeFn := newPullBenchStore(b, backend)
			defer closeFn()

			seedPullBenchStore(b, store, 1)
			srv := newPullBenchServer(store)
			leaseIDs := pullBenchDequeue(b, srv, 1)
			leaseID := leaseIDs[0]
			pullBenchNackSingle(b, srv, leaseID)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					pullBenchNackSingle(b, srv, leaseID)
				}
			})
		})
	}
}

func BenchmarkMixedIngressDrain(b *testing.B) {
	for _, backend := range []string{"memory", "sqlite"} {
		b.Run(backend, func(b *testing.B) {
			store, closeFn := newPullBenchStore(b, backend)
			defer closeFn()

			ingressSrv := ingress.NewServer(store)
			ingressSrv.ResolveRoute = func(_ *http.Request, requestPath string) (string, bool) {
				if requestPath == mixedBenchIngressPath {
					return pullBenchInternalRoute, true
				}
				return "", false
			}
			ingressSrv.TargetsFor = func(route string) []string {
				if route == pullBenchInternalRoute {
					return []string{"pull"}
				}
				return nil
			}

			pullSrv := newPullBenchServer(store)

			const (
				workerCount = 16
				batchSize   = 15
			)
			stopCh := make(chan struct{})
			var wg sync.WaitGroup
			var drainErrors atomic.Int64
			for i := 0; i < workerCount; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					mixedBenchDrainWorker(stopCh, pullSrv, batchSize, &drainErrors)
				}()
			}

			latencyNanos := make([]int64, 0, b.N)
			var ingressRejects int64
			body := []byte(`{"ok":true}`)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				rr := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodPost, "http://example"+mixedBenchIngressPath, bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				ingressSrv.ServeHTTP(rr, req)

				latencyNanos = append(latencyNanos, time.Since(start).Nanoseconds())
				if rr.Code != http.StatusAccepted {
					ingressRejects++
				}
			}
			b.StopTimer()

			close(stopCh)
			wg.Wait()

			p95 := mixedBenchPercentileNanos(latencyNanos, 95)
			p99 := mixedBenchPercentileNanos(latencyNanos, 99)
			b.ReportMetric(float64(p95)/float64(time.Millisecond), "p95_ms")
			b.ReportMetric(float64(p99)/float64(time.Millisecond), "p99_ms")
			b.ReportMetric(float64(ingressRejects), "ingress_rejects")
			b.ReportMetric(float64(drainErrors.Load()), "drain_errors")
		})
	}
}

func mixedBenchDrainWorker(stopCh <-chan struct{}, srv *Server, batchSize int, drainErrors *atomic.Int64) {
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		leaseIDs, statusCode := mixedBenchDequeue(srv, batchSize)
		if statusCode != http.StatusOK {
			drainErrors.Add(1)
			continue
		}
		if len(leaseIDs) == 0 {
			continue
		}
		if mixedBenchAckBatch(srv, leaseIDs) != http.StatusOK {
			drainErrors.Add(1)
		}
	}
}

func mixedBenchDequeue(srv *Server, batchSize int) ([]string, int) {
	body := `{"batch":` + strconv.Itoa(batchSize) + `,"lease_ttl":"30s","max_wait":"10ms"}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example"+pullBenchEndpointRoute+"/dequeue", strings.NewReader(body))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		return nil, rr.Code
	}

	var out struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		return nil, http.StatusInternalServerError
	}

	leaseIDs := make([]string, 0, len(out.Items))
	for _, item := range out.Items {
		if item.LeaseID == "" {
			continue
		}
		leaseIDs = append(leaseIDs, item.LeaseID)
	}
	return leaseIDs, rr.Code
}

func mixedBenchAckBatch(srv *Server, leaseIDs []string) int {
	var body strings.Builder
	body.Grow(32 + len(leaseIDs)*40)
	body.WriteString(`{"lease_ids":[`)
	for i, leaseID := range leaseIDs {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteByte('"')
		body.WriteString(leaseID)
		body.WriteByte('"')
	}
	body.WriteString(`]}`)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example"+pullBenchEndpointRoute+"/ack", strings.NewReader(body.String()))
	srv.ServeHTTP(rr, req)
	return rr.Code
}

func mixedBenchPercentileNanos(values []int64, percentile int) int64 {
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

	rank := (percentile*len(sorted) + 99) / 100 // nearest-rank ceil(p*n/100)
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func benchmarkPullDequeueAckBatch(b *testing.B, batchSize int) {
	b.Helper()

	for _, backend := range []string{"memory", "sqlite"} {
		b.Run(backend, func(b *testing.B) {
			store, closeFn := newPullBenchStore(b, backend)
			defer closeFn()

			refillChunk := pullBenchRefillChunk(backend, batchSize)
			nextSeedID := seedPullBenchStoreRange(b, store, 0, refillChunk)
			queued := refillChunk
			srv := newPullBenchServer(store)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if queued < batchSize {
					b.StopTimer()
					nextSeedID = seedPullBenchStoreRange(b, store, nextSeedID, refillChunk)
					queued += refillChunk
					b.StartTimer()
				}
				leaseIDs := pullBenchDequeue(b, srv, batchSize)
				pullBenchAckBatch(b, srv, leaseIDs)
				queued -= batchSize
			}
		})
	}
}

func newPullBenchStore(b *testing.B, backend string) (queue.Store, func()) {
	b.Helper()

	switch backend {
	case "memory":
		s := queue.NewMemoryStore()
		return s, func() {}
	case "sqlite":
		dbPath := filepath.Join(b.TempDir(), "hookaido-bench.db")
		s, err := queue.NewSQLiteStore(
			dbPath,
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

func seedPullBenchStore(b *testing.B, store queue.Store, count int) {
	_ = seedPullBenchStoreRange(b, store, 0, count)
}

func seedPullBenchStoreRange(b *testing.B, store queue.Store, startID int, count int) int {
	b.Helper()

	now := time.Date(2026, 2, 4, 12, 0, 0, 0, time.UTC)
	if batchStore, ok := store.(queue.BatchEnqueuer); ok {
		items := make([]queue.Envelope, 0, count)
		for i := 0; i < count; i++ {
			items = append(items, queue.Envelope{
				ID:         fmt.Sprintf("bench_evt_%08d", startID+i),
				Route:      pullBenchInternalRoute,
				Target:     "pull",
				ReceivedAt: now,
				NextRunAt:  now,
				Payload:    []byte(`{"ok":true}`),
			})
		}
		if _, err := batchStore.EnqueueBatch(items); err != nil {
			b.Fatalf("seed batch enqueue count=%d: %v", count, err)
		}
		return startID + count
	}

	for i := 0; i < count; i++ {
		if err := store.Enqueue(queue.Envelope{
			ID:         fmt.Sprintf("bench_evt_%08d", startID+i),
			Route:      pullBenchInternalRoute,
			Target:     "pull",
			ReceivedAt: now,
			NextRunAt:  now,
			Payload:    []byte(`{"ok":true}`),
		}); err != nil {
			b.Fatalf("seed enqueue %d/%d: %v", i+1, count, err)
		}
	}
	return startID + count
}

func pullBenchRefillChunk(backend string, min int) int {
	chunk := pullBenchRefillMemory
	if backend == "sqlite" {
		chunk = pullBenchRefillSQLite
	}
	if chunk < min {
		return min
	}
	return chunk
}

func newPullBenchServer(store queue.Store) *Server {
	srv := NewServer(store)
	srv.ResolveRoute = func(endpoint string) (string, bool) {
		if endpoint == pullBenchEndpointRoute {
			return pullBenchInternalRoute, true
		}
		return "", false
	}
	return srv
}

func pullBenchDequeue(b *testing.B, srv *Server, batch int) []string {
	b.Helper()

	body := `{"batch":` + strconv.Itoa(batch) + `,"lease_ttl":"30s"}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example"+pullBenchEndpointRoute+"/dequeue", strings.NewReader(body))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		b.Fatalf("dequeue status=%d body=%s", rr.Code, rr.Body.String())
	}

	var out struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		b.Fatalf("decode dequeue response: %v", err)
	}
	if len(out.Items) != batch {
		b.Fatalf("expected %d dequeued items, got %d", batch, len(out.Items))
	}

	leaseIDs := make([]string, 0, len(out.Items))
	for _, item := range out.Items {
		leaseIDs = append(leaseIDs, item.LeaseID)
	}
	return leaseIDs
}

func pullBenchAckSingle(b *testing.B, srv *Server, leaseID string) {
	b.Helper()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example"+pullBenchEndpointRoute+"/ack", strings.NewReader(`{"lease_id":"`+leaseID+`"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		b.Fatalf("ack status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func pullBenchAckBatch(b *testing.B, srv *Server, leaseIDs []string) {
	b.Helper()

	var body strings.Builder
	body.Grow(32 + len(leaseIDs)*40)
	body.WriteString(`{"lease_ids":[`)
	for i, leaseID := range leaseIDs {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteByte('"')
		body.WriteString(leaseID)
		body.WriteByte('"')
	}
	body.WriteString(`]}`)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example"+pullBenchEndpointRoute+"/ack", strings.NewReader(body.String()))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		b.Fatalf("ack batch status=%d body=%s", rr.Code, rr.Body.String())
	}

	var out ackBatchResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		b.Fatalf("decode ack batch response: %v", err)
	}
	if out.Acked != len(leaseIDs) || len(out.Conflicts) != 0 {
		b.Fatalf("unexpected ack batch response: %+v", out)
	}
}

func pullBenchNackSingle(b *testing.B, srv *Server, leaseID string) {
	b.Helper()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example"+pullBenchEndpointRoute+"/nack", strings.NewReader(`{"lease_id":"`+leaseID+`","delay":"0s"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		b.Fatalf("nack status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func pullBenchExtendSingle(b *testing.B, srv *Server, leaseID string) {
	b.Helper()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example"+pullBenchEndpointRoute+"/extend", strings.NewReader(`{"lease_id":"`+leaseID+`","extend_by":"5s"}`))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		b.Fatalf("extend status=%d body=%s", rr.Code, rr.Body.String())
	}
}
