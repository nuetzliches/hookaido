package hookaido

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Minimal stub types
// ---------------------------------------------------------------------------

type stubQueueBackend struct{ name string }

func (s stubQueueBackend) Name() string { return s.name }
func (s stubQueueBackend) OpenStore(QueueBackendConfig) (any, func() error, error) {
	return nil, nil, nil
}

type stubTracingProvider struct{ name string }

func (s stubTracingProvider) Name() string { return s.name }
func (s stubTracingProvider) Init(context.Context, any, string, func(error)) (func(context.Context) error, error) {
	return nil, nil
}
func (s stubTracingProvider) WrapHandler(_ string, h http.Handler) http.Handler { return h }
func (s stubTracingProvider) HTTPClient() *http.Client                          { return nil }

type stubWorkerTransport struct{ name string }

func (s stubWorkerTransport) Name() string                                    { return s.name }
func (s stubWorkerTransport) Serve(net.Listener, WorkerTransportConfig) error { return nil }
func (s stubWorkerTransport) Stop(context.Context) error                      { return nil }

type stubMCPProvider struct{ name string }

func (s stubMCPProvider) Name() string              { return s.name }
func (s stubMCPProvider) ServeCommand([]string) int { return 0 }

// ---------------------------------------------------------------------------
// QueueBackend tests
// ---------------------------------------------------------------------------

func TestRegisterAndLookupQueueBackend(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterQueueBackend(stubQueueBackend{name: "sqlite"})

	got, ok := LookupQueueBackend("sqlite")
	if !ok {
		t.Fatal("expected LookupQueueBackend to find 'sqlite'")
	}
	if got.Name() != "sqlite" {
		t.Fatalf("expected name 'sqlite', got %q", got.Name())
	}
}

func TestLookupQueueBackend_NotFound(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	_, ok := LookupQueueBackend("nonexistent")
	if ok {
		t.Fatal("expected LookupQueueBackend to return false for unregistered name")
	}
}

func TestRegisterQueueBackend_DuplicatePanics(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterQueueBackend(stubQueueBackend{name: "sqlite"})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate QueueBackend registration")
		}
	}()

	RegisterQueueBackend(stubQueueBackend{name: "sqlite"})
}

func TestHasQueueBackend(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	if HasQueueBackend("memory") {
		t.Fatal("expected HasQueueBackend to return false before registration")
	}

	RegisterQueueBackend(stubQueueBackend{name: "memory"})

	if !HasQueueBackend("memory") {
		t.Fatal("expected HasQueueBackend to return true after registration")
	}
}

func TestQueueBackendNames(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	// Register in unsorted order.
	RegisterQueueBackend(stubQueueBackend{name: "sqlite"})
	RegisterQueueBackend(stubQueueBackend{name: "memory"})
	RegisterQueueBackend(stubQueueBackend{name: "postgres"})

	names := QueueBackendNames()
	want := []string{"memory", "postgres", "sqlite"}

	if len(names) != len(want) {
		t.Fatalf("expected %d names, got %d", len(want), len(names))
	}
	for i, n := range names {
		if n != want[i] {
			t.Errorf("names[%d] = %q, want %q", i, n, want[i])
		}
	}
}

// ---------------------------------------------------------------------------
// TracingProvider tests
// ---------------------------------------------------------------------------

func TestRegisterAndLookupTracingProvider(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterTracingProvider(stubTracingProvider{name: "otel"})

	got, ok := LookupTracingProvider("otel")
	if !ok {
		t.Fatal("expected LookupTracingProvider to find 'otel'")
	}
	if got.Name() != "otel" {
		t.Fatalf("expected name 'otel', got %q", got.Name())
	}
}

func TestLookupTracingProvider_NotFound(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	_, ok := LookupTracingProvider("nonexistent")
	if ok {
		t.Fatal("expected LookupTracingProvider to return false for unregistered name")
	}
}

func TestRegisterTracingProvider_DuplicatePanics(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterTracingProvider(stubTracingProvider{name: "otel"})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate TracingProvider registration")
		}
	}()

	RegisterTracingProvider(stubTracingProvider{name: "otel"})
}

// ---------------------------------------------------------------------------
// WorkerTransport tests
// ---------------------------------------------------------------------------

func TestRegisterAndLookupWorkerTransport(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterWorkerTransport(stubWorkerTransport{name: "grpc"})

	got, ok := LookupWorkerTransport("grpc")
	if !ok {
		t.Fatal("expected LookupWorkerTransport to find 'grpc'")
	}
	if got.Name() != "grpc" {
		t.Fatalf("expected name 'grpc', got %q", got.Name())
	}
}

func TestLookupWorkerTransport_NotFound(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	_, ok := LookupWorkerTransport("nonexistent")
	if ok {
		t.Fatal("expected LookupWorkerTransport to return false for unregistered name")
	}
}

func TestRegisterWorkerTransport_DuplicatePanics(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterWorkerTransport(stubWorkerTransport{name: "grpc"})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate WorkerTransport registration")
		}
	}()

	RegisterWorkerTransport(stubWorkerTransport{name: "grpc"})
}

// ---------------------------------------------------------------------------
// MCPProvider tests
// ---------------------------------------------------------------------------

func TestRegisterAndLookupMCPProvider(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterMCPProvider(stubMCPProvider{name: "mcp1"})

	providers := MCPProviders()
	if len(providers) != 1 {
		t.Fatalf("expected 1 MCP provider, got %d", len(providers))
	}
	if providers[0].Name() != "mcp1" {
		t.Fatalf("expected name 'mcp1', got %q", providers[0].Name())
	}
}

func TestMCPProviders_ReturnsCopy(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterMCPProvider(stubMCPProvider{name: "mcp1"})
	RegisterMCPProvider(stubMCPProvider{name: "mcp2"})

	providers := MCPProviders()
	if len(providers) != 2 {
		t.Fatalf("expected 2 providers, got %d", len(providers))
	}

	// Mutate the returned slice and verify the registry is unaffected.
	providers[0] = stubMCPProvider{name: "mutated"}

	fresh := MCPProviders()
	if fresh[0].Name() == "mutated" {
		t.Fatal("MCPProviders returned the internal slice, not a copy")
	}
	if fresh[0].Name() != "mcp1" {
		t.Fatalf("expected first provider 'mcp1', got %q", fresh[0].Name())
	}
}

// ---------------------------------------------------------------------------
// ResetForTesting
// ---------------------------------------------------------------------------

func TestResetForTesting(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	RegisterQueueBackend(stubQueueBackend{name: "sqlite"})
	RegisterTracingProvider(stubTracingProvider{name: "otel"})
	RegisterWorkerTransport(stubWorkerTransport{name: "grpc"})
	RegisterMCPProvider(stubMCPProvider{name: "mcp1"})

	ResetForTesting()

	if HasQueueBackend("sqlite") {
		t.Error("expected queue backend cleared after ResetForTesting")
	}
	if _, ok := LookupTracingProvider("otel"); ok {
		t.Error("expected tracing provider cleared after ResetForTesting")
	}
	if _, ok := LookupWorkerTransport("grpc"); ok {
		t.Error("expected worker transport cleared after ResetForTesting")
	}
	if len(MCPProviders()) != 0 {
		t.Error("expected MCP providers cleared after ResetForTesting")
	}
}

// ---------------------------------------------------------------------------
// Concurrent registration safety
// ---------------------------------------------------------------------------

func TestConcurrentRegistration(t *testing.T) {
	ResetForTesting()
	defer ResetForTesting()

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n * 4)

	for i := 0; i < n; i++ {
		name := fmt.Sprintf("backend-%d", i)
		go func() {
			defer wg.Done()
			RegisterQueueBackend(stubQueueBackend{name: name})
		}()
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("tracer-%d", i)
		go func() {
			defer wg.Done()
			RegisterTracingProvider(stubTracingProvider{name: name})
		}()
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("transport-%d", i)
		go func() {
			defer wg.Done()
			RegisterWorkerTransport(stubWorkerTransport{name: name})
		}()
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("mcp-%d", i)
		go func() {
			defer wg.Done()
			RegisterMCPProvider(stubMCPProvider{name: name})
		}()
	}

	wg.Wait()

	// Verify all registrations landed.
	names := QueueBackendNames()
	if len(names) != n {
		t.Errorf("expected %d queue backends, got %d", n, len(names))
	}
	providers := MCPProviders()
	if len(providers) != n {
		t.Errorf("expected %d MCP providers, got %d", n, len(providers))
	}
	for i := 0; i < n; i++ {
		if _, ok := LookupTracingProvider(fmt.Sprintf("tracer-%d", i)); !ok {
			t.Errorf("missing tracing provider tracer-%d", i)
		}
		if _, ok := LookupWorkerTransport(fmt.Sprintf("transport-%d", i)); !ok {
			t.Errorf("missing worker transport transport-%d", i)
		}
	}
}
