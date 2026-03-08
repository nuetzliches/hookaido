package hookaido

import (
	"sort"
	"sync"
)

// Global module registries, populated by init() functions in module packages.
// The standard build imports all modules; minimal builds import only what they need.

var (
	mu               sync.RWMutex
	queueBackends    = map[string]QueueBackend{}
	tracingProviders = map[string]TracingProvider{}
	mcpProviders     []MCPProvider
	workerTransports = map[string]WorkerTransport{}
)

// RegisterQueueBackend registers a queue backend module.
// Panics if a backend with the same name is already registered.
func RegisterQueueBackend(b QueueBackend) {
	mu.Lock()
	defer mu.Unlock()
	name := b.Name()
	if _, exists := queueBackends[name]; exists {
		panic("hookaido: queue backend already registered: " + name)
	}
	queueBackends[name] = b
}

// LookupQueueBackend returns the registered queue backend with the given name.
func LookupQueueBackend(name string) (QueueBackend, bool) {
	mu.RLock()
	defer mu.RUnlock()
	b, ok := queueBackends[name]
	return b, ok
}

// HasQueueBackend reports whether a queue backend with the given name is registered.
func HasQueueBackend(name string) bool {
	mu.RLock()
	defer mu.RUnlock()
	_, ok := queueBackends[name]
	return ok
}

// QueueBackendNames returns sorted names of all registered queue backends.
func QueueBackendNames() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(queueBackends))
	for name := range queueBackends {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// RegisterTracingProvider registers a tracing provider module.
// Panics if a provider with the same name is already registered.
func RegisterTracingProvider(p TracingProvider) {
	mu.Lock()
	defer mu.Unlock()
	name := p.Name()
	if _, exists := tracingProviders[name]; exists {
		panic("hookaido: tracing provider already registered: " + name)
	}
	tracingProviders[name] = p
}

// LookupTracingProvider returns the registered tracing provider with the given name.
func LookupTracingProvider(name string) (TracingProvider, bool) {
	mu.RLock()
	defer mu.RUnlock()
	p, ok := tracingProviders[name]
	return p, ok
}

// RegisterMCPProvider registers an MCP provider module.
func RegisterMCPProvider(p MCPProvider) {
	mu.Lock()
	defer mu.Unlock()
	mcpProviders = append(mcpProviders, p)
}

// MCPProviders returns all registered MCP providers.
func MCPProviders() []MCPProvider {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]MCPProvider, len(mcpProviders))
	copy(out, mcpProviders)
	return out
}

// RegisterWorkerTransport registers a worker transport module.
// Panics if a transport with the same name is already registered.
func RegisterWorkerTransport(t WorkerTransport) {
	mu.Lock()
	defer mu.Unlock()
	name := t.Name()
	if _, exists := workerTransports[name]; exists {
		panic("hookaido: worker transport already registered: " + name)
	}
	workerTransports[name] = t
}

// LookupWorkerTransport returns the registered worker transport with the given name.
func LookupWorkerTransport(name string) (WorkerTransport, bool) {
	mu.RLock()
	defer mu.RUnlock()
	t, ok := workerTransports[name]
	return t, ok
}

// ResetForTesting clears all registries. Only for use in tests.
func ResetForTesting() {
	mu.Lock()
	defer mu.Unlock()
	queueBackends = map[string]QueueBackend{}
	tracingProviders = map[string]TracingProvider{}
	mcpProviders = nil
	workerTransports = map[string]WorkerTransport{}
}
