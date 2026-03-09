// Package hookaido defines module interfaces and the global module registry.
//
// Hookaido uses typed module interfaces per category (queue backend, tracing,
// worker transport, MCP) rather than a single generic Module interface.
// Each module category has its own registration function and lookup.
//
// The standard build imports all modules; minimal builds omit heavy
// dependencies by skipping module blank-imports.
package hookaido

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// Module is the base interface all modules implement.
type Module interface {
	// Name returns the unique identifier for this module (e.g. "sqlite", "postgres", "otel").
	Name() string
}

// QueueBackend provides a queue store implementation.
//
// OpenStore returns an any that must implement queue.Store. Callers assert the
// concrete interface after retrieval. This avoids an import cycle between the
// hookaido registry package and the queue package.
type QueueBackend interface {
	Module

	// OpenStore creates and returns a queue store configured with cfg.
	// The returned store value must implement queue.Store.
	// The closer function, if non-nil, is called during graceful shutdown.
	OpenStore(cfg QueueBackendConfig) (store any, closer func() error, err error)
}

// QueueBackendConfig holds the configuration passed to QueueBackend.OpenStore.
type QueueBackendConfig struct {
	// DSN is the backend-specific connection identifier.
	// SQLite: file path. Postgres: connection string. Memory: unused.
	DSN string

	// Queue limits.
	QueueMaxDepth   int
	QueueDropPolicy string

	// Queue retention (completed/expired message pruning).
	RetentionMaxAge        time.Duration
	RetentionPruneInterval time.Duration

	// Delivered message retention.
	DeliveredRetentionMaxAge time.Duration

	// DLQ retention.
	DLQRetentionMaxAge   time.Duration
	DLQRetentionMaxDepth int
}

// TracingProvider sets up distributed tracing and provides HTTP instrumentation.
type TracingProvider interface {
	Module

	// Init configures the tracing provider. The cfg value is expected to be
	// a config.ObservabilityConfig (typed as any to avoid import cycles).
	// Returns a shutdown function.
	Init(ctx context.Context, cfg any, version string, onError func(error)) (shutdown func(context.Context) error, err error)

	// WrapHandler wraps an HTTP handler with trace instrumentation.
	WrapHandler(name string, h http.Handler) http.Handler

	// HTTPClient returns an HTTP client with trace propagation, or nil.
	HTTPClient() *http.Client
}

// WorkerTransportConfig holds configuration for starting a worker transport.
type WorkerTransportConfig struct {
	// TLSConfig is the optional TLS configuration for the transport.
	// When non-nil the transport wraps the listener with TLS.
	TLSConfig *tls.Config

	// PullServer is the underlying pull API server (typed as any; must be
	// *pullapi.Server to avoid an import cycle with the registry package).
	PullServer any

	// ResolveRoute maps a pull endpoint to a route name.
	ResolveRoute func(endpoint string) (route string, ok bool)

	// Authorize checks if a worker request is authorized.
	Authorize func(ctx context.Context, endpoint string) bool

	// MaxLeaseBatch limits batch size for lease operations.
	MaxLeaseBatch int
}

// WorkerTransport provides a pull-worker transport (e.g. gRPC).
type WorkerTransport interface {
	Module

	// Serve starts the transport on the given listener with the provided
	// configuration. This call blocks until the transport is stopped or the
	// listener closes.
	Serve(listener net.Listener, cfg WorkerTransportConfig) error

	// Stop gracefully shuts down the transport.
	Stop(ctx context.Context) error
}

// MCPProvider supplies MCP (Model Context Protocol) server capabilities.
type MCPProvider interface {
	Module

	// ServeCommand handles the "mcp" CLI subcommand.
	// args are the arguments after "hookaido mcp" (e.g. ["serve", "--config", "..."]).
	// Returns the process exit code.
	ServeCommand(args []string) int
}
