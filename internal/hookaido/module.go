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
	"net"
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

// TracingProvider sets up distributed tracing.
type TracingProvider interface {
	Module

	// Setup configures the tracing provider using the given configuration.
	// The cfg value is expected to be a config.ObservabilityConfig but typed as
	// any to avoid import cycles. Returns a shutdown function.
	Setup(cfg any) (shutdown func(context.Context) error, err error)
}

// WorkerTransport provides a pull-worker transport (e.g. gRPC).
type WorkerTransport interface {
	Module

	// Serve starts the transport, accepting connections on the given listener.
	// The handler value must implement the pull handler interface.
	Serve(listener net.Listener, handler any) error

	// Stop gracefully shuts down the transport.
	Stop(ctx context.Context) error
}

// MCPProvider supplies MCP (Model Context Protocol) server capabilities.
type MCPProvider interface {
	Module
}
