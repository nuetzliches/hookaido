package grpcworker

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/nuetzliches/hookaido/internal/hookaido"
	"github.com/nuetzliches/hookaido/internal/pullapi"
	workerapipb "github.com/nuetzliches/hookaido/modules/grpcworker/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func init() {
	hookaido.RegisterWorkerTransport(&grpcWorkerModule{})
}

type grpcWorkerModule struct {
	srv *grpc.Server
}

func (m *grpcWorkerModule) Name() string { return "grpc" }

func (m *grpcWorkerModule) Serve(ln net.Listener, cfg hookaido.WorkerTransportConfig) error {
	pullServer, ok := cfg.PullServer.(*pullapi.Server)
	if !ok {
		return fmt.Errorf("grpcworker: PullServer must be *pullapi.Server, got %T", cfg.PullServer)
	}

	var opts []grpc.ServerOption
	if cfg.TLSConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(cfg.TLSConfig)))
	}

	m.srv = grpc.NewServer(opts...)
	ws := NewServer(pullServer)
	ws.ResolveRoute = cfg.ResolveRoute
	if cfg.Authorize != nil {
		ws.Authorize = cfg.Authorize
	}
	if cfg.MaxLeaseBatch > 0 {
		ws.MaxLeaseBatch = cfg.MaxLeaseBatch
	}
	workerapipb.RegisterWorkerServiceServer(m.srv, ws)
	return m.srv.Serve(ln)
}

func (m *grpcWorkerModule) Stop(ctx context.Context) error {
	if m.srv == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		m.srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		m.srv.Stop()
		<-done
		return ctx.Err()
	}
}

// TLSConfigForGRPC is a helper that builds a *tls.Config from an existing
// TLS config. Callers can use this when the transport needs TLS but must
// remain gRPC-import-free.
func TLSConfigForGRPC(cfg *tls.Config) *tls.Config {
	if cfg == nil {
		return nil
	}
	return cfg.Clone()
}
