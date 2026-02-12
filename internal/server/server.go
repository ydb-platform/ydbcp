package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"ydbcp/internal/audit"
	"ydbcp/internal/config"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
	"ydbcp/pkg/plugins/auth"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

type Server interface {
	GRPCServer() *grpc.Server
	Start(context.Context, *sync.WaitGroup) error
}

type ServerImpl struct {
	addr   string
	server *grpc.Server
}

func (s *ServerImpl) GRPCServer() *grpc.Server {
	return s.server
}

func (s *ServerImpl) Start(ctx context.Context, wg *sync.WaitGroup) error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		xlog.Info(ctx, "Starting server", zap.String(log_keys.Address, lis.Addr().String()))
		if err := s.server.Serve(lis); err != nil {
			xlog.Error(ctx, "failed to serve", zap.Error(err))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		s.server.GracefulStop()
	}()
	return nil
}

func NewServer(cfg *config.GRPCServerConfig, provider auth.AuthProvider) (*ServerImpl, error) {
	opts := []grpc.ServerOption{}
	if len(cfg.TLSCertificatePath) > 0 && len(cfg.TLSKeyPath) > 0 {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCertificatePath, cfg.TLSKeyPath)
		if err != nil {
			return nil, fmt.Errorf("can't load tls certificates for GRPC server: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}
	auditInterceptor := audit.NewAuditGRPCInterceptor(provider)
	opts = append(opts, grpc.UnaryInterceptor(auditInterceptor))
	server := grpc.NewServer(opts...)
	reflection.Register(server)
	return &ServerImpl{
		addr:   fmt.Sprintf("%s:%d", cfg.BindAddress, cfg.BindPort),
		server: server,
	}, nil
}
