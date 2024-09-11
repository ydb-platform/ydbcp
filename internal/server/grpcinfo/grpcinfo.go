package grpcinfo

import (
	"context"
	"ydbcp/internal/util/xlog"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func WithGRPCInfo(ctx context.Context) context.Context {
	if p, ok := peer.FromContext(ctx); ok {
		ctx = xlog.With(ctx, zap.String("RemoteAddr", p.Addr.String()))
	}
	if method, ok := grpc.Method(ctx); ok {
		ctx = xlog.With(ctx, zap.String("GRPCMethod", method))
	}
	requestID := uuid.New().String()
	ctx = xlog.With(ctx, zap.String("RequestID", requestID))
	grpc.SendHeader(ctx, metadata.Pairs("X-Request-Id", requestID))
	xlog.Debug(ctx, "New grpc request")
	return ctx
}
