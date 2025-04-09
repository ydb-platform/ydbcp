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

func getFromCtx(ctx context.Context, key string) *string {
	if ctx.Value(key) != nil {
		v, ok := ctx.Value(key).(string)
		if !ok {
			return &v
		}
	}
	return nil
}

func getRequestID(ctx context.Context) string {
	for _, key := range []string{"RequestID", "RequestId", "request-id", "request_id"} {
		val := getFromCtx(ctx, key)
		if val != nil {
			return *val
		}
	}
	return uuid.New().String()
}

func WithGRPCInfo(ctx context.Context) context.Context {
	if p, ok := peer.FromContext(ctx); ok {
		ctx = xlog.With(ctx, zap.String("RemoteAddr", p.Addr.String()))
	}
	if method, ok := grpc.Method(ctx); ok {
		ctx = xlog.With(ctx, zap.String("GRPCMethod", method))
	}
	requestID := getRequestID(ctx)
	ctx = xlog.With(ctx, zap.String("RequestID", requestID))
	err := grpc.SendHeader(ctx, metadata.Pairs("X-Request-ID", requestID))
	if err != nil {
		xlog.Error(ctx, "failed to set X-Request-ID header", zap.Error(err))
	}
	xlog.Debug(ctx, "New grpc request")
	return ctx
}
