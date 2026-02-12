package grpcinfo

import (
	"context"
	"ydbcp/internal/util/log_keys"
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
		if ok {
			return &v
		}
	}
	return nil
}

type ctxKeyRequestID struct{}

func SetRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID{}, id)
}

func GetRequestID(ctx context.Context) (string, bool) {
	if id, ok := ctx.Value(ctxKeyRequestID{}).(string); ok {
		return id, false
	}
	for _, key := range []string{"RequestID", "RequestId", "request-id", "request_id"} {
		val := getFromCtx(ctx, key)
		if val != nil {
			return *val, false
		}
	}
	return uuid.New().String(), true
}

func GetTraceID(ctx context.Context) *string {
	return getFromCtx(ctx, log_keys.TraceID)
}

func GetGRPCHeaderValue(ctx context.Context, key string) *string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	headerValues := md.Get(key)
	if len(headerValues) == 0 {
		return nil
	}
	return &headerValues[0]
}

func GetRemoteAddressChain(ctx context.Context) *string {
	headers := []string{
		"x_forwarded_for", "X-Forwarded-For", "downstream_remote_address", "req_headers_x_envoy_external_address",
	}
	for _, header := range headers {
		v := GetGRPCHeaderValue(ctx, header)
		if v != nil {
			return v
		}
	}
	return nil
}

func WithGRPCInfo(ctx context.Context) context.Context {
	if p, ok := peer.FromContext(ctx); ok {
		ctx = xlog.With(ctx, zap.String(log_keys.RemoteAddr, p.Addr.String()))
	}
	if method, ok := grpc.Method(ctx); ok {
		ctx = xlog.With(ctx, zap.String(log_keys.GRPCMethod, method))
	}
	requestID, newID := GetRequestID(ctx)
	ctx = xlog.With(ctx, zap.String(log_keys.RequestID, requestID))
	err := grpc.SendHeader(ctx, metadata.Pairs("X-Request-ID", requestID))
	if err != nil {
		xlog.Error(ctx, "failed to set X-Request-ID header", zap.Error(err))
	}
	xlog.Debug(ctx, "New grpc request")
	if newID {
		ctx = SetRequestID(ctx, requestID)
	}
	return ctx
}
