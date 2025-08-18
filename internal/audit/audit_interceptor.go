package audit

import (
	"context"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"sync"
	authHelper "ydbcp/internal/auth"
	"ydbcp/internal/server/grpcinfo"
	"ydbcp/internal/util/xlog"
	"ydbcp/pkg/plugins/auth"
)

var (
	containerStore = sync.Map{} // map[requestID]string
)

func SetContainerIDForRequest(ctx context.Context, containerID string) {
	containerStore.Store(grpcinfo.GetRequestID(ctx), containerID)
}

func GetContainerIDForRequest(requestID string) string {
	v, ok := containerStore.Load(requestID)
	if !ok {
		return "{none}"
	}
	return v.(string)
}

func ClearContainerIDForRequest(requestID string) {
	containerStore.Delete(requestID)
}

func NewAuditGRPCInterceptor(provider auth.AuthProvider) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (interface{}, error) {
		requestID := grpcinfo.GetRequestID(ctx)
		ctx = grpcinfo.SetRequestID(ctx, requestID)
		subject, _ := authHelper.Authenticate(ctx, provider)
		token, _ := authHelper.GetMaskedToken(ctx, provider)
		pm, ok := req.(proto.Message)
		if !ok {
			xlog.Error(ctx, "got invalid proto.Message", zap.Any("GRPCRequest", req))
		} else {
			ReportGRPCCallBegin(
				ctx, pm, info.FullMethod, subject, token,
			)
		}
		response, grpcErr := handler(ctx, req)
		containerID := GetContainerIDForRequest(requestID)
		defer ClearContainerIDForRequest(requestID)
		ReportGRPCCallEnd(ctx, info.FullMethod, subject, containerID, token, grpcErr)
		return response, grpcErr
	}
}
