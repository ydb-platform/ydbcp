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

type AuditFields struct {
	ContainerID string
	Database    string
}

var (
	containerStore = sync.Map{} // map[requestID]string
)

func SetAuditFieldsForRequest(ctx context.Context, fields *AuditFields) {
	containerStore.Store(grpcinfo.GetRequestID(ctx), fields)
}

func GetAuditFieldsForRequest(requestID string) *AuditFields {
	v, ok := containerStore.Load(requestID)
	if !ok {
		return nil
	}
	return v.(*AuditFields)
}

func ClearAuditFieldsForRequest(requestID string) {
	containerStore.Delete(requestID)
}

func NewAuditGRPCInterceptor(provider auth.AuthProvider) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (interface{}, error) {
		ctx = grpcinfo.WithGRPCInfo(ctx)
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
		fields := GetAuditFieldsForRequest(requestID)
		defer ClearAuditFieldsForRequest(requestID)
		ReportGRPCCallEnd(ctx, info.FullMethod, subject, fields.ContainerID, fields.Database, token, grpcErr)
		return response, grpcErr
	}
}
