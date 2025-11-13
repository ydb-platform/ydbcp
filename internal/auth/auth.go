package auth

import (
	"context"
	"errors"
	"fmt"

	"ydbcp/internal/config"
	"ydbcp/internal/plugin"
	"ydbcp/internal/util/xlog"
	"ydbcp/pkg/plugins/auth"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	PermissionBackupList    = "ydb.databases.list"
	PermissionBackupCreate  = "ydb.databases.backup"
	PermissionBackupRestore = "ydb.tables.create"
	PermissionBackupGet     = "ydb.databases.get"
)

var (
	ErrGetAuthToken = errors.New("can't get auth token")
)

func NewAuthProvider(ctx context.Context, cfg config.PluginConfig) (auth.AuthProvider, error) {
	return plugin.Load[auth.AuthProvider](ctx, cfg, "AuthProvider", "auth")
}

func Authenticate(ctx context.Context, provider auth.AuthProvider) (string, error) {
	token, err := TokenFromGRPCContext(ctx)
	if err != nil {
		xlog.Debug(ctx, "can't get auth token", zap.Error(err))
		token = ""
	}
	return provider.Authenticate(ctx, token)
}

func GetMaskedToken(ctx context.Context, provider auth.AuthProvider) (string, error) {
	t, err := TokenFromGRPCContext(ctx)
	if err == nil {
		return provider.MaskToken(t), nil
	} else {
		return t, err
	}
}

func CheckAuth(ctx context.Context, provider auth.AuthProvider, permission, containerID, resourceID string) (
	string, error,
) {
	ctx = xlog.With(ctx, zap.String("ContainerID", containerID), zap.String("ResourceID", resourceID))
	token, err := TokenFromGRPCContext(ctx)
	if err != nil {
		xlog.Debug(ctx, "can't get auth token", zap.Error(err))
		token = ""
	}
	check := auth.AuthorizeCheck{
		Permission:  permission,
		ContainerID: containerID,
	}
	if len(resourceID) > 0 {
		check.ResourceID = []string{resourceID}
	}

	resp, subject, err := provider.Authorize(ctx, token, check)
	if err != nil {
		xlog.Error(ctx, "auth plugin authorize error", zap.Error(err))
		return "", status.Error(codes.Internal, "authorize error")
	}
	if len(resp) != 1 {
		xlog.Error(ctx, "incorrect auth plugin response length != 1")
		return "", status.Error(codes.Internal, "authorize error")
	}
	if resp[0].Code != auth.AuthCodeSuccess {
		xlog.Error(
			ctx, "auth plugin response", zap.String("AuthCode", resp[0].Code.String()),
			zap.String("AuthMessage", resp[0].Message),
		)
		return "", status.Errorf(
			codes.PermissionDenied, "Code: %s, Message: %s", resp[0].Code.String(), resp[0].Message,
		)
	}
	return subject, nil
}

func TokenFromGRPCContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrGetAuthToken
	}
	tokens, ok := md["authorization"]
	if !ok {
		return "", fmt.Errorf("can't find authorization header, %w", ErrGetAuthToken)
	}
	if len(tokens) == 0 {
		return "", fmt.Errorf("incorrect authorization header format, %w", ErrGetAuthToken)
	}
	token := tokens[0]
	if len(token) < 8 || token[0:7] != "Bearer " {
		return "", fmt.Errorf("incorrect authorization header format, %w", ErrGetAuthToken)
	}
	token = token[7:]
	return token, nil
}
