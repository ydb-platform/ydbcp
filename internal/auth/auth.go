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
	PermissionBackupList           = "ydb.databases.list"
	PermissionBackupCreate         = "ydb.databases.backup"
	PermissionBackupRestore        = "ydb.tables.create"
	PermissionBackupGet            = "ydb.databases.get"
	PermissionBackupScheduleCreate = "ydbcp.backupSchedules.create"
)

var (
	ErrGetAuthToken = errors.New("can't get auth token")
)

type AuthError struct {
	Code        auth.AuthCode
	Message     string
	Permission  string
	ContainerID string
	Subject     string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf(
		"authorization failed: subject=%s, permission=%s, container=%s, code=%s, message=%s",
		e.Subject, e.Permission, e.ContainerID, e.Code.String(), e.Message,
	)
}

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
	err = processAuthResult(ctx, resp, subject, err, permission, containerID)
	if err != nil {
		return "", err
	}
	return subject, nil
}

func CheckCreateScheduleAuth(ctx context.Context, provider auth.AuthProvider, containerID, resourceID string) (
	string, error,
) {
	ctx = xlog.With(ctx, zap.String("ContainerID", containerID), zap.String("ResourceID", resourceID))
	token, err := TokenFromGRPCContext(ctx)
	if err != nil {
		xlog.Debug(ctx, "can't get auth token", zap.Error(err))
		token = ""
	}

	// First, try the primary permission (PermissionBackupCreate) for the requested container
	primaryCheck := auth.AuthorizeCheck{
		Permission:  PermissionBackupCreate,
		ContainerID: containerID,
	}
	if len(resourceID) > 0 {
		primaryCheck.ResourceID = []string{resourceID}
	}

	resp, subject, err := provider.Authorize(ctx, token, primaryCheck)
	err = processAuthResult(ctx, resp, subject, err, PermissionBackupCreate, containerID)

	if err == nil {
		return subject, nil
	}
	ydbcpContainerID := provider.GetYDBCPContainerID()
	if len(ydbcpContainerID) > 0 {
		fallbackCheck := auth.AuthorizeCheck{
			Permission:  PermissionBackupScheduleCreate,
			ContainerID: ydbcpContainerID,
		}
		if len(resourceID) > 0 {
			fallbackCheck.ResourceID = []string{resourceID}
		}

		check, subject, managerErr := provider.Authorize(ctx, token, fallbackCheck)
		createAnyErr := processAuthResult(ctx, check, subject, managerErr, PermissionBackupScheduleCreate, ydbcpContainerID)
		if createAnyErr == nil {
			return subject, nil
		}
		err = errors.Join(err, createAnyErr)
	}

	return "", err
}

func processAuthResult(ctx context.Context, resp []auth.AuthorizeResult, subject string, err error, permission, containerID string) error {
	if err != nil {
		xlog.Error(ctx, "auth plugin authorize error", zap.Error(err))
		return status.Error(codes.Internal, err.Error())
	}
	if len(resp) != 1 {
		msg := "incorrect auth plugin response length != 1"
		xlog.Error(ctx, msg)
		return status.Error(codes.Internal, msg)
	}
	if resp[0].Code != auth.AuthCodeSuccess {
		authErr := formatAuthError(resp[0].Code, resp[0].Message, subject, permission, containerID)
		xlog.Error(
			ctx, "auth plugin response", zap.Error(authErr),
		)
		return status.Error(codes.PermissionDenied, authErr.Error())
	}
	return nil
}

func formatAuthError(code auth.AuthCode, message, subject, permission, containerID string) *AuthError {
	return &AuthError{
		Code:        code,
		Message:     message,
		Subject:     subject,
		Permission:  permission,
		ContainerID: containerID,
	}
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
