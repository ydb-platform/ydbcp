package helpers

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/types"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
)

func CheckClientDbAccess(ctx context.Context, clientConn client.ClientConnector, clientConnectionParams types.YdbConnectionParams) error {
	driver, err := GetClientDbAccess(ctx, clientConn, clientConnectionParams)
	if driver != nil {
		_ = driver.Close(ctx)
	}
	return err
}

func GetClientDbAccess(
	ctx context.Context, clientConn client.ClientConnector, clientConnectionParams types.YdbConnectionParams) (*ydb.Driver, error) {
	dsn := types.MakeYdbConnectionString(clientConnectionParams)
	ctx = xlog.With(ctx, zap.String(log_keys.ClientDSN, dsn))
	connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	clientDriver, err := clientConn.Open(connCtx, dsn)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, err.Error())
	}
	_, err = clientConn.ListExportOperations(connCtx, clientDriver)
	if err != nil {
		xlog.Error(ctx, "can't list export operations", zap.Error(err))
		return nil, status.Errorf(codes.PermissionDenied, "user has no access to database %s", dsn)
	}
	return clientDriver, nil
}
