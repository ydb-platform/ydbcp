package processor

import (
	"context"
	"errors"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"
	client_db_connector "ydbcp/internal/client_db_connector"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	ydbcp_db_connector "ydbcp/internal/ydbcp-db-connector"
)

type OperationHandler func(context.Context, types.Operation) (
	types.Operation, error,
)

func MakeTBOperationHandler(db ydbcp_db_connector.YdbDriver, clientDb client_db_connector.ClientDbConnector) OperationHandler {
	return func(ctx context.Context, op types.Operation) (types.Operation, error) {
		return TBOperationHandler(ctx, op, db, clientDb)
	}
}

func TBOperationHandler(
	ctx context.Context,
	operation types.Operation,
	db ydbcp_db_connector.YdbDriver,
	clientDb client_db_connector.ClientDbConnector,
) (types.Operation, error) {
	if operation.GetType() != "TB" {
		return operation, errors.New("Passed wrong op type to TBOperationHandler")
	}
	tb := operation.(*types.TakeBackupOperation)
	//lookup YdbServerOperationStatus
	opInfo, err := clientDb.GetOperationStatus(ctx, tb.YdbConnectionParams, tb.YdbOperationId)
	if err != nil {
		//skip, write log
		//upsert message into operation?
		xlog.Error(
			ctx,
			"Failed to lookup operation status for",
			zap.String("operation_id", tb.YdbOperationId),
			zap.Error(err),
		)
		return operation, nil
	}
	switch tb.State {
	case types.OperationStatePending:
		{
			if !opInfo.Ready {
				//if pending: return op, nil
				//if backup deadline failed: cancel operation. (skip for now)
				return operation, nil
			} else {
				if opInfo.Status == Ydb.StatusIds_SUCCESS {
					//upsert into operations (id, status) values (id, done)?
					//db.StartUpdate()
					//.WithUpdateBackup()
					//.WithYUpdateOperation()
					err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateAvailable)
					if err != nil {
						xlog.Error(ctx, "error updating backup table", zap.Error(err))
						return operation, nil
					} else {
						operation.SetState(types.OperationStateDone)
						operation.SetMessage("Success")
					}
				} else {
					//op.State = Error
					//upsert into operations (id, status, message) values (id, error, message)?
					err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateError)
					if err != nil {
						xlog.Error(ctx, "error updating backup table", zap.Error(err))
						return operation, nil
					} else {
						if opInfo.Status == Ydb.StatusIds_CANCELLED {
							operation.SetMessage("got CANCELLED status for PENDING operation")
						} else {
							operation.SetMessage(types.IssuesToString(opInfo.Issues))
						}
						operation.SetState(types.OperationStateError)
					}
				}
			}
		}
	case types.OperationStateCancelling:
		{
			if !opInfo.Ready {
				//can this hang in cancelling state?
				return operation, nil
			} else {
				if opInfo.Status == Ydb.StatusIds_CANCELLED {
					//upsert into operations (id, status, message) values (id, cancelled)?
					err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateCancelled)
					if err != nil {
						xlog.Error(ctx, "error updating backup table", zap.Error(err))
						return operation, nil
					} else {
						operation.SetState(types.OperationStateCancelled)
						operation.SetMessage("Success")
					}
				} else {
					//upsert into operations (id, status, message) values (id, error, error.message)?
					err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateError)
					if err != nil {
						xlog.Error(ctx, "error updating backup table", zap.Error(err))
						return operation, nil
					} else {
						operation.SetState(types.OperationStateError)
						operation.SetMessage(types.IssuesToString(opInfo.Issues))
					}
				}
			}
		}
	}
	return operation, nil
}
