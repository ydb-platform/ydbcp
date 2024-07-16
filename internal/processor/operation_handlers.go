package processor

import (
	"context"
	"errors"
	"fmt"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"
)

type OperationHandler func(context.Context, types.Operation) (
	types.Operation, error,
)

func MakeTBOperationHandler(db db.DBConnector, client client.ClientConnector) OperationHandler {
	return func(ctx context.Context, op types.Operation) (types.Operation, error) {
		return TBOperationHandler(ctx, op, db, client)
	}
}

func TBOperationHandler(
	ctx context.Context,
	operation types.Operation,
	db db.DBConnector,
	client client.ClientConnector,
) (types.Operation, error) {
	if operation.GetType() != types.OperationTypeTB {
		return operation, errors.New("Passed wrong operation type to TBOperationHandler")
	}
	tb, ok := operation.(*types.TakeBackupOperation)
	if !ok {
		return operation, fmt.Errorf("can't cast Operation to TakeBackupOperation %s", types.OperationToString(operation))
	}

	conn, err := client.Open(ctx, types.MakeYdbConnectionString(tb.YdbConnectionParams))
	if err != nil {
		xlog.Error(ctx, "error initializing client db driver", zap.Error(err))
		return operation, nil
	}

	defer func() { _ = client.Close(ctx, conn) }()

	//lookup YdbServerOperationStatus
	opInfo, err := client.GetOperationStatus(ctx, conn, tb.YdbOperationId)
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
			if !opInfo.GetOperation().Ready {
				//if pending: return op, nil
				//if backup deadline failed: cancel operation. (skip for now)
				return operation, nil
			}
			if opInfo.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				//upsert into operations (id, status) values (id, done)?
				//db.StartUpdate()
				//.WithUpdateBackup()
				//.WithYUpdateOperation()
				err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateAvailable)
				if err != nil {
					xlog.Error(ctx, "error updating backup table", zap.Error(err))
					return operation, nil
				}
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Success")
			} else {
				//op.State = Error
				//upsert into operations (id, status, message) values (id, error, message)?
				err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateError)
				if err != nil {
					xlog.Error(ctx, "error updating backup table", zap.Error(err))
					return operation, nil
				}
				if opInfo.GetOperation().Status == Ydb.StatusIds_CANCELLED {
					operation.SetMessage("got CANCELLED status for PENDING operation")
				} else {
					operation.SetMessage(types.IssuesToString(opInfo.GetOperation().Issues))
				}
				operation.SetState(types.OperationStateError)
			}

			response, err := client.ForgetOperation(ctx, conn, tb.YdbOperationId)
			if err != nil {
				xlog.Error(ctx, err.Error())
			}

			if response != nil && response.GetStatus() != Ydb.StatusIds_SUCCESS {
				xlog.Error(ctx, "error forgetting operation", zap.Any("issues", response.GetIssues()))
			}
		}
	case types.OperationStateCancelling:
		{
			if !opInfo.GetOperation().Ready {
				//can this hang in cancelling state?
				return operation, nil
			}
			if opInfo.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				//upsert into operations (id, status, message) values (id, cancelled)?
				err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateCancelled)
				if err != nil {
					xlog.Error(ctx, "error updating backup table", zap.Error(err))
					return operation, nil
				}
				operation.SetState(types.OperationStateCancelled)
				operation.SetMessage("Success")
			} else {
				//upsert into operations (id, status, message) values (id, error, error.message)?
				err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateError)
				if err != nil {
					xlog.Error(ctx, "error updating backup table", zap.Error(err))
					return operation, nil
				}
				operation.SetState(types.OperationStateError)
				operation.SetMessage(types.IssuesToString(opInfo.GetOperation().Issues))
			}

			response, err := client.ForgetOperation(ctx, conn, tb.YdbOperationId)
			if err != nil {
				xlog.Error(ctx, err.Error())
			}

			if response != nil && response.GetStatus() != Ydb.StatusIds_SUCCESS {
				xlog.Error(ctx, "error forgetting operation", zap.Any("issues", response.GetIssues()))
			}
		}
	}
	return operation, nil
}
