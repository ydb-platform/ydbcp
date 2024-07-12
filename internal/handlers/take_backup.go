package handlers

import (
	"context"
	"fmt"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/types"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func MakeTBOperationHandler(db db.DBConnector, client client.ClientConnector) types.OperationHandler {
	return func(ctx context.Context, op types.Operation) error {
		return TBOperationHandler(ctx, op, db, client)
	}
}

func TBOperationHandler(
	ctx context.Context,
	operation types.Operation,
	db db.DBConnector,
	client client.ClientConnector,
) error {
	if operation.GetType() != types.OperationTypeTB {
		return fmt.Errorf("wrong operation type %s != %s", operation.GetType(), types.OperationTypeTB)
	}
	tb, ok := operation.(*types.TakeBackupOperation)
	if !ok {
		return fmt.Errorf("can't cast Operation to TakeBackupOperation %s", types.OperationToString(operation))
	}

	conn, err := client.Open(ctx, types.MakeYdbConnectionString(tb.YdbConnectionParams))
	if err != nil {
		return fmt.Errorf("error initializing client connector %w", err)
	}

	defer func() { _ = client.Close(ctx, conn) }()

	//lookup YdbServerOperationStatus
	opInfo, err := client.GetOperationStatus(ctx, conn, tb.YdbOperationId)
	if err != nil {
		//skip, write log
		//upsert message into operation?
		return fmt.Errorf(
			"failed to lookup operation status for operation id %s, export operation id %s: %w",
			tb.GetId().String(),
			tb.YdbOperationId,
			err,
		)
	}
	switch tb.State {
	case types.OperationStatePending:
		{
			if !opInfo.GetOperation().Ready {
				//if pending: return op, nil
				//if backup deadline failed: cancel operation. (skip for now)
				return nil
			}
			if opInfo.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				//upsert into operations (id, status) values (id, done)?
				//db.StartUpdate()
				//.WithUpdateBackup()
				//.WithYUpdateOperation()
				err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateAvailable)
				if err != nil {
					return fmt.Errorf(
						"error updating backup table, operation id %s: %w",
						tb.GetId().String(),
						err,
					)
				}
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Success")
			} else {
				//op.State = Error
				//upsert into operations (id, status, message) values (id, error, message)?
				err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateError)
				if err != nil {
					return fmt.Errorf(
						"error updating backup table, operation id %s: %w",
						tb.GetId().String(),
						err,
					)
				}
				if opInfo.GetOperation().Status == Ydb.StatusIds_CANCELLED {
					operation.SetMessage("got CANCELLED status for PENDING operation")
				} else {
					operation.SetMessage(types.IssuesToString(opInfo.GetOperation().Issues))
				}
				operation.SetState(types.OperationStateError)
			}
		}
	case types.OperationStateCancelling:
		{
			if !opInfo.GetOperation().Ready {
				//can this hang in cancelling state?
				return nil
			}
			if opInfo.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				//upsert into operations (id, status, message) values (id, cancelled)?
				err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateCancelled)
				if err != nil {
					return fmt.Errorf(
						"error updating backup table, operation id %s: %w",
						tb.GetId().String(),
						err,
					)
				}
				operation.SetState(types.OperationStateCancelled)
				operation.SetMessage("Success")
			} else {
				//upsert into operations (id, status, message) values (id, error, error.message)?
				err = db.UpdateBackup(ctx, tb.BackupId, types.BackupStateError)
				if err != nil {
					return fmt.Errorf(
						"error updating backup table, operation id %s: %w",
						tb.GetId().String(),
						err,
					)
				}
				operation.SetState(types.OperationStateError)
				operation.SetMessage(types.IssuesToString(opInfo.GetOperation().Issues))
			}
		}
	}
	response, err := client.ForgetOperation(ctx, conn, tb.YdbOperationId)
	if err != nil {
		return fmt.Errorf(
			"error forgetting operation id %s, export operation id %s: %w",
			tb.GetId().String(),
			tb.YdbOperationId,
			err,
		)
	}

	if response == nil || response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return fmt.Errorf(
			"error forgetting operation id %s, export operation id %s, issues: %s",
			tb.GetId().String(),
			tb.YdbOperationId,
			types.IssuesToString(response.GetIssues()),
		)
	}
	return db.UpdateOperation(ctx, operation)
}
