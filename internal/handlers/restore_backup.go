package handlers

import (
	"context"
	"fmt"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"
)

func NewRBOperationHandler(
	db db.DBConnector, client client.ClientConnector, config config.Config,
) types.OperationHandler {
	return func(ctx context.Context, op types.Operation) error {
		return RBOperationHandler(ctx, op, db, client, config)
	}
}

func RBOperationHandler(
	ctx context.Context,
	operation types.Operation,
	db db.DBConnector,
	client client.ClientConnector,
	config config.Config,
) error {
	xlog.Info(
		ctx, "received operation",
		zap.String("id", operation.GetID()),
		zap.String("type", string(operation.GetType())),
		zap.String("state", string(operation.GetState())),
		zap.String("message", operation.GetMessage()),
	)

	if operation.GetType() != types.OperationTypeRB {
		return fmt.Errorf(
			"wrong type %s != %s for operation %s",
			operation.GetType(), types.OperationTypeRB, types.OperationToString(operation),
		)
	}

	mr, ok := operation.(*types.RestoreBackupOperation)
	if !ok {
		return fmt.Errorf("can't cast operation to RestoreBackupOperation %s", types.OperationToString(operation))
	}

	conn, err := client.Open(ctx, types.MakeYdbConnectionString(mr.YdbConnectionParams))
	if err != nil {
		return fmt.Errorf(
			"error initializing client connector for operation #%s: %w",
			mr.GetID(), err,
		)
	}

	defer func() { _ = client.Close(ctx, conn) }()

	ydbOpResponse, err := lookupYdbOperationStatus(
		ctx, client, conn, operation, mr.YdbOperationId, mr.CreatedAt, config,
	)
	if err != nil {
		return err
	}
	if ydbOpResponse.shouldAbortHandler {
		operation.SetState(ydbOpResponse.opState)
		operation.SetMessage(ydbOpResponse.opMessage)
		return db.UpdateOperation(ctx, operation)
	}

	if ydbOpResponse.opResponse == nil {
		return nil
	}
	opResponse := ydbOpResponse.opResponse

	switch mr.State {
	case types.OperationStatePending:
		{
			if !opResponse.GetOperation().Ready {
				if deadlineExceeded(mr.CreatedAt, config) {
					err = CancelYdbOperation(ctx, client, conn, operation, mr.YdbOperationId, "TTL")
					if err != nil {
						return err
					}
					return db.UpdateOperation(ctx, operation)
				}
				return nil
			}
			if opResponse.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Success")
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				operation.SetState(types.OperationStateError)
				operation.SetMessage("Pending operation was cancelled")
			}
		}
	case types.OperationStateCancelling:
		{
			if !opResponse.GetOperation().Ready {
				if deadlineExceeded(mr.CreatedAt, config) {
					operation.SetState(types.OperationStateError)
					operation.SetMessage("Operation deadline exceeded")
					return db.UpdateOperation(ctx, operation)
				}

				return nil
			}
			if opResponse.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Operation was completed despite cancellation")
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				operation.SetState(types.OperationStateCancelled)
				operation.SetMessage("Success")
			}
		}
	}

	xlog.Info(
		ctx, "forgetting operation",
		zap.String("id", mr.ID),
		zap.String("type", string(operation.GetType())),
		zap.String("ydb_operation_id", mr.YdbOperationId),
	)

	response, err := client.ForgetOperation(ctx, conn, mr.YdbOperationId)
	if err != nil {
		return fmt.Errorf(
			"error forgetting operation #%s, import operation id %s: %w",
			mr.GetID(),
			mr.YdbOperationId,
			err,
		)
	}

	if response == nil || response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return fmt.Errorf(
			"error forgetting operation #%s, import operation id %s, issues: %s",
			mr.GetID(),
			mr.YdbOperationId,
			types.IssuesToString(response.GetIssues()),
		)
	}

	return db.UpdateOperation(ctx, operation)
}
