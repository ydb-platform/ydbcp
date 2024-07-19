package handlers

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"
	"time"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
)

func MakeRBOperationHandler(db db.DBConnector, client client.ClientConnector, config config.Config) types.OperationHandler {
	return func(ctx context.Context, op types.Operation) error {
		return RBOperationHandler(ctx, op, db, client, config)
	}
}

func valid(status Ydb.StatusIds_StatusCode) bool {
	return status == Ydb.StatusIds_SUCCESS || status == Ydb.StatusIds_CANCELLED
}

func retriable(status Ydb.StatusIds_StatusCode) bool {
	return status == Ydb.StatusIds_OVERLOADED || status == Ydb.StatusIds_UNAVAILABLE
}

func RBOperationHandler(
	ctx context.Context,
	operation types.Operation,
	db db.DBConnector,
	client client.ClientConnector,
	config config.Config,
) error {
	xlog.Info(ctx, "received operation",
		zap.String("id", operation.GetId().String()),
		zap.String("type", string(operation.GetType())),
		zap.String("state", string(operation.GetState())),
		zap.String("message", operation.GetMessage()),
	)

	if operation.GetType() != types.OperationTypeRB {
		return fmt.Errorf("wrong type %s != %s for operation %s",
			operation.GetType(), types.OperationTypeRB, types.OperationToString(operation),
		)
	}

	mr, ok := operation.(*types.RestoreBackupOperation)
	if !ok {
		return fmt.Errorf("can't cast operation to RestoreBackupOperation %s", types.OperationToString(operation))
	}

	conn, err := client.Open(ctx, types.MakeYdbConnectionString(mr.YdbConnectionParams))
	if err != nil {
		return fmt.Errorf("error initializing client connector for operation #%s: %w",
			mr.GetId().String(), err,
		)
	}

	defer func() { _ = client.Close(ctx, conn) }()

	xlog.Info(ctx, "getting operation status",
		zap.String("id", mr.Id.String()),
		zap.String("type", string(operation.GetType())),
		zap.String("ydb_operation_id", mr.YdbOperationId),
	)

	opResponse, err := client.GetOperationStatus(ctx, conn, mr.YdbOperationId)
	if err != nil {
		if (mr.CreatedAt.Unix() + config.OperationTtlSeconds) <= time.Now().Unix() {
			operation.SetState(types.OperationStateError)
			operation.SetMessage("Operation deadline exceeded")
			return db.UpdateOperation(ctx, operation)
		}

		return fmt.Errorf(
			"failed to get operation status for operation #%s, import operation id %s: %w",
			mr.GetId().String(),
			mr.YdbOperationId,
			err,
		)
	}

	if retriable(opResponse.GetOperation().GetStatus()) {
		xlog.Info(ctx, "received retriable error",
			zap.String("id", mr.Id.String()),
			zap.String("type", string(operation.GetType())),
			zap.String("ydb_operation_id", mr.YdbOperationId),
		)

		return nil
	}

	if !valid(opResponse.GetOperation().GetStatus()) {
		operation.SetState(types.OperationStateError)
		operation.SetMessage(fmt.Sprintf("Error status: %s, issues: %s",
			opResponse.GetOperation().GetStatus(),
			types.IssuesToString(opResponse.GetOperation().Issues)),
		)
		return db.UpdateOperation(ctx, operation)
	}

	switch mr.State {
	case types.OperationStatePending:
		{
			if !opResponse.GetOperation().Ready {
				if (mr.CreatedAt.Unix() + config.OperationTtlSeconds) <= time.Now().Unix() {
					xlog.Info(ctx, "cancelling operation due to ttl",
						zap.String("id", mr.Id.String()),
						zap.String("type", string(operation.GetType())),
						zap.String("ydb_operation_id", mr.YdbOperationId),
					)

					response, err := client.CancelOperation(ctx, conn, mr.YdbOperationId)
					if err != nil {
						return fmt.Errorf(
							"error cancelling operation #%s, import operation id %s: %w",
							mr.GetId().String(),
							mr.YdbOperationId,
							err,
						)
					}

					if response == nil || response.GetStatus() != Ydb.StatusIds_SUCCESS {
						return fmt.Errorf(
							"error cancelling operation id %s, import operation id %s, issues: %s",
							mr.GetId().String(),
							mr.YdbOperationId,
							types.IssuesToString(response.GetIssues()),
						)
					}

					operation.SetState(types.OperationStateCancelling)
					operation.SetMessage("Operation deadline exceeded")
					return db.UpdateOperation(ctx, operation)
				}

				return nil
			}

			if opResponse.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Success")
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				operation.SetState(types.OperationStateCancelled)
				operation.SetMessage("Pending operation wac cancelled")
			}
		}
	case types.OperationStateCancelling:
		{
			if !opResponse.GetOperation().Ready {
				if (mr.CreatedAt.Unix() + config.OperationTtlSeconds) <= time.Now().Unix() {
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

	xlog.Info(ctx, "forgetting operation",
		zap.String("id", mr.Id.String()),
		zap.String("type", string(operation.GetType())),
		zap.String("ydb_operation_id", mr.YdbOperationId),
	)

	response, err := client.ForgetOperation(ctx, conn, mr.YdbOperationId)
	if err != nil {
		return fmt.Errorf(
			"error forgetting operation #%s, import operation id %s: %w",
			mr.GetId().String(),
			mr.YdbOperationId,
			err,
		)
	}

	if response == nil || response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return fmt.Errorf(
			"error forgetting operation #%s, import operation id %s, issues: %s",
			mr.GetId().String(),
			mr.YdbOperationId,
			types.IssuesToString(response.GetIssues()),
		)
	}

	return db.UpdateOperation(ctx, operation)
}
