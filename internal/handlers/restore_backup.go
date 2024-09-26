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
	"google.golang.org/protobuf/types/known/timestamppb"
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
	xlog.Info(ctx, "RBOperationHandler", zap.String("OperationMessage", operation.GetMessage()))

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

	prevState := operation.GetState()
	ydbOpResponse, err := lookupYdbOperationStatus(
		ctx, client, conn, operation, mr.YdbOperationId, mr.Audit.CreatedAt, config,
	)
	if err != nil {
		return err
	}
	if ydbOpResponse.shouldAbortHandler {
		operation.SetState(ydbOpResponse.opState)
		operation.SetMessage(ydbOpResponse.opMessage)
		operation.GetAudit().CompletedAt = timestamppb.Now()
		return db.UpdateOperation(ctx, operation, prevState)
	}

	if ydbOpResponse.opResponse == nil {
		return nil
	}
	opResponse := ydbOpResponse.opResponse

	switch mr.State {
	case types.OperationStateRunning:
		{
			if !opResponse.GetOperation().Ready {
				if deadlineExceeded(mr.Audit.CreatedAt, config) {
					operation.SetState(types.OperationStateStartCancelling)
					operation.SetMessage("Operation deadline exceeded")
				}

				return db.UpdateOperation(ctx, operation, prevState)
			}
			if opResponse.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Success")
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				operation.SetState(types.OperationStateError)
				if opResponse.GetOperation().Issues != nil {
					operation.SetMessage(ydbOpResponse.IssueString())
				} else {
					operation.SetMessage("Running operation was cancelled")
				}
			} else {
				operation.SetState(types.OperationStateError)
				operation.SetMessage(ydbOpResponse.IssueString())
			}
		}
	case types.OperationStateStartCancelling:
		{
			err := CancelYdbOperation(ctx, client, conn, operation, mr.YdbOperationId, mr.Message)
			if err != nil {
				return err
			}

			return db.UpdateOperation(ctx, operation, prevState)
		}
	case types.OperationStateCancelling:
		{
			if !opResponse.GetOperation().Ready {
				if deadlineExceeded(mr.Audit.CreatedAt, config) {
					operation.SetState(types.OperationStateError)
					operation.SetMessage("Operation deadline exceeded")
					operation.GetAudit().CompletedAt = timestamppb.Now()
				}

				return db.UpdateOperation(ctx, operation, prevState)
			}
			if opResponse.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Operation was completed despite cancellation: " + mr.Message)
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				operation.SetState(types.OperationStateCancelled)
				operation.SetMessage(mr.Message)
			} else {
				operation.SetState(types.OperationStateError)
				operation.SetMessage(ydbOpResponse.IssueString())
			}
		}
	default:
		return fmt.Errorf("unexpected operation state %s", mr.State)
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

	operation.GetAudit().CompletedAt = timestamppb.Now()
	return db.UpdateOperation(ctx, operation, prevState)
}
