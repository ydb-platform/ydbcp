package handlers

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"
	"time"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
)

func deadlineExceeded(createdAt time.Time, config config.Config) bool {
	return time.Since(createdAt) > time.Duration(config.OperationTtlSeconds)*time.Second
}
func isValidStatus(status Ydb.StatusIds_StatusCode) bool {
	return status == Ydb.StatusIds_SUCCESS || status == Ydb.StatusIds_CANCELLED
}

func isRetriableStatus(status Ydb.StatusIds_StatusCode) bool {
	return status == Ydb.StatusIds_OVERLOADED || status == Ydb.StatusIds_UNAVAILABLE
}

type LookupYdbOperationResponse struct {
	opResponse         *Ydb_Operations.GetOperationResponse
	shouldAbortHandler bool

	opState   types.OperationState
	opMessage string
}

func (r *LookupYdbOperationResponse) IssueString() string {
	return types.IssuesToString(r.opResponse.GetOperation().Issues)
}

func lookupYdbOperationStatus(
	ctx context.Context, client client.ClientConnector, conn *ydb.Driver, operation types.Operation,
	ydbOperationId string,
	createdAt time.Time, config config.Config,
) (*LookupYdbOperationResponse, error) {
	xlog.Info(
		ctx, "getting operation status",
		zap.String("id", operation.GetId().String()),
		zap.String("type", string(operation.GetType())),
		zap.String("ydb_operation_id", ydbOperationId),
	)
	opResponse, err := client.GetOperationStatus(ctx, conn, ydbOperationId)
	if err != nil {
		if deadlineExceeded(createdAt, config) {
			return &LookupYdbOperationResponse{
				shouldAbortHandler: true,
				opState:            types.OperationStateError,
				opMessage:          "Operation deadline exceeded",
			}, nil
		}

		return nil, fmt.Errorf(
			"failed to get operation status for operation #%s, import operation id %s: %w",
			operation.GetId().String(), ydbOperationId, err,
		)
	}

	if isRetriableStatus(opResponse.GetOperation().GetStatus()) {
		xlog.Info(
			ctx, "received retriable error",
			zap.String("id", operation.GetId().String()),
			zap.String("type", string(operation.GetType())),
			zap.String("ydb_operation_id", ydbOperationId),
		)
		return &LookupYdbOperationResponse{}, nil
	}

	if !isValidStatus(opResponse.GetOperation().GetStatus()) {
		return &LookupYdbOperationResponse{
			shouldAbortHandler: true,
			opState:            types.OperationStateError,
			opMessage: fmt.Sprintf(
				"Error status: %s, issues: %s",
				opResponse.GetOperation().GetStatus(),
				types.IssuesToString(opResponse.GetOperation().Issues),
			),
		}, nil
	}

	return &LookupYdbOperationResponse{
		opResponse: opResponse,
	}, nil
}

func CancelYdbOperation(
	ctx context.Context, client client.ClientConnector, conn *ydb.Driver,
	operation types.Operation, ydbOperationId string, reason string,
) error {
	xlog.Info(
		ctx, "cancelling operation", zap.String("reason", reason),
		zap.String("id", operation.GetId().String()),
		zap.String("type", string(operation.GetType())),
		zap.String("ydb_operation_id", ydbOperationId),
	)

	response, err := client.CancelOperation(ctx, conn, ydbOperationId)
	if err != nil {
		return fmt.Errorf(
			"error cancelling operation #%s, import operation id %s: %w",
			operation.GetId().String(),
			ydbOperationId,
			err,
		)
	}

	if response == nil || response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return fmt.Errorf(
			"error cancelling operation id %s, import operation id %s, issues: %s",
			operation.GetId().String(),
			ydbOperationId,
			types.IssuesToString(response.GetIssues()),
		)
	}

	operation.SetState(types.OperationStateCancelling)
	operation.SetMessage("Operation deadline exceeded")
	return nil
}
