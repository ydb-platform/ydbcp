package handlers

import (
	"context"
	"fmt"
	"time"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/s3"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func deadlineExceeded(createdAt *timestamppb.Timestamp, config config.Config) bool {
	return time.Since(createdAt.AsTime()) > time.Duration(config.GetOperationTtlSeconds())*time.Second
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
	createdAt *timestamppb.Timestamp, config config.Config,
) (*LookupYdbOperationResponse, error) {
	xlog.Info(
		ctx, "getting operation status",
		zap.String("id", operation.GetID()),
		zap.String("type", string(operation.GetType())),
		zap.String("ydb_operation_id", ydbOperationId),
	)
	opResponse, err := client.GetOperationStatus(ctx, conn, ydbOperationId)
	if err != nil {
		xlog.Error(ctx, "GetOperationStatus error", zap.Error(err))
		if deadlineExceeded(createdAt, config) {
			return &LookupYdbOperationResponse{
				shouldAbortHandler: true,
				opState:            types.OperationStateError,
				opMessage:          "Operation deadline exceeded",
			}, nil
		}

		return nil, fmt.Errorf(
			"failed to get operation status for operation #%s, import operation id %s: %w",
			operation.GetID(), ydbOperationId, err,
		)
	}

	if isRetriableStatus(opResponse.GetOperation().GetStatus()) {
		xlog.Info(
			ctx, "received retriable error",
			zap.String("id", operation.GetID()),
			zap.String("type", string(operation.GetType())),
			zap.String("ydb_operation_id", ydbOperationId),
		)
		return &LookupYdbOperationResponse{}, nil
	}

	if !isValidStatus(opResponse.GetOperation().GetStatus()) {
		xlog.Info(
			ctx, "received error status",
			zap.String("id", operation.GetID()),
			zap.String("type", string(operation.GetType())),
			zap.String("ydb_operation_id", ydbOperationId),
			zap.String("operation_status", string(opResponse.GetOperation().GetStatus())),
		)
		return &LookupYdbOperationResponse{
			opResponse:         opResponse,
			shouldAbortHandler: true,
			opState:            types.OperationStateError,
			opMessage: fmt.Sprintf(
				"Error status: %s, issues: %s",
				opResponse.GetOperation().GetStatus(),
				types.IssuesToString(opResponse.GetOperation().Issues),
			),
		}, nil
	}

	xlog.Info(ctx, "got operation status from server",
		zap.String("id", operation.GetID()),
		zap.String("type", string(operation.GetType())),
		zap.String("ydb_operation_id", ydbOperationId),
		zap.String("operation_status", string(opResponse.GetOperation().GetStatus())),
	)
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
		zap.String("id", operation.GetID()),
		zap.String("type", string(operation.GetType())),
		zap.String("ydb_operation_id", ydbOperationId),
	)

	response, err := client.CancelOperation(ctx, conn, ydbOperationId)
	if err != nil {
		return fmt.Errorf(
			"error cancelling operation #%s, import operation id %s: %w",
			operation.GetID(),
			ydbOperationId,
			err,
		)
	}

	if response == nil || response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return fmt.Errorf(
			"error cancelling operation id %s, import operation id %s, issues: %s",
			operation.GetID(),
			ydbOperationId,
			types.IssuesToString(response.GetIssues()),
		)
	}

	operation.SetState(types.OperationStateCancelling)
	operation.SetMessage(reason)
	operation.GetAudit().CompletedAt = timestamppb.Now()
	return nil
}

func DeleteBackupData(s3 s3.S3Connector, s3PathPrefix string, s3Bucket string) (int64, error) {
	objects, size, err := s3.ListObjects(s3PathPrefix, s3Bucket)
	if err != nil {
		return 0, fmt.Errorf("failed to list S3 objects: %v", err)
	}

	if len(objects) != 0 {
		err = s3.DeleteObjects(objects, s3Bucket)
		if err != nil {
			return 0, fmt.Errorf("failed to delete S3 objects: %v", err)
		}
	}

	return size, nil
}
