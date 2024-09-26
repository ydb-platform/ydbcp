package handlers

import (
	"context"
	"fmt"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/connectors/s3"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewTBOperationHandler(
	db db.DBConnector, client client.ClientConnector, s3 s3.S3Connector, config config.Config,
	queryBulderFactory queries.WriteQueryBulderFactory,
) types.OperationHandler {
	return func(ctx context.Context, op types.Operation) error {
		return TBOperationHandler(ctx, op, db, client, s3, config, queryBulderFactory)
	}
}

func TBOperationHandler(
	ctx context.Context,
	operation types.Operation,
	db db.DBConnector,
	client client.ClientConnector,
	s3 s3.S3Connector,
	config config.Config,
	queryBuilderFactory queries.WriteQueryBulderFactory,
) error {
	xlog.Info(ctx, "TBOperationHandler", zap.String("OperationMessage", operation.GetMessage()))

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

	prevState := operation.GetState()
	ydbOpResponse, err := lookupYdbOperationStatus(
		ctx, client, conn, operation, tb.YdbOperationId, tb.Audit.CreatedAt, config,
	)
	if err != nil {
		return err
	}

	now := timestamppb.Now()
	backupToWrite := types.Backup{
		ID:        tb.BackupID,
		Status:    types.BackupStateUnknown,
		AuditInfo: &pb.AuditInfo{},
	}

	if ydbOpResponse.shouldAbortHandler {
		operation.SetState(ydbOpResponse.opState)
		operation.SetMessage(ydbOpResponse.opMessage)
		operation.GetAudit().CompletedAt = now
		backupToWrite.Status = types.BackupStateError
		backupToWrite.Message = operation.GetMessage()
		backupToWrite.AuditInfo.CompletedAt = now
		return db.ExecuteUpsert(
			ctx, queryBuilderFactory().WithUpdateOperation(operation, prevState).WithUpdateBackup(backupToWrite),
		)
	}
	if ydbOpResponse.opResponse == nil {
		return nil
	}
	opResponse := ydbOpResponse.opResponse

	getBackupSize := func(backupID string) (int64, error) {
		backups, err := db.SelectBackups(
			ctx, queries.NewReadTableQuery(
				queries.WithTableName("Backups"),
				queries.WithQueryFilters(
					queries.QueryFilter{
						Field:  "id",
						Values: []table_types.Value{table_types.StringValueFromString(backupID)},
					},
				),
			),
		)

		if err != nil {
			return 0, fmt.Errorf("can't select backups: %v", err)
		}

		if len(backups) == 0 {
			return 0, fmt.Errorf("backup not found: %s", backupID)
		}

		size, err := s3.GetSize(backups[0].S3PathPrefix, backups[0].S3Bucket)
		if err != nil {
			return 0, fmt.Errorf("can't get size of objects by path: %s", backups[0].S3PathPrefix)
		}

		return size, nil
	}

	switch tb.State {
	case types.OperationStateRunning:
		{
			if !opResponse.GetOperation().Ready {
				if deadlineExceeded(tb.Audit.CreatedAt, config) {
					operation.SetState(types.OperationStateStartCancelling)
					operation.SetMessage("Operation deadline exceeded")
				}
				return db.UpdateOperation(ctx, operation, prevState)
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				size, err := getBackupSize(tb.BackupID)
				if err != nil {
					return err
				}

				backupToWrite.Status = types.BackupStateAvailable
				backupToWrite.Size = size
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Success")
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				backupToWrite.Status = types.BackupStateError
				operation.SetState(types.OperationStateError)
				if opResponse.GetOperation().Issues != nil {
					operation.SetMessage(ydbOpResponse.IssueString())
				} else {
					operation.SetMessage("got CANCELLED status for running operation")
				}
			} else {
				backupToWrite.Status = types.BackupStateError
				operation.SetState(types.OperationStateError)
				operation.SetMessage(ydbOpResponse.IssueString())
			}
			backupToWrite.Message = operation.GetMessage()
		}
	case types.OperationStateStartCancelling:
		{
			err = CancelYdbOperation(ctx, client, conn, operation, tb.YdbOperationId, operation.GetMessage())
			if err != nil {
				return err
			}
			backupToWrite.Status = types.BackupStateError
			backupToWrite.Message = operation.GetMessage()
			backupToWrite.AuditInfo.CompletedAt = operation.GetAudit().CompletedAt
			return db.ExecuteUpsert(
				ctx, queryBuilderFactory().WithUpdateOperation(operation, prevState).WithUpdateBackup(backupToWrite),
			)
		}
	case types.OperationStateCancelling:
		{
			if !opResponse.GetOperation().Ready {
				if deadlineExceeded(tb.Audit.CreatedAt, config) {
					backupToWrite.Status = types.BackupStateError
					backupToWrite.AuditInfo.CompletedAt = now
					operation.SetState(types.OperationStateError)
					operation.SetMessage("Operation deadline exceeded")
					operation.GetAudit().CompletedAt = now
					backupToWrite.Message = operation.GetMessage()
					return db.ExecuteUpsert(
						ctx, queryBuilderFactory().WithUpdateOperation(operation, prevState).WithUpdateBackup(backupToWrite),
					)
				}

				return db.UpdateOperation(ctx, operation, prevState)
			}
			if opResponse.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				size, err := getBackupSize(tb.BackupID)
				if err != nil {
					return err
				}

				backupToWrite.Status = types.BackupStateAvailable
				backupToWrite.Size = size
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Operation was completed despite cancellation: " + tb.Message)
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				backupToWrite.Status = types.BackupStateCancelled
				operation.SetState(types.OperationStateCancelled)
				operation.SetMessage(tb.Message)
			} else {
				backupToWrite.Status = types.BackupStateError
				operation.SetState(types.OperationStateError)
				operation.SetMessage(ydbOpResponse.IssueString())
			}
			backupToWrite.Message = operation.GetMessage()
		}
	default:
		return fmt.Errorf("unexpected operation state %s", tb.State)
	}
	response, err := client.ForgetOperation(ctx, conn, tb.YdbOperationId)
	if err != nil {
		return fmt.Errorf(
			"error forgetting operation id %s, export operation id %s: %w",
			tb.GetID(),
			tb.YdbOperationId,
			err,
		)
	}

	if response == nil || response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return fmt.Errorf(
			"error forgetting operation id %s, export operation id %s, issues: %s",
			tb.GetID(),
			tb.YdbOperationId,
			types.IssuesToString(response.GetIssues()),
		)
	}
	backupToWrite.AuditInfo.CompletedAt = now
	operation.GetAudit().CompletedAt = now
	return db.ExecuteUpsert(
		ctx, queryBuilderFactory().WithUpdateOperation(operation, prevState).WithUpdateBackup(backupToWrite),
	)
}
