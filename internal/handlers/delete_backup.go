package handlers

import (
	"context"
	"fmt"
	"ydbcp/internal/metrics"

	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/connectors/s3"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"

	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewDBOperationHandler(
	db db.DBConnector,
	s3 s3.S3Connector,
	config config.Config,
	queryBuilderFactory queries.WriteQueryBuilderFactory,
	mon metrics.MetricsRegistry,
) types.OperationHandler {
	return func(ctx context.Context, op types.Operation) error {
		return DBOperationHandler(ctx, op, db, s3, config, queryBuilderFactory, mon)
	}
}

func DBOperationHandler(
	ctx context.Context,
	operation types.Operation,
	db db.DBConnector,
	s3 s3.S3Connector,
	config config.Config,
	queryBuilderFactory queries.WriteQueryBuilderFactory,
	mon metrics.MetricsRegistry,
) error {
	xlog.Info(ctx, "DBOperationHandler", zap.String("OperationMessage", operation.GetMessage()))

	if operation.GetType() != types.OperationTypeDB {
		return fmt.Errorf(
			"wrong type %s != %s for operation %s",
			operation.GetType(), types.OperationTypeDB, types.OperationToString(operation),
		)
	}

	dbOp, ok := operation.(*types.DeleteBackupOperation)
	if !ok {
		return fmt.Errorf("can't cast operation to DeleteBackupOperation %s", types.OperationToString(operation))
	}

	backupToWrite := types.Backup{
		ID:     dbOp.BackupID,
		Status: types.BackupStateUnknown,
	}

	upsertAndReportMetrics := func(operation types.Operation, backup *types.Backup) error {
		var err error

		if backup != nil {
			err = db.ExecuteUpsert(
				ctx, queryBuilderFactory().WithUpdateOperation(operation).WithUpdateBackup(*backup),
			)
		} else {
			err = db.UpdateOperation(ctx, operation)
		}

		if err == nil {
			mon.ObserveOperationDuration(operation)
		}

		return err
	}

	if deadlineExceeded(dbOp.Audit.CreatedAt, config) {
		backupToWrite.Status = types.BackupStateError
		operation.SetState(types.OperationStateError)
		operation.SetMessage("Operation deadline exceeded")
		operation.GetAudit().CompletedAt = timestamppb.Now()
		return upsertAndReportMetrics(operation, &backupToWrite)
	}

	backups, err := db.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.StringValueFromString(dbOp.BackupID)},
				},
			),
		),
	)

	if err != nil {
		return fmt.Errorf("can't select backups: %v", err)
	}

	if len(backups) == 0 {
		operation.SetState(types.OperationStateError)
		operation.SetMessage("Backup not found")
		operation.GetAudit().CompletedAt = timestamppb.Now()
		return upsertAndReportMetrics(operation, nil)
	}

	backup := backups[0]
	if backup.Status != types.BackupStateDeleting {
		operation.SetState(types.OperationStateError)
		operation.SetMessage(fmt.Sprintf("Unexpected backup status: %s", backup.Status))
		operation.GetAudit().CompletedAt = timestamppb.Now()
		return upsertAndReportMetrics(operation, nil)
	}

	deleteBackup := func(pathPrefix string, bucket string) error {
		size, err := DeleteBackupData(s3, pathPrefix, bucket)
		if err != nil {
			return fmt.Errorf("failed to delete backup data: %v", err)
		}

		mon.IncBytesDeletedCounter(backup.ContainerID, backup.S3Bucket, backup.DatabaseName, size)

		backupToWrite.Status = types.BackupStateDeleted
		operation.SetState(types.OperationStateDone)
		operation.SetMessage("Success")
		operation.GetAudit().CompletedAt = timestamppb.Now()
		return nil
	}

	switch dbOp.State {
	case types.OperationStatePending:
		{
			operation.SetState(types.OperationStateRunning)
			err := db.UpdateOperation(ctx, operation)
			if err != nil {
				return fmt.Errorf("can't update operation: %v", err)
			}

			err = deleteBackup(backup.S3PathPrefix, backup.S3Bucket)
			if err != nil {
				return err
			}
		}
	case types.OperationStateRunning:
		{
			err = deleteBackup(backup.S3PathPrefix, backup.S3Bucket)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected operation state %s", dbOp.State)
	}

	return upsertAndReportMetrics(operation, &backupToWrite)
}
