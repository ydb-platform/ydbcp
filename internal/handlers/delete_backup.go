package handlers

import (
	"context"
	"errors"
	"fmt"

	"ydbcp/internal/metrics"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ydbcp/internal/config"
	dbconnector "ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/s3"
	"ydbcp/internal/types"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
)

func NewDBOperationHandler(
	db dbconnector.DBConnector,
	s3 s3.S3Connector,
	config config.Config,
) types.OperationHandler {
	return func(ctx context.Context, op types.Operation) error {
		err := DBOperationHandler(ctx, op, db, s3, config)
		if err == nil {
			metrics.GlobalMetricsRegistry.ReportOperationMetrics(op)
		}
		return err
	}
}

func DBOperationHandler(
	ctx context.Context,
	operation types.Operation,
	db dbconnector.DBConnector,
	s3 s3.S3Connector,
	config config.Config,
) error {
	xlog.Info(ctx, "DBOperationHandler", zap.String(log_keys.OperationMessage, operation.GetMessage()))

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

	executeUpsert := func(operation types.Operation, backup *types.Backup) error {
		var err error

		if backup != nil {
			err = db.Apply(
				ctx, dbconnector.Changes{UpdateOperations: []types.Operation{operation}, UpdateBackups: []types.Backup{*backup}},
			)
		} else {
			err = db.UpdateOperation(ctx, operation)
		}

		return err
	}

	backup, err := db.GetBackup(ctx, dbOp.BackupID)

	if err != nil && !errors.Is(err, dbconnector.ErrNotFound) {
		return fmt.Errorf("can't select backups: %v", err)
	}

	if errors.Is(err, dbconnector.ErrNotFound) {
		operation.SetState(types.OperationStateError)
		operation.SetMessage("Backup not found")
		operation.GetAudit().CompletedAt = timestamppb.Now()
		return executeUpsert(operation, nil)
	}

	if deadlineExceeded(dbOp.Audit.CreatedAt, config) {
		backup.Status = types.BackupStateError
		operation.SetState(types.OperationStateError)
		operation.SetMessage("Operation deadline exceeded")
		operation.GetAudit().CompletedAt = timestamppb.Now()
		return executeUpsert(operation, backup)
	}

	if backup.Status != types.BackupStateDeleting {
		operation.SetState(types.OperationStateError)
		operation.SetMessage(fmt.Sprintf("Unexpected backup status: %s", backup.Status))
		operation.GetAudit().CompletedAt = timestamppb.Now()
		return executeUpsert(operation, nil)
	}

	deleteBackup := func(pathPrefix string, bucket string) error {
		size, err := DeleteBackupData(s3, pathPrefix, bucket)
		if err != nil {
			return fmt.Errorf("failed to delete backup data: %v", err)
		}

		metrics.GlobalMetricsRegistry.IncBytesDeletedCounter(backup.ContainerID, backup.S3Bucket, backup.DatabaseName, size)

		backup.Status = types.BackupStateDeleted
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

	return executeUpsert(operation, backup)
}
