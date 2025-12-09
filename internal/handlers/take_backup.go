package handlers

import (
	"context"
	"fmt"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/connectors/s3"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewTBOperationHandler(
	db db.DBConnector, client client.ClientConnector, s3 s3.S3Connector, config config.Config,
	queryBuilderFactory queries.WriteQueryBuilderFactory,
) types.OperationHandler {
	return func(ctx context.Context, op types.Operation) error {
		err := TBOperationHandler(ctx, op, db, client, s3, config, queryBuilderFactory)
		if err == nil {
			metrics.GlobalMetricsRegistry.ReportOperationMetrics(op)
		}
		return err
	}
}

func TBOperationHandler(
	ctx context.Context,
	operation types.Operation,
	db db.DBConnector,
	client client.ClientConnector,
	s3 s3.S3Connector,
	config config.Config,
	queryBuilderFactory queries.WriteQueryBuilderFactory,
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

	upsertAndReportMetrics := func(
		operation types.Operation,
		backup types.Backup,
		containerId string,
		database string,
		status Ydb.StatusIds_StatusCode,
	) error {
		err := db.ExecuteUpsert(
			ctx, queryBuilderFactory().WithUpdateOperation(operation).WithUpdateBackup(backup),
		)

		if err == nil {
			metrics.GlobalMetricsRegistry.IncCompletedBackupsCount(containerId, database, backup.ScheduleID, status, backup.EncryptionSettings != nil)
		}

		return err
	}

	backups, err := db.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.StringValueFromString(tb.BackupID)},
				},
			),
		),
	)

	if err != nil {
		return fmt.Errorf("can't select backups: %v", err)
	}

	if len(backups) == 0 {
		return fmt.Errorf("backup not found: %s", tb.BackupID)
	}

	backup := backups[0]

	ydbOpResponse, err := lookupYdbOperationStatus(
		ctx, client, conn, operation, tb.YdbOperationId, tb.Audit.CreatedAt, config,
	)
	if err != nil {
		return err
	}

	now := timestamppb.Now()
	if ydbOpResponse.shouldAbortHandler {
		operation.SetState(ydbOpResponse.opState)
		operation.SetMessage(ydbOpResponse.opMessage)
		operation.GetAudit().CompletedAt = now
		backup.Status = types.BackupStateError
		backup.Message = operation.GetMessage()
		backup.SetCompletedAt(now)

		if ydbOpResponse.opResponse != nil {
			return upsertAndReportMetrics(
				operation,
				*backup,
				backup.ContainerID,
				backup.DatabaseName,
				ydbOpResponse.opResponse.GetOperation().Status,
			)
		}

		return upsertAndReportMetrics(
			operation,
			*backup,
			backup.ContainerID,
			backup.DatabaseName,
			Ydb.StatusIds_TIMEOUT,
		)
	}
	if ydbOpResponse.opResponse == nil {
		return nil
	}
	opResponse := ydbOpResponse.opResponse

	getBackupSize := func(s3PathPrefix string, s3Bucket string) (int64, error) {
		size, err := s3.GetSize(s3PathPrefix, s3Bucket)
		if err != nil {
			return 0, fmt.Errorf("can't get size of objects by path: %s", s3PathPrefix)
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
				return db.UpdateOperation(ctx, operation)
			}

			size, err := getBackupSize(backup.S3PathPrefix, backup.S3Bucket)
			if err != nil {
				return err
			}

			if opResponse.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				backup.Status = types.BackupStateAvailable
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Success")
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				backup.Status = types.BackupStateError
				operation.SetState(types.OperationStateError)
				if opResponse.GetOperation().Issues != nil {
					operation.SetMessage(ydbOpResponse.IssueString())
				} else {
					operation.SetMessage("got CANCELLED status for running operation")
				}
			} else {
				backup.Status = types.BackupStateError
				operation.SetState(types.OperationStateError)
				operation.SetMessage(ydbOpResponse.IssueString())
			}
			backup.Message = operation.GetMessage()
			backup.Size = size
			metrics.GlobalMetricsRegistry.IncBytesWrittenCounter(backup.ContainerID, backup.DatabaseName, backup.S3Bucket, size)
		}
	case types.OperationStateStartCancelling:
		{
			err = CancelYdbOperation(ctx, client, conn, operation, tb.YdbOperationId, operation.GetMessage())
			if err != nil {
				return err
			}
			backup.Status = types.BackupStateError
			backup.Message = operation.GetMessage()
			backup.SetCompletedAt(operation.GetAudit().CompletedAt)
			return db.ExecuteUpsert(
				ctx, queryBuilderFactory().WithUpdateOperation(operation).WithUpdateBackup(*backup),
			)
		}
	case types.OperationStateCancelling:
		{
			if !opResponse.GetOperation().Ready {
				if deadlineExceeded(tb.Audit.CreatedAt, config) {
					backup.Status = types.BackupStateError
					backup.SetCompletedAt(now)
					operation.SetState(types.OperationStateError)
					operation.SetMessage("Operation deadline exceeded")
					operation.GetAudit().CompletedAt = now
					backup.Message = operation.GetMessage()
					return upsertAndReportMetrics(
						operation,
						*backup,
						backup.ContainerID,
						backup.DatabaseName,
						Ydb.StatusIds_TIMEOUT,
					)
				}

				return db.UpdateOperation(ctx, operation)
			}

			size, err := getBackupSize(backup.S3PathPrefix, backup.S3Bucket)
			if err != nil {
				return err
			}

			if opResponse.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				backup.Status = types.BackupStateAvailable
				backup.Size = size
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Operation was completed despite cancellation: " + tb.Message)
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				size, err = DeleteBackupData(s3, backup.S3PathPrefix, backup.S3Bucket)
				if err != nil {
					return err
				}

				metrics.GlobalMetricsRegistry.IncBytesDeletedCounter(backup.ContainerID, backup.S3Bucket, backup.DatabaseName, size)

				backup.Status = types.BackupStateCancelled
				operation.SetState(types.OperationStateCancelled)
				operation.SetMessage(tb.Message)
			} else {
				backup.Status = types.BackupStateError
				backup.Size = size
				operation.SetState(types.OperationStateError)
				operation.SetMessage(ydbOpResponse.IssueString())
			}
			backup.Message = operation.GetMessage()
			metrics.GlobalMetricsRegistry.IncBytesWrittenCounter(backup.ContainerID, backup.DatabaseName, backup.S3Bucket, size)
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
	backup.SetCompletedAt(now)
	operation.GetAudit().CompletedAt = now
	return upsertAndReportMetrics(
		operation,
		*backup,
		backup.ContainerID,
		backup.DatabaseName,
		opResponse.GetOperation().Status,
	)
}
