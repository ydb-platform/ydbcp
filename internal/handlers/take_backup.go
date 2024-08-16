package handlers

import (
	"context"
	"fmt"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func NewTBOperationHandler(
	db db.DBConnector, client client.ClientConnector, config config.Config,
	getQueryBuilder func(ctx context.Context) queries.WriteTableQuery,
) types.OperationHandler {
	return func(ctx context.Context, op types.Operation) error {
		return TBOperationHandler(ctx, op, db, client, config, getQueryBuilder)
	}
}

func TBOperationHandler(
	ctx context.Context,
	operation types.Operation,
	db db.DBConnector,
	client client.ClientConnector,
	config config.Config,
	getQueryBuilder func(ctx context.Context) queries.WriteTableQuery,
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

	ydbOpResponse, err := lookupYdbOperationStatus(
		ctx, client, conn, operation, tb.YdbOperationId, tb.Audit.CreatedAt, config,
	)
	if err != nil {
		return err
	}

	now := timestamppb.Now()
	backupToWrite := types.Backup{
		ID:        tb.BackupId,
		Status:    types.BackupStateUnknown,
		AuditInfo: &pb.AuditInfo{},
	}

	if ydbOpResponse.shouldAbortHandler {
		operation.SetState(ydbOpResponse.opState)
		operation.SetMessage(ydbOpResponse.opMessage)
		operation.GetAudit().CompletedAt = now
		backupToWrite.Status = types.BackupStateError
		backupToWrite.AuditInfo.CompletedAt = now
		return db.ExecuteUpsert(
			ctx, getQueryBuilder(ctx).WithUpdateOperation(operation).WithUpdateBackup(backupToWrite),
		)
	}
	if ydbOpResponse.opResponse == nil {
		return nil
	}
	opResponse := ydbOpResponse.opResponse

	switch tb.State {
	case types.OperationStatePending:
		{
			if !opResponse.GetOperation().Ready {
				if deadlineExceeded(tb.Audit.CreatedAt, config) {
					err = CancelYdbOperation(ctx, client, conn, operation, tb.YdbOperationId, "TTL")
					if err != nil {
						return err
					}
					backupToWrite.Status = types.BackupStateError
					backupToWrite.AuditInfo.CompletedAt = operation.GetAudit().CompletedAt
					return db.ExecuteUpsert(
						ctx, getQueryBuilder(ctx).WithUpdateOperation(operation).WithUpdateBackup(backupToWrite),
					)
				} else {
					return nil
				}
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				backupToWrite.Status = types.BackupStateAvailable
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Success")
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				backupToWrite.Status = types.BackupStateError
				operation.SetState(types.OperationStateError)
				operation.SetMessage("got CANCELLED status for PENDING operation")
			} else {
				backupToWrite.Status = types.BackupStateError
				operation.SetState(types.OperationStateError)
				operation.SetMessage(ydbOpResponse.IssueString())
			}
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
					return db.ExecuteUpsert(
						ctx, getQueryBuilder(ctx).WithUpdateOperation(operation).WithUpdateBackup(backupToWrite),
					)
				} else {
					return nil
				}
			}
			if opResponse.GetOperation().Status == Ydb.StatusIds_SUCCESS {
				backupToWrite.Status = types.BackupStateAvailable
				operation.SetState(types.OperationStateDone)
				operation.SetMessage("Operation was completed despite cancellation")
			} else if opResponse.GetOperation().Status == Ydb.StatusIds_CANCELLED {
				backupToWrite.Status = types.BackupStateCancelled
				operation.SetState(types.OperationStateCancelled)
				operation.SetMessage("Success")
			} else {
				backupToWrite.Status = types.BackupStateError
				operation.SetState(types.OperationStateError)
				operation.SetMessage(ydbOpResponse.IssueString())
			}
		}
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
		ctx, getQueryBuilder(ctx).WithUpdateOperation(operation).WithUpdateBackup(backupToWrite),
	)
}
