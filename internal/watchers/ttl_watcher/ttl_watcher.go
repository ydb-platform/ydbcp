package ttl_watcher

import (
	"context"
	"sync"
	"time"

	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ydbcp/internal/backup_operations"
	dbconnector "ydbcp/internal/connectors/db"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	"ydbcp/internal/watchers"
)

func NewTtlWatcher(
	ctx context.Context,
	wg *sync.WaitGroup,
	db dbconnector.DBConnector,
	options ...watchers.Option,
) *watchers.WatcherImpl {
	return watchers.NewWatcher(
		ctx,
		wg,
		func(ctx context.Context, period time.Duration) {
			TtlWatcherAction(ctx, period, db)
		},
		time.Minute,
		"Ttl",
		options...,
	)
}

func TtlWatcherAction(
	baseCtx context.Context,
	period time.Duration,
	db dbconnector.DBConnector,
) {
	ctx, cancel := context.WithTimeout(baseCtx, period)
	defer cancel()

	backups, err := db.ListExpiredBackups(ctx, 100)

	if err != nil {
		xlog.Error(ctx, "can't select backups", zap.Error(err))
		return
	}

	for _, backup := range backups {
		backupCtx := backup.SetLogFields(ctx)
		if backup.ExpireAt != nil && backup.ExpireAt.Before(time.Now()) {
			now := timestamppb.Now()
			if backup_operations.IsEmptyBackup(backup) {
				backup.Status = types.BackupStateDeleted
				err = db.Apply(
					backupCtx, dbconnector.Changes{UpdateBackups: []types.Backup{*backup}},
				)
				if err != nil {
					xlog.Error(
						backupCtx, "can't update backup status", zap.Error(err),
					)
				}
				xlog.Debug(backupCtx, "Marked empty backup as deleted")
			} else {
				dbOp := &types.DeleteBackupOperation{
					ID:          types.GenerateObjectID(),
					ContainerID: backup.ContainerID,
					BackupID:    backup.ID,
					State:       types.OperationStatePending,
					YdbConnectionParams: types.YdbConnectionParams{
						DatabaseName: backup.DatabaseName,
						Endpoint:     backup.DatabaseEndpoint,
					},
					Audit: &pb.AuditInfo{
						CreatedAt: now,
						Creator:   types.OperationCreatorName,
					},
					PathPrefix: backup.S3PathPrefix,
					UpdatedAt:  now,
				}

				backup.Status = types.BackupStateDeleting
				err := db.Apply(
					backupCtx, dbconnector.Changes{CreateOperations: []types.Operation{dbOp}, UpdateBackups: []types.Backup{*backup}},
				)

				if err != nil {
					xlog.Error(
						backupCtx, "can't create DeleteBackup operation", zap.Error(err),
					)
				}

				xlog.Debug(backupCtx, "DeleteBackup operation was created successfully")
			}
		}
	}
}
