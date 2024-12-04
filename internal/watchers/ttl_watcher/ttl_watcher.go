package ttl_watcher

import (
	"context"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync"
	"time"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	"ydbcp/internal/watchers"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

func NewTtlWatcher(
	ctx context.Context,
	wg *sync.WaitGroup,
	db db.DBConnector,
	queryBuilderFactory queries.WriteQueryBuilderFactory,
	options ...watchers.Option,
) *watchers.WatcherImpl {
	return watchers.NewWatcher(
		ctx,
		wg,
		func(ctx context.Context, period time.Duration) {
			TtlWatcherAction(ctx, period, db, queryBuilderFactory)
		},
		time.Minute,
		"Ttl",
		options...,
	)
}

func TtlWatcherAction(
	baseCtx context.Context,
	period time.Duration,
	db db.DBConnector,
	queryBuilderFactory queries.WriteQueryBuilderFactory,
) {
	ctx, cancel := context.WithTimeout(baseCtx, period)
	defer cancel()

	backups, err := db.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithRawQuery(queries.GetBackupsToDeleteQuery),
		),
	)

	if err != nil {
		xlog.Error(ctx, "can't select backups", zap.Error(err))
		return
	}

	for _, backup := range backups {
		if backup.ExpireAt != nil && backup.ExpireAt.Before(time.Now()) {
			now := timestamppb.Now()
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
			err := db.ExecuteUpsert(
				ctx, queryBuilderFactory().WithCreateOperation(dbOp).WithUpdateBackup(*backup),
			)

			if err != nil {
				xlog.Error(
					ctx, "can't create DeleteBackup operation", zap.String("BackupID", backup.ID), zap.Error(err),
				)
			}

			xlog.Debug(ctx, "DeleteBackup operation was created successfully", zap.String("BackupID", backup.ID))
		}
	}
}
