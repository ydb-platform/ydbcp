package handlers

import (
	"context"
	"errors"
	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

type BackupScheduleHandlerType func(context.Context, db.DBConnector, types.BackupSchedule) error

func NewBackupScheduleHandler(
	queryBuilderFactory queries.WriteQueryBuilderFactory,
	clock clockwork.Clock,
	mon metrics.MetricsRegistry,
) BackupScheduleHandlerType {
	return func(ctx context.Context, driver db.DBConnector, schedule types.BackupSchedule) error {
		return BackupScheduleHandler(
			ctx, driver, schedule,
			queryBuilderFactory, clock, mon,
		)
	}
}

func BackupScheduleHandler(
	ctx context.Context,
	driver db.DBConnector,
	schedule types.BackupSchedule,
	queryBuilderFactory queries.WriteQueryBuilderFactory,
	clock clockwork.Clock,
	mon metrics.MetricsRegistry,
) error {
	if schedule.Status != types.BackupScheduleStateActive {
		xlog.Error(ctx, "backup schedule is not active", zap.String("scheduleID", schedule.ID))
		return errors.New("backup schedule is not active")
	}
	// do not handle last_backup_id status = (failed | deleted) for now, just do backups on cron.
	if schedule.NextLaunch != nil && schedule.NextLaunch.Before(clock.Now()) {
		backoff, err := schedule.GetCronDuration()
		if err != nil {
			return err
		}
		now := timestamppb.New(clock.Now())
		schedule.ScheduleSettings.Ttl.AsDuration()
		tbwr := &types.TakeBackupWithRetryOperation{
			TakeBackupOperation: types.TakeBackupOperation{
				ID:          types.GenerateObjectID(),
				ContainerID: schedule.ContainerID,
				State:       types.OperationStateRunning,
				YdbConnectionParams: types.YdbConnectionParams{
					Endpoint:     schedule.DatabaseEndpoint,
					DatabaseName: schedule.DatabaseName,
				},
				SourcePaths:          schedule.SourcePaths,
				SourcePathsToExclude: schedule.SourcePathsToExclude,
				Audit: &pb.AuditInfo{
					Creator:   types.OperationCreatorName,
					CreatedAt: now,
				},
				UpdatedAt: now,
			},
			ScheduleID: &schedule.ID,
			RetryConfig: &pb.RetryConfig{
				Retries: &pb.RetryConfig_MaxBackoff{MaxBackoff: durationpb.New(backoff)},
			},
		}
		if schedule.ScheduleSettings != nil {
			if schedule.ScheduleSettings.Ttl != nil {
				d := schedule.ScheduleSettings.Ttl.AsDuration()
				tbwr.Ttl = &d
			}
		}

		xlog.Info(
			ctx, "create TakeBackupWithRetryOperation for schedule", zap.String("scheduleID", schedule.ID),
			zap.String("TakeBackupWithRetryOperation", tbwr.Proto().String()),
		)

		err = schedule.UpdateNextLaunch(clock.Now())
		if err != nil {
			return err
		}
		return driver.ExecuteUpsert(
			ctx,
			queryBuilderFactory().WithCreateOperation(tbwr).WithUpdateBackupSchedule(schedule),
		)
	}
	return nil
}
