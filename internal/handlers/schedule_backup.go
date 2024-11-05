package handlers

import (
	"context"
	"errors"
	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"
	"ydbcp/internal/backup_operations"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

type BackupScheduleHandlerType func(context.Context, db.DBConnector, types.BackupSchedule) error

func NewBackupScheduleHandler(
	clientConn client.ClientConnector,
	s3 config.S3Config,
	clientConfig config.ClientConnectionConfig,
	queryBuilderFactory queries.WriteQueryBulderFactory,
	clock clockwork.Clock,
) BackupScheduleHandlerType {
	return func(ctx context.Context, driver db.DBConnector, schedule types.BackupSchedule) error {
		return BackupScheduleHandler(
			ctx, driver, schedule, clientConn, s3, clientConfig,
			queryBuilderFactory, clock,
		)
	}
}

func BackupScheduleHandler(
	ctx context.Context,
	driver db.DBConnector,
	schedule types.BackupSchedule,
	clientConn client.ClientConnector,
	s3 config.S3Config,
	clientConfig config.ClientConnectionConfig,
	queryBuilderFactory queries.WriteQueryBulderFactory,
	clock clockwork.Clock,
) error {
	if schedule.Status != types.BackupScheduleStateActive {
		xlog.Error(ctx, "backup schedule is not active", zap.String("scheduleID", schedule.ID))
		return errors.New("backup schedule is not active")
	}
	// do not handle last_backup_id status = (failed | deleted) for now, just do backups on cron.
	if schedule.NextLaunch != nil && schedule.NextLaunch.Before(clock.Now()) {

		backupRequest := &pb.MakeBackupRequest{
			ContainerId:          schedule.ContainerID,
			DatabaseName:         schedule.DatabaseName,
			DatabaseEndpoint:     schedule.DatabaseEndpoint,
			SourcePaths:          schedule.SourcePaths,
			SourcePathsToExclude: schedule.SourcePathsToExclude,
		}
		if schedule.ScheduleSettings != nil {
			backupRequest.Ttl = schedule.ScheduleSettings.Ttl
		}
		xlog.Info(
			ctx, "call MakeBackup for schedule", zap.String("scheduleID", schedule.ID),
			zap.String("backupRequest", backupRequest.String()),
		)

		b, op, err := backup_operations.MakeBackup(
			ctx, clientConn, s3, clientConfig.AllowedEndpointDomains, clientConfig.AllowInsecureEndpoint,
			backup_operations.FromGRPCRequest(backupRequest, &schedule.ID), types.OperationCreatorName, clock,
		)
		if err != nil {
			return err
		}
		err = schedule.UpdateNextLaunch(clock.Now())
		if err != nil {
			return err
		}
		return driver.ExecuteUpsert(
			ctx,
			queryBuilderFactory().WithCreateBackup(*b).WithCreateOperation(op).WithUpdateBackupSchedule(schedule),
		)
	}
	return nil
}
