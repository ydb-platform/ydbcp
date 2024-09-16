package handlers

import (
	"context"
	"time"
	"ydbcp/internal/backup_operations"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

func NewBackupScheduleHandler(
	driver db.DBConnector,
	clientConn client.ClientConnector,
	s3 config.S3Config,
	allowedEndpointDomains []string,
	allowInsecureEndpoint bool,
	queryBuilderFactory queries.WriteQueryBulderFactory,
) types.BackupScheduleHandler {
	return func(ctx context.Context, schedule types.BackupSchedule) error {
		return BackupScheduleHandler(
			ctx, schedule, driver, clientConn, s3, allowedEndpointDomains, allowInsecureEndpoint, queryBuilderFactory,
		)
	}
}

func BackupScheduleHandler(
	ctx context.Context,
	schedule types.BackupSchedule,
	driver db.DBConnector,
	clientConn client.ClientConnector,
	s3 config.S3Config,
	allowedEndpointDomains []string,
	allowInsecureEndpoint bool,
	queryBuilderFactory queries.WriteQueryBulderFactory,
) error {
	if !schedule.Active { //maybe just select active schedules; will be done in processor
		return nil
	}
	now := time.Now()
	if schedule.NextLaunch != nil && schedule.NextLaunch.Before(now) {
		b, op, err := backup_operations.MakeBackup(
			ctx, clientConn, s3, allowedEndpointDomains, allowInsecureEndpoint, &pb.MakeBackupRequest{
				ContainerId:          schedule.ContainerID,
				DatabaseName:         schedule.DatabaseName,
				DatabaseEndpoint:     schedule.DatabaseEndpoint,
				SourcePaths:          schedule.SourcePaths,
				SourcePathsToExclude: schedule.SourcePathsToExclude,
			}, &schedule.ID, "YDBCP", //TODO: who to put as subject here?
		)
		if err != nil {
			return err
		}
		schedule.LastBackupID = &op.BackupID
		err = schedule.UpdateNextLaunch(now)
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
