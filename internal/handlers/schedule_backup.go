package handlers

import (
	"context"
	"errors"

	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"

	"ydbcp/internal/audit"
	"ydbcp/internal/config"
	dbconnector "ydbcp/internal/connectors/db"
	"ydbcp/internal/types"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"

	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BackupScheduleHandlerType func(context.Context, dbconnector.DBConnector, *types.BackupSchedule) error

func NewBackupScheduleHandler(
	clock clockwork.Clock,
	featureFlags config.FeatureFlagsConfig,
) BackupScheduleHandlerType {
	return func(ctx context.Context, driver dbconnector.DBConnector, schedule *types.BackupSchedule) error {
		return BackupScheduleHandler(
			ctx, driver, schedule,
			clock,
			featureFlags,
		)
	}
}

func withNewBackupAudit(
	ctx context.Context, tbwr *types.TakeBackupWithRetryOperation,
	upsertError error,
) error {
	if upsertError != nil {
		return upsertError
	}
	audit.ReportBackupStateAuditEvent(ctx, tbwr, true)
	return nil
}

func BackupScheduleHandler(
	ctx context.Context,
	driver dbconnector.DBConnector,
	schedule *types.BackupSchedule,
	clock clockwork.Clock,
	featureFlags config.FeatureFlagsConfig,
) error {
	ctx = schedule.SetLogFields(ctx)

	if schedule.Status != types.BackupScheduleStateActive {
		xlog.Error(ctx, "backup schedule is not active")
		return errors.New("backup schedule is not active")
	}
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
				RootPath:             schedule.RootPath,
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

			if schedule.ScheduleSettings.EncryptionSettings != nil && featureFlags.EnableBackupsEncryption {
				tbwr.EncryptionSettings = schedule.ScheduleSettings.EncryptionSettings
			}
		}

		xlog.Info(
			ctx, "create TakeBackupWithRetryOperation for schedule",
			zap.String(log_keys.TakeBackupWithRetryOperation, tbwr.Proto().String()),
		)

		err = schedule.UpdateNextLaunch(clock.Now())
		if err != nil {
			return err
		}
		return withNewBackupAudit(
			ctx, tbwr, driver.Apply(
				ctx,
				dbconnector.Changes{CreateOperations: []types.Operation{tbwr}, UpdateSchedules: []types.BackupSchedule{*schedule}},
			),
		)
	}
	return nil
}
