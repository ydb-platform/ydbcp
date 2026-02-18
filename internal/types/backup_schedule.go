package types

import (
	"context"
	"fmt"
	"time"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"

	"github.com/adhocore/gronx"
	"github.com/gorhill/cronexpr"
	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"

	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	BackupScheduleStateUnknown  = pb.BackupSchedule_STATUS_UNSPECIFIED.String()
	BackupScheduleStateActive   = pb.BackupSchedule_ACTIVE.String()
	BackupScheduleStateInactive = pb.BackupSchedule_INACTIVE.String()
	BackupScheduleStateDeleted  = pb.BackupSchedule_DELETED.String()
)

type BackupSchedule struct {
	ID                     string
	ContainerID            string
	DatabaseName           string
	DatabaseEndpoint       string
	RootPath               string
	SourcePaths            []string
	SourcePathsToExclude   []string
	Audit                  *pb.AuditInfo
	Name                   *string
	Status                 string
	ScheduleSettings       *pb.BackupScheduleSettings
	NextLaunch             *time.Time
	LastBackupID           *string
	LastSuccessfulBackupID *string
	RecoveryPoint          *time.Time
}

func (b *BackupSchedule) SetLogFields(ctx context.Context) context.Context {
	if b == nil {
		return ctx
	}

	fields := make([]zap.Field, 0, 4)
	if b.ID != "" {
		fields = append(fields, zap.String(log_keys.ScheduleID, b.ID))
	}
	if b.ContainerID != "" {
		fields = append(fields, zap.String(log_keys.ContainerID, b.ContainerID))
	}
	if b.DatabaseName != "" {
		fields = append(fields, zap.String(log_keys.Database, b.DatabaseName))
	}
	if b.DatabaseEndpoint != "" {
		fields = append(fields, zap.String(log_keys.DatabaseEndpoint, b.DatabaseEndpoint))
	}
	if len(fields) == 0 {
		return ctx
	}
	return xlog.With(ctx, fields...)
}

func ParseCronExpr(str string) (*cronexpr.Expression, error) {
	valid := gronx.IsValid(str)
	if !valid {
		return nil, fmt.Errorf("failed to parse crontab: \"%s\"", str)
	}
	return cronexpr.Parse(str)
}

func (b *BackupSchedule) GetBackupInfo(clock clockwork.Clock) *pb.ScheduledBackupInfo {
	var backupInfo *pb.ScheduledBackupInfo
	if b.LastSuccessfulBackupID != nil {
		backupInfo = &pb.ScheduledBackupInfo{
			BackupId: *b.LastSuccessfulBackupID,
		}
		if b.RecoveryPoint != nil {
			backupInfo.RecoveryPoint = timestamppb.New(*b.RecoveryPoint)
			if b.ScheduleSettings.RecoveryPointObjective != nil {
				rpoMargin := clock.Since(*b.RecoveryPoint)
				backupInfo.LastBackupRpoMarginInterval = durationpb.New(rpoMargin)
				backupInfo.LastBackupRpoMarginRatio = rpoMargin.Seconds() / float64(b.ScheduleSettings.RecoveryPointObjective.Seconds)
			}
		}
	}
	return backupInfo
}

func (b *BackupSchedule) Proto(clock clockwork.Clock) *pb.BackupSchedule {
	var nextLaunchTs *timestamppb.Timestamp
	nextLaunchTs = nil
	if b.NextLaunch != nil {
		nextLaunchTs = timestamppb.New(*b.NextLaunch)
	}
	schedule := &pb.BackupSchedule{
		Id:                       b.ID,
		ContainerId:              b.ContainerID,
		DatabaseName:             b.DatabaseName,
		RootPath:                 b.RootPath,
		Endpoint:                 b.DatabaseEndpoint,
		Audit:                    b.Audit,
		Status:                   pb.BackupSchedule_Status(pb.BackupSchedule_Status_value[b.Status]),
		ScheduleSettings:         b.ScheduleSettings,
		SourcePaths:              b.SourcePaths,
		SourcePathsToExclude:     b.SourcePathsToExclude,
		NextLaunch:               nextLaunchTs,
		LastSuccessfulBackupInfo: b.GetBackupInfo(clock),
	}
	if b.Name != nil {
		schedule.ScheduleName = *b.Name
	}
	return schedule
}

func (b *BackupSchedule) String() string {
	return fmt.Sprintf(
		"ID: %s, Name: %s, ContainerID: %s, DatabaseEndpoint: %s, DatabaseName: %s, Status: %s",
		b.ID,
		*b.Name,
		b.ContainerID,
		b.DatabaseEndpoint,
		b.DatabaseName,
		b.Status,
	)
}

func (b *BackupSchedule) UpdateNextLaunch(now time.Time) error {
	expr, err := ParseCronExpr(b.ScheduleSettings.SchedulePattern.Crontab)
	if err != nil {
		return err
	}
	nextTime := expr.Next(now)
	b.NextLaunch = &nextTime
	return nil
}

func (b *BackupSchedule) GetCronDuration() (time.Duration, error) {
	expr, err := ParseCronExpr(b.ScheduleSettings.SchedulePattern.Crontab)
	if err != nil {
		return time.Duration(0), err
	}
	nextTime := expr.NextN(time.Now(), 2)
	return nextTime[1].Sub(nextTime[0]), nil
}
