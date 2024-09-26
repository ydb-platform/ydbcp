package types

import (
	"fmt"
	"github.com/gorhill/cronexpr"
	"time"

	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BackupSchedule struct {
	ID                     string
	ContainerID            string
	DatabaseName           string
	DatabaseEndpoint       string
	SourcePaths            []string
	SourcePathsToExclude   []string
	Audit                  *pb.AuditInfo
	Name                   *string
	Active                 bool
	ScheduleSettings       *pb.BackupScheduleSettings
	NextLaunch             *time.Time
	LastBackupID           *string
	LastSuccessfulBackupID *string
	RecoveryPoint          *time.Time
}

func (b *BackupSchedule) Proto() *pb.BackupSchedule {
	var backupInfo *pb.ScheduledBackupInfo
	if b.LastSuccessfulBackupID != nil {
		backupInfo = &pb.ScheduledBackupInfo{
			BackupId: *b.LastSuccessfulBackupID,
		}
		if b.RecoveryPoint != nil {
			backupInfo.RecoveryPoint = timestamppb.New(*b.RecoveryPoint)
			if b.ScheduleSettings.RecoveryPointObjective != nil {
				rpoMargin := time.Since(*b.RecoveryPoint)
				backupInfo.RecoveryPoint = timestamppb.New(*b.RecoveryPoint)
				backupInfo.LastBackupRpoMarginInterval = durationpb.New(rpoMargin)
				backupInfo.LastBackupRpoMarginRatio = rpoMargin.Seconds() / float64(b.ScheduleSettings.RecoveryPointObjective.Seconds)
			}
		}
	}
	var nextLaunchTs *timestamppb.Timestamp
	nextLaunchTs = nil
	if b.NextLaunch != nil {
		nextLaunchTs = timestamppb.New(*b.NextLaunch)
	}
	schedule := &pb.BackupSchedule{
		Id:                       b.ID,
		ContainerId:              b.ContainerID,
		DatabaseName:             b.DatabaseName,
		Endpoint:                 b.DatabaseEndpoint,
		Audit:                    b.Audit,
		Active:                   b.Active,
		ScheduleSettings:         b.ScheduleSettings,
		SourcePaths:              b.SourcePaths,
		SourcePathsToExclude:     b.SourcePathsToExclude,
		NextLaunch:               nextLaunchTs,
		LastSuccessfulBackupInfo: backupInfo,
	}
	if b.Name != nil {
		schedule.ScheduleName = *b.Name
	}
	return schedule
}

func (b *BackupSchedule) String() string {
	return fmt.Sprintf(
		"ID: %s, Name: %s, ContainerID: %s, DatabaseEndpoint: %s, DatabaseName: %s",
		b.ID,
		*b.Name,
		b.ContainerID,
		b.DatabaseEndpoint,
		b.DatabaseName,
	)
}

func (b *BackupSchedule) UpdateNextLaunch(now time.Time) error {
	expr, err := cronexpr.Parse(b.ScheduleSettings.SchedulePattern.Crontab)
	if err != nil {
		return fmt.Errorf("failed to parse crontab: %v", err)
	}
	nextTime := expr.Next(now)
	b.NextLaunch = &nextTime
	return nil
}
