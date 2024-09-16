package types

import (
	"context"
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
	Name                   string
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
		rpoMargin := time.Since(*b.RecoveryPoint)
		backupInfo = &pb.ScheduledBackupInfo{
			BackupId:                    *b.LastSuccessfulBackupID,
			RecoveryPoint:               timestamppb.New(*b.RecoveryPoint),
			LastBackupRpoMarginInterval: durationpb.New(rpoMargin),
			LastBackupRpoMarginPercent:  rpoMargin.Seconds() / float64(b.ScheduleSettings.RecoveryPointObjective.Seconds),
		}
	}
	var nextLaunchTs *timestamppb.Timestamp
	nextLaunchTs = nil
	if b.NextLaunch != nil {
		nextLaunchTs = timestamppb.New(*b.NextLaunch)
	}
	return &pb.BackupSchedule{
		Id:                       b.ID,
		ContainerId:              b.ContainerID,
		DatabaseName:             b.DatabaseName,
		Endpoint:                 b.DatabaseEndpoint,
		SourcePaths:              b.SourcePaths,
		SourcePathsToExclude:     b.SourcePathsToExclude,
		Audit:                    b.Audit,
		ScheduleName:             b.Name,
		Active:                   b.Active,
		ScheduleSettings:         b.ScheduleSettings,
		NextLaunch:               nextLaunchTs,
		LastSuccessfulBackupInfo: backupInfo,
	}
}

func (b *BackupSchedule) String() string {
	return fmt.Sprintf(
		"ID: %s, Name: %s, ContainerID: %s, DatabaseEndpoint: %s, DatabaseName: %s",
		b.ID,
		b.Name,
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

type BackupScheduleHandler func(context.Context, BackupSchedule) error
