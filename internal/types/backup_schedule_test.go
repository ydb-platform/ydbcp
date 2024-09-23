package types

import (
	"github.com/stretchr/testify/assert"
	"github.com/undefinedlabs/go-mpatch"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"testing"
	"time"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

var (
	fourPM = time.Date(2024, 01, 01, 16, 0, 0, 0, time.UTC)
	fivePM = time.Date(2024, 01, 01, 17, 0, 0, 0, time.UTC)
)

func TestProtoConversion_Simple(t *testing.T) {
	_, err := mpatch.PatchMethod(
		time.Now, func() time.Time {
			return fivePM.Add(time.Minute * 30)
		},
	)
	if err != nil {
		log.Panicln("failed to monkeypatch time.Now")

	}
	id1 := "1"
	id2 := "2"
	sc := BackupSchedule{
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern:        &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
			RecoveryPointObjective: durationpb.New(time.Hour),
		},
		LastBackupID:           &id1,
		LastSuccessfulBackupID: &id2,
		RecoveryPoint:          &fourPM,
	}
	scProto := sc.Proto()
	info := pb.ScheduledBackupInfo{
		BackupId:                    "2",
		RecoveryPoint:               timestamppb.New(fourPM),
		LastBackupRpoMarginInterval: durationpb.New(time.Minute * 90),
		LastBackupRpoMarginRatio:    1.5,
	}
	assert.Equal(t, scProto.LastSuccessfulBackupInfo.String(), info.String())
}

func TestProtoConversion_HugeMargin(t *testing.T) {
	id1 := "1"
	id2 := "2"
	sc := BackupSchedule{
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern:        &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
			RecoveryPointObjective: durationpb.New(time.Second),
		},
		LastBackupID:           &id1,
		LastSuccessfulBackupID: &id2,
		RecoveryPoint:          &fourPM,
	}
	scProto := sc.Proto()
	info := pb.ScheduledBackupInfo{
		BackupId:                    "2",
		RecoveryPoint:               timestamppb.New(fourPM),
		LastBackupRpoMarginInterval: durationpb.New(time.Minute * 90),
		LastBackupRpoMarginRatio:    5400.0,
	}
	assert.Equal(t, info.String(), scProto.LastSuccessfulBackupInfo.String())
}

func TestProtoConversion_NoAvailableBackup(t *testing.T) {
	id1 := "1"
	sc := BackupSchedule{
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern:        &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
			RecoveryPointObjective: durationpb.New(time.Second),
		},
		LastBackupID:           &id1,
		LastSuccessfulBackupID: nil,
		RecoveryPoint:          &fourPM,
	}
	scProto := sc.Proto()
	assert.Empty(t, scProto.LastSuccessfulBackupInfo)
}

func TestProtoConversion_NoRPO(t *testing.T) {
	id1 := "1"
	id2 := "2"
	sc := BackupSchedule{
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
		},
		LastBackupID:           &id1,
		LastSuccessfulBackupID: &id2,
		RecoveryPoint:          &fourPM,
	}
	scProto := sc.Proto()
	info := pb.ScheduledBackupInfo{
		BackupId:      "2",
		RecoveryPoint: timestamppb.New(fourPM),
	}
	assert.Equal(t, info.String(), scProto.LastSuccessfulBackupInfo.String())
}

func TestProtoConversion_NoRPONoBackup(t *testing.T) {

	id1 := "1"
	sc := BackupSchedule{
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
		},
		LastBackupID:           &id1,
		LastSuccessfulBackupID: nil,
		RecoveryPoint:          &fourPM,
	}
	scProto := sc.Proto()
	assert.Empty(t, scProto.LastSuccessfulBackupInfo)
}
