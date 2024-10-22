package types

import (
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"

	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

var (
	fourPM = time.Date(2024, 01, 01, 16, 0, 0, 0, time.UTC)
	fivePM = time.Date(2024, 01, 01, 17, 0, 0, 0, time.UTC)
)

func TestCrontab(t *testing.T) {
	_, err := ParseCronExpr("* * * * * * * * * * *& ")
	assert.Error(t, err)
	_, err = ParseCronExpr("* * * * *")
	assert.NoError(t, err)
	sc := BackupSchedule{
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{
				Crontab: "0 * 1 * *",
			},
		},
	}
	err = sc.UpdateNextLaunch(fivePM)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2024, 01, 01, 18, 0, 0, 0, time.UTC), *sc.NextLaunch)
	err = sc.UpdateNextLaunch(time.Date(2024, 01, 02, 17, 0, 0, 0, time.UTC))
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2024, 02, 01, 0, 0, 0, 0, time.UTC), *sc.NextLaunch)
}

func TestProtoConversion_Simple(t *testing.T) {
	clock := clockwork.NewFakeClockAt(fivePM.Add(time.Minute * 30))
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
	scProto := sc.Proto(clock)
	info := pb.ScheduledBackupInfo{
		BackupId:                    "2",
		RecoveryPoint:               timestamppb.New(fourPM),
		LastBackupRpoMarginInterval: durationpb.New(time.Minute * 90),
		LastBackupRpoMarginRatio:    1.5,
	}
	assert.Equal(t, scProto.LastSuccessfulBackupInfo.String(), info.String())
}

func TestProtoConversion_HugeMargin(t *testing.T) {
	clock := clockwork.NewFakeClockAt(fivePM.Add(time.Minute * 30))
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
	scProto := sc.Proto(clock)
	info := pb.ScheduledBackupInfo{
		BackupId:                    "2",
		RecoveryPoint:               timestamppb.New(fourPM),
		LastBackupRpoMarginInterval: durationpb.New(time.Minute * 90),
		LastBackupRpoMarginRatio:    5400.0,
	}
	assert.Equal(t, info.String(), scProto.LastSuccessfulBackupInfo.String())
}

func TestProtoConversion_NoAvailableBackup(t *testing.T) {
	clock := clockwork.NewFakeClockAt(fivePM.Add(time.Minute * 30))
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
	scProto := sc.Proto(clock)
	assert.Empty(t, scProto.LastSuccessfulBackupInfo)
}

func TestProtoConversion_NoRPO(t *testing.T) {
	clock := clockwork.NewFakeClockAt(fivePM.Add(time.Minute * 30))
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
	scProto := sc.Proto(clock)
	info := pb.ScheduledBackupInfo{
		BackupId:      "2",
		RecoveryPoint: timestamppb.New(fourPM),
	}
	assert.Equal(t, info.String(), scProto.LastSuccessfulBackupInfo.String())
}

func TestProtoConversion_NoRPONoBackup(t *testing.T) {
	clock := clockwork.NewFakeClockAt(fivePM.Add(time.Minute * 30))
	id1 := "1"
	sc := BackupSchedule{
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
		},
		LastBackupID:           &id1,
		LastSuccessfulBackupID: nil,
		RecoveryPoint:          &fourPM,
	}
	scProto := sc.Proto(clock)
	assert.Empty(t, scProto.LastSuccessfulBackupInfo)
}
