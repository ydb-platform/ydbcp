package main

import (
	"context"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"time"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"

	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	containerID      = "abcde"
	databaseName     = "/local"
	ydbcpEndpoint    = "localhost:50051"
	databaseEndpoint = "grpcs://local-ydb:2135"
	connectionString = "grpcs://local-ydb:2135/local"
)

var (
	threePM = time.Date(2024, 01, 01, 15, 0, 0, 0, time.UTC)
	fourPM  = time.Date(2024, 01, 01, 16, 0, 0, 0, time.UTC)
	fivePM  = time.Date(2024, 01, 01, 17, 0, 0, 0, time.UTC)
)

func BackupsToInsert() ([]types.Backup, error) {
	audit1 := &pb.AuditInfo{
		Creator:     containerID,
		CreatedAt:   timestamppb.New(threePM),
		CompletedAt: timestamppb.New(fourPM),
	}
	audit2 := &pb.AuditInfo{
		Creator:     containerID,
		CreatedAt:   timestamppb.New(fourPM),
		CompletedAt: timestamppb.New(fivePM),
	}
	scheduleId1 := "1"
	scheduleId2 := "2"
	scheduleId3 := "3"
	scheduleId4 := "4"
	return []types.Backup{
		{
			ID:               "1",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Status:           types.BackupStateAvailable,
			Message:          "for schedule 1",
			AuditInfo:        audit1,
			Size:             10,
			ScheduleID:       &scheduleId1,
		},
		{
			ID:               "2",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Status:           types.BackupStateAvailable,
			Message:          "for schedule 1",
			AuditInfo:        audit2,
			Size:             10,
			ScheduleID:       &scheduleId1,
		},
		{
			ID:               "3",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Status:           types.BackupStateAvailable,
			Message:          "not for schedule",
			AuditInfo:        audit2,
			Size:             10,
			ScheduleID:       nil,
		},
		{
			ID:               "4",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Status:           types.BackupStateAvailable,
			Message:          "for schedule 2",
			AuditInfo:        audit1,
			Size:             10,
			ScheduleID:       &scheduleId2,
		},
		{
			ID:               "5",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Status:           types.BackupStateDeleted,
			Message:          "for schedule 2",
			AuditInfo:        audit2,
			Size:             10,
			ScheduleID:       &scheduleId2,
		},
		{
			ID:               "6",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Status:           types.BackupStateAvailable,
			Message:          "for schedule 3",
			AuditInfo:        audit1,
			Size:             10,
			ScheduleID:       &scheduleId3,
		},
		{
			ID:               "7",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Status:           types.BackupStateDeleted,
			Message:          "for schedule 3",
			AuditInfo:        audit2,
			Size:             10,
			ScheduleID:       &scheduleId3,
		},
		{
			ID:               "8",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Status:           types.BackupStateDeleted,
			Message:          "for schedule 4",
			AuditInfo:        audit2,
			Size:             10,
			ScheduleID:       &scheduleId4,
		},
	}, nil
}

func SchedulesToInsert() []types.BackupSchedule {
	name1 := "schedule 1"
	name2 := "schedule 2"
	name3 := "schedule 3"
	name4 := "schedule 4"
	return []types.BackupSchedule{
		{
			ID:               "1",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Name:             &name1,
			Active:           true,
			ScheduleSettings: &pb.BackupScheduleSettings{
				SchedulePattern:        &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
				RecoveryPointObjective: durationpb.New(time.Hour),
			},
		},
		{
			ID:               "2",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Name:             &name2,
			Active:           true,
			ScheduleSettings: &pb.BackupScheduleSettings{
				SchedulePattern:        &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
				RecoveryPointObjective: durationpb.New(time.Minute * 15),
			},
		},
		{
			ID:               "3",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Name:             &name3,
			Active:           true,
			ScheduleSettings: &pb.BackupScheduleSettings{
				SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
			},
		},
		{
			ID:               "4",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Name:             &name4,
			Active:           true,
			ScheduleSettings: &pb.BackupScheduleSettings{
				SchedulePattern:        &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
				RecoveryPointObjective: durationpb.New(time.Minute * 15),
			},
		},
	}
}

func main() {
	ctx := context.Background()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(ydbcpEndpoint, opts...)
	if err != nil {
		log.Panicln("failed to dial")
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Panicln("failed to close connection")
		}
	}(conn)
	ydbConn, err := db.NewYdbConnector(
		ctx,
		config.YDBConnectionConfig{
			ConnectionString:   connectionString,
			Insecure:           true,
			Discovery:          false,
			DialTimeoutSeconds: 10,
		},
	)
	if err != nil {
		log.Panicf("failed to create ydb connector: %v", err)
	}
	backups, err := BackupsToInsert()
	if err != nil {
		log.Panicf("failed to create backups to insert: %v", err)
	}
	for _, b := range backups {
		err = ydbConn.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithCreateBackup(b))
		if err != nil {
			log.Panicf("failed to insert backup: %v", err)
		}
	}

	schedulesToInsert := SchedulesToInsert()
	for _, s := range schedulesToInsert {
		err = ydbConn.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithCreateBackupSchedule(s))
		if err != nil {
			log.Panicf("failed to insert schedule: %v", err)
		}
	}

	scheduleClient := pb.NewBackupScheduleServiceClient(conn)
	schedules, err := scheduleClient.ListBackupSchedules(
		context.Background(), &pb.ListBackupSchedulesRequest{
			ContainerId:      containerID,
			DatabaseNameMask: "%",
		},
	)
	if err != nil {
		log.Panicf("failed to list backup schedules: %v", err)
	}

	if len(schedules.Schedules) != 4 {
		log.Panicln("did not get all the schedules")
	}
	for _, s := range schedules.Schedules {
		switch s.Id {
		case "1":
			{
				if s.LastSuccessfulBackupInfo.BackupId != "2" || s.LastSuccessfulBackupInfo.RecoveryPoint.AsTime() != fivePM {
					log.Panicf(
						"Expected BackupID = 2, RecoveryPoint = %s, got %s for scheduleID %s", fivePM.String(),
						s.LastSuccessfulBackupInfo.String(),
						s.Id,
					)
				}
			}
		case "2":
			{
				if s.LastSuccessfulBackupInfo.BackupId != "4" || s.LastSuccessfulBackupInfo.RecoveryPoint.AsTime() != fourPM {
					log.Panicf(
						"Expected BackupID = 4, RecoveryPoint = %s, got %s for scheduleID %s", fourPM.String(),
						s.LastSuccessfulBackupInfo.String(),
						s.Id,
					)

				}
			}
		case "3":
			{
				info := &pb.ScheduledBackupInfo{
					BackupId:      "6",
					RecoveryPoint: timestamppb.New(fourPM),
				}
				if !proto.Equal(info, s.LastSuccessfulBackupInfo) {
					log.Panicf(
						"Expected %s, got %s for scheduleID %s", info.String(), s.LastSuccessfulBackupInfo.String(),
						s.Id,
					)
				}
			}
		case "4":
			{
				if s.LastSuccessfulBackupInfo != nil {
					log.Panicf(
						"Expected nil, got %s for scheduleID %s", s.LastSuccessfulBackupInfo.String(),
						s.Id,
					)
				}
			}
		default:
			{
				log.Panicf("unexpected schedule id: %s", s.Id)
			}
		}
	}
	{
		s, err := scheduleClient.GetBackupSchedule(ctx, &pb.GetBackupScheduleRequest{Id: "1"})
		if err != nil {
			log.Panicf("failed to get backup schedule: %v", err)
		}
		if s.LastSuccessfulBackupInfo.BackupId != "2" || s.LastSuccessfulBackupInfo.RecoveryPoint.AsTime() != fivePM {
			log.Panicf(
				"Expected BackupID = 2, RecoveryPoint = %s, got %s for scheduleID %s", fivePM.String(),
				s.LastSuccessfulBackupInfo.String(),
				s.Id,
			)
		}
	}
	{
		s, err := scheduleClient.GetBackupSchedule(ctx, &pb.GetBackupScheduleRequest{Id: "2"})
		if err != nil {
			log.Panicf("failed to get backup schedule: %v", err)
		}
		if s.LastSuccessfulBackupInfo.BackupId != "4" || s.LastSuccessfulBackupInfo.RecoveryPoint.AsTime() != fourPM {
			log.Panicf(
				"Expected BackupID = 4, RecoveryPoint = %s, got %s for scheduleID %s", fourPM.String(),
				s.LastSuccessfulBackupInfo.String(),
				s.Id,
			)

		}
	}
	{
		s, err := scheduleClient.GetBackupSchedule(ctx, &pb.GetBackupScheduleRequest{Id: "3"})
		if err != nil {
			log.Panicf("failed to get backup schedule: %v", err)
		}
		info := &pb.ScheduledBackupInfo{
			BackupId:      "6",
			RecoveryPoint: timestamppb.New(fourPM),
		}
		if !proto.Equal(info, s.LastSuccessfulBackupInfo) {
			log.Panicf(
				"Expected %s, got %s for scheduleID %s", info.String(), s.LastSuccessfulBackupInfo.String(),
				s.Id,
			)
		}
	}
	{
		s, err := scheduleClient.GetBackupSchedule(ctx, &pb.GetBackupScheduleRequest{Id: "4"})
		if err != nil {
			log.Panicf("failed to get backup schedule: %v", err)
		}
		if s.LastSuccessfulBackupInfo != nil {
			log.Panicf(
				"Expected nil, got %s for scheduleID %s", s.LastSuccessfulBackupInfo.String(),
				s.Id,
			)
		}
	}

}
