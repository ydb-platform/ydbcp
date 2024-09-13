package main

import (
	"context"
	"log"
	"time"

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
)

func main() {
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
	client := pb.NewBackupServiceClient(conn)
	backups, err := client.ListBackups(
		context.Background(), &pb.ListBackupsRequest{
			ContainerId:      containerID,
			DatabaseNameMask: "%",
		},
	)
	if err != nil {
		log.Panicf("failed to list backups: %v", err)
	}
	if len(backups.Backups) > 0 {
		log.Panicf("got backup from empty YDBCP: %s", backups.Backups[0].String())
	}
	backupOperation, err := client.MakeBackup(
		context.Background(), &pb.MakeBackupRequest{
			ContainerId:          containerID,
			DatabaseName:         databaseName,
			DatabaseEndpoint:     databaseEndpoint,
			SourcePaths:          nil,
			SourcePathsToExclude: nil,
		},
	)
	if err != nil {
		log.Panicf("failed to make backup: %v", err)
	}
	backups, err = client.ListBackups(
		context.Background(), &pb.ListBackupsRequest{
			ContainerId:      containerID,
			DatabaseNameMask: "%",
		},
	)
	if err != nil {
		log.Panicf("failed to list backups: %v", err)
	}
	if len(backups.Backups) != 1 {
		log.Panicf("Did not list freshly made backup")
	}
	backupPb := backups.Backups[0]
	if backupPb.Id != backupOperation.BackupId {
		log.Panicf(
			"backupOperation backupID %s does not match listed backup id %s", backupOperation.BackupId, backupPb.Id,
		)
	}
	done := false
	for range 30 {
		backup, err := client.GetBackup(
			context.Background(),
			&pb.GetBackupRequest{Id: backupOperation.BackupId},
		)
		if err != nil {
			log.Panicf("failed to get backup: %v", err)
		}
		if backup.GetStatus().String() == types.BackupStateAvailable {
			done = true
			break
		}
		time.Sleep(time.Second)
	}
	if !done {
		log.Panicln("failed to complete a backup in 30 seconds")
	}
	restoreOperation, err := client.MakeRestore(
		context.Background(), &pb.MakeRestoreRequest{
			ContainerId:       containerID,
			BackupId:          backupOperation.BackupId,
			DatabaseName:      databaseName,
			DatabaseEndpoint:  databaseEndpoint,
			DestinationPrefix: "/tmp",
		},
	)
	if err != nil {
		log.Panicf("failed to make restore: %v", err)
	}
	opClient := pb.NewOperationServiceClient(conn)
	done = false
	for range 30 {
		op, err := opClient.GetOperation(
			context.Background(), &pb.GetOperationRequest{
				Id: restoreOperation.Id,
			},
		)
		if err != nil {
			log.Panicf("failed to get operation: %v", err)
		}
		if op.GetStatus().String() == types.OperationStateDone.String() {
			done = true
			break
		}
		time.Sleep(time.Second)
	}
	if !done {
		log.Panicln("failed to complete a restore in 30 seconds")
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
	if len(schedules.Schedules) > 0 {
		log.Panicf("got backup schedule, but none created: %s", schedules.Schedules[0].String())
	}
	schedule, err := scheduleClient.CreateBackupSchedule(
		context.Background(), &pb.CreateBackupScheduleRequest{
			ContainerId:  containerID,
			DatabaseName: databaseName,
			Endpoint:     databaseEndpoint,
			ScheduleName: "schedule",
			ScheduleSettings: &pb.BackupScheduleSettings{
				SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
			},
		},
	)
	if err != nil {
		log.Panicf("failed to create backup schedule: %v", err)
	}
	schedules, err = scheduleClient.ListBackupSchedules(
		context.Background(), &pb.ListBackupSchedulesRequest{
			ContainerId:      containerID,
			DatabaseNameMask: "%",
		},
	)
	if err != nil {
		log.Panicf("failed to list backup schedules: %v", err)
	}
	if len(schedules.Schedules) != 1 {
		log.Panicln("did not list created schedule")
	}
	if schedules.Schedules[0].Id != schedule.Id {
		log.Panicf("schedule and listed schedule ids does not match: %s, %s", schedules.Schedules[0].Id, schedule.Id)
	}
}
