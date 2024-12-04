package main

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"log"
	"strings"
	"time"
	"ydbcp/cmd/integration/common"

	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"google.golang.org/grpc"
)

const (
	containerID      = "abcde"
	databaseName     = "/local"
	ydbcpEndpoint    = "0.0.0.0:50051"
	databaseEndpoint = "grpcs://local-ydb:2135"
)

func main() {
	conn := common.CreateGRPCClient(ydbcpEndpoint)
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Panicln("failed to close connection")
		}
	}(conn)
	client := pb.NewBackupServiceClient(conn)
	opClient := pb.NewOperationServiceClient(conn)
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

	_, err = client.MakeBackup(
		context.Background(), &pb.MakeBackupRequest{
			ContainerId:          containerID,
			DatabaseName:         databaseName,
			DatabaseEndpoint:     databaseEndpoint,
			SourcePaths:          nil,
			SourcePathsToExclude: []string{".+"}, // exclude all paths
		},
	)
	if err == nil {
		log.Panicf("backup with empty source paths was created")
	}

	if status.Code(err) != codes.FailedPrecondition {
		log.Panicf("unexpected error code: %v", err)
	}

	tbwr, err := client.MakeBackup(
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
	op, err := opClient.GetOperation(context.Background(), &pb.GetOperationRequest{
		Id: tbwr.Id,
	})
	if err != nil {
		log.Panicf("failed to get operation: %v", err)
	}
	if op.GetType() != types.OperationTypeTBWR.String() {
		log.Panicf("unexpected operation type: %v", op.GetType())
	}
	time.Sleep(time.Second * 11) // to wait for operation handler
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
	ops, err := opClient.ListOperations(context.Background(), &pb.ListOperationsRequest{
		ContainerId:      containerID,
		DatabaseNameMask: databaseName,
		OperationTypes:   []string{types.OperationTypeTB.String()},
	})
	if err != nil {
		log.Panicf("failed to list operations: %v", err)
	}
	if len(ops.Operations) != 1 {
		log.Panicf("expected one TB operation, got %d", len(ops.Operations))
	}
	backupOperation := ops.Operations[0]
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
	time.Sleep(time.Second * 11) // to wait for operation handler
	tbwr, err = opClient.GetOperation(context.Background(), &pb.GetOperationRequest{
		Id: op.Id,
	})
	if err != nil {
		log.Panicf("failed to get operation: %v", err)
	}
	if tbwr.Status.String() != string(types.OperationStateDone) {
		log.Panicf("unexpected operation state: %v", tbwr.Status.String())
	}
	if tbwr.UpdatedAt == nil || !tbwr.UpdatedAt.AsTime().Equal(tbwr.Audit.CompletedAt.AsTime()) {
		log.Panicf("unexpected operation updatedAt/completedAt: %v, %v", tbwr.UpdatedAt, tbwr.Audit.CompletedAt)
	}

	// set ttl to 1 hour
	updatedBackup, err := client.UpdateBackupTtl(
		context.Background(), &pb.UpdateBackupTtlRequest{
			BackupId: backupPb.Id,
			Ttl:      durationpb.New(time.Hour),
		},
	)
	if err != nil {
		log.Panicf("failed to update backup ttl: %v", err)
	}

	if updatedBackup.ExpireAt == nil {
		log.Panicln("expected expireAt to be set")
	}

	if updatedBackup.ExpireAt.AsTime().Sub(time.Now()).Hours() > 1 {
		log.Panicln("expected expireAt to be in an hour, but got in ",
			updatedBackup.ExpireAt.AsTime().Sub(time.Now()).Hours())
	}

	updatedBackupFromDb, err := client.GetBackup(
		context.Background(),
		&pb.GetBackupRequest{Id: backupPb.Id},
	)
	if err != nil {
		log.Panicf("failed to get backup: %v", err)
	}

	if updatedBackupFromDb.ExpireAt == nil {
		log.Panicln("expected expireAt to be set")
	}

	if updatedBackupFromDb.ExpireAt.AsTime().Sub(time.Now()).Hours() > 1 {
		log.Panicln("expected expireAt to be in an hour, but got in ",
			updatedBackup.ExpireAt.AsTime().Sub(time.Now()).Hours())
	}

	// set infinite ttl
	updatedBackup, err = client.UpdateBackupTtl(
		context.Background(), &pb.UpdateBackupTtlRequest{
			BackupId: backupPb.Id,
			Ttl:      nil,
		},
	)
	if err != nil {
		log.Panicf("failed to update backup ttl: %v", err)
	}

	if updatedBackup.ExpireAt != nil {
		log.Panicln("expected empty expireAt")
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
	deleteOperation, err := client.DeleteBackup(
		context.Background(), &pb.DeleteBackupRequest{
			BackupId: backupOperation.BackupId,
		},
	)
	if err != nil {
		log.Panicf("failed to delete backup: %v", err)
	}
	done = false
	for range 30 {
		op, err := opClient.GetOperation(
			context.Background(), &pb.GetOperationRequest{
				Id: deleteOperation.Id,
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
		log.Panicln("failed to complete a delete backup in 30 seconds")
	}
	backup, err := client.GetBackup(
		context.Background(),
		&pb.GetBackupRequest{Id: backupOperation.BackupId},
	)
	if err != nil {
		log.Panicf("failed to get backup: %v", err)
	}
	if backup.GetStatus().String() != types.BackupStateDeleted {
		log.Panicf("expected DELETED backup status, but received: %s", backup.GetStatus().String())
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
	_, err = scheduleClient.CreateBackupSchedule(
		context.Background(), &pb.CreateBackupScheduleRequest{
			ContainerId:  containerID,
			DatabaseName: "/non-existent-db",
			Endpoint:     databaseEndpoint,
			ScheduleName: "schedule",
			ScheduleSettings: &pb.BackupScheduleSettings{
				SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * *"},
			},
		},
	)
	if err == nil {
		log.Panicln("created schedule for non-existent-db")
	}
	if !strings.Contains(err.Error(), "user has no access to database") {
		log.Panicf("Unexpected error message: %s", err.Error())
	}
	schedule, err := scheduleClient.CreateBackupSchedule(
		context.Background(), &pb.CreateBackupScheduleRequest{
			ContainerId:  containerID,
			DatabaseName: databaseName,
			Endpoint:     databaseEndpoint,
			ScheduleName: "schedule",
			ScheduleSettings: &pb.BackupScheduleSettings{
				SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * *"},
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

	if len(schedules.Schedules[0].SourcePaths) != 0 {
		log.Panicln("unexpected number of source paths")
	}

	if len(schedules.Schedules[0].SourcePathsToExclude) != 0 {
		log.Panicln("unexpected number of source paths to exclude")
	}

	if schedules.Schedules[0].Id != schedule.Id {
		log.Panicf("schedule and listed schedule ids does not match: %s, %s", schedules.Schedules[0].Id, schedule.Id)
	}

	newScheduleName := "schedule-2.0"
	newSourcePath := "/kv_test"
	newSchedule, err := scheduleClient.UpdateBackupSchedule(
		context.Background(), &pb.UpdateBackupScheduleRequest{
			Id:           schedule.Id,
			ScheduleName: newScheduleName,
			SourcePaths:  []string{newSourcePath},
		},
	)
	if err != nil {
		log.Panicf("failed to update backup schedule: %v", err)
	}
	if newSchedule.Id != schedule.Id {
		log.Panicf("schedule and updated schedule ids does not match: %s != %s", schedule.Id, newSchedule.Id)
	}
	if newSchedule.ScheduleName != newScheduleName {
		log.Panicf("schedule name does not match: %s != %s", newSchedule.ScheduleName, newScheduleName)
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
		log.Panicln("unexpected number of schedules")
	}
	if schedules.Schedules[0].ScheduleName != newScheduleName {
		log.Panicf("schedule name does not match: %s != %s", schedules.Schedules[0].ScheduleName, newScheduleName)
	}
	if len(schedules.Schedules[0].SourcePaths) != 1 {
		log.Panicf("unexpected number of source paths: %d", len(schedules.Schedules[0].SourcePaths))
	}
	if schedules.Schedules[0].SourcePaths[0] != newSourcePath {
		log.Panicf("source paths not match: %s != %s", schedules.Schedules[0].ScheduleName, newScheduleName)
	}
	if len(schedules.Schedules[0].SourcePathsToExclude) != 0 {
		log.Panicln("unexpected number of source paths to exclude")
	}

	inactiveSchedule, err := scheduleClient.ToggleBackupSchedule(
		context.Background(), &pb.ToggleBackupScheduleRequest{
			Id:          newSchedule.Id,
			ActiveState: false,
		},
	)

	if err != nil {
		log.Panicf("failed to deactivate backup schedule: %v", err)
	}

	if inactiveSchedule.Status != pb.BackupSchedule_INACTIVE {
		log.Panicf("expected INACTIVE backup schedule status, but received: %s", inactiveSchedule.Status.String())
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
		log.Panicln("unexpected number of schedules")
	}

	if schedules.Schedules[0].Status != pb.BackupSchedule_INACTIVE {
		log.Panicf("expected INACTIVE backup schedule status, but received: %s", schedules.Schedules[0].Status.String())
	}

	deletedSchedule, err := scheduleClient.DeleteBackupSchedule(
		context.Background(), &pb.DeleteBackupScheduleRequest{
			Id: schedule.Id,
		},
	)
	if err != nil {
		log.Panicf("failed to delete backup schedule: %v", err)
	}

	if deletedSchedule.Status != pb.BackupSchedule_DELETED {
		log.Panicf("expected DELETED backup schedule status, but received: %s", deletedSchedule.Status.String())
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
		log.Panicln("unexpected number of schedules")
	}
	if schedules.Schedules[0].Status != pb.BackupSchedule_DELETED {
		log.Panicf("expected DELETED backup schedule status, but received: %s", schedules.Schedules[0].Status.String())
	}

	_, err = scheduleClient.ToggleBackupSchedule(
		context.Background(), &pb.ToggleBackupScheduleRequest{
			Id:          deletedSchedule.Id,
			ActiveState: true,
		},
	)

	if err == nil {
		log.Panicln("deleted schedule was successfully activated")
	}
}
