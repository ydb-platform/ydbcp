package main

import (
	"bytes"
	"context"
	"github.com/jonboulle/clockwork"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
	"ydbcp/cmd/integration/common"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/metrics"

	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"google.golang.org/grpc"
)

const (
	containerID      = "abcde"
	databaseName     = "/local"
	ydbcpEndpoint    = "0.0.0.0:50051"
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
			Status:           types.BackupScheduleStateActive,
			ScheduleSettings: &pb.BackupScheduleSettings{
				SchedulePattern:        &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
				RecoveryPointObjective: durationpb.New(time.Hour),
			},
			Audit: &pb.AuditInfo{
				CreatedAt:   timestamppb.New(threePM),
				CompletedAt: nil,
			},
		},
		{
			ID:               "2",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Name:             &name2,
			Status:           types.BackupScheduleStateActive,
			ScheduleSettings: &pb.BackupScheduleSettings{
				SchedulePattern:        &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
				RecoveryPointObjective: durationpb.New(time.Minute * 15),
			},
			Audit: &pb.AuditInfo{
				CreatedAt:   timestamppb.New(fourPM),
				CompletedAt: nil,
			},
		},
		{
			ID:               "3",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Name:             &name3,
			Status:           types.BackupScheduleStateActive,
			ScheduleSettings: &pb.BackupScheduleSettings{
				SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
			},
			Audit: &pb.AuditInfo{
				CreatedAt:   timestamppb.New(fivePM),
				CompletedAt: nil,
			},
		},
		{
			ID:               "4",
			ContainerID:      containerID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			Name:             &name4,
			Status:           types.BackupScheduleStateActive,
			ScheduleSettings: &pb.BackupScheduleSettings{
				SchedulePattern:        &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
				RecoveryPointObjective: durationpb.New(time.Minute * 15),
			},
			Audit: &pb.AuditInfo{
				CreatedAt:   timestamppb.New(fivePM.Add(time.Hour)),
				CompletedAt: nil,
			},
		},
	}
}

func ParseMetric() {
	var res *http.Response
	var err error
	for {
		res, err = http.Get("http://0.0.0.0:50052/metrics")
		if err != nil {
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	resBody, err := io.ReadAll(res.Body)

	if err != nil {
		log.Panicf(err.Error())
	}

	pattern := []byte("healthcheck_ydb_errors")
	val := 0
	for _, line := range bytes.Split(resBody, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		if line[0] == byte('#') {
			continue
		}

		i := bytes.Index(line, pattern)
		if i < 0 {
			continue
		}
		i += len(pattern)
		val, _ = strconv.Atoi(string(line[i+1:]))
		break
	}

	if val != 1 {
		log.Panicf("wrong ydb errors metric")
	}
}

func NewMetricsServer() {
	var wg sync.WaitGroup

	cfg := &config.MetricsServerConfig{
		BindPort:    50052,
		BindAddress: "0.0.0.0",
	}
	metrics.InitializeMetricsRegistry(context.Background(), &wg, cfg, clockwork.NewFakeClock())
}

func main() {
	ctx := context.Background()
	conn := common.CreateGRPCClient(ydbcpEndpoint)
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

	//test errors metric
	NewMetricsServer()
	err = ydbConn.ExecuteUpsert(ctx, queries.NewWriteTableQueryMock())
	if err == nil {
		log.Panicf("error should be present")
	}
	ParseMetric()

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

	{
		schedules, err := scheduleClient.ListBackupSchedules(
			context.Background(), &pb.ListBackupSchedulesRequest{
				ContainerId:      containerID,
				DatabaseNameMask: "%",
				PageSize:         3,
			},
		)
		if err != nil {
			log.Panicf("failed to list backup schedules: %v", err)
		}
		if len(schedules.Schedules) != 3 {
			log.Panicln("did not get expected amount schedules")
		}
		if schedules.NextPageToken != "3" {
			log.Panicln("wrong next page token")
		}
		for i, s := range schedules.Schedules {
			if strconv.Itoa(4-i) != s.Id {
				log.Panicf("wrong schedules order: expected %d, got %s", i, s.Id)
			}
			switch s.Id {
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
	}
	{
		schedules, err := scheduleClient.ListBackupSchedules(
			context.Background(), &pb.ListBackupSchedulesRequest{
				ContainerId:      containerID,
				DatabaseNameMask: "%",
				PageSize:         3,
				PageToken:        "3",
			},
		)
		if err != nil {
			log.Panicf("failed to list backup schedules: %v", err)
		}
		if len(schedules.Schedules) != 1 {
			log.Panicln("did not get expected amount schedules")
		}
		if schedules.NextPageToken != "" {
			log.Panicln("wrong next page token")
		}

		for _, s := range schedules.Schedules {
			if s.Id != "1" {
				log.Panicf("wrong schedule id, expected 1, got %s", s.Id)
			}
			if s.LastSuccessfulBackupInfo.BackupId != "2" || s.LastSuccessfulBackupInfo.RecoveryPoint.AsTime() != fivePM {
				log.Panicf(
					"Expected BackupID = 2, RecoveryPoint = %s, got %s for scheduleID %s", fivePM.String(),
					s.LastSuccessfulBackupInfo.String(),
					s.Id,
				)
			}
		}
	}
	{ // list inactive/deleted schedules
		schedules, err := scheduleClient.ListBackupSchedules(
			context.Background(), &pb.ListBackupSchedulesRequest{
				ContainerId:      containerID,
				DatabaseNameMask: "%",
				DisplayStatus:    []pb.BackupSchedule_Status{pb.BackupSchedule_INACTIVE, pb.BackupSchedule_DELETED},
			},
		)
		if err != nil {
			log.Panicf("failed to list backup schedules: %v", err)
		}
		if len(schedules.Schedules) != 0 {
			log.Panicln("unexpected number of schedules")
		}
	}
	{ // list active schedules
		schedules, err := scheduleClient.ListBackupSchedules(
			context.Background(), &pb.ListBackupSchedulesRequest{
				ContainerId:      containerID,
				DatabaseNameMask: "%",
				DisplayStatus:    []pb.BackupSchedule_Status{pb.BackupSchedule_ACTIVE},
			},
		)
		if err != nil {
			log.Panicf("failed to list backup schedules: %v", err)
		}
		if len(schedules.Schedules) != 4 {
			log.Panicln("unexpected number of schedules")
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
	{
		backupClient := pb.NewBackupServiceClient(conn)
		backupsPb, err := backupClient.ListBackups(
			ctx, &pb.ListBackupsRequest{
				ContainerId:      containerID,
				DatabaseNameMask: "%",
				PageSize:         4,
			},
		)
		if err != nil {
			log.Panicf("failed to list backups: %v", err)
		}
		if len(backupsPb.Backups) != 4 {
			log.Panicf("wrong list response size")
		}
		if backupsPb.NextPageToken != "4" {
			log.Panicf("wrong next page token, expected \"4\", got \"%s\"", backupsPb.NextPageToken)
		}
		backupsPb, err = backupClient.ListBackups(
			ctx, &pb.ListBackupsRequest{
				ContainerId:      containerID,
				DatabaseNameMask: "%",
				PageSize:         4,
				PageToken:        "4",
			},
		)
		if err != nil {
			log.Panicf("failed to list backups: %v", err)
		}
		if len(backupsPb.Backups) != 4 {
			log.Panicf("wrong list response size")
		}
		if backupsPb.NextPageToken != "8" {
			log.Panicf("wrong next page token, expected \"8\", got \"%s\"", backupsPb.NextPageToken)
		}
		backupsPb, err = backupClient.ListBackups(
			ctx, &pb.ListBackupsRequest{
				ContainerId:      containerID,
				DatabaseNameMask: "%",
				PageSize:         4,
				PageToken:        "8",
			},
		)
		if err != nil {
			log.Panicf("failed to list backups: %v", err)
		}
		if len(backupsPb.Backups) != 0 {
			log.Panicf("wrong list response size")
		}
		if backupsPb.NextPageToken != "" {
			log.Panicf("wrong next page token, expected \"\", got \"%s\"", backupsPb.NextPageToken)
		}
		backupsPb, err = backupClient.ListBackups(
			ctx, &pb.ListBackupsRequest{
				ContainerId:      containerID,
				DatabaseNameMask: "%",
				Order: &pb.ListBackupsOrder{
					Field: pb.BackupField_CREATED_AT,
					Desc:  true,
				},
				DisplayStatus: []pb.Backup_Status{pb.Backup_DELETED},
			},
		)
		if err != nil {
			log.Panicf("failed to list backups: %v", err)
		}
		if len(backupsPb.Backups) != 3 {
			log.Panicf("wrong list response size")
		}
		if backupsPb.Backups[0].Audit.CreatedAt.AsTime() != fourPM {
			log.Panicf(
				"expected created_at: %s, got: %s", fourPM.String(),
				backupsPb.Backups[0].Audit.CreatedAt.AsTime().String(),
			)
		}
	}
}
