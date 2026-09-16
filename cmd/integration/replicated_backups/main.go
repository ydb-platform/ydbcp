package main

import (
	"context"
	"log"
	"os"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"
	"ydbcp/cmd/integration/common"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/types"
)

const (
	containerID      = "cross-region-catalog"
	databaseName     = "/local"
	databaseEndpoint = "grpcs://local-ydb:2135"
	connectionString = "grpcs://local-ydb:2135/local"
	ydbcpEndpoint    = "0.0.0.0:50051"

	localBackupID  = "00000000-0000-4000-8000-000000000001"
	remoteBackupID = "00000000-0000-4000-8000-000000000002"
)

func InsertReplicatedBackups(ctx context.Context, ydbConn *db.YdbConnector) error {
	return ydbConn.GetQueryClient().Do(ctx, func(ctx context.Context, session query.Session) error {
		res, err := session.Query(
			ctx,
			`UPSERT INTO replicated_backups (id, container_id, database, endpoint, status, schedule_id) VALUES
    ("00000000-0000-4000-8000-000000000002", "cross-region-catalog", "/local", "grpcs://local-ydb:2135", "AVAILABLE", "remote-schedule"),
    ("00000000-0000-4000-8000-000000000003", "cross-region-catalog", "/local", "grpcs://local-ydb:2135", "ERROR", "remote-schedule"),
    ("00000000-0000-4000-8000-000000000004", "another-container", "/local", "grpcs://local-ydb:2135", "AVAILABLE", "remote-schedule");`,
			query.WithTxControl(query.TxControl(
				query.BeginTx(query.WithSerializableReadWrite()),
				query.CommitTx(),
			)),
		)
		if err != nil {
			return err
		}
		return res.Close(ctx)
	})
}

func main() {
	ctx := context.Background()
	conn := common.CreateGRPCClient(ydbcpEndpoint)
	defer conn.Close()

	ydbConn, err := db.NewYdbConnector(ctx, config.YDBConnectionConfig{
		ConnectionString:   connectionString,
		Insecure:           true,
		Discovery:          false,
		DialTimeoutSeconds: 10,
	})
	if err != nil {
		log.Panicf("failed to create ydb connector: %v", err)
	}
	defer ydbConn.Close(ctx)

	localScheduleID := "local-schedule"
	err = ydbConn.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithCreateBackup(types.Backup{
		ID:               localBackupID,
		ContainerID:      containerID,
		DatabaseName:     databaseName,
		DatabaseEndpoint: databaseEndpoint,
		Status:           types.BackupStateAvailable,
		ScheduleID:       &localScheduleID,
	}))
	if err != nil {
		log.Panicf("failed to insert local backup: %v", err)
	}
	if err = InsertReplicatedBackups(ctx, ydbConn); err != nil {
		log.Panicf("failed to insert replicated backups: %v", err)
	}

	backupClient := pb.NewBackupServiceClient(conn)
	catalog, err := backupClient.ListBackups(ctx, &pb.ListBackupsRequest{
		ContainerId:      containerID,
		DatabaseNameMask: "%",
		DisplayStatus:    []pb.Backup_Status{pb.Backup_AVAILABLE},
	})
	if err != nil {
		log.Panicf("failed to list backups: %v", err)
	}

	if os.Getenv("ENABLE_CROSS_REGION_BACKUP_RESTORE") != "true" {
		if len(catalog.Backups) != 1 || catalog.Backups[0].Id != localBackupID {
			log.Panicf("expected only local backup, got %v", catalog.Backups)
		}
		_, err = backupClient.GetBackup(ctx, &pb.GetBackupRequest{Id: remoteBackupID})
		if status.Code(err) != codes.NotFound {
			log.Panicf("expected replicated backup to be hidden, got %v", err)
		}
		_, err = backupClient.MakeRestore(ctx, &pb.MakeRestoreRequest{
			ContainerId:      containerID,
			BackupId:         remoteBackupID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
		})
		if status.Code(err) != codes.NotFound {
			log.Panicf("expected replicated backup restore to be hidden, got %v", err)
		}
		return
	}

	if len(catalog.Backups) != 2 {
		log.Panicf("expected two matching backups, got %d", len(catalog.Backups))
	}
	backupsByID := make(map[string]*pb.Backup, len(catalog.Backups))
	for _, backup := range catalog.Backups {
		backupsByID[backup.Id] = backup
	}
	localBackup, remoteBackup := backupsByID[localBackupID], backupsByID[remoteBackupID]
	if localBackup == nil || localBackup.ScheduleId != localScheduleID {
		log.Panicf("local schedule ID was not preserved")
	}
	if remoteBackup == nil || remoteBackup.ScheduleId != "" {
		log.Panicf("replicated schedule ID was exposed")
	}
	if _, err = backupClient.GetBackup(ctx, &pb.GetBackupRequest{Id: remoteBackupID}); err != nil {
		log.Panicf("failed to get replicated backup: %v", err)
	}
}
