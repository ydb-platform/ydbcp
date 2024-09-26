package handlers

import (
	"context"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"testing"
	"time"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

func TestBackupScheduleHandler(t *testing.T) {
	ctx := context.Background()
	clock := clockwork.NewFakeClockAt(time.Now())
	now := clock.Now()
	clock.Advance(time.Second)
	schedule := types.BackupSchedule{
		ID:               "12345",
		ContainerID:      "abcde",
		Active:           true,
		DatabaseName:     "mydb",
		DatabaseEndpoint: "mydb.valid.com",
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
		},
		NextLaunch: &now,
	}
	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	scheduleMap := make(map[string]types.BackupSchedule)
	scheduleMap[schedule.ID] = schedule
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
		db.WithBackupSchedules(scheduleMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := NewBackupScheduleHandler(
		clientConnector,
		config.S3Config{
			S3ForcePathStyle: false,
			IsMock:           true,
		},
		config.ClientConnectionConfig{
			AllowedEndpointDomains: []string{".valid.com"},
			AllowInsecureEndpoint:  true,
		},
		queries.NewWriteTableQueryMock,
	)
	err := handler(ctx, dbConnector, schedule, clock.Now())
	assert.Empty(t, err)

	// check operation status (should be pending)
	ops, err := dbConnector.SelectOperations(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, ops)
	assert.Equal(t, len(ops), 1)
	assert.Equal(t, types.OperationStateRunning, ops[0].GetState())

	// check backup status (should be running)
	backups, err := dbConnector.SelectBackups(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, backups)
	assert.Equal(t, len(backups), 1)
	assert.Equal(t, types.BackupStateRunning, backups[0].Status)

	// check schedule next launch
	schedules, err := dbConnector.SelectBackupSchedules(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, schedules)
	assert.Equal(t, len(schedules), 1)
	assert.Greater(t, *schedules[0].NextLaunch, now)
}
