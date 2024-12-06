package handlers

import (
	"context"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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
		Status:           types.BackupScheduleStateActive,
		DatabaseName:     "mydb",
		DatabaseEndpoint: "mydb.valid.com",
		SourcePaths:      []string{"/path/to/table"},
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * * *"},
		},
		NextLaunch: &now,
	}
	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	scheduleMap := make(map[string]types.BackupSchedule)
	scheduleMap[schedule.ID] = schedule
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
		db.WithBackupSchedules(scheduleMap),
	)

	handler := NewBackupScheduleHandler(
		queries.NewWriteTableQueryMock, clock,
	)
	err := handler(ctx, dbConnector, &schedule)
	assert.Empty(t, err)

	// check operation status (should be running)
	ops, err := dbConnector.SelectOperations(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, ops)
	assert.Equal(t, len(ops), 1)
	assert.Equal(t, types.OperationTypeTBWR, ops[0].GetType())
	assert.Equal(t, types.OperationStateRunning, ops[0].GetState())

	// check backup status (should be empty)
	backups, err := dbConnector.SelectBackups(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.Empty(t, backups)

	// check schedule next launch
	schedules, err := dbConnector.SelectBackupSchedules(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, schedules)
	assert.Equal(t, len(schedules), 1)
	assert.Greater(t, *schedules[0].NextLaunch, now)
}
