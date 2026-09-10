package handlers

import (
	"context"
	"testing"
	"time"

	"ydbcp/internal/metrics"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"

	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"

	"ydbcp/internal/config"
	dbconnector "ydbcp/internal/connectors/db"
	"ydbcp/internal/types"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestBackupScheduleHandler(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
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
	dbConnector := dbconnector.NewMockDBConnector(
		dbconnector.WithBackups(backupMap),
		dbconnector.WithOperations(opMap),
		dbconnector.WithBackupSchedules(scheduleMap),
	)

	observed := xlog.SetupLoggingWithObserver()
	ctx = xlog.With(ctx, zap.String(log_keys.ScheduleID, schedule.ID))

	handler := NewBackupScheduleHandler(
		clock, config.FeatureFlagsConfig{},
	)
	err := handler(ctx, dbConnector, &schedule)
	assert.Empty(t, err)
	assert.Equal(t, len(observed.All()), len(observed.FilterField(zap.String(log_keys.ScheduleID, schedule.ID)).All()))

	// check operation status (should be running)
	ops, err := dbConnector.ListOperations(ctx, dbconnector.OperationFilter{})
	assert.Empty(t, err)
	assert.NotEmpty(t, ops)
	assert.Equal(t, len(ops), 1)
	assert.Equal(t, types.OperationTypeTBWR, ops[0].GetType())
	assert.Equal(t, types.OperationStateRunning, ops[0].GetState())

	// check backup status (should be empty)
	backups, err := dbConnector.ListBackups(ctx, dbconnector.BackupFilter{})
	assert.Empty(t, err)
	assert.Empty(t, backups)

	// check schedule next launch
	schedules, err := dbConnector.ListSchedules(ctx, dbconnector.ScheduleFilter{})
	assert.Empty(t, err)
	assert.NotEmpty(t, schedules)
	assert.Equal(t, len(schedules), 1)
	assert.Greater(t, *schedules[0].NextLaunch, now)
}
