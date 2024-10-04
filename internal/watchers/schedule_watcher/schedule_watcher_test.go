package schedule_watcher

import (
	"context"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"sync"
	"testing"
	"time"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/handlers"
	"ydbcp/internal/types"
	"ydbcp/internal/util/ticker"
	"ydbcp/internal/watchers"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

var (
	fourPM = time.Date(2024, 01, 01, 16, 0, 0, 0, time.UTC)
)

func TestScheduleWatcherSimple(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prepare fake ticker
	clock := clockwork.NewFakeClockAt(fourPM)
	var fakeTicker *ticker.FakeTicker
	tickerInitialized := make(chan struct{})
	tickerProvider := func(duration time.Duration) ticker.Ticker {
		assert.Empty(t, fakeTicker, "ticker reuse")
		fakeTicker = ticker.NewFakeTicker(duration)
		tickerInitialized <- struct{}{}
		return fakeTicker
	}
	now := clock.Now()
	clock.Advance(time.Second)
	schedule := types.BackupSchedule{
		ID:               "12345",
		ContainerID:      "abcde",
		Status:           types.BackupScheduleStateActive,
		DatabaseName:     "mydb",
		DatabaseEndpoint: "mydb.valid.com",
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * *"}, //every minute
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

	handler := handlers.NewBackupScheduleHandler(
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

	_ = NewScheduleWatcher(
		ctx,
		&wg,
		clock,
		dbConnector,
		handler,
		watchers.WithTickerProvider(tickerProvider),
	)

	// Wait for the ticker to be initialized
	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, time.Minute, "incorrect period")
	}

	fakeTicker.Send(clock.Now())
	cancel()
	wg.Wait()

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
	assert.Equal(t, schedule.ID, *backups[0].ScheduleID)

	// check schedule next launch
	schedules, err := dbConnector.SelectBackupSchedules(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, schedules)
	assert.Equal(t, len(schedules), 1)
	assert.Equal(t, *schedules[0].NextLaunch, now.Add(time.Minute))
}

func TestScheduleWatcherTwoSchedulesOneBackup(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prepare fake ticker
	clock := clockwork.NewFakeClockAt(fourPM)
	var fakeTicker *ticker.FakeTicker
	tickerInitialized := make(chan struct{})
	tickerProvider := func(duration time.Duration) ticker.Ticker {
		assert.Empty(t, fakeTicker, "ticker reuse")
		fakeTicker = ticker.NewFakeTicker(duration)
		tickerInitialized <- struct{}{}
		return fakeTicker
	}
	now := clock.Now()
	clock.Advance(time.Second)
	s1 := types.BackupSchedule{
		ID:               "1",
		ContainerID:      "abcde",
		Status:           types.BackupScheduleStateActive,
		DatabaseName:     "mydb",
		DatabaseEndpoint: "mydb.valid.com",
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * *"}, //every minute
		},
		NextLaunch: &now,
	}
	nextLaunch := now.Add(time.Hour)
	s2 := types.BackupSchedule{
		ID:               "2",
		ContainerID:      "abcde",
		Status:           types.BackupScheduleStateActive,
		DatabaseName:     "mydb",
		DatabaseEndpoint: "mydb.valid.com",
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * *"}, //every minute
		},
		NextLaunch: &nextLaunch,
	}
	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	scheduleMap := make(map[string]types.BackupSchedule)
	scheduleMap[s1.ID] = s1
	scheduleMap[s2.ID] = s2
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
		db.WithBackupSchedules(scheduleMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := handlers.NewBackupScheduleHandler(
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

	_ = NewScheduleWatcher(
		ctx,
		&wg,
		clock,
		dbConnector,
		handler,
		watchers.WithTickerProvider(tickerProvider),
	)

	// Wait for the ticker to be initialized
	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, time.Minute, "incorrect period")
	}

	fakeTicker.Send(clock.Now())
	cancel()
	wg.Wait()

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
	assert.Equal(t, s1.ID, *backups[0].ScheduleID)

	m := map[string]time.Time{
		"1": now.Add(time.Minute),
		"2": nextLaunch,
	}

	// check schedule next launch
	schedules, err := dbConnector.SelectBackupSchedules(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, schedules)
	assert.Equal(t, len(schedules), 2)
	assert.Equal(t, *schedules[0].NextLaunch, m[schedules[0].ID])
	assert.Equal(t, *schedules[1].NextLaunch, m[schedules[1].ID])
}

func TestScheduleWatcherTwoBackups(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prepare fake ticker
	clock := clockwork.NewFakeClockAt(fourPM)
	var fakeTicker *ticker.FakeTicker
	tickerInitialized := make(chan struct{})
	tickerProvider := func(duration time.Duration) ticker.Ticker {
		assert.Empty(t, fakeTicker, "ticker reuse")
		fakeTicker = ticker.NewFakeTicker(duration)
		tickerInitialized <- struct{}{}
		return fakeTicker
	}
	now := clock.Now()
	clock.Advance(time.Hour)
	s1 := types.BackupSchedule{
		ID:               "1",
		ContainerID:      "abcde",
		Status:           types.BackupScheduleStateActive,
		DatabaseName:     "mydb",
		DatabaseEndpoint: "mydb.valid.com",
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * *"}, //every minute
		},
		NextLaunch: &now,
	}
	sourcePath := []string{"mydb/path1"}
	s2 := types.BackupSchedule{
		ID:               "2",
		ContainerID:      "abcde",
		DatabaseName:     "mydb",
		DatabaseEndpoint: "mydb.valid.com",
		SourcePaths:      sourcePath,
		Status:           types.BackupScheduleStateActive,
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "0 * * * *"}, //every hour
		},
		NextLaunch: &now,
	}
	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	scheduleMap := make(map[string]types.BackupSchedule)
	scheduleMap[s1.ID] = s1
	scheduleMap[s2.ID] = s2
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
		db.WithBackupSchedules(scheduleMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := handlers.NewBackupScheduleHandler(
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

	_ = NewScheduleWatcher(
		ctx,
		&wg,
		clock,
		dbConnector,
		handler,
		watchers.WithTickerProvider(tickerProvider),
	)

	// Wait for the ticker to be initialized
	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, time.Minute, "incorrect period")
	}

	fakeTicker.Send(clock.Now())
	cancel()
	wg.Wait()

	// check operation status (should be pending)
	ops, err := dbConnector.SelectOperations(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, ops)
	assert.Equal(t, len(ops), 2)
	for _, op := range ops {
		assert.Equal(t, types.OperationStateRunning, op.GetState())
		assert.Equal(t, types.OperationTypeTB, op.GetType())
	}

	// check backup status (should be running)
	backups, err := dbConnector.SelectBackups(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, backups)
	assert.Equal(t, len(backups), 2)
	assert.Equal(t, types.BackupStateRunning, backups[0].Status)
	assert.Equal(t, types.BackupStateRunning, backups[1].Status)

	m := map[string]time.Time{
		"1": now.Add(time.Minute * 61),
		"2": now.Add(time.Hour * 2),
	}

	// check schedule next launch
	schedules, err := dbConnector.SelectBackupSchedules(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, schedules)
	assert.Equal(t, len(schedules), 2)
	assert.Equal(t, *schedules[0].NextLaunch, m[schedules[0].ID])
	assert.Equal(t, *schedules[1].NextLaunch, m[schedules[1].ID])
}
