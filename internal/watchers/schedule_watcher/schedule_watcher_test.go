package schedule_watcher

import (
	"context"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync"
	"testing"
	"time"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/handlers"
	"ydbcp/internal/metrics"
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
		SourcePaths:      []string{"/path/to/table"},
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * *"}, //every minute
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

	metrics.InitializeMockMetricsRegistry(metrics.WithClock(clock))

	handler := handlers.NewBackupScheduleHandler(
		queries.NewWriteTableQueryMock, clock,
	)

	scheduleWatcherActionCompleted := make(chan struct{})
	_ = NewScheduleWatcher(
		ctx,
		&wg,
		dbConnector,
		handler,
		clock,
		watchers.WithTickerProvider(tickerProvider),
		watchers.WithActionCompletedChannel(&scheduleWatcherActionCompleted),
	)

	// Wait for the ticker to be initialized
	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, time.Minute, "incorrect period")
	}

	fakeTicker.Send(clock.Now())

	// Wait for the watcher action to be completed
	select {
	case <-ctx.Done():
		t.Error("action wasn't completed")
	case <-scheduleWatcherActionCompleted:
		cancel()
	}

	wg.Wait()

	// check operation status (should be pending)
	ops, err := dbConnector.SelectOperations(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, ops)
	assert.Equal(t, len(ops), 1)
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
	assert.Equal(t, *schedules[0].NextLaunch, now.Add(time.Minute))

	assert.Equal(t, float64(1), metrics.GetMetrics()["schedules_succeeded_count"])
	assert.Equal(t, float64(1), metrics.GetMetrics()["operations_started_count"])
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
		SourcePaths:      []string{"/path/to/table"},
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
		SourcePaths:      []string{"/path/to/table"},
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * *"}, //every minute
		},
		NextLaunch: &nextLaunch,
	}
	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	scheduleMap := make(map[string]types.BackupSchedule)
	scheduleMap[s1.ID] = s1
	scheduleMap[s2.ID] = s2
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
		db.WithBackupSchedules(scheduleMap),
	)

	metrics.InitializeMockMetricsRegistry(metrics.WithClock(clock))
	handler := handlers.NewBackupScheduleHandler(
		queries.NewWriteTableQueryMock, clock,
	)

	scheduleWatcherActionCompleted := make(chan struct{})
	_ = NewScheduleWatcher(
		ctx,
		&wg,
		dbConnector,
		handler,
		clock,
		watchers.WithTickerProvider(tickerProvider),
		watchers.WithActionCompletedChannel(&scheduleWatcherActionCompleted),
	)

	// Wait for the ticker to be initialized
	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, time.Minute, "incorrect period")
	}

	fakeTicker.Send(clock.Now())

	// Wait for the watcher action to be completed
	select {
	case <-ctx.Done():
		t.Error("action wasn't completed")
	case <-scheduleWatcherActionCompleted:
		cancel()
	}

	wg.Wait()

	// check operation status (should be pending)
	ops, err := dbConnector.SelectOperations(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, ops)
	assert.Equal(t, len(ops), 1)
	assert.Equal(t, types.OperationStateRunning, ops[0].GetState())
	assert.Equal(t, types.OperationTypeTBWR, ops[0].GetType())
	assert.Equal(t, s1.ID, *ops[0].(*types.TakeBackupWithRetryOperation).ScheduleID)

	// check backup status (should be empty)
	backups, err := dbConnector.SelectBackups(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.Empty(t, backups)

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
	assert.Equal(t, float64(2), metrics.GetMetrics()["schedules_succeeded_count"])
	assert.Equal(t, float64(1), metrics.GetMetrics()["operations_started_count"])
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
		SourcePaths:      []string{"/path/to/table"},
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
	scheduleMap := make(map[string]types.BackupSchedule)
	scheduleMap[s1.ID] = s1
	scheduleMap[s2.ID] = s2
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
		db.WithBackupSchedules(scheduleMap),
	)

	metrics.InitializeMockMetricsRegistry(metrics.WithClock(clock))

	handler := handlers.NewBackupScheduleHandler(
		queries.NewWriteTableQueryMock, clock,
	)

	scheduleWatcherActionCompleted := make(chan struct{})
	_ = NewScheduleWatcher(
		ctx,
		&wg,
		dbConnector,
		handler,
		clock,
		watchers.WithTickerProvider(tickerProvider),
		watchers.WithActionCompletedChannel(&scheduleWatcherActionCompleted),
	)

	// Wait for the ticker to be initialized
	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, time.Minute, "incorrect period")
	}

	fakeTicker.Send(clock.Now())

	// Wait for the watcher action to be completed
	select {
	case <-ctx.Done():
		t.Error("action wasn't completed")
	case <-scheduleWatcherActionCompleted:
		cancel()
	}

	wg.Wait()

	m := map[string]time.Time{
		"1": now.Add(time.Minute * 61),
		"2": now.Add(time.Hour * 2),
	}

	// check operation status (should be pending)
	ops, err := dbConnector.SelectOperations(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, ops)
	assert.Equal(t, len(ops), 2)
	for _, op := range ops {
		assert.Equal(t, types.OperationStateRunning, op.GetState())
		assert.Equal(t, types.OperationTypeTBWR, op.GetType())
		_, ok := m[*op.(*types.TakeBackupWithRetryOperation).ScheduleID]
		assert.True(t, ok)
	}

	// check backup status (should be none)
	backups, err := dbConnector.SelectBackups(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.Empty(t, backups)

	// check schedule next launch
	schedules, err := dbConnector.SelectBackupSchedules(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, schedules)
	assert.Equal(t, len(schedules), 2)
	assert.Equal(t, *schedules[0].NextLaunch, m[schedules[0].ID])
	assert.Equal(t, *schedules[1].NextLaunch, m[schedules[1].ID])
	assert.Equal(t, float64(2), metrics.GetMetrics()["schedules_succeeded_count"])
	assert.Equal(t, float64(2), metrics.GetMetrics()["operations_started_count"])
}

func TestAllScheduleMetrics(t *testing.T) {
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

	backupID := "1"
	rp := fourPM.Add(time.Minute * 30)
	nl := fourPM.Add(time.Hour)
	clock.Advance(time.Hour + time.Minute)
	s1 := types.BackupSchedule{
		ID:               "1",
		ContainerID:      "abcde",
		Status:           types.BackupScheduleStateActive,
		DatabaseName:     "mydb",
		DatabaseEndpoint: "mydb.valid.com",
		SourcePaths:      []string{"/path/to/table"},
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern:        &pb.BackupSchedulePattern{Crontab: "* * * * *"}, //every minute
			RecoveryPointObjective: durationpb.New(time.Hour),
		},
		RecoveryPoint:          &rp,
		LastSuccessfulBackupID: &backupID,
		NextLaunch:             &nl,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	scheduleMap := make(map[string]types.BackupSchedule)
	scheduleMap[s1.ID] = s1
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
		db.WithBackupSchedules(scheduleMap),
	)

	metrics.InitializeMockMetricsRegistry(metrics.WithClock(clock))

	handler := handlers.NewBackupScheduleHandler(
		queries.NewWriteTableQueryMock, clock,
	)

	scheduleWatcherActionCompleted := make(chan struct{})
	_ = NewScheduleWatcher(
		ctx,
		&wg,
		dbConnector,
		handler,
		clock,
		watchers.WithTickerProvider(tickerProvider),
		watchers.WithActionCompletedChannel(&scheduleWatcherActionCompleted),
	)

	// Wait for the ticker to be initialized
	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, time.Minute, "incorrect period")
	}

	fakeTicker.Send(clock.Now())

	// Wait for the watcher action to be completed
	select {
	case <-ctx.Done():
		t.Error("action wasn't completed")
	case <-scheduleWatcherActionCompleted:
		cancel()
	}

	wg.Wait()

	m := map[string]time.Time{
		"1": clock.Now().Add(time.Minute),
	}

	// check operation status (should be pending)
	ops, err := dbConnector.SelectOperations(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, ops)
	assert.Equal(t, len(ops), 1)
	for _, op := range ops {
		assert.Equal(t, types.OperationStateRunning, op.GetState())
		assert.Equal(t, types.OperationTypeTBWR, op.GetType())
		_, ok := m[*op.(*types.TakeBackupWithRetryOperation).ScheduleID]
		assert.True(t, ok)
	}

	// check schedule next launch
	schedules, err := dbConnector.SelectBackupSchedules(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, schedules)
	assert.Equal(t, len(schedules), 1)
	assert.Equal(t, m[schedules[0].ID], *schedules[0].NextLaunch)
	assert.Equal(t, float64(1), metrics.GetMetrics()["schedules_succeeded_count"])
	assert.Equal(t, float64(1), metrics.GetMetrics()["operations_started_count"])
	assert.Equal(t, float64(schedules[0].RecoveryPoint.Unix()), metrics.GetMetrics()["schedules_last_backup_timestamp"])
	assert.Equal(t, 0.5166666666666667, metrics.GetMetrics()["schedules_rpo_margin_ratio"])
}

func TestAllScheduleMetricsBeforeFirstBackup(t *testing.T) {
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

	nl := fourPM.Add(time.Hour)
	clock.Advance(time.Hour + time.Minute)
	s1 := types.BackupSchedule{
		ID:               "1",
		ContainerID:      "abcde",
		Status:           types.BackupScheduleStateActive,
		DatabaseName:     "mydb",
		DatabaseEndpoint: "mydb.valid.com",
		SourcePaths:      []string{"/path/to/table"},
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern:        &pb.BackupSchedulePattern{Crontab: "* * * * *"}, //every minute
			RecoveryPointObjective: durationpb.New(time.Hour),
		},
		NextLaunch: &nl,
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.New(fourPM.Add(time.Minute * 30)),
		},
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	scheduleMap := make(map[string]types.BackupSchedule)
	scheduleMap[s1.ID] = s1
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
		db.WithBackupSchedules(scheduleMap),
	)

	metrics.InitializeMockMetricsRegistry(metrics.WithClock(clock))

	handler := handlers.NewBackupScheduleHandler(
		queries.NewWriteTableQueryMock, clock,
	)

	scheduleWatcherActionCompleted := make(chan struct{})
	_ = NewScheduleWatcher(
		ctx,
		&wg,
		dbConnector,
		handler,
		clock,
		watchers.WithTickerProvider(tickerProvider),
		watchers.WithActionCompletedChannel(&scheduleWatcherActionCompleted),
	)

	// Wait for the ticker to be initialized
	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, time.Minute, "incorrect period")
	}

	fakeTicker.Send(clock.Now())

	// Wait for the watcher action to be completed
	select {
	case <-ctx.Done():
		t.Error("action wasn't completed")
	case <-scheduleWatcherActionCompleted:
		cancel()
	}

	wg.Wait()

	m := map[string]time.Time{
		"1": clock.Now().Add(time.Minute),
	}

	// check operation status (should be pending)
	ops, err := dbConnector.SelectOperations(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, ops)
	assert.Equal(t, len(ops), 1)
	for _, op := range ops {
		assert.Equal(t, types.OperationStateRunning, op.GetState())
		assert.Equal(t, types.OperationTypeTBWR, op.GetType())
		_, ok := m[*op.(*types.TakeBackupWithRetryOperation).ScheduleID]
		assert.True(t, ok)
	}

	// check schedule next launch
	schedules, err := dbConnector.SelectBackupSchedules(ctx, &queries.ReadTableQueryImpl{})
	assert.Empty(t, err)
	assert.NotEmpty(t, schedules)
	assert.Equal(t, len(schedules), 1)
	assert.Equal(t, m[schedules[0].ID], *schedules[0].NextLaunch)
	assert.Equal(t, float64(1), metrics.GetMetrics()["schedules_succeeded_count"])
	assert.Equal(t, float64(1), metrics.GetMetrics()["operations_started_count"])
	assert.Equal(t, float64(schedules[0].Audit.CreatedAt.AsTime().Unix()), metrics.GetMetrics()["schedules_last_backup_timestamp"])
	assert.Equal(t, 0.5166666666666667, metrics.GetMetrics()["schedules_rpo_margin_ratio"])
}
