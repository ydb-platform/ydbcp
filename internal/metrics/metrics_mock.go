package metrics

import (
	"context"
	"sync"
	"ydbcp/internal/types"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type MockMetricsRegistry struct {
	mutex           sync.Mutex
	metrics         map[string]float64
	scheduleMetrics map[mockScheduleLabels]map[string]float64
	clock           clockwork.Clock
}

type mockScheduleLabels struct {
	containerID  string
	database     string
	scheduleID   string
	scheduleName string
}

func getMockScheduleLabels(schedule *types.BackupSchedule) mockScheduleLabels {
	labels := mockScheduleLabels{
		containerID: schedule.ContainerID,
		database:    schedule.DatabaseName,
		scheduleID:  schedule.ID,
	}
	if schedule.Name != nil {
		labels.scheduleName = *schedule.Name
	}
	return labels
}

func (s *MockMetricsRegistry) GetReg() *prometheus.Registry {
	return nil
}

func (s *MockMetricsRegistry) ReportHealthCheck() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["healthcheck_gauge"] = 1
}

func (s *MockMetricsRegistry) IncYdbErrorsCounter() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["ydb_errors_counter"]++
}

func (s *MockMetricsRegistry) IncOperationsStartedCounter(operation types.Operation) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["operations_started_count"]++
}

func (s *MockMetricsRegistry) IncCompletedBackupsCount(
	containerId string, database string, scheduleId *string, code Ydb.StatusIds_StatusCode, isEncrypted bool,
) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if code == Ydb.StatusIds_SUCCESS {
		if isEncrypted {
			s.metrics["backups_succeeded_count_encrypted"]++
		} else {
			s.metrics["backups_succeeded_count_unencrypted"]++
		}
	} else {
		s.metrics["backups_failed_count"]++
	}
}

func (s *MockMetricsRegistry) ReportScheduleBackupsStatus(
	schedule *types.BackupSchedule, lastBackupEncrypted bool,
) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["backups_failed_count"] = 0
	s.metrics["backups_succeeded_count_encrypted"] = 0
	s.metrics["backups_succeeded_count_unencrypted"] = 0
	if schedule.LastBackupStatus == nil {
		return
	}
	switch *schedule.LastBackupStatus {
	case types.BackupStateError:
		s.metrics["backups_failed_count"] = 1
	case types.BackupStateAvailable:
		if lastBackupEncrypted {
			s.metrics["backups_succeeded_count_encrypted"] = 1
		} else {
			s.metrics["backups_succeeded_count_unencrypted"] = 1
		}
	}
}

func (s *MockMetricsRegistry) ReportLastBackupSize(
	containerId string, database string, scheduleId *string, sizeBytes int64,
) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["backups_last_size_bytes"] = float64(sizeBytes)
}

func (s *MockMetricsRegistry) GetMetrics() map[string]float64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	values := make(map[string]float64)
	for name, value := range s.metrics {
		values[name] = value
	}
	for _, scheduleMetrics := range s.scheduleMetrics {
		for name, value := range scheduleMetrics {
			values[name] += value
		}
	}
	return values
}

func (s *MockMetricsRegistry) IncApiCallsCounter(serviceName string, methodName string, status string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["api_calls_count"]++
}

func (s *MockMetricsRegistry) IncBytesWrittenCounter(containerId string, bucket string, database string, bytes int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["storage_bytes_written"] += float64(bytes)
}

func (s *MockMetricsRegistry) IncBytesDeletedCounter(containerId string, bucket string, database string, bytes int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["storage_bytes_deleted"] += float64(bytes)
}

func (s *MockMetricsRegistry) ResetOperationsInflight() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["operations_inflight"] = 0
	s.metrics["operations_inflight_duration_seconds"] = 0
}
func (s *MockMetricsRegistry) ReportOperationInflight(operation types.Operation) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["operations_inflight"]++
	s.metrics["operations_inflight_duration_seconds"]++
}

func (s *MockMetricsRegistry) ReportOperationMetrics(operation types.Operation) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if !types.IsActive(operation) {
		s.metrics["operations_duration_seconds"]++
		s.metrics["operations_finished_count"]++
	}
}

func (s *MockMetricsRegistry) IncHandlerRunsCount(containerId string, operationType string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["operation_processor_handler_runs_count"]++
}

func (s *MockMetricsRegistry) IncFailedHandlerRunsCount(containerId string, operationType string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["operation_processor_handler_runs_failed_count"]++
}

func (s *MockMetricsRegistry) IncSuccessfulHandlerRunsCount(containerId string, operationType string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["operation_processor_handler_runs_successful_count"]++
}

func (s *MockMetricsRegistry) IncScheduledBackupsCount(schedule *types.BackupSchedule) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.metrics["schedules_launched_take_backup_with_retry_count"]++
}

func (s *MockMetricsRegistry) IncScheduleCounters(
	_ context.Context,
	schedule *types.BackupSchedule,
	err error,
) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	labels := getMockScheduleLabels(schedule)
	values, ok := s.scheduleMetrics[labels]
	if !ok {
		values = make(map[string]float64)
		s.scheduleMetrics[labels] = values
	}
	if err != nil {
		values["schedules_failed_count"]++
	} else {
		values["schedules_succeeded_count"]++
	}
	if schedule.RecoveryPoint != nil {
		values["schedules_last_backup_timestamp"] = float64(schedule.RecoveryPoint.Unix())
		values["schedules_elapsed_seconds_since_last_backup"] = s.clock.Since(*schedule.RecoveryPoint).Seconds()
		if schedule.ScheduleSettings.RecoveryPointObjective != nil {
			info := schedule.GetBackupInfo(s.clock)
			values["schedules_rpo_margin_ratio"] = info.LastBackupRpoMarginRatio
			values["schedules_rpo_duration_seconds"] = float64(schedule.ScheduleSettings.RecoveryPointObjective.Seconds)
		}
	} else if schedule.Audit != nil && schedule.Audit.CreatedAt != nil {
		values["schedules_last_backup_timestamp"] = float64(schedule.Audit.CreatedAt.AsTime().Unix())
		values["schedules_elapsed_seconds_since_last_backup"] = s.clock.Since(schedule.Audit.CreatedAt.AsTime()).Seconds()
		if schedule.ScheduleSettings.RecoveryPointObjective != nil {
			fakeRpoMargin := s.clock.Since(schedule.Audit.CreatedAt.AsTime())
			fakeLastBackupRpoMarginRatio := fakeRpoMargin.Seconds() / float64(schedule.ScheduleSettings.RecoveryPointObjective.Seconds)
			values["schedules_rpo_margin_ratio"] = fakeLastBackupRpoMarginRatio
			values["schedules_rpo_duration_seconds"] = float64(schedule.ScheduleSettings.RecoveryPointObjective.Seconds)
		}
	}
}

func (s *MockMetricsRegistry) ResetRenamedScheduleCounters(schedule *types.BackupSchedule) {
	s.ResetScheduleCounters(schedule, true)
}

func (s *MockMetricsRegistry) ResetScheduleCounters(schedule *types.BackupSchedule, renamedOnly ...bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	values := s.scheduleMetrics[getMockScheduleLabels(schedule)]
	delete(values, "schedules_last_backup_timestamp")
	delete(values, "schedules_elapsed_seconds_since_last_backup")
	delete(values, "schedules_rpo_margin_ratio")
	delete(values, "schedules_rpo_duration_seconds")

	if len(renamedOnly) > 0 && renamedOnly[0] {
		return
	}

	delete(s.metrics, "backups_failed_count")
	delete(s.metrics, "backups_succeeded_count_encrypted")
	delete(s.metrics, "backups_succeeded_count_unencrypted")
	delete(s.metrics, "backups_last_size_bytes")
}

type Option func(*MockMetricsRegistry)

func WithClock(clock clockwork.Clock) Option {
	return func(s *MockMetricsRegistry) {
		s.clock = clock
	}
}

func InitializeMockMetricsRegistry(options ...Option) {
	GlobalMetricsRegistry = newMockMetricsRegistry(options...)
}

func GetMetrics() map[string]float64 {
	return GlobalMetricsRegistry.(*MockMetricsRegistry).GetMetrics()
}

func newMockMetricsRegistry(options ...Option) *MockMetricsRegistry {
	mock := &MockMetricsRegistry{
		metrics:         make(map[string]float64),
		scheduleMetrics: make(map[mockScheduleLabels]map[string]float64),
		clock:           clockwork.NewFakeClock(),
	}

	for _, option := range options {
		option(mock)
	}

	return mock
}
