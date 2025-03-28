package metrics

import (
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"sync"
	"ydbcp/internal/types"
)

type MockMetricsRegistry struct {
	mutex   sync.Mutex
	metrics map[string]float64
	clock   clockwork.Clock
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

func (s *MockMetricsRegistry) IncCompletedBackupsCount(containerId string, database string, scheduleId *string, code Ydb.StatusIds_StatusCode) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if code == Ydb.StatusIds_SUCCESS {
		s.metrics["backups_succeeded_count"]++
	} else {
		s.metrics["backups_failed_count"]++
	}
}

func (s *MockMetricsRegistry) GetMetrics() map[string]float64 {
	return s.metrics
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

func (s *MockMetricsRegistry) IncScheduleCounters(schedule *types.BackupSchedule, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if err != nil {
		s.metrics["schedules_failed_count"]++
	} else {
		s.metrics["schedules_succeeded_count"]++
	}
	if schedule.RecoveryPoint != nil {
		s.metrics["schedules_last_backup_timestamp"] = float64(schedule.RecoveryPoint.Unix())
		s.metrics["schedules_elapsed_seconds_since_last_backup"] = s.clock.Since(*schedule.RecoveryPoint).Seconds()
		if schedule.ScheduleSettings.RecoveryPointObjective != nil {
			info := schedule.GetBackupInfo(s.clock)
			s.metrics["schedules_rpo_margin_ratio"] = info.LastBackupRpoMarginRatio
			s.metrics["schedules_rpo_duration_seconds"] = float64(schedule.ScheduleSettings.RecoveryPointObjective.Seconds)
		}
	} else if schedule.Audit != nil && schedule.Audit.CreatedAt != nil {
		s.metrics["schedules_last_backup_timestamp"] = float64(schedule.Audit.CreatedAt.AsTime().Unix())
		s.metrics["schedules_elapsed_seconds_since_last_backup"] = s.clock.Since(schedule.Audit.CreatedAt.AsTime()).Seconds()
		if schedule.ScheduleSettings.RecoveryPointObjective != nil {
			fakeRpoMargin := s.clock.Since(schedule.Audit.CreatedAt.AsTime())
			fakeLastBackupRpoMarginRatio := fakeRpoMargin.Seconds() / float64(schedule.ScheduleSettings.RecoveryPointObjective.Seconds)
			s.metrics["schedules_rpo_margin_ratio"] = fakeLastBackupRpoMarginRatio
			s.metrics["schedules_rpo_duration_seconds"] = float64(schedule.ScheduleSettings.RecoveryPointObjective.Seconds)
		}
	}
}

func (s *MockMetricsRegistry) ResetScheduleCounters(schedule *types.BackupSchedule) {
	delete(s.metrics, "schedules_last_backup_timestamp")
	delete(s.metrics, "schedules_elapsed_seconds_since_last_backup")
	delete(s.metrics, "schedules_rpo_margin_ratio")
	delete(s.metrics, "schedules_rpo_duration_seconds")
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
		metrics: make(map[string]float64),
		clock:   clockwork.NewFakeClock(),
	}

	for _, option := range options {
		option(mock)
	}

	return mock
}
