package metrics

import (
	"github.com/jonboulle/clockwork"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"ydbcp/internal/types"
)

type MockMetricsRegistry struct {
	metrics map[string]float64
}

func (s *MockMetricsRegistry) IncOperationsStartedCounter(operation types.Operation) {
	s.metrics["operations_started_count"]++
}

func (s *MockMetricsRegistry) IncCompletedBackupsCount(containerId string, database string, code Ydb.StatusIds_StatusCode) {
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
	s.metrics["api_calls_count"]++
}

func (s *MockMetricsRegistry) IncBytesWrittenCounter(containerId string, bucket string, database string, bytes int64) {
	s.metrics["storage_bytes_written"] += float64(bytes)
}

func (s *MockMetricsRegistry) IncBytesDeletedCounter(containerId string, bucket string, database string, bytes int64) {
	s.metrics["storage_bytes_deleted"] += float64(bytes)
}

func (s *MockMetricsRegistry) ResetOperationsInflight() {
	s.metrics["operations_inflight"] = 0
}
func (s *MockMetricsRegistry) ReportOperationInflight(operation types.Operation) {
	s.metrics["operations_inflight"]++
}

func (s *MockMetricsRegistry) ReportOperationMetrics(operation types.Operation) {
	if !types.IsActive(operation) {
		s.metrics["operations_duration_seconds"]++
		s.metrics["operations_finished_count"]++
	}
}

func (s *MockMetricsRegistry) IncHandlerRunsCount(containerId string, operationType string) {
	s.metrics["operation_processor_handler_runs_count"]++
}

func (s *MockMetricsRegistry) IncFailedHandlerRunsCount(containerId string, operationType string) {
	s.metrics["operation_processor_handler_runs_failed_count"]++
}

func (s *MockMetricsRegistry) IncSuccessfulHandlerRunsCount(containerId string, operationType string) {
	s.metrics["operation_processor_handler_runs_successful_count"]++
}

func (s *MockMetricsRegistry) IncScheduledBackupsCount(schedule *types.BackupSchedule) {
	s.metrics["schedules_launched_take_backup_with_retry_count"]++
}

func (s *MockMetricsRegistry) IncScheduleCounters(schedule *types.BackupSchedule, clock clockwork.Clock, err error) {
	if err != nil {
		s.metrics["schedules_failed_count"]++
	} else {
		s.metrics["schedules_succeeded_count"]++
	}
	if schedule.RecoveryPoint != nil {
		s.metrics["schedules_last_backup_timestamp"] = float64(schedule.RecoveryPoint.Unix())
		if schedule.ScheduleSettings.RecoveryPointObjective != nil {
			info := schedule.GetBackupInfo(clock)
			s.metrics["schedules_recovery_point_objective"] = info.LastBackupRpoMarginRatio
		}
	}
}

func InitializeMockMetricsRegistry() {
	GlobalMetricsRegistry = newMockMetricsRegistry()
}

func GetMetrics() map[string]float64 {
	return GlobalMetricsRegistry.(*MockMetricsRegistry).GetMetrics()
}

func newMockMetricsRegistry() *MockMetricsRegistry {
	return &MockMetricsRegistry{
		metrics: make(map[string]float64),
	}
}
