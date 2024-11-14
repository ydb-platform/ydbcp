package metrics

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"ydbcp/internal/types"
)

type MockMetricsRegistry struct {
	metrics map[string]int64
}

func (s *MockMetricsRegistry) GetMetrics() map[string]int64 {
	return s.metrics
}

func (s *MockMetricsRegistry) IncApiCallsCounter(serviceName string, methodName string, status string) {
	s.metrics["api_calls_count"]++
}

func (s *MockMetricsRegistry) IncBytesWrittenCounter(containerId string, bucket string, database string, bytes int64) {
	s.metrics["storage_bytes_written"] += bytes
}

func (s *MockMetricsRegistry) IncBytesDeletedCounter(containerId string, bucket string, database string, bytes int64) {
	s.metrics["storage_bytes_deleted"] += bytes
}

func (s *MockMetricsRegistry) ObserveOperationDuration(operation types.Operation) {
	s.metrics["operations_duration_seconds"]++
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

func (s *MockMetricsRegistry) IncCompletedBackupsCount(containerId string, database string, code Ydb.StatusIds_StatusCode) {
	if code == Ydb.StatusIds_SUCCESS {
		s.metrics["backups_succeeded_count"]++
	} else {
		s.metrics["backups_failed_count"]++
	}
}

func NewMockMetricsRegistry() *MockMetricsRegistry {
	return &MockMetricsRegistry{
		metrics: make(map[string]int64),
	}
}
