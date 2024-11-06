package metrics

import (
	"context"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"net/http"
	"sync"
	"time"
	"ydbcp/internal/types"

	"ydbcp/internal/config"
	"ydbcp/internal/util/xlog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type MetricsRegistry interface {
	IncApiCallsCounter(serviceName string, methodName string, status string)
	IncBytesWrittenCounter(containerId string, bucket string, database string, bytes int64)
	IncBytesDeletedCounter(containerId string, bucket string, database string, bytes int64)
	ObserveOperationDuration(operation types.Operation)
	IncHandlerRunsCount(containerId string, operationType string)
	IncFailedHandlerRunsCount(containerId string, operationType string)
	IncSuccessfulHandlerRunsCount(containerId string, operationType string)
	IncCompletedBackupsCount(containerId string, database string, code Ydb.StatusIds_StatusCode)
}

type MetricsRegistryImpl struct {
	server *http.Server
	reg    *prometheus.Registry
	cfg    config.MetricsServerConfig

	// api metrics
	apiCallsCounter *prometheus.CounterVec

	// storage metrics
	bytesWrittenCounter *prometheus.CounterVec
	bytesDeletedCounter *prometheus.CounterVec

	// operation metrics
	operationsDuration *prometheus.HistogramVec

	// operation processor metrics
	handlerRunsCount       *prometheus.CounterVec
	handlerFailedCount     *prometheus.CounterVec
	handlerSuccessfulCount *prometheus.CounterVec

	// backup metrics
	backupsFailedCount    *prometheus.CounterVec
	backupsSucceededCount *prometheus.CounterVec
}

func (s *MetricsRegistryImpl) IncApiCallsCounter(serviceName string, methodName string, code string) {
	s.apiCallsCounter.WithLabelValues(serviceName, methodName, code).Inc()
}

func (s *MetricsRegistryImpl) IncBytesWrittenCounter(containerId string, bucket string, database string, bytes int64) {
	s.bytesWrittenCounter.WithLabelValues(containerId, bucket, database).Add(float64(bytes))
}

func (s *MetricsRegistryImpl) IncBytesDeletedCounter(containerId string, bucket string, database string, bytes int64) {
	s.bytesDeletedCounter.WithLabelValues(containerId, bucket, database).Add(float64(bytes))
}

func (s *MetricsRegistryImpl) ObserveOperationDuration(operation types.Operation) {
	if operation.GetAudit() != nil && operation.GetAudit().CompletedAt != nil {
		duration := operation.GetAudit().CompletedAt.AsTime().Sub(operation.GetAudit().CreatedAt.AsTime())
		s.operationsDuration.WithLabelValues(
			operation.GetContainerID(),
			operation.GetType().String(),
			operation.GetState().String(),
		).Observe(duration.Seconds())
	}
}

func (s *MetricsRegistryImpl) IncHandlerRunsCount(containerId string, operationType string) {
	s.handlerRunsCount.WithLabelValues(containerId, operationType).Inc()
}

func (s *MetricsRegistryImpl) IncFailedHandlerRunsCount(containerId string, operationType string) {
	s.handlerFailedCount.WithLabelValues(containerId, operationType).Inc()
}

func (s *MetricsRegistryImpl) IncSuccessfulHandlerRunsCount(containerId string, operationType string) {
	s.handlerSuccessfulCount.WithLabelValues(containerId, operationType).Inc()
}

func (s *MetricsRegistryImpl) IncCompletedBackupsCount(containerId string, database string, code Ydb.StatusIds_StatusCode) {
	if code == Ydb.StatusIds_SUCCESS {
		s.backupsSucceededCount.WithLabelValues(containerId, database).Inc()
	} else {
		s.backupsFailedCount.WithLabelValues(containerId, database, code.String()).Inc()
	}
}

func NewMetricsRegistry(ctx context.Context, wg *sync.WaitGroup, cfg *config.MetricsServerConfig) *MetricsRegistryImpl {
	s := &MetricsRegistryImpl{
		reg: prometheus.NewRegistry(),
		cfg: *cfg,
	}

	s.apiCallsCounter = promauto.With(s.reg).NewCounterVec(prometheus.CounterOpts{
		Subsystem: "api",
		Name:      "calls_count",
		Help:      "Total count of API calls",
	}, []string{"service", "method", "status"})

	s.bytesWrittenCounter = promauto.With(s.reg).NewCounterVec(prometheus.CounterOpts{
		Subsystem: "storage",
		Name:      "bytes_written",
		Help:      "Count of bytes written to storage",
	}, []string{"container_id", "bucket", "database"})

	s.bytesDeletedCounter = promauto.With(s.reg).NewCounterVec(prometheus.CounterOpts{
		Subsystem: "storage",
		Name:      "bytes_deleted",
		Help:      "Count of bytes deleted from storage",
	}, []string{"container_id", "bucket", "database"})

	s.operationsDuration = promauto.With(s.reg).NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "operations",
		Name:      "duration_seconds",
		Help:      "Duration of operations in seconds",
		Buckets:   prometheus.ExponentialBuckets(10, 2, 8),
	}, []string{"container_id", "type", "status"})

	s.handlerRunsCount = promauto.With(s.reg).NewCounterVec(prometheus.CounterOpts{
		Subsystem: "operation_processor",
		Name:      "handler_runs_count",
		Help:      "Total count of operation handler runs",
	}, []string{"container_id", "operation_type"})

	s.handlerFailedCount = promauto.With(s.reg).NewCounterVec(prometheus.CounterOpts{
		Subsystem: "operation_processor",
		Name:      "handler_runs_failed_count",
		Help:      "Total count of failed operation handler runs",
	}, []string{"container_id", "operation_type"})

	s.handlerSuccessfulCount = promauto.With(s.reg).NewCounterVec(prometheus.CounterOpts{
		Subsystem: "operation_processor",
		Name:      "handler_runs_successful_count",
		Help:      "Total count of successful operation handler runs",
	}, []string{"container_id", "operation_type"})

	s.backupsFailedCount = promauto.With(s.reg).NewCounterVec(prometheus.CounterOpts{
		Subsystem: "backups",
		Name:      "failed_count",
		Help:      "Total count of failed backups",
	}, []string{"container_id", "database", "reason"})

	s.backupsSucceededCount = promauto.With(s.reg).NewCounterVec(prometheus.CounterOpts{
		Subsystem: "backups",
		Name:      "succeeded_count",
		Help:      "Total count of successful backups",
	}, []string{"container_id", "database"})

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{Registry: s.reg}))

	s.server = &http.Server{
		Addr:     fmt.Sprintf("%s:%d", s.cfg.BindAddress, s.cfg.BindPort),
		Handler:  mux,
		ErrorLog: zap.NewStdLog(xlog.Logger(ctx)),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		xlog.Info(ctx, "Starting metrics server", zap.String("address", s.server.Addr))
		var err error
		if len(s.cfg.TLSCertificatePath) > 0 && len(s.cfg.TLSKeyPath) > 0 {
			err = s.server.ListenAndServeTLS(s.cfg.TLSCertificatePath, s.cfg.TLSKeyPath)
		} else {
			err = s.server.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			xlog.Fatal(ctx, "metrics server failed to serve ", zap.Error(err))
		}
		xlog.Info(ctx, "metrics server stopped")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			xlog.Error(ctx, "metrics server shutdown error", zap.Error(err))
		}
	}()
	return s
}
