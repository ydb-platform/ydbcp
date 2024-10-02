package processor

import (
	"context"
	"sync"
	"time"

	"ydbcp/internal/connectors/db"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
	"ydbcp/internal/util/ticker"
	"ydbcp/internal/util/xlog"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	metricsSubsystem = "operation_processor"
)

type OperationProcessorImpl struct {
	ctx                    context.Context
	workersWaitGroup       sync.WaitGroup
	period                 time.Duration
	handleOperationTimeout time.Duration

	tickerProvider ticker.TickerProvider
	handlers       OperationHandlerRegistry
	db             db.DBConnector

	runningOperations map[string]bool
	results           chan string

	runsCount       *prometheus.CounterVec
	failedCount     *prometheus.CounterVec
	successfulCount *prometheus.CounterVec
}

type Option func(*OperationProcessorImpl)

func WithPeriod(period time.Duration) Option {
	return func(o *OperationProcessorImpl) {
		o.period = period
	}
}
func WithTickerProvider(ticketProvider ticker.TickerProvider) Option {
	return func(o *OperationProcessorImpl) {
		o.tickerProvider = ticketProvider
	}
}
func WithHandleOperationTimeout(timeout time.Duration) Option {
	return func(o *OperationProcessorImpl) {
		o.handleOperationTimeout = timeout
	}
}

func NewOperationProcessor(
	ctx context.Context,
	wg *sync.WaitGroup,
	db db.DBConnector,
	handlers OperationHandlerRegistry,
	mon metrics.MetricsRegistry,
	options ...Option,
) *OperationProcessorImpl {
	op := &OperationProcessorImpl{
		ctx:                    ctx,
		period:                 time.Second * 10,
		handleOperationTimeout: time.Second * 600,
		handlers:               handlers,
		db:                     db,
		tickerProvider:         ticker.NewRealTicker,
		runningOperations:      make(map[string]bool),
		results:                make(chan string),

		runsCount: mon.Factory().NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricsSubsystem,
			Name:      "operations_runs_count",
			Help:      "Total count of runs of the operation",
		}, []string{"task_type"}),
		failedCount: mon.Factory().NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricsSubsystem,
			Name:      "operations_failed_count",
			Help:      "Total count of failed operations",
		}, []string{"task_type"}),
		successfulCount: mon.Factory().NewCounterVec(prometheus.CounterOpts{
			Subsystem: metricsSubsystem,
			Name:      "operations_successful_count",
			Help:      "Total count of successful operations",
		}, []string{"task_type"}),
	}
	for _, opt := range options {
		opt(op)
	}
	wg.Add(1)
	go op.run(wg)
	return op
}

func (o *OperationProcessorImpl) run(wg *sync.WaitGroup) {
	defer wg.Done()
	xlog.Debug(o.ctx, "Operation Processor started", zap.Duration("period", o.period))
	ticker := o.tickerProvider(o.period)
	for {
		select {
		case <-o.ctx.Done():
			xlog.Debug(o.ctx, "Operation Processor stopping. Waiting for running operations...")
			ticker.Stop()
			o.finishing()
			xlog.Debug(o.ctx, "Operation Processor stopped")
			return
		case <-ticker.Chan():
			o.processOperations()
		case operationID := <-o.results:
			o.handleOperationResult(operationID)
		}
	}
}

func (o *OperationProcessorImpl) finishing() {
	waitCh := make(chan struct{})
	go func() {
		o.workersWaitGroup.Wait()
		close(waitCh)
	}()

	for {
		select {
		case <-waitCh:
			return
		case operationID := <-o.results:
			o.handleOperationResult(operationID)
		}
	}
}

func (o *OperationProcessorImpl) processOperations() {
	ctx, cancel := context.WithTimeout(o.ctx, o.period)
	defer cancel()
	operations, err := o.db.ActiveOperations(ctx)
	if err != nil {
		xlog.Error(ctx, "cannot get Active Operations", zap.Error(err))
		return
	}
	for _, op := range operations {
		o.processOperation(op)
	}
}

func (o *OperationProcessorImpl) processOperation(op types.Operation) {
	runID := uuid.New().String()
	ctx := xlog.With(
		o.ctx,
		zap.String("RunID", runID),
		zap.String("OperationID", op.GetID()),
		zap.String("OperationType", op.GetType().String()),
		zap.String("OperationState", op.GetState().String()),
	)
	if _, exist := o.runningOperations[op.GetID()]; exist {
		xlog.Debug(
			ctx,
			"operation already running",
			zap.String("operation", types.OperationToString(op)),
		)
		return
	}
	o.runsCount.WithLabelValues(op.GetType().String()).Inc()
	o.runningOperations[op.GetID()] = true
	o.workersWaitGroup.Add(1)
	go func() {
		defer o.workersWaitGroup.Done()
		ctx, cancel := context.WithTimeout(ctx, o.handleOperationTimeout)
		defer cancel()
		xlog.Debug(
			ctx, "start operation handler",
			zap.String("operation", types.OperationToString(op)),
		)
		err := o.handlers.Call(ctx, op)
		if err != nil {
			xlog.Error(
				ctx, "operation handler failed",
				zap.String("operation", types.OperationToString(op)),
				zap.Error(err),
			)
			o.failedCount.WithLabelValues(op.GetType().String()).Inc()
		} else {
			xlog.Debug(
				ctx,
				"operation handler finished successfully",
				zap.String("operation", types.OperationToString(op)),
			)
			o.successfulCount.WithLabelValues(op.GetType().String()).Inc()
		}
		o.results <- op.GetID()
	}()
}

func (o *OperationProcessorImpl) handleOperationResult(operationID string) {
	ctx := xlog.With(o.ctx, zap.String("OperationID", operationID))
	if _, exist := o.runningOperations[operationID]; !exist {
		xlog.Error(ctx, "got result from not running operation")
		return
	}
	xlog.Debug(ctx, "operation handler is marked as finished")
	delete(o.runningOperations, operationID)
}
