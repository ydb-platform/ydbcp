package processor

import (
	"context"
	"sync"
	"time"

	"ydbcp/internal/connectors/db"
	"ydbcp/internal/types"
	"ydbcp/internal/util/ticker"
	"ydbcp/internal/util/xlog"

	"go.uber.org/zap"
)

type OperationProcessorImpl struct {
	ctx                    context.Context
	wg                     *sync.WaitGroup
	period                 time.Duration
	handleOperationTimeout time.Duration

	tickerProvider ticker.TickerProvider
	handlers       OperationHandlerRegistry
	db             db.DBConnector

	runningOperations map[string]bool
	results           chan string
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
	options ...Option,
) *OperationProcessorImpl {
	op := &OperationProcessorImpl{
		ctx:                    ctx,
		period:                 time.Second * 10,
		handleOperationTimeout: time.Second * 600,
		wg:                     wg,
		handlers:               handlers,
		db:                     db,
		tickerProvider:         ticker.NewRealTicker,
		runningOperations:      make(map[string]bool),
		results:                make(chan string),
	}
	for _, opt := range options {
		opt(op)
	}
	wg.Add(1)
	go op.run()
	return op
}

func (o *OperationProcessorImpl) run() {
	defer o.wg.Done()
	xlog.Debug(
		o.ctx, "Operation Processor started", zap.Duration("period", o.period),
	)
	ticker := o.tickerProvider(o.period)
	for {
		select {
		case <-o.ctx.Done():
			xlog.Debug(o.ctx, "Operation Processor stopped")
			return
		case <-ticker.Chan():
			o.processOperations()
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
	if _, exist := o.runningOperations[op.GetID()]; exist {
		xlog.Debug(
			o.ctx, "operation already running",
			zap.String("operation", types.OperationToString(op)),
		)
		return
	}
	o.runningOperations[op.GetID()] = true
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		ctx, cancel := context.WithTimeout(o.ctx, o.handleOperationTimeout)
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
		}
		o.results <- op.GetID()
	}()
}

func (o *OperationProcessorImpl) handleOperationResult(operationID string) {
	xlog.Debug(o.ctx, "operation handler is finished", zap.String("operationID", operationID))
	if _, exist := o.runningOperations[operationID]; !exist {
		xlog.Error(
			o.ctx, "got result from not running operation",
			zap.String("operationID", operationID),
		)
		return
	}
	delete(o.runningOperations, operationID)
}
