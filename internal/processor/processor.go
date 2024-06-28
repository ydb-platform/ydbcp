package processor

import (
	"context"
	"sync"
	"time"

	"ydbcp/internal/types"
	"ydbcp/internal/util/ticker"
	"ydbcp/internal/util/xlog"
	ydbcp_db_connector "ydbcp/internal/ydbcp-db-connector"

	"go.uber.org/zap"
)

type OperationProcessorImpl struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	period time.Duration

	tickerProvider ticker.TickerProvider
	handlers       OperationHandlerRegistry
	db             ydbcp_db_connector.YdbDriver

	runningOperations map[types.ObjectID]bool
	results           chan OperationHandlerResult
}

type OperationHandlerResult struct {
	old types.Operation
	new types.Operation
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

func NewOperationProcessor(
	ctx context.Context,
	wg *sync.WaitGroup,
	db ydbcp_db_connector.YdbDriver,
	handlers OperationHandlerRegistry,
	options ...Option,
) *OperationProcessorImpl {
	op := &OperationProcessorImpl{
		ctx:               ctx,
		period:            time.Duration(time.Second * 10),
		wg:                wg,
		handlers:          handlers,
		db:                db,
		tickerProvider:    ticker.NewRealTicker,
		runningOperations: make(map[types.ObjectID]bool),
		results:           make(chan OperationHandlerResult),
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
	xlog.Debug(o.ctx, "Operation Processor started", zap.Duration("period", o.period))
	ticker := o.tickerProvider(o.period)
	for {
		select {
		case <-o.ctx.Done():
			xlog.Debug(o.ctx, "Operation Processor stopped")
			return
		case <-ticker.Chan():
			o.processOperations()
		case result := <-o.results:
			o.handleOperationResult(result)
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
		o.launchOperation(ctx, op)
	}
}

func (o *OperationProcessorImpl) launchOperation(ctx context.Context, op types.Operation) {
	if _, exist := o.runningOperations[op.Id]; exist {
		xlog.Debug(ctx, "operation already running", zap.String("operation", op.String()))
		return
	}
	o.runningOperations[op.Id] = true
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		xlog.Debug(ctx, "start operation handler", zap.String("operation", op.String()))
		result, err := o.handlers.Call(ctx, op)
		if err != nil {
			xlog.Error(ctx, "operation handler failed", zap.String("operation", op.String()))
		}
		o.results <- OperationHandlerResult{old: op, new: result}
	}()
}

func (o *OperationProcessorImpl) handleOperationResult(result OperationHandlerResult) {
	ctx, cancel := context.WithTimeout(o.ctx, time.Duration(time.Second*10))
	defer cancel()

	xlog.Debug(
		ctx,
		"operation handler result",
		zap.String("oldOperation", result.old.String()),
		zap.String("newOperation", result.new.String()),
	)
	if _, exist := o.runningOperations[result.old.Id]; !exist {
		xlog.Error(ctx, "got result from not running operation", zap.String("operation", result.old.String()))
		return
	}
	o.updateOperationState(ctx, result.old, result.new)
	delete(o.runningOperations, result.old.Id)
}

func (o *OperationProcessorImpl) updateOperationState(
	ctx context.Context,
	old types.Operation,
	new types.Operation,
) error {
	changed := false
	if old.State != new.State {
		xlog.Debug(
			ctx,
			"operation state changed",
			zap.String("OperationId", old.Id.String()),
			zap.String("oldState", old.State),
			zap.String("newState", new.State),
		)
		changed = true
	}
	if old.Message != new.Message {
		xlog.Debug(
			ctx,
			"operation message changed",
			zap.String("OperationId", old.Id.String()),
			zap.String("Message", new.Message),
		)
		changed = true
	}
	if !changed {
		return nil
	}
	return o.db.UpdateOperation(ctx, new)
}
