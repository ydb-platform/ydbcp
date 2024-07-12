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
	ctx                          context.Context
	wg                           *sync.WaitGroup
	period                       time.Duration
	handleOperationResultTimeout time.Duration

	tickerProvider ticker.TickerProvider
	handlers       OperationHandlerRegistry
	db             db.DBConnector

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
func WithHandleOperationResultTimeout(timeout time.Duration) Option {
	return func(o *OperationProcessorImpl) {
		o.handleOperationResultTimeout = timeout
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
		ctx:                          ctx,
		period:                       time.Second * 10,
		handleOperationResultTimeout: time.Second * 10,
		wg:                           wg,
		handlers:                     handlers,
		db:                           db,
		tickerProvider:               ticker.NewRealTicker,
		runningOperations:            make(map[types.ObjectID]bool),
		results:                      make(chan OperationHandlerResult),
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
		o.processOperation(ctx, op)
	}
}

func (o *OperationProcessorImpl) processOperation(
	ctx context.Context, op types.Operation,
) {
	if _, exist := o.runningOperations[op.GetId()]; exist {
		xlog.Debug(
			ctx, "operation already running",
			zap.String("operation", types.OperationToString(op)),
		)
		return
	}
	o.runningOperations[op.GetId()] = true
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		xlog.Debug(
			ctx, "start operation handler",
			zap.String("operation", types.OperationToString(op)),
		)
		genericOp := types.GenericOperation{
			Id:      op.GetId(),
			Type:    op.GetType(),
			State:   op.GetState(),
			Message: op.GetMessage(),
		}
		result, err := o.handlers.Call(ctx, op)
		if err != nil {
			xlog.Error(
				ctx, "operation handler failed",
				zap.String("operation", types.OperationToString(op)),
			)
		}
		o.results <- OperationHandlerResult{old: &genericOp, new: result}
	}()
}

func (o *OperationProcessorImpl) handleOperationResult(result OperationHandlerResult) {
	ctx, cancel := context.WithTimeout(o.ctx, o.handleOperationResultTimeout)
	defer cancel()

	xlog.Debug(
		ctx,
		"operation handler result",
		zap.String("oldOperation", types.OperationToString(result.old)),
		zap.String("newOperation", types.OperationToString(result.new)),
	)
	if _, exist := o.runningOperations[result.old.GetId()]; !exist {
		xlog.Error(
			ctx, "got result from not running operation",
			zap.String("operation", types.OperationToString(result.old)),
		)
		return
	}
	o.updateOperationState(ctx, result.old, result.new)
	delete(o.runningOperations, result.old.GetId())
}

func (o *OperationProcessorImpl) updateOperationState(
	ctx context.Context,
	old types.Operation,
	new types.Operation,
) error {
	changed := false
	if old.GetState() != new.GetState() {
		xlog.Debug(
			ctx,
			"operation state changed",
			zap.String("OperationId", old.GetId().String()),
			zap.String("oldState", old.GetState().String()),
			zap.String("newState", new.GetState().String()),
		)
		changed = true
	}
	if old.GetMessage() != new.GetMessage() {
		xlog.Debug(
			ctx,
			"operation message changed",
			zap.String("OperationId", old.GetId().String()),
			zap.String("Message", new.GetMessage()),
		)
		changed = true
	}
	if !changed {
		return nil
	}
	return o.db.UpdateOperation(ctx, new)
}
