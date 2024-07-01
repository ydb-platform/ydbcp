package processor

import (
	"context"
	"fmt"
	"ydbcp/internal/types"
)

type OperationHandler func(context.Context, types.Operation) (types.Operation, error)

type OperationHandlerRegistry interface {
	Add(types.OperationType, OperationHandler) error
	Call(context.Context, types.Operation) (types.Operation, error)
}

type OperationHandlerRegistryImpl struct {
	handlers map[types.OperationType]OperationHandler
}

func NewOperationHandlerRegistry() *OperationHandlerRegistryImpl {
	return &OperationHandlerRegistryImpl{
		handlers: make(map[types.OperationType]OperationHandler),
	}
}

func (r OperationHandlerRegistryImpl) Add(
	operationType types.OperationType,
	handler OperationHandler,
) error {
	if _, ok := r.handlers[operationType]; ok {
		return fmt.Errorf("OperationType %s already registred", operationType)
	}
	r.handlers[operationType] = handler
	return nil
}

func (r OperationHandlerRegistryImpl) Call(
	ctx context.Context,
	op types.Operation,
) (types.Operation, error) {
	operationType := op.Type
	handler, ok := r.handlers[operationType]
	if !ok {
		return op, fmt.Errorf("unknown OperationType %s", operationType)
	}
	return handler(ctx, op)
}
