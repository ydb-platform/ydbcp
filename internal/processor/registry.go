package processor

import (
	"context"
	"fmt"

	"ydbcp/internal/types"
)

type OperationHandlerRegistry interface {
	Add(types.OperationType, types.OperationHandler) error
	Call(context.Context, types.Operation) error
}

type OperationHandlerRegistryImpl struct {
	handlers map[types.OperationType]types.OperationHandler
}

func NewOperationHandlerRegistry() *OperationHandlerRegistryImpl {
	return &OperationHandlerRegistryImpl{
		handlers: make(map[types.OperationType]types.OperationHandler),
	}
}

func (r OperationHandlerRegistryImpl) Add(
	operationType types.OperationType,
	handler types.OperationHandler,
) error {
	if _, ok := r.handlers[operationType]; ok {
		return fmt.Errorf("OperationType %s already registred", operationType)
	}
	r.handlers[operationType] = handler
	return nil
}

func (r OperationHandlerRegistryImpl) Call(ctx context.Context, op types.Operation) error {
	operationType := op.GetType()
	handler, ok := r.handlers[operationType]
	if !ok {
		return fmt.Errorf("unknown OperationType %s", operationType)
	}
	return handler(ctx, op)
}
