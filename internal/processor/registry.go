package processor

import (
	"context"
	"fmt"
	"ydbcp/internal/types"
	ydbcp_db_connector "ydbcp/internal/ydbcp-db-connector"
)

type OperationHandlerRegistry interface {
	Add(types.OperationType, OperationHandler) error
	Call(context.Context, types.Operation) (types.Operation, error)
}

type OperationHandlerRegistryImpl struct {
	handlers map[types.OperationType]OperationHandler
	db       ydbcp_db_connector.YdbDriver
}

func NewOperationHandlerRegistry(driver ydbcp_db_connector.YdbDriver) *OperationHandlerRegistryImpl {
	return &OperationHandlerRegistryImpl{
		handlers: make(map[types.OperationType]OperationHandler),
		db:       driver,
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
	operationType := op.GetType()
	handler, ok := r.handlers[operationType]
	if !ok {
		return op, fmt.Errorf("unknown OperationType %s", operationType)
	}
	return handler(ctx, op)
}
