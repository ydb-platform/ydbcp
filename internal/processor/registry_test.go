package processor

import (
	"context"
	"testing"

	"ydbcp/internal/types"

	"github.com/stretchr/testify/assert"
)

func TestOperationHandlerRegistry(t *testing.T) {
	registry := NewOperationHandlerRegistry()
	ctx := context.Background()
	op := types.Operation{
		Type: types.OperationType("UNKNOWN"),
	}
	_, err := registry.Call(ctx, op)
	assert.NotEmpty(t, err, "unknown operation type should raise error")

	opType := types.OperationType("TEST")
	expectedMessage := "Test message"
	err = registry.Add(opType, func(ctx context.Context, op types.Operation) (types.Operation, error) {
		op.State = types.OperationStateDone
		op.Message = expectedMessage
		return op, nil
	})
	assert.Empty(t, err)

	err = registry.Add(opType, func(_ context.Context, op types.Operation) (types.Operation, error) {
		return op, nil
	})
	assert.NotEmpty(t, err, "registry must prohibit re-register handlers")

	op = types.Operation{
		Id:    types.GenerateObjectID(),
		Type:  opType,
		State: types.OperationStatePending,
	}
	result, err := registry.Call(ctx, op)
	assert.Equal(t, result.State, types.OperationStateDone)
	assert.Equal(t, result.Message, expectedMessage)
}
