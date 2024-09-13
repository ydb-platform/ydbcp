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
	op := &types.GenericOperation{
		Type: types.OperationType("UNKNOWN"),
	}
	err := registry.Call(ctx, op)
	assert.NotEmpty(t, err, "unknown operation type should raise error")

	opType := types.OperationType("TEST")
	expectedMessage := "Test message"
	var result types.Operation
	err = registry.Add(
		opType,
		func(ctx context.Context, op types.Operation) error {
			op.SetState(types.OperationStateDone)
			op.SetMessage(expectedMessage)
			result = op
			return nil
		},
	)
	assert.Empty(t, err)

	err = registry.Add(
		opType,
		func(_ context.Context, op types.Operation) error {
			return nil
		},
	)
	assert.NotEmpty(t, err, "registry must prohibit re-register handlers")

	op = &types.GenericOperation{
		ID:    types.GenerateObjectID(),
		Type:  opType,
		State: types.OperationStatePending,
	}
	err = registry.Call(ctx, op)
	assert.Empty(t, err)
	assert.Equal(t, result.GetState(), types.OperationStateDone)
	assert.Equal(t, result.GetMessage(), expectedMessage)
}
