package processor

import (
	"context"
	"testing"
	"ydbcp/internal/types"
	ydbcp_db_connector "ydbcp/internal/ydbcp-db-connector"

	"github.com/stretchr/testify/assert"
)

func TestOperationHandlerRegistry(t *testing.T) {
	registry := NewOperationHandlerRegistry(ydbcp_db_connector.NewFakeYdbConnector())
	ctx := context.Background()
	op := &types.GenericOperation{
		Type: types.OperationType("UNKNOWN"),
	}
	_, err := registry.Call(ctx, op)
	assert.NotEmpty(t, err, "unknown operation type should raise error")

	opType := types.OperationType("TEST")
	expectedMessage := "Test message"
	err = registry.Add(
		opType,
		func(ctx context.Context, op types.Operation) (types.Operation, error) {
			op.SetState(types.OperationStateDone)
			op.SetMessage(expectedMessage)
			return op, nil
		},
	)
	assert.Empty(t, err)

	err = registry.Add(
		opType,
		func(_ context.Context, op types.Operation) (types.Operation, error) {
			return op, nil
		},
	)
	assert.NotEmpty(t, err, "registry must prohibit re-register handlers")

	op = &types.GenericOperation{
		Id:    types.GenerateObjectID(),
		Type:  opType,
		State: types.OperationStatePending,
	}
	result, err := registry.Call(ctx, op)
	assert.Equal(t, result.GetState(), types.OperationStateDone)
	assert.Equal(t, result.GetMessage(), expectedMessage)
}
