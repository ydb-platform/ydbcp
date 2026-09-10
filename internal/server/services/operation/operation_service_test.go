package operation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"ydbcp/internal/auth"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
)

func TestCancelOperationUpdatesOnlyRequestedOperation(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	ctx := context.Background()
	provider, err := auth.NewDummyAuthProvider(ctx)
	require.NoError(t, err)
	id, other := types.GenerateObjectID(), types.GenerateObjectID()
	var store db.DBConnector = db.NewMockDBConnector(db.WithOperations(map[string]types.Operation{
		id:    &types.TakeBackupOperation{ID: id, ContainerID: "tenant", State: types.OperationStateRunning},
		other: &types.TakeBackupOperation{ID: other, ContainerID: "other", State: types.OperationStateRunning},
	}))
	service := &OperationService{driver: store, auth: provider}
	response, err := service.CancelOperation(ctx, &pb.CancelOperationRequest{OperationId: id})
	require.NoError(t, err)
	assert.Equal(t, pb.Operation_START_CANCELLING, response.Status)
	requested, err := store.GetOperation(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, types.OperationStateStartCancelling, requested.GetState())
	assert.NotNil(t, requested.GetUpdatedAt())
	unrelated, err := store.GetOperation(ctx, other)
	require.NoError(t, err)
	assert.Equal(t, types.OperationStateRunning, unrelated.GetState())
	_, err = service.GetOperation(ctx, &pb.GetOperationRequest{Id: types.GenerateObjectID()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
