package handlers

import (
	"context"
	"fmt"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

var (
	t1 = timestamppb.New(time.Date(2024, 01, 01, 0, 0, 0, 0, time.UTC))
	t2 = timestamppb.New(time.Date(2024, 01, 01, 1, 0, 0, 0, time.UTC))
	t3 = timestamppb.New(time.Date(2024, 01, 01, 2, 0, 0, 0, time.UTC))
	t4 = timestamppb.New(time.Date(2024, 01, 01, 3, 0, 0, 0, time.UTC))
	r1 = t3.AsTime().Add(exp(2) * time.Minute)
	r2 = t4.AsTime().Add(exp(3) * time.Minute)
)

func TestRetryLogic(t *testing.T) {
	for _, tt := range []struct {
		config    *pb.RetryConfig
		ops       []*types.TakeBackupOperation
		res       *time.Time
		clockTime time.Time
	}{
		{
			config: nil,
			ops: []*types.TakeBackupOperation{
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t1,
						CompletedAt: t2,
					},
				},
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t2,
						CompletedAt: t3,
					},
				},
			},
			res: nil,
		},
		{
			config: &pb.RetryConfig{},
			ops: []*types.TakeBackupOperation{
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t1,
						CompletedAt: t2,
					},
				},
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t2,
						CompletedAt: t3,
					},
				},
			},
			res: nil,
		},
		{
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_Count{Count: 2},
			},
			ops: []*types.TakeBackupOperation{
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t1,
						CompletedAt: t2,
					},
				},
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t2,
						CompletedAt: t3,
					},
				},
			},
			res: nil,
		},
		{
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_Count{Count: 3},
			},
			ops: []*types.TakeBackupOperation{
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t1,
						CompletedAt: t2,
					},
				},
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t2,
						CompletedAt: t3,
					},
				},
			},
			res: &r1,
		},
		{
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_Count{Count: 4},
			},
			ops: []*types.TakeBackupOperation{
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t1,
						CompletedAt: t2,
					},
				},
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t2,
						CompletedAt: t3,
					},
				},
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t3,
						CompletedAt: t4,
					},
				},
			},
			res: &r2,
		},
		{
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_MaxBackoff{MaxBackoff: durationpb.New(time.Hour)},
			},
			ops: []*types.TakeBackupOperation{
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t1,
						CompletedAt: t2,
					},
				},
			},
			clockTime: t4.AsTime(),
			res:       nil,
		},
		{
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_MaxBackoff{MaxBackoff: durationpb.New(time.Hour * 24)},
			},
			ops: []*types.TakeBackupOperation{
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t1,
						CompletedAt: t2,
					},
				},
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t2,
						CompletedAt: t3,
					},
				},
				{
					Audit: &pb.AuditInfo{
						CreatedAt:   t3,
						CompletedAt: t4,
					},
				},
			},
			clockTime: t4.AsTime().Add(time.Second),
			res:       &r2,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.res, shouldRetry(tt.config, tt.ops, clockwork.NewFakeClockAt(tt.clockTime)))
		})
	}
}

func toMap(list ...types.Operation) map[string]types.Operation {
	res := make(map[string]types.Operation)
	for _, op := range list {
		res[op.GetID()] = op
	}
	return res
}

func TestTBWRHandlerSuccess(t *testing.T) {
	ctx := context.Background()
	tbwrID := types.GenerateObjectID()
	tbwr := types.TakeBackupWithRetryOperation{
		TakeBackupOperation: types.TakeBackupOperation{
			ID:                  tbwrID,
			ContainerID:         "abcde",
			State:               types.OperationStateRunning,
			Message:             "",
			YdbConnectionParams: types.YdbConnectionParams{},
			Audit:               &pb.AuditInfo{},
		},
		RetryConfig: nil,
	}
	ops := []types.Operation{
		&types.TakeBackupOperation{
			ID:    types.GenerateObjectID(),
			State: types.OperationStateError,
			Audit: &pb.AuditInfo{
				CreatedAt:   t1,
				CompletedAt: t2,
			},
			ParentOperationID: &tbwrID,
		},
		&types.TakeBackupOperation{
			ID:    types.GenerateObjectID(),
			State: types.OperationStateDone,
			Audit: &pb.AuditInfo{
				CreatedAt:   t2,
				CompletedAt: t3,
			},
			ParentOperationID: &tbwrID,
		},
		&tbwr,
	}

	dbConnector := db.NewMockDBConnector(
		db.WithOperations(toMap(ops...)),
	)
	dbConnector.SetOperationsIDSelector([]string{ops[0].GetID(), ops[1].GetID()})

	clientConnector := client.NewMockClientConnector()

	metrics.InitializeMockMetricsRegistry()
	handler := NewTBWROperationHandler(
		dbConnector,
		clientConnector,
		config.S3Config{},
		config.ClientConnectionConfig{},
		queries.NewWriteTableQueryMock,
		clockwork.NewFakeClockAt(t1.AsTime()),
	)
	err := handler(ctx, &tbwr)
	assert.Empty(t, err)

	op, err := dbConnector.GetOperation(ctx, tbwrID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateDone, op.GetState())
	assert.Equal(t, float64(1), metrics.GetMetrics()["operations_duration_seconds"])
	assert.Equal(t, float64(1), metrics.GetMetrics()["schedule_finished_take_backup_with_retry_count"])
}

func TestTBWRHandlerSkipRunning(t *testing.T) {
	ctx := context.Background()
	tbwrID := types.GenerateObjectID()
	tbwr := types.TakeBackupWithRetryOperation{
		TakeBackupOperation: types.TakeBackupOperation{
			ID:                  tbwrID,
			ContainerID:         "abcde",
			State:               types.OperationStateRunning,
			Message:             "",
			YdbConnectionParams: types.YdbConnectionParams{},
			Audit:               &pb.AuditInfo{},
		},
		RetryConfig: nil,
	}

	ops := []types.Operation{
		&types.TakeBackupOperation{
			ID:    types.GenerateObjectID(),
			State: types.OperationStateError,
			Audit: &pb.AuditInfo{
				CreatedAt:   t1,
				CompletedAt: t2,
			},
			ParentOperationID: &tbwrID,
		},
		&types.TakeBackupOperation{
			ID:    types.GenerateObjectID(),
			State: types.OperationStateRunning,
			Audit: &pb.AuditInfo{
				CreatedAt:   t2,
				CompletedAt: t3,
			},
			ParentOperationID: &tbwrID,
		},
		&tbwr,
	}

	dbConnector := db.NewMockDBConnector(
		db.WithOperations(toMap(ops...)),
	)
	dbConnector.SetOperationsIDSelector([]string{ops[0].GetID(), ops[1].GetID()})

	clientConnector := client.NewMockClientConnector()

	metrics.InitializeMockMetricsRegistry()
	handler := NewTBWROperationHandler(
		dbConnector,
		clientConnector,
		config.S3Config{},
		config.ClientConnectionConfig{},
		queries.NewWriteTableQueryMock,
		clockwork.NewFakeClockAt(t1.AsTime()),
	)
	err := handler(ctx, &tbwr)
	assert.Empty(t, err)

	op, err := dbConnector.GetOperation(ctx, tbwrID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateRunning, op.GetState())
	operations, err := dbConnector.SelectOperations(ctx, queries.NewReadTableQuery())
	assert.Empty(t, err)
	assert.Equal(t, 3, len(operations))
	assert.Equal(t, float64(0), metrics.GetMetrics()["operations_duration_seconds"])
	assert.Equal(t, float64(0), metrics.GetMetrics()["schedule_finished_take_backup_with_retry_count"])
}

func TestTBWRHandlerSkipError(t *testing.T) {
	ctx := context.Background()
	tbwrID := types.GenerateObjectID()
	tbwr := types.TakeBackupWithRetryOperation{
		TakeBackupOperation: types.TakeBackupOperation{
			ID:                  tbwrID,
			ContainerID:         "abcde",
			State:               types.OperationStateRunning,
			Message:             "",
			YdbConnectionParams: types.YdbConnectionParams{},
			Audit:               &pb.AuditInfo{},
		},
		RetryConfig: &pb.RetryConfig{Retries: &pb.RetryConfig_Count{Count: 3}},
	}

	ops := []types.Operation{
		&types.TakeBackupOperation{
			ID:    types.GenerateObjectID(),
			State: types.OperationStateError,
			Audit: &pb.AuditInfo{
				CreatedAt:   t1,
				CompletedAt: t2,
			},
			ParentOperationID: &tbwrID,
		},
		&types.TakeBackupOperation{
			ID:    types.GenerateObjectID(),
			State: types.OperationStateError,
			Audit: &pb.AuditInfo{
				CreatedAt:   t2,
				CompletedAt: t3,
			},
			ParentOperationID: &tbwrID,
		},
		&tbwr,
	}

	dbConnector := db.NewMockDBConnector(
		db.WithOperations(toMap(ops...)),
	)
	dbConnector.SetOperationsIDSelector([]string{ops[0].GetID(), ops[1].GetID()})

	clientConnector := client.NewMockClientConnector()

	handler := NewTBWROperationHandler(
		dbConnector,
		clientConnector,
		config.S3Config{},
		config.ClientConnectionConfig{},
		queries.NewWriteTableQueryMock,
		clockwork.NewFakeClockAt(t3.AsTime()),
	)
	err := handler(ctx, &tbwr)
	assert.Empty(t, err)

	op, err := dbConnector.GetOperation(ctx, tbwrID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateRunning, op.GetState())
	operations, err := dbConnector.SelectOperations(ctx, queries.NewReadTableQuery())
	assert.Empty(t, err)
	assert.Equal(t, 3, len(operations))
}

func TestTBWRHandlerError(t *testing.T) {
	ctx := context.Background()
	tbwrID := types.GenerateObjectID()
	tbwr := types.TakeBackupWithRetryOperation{
		TakeBackupOperation: types.TakeBackupOperation{
			ID:                  tbwrID,
			ContainerID:         "abcde",
			State:               types.OperationStateRunning,
			Message:             "",
			YdbConnectionParams: types.YdbConnectionParams{},
			Audit:               &pb.AuditInfo{},
		},
		RetryConfig: nil,
	}

	ops := []types.Operation{
		&types.TakeBackupOperation{
			ID:    types.GenerateObjectID(),
			State: types.OperationStateError,
			Audit: &pb.AuditInfo{
				CreatedAt:   t1,
				CompletedAt: t2,
			},
			ParentOperationID: &tbwrID,
		},
		&tbwr,
	}

	dbConnector := db.NewMockDBConnector(
		db.WithOperations(toMap(ops...)),
	)
	dbConnector.SetOperationsIDSelector([]string{ops[0].GetID()})

	clientConnector := client.NewMockClientConnector()

	metrics.InitializeMockMetricsRegistry()
	handler := NewTBWROperationHandler(
		dbConnector,
		clientConnector,
		config.S3Config{},
		config.ClientConnectionConfig{},
		queries.NewWriteTableQueryMock,
		clockwork.NewFakeClockAt(t2.AsTime()),
	)
	err := handler(ctx, &tbwr)
	assert.Empty(t, err)

	op, err := dbConnector.GetOperation(ctx, tbwrID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())
	assert.Equal(t, fmt.Sprintf("retry attempts exceeded limit: 1. Launched operations %s", ops[0].GetID()), op.GetMessage())
	assert.Equal(t, float64(1), metrics.GetMetrics()["operations_duration_seconds"])
	assert.Equal(t, float64(1), metrics.GetMetrics()["schedule_finished_take_backup_with_retry_count"])

}

func TestTBWRHandlerAlwaysRunOnce(t *testing.T) {
	ctx := context.Background()
	tbwrID := types.GenerateObjectID()
	tbwr := types.TakeBackupWithRetryOperation{
		TakeBackupOperation: types.TakeBackupOperation{
			ID:          tbwrID,
			ContainerID: "abcde",
			State:       types.OperationStateRunning,
			Message:     "",
			SourcePaths: []string{"path"},
			YdbConnectionParams: types.YdbConnectionParams{
				Endpoint:     "i.valid.com",
				DatabaseName: "/mydb",
			},
			Audit: &pb.AuditInfo{},
		},
		RetryConfig: nil,
	}

	ops := []types.Operation{
		&tbwr,
	}

	dbConnector := db.NewMockDBConnector(
		db.WithOperations(toMap(ops...)),
	)
	dbConnector.SetOperationsIDSelector([]string{})

	clientConnector := client.NewMockClientConnector()

	handler := NewTBWROperationHandler(
		dbConnector,
		clientConnector,
		config.S3Config{
			IsMock: true,
		},
		config.ClientConnectionConfig{
			AllowedEndpointDomains: []string{".valid.com"},
			AllowInsecureEndpoint:  true,
		},
		queries.NewWriteTableQueryMock,
		clockwork.NewFakeClockAt(t1.AsTime()),
	)
	err := handler(ctx, &tbwr)
	assert.Empty(t, err)

	op, err := dbConnector.GetOperation(ctx, tbwrID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateRunning, op.GetState())
	operations, err := dbConnector.SelectOperations(ctx, queries.NewReadTableQuery())
	assert.Empty(t, err)
	assert.Equal(t, 2, len(operations))
	tbwr = *op.(*types.TakeBackupWithRetryOperation)
	var tb *types.TakeBackupOperation
	for _, op = range operations {
		if op.GetType() == types.OperationTypeTB {
			tb = op.(*types.TakeBackupOperation)
			break
		}
	}
	assert.NotNil(t, tb)
	assert.Equal(t, types.OperationStateRunning, tb.State)
	assert.Equal(t, t1, tb.Audit.CreatedAt)
}

func TestTBWRHandlerStartCancel(t *testing.T) {
	ctx := context.Background()
	tbwrID := types.GenerateObjectID()
	tbwr := types.TakeBackupWithRetryOperation{
		TakeBackupOperation: types.TakeBackupOperation{
			ID:          tbwrID,
			ContainerID: "abcde",
			State:       types.OperationStateCancelling,
			Message:     "",
			SourcePaths: []string{"path"},
			YdbConnectionParams: types.YdbConnectionParams{
				Endpoint:     "i.valid.com",
				DatabaseName: "/mydb",
			},
			Audit: &pb.AuditInfo{},
		},
		RetryConfig: nil,
	}

	ops := []types.Operation{
		&types.TakeBackupOperation{
			ID:    types.GenerateObjectID(),
			State: types.OperationStateRunning,
			Audit: &pb.AuditInfo{
				CreatedAt: t1,
			},
			ParentOperationID: &tbwrID,
		},
		&tbwr,
	}

	dbConnector := db.NewMockDBConnector(
		db.WithOperations(toMap(ops...)),
	)
	dbConnector.SetOperationsIDSelector([]string{ops[0].GetID()})

	clientConnector := client.NewMockClientConnector()

	handler := NewTBWROperationHandler(
		dbConnector,
		clientConnector,
		config.S3Config{
			IsMock: true,
		},
		config.ClientConnectionConfig{
			AllowedEndpointDomains: []string{".valid.com"},
			AllowInsecureEndpoint:  true,
		},
		queries.NewWriteTableQueryMock,
		clockwork.NewFakeClockAt(t1.AsTime()),
	)
	err := handler(ctx, &tbwr)
	assert.Empty(t, err)

	op, err := dbConnector.GetOperation(ctx, tbwrID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateCancelling, op.GetState())
	operations, err := dbConnector.SelectOperations(ctx, queries.NewReadTableQuery())
	assert.Empty(t, err)
	assert.Equal(t, 2, len(operations))
	tbwr = *op.(*types.TakeBackupWithRetryOperation)
	var tb *types.TakeBackupOperation
	for _, op = range operations {
		if op.GetType() == types.OperationTypeTB {
			tb = op.(*types.TakeBackupOperation)
			break
		}
	}
	assert.NotNil(t, tb)
	assert.Equal(t, types.OperationStateStartCancelling, tb.State)
	assert.Equal(t, "Cancelling by parent operation", tb.Message)
	assert.Equal(t, types.OperationStateCancelling, tbwr.State)
}

func TestTBWRHandlerFullCancel(t *testing.T) {
	ctx := context.Background()
	tbwrID := types.GenerateObjectID()
	tbwr := types.TakeBackupWithRetryOperation{
		TakeBackupOperation: types.TakeBackupOperation{
			ID:          tbwrID,
			ContainerID: "abcde",
			State:       types.OperationStateCancelling,
			Message:     "",
			SourcePaths: []string{"path"},
			YdbConnectionParams: types.YdbConnectionParams{
				Endpoint:     "i.valid.com",
				DatabaseName: "/mydb",
			},
			Audit: &pb.AuditInfo{},
		},
		RetryConfig: nil,
	}

	ops := []types.Operation{
		&types.TakeBackupOperation{
			ID:    types.GenerateObjectID(),
			State: types.OperationStateCancelled,
			Audit: &pb.AuditInfo{
				CreatedAt: t1,
			},
			ParentOperationID: &tbwrID,
		},
		&tbwr,
	}

	dbConnector := db.NewMockDBConnector(
		db.WithOperations(toMap(ops...)),
	)
	dbConnector.SetOperationsIDSelector([]string{ops[0].GetID()})

	clientConnector := client.NewMockClientConnector()

	metrics.InitializeMockMetricsRegistry()
	handler := NewTBWROperationHandler(
		dbConnector,
		clientConnector,
		config.S3Config{
			IsMock: true,
		},
		config.ClientConnectionConfig{
			AllowedEndpointDomains: []string{".valid.com"},
			AllowInsecureEndpoint:  true,
		},
		queries.NewWriteTableQueryMock,
		clockwork.NewFakeClockAt(t1.AsTime()),
	)
	err := handler(ctx, &tbwr)
	assert.Empty(t, err)

	op, err := dbConnector.GetOperation(ctx, tbwrID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateCancelled, op.GetState())
	operations, err := dbConnector.SelectOperations(ctx, queries.NewReadTableQuery())
	assert.Empty(t, err)
	assert.Equal(t, 2, len(operations))
	tbwr = *op.(*types.TakeBackupWithRetryOperation)
	var tb *types.TakeBackupOperation
	for _, op = range operations {
		if op.GetType() == types.OperationTypeTB {
			tb = op.(*types.TakeBackupOperation)
			break
		}
	}
	assert.NotNil(t, tb)
	assert.Equal(t, types.OperationStateCancelled, tb.State)
	assert.Equal(t, types.OperationStateCancelled, tbwr.State)
	assert.Equal(t, "Success", tbwr.Message)
	assert.Equal(t, float64(1), metrics.GetMetrics()["operations_duration_seconds"])
	assert.Equal(t, float64(1), metrics.GetMetrics()["schedule_finished_take_backup_with_retry_count"])
}
