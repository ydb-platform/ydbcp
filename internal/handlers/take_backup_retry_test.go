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
	t1  = timestamppb.New(time.Date(2024, 01, 01, 0, 0, 0, 0, time.UTC))
	t2  = timestamppb.New(time.Date(2024, 01, 01, 1, 0, 0, 0, time.UTC))
	t3  = timestamppb.New(time.Date(2024, 01, 01, 2, 0, 0, 0, time.UTC))
	t4  = timestamppb.New(time.Date(2024, 01, 01, 3, 0, 0, 0, time.UTC))
	tt2 = t2.AsTime()
	tt3 = t3.AsTime()
	tt4 = t4.AsTime()
	r1  = t3.AsTime().Add(exp(2) * time.Minute)
	r2  = t4.AsTime().Add(exp(3) * time.Minute)
)

func TestRetryWithOpsLogic(t *testing.T) {
	for _, tt := range []struct {
		config    *pb.RetryConfig
		tbwr      *types.TakeBackupWithRetryOperation
		tb        *types.TakeBackupOperation
		res       *time.Time
		clockTime time.Time
		decision  RetryDecision
	}{
		{ //00
			tbwr: &types.TakeBackupWithRetryOperation{},
			tb: &types.TakeBackupOperation{
				State: types.OperationStateError,
				Audit: &pb.AuditInfo{
					CreatedAt:   t2,
					CompletedAt: t3,
				},
			},
			res: nil,
		},
		{ //01
			tbwr: &types.TakeBackupWithRetryOperation{
				Retries:     0,
				RetryConfig: &pb.RetryConfig{},
			},
			tb: &types.TakeBackupOperation{
				State: types.OperationStateError,
				Audit: &pb.AuditInfo{
					CreatedAt:   t2,
					CompletedAt: t3,
				},
			},
			res: nil,
		},
		{ //02
			tbwr: &types.TakeBackupWithRetryOperation{
				TakeBackupOperation: types.TakeBackupOperation{
					Audit: &pb.AuditInfo{
						CreatedAt: t1,
					},
				},
				Retries: 2,
				RetryConfig: &pb.RetryConfig{
					Retries: &pb.RetryConfig_Count{Count: 2},
				},
			},
			tb: &types.TakeBackupOperation{
				State: types.OperationStateError,
				Audit: &pb.AuditInfo{
					CreatedAt:   t2,
					CompletedAt: t3,
				},
			},
			res: nil,
		},
		{ //03
			tbwr: &types.TakeBackupWithRetryOperation{
				TakeBackupOperation: types.TakeBackupOperation{
					Audit: &pb.AuditInfo{
						CreatedAt: t1,
					},
				},
				Retries: 2,
				RetryConfig: &pb.RetryConfig{
					Retries: &pb.RetryConfig_Count{Count: 3},
				},
			},
			tb: &types.TakeBackupOperation{
				State: types.OperationStateError,
				Audit: &pb.AuditInfo{
					CreatedAt:   t2,
					CompletedAt: t3,
				},
			},
			clockTime: tt3.Add(time.Minute * 3),
			res:       &r1,
		},
		{ //04
			tbwr: &types.TakeBackupWithRetryOperation{
				TakeBackupOperation: types.TakeBackupOperation{
					Audit: &pb.AuditInfo{
						CreatedAt: t1,
					},
				},
				Retries: 3,
				RetryConfig: &pb.RetryConfig{
					Retries: &pb.RetryConfig_Count{Count: 4},
				},
			},
			tb: &types.TakeBackupOperation{
				State: types.OperationStateError,
				Audit: &pb.AuditInfo{
					CreatedAt:   t3,
					CompletedAt: t4,
				},
			},
			clockTime: tt4.Add(time.Minute * 10),
			res:       &r2,
		},
		{ //05
			tbwr: &types.TakeBackupWithRetryOperation{
				TakeBackupOperation: types.TakeBackupOperation{
					Audit: &pb.AuditInfo{
						CreatedAt: t1,
					},
				},
				Retries: 5,
				RetryConfig: &pb.RetryConfig{
					Retries: &pb.RetryConfig_MaxBackoff{MaxBackoff: durationpb.New(time.Hour)},
				},
			},
			tb: &types.TakeBackupOperation{
				State: types.OperationStateError,
				Audit: &pb.AuditInfo{
					CreatedAt:   t1,
					CompletedAt: t2,
				},
			},
			clockTime: t4.AsTime(),
			res:       nil,
		},
		{ //06
			tbwr: &types.TakeBackupWithRetryOperation{
				TakeBackupOperation: types.TakeBackupOperation{
					Audit: &pb.AuditInfo{
						CreatedAt: t1,
					},
				},
				Retries: 3,
				RetryConfig: &pb.RetryConfig{
					Retries: &pb.RetryConfig_MaxBackoff{MaxBackoff: durationpb.New(time.Hour * 24)},
				},
			},
			tb: &types.TakeBackupOperation{
				State: types.OperationStateError,
				Audit: &pb.AuditInfo{
					CreatedAt:   t3,
					CompletedAt: t4,
				},
			},
			clockTime: t4.AsTime().Add(time.Minute * 5),
			res:       &r2,
		},
	} {
		t.Run("", func(t *testing.T) {
			decision, err := MakeRetryDecision(context.TODO(), tt.tbwr, tt.tb, clockwork.NewFakeClockAt(tt.clockTime))
			if err != nil {
				require.Nil(t, tt.tbwr.Audit)
			} else {
				if decision == RunNewTb {
					require.NotNil(t, tt.res)
					cat := tt.tb.Audit.CompletedAt.AsTime()
					r := shouldRetry(
						tt.tbwr.RetryConfig,
						tt.tbwr.Retries,
						tt.tbwr.Audit.CreatedAt.AsTime(),
						&cat,
						clockwork.NewFakeClockAt(tt.clockTime),
					)
					require.Equal(t, r, tt.res)
				} else {
					require.Equal(t, decision, Error)
					require.Nil(t, tt.res)
				}
			}
		})
	}
}

func TestRetryLogic(t *testing.T) {
	for _, tt := range []struct {
		config     *pb.RetryConfig
		retries    int
		firstStart time.Time
		lastEnd    *time.Time
		res        *time.Time
		clockTime  time.Time
	}{
		{ //00
			firstStart: t1.AsTime(),
			clockTime:  tt2,
			res:        &tt2,
		},
		{ //01
			config:     &pb.RetryConfig{}, //faulty config
			firstStart: t1.AsTime(),
			clockTime:  tt2,
			res:        nil,
		},
		{ //02
			config:     &pb.RetryConfig{},
			firstStart: t1.AsTime(),
			retries:    2,
			res:        nil,
		},
		{ //03
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_Count{Count: 2},
			},
			firstStart: t1.AsTime(),
			clockTime:  t2.AsTime(),
			res:        &tt2,
		},
		{ //04
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_Count{Count: 2},
			},
			firstStart: t1.AsTime(),

			retries: 2,
		},
		{ //05
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_Count{Count: 3},
			},
			retries:    2,
			firstStart: t1.AsTime(),
			lastEnd:    &tt3,
			res:        &r1,
		},
		{ //06
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_Count{Count: 4},
			},
			firstStart: t1.AsTime(),
			lastEnd:    &tt4,
			retries:    3,
			res:        &r2,
		},
		{ //07
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_MaxBackoff{MaxBackoff: durationpb.New(time.Hour)},
			},
			firstStart: t1.AsTime(),
			lastEnd:    &tt2,
			retries:    0,
			clockTime:  t4.AsTime(),
			res:        nil,
		},
		{ //08
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_MaxBackoff{MaxBackoff: durationpb.New(time.Hour * 24)},
			},
			firstStart: t1.AsTime(),
			lastEnd:    &tt4,
			retries:    3,
			clockTime:  t4.AsTime().Add(time.Second),
			res:        &r2,
		},
		{ //09
			config: &pb.RetryConfig{
				Retries: &pb.RetryConfig_MaxBackoff{MaxBackoff: durationpb.New(time.Hour * 24)},
			},
			firstStart: t1.AsTime(),
			lastEnd:    &tt4,
			retries:    3,
			clockTime:  t4.AsTime().Add(time.Second),
			res:        &r2,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.res, shouldRetry(tt.config, tt.retries, tt.firstStart, tt.lastEnd, clockwork.NewFakeClockAt(tt.clockTime)))
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
	assert.Equal(t, float64(1), metrics.GetMetrics()["operations_finished_count"])
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
	assert.Equal(t, float64(0), metrics.GetMetrics()["operations_finished_count"])
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
	assert.Equal(t, float64(1), metrics.GetMetrics()["operations_finished_count"])

}

func TestTBWRHandlerAlwaysRunOnce(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
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
			Audit: &pb.AuditInfo{
				CreatedAt: t1,
			},
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

func TestTBWRHandlerInvalidEndpointRetry(t *testing.T) {
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
				Endpoint:     "invalid.com",
				DatabaseName: "/mydb",
			},
			Audit: &pb.AuditInfo{},
		},
		RetryConfig: &pb.RetryConfig{Retries: &pb.RetryConfig_Count{Count: 3}},
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
	assert.Equal(t, 1, len(operations))
	tbwr = *op.(*types.TakeBackupWithRetryOperation)
	assert.Equal(t, 1, tbwr.Retries)
}

func TestTBWRHandlerInvalidEndpointError(t *testing.T) {
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
				Endpoint:     "invalid.com",
				DatabaseName: "/mydb",
			},
			Audit: &pb.AuditInfo{},
		},
		RetryConfig: &pb.RetryConfig{Retries: &pb.RetryConfig_Count{Count: 1}},
	}
	tbwr.IncRetries()

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
	assert.Equal(t, types.OperationStateError, op.GetState())
	tbwr = *op.(*types.TakeBackupWithRetryOperation)
	assert.Equal(t, 1, tbwr.Retries)
	assert.Equal(t, "retry attempts exceeded limit: 1.", tbwr.Message)
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
	assert.Equal(t, float64(1), metrics.GetMetrics()["operations_finished_count"])
}
