package handlers

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"testing"
	"time"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/types"
)

func TestRBOperationHandlerInvalidOperationResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rbOp := types.RestoreBackupOperation{
		Id:                  types.GenerateObjectID(),
		BackupId:            types.GenerateObjectID(),
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
	}

	opMap := make(map[types.ObjectID]types.Operation)
	opMap[rbOp.Id] = &rbOp

	clientConnector := client.NewMockClientConnector()
	dbConnector := db.NewMockDBConnector(
		db.WithOperations(opMap),
	)

	// try to handle rb operation with non-existing ydb operation id
	handler := NewRBOperationHandler(dbConnector, clientConnector, config.Config{})
	err := handler(ctx, &rbOp)
	assert.Empty(t, err)

	op, err := dbConnector.GetOperation(ctx, rbOp.Id)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())
	assert.Equal(t, "Error status: NOT_FOUND, issues: message:\"operation not found\"", op.GetMessage())
}

func TestRBOperationHandlerDeadlineExceededForPendingOperation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	rbOp := types.RestoreBackupOperation{
		Id:                  types.GenerateObjectID(),
		BackupId:            types.GenerateObjectID(),
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      ydbOp.Id,
		CreatedAt:           time.Now(),
	}

	opMap := make(map[types.ObjectID]types.Operation)
	opMap[rbOp.Id] = &rbOp
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	ydbOpMap[ydbOp.Id] = ydbOp

	clientConnector := client.NewMockClientConnector(client.WithOperations(ydbOpMap))
	dbConnector := db.NewMockDBConnector(db.WithOperations(opMap))

	// try to handle pending rb operation with zero ttl
	handler := NewRBOperationHandler(dbConnector, clientConnector, config.Config{})
	err := handler(ctx, &rbOp)
	assert.Empty(t, err)

	// check operation status (should be cancelled because of deadline exceeded)
	op, err := dbConnector.GetOperation(ctx, rbOp.Id)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateCancelling, op.GetState())
	assert.Equal(t, "Operation deadline exceeded", op.GetMessage())

	// check ydb operation status (should be cancelled)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, rbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_CANCELLED, ydbOpStatus.GetOperation().GetStatus())
}

func TestRBOperationHandlerPendingOperationInProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	rbOp := types.RestoreBackupOperation{
		Id:                  types.GenerateObjectID(),
		BackupId:            types.GenerateObjectID(),
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      ydbOp.Id,
		CreatedAt:           time.Now(),
	}

	opMap := make(map[types.ObjectID]types.Operation)
	opMap[rbOp.Id] = &rbOp
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	ydbOpMap[ydbOp.Id] = ydbOp

	clientConnector := client.NewMockClientConnector(client.WithOperations(ydbOpMap))
	dbConnector := db.NewMockDBConnector(db.WithOperations(opMap))

	// try to handle pending rb operation with ttl
	handler := NewRBOperationHandler(dbConnector, clientConnector, config.Config{OperationTtlSeconds: 1000})
	err := handler(ctx, &rbOp)
	assert.Empty(t, err)

	// check operation status (should be pending)
	op, err := dbConnector.GetOperation(ctx, rbOp.Id)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStatePending, op.GetState())

	// check ydb operation status (should be in progress
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, rbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_SUCCESS, ydbOpStatus.GetOperation().GetStatus())
	assert.Equal(t, false, ydbOpStatus.GetOperation().GetReady())
}

func TestRBOperationHandlerPendingOperationCompletedSuccessfully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	rbOp := types.RestoreBackupOperation{
		Id:                  types.GenerateObjectID(),
		BackupId:            types.GenerateObjectID(),
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      ydbOp.Id,
		CreatedAt:           time.Now(),
	}

	opMap := make(map[types.ObjectID]types.Operation)
	opMap[rbOp.Id] = &rbOp
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	ydbOpMap[ydbOp.Id] = ydbOp

	clientConnector := client.NewMockClientConnector(client.WithOperations(ydbOpMap))
	dbConnector := db.NewMockDBConnector(db.WithOperations(opMap))

	handler := NewRBOperationHandler(dbConnector, clientConnector, config.Config{OperationTtlSeconds: 1000})
	err := handler(ctx, &rbOp)
	assert.Empty(t, err)

	// check operation status (should be done)
	op, err := dbConnector.GetOperation(ctx, rbOp.Id)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateDone, op.GetState())

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, rbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())
}

func TestRBOperationHandlerPendingOperationCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_CANCELLED,
		Issues: nil,
	}

	rbOp := types.RestoreBackupOperation{
		Id:                  types.GenerateObjectID(),
		BackupId:            types.GenerateObjectID(),
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      ydbOp.Id,
		CreatedAt:           time.Now(),
	}

	opMap := make(map[types.ObjectID]types.Operation)
	opMap[rbOp.Id] = &rbOp
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	ydbOpMap[ydbOp.Id] = ydbOp

	clientConnector := client.NewMockClientConnector(client.WithOperations(ydbOpMap))
	dbConnector := db.NewMockDBConnector(db.WithOperations(opMap))

	handler := NewRBOperationHandler(dbConnector, clientConnector, config.Config{OperationTtlSeconds: 10})
	err := handler(ctx, &rbOp)
	assert.Empty(t, err)

	// check operation status (should be error)
	op, err := dbConnector.GetOperation(ctx, rbOp.Id)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())
	assert.Equal(t, "Pending operation was cancelled", op.GetMessage())

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, rbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())
}

func TestRBOperationHandlerDeadlineExceededForCancellingOperation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	rbOp := types.RestoreBackupOperation{
		Id:                  types.GenerateObjectID(),
		BackupId:            types.GenerateObjectID(),
		State:               types.OperationStateCancelling,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      ydbOp.Id,
		CreatedAt:           time.Now(),
	}

	opMap := make(map[types.ObjectID]types.Operation)
	opMap[rbOp.Id] = &rbOp
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	ydbOpMap[ydbOp.Id] = ydbOp

	clientConnector := client.NewMockClientConnector(client.WithOperations(ydbOpMap))
	dbConnector := db.NewMockDBConnector(db.WithOperations(opMap))

	// try to handle cancelling rb operation with zero ttl
	handler := NewRBOperationHandler(dbConnector, clientConnector, config.Config{})
	err := handler(ctx, &rbOp)
	assert.Empty(t, err)

	// check operation status (should be failed because of deadline exceeded)
	op, err := dbConnector.GetOperation(ctx, rbOp.Id)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())
	assert.Equal(t, "Operation deadline exceeded", op.GetMessage())

	// check ydb operation status (should be in progress)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, rbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_SUCCESS, ydbOpStatus.GetOperation().GetStatus())
	assert.Equal(t, false, ydbOpStatus.GetOperation().GetReady())
}

func TestRBOperationHandlerCancellingOperationInProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	rbOp := types.RestoreBackupOperation{
		Id:                  types.GenerateObjectID(),
		BackupId:            types.GenerateObjectID(),
		State:               types.OperationStateCancelling,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      ydbOp.Id,
		CreatedAt:           time.Now(),
	}

	opMap := make(map[types.ObjectID]types.Operation)
	opMap[rbOp.Id] = &rbOp
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	ydbOpMap[ydbOp.Id] = ydbOp

	clientConnector := client.NewMockClientConnector(client.WithOperations(ydbOpMap))
	dbConnector := db.NewMockDBConnector(db.WithOperations(opMap))

	handler := NewRBOperationHandler(dbConnector, clientConnector, config.Config{OperationTtlSeconds: 1000})
	err := handler(ctx, &rbOp)
	assert.Empty(t, err)

	// check operation status (should be the same as before)
	op, err := dbConnector.GetOperation(ctx, rbOp.Id)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateCancelling, op.GetState())

	// check ydb operation status (should be in progress)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, rbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_SUCCESS, ydbOpStatus.GetOperation().GetStatus())
	assert.Equal(t, false, ydbOpStatus.GetOperation().GetReady())
}

func TestRBOperationHandlerCancellingOperationCompletedSuccessfully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	rbOp := types.RestoreBackupOperation{
		Id:                  types.GenerateObjectID(),
		BackupId:            types.GenerateObjectID(),
		State:               types.OperationStateCancelling,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      ydbOp.Id,
		CreatedAt:           time.Now(),
	}

	opMap := make(map[types.ObjectID]types.Operation)
	opMap[rbOp.Id] = &rbOp
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	ydbOpMap[ydbOp.Id] = ydbOp

	clientConnector := client.NewMockClientConnector(client.WithOperations(ydbOpMap))
	dbConnector := db.NewMockDBConnector(db.WithOperations(opMap))

	handler := NewRBOperationHandler(dbConnector, clientConnector, config.Config{OperationTtlSeconds: 10})
	err := handler(ctx, &rbOp)
	assert.Empty(t, err)

	// check operation status (should be done)
	op, err := dbConnector.GetOperation(ctx, rbOp.Id)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateDone, op.GetState())
	assert.Equal(t, "Operation was completed despite cancellation", op.GetMessage())

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, rbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())
}

func TestRBOperationHandlerCancellingOperationCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_CANCELLED,
		Issues: nil,
	}

	rbOp := types.RestoreBackupOperation{
		Id:                  types.GenerateObjectID(),
		BackupId:            types.GenerateObjectID(),
		State:               types.OperationStateCancelling,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      ydbOp.Id,
		CreatedAt:           time.Now(),
	}

	opMap := make(map[types.ObjectID]types.Operation)
	opMap[rbOp.Id] = &rbOp
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	ydbOpMap[ydbOp.Id] = ydbOp

	clientConnector := client.NewMockClientConnector(client.WithOperations(ydbOpMap))
	dbConnector := db.NewMockDBConnector(db.WithOperations(opMap))

	handler := NewRBOperationHandler(dbConnector, clientConnector, config.Config{OperationTtlSeconds: 10})
	err := handler(ctx, &rbOp)
	assert.Empty(t, err)

	// check operation status (should be cancelled)
	op, err := dbConnector.GetOperation(ctx, rbOp.Id)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateCancelled, op.GetState())

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, rbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())

}

func TestRBOperationHandlerRetriableErrorForPendingOperation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_UNAVAILABLE,
		Issues: nil,
	}

	rbOp := types.RestoreBackupOperation{
		Id:                  types.GenerateObjectID(),
		BackupId:            types.GenerateObjectID(),
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      ydbOp.Id,
		CreatedAt:           time.Now(),
	}

	opMap := make(map[types.ObjectID]types.Operation)
	opMap[rbOp.Id] = &rbOp
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	ydbOpMap[ydbOp.Id] = ydbOp

	clientConnector := client.NewMockClientConnector(client.WithOperations(ydbOpMap))
	dbConnector := db.NewMockDBConnector(db.WithOperations(opMap))

	handler := NewRBOperationHandler(dbConnector, clientConnector, config.Config{OperationTtlSeconds: 10})
	err := handler(ctx, &rbOp)
	assert.Empty(t, err)

	// check operation status (should be the same as before)
	op, err := dbConnector.GetOperation(ctx, rbOp.Id)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStatePending, op.GetState())

	// check ydb operation status
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, rbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_UNAVAILABLE, ydbOpStatus.GetOperation().GetStatus())
}
