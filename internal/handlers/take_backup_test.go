package handlers

import (
	"context"
	"testing"
	"time"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db/yql/queries"

	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/types"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
)

func TestTBOperationHandlerInvalidOperationResponse(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupId:            backupID,
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStatePending,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector()
	// try to handle tb operation with non-existing ydb operation id
	handler := NewTBOperationHandler(dbConnector, clientConnector, config.Config{}, queries.NewWriteTableQueryMock)
	err := handler(ctx, &tbOp)
	assert.Empty(t, err)

	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())
	assert.Equal(t, "Error status: NOT_FOUND, issues: message:\"operation not found\"", op.GetMessage())
}

func TestTBOperationHandlerDeadlineExceededForPendingOperation(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupId:            backupID,
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStatePending,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, config.Config{
			OperationTtlSeconds: 0,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &tbOp)
	assert.Empty(t, err)

	// check operation status (should be cancelled because of deadline exceeded)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateCancelling, op.GetState())
	assert.Equal(t, "Operation deadline exceeded", op.GetMessage())

	// check backup status (should be error)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateError, b.Status)

	// check ydb operation status (should be cancelled)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_CANCELLED, ydbOpStatus.GetOperation().GetStatus())
}

func TestTBOperationHandlerPendingOperationInProgress(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupId:            backupID,
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		CreatedAt:           time.Now(),
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStatePending,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &tbOp)
	assert.Empty(t, err)

	// check operation status (should be pending)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStatePending, op.GetState())
	assert.Equal(t, "", op.GetMessage())

	// check backup status (should be in pending)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStatePending, b.Status)

	// check ydb operation status (should be in progress)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_SUCCESS, ydbOpStatus.GetOperation().GetStatus())
	assert.Equal(t, false, ydbOpStatus.GetOperation().GetReady())
}

func TestTBOperationHandlerPendingOperationCompletedSuccessfully(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupId:            backupID,
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		CreatedAt:           time.Now(),
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStatePending,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &tbOp)
	assert.Empty(t, err)

	// check operation status (should be done)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateDone, op.GetState())

	// check backup status (should be done)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateAvailable, b.Status)

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())
}

func TestTBOperationHandlerPendingOperationCancelled(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupId:            backupID,
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		CreatedAt:           time.Now(),
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStatePending,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_CANCELLED,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &tbOp)
	assert.Empty(t, err)

	// check operation status (should be error)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())

	// check backup status (should be error)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateError, b.Status)

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())
}

func TestTBOperationHandlerDeadlineExceededForCancellingOperation(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupId:            backupID,
		State:               types.OperationStateCancelling,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		CreatedAt:           time.Now(),
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStatePending,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, config.Config{
			OperationTtlSeconds: 0,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &tbOp)
	assert.Empty(t, err)

	// check operation status (should be failed because of deadline exceeded)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())
	assert.Equal(t, "Operation deadline exceeded", op.GetMessage())

	// check backup status (should be error)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateError, b.Status)

	// check ydb operation status (should be in progress)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_SUCCESS, ydbOpStatus.GetOperation().GetStatus())
	assert.Equal(t, false, ydbOpStatus.GetOperation().GetReady())
}

func TestTBOperationHandlerCancellingOperationInProgress(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupId:            backupID,
		State:               types.OperationStateCancelling,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		CreatedAt:           time.Now(),
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStatePending,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &tbOp)
	assert.Empty(t, err)

	// check operation status (should be the same as before)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateCancelling, op.GetState())

	// check backup status (should be pending)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStatePending, b.Status)

	// check ydb operation status (should be in progress)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_SUCCESS, ydbOpStatus.GetOperation().GetStatus())
	assert.Equal(t, false, ydbOpStatus.GetOperation().GetReady())
}

func TestTBOperationHandlerCancellingOperationCompletedSuccessfully(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupId:            backupID,
		State:               types.OperationStateCancelling,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		CreatedAt:           time.Now(),
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStatePending,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &tbOp)
	assert.Empty(t, err)
	// check operation status (should be done)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateDone, op.GetState())
	assert.Equal(t, "Operation was completed despite cancellation", op.GetMessage())

	// check backup status (should be available)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateAvailable, b.Status)

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())
}

func TestTBOperationHandlerCancellingOperationCancelled(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupId:            backupID,
		State:               types.OperationStateCancelling,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		CreatedAt:           time.Now(),
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStatePending,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_CANCELLED,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &tbOp)
	assert.Empty(t, err)

	// check operation status (should be cancelled)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateCancelled, op.GetState())

	// check backup status (should be cancelled)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateCancelled, b.Status)

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())

}

func TestTBOperationHandlerRetriableErrorForPendingOperation(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupId:            backupID,
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		CreatedAt:           time.Now(),
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStatePending,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_UNAVAILABLE,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &tbOp)
	assert.Empty(t, err)

	// check operation status (should be the same as before)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStatePending, op.GetState())

	// check backup status (should be pending)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStatePending, b.Status)

	// check ydb operation status
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_UNAVAILABLE, ydbOpStatus.GetOperation().GetStatus())
}
