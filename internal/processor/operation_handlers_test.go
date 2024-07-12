package processor

import (
	"context"
	"sync"
	"testing"

	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/types"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
)

func TestTBOperationHandler(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		Id:                  opId,
		BackupId:            backupID,
		Type:                types.OperationType("TB"),
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
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[types.ObjectID]types.Operation)
	backupMap := make(map[types.ObjectID]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	db := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	client := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	handler := MakeTBOperationHandler(db, client)

	result, err := handler(ctx, &tbOp)

	assert.Empty(t, err)
	assert.Equal(
		t, result.GetState(), types.OperationStateDone,
		"operation state should be Done",
	)

	backups, err2 := db.SelectBackups(ctx, types.BackupStateAvailable)
	assert.Empty(t, err2)
	assert.Equal(t, 1, len(backups))
	assert.Equal(t, types.BackupStateAvailable, backups[0].Status)

	cancel()
	wg.Wait()
}
