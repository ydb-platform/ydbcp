package processor

import (
	"context"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"sync"
	"testing"
	"time"
	"ydbcp/internal/client_db_connector"
	"ydbcp/internal/types"
	"ydbcp/internal/util/ticker"
	ydbcp_db_connector "ydbcp/internal/ydbcp-db-connector"
)

func TestTBOperationHandler(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var fakeTicker *ticker.FakeTicker
	tickerInitialized := make(chan struct{})
	tickerProvider := func(duration time.Duration) ticker.Ticker {
		assert.Empty(t, fakeTicker, "ticker reuse")
		fakeTicker = ticker.NewFakeTicker(duration)
		tickerInitialized <- struct{}{}
		return fakeTicker
	}

	clock := clockwork.NewFakeClock()

	opId := types.GenerateObjectID()
	backupId := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		Id:                  opId,
		BackupId:            backupId,
		Type:                types.OperationType("TB"),
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
	}
	backup := types.Backup{
		Id:          backupId,
		OperationId: opId,
		Status:      types.BackupStatePending,
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
	backupMap[backupId] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	db := ydbcp_db_connector.NewFakeYdbConnector(
		ydbcp_db_connector.WithBackups(backupMap),
		ydbcp_db_connector.WithOperations(opMap),
	)
	clientDb := client_db_connector.NewMockClientDbConnector(
		client_db_connector.WithOperations(ydbOpMap),
	)
	handlers := NewOperationHandlerRegistry(db)
	operationTypeTB := types.OperationType("TB")
	handlers.Add(
		operationTypeTB,
		MakeTBOperationHandler(db, clientDb),
	)

	processor := NewOperationProcessor(
		ctx,
		&wg,
		db,
		handlers,
		WithTickerProvider(tickerProvider),
		WithPeriod(time.Second*10),
		WithExportResults(true),
	)

	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, 10*time.Second, "incorrect period")
	}

	t0 := clock.Now()
	fakeTicker.Send(t0)

	t1 := t0.Add(10 * time.Second)
	clock.Advance(10 * time.Second)
	fakeTicker.Send(t1)

	t2 := t1.Add(10 * time.Second)
	clock.Advance(10 * time.Second)
	fakeTicker.Send(t2)

	select {
	case <-ctx.Done():
		t.Error("result handler not invoked")
	case <-processor.ResultCounter:
	}

	cancel()
	wg.Wait()

	op, err := db.GetOperation(ctx, opId)
	assert.Empty(t, err)
	assert.Equal(
		t, op.GetState(), types.OperationStateDone,
		"operation state should be Done",
	)
	backups, err2 := db.SelectBackups(ctx, types.BackupStateAvailable)
	assert.Empty(t, err2)
	assert.Equal(t, 1, len(backups))
	assert.Equal(t, types.BackupStateAvailable, backups[0].Status)
}
