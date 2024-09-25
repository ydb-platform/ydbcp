package ttl_watcher

import (
	"context"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/types"
	"ydbcp/internal/util/ticker"
	"ydbcp/internal/watchers"
)

func TestTtlWatcher(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prepare fake clock and ticker
	clock := clockwork.NewFakeClock()
	var fakeTicker *ticker.FakeTicker
	tickerInitialized := make(chan struct{})
	tickerProvider := func(duration time.Duration) ticker.Ticker {
		assert.Empty(t, fakeTicker, "ticker reuse")
		fakeTicker = ticker.NewFakeTicker(duration)
		tickerInitialized <- struct{}{}
		return fakeTicker
	}

	// Prepare a backup
	backupID := types.GenerateObjectID()
	expireAt := time.Now()
	backup := types.Backup{
		ID:       backupID,
		Status:   types.BackupStateAvailable,
		ExpireAt: &expireAt,
	}
	backupMap := make(map[string]types.Backup)
	backupMap[backupID] = backup

	// Prepare mock db and ttl watcher
	db := db.NewMockDBConnector(
		db.WithBackups(backupMap),
	)
	_ = NewTtlWatcher(
		ctx,
		&wg,
		db,
		queries.NewWriteTableQueryMock,
		watchers.WithTickerProvider(tickerProvider),
	)

	// Wait for the ticker to be initialized
	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, time.Hour, "incorrect period")
	}

	// Send a tick to the fake ticker
	t0 := clock.Now().Add(time.Hour)
	fakeTicker.Send(t0)

	cancel()
	wg.Wait()

	// Check that DeleteBackup operation was created
	ops, err := db.ActiveOperations(ctx)
	assert.Empty(t, err)
	assert.Equal(t, len(ops), 1)
	assert.Equal(t, ops[0].GetType(), types.OperationTypeDB, "operation type should be DB")
	assert.Equal(t, ops[0].GetState(), types.OperationStatePending, "operation state should be Pending")
}
