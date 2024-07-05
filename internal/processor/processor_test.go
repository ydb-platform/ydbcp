package processor

import (
	"context"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"sync"
	"testing"
	"time"
	"ydbcp/internal/types"
	"ydbcp/internal/util/ticker"
	"ydbcp/internal/util/xlog"
	ydbcp_db_connector "ydbcp/internal/ydbcp-db-connector"
)

func TestProcessor(t *testing.T) {
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

	db := ydbcp_db_connector.NewFakeYdbConnector()
	handlers := NewOperationHandlerRegistry(db)
	operationTypeTB := types.OperationType("TB")
	handlerCalled := make(chan struct{})
	handlers.Add(
		operationTypeTB,
		func(ctx context.Context, op types.Operation) (types.Operation, error) {
			xlog.Debug(
				ctx, "TB handler called for operation",
				zap.String("operation", types.OperationToString(op)),
			)
			op.SetState(types.OperationStateDone)
			op.SetMessage("Success")
			handlerCalled <- struct{}{}
			return op, nil
		},
	)

	_ = NewOperationProcessor(
		ctx,
		&wg,
		db,
		handlers,
		WithTickerProvider(tickerProvider),
		WithPeriod(time.Second*10),
	)

	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, 10*time.Second, "incorrect period")
	}

	t0 := clock.Now()
	fakeTicker.Send(t0)

	opID, _ := db.CreateOperation(
		ctx,
		&types.TakeBackupOperation{
			Type:  operationTypeTB,
			State: types.OperationStatePending,
		},
	)

	t1 := t0.Add(10 * time.Second)
	clock.Advance(10 * time.Second)
	fakeTicker.Send(t1)

	t2 := t1.Add(10 * time.Second)
	clock.Advance(10 * time.Second)
	fakeTicker.Send(t2)

	select {
	case <-ctx.Done():
		t.Error("operation handler has not been called")
	case <-handlerCalled:
	}

	cancel()
	wg.Wait()

	op, err := db.GetOperation(ctx, opID)
	assert.Empty(t, err)
	assert.Equal(
		t, op.GetState(), types.OperationStateDone,
		"operation state should be Done",
	)
}
