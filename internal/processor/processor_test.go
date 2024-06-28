package processor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"ydbcp/internal/types"
	"ydbcp/internal/util/ticker"
	"ydbcp/internal/util/xlog"
	ydbcp_db_connector "ydbcp/internal/ydbcp-db-connector"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type FakeYdbConnector struct {
	operations map[types.ObjectID]types.Operation
}

func NewFakeYdbConnector() *FakeYdbConnector {
	return &FakeYdbConnector{
		operations: make(map[types.ObjectID]types.Operation),
	}
}

func (c *FakeYdbConnector) SelectBackups(ctx context.Context, backupStatus string) ([]types.Backup, error) {
	return []types.Backup{}, ydbcp_db_connector.ErrUnimplemented
}

func (c *FakeYdbConnector) Close() {
}

func (c *FakeYdbConnector) ActiveOperations(_ context.Context) ([]types.Operation, error) {
	operations := make([]types.Operation, 0, len(c.operations))
	for _, op := range c.operations {
		if op.IsActive() {
			operations = append(operations, op)
		}
	}
	return operations, nil
}

func (c *FakeYdbConnector) UpdateOperation(_ context.Context, op types.Operation) error {
	if _, exist := c.operations[op.Id]; !exist {
		return fmt.Errorf("update nonexistent operation %s", op.String())
	}
	c.operations[op.Id] = op
	return nil
}

func (c *FakeYdbConnector) CreateOperation(_ context.Context, op types.Operation) (types.ObjectID, error) {
	var id types.ObjectID
	for {
		id = types.GenerateObjectID()
		if _, exist := c.operations[id]; !exist {
			break
		}
	}
	op.Id = id
	c.operations[id] = op
	return id, nil
}

func (c *FakeYdbConnector) GetOperation(_ context.Context, operationID types.ObjectID) (types.Operation, error) {
	if op, exist := c.operations[operationID]; exist {
		return op, nil
	}
	return types.Operation{}, fmt.Errorf("operation not found, id %s", operationID.String())
}

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

	db := NewFakeYdbConnector()
	handlers := NewOperationHandlerRegistry()
	operationTypeTB := types.OperationType("TB")
	handlerCalled := make(chan struct{})
	handlers.Add(operationTypeTB, func(ctx context.Context, op types.Operation) (types.Operation, error) {
		xlog.Debug(ctx, "TB handler called for operation", zap.String("operation", op.String()))
		op.State = types.OperationStateDone
		op.Message = "Success"
		handlerCalled <- struct{}{}
		return op, nil
	})

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

	opId, _ := db.CreateOperation(
		ctx,
		types.Operation{
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

	op, err := db.GetOperation(ctx, opId)
	assert.Empty(t, err)
	assert.Equal(t, op.State, types.OperationStateDone, "operation state should be Done")
}
