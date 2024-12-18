package healthcheck

import (
	"context"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
	"ydbcp/internal/metrics"
	"ydbcp/internal/util/ticker"
	"ydbcp/internal/watchers"
)

func TestHealthCheckWatcher(t *testing.T) {
	clock := clockwork.NewFakeClock()
	metrics.InitializeMockMetricsRegistry()
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prepare fake clock and ticker
	var fakeTicker *ticker.FakeTicker
	tickerInitialized := make(chan struct{})
	tickerProvider := func(duration time.Duration) ticker.Ticker {
		assert.Empty(t, fakeTicker, "ticker reuse")
		fakeTicker = ticker.NewFakeTicker(duration)
		tickerInitialized <- struct{}{}
		return fakeTicker
	}

	actionOk := make(chan struct{})
	_ = NewHealthCheck(
		ctx,
		&wg,
		watchers.WithTickerProvider(tickerProvider),
		watchers.WithActionCompletedChannel(&actionOk),
	)

	// Wait for the ticker to be initialized
	select {
	case <-ctx.Done():
		t.Error("ticker not initialized")
	case <-tickerInitialized:
		assert.Equal(t, fakeTicker.Period, time.Second, "incorrect period")
	}

	// Send a tick to the fake ticker
	t0 := clock.Now().Add(time.Hour)
	fakeTicker.Send(t0)

	// Wait for the watcher action to be completed
	select {
	case <-ctx.Done():
		t.Error("action wasn't completed")
	case <-actionOk:
		cancel()
	}

	wg.Wait()

	assert.Equal(t, float64(1), metrics.GetMetrics()["healthcheck_gauge"])
}
