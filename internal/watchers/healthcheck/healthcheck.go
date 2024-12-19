package healthcheck

import (
	"context"
	"sync"
	"time"
	"ydbcp/internal/metrics"
	"ydbcp/internal/watchers"
)

func NewHealthCheck(
	ctx context.Context,
	wg *sync.WaitGroup,
	options ...watchers.Option,
) *watchers.WatcherImpl {
	return watchers.NewWatcher(
		ctx,
		wg,
		func(_ context.Context, _ time.Duration) {
			metrics.GlobalMetricsRegistry.ReportHealthCheck()
		},
		time.Second,
		"HealthCheck",
		options...,
	)
}
