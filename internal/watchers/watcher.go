package watchers

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"sync"
	"time"
	"ydbcp/internal/util/ticker"
	"ydbcp/internal/util/xlog"
)

type WatcherAction func(context.Context, time.Duration)

type WatcherImpl struct {
	ctx            context.Context
	period         time.Duration
	tickerProvider ticker.TickerProvider
	action         WatcherAction
	prefixName     string
}

type Option func(*WatcherImpl)

func WithTickerProvider(ticketProvider ticker.TickerProvider) Option {
	return func(o *WatcherImpl) {
		o.tickerProvider = ticketProvider
	}
}

func NewWatcher(
	ctx context.Context,
	wg *sync.WaitGroup,
	action WatcherAction,
	period time.Duration,
	prefixName string,
	options ...Option,
) *WatcherImpl {
	watcher := &WatcherImpl{
		ctx:            ctx,
		period:         period,
		tickerProvider: ticker.NewRealTicker,
		action:         action,
		prefixName:     prefixName,
	}

	for _, opt := range options {
		opt(watcher)
	}

	wg.Add(1)
	go watcher.run(wg)
	return watcher
}

func (o *WatcherImpl) run(wg *sync.WaitGroup) {
	defer wg.Done()
	xlog.Debug(o.ctx, fmt.Sprintf("%s watcher started", o.prefixName), zap.Duration("period", o.period))
	ticker := o.tickerProvider(o.period)
	for {
		select {
		case <-o.ctx.Done():
			ticker.Stop()
			xlog.Debug(o.ctx, fmt.Sprintf("%s watcher stopped", o.prefixName))
			return
		case <-ticker.Chan():
			xlog.Debug(o.ctx, fmt.Sprintf("Starting %s watcher action", o.prefixName))
			o.action(o.ctx, o.period)
			xlog.Debug(o.ctx, fmt.Sprintf("%s watcher action was completed", o.prefixName))
		}
	}
}
