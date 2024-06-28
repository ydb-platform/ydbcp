package ticker

import "time"

// Ticker interface describe common ticker
type Ticker interface {
	Chan() <-chan time.Time
	Stop()
}

// TickerProvider is factory for Ticker
type TickerProvider func(period time.Duration) Ticker

// RealTicker is real time.Ticker
type RealTicker struct {
	timer *time.Ticker
}

// NewRealTicker returns RealTicker
func NewRealTicker(period time.Duration) Ticker {
	return &RealTicker{
		timer: time.NewTicker(period),
	}
}

// Chan returns ticker channel
func (t *RealTicker) Chan() <-chan time.Time {
	return t.timer.C
}

// Stop ticker
func (t *RealTicker) Stop() {
	t.timer.Stop()
}

// FakeTicker is fake ticker for tests
type FakeTicker struct {
	tickChan chan time.Time
	Stopped  bool
	Period   time.Duration
}

// NewFakeTicker returns created fake ticker
func NewFakeTicker(period time.Duration) *FakeTicker {
	return &FakeTicker{
		tickChan: make(chan time.Time, 1),
		Stopped:  false,
		Period:   period,
	}
}

// Chan returns ticker channel
func (t *FakeTicker) Chan() <-chan time.Time {
	return t.tickChan
}

// Stop ticker
func (t *FakeTicker) Stop() {
	t.Stopped = true
}

// Send send ticker event
func (t *FakeTicker) Send(tick time.Time) {
	t.tickChan <- tick
}
