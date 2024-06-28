package timer

import "time"

// Timer describes common timer interface
type Timer interface {
	Chan() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

// TimerProvider is factory function for Timer
type TimerProvider func(duration time.Duration) Timer

// RealTimer is real time.Timer
type RealTimer struct {
	timer *time.Timer
}

// NewRealTimer create and return Timer
func NewRealTimer(duration time.Duration) Timer {
	return &RealTimer{
		timer: time.NewTimer(duration),
	}
}

// Chan returns timer channel
func (t *RealTimer) Chan() <-chan time.Time {
	return t.timer.C
}

// Stop timer and return status of  timer
func (t *RealTimer) Stop() bool {
	return t.timer.Stop()
}

// Reset timer with duration and return status of timer
func (t *RealTimer) Reset(duration time.Duration) bool {
	return t.timer.Reset(duration)
}

// FakeTimer describes fake timer for testing
type FakeTimer struct {
	timerChan chan time.Time
	Stopped   bool
	Duration  time.Duration
}

// NewFakeTimer create and return FakeTimer
func NewFakeTimer(duration time.Duration) *FakeTimer {
	return &FakeTimer{
		timerChan: make(chan time.Time, 1),
		Stopped:   false,
		Duration:  duration,
	}
}

// Chan return timer channel
func (t *FakeTimer) Chan() <-chan time.Time {
	return t.timerChan
}

// Stop timer and return current status of timer
func (t *FakeTimer) Stop() bool {
	stopped := t.Stopped
	t.Stopped = true
	return stopped
}

// Reset timer with duration and return status of timer
func (t *FakeTimer) Reset(duration time.Duration) bool {
	stopped := t.Stopped
	t.Stopped = false
	t.Duration = duration
	return stopped
}

// Send activate timer channel
func (t *FakeTimer) Send(tick time.Time) {
	t.timerChan <- tick
}
