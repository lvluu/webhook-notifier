package scenarios

import (
	"context"
	"sync/atomic"

	"github.com/lvluu/webhook-notifier/internal/scheduler"
)

// instrumentedScheduler wraps a Scheduler and counts Pick invocations.
// Used by Scenario 1 to measure the round-robin cycle bound for the
// late-arrival acceptance criterion.
//
// It also supports per-customer pick tracking: call BeginTracking(customerID)
// to start counting picks until the tracked customer is first returned by Pick.
// CyclePicks() returns the number of picks between BeginTracking and that
// first service.
type instrumentedScheduler struct {
	inner           scheduler.Scheduler
	totalPicks      atomic.Int64
	cycleStartPicks atomic.Int64
	firstPickTotal  atomic.Int64
	trackedCustomer atomic.Value // stores string
}

func newInstrumentedScheduler(inner scheduler.Scheduler) *instrumentedScheduler {
	return &instrumentedScheduler{inner: inner}
}

// Notify delegates to the inner Scheduler.
func (is *instrumentedScheduler) Notify(customerID string) {
	is.inner.Notify(customerID)
}

// Remove delegates to the inner Scheduler.
func (is *instrumentedScheduler) Remove(customerID string) {
	is.inner.Remove(customerID)
}

// WaitForWork delegates to the inner Scheduler.
func (is *instrumentedScheduler) WaitForWork(ctx context.Context) error {
	return is.inner.WaitForWork(ctx)
}

// Pick delegates to the inner Scheduler and increments the Pick counter on
// success. If the returned customer matches the tracked customer and
// firstPickTotal hasn't been set yet, records the pick as the first service.
func (is *instrumentedScheduler) Pick(ctx context.Context) (scheduler.CustomerID, uint32, bool, error) {
	id, hint, ok, err := is.inner.Pick(ctx)
	if ok {
		picks := is.totalPicks.Add(1)
		tracked := is.trackedCustomer.Load()
		if tracked != nil {
			if trackedStr, ok := tracked.(string); ok && trackedStr != "" && string(id) == trackedStr {
				is.firstPickTotal.CompareAndSwap(0, picks)
			}
		}
	}
	return id, hint, ok, err
}

// TotalPicks returns the number of successful Picks since creation.
func (is *instrumentedScheduler) TotalPicks() int64 {
	return is.totalPicks.Load()
}

// BeginTracking records the current Pick count and starts watching for the
// given customer. Call before Notify so the cycle-start baseline is captured
// before the customer can be picked.
func (is *instrumentedScheduler) BeginTracking(customerID string) {
	is.cycleStartPicks.Store(is.totalPicks.Load())
	is.trackedCustomer.Store(customerID)
	// firstPickTotal stays at 0 until the tracked customer is first picked
}

// CyclePicks returns the number of Pick invocations between BeginTracking and
// the first time the tracked customer was returned by Pick. If the tracked
// customer was never picked, falls back to total picks since BeginTracking.
func (is *instrumentedScheduler) CyclePicks() int64 {
	first := is.firstPickTotal.Load()
	start := is.cycleStartPicks.Load()
	if first == 0 {
		// Tracked customer was never picked — fall back to total delta.
		return is.totalPicks.Load() - start
	}
	return first - start
}
