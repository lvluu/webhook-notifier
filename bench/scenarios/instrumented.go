package scenarios

import (
	"context"
	"sync/atomic"

	"github.com/lvluu/webhook-notifier/internal/scheduler"
)

// instrumentedScheduler wraps a Scheduler and counts Pick invocations.
// Used by Scenario 1 to measure the round-robin cycle bound for the
// late-arrival acceptance criterion.
type instrumentedScheduler struct {
	inner      scheduler.Scheduler
	totalPicks atomic.Int64
}

func newInstrumentedScheduler(inner scheduler.Scheduler) *instrumentedScheduler {
	return &instrumentedScheduler{inner: inner}
}

// Notify delegates to the inner Scheduler.
func (is *instrumentedScheduler) Notify(customerID string) {
	is.inner.Notify(customerID)
}

// Pick delegates to the inner Scheduler and increments the Pick counter on
// success.
func (is *instrumentedScheduler) Pick(ctx context.Context) (scheduler.CustomerID, uint32, bool, error) {
	id, hint, ok, err := is.inner.Pick(ctx)
	if ok {
		is.totalPicks.Add(1)
	}
	return id, hint, ok, err
}

// TotalPicks returns the number of successful Picks since creation.
func (is *instrumentedScheduler) TotalPicks() int64 {
	return is.totalPicks.Load()
}
