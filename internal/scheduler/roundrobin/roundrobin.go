// Package roundrobin implements a plain round-robin Scheduler.
package roundrobin

import (
	"context"
	"sync"

	"github.com/lvluu/webhook-notifier/internal/scheduler"
)

// ClaimBatchSize is the number of deliveries a worker can claim per scheduler
// pick. Higher values amortize DB transaction overhead across multiple deliveries.
const ClaimBatchSize uint32 = 32

// roundRobin is a plain round-robin Scheduler over the set of Customers with
// pending Deliveries (ADR-0001).
//
// State: an ordered slice of CustomerIDs and a cursor. Notify appends a
// Customer when not already present; Pick returns the Customer at the cursor
// and removes it from rotation. The worker loop honours the "set of Customers
// with pending Deliveries" contract by re-calling Notify after each
// successful claim — if the claim returns ok=false the worker does not
// re-Notify and the Customer leaves rotation naturally.
type roundRobin struct {
	mu        sync.Mutex
	customers []scheduler.CustomerID
	cursor    int
}

// New returns a round-robin Scheduler.
func New() scheduler.Scheduler {
	return &roundRobin{}
}

// Pick returns the next Customer in rotation and removes it from the active
// set, or ok=false when empty. batchHint is set to ClaimBatchSize so workers
// can claim multiple deliveries per round-trip, amortizing DB overhead.
// The caller must re-Notify if the Customer still has pending work.
func (r *roundRobin) Pick(ctx context.Context) (scheduler.CustomerID, uint32, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, false, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.customers) == 0 {
		return "", 0, false, nil
	}

	if r.cursor >= len(r.customers) {
		r.cursor = 0
	}

	customer := r.customers[r.cursor]
	// Remove by shifting elements after cursor left by one. This preserves
	// insertion order so round-robin cycles through Customers in the order
	// they were Notify'd.
	copy(r.customers[r.cursor:], r.customers[r.cursor+1:])
	r.customers = r.customers[:len(r.customers)-1]
	// Clamp cursor if we removed the last element.
	if len(r.customers) > 0 && r.cursor >= len(r.customers) {
		r.cursor = 0
	}

	return customer, ClaimBatchSize, true, nil
}

// Notify registers the Customer for round-robin rotation if not already
// present. Idempotent.
func (r *roundRobin) Notify(customerID scheduler.CustomerID) {
	if customerID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, c := range r.customers {
		if c == customerID {
			return
		}
	}
	r.customers = append(r.customers, customerID)
}
