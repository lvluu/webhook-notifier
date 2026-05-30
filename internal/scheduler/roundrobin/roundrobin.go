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
// and advances. Customers remain in rotation until explicitly removed via
// Remove, so multiple workers can claim from the same Customer concurrently.
type roundRobin struct {
	mu        sync.Mutex
	customers []scheduler.CustomerID
	cursor    int
}

// New returns a round-robin Scheduler.
func New() scheduler.Scheduler {
	return &roundRobin{}
}

// Pick returns the next Customer in rotation and advances the cursor.
// Customers remain in rotation — the caller must call Remove when the
// Customer has no remaining work. batchHint is set to ClaimBatchSize so
// workers can claim multiple deliveries per round-trip.
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
	r.cursor = (r.cursor + 1) % len(r.customers)

	return customer, ClaimBatchSize, true, nil
}

// Remove removes the Customer from the rotation. No-op if not present.
func (r *roundRobin) Remove(customerID scheduler.CustomerID) {
	if customerID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for i, c := range r.customers {
		if c == customerID {
			if i < len(r.customers)-1 {
				copy(r.customers[i:], r.customers[i+1:])
			}
			r.customers = r.customers[:len(r.customers)-1]
			if i < r.cursor {
				r.cursor--
			}
			if len(r.customers) > 0 && r.cursor >= len(r.customers) {
				r.cursor = 0
			}
			return
		}
	}
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
