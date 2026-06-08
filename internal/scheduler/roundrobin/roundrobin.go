// Package roundrobin implements a plain round-robin Scheduler.
package roundrobin

import (
	"container/list"
	"context"
	"sync"

	"github.com/lvluu/webhook-notifier/internal/scheduler"
)

// ClaimBatchSize is the default number of deliveries a worker can claim per
// scheduler pick. Higher values amortize DB transaction overhead across
// multiple deliveries; lower values tighten per-Customer fairness because a
// worker yields back to rotation sooner.
const ClaimBatchSize uint32 = 64

// roundRobin is a plain round-robin Scheduler over the set of Customers with
// pending Deliveries (ADR-0001).
//
// State: a doubly-linked list of CustomerIDs and a cursor pointer. A map
// provides O(1) dedup and removal lookups so membership operations do not
// degrade with active Customer count.
//
// Notify appends a Customer when not already present; Pick returns the
// Customer at the cursor and advances. Customers remain in rotation until
// explicitly removed via Remove, so multiple workers can claim from the same
// Customer concurrently.
type roundRobin struct {
	mu     sync.Mutex
	list   *list.List
	index  map[scheduler.CustomerID]*list.Element
	cursor *list.Element
}

// New returns a round-robin Scheduler with O(1) Notify and Remove.
func New() scheduler.Scheduler {
	r := &roundRobin{
		list:  list.New(),
		index: make(map[scheduler.CustomerID]*list.Element),
	}
	return r
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

	if r.list.Len() == 0 {
		return "", 0, false, nil
	}

	if r.cursor == nil {
		r.cursor = r.list.Front()
	}

	customerID, ok := r.cursor.Value.(scheduler.CustomerID)
	if !ok {
		r.cursor = r.cursor.Next()
		return "", 0, false, nil
	}
	r.cursor = r.cursor.Next()

	return customerID, ClaimBatchSize, true, nil
}

// Remove removes the Customer from the rotation. O(1). No-op if not present.
func (r *roundRobin) Remove(customerID scheduler.CustomerID) {
	if customerID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	elem, ok := r.index[customerID]
	if !ok {
		return
	}

	if r.cursor == elem {
		r.cursor = elem.Next()
	}

	delete(r.index, customerID)
	r.list.Remove(elem)
}

// Notify registers the Customer for round-robin rotation if not already
// present. Idempotent. O(1).
func (r *roundRobin) Notify(customerID scheduler.CustomerID) {
	if customerID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.index[customerID]; ok {
		return
	}

	r.index[customerID] = r.list.PushBack(customerID)
}
