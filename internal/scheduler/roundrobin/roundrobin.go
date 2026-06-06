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
// Customer at the cursor and removes it from the active list. Workers re-Notify
// Customers that still have claimable work, so a Customer is not picked
// concurrently by multiple workers within the same process.
type roundRobin struct {
	mu     sync.Mutex
	list   *list.List
	index  map[scheduler.CustomerID]*list.Element
	cursor *list.Element
	wake   chan struct{}
}

// New returns a round-robin Scheduler with O(1) Notify and Remove.
func New() scheduler.Scheduler {
	r := &roundRobin{
		list:  list.New(),
		index: make(map[scheduler.CustomerID]*list.Element),
		wake:  make(chan struct{}),
	}
	return r
}

// Pick returns the next Customer in rotation and removes it from the list.
// The caller must call Notify to re-add the Customer after a successful claim
// or after confirming that the Customer still has claimable work. This prevents
// multiple workers from picking the same Customer concurrently, reducing empty
// claims from intra-process contention.
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

	elem := r.cursor
	customerID := elem.Value.(scheduler.CustomerID)
	r.cursor = elem.Next()
	delete(r.index, customerID)
	r.list.Remove(elem)

	return customerID, ClaimBatchSize, true, nil
}

// WaitForWork blocks until at least one Customer is registered or ctx is
// cancelled. Callers use this to avoid polling the database when the
// scheduler has no work queued.
func (r *roundRobin) WaitForWork(ctx context.Context) error {
	r.mu.Lock()
	if r.list.Len() > 0 {
		r.mu.Unlock()
		return nil
	}
	wake := r.wake
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	}
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
// present. Idempotent. O(1). Wakes any workers blocked in WaitForWork.
func (r *roundRobin) Notify(customerID scheduler.CustomerID) {
	if customerID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.index[customerID]; ok {
		return
	}

	wasEmpty := r.list.Len() == 0
	r.index[customerID] = r.list.PushBack(customerID)
	if wasEmpty {
		close(r.wake)
		r.wake = make(chan struct{})
	}
}
