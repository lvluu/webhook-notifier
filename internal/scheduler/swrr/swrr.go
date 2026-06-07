// Package swrr implements a Smooth Weighted Round Robin Scheduler.
package swrr

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/scheduler"
)

// swrr is a Smooth Weighted Round Robin Scheduler (Nginx-style) over the set
// of Customers with pending Deliveries (ADR-0001).
//
// Smooth WRR interleaves picks so weights [3,1] yield AABAAABA rather than
// AAAB — same share, no head-of-line bursts for low-weight Customers. With
// uniform weights the behaviour is bit-for-bit equivalent to plain RR.
//
// State: per-Customer currentWeight (can be negative) and effectiveWeight
// read from a WeightSource on each Pick cycle. Notify appends a Customer when
// not already present; Pick returns the Customer with the highest current
// weight and removes it from rotation. The worker loop honours the "set of
// Customers with pending Deliveries" contract by re-calling Notify after each
// successful claim.
//
// Implementation: the active set is a doubly-linked list with a companion
// map[CustomerID]*list.Element index. This gives O(1) Notify and Remove
// (previously O(n) for Remove) and eliminates all per-Pick slice allocations
// (previously two: candidates and newList). The SWRR weight state is kept in
// a separate entries map; entries are evicted on Remove to bound memory under
// high Customer churn. Evicted Customers re-enter with order=nextOrder when
// re-added via Notify, which is correct — tie-breaking order only matters
// within a single Pick traversal.
type swrr struct {
	mu        sync.Mutex
	weights   scheduler.WeightSource
	entries   map[scheduler.CustomerID]*entry        // persistent weight + order state
	active    *list.List                             // ordered active customers
	activeIdx map[scheduler.CustomerID]*list.Element // O(1) membership + removal
	nextOrder uint64
}

type entry struct {
	currentWeight int64
	order         uint64
}

// New returns a smooth weighted round-robin Scheduler backed by weights.
// A nil weights defaults to ConstantWeights{Value: 1}.
func New(weights scheduler.WeightSource) scheduler.Scheduler {
	if weights == nil {
		weights = scheduler.ConstantWeights{Value: 1}
	}
	return &swrr{
		weights:   weights,
		entries:   make(map[scheduler.CustomerID]*entry),
		active:    list.New(),
		activeIdx: make(map[scheduler.CustomerID]*list.Element),
	}
}

// Pick returns the next Customer in weighted rotation and removes it from the
// active set, or ok=false when empty. The caller must re-Notify if the
// Customer still has pending work after this pick.
//
// The active list is traversed once: zero-weight Customers are evicted
// in-place (O(1) per eviction via list.Remove), all others accumulate their
// weight and the winner is tracked with tie-breaking by entry.order.
// No slices are allocated during Pick.
func (s *swrr) Pick(ctx context.Context) (scheduler.CustomerID, uint32, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, false, err
	}

	pickStarted := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		totalWeight int64
		winnerElem  *list.Element
		winnerID    scheduler.CustomerID
		winnerW     uint32
		winnerCW    int64
		winnerOrder uint64
	)

	for elem := s.active.Front(); elem != nil; {
		next := elem.Next()
		id := elem.Value.(scheduler.CustomerID)
		w := s.weights.Weight(id)
		if w == 0 {
			// Paused customer — evict without touching the entries map.
			s.active.Remove(elem)
			delete(s.activeIdx, id)
			elem = next
			continue
		}
		totalWeight += int64(w)
		e := s.entryFor(id)
		e.currentWeight += int64(w)
		if winnerElem == nil ||
			e.currentWeight > winnerCW ||
			(e.currentWeight == winnerCW && e.order < winnerOrder) {
			winnerElem = elem
			winnerID = id
			winnerW = w
			winnerCW = e.currentWeight
			winnerOrder = e.order
		}
		elem = next
	}

	if winnerElem == nil {
		observe.ObserveSchedulerPick(time.Since(pickStarted).Seconds())
		observe.SetActiveCustomers(0)
		return "", 0, false, nil
	}

	s.entries[winnerID].currentWeight -= totalWeight
	s.active.Remove(winnerElem)
	delete(s.activeIdx, winnerID)

	observe.ObserveSchedulerPick(time.Since(pickStarted).Seconds())
	observe.SetActiveCustomers(float64(s.active.Len()))
	return winnerID, winnerW, true, nil
}

// Remove removes the Customer from rotation and evicts its weight state.
// O(1). No-op if not present.
func (s *swrr) Remove(customerID scheduler.CustomerID) {
	if customerID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem, ok := s.activeIdx[customerID]; ok {
		s.active.Remove(elem)
		delete(s.activeIdx, customerID)
	}
	// Evict entry so the entries map doesn't grow unboundedly under high
	// Customer churn. The order and currentWeight will be re-initialised
	// if the Customer is re-added via Notify.
	delete(s.entries, customerID)
}

// Notify registers the Customer for weighted rotation if not already present
// and if its weight is > 0. Idempotent. O(1).
func (s *swrr) Notify(customerID scheduler.CustomerID) {
	if customerID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.weights.Weight(customerID) == 0 {
		return
	}
	if _, ok := s.activeIdx[customerID]; ok {
		return
	}
	s.entryFor(customerID)
	elem := s.active.PushBack(customerID)
	s.activeIdx[customerID] = elem
}

func (s *swrr) entryFor(customerID scheduler.CustomerID) *entry {
	if e, ok := s.entries[customerID]; ok {
		return e
	}
	e := &entry{order: s.nextOrder}
	s.nextOrder++
	s.entries[customerID] = e
	return e
}
