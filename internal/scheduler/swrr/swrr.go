// Package swrr implements a Smooth Weighted Round Robin Scheduler.
package swrr

import (
	"context"
	"sort"
	"sync"

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
type swrr struct {
	mu        sync.Mutex
	weights   scheduler.WeightSource
	entries   map[scheduler.CustomerID]*entry
	activeSet map[scheduler.CustomerID]struct{}

	activeList []scheduler.CustomerID
	nextOrder  uint64
}

type entry struct {
	currentWeight int64
	order         uint64
}

type candidate struct {
	id            scheduler.CustomerID
	weight        uint32
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
		weights:    weights,
		entries:    make(map[scheduler.CustomerID]*entry),
		activeSet:  make(map[scheduler.CustomerID]struct{}),
		activeList: make([]scheduler.CustomerID, 0),
	}
}

// Pick returns the next Customer in weighted rotation and removes it from the
// active set, or ok=false when empty. The caller must re-Notify if the
// Customer still has pending work after this pick.
func (s *swrr) Pick(ctx context.Context) (scheduler.CustomerID, uint32, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var candidates []candidate
	var totalWeight int64
	for _, id := range s.activeList {
		w := s.weights.Weight(id)
		if w == 0 {
			delete(s.activeSet, id)
			continue
		}
		e := s.entryFor(id)
		candidates = append(candidates, candidate{id: id, weight: w, order: e.order})
		totalWeight += int64(w)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].order < candidates[j].order
	})

	s.activeList = make([]scheduler.CustomerID, 0, len(candidates))
	for _, c := range candidates {
		s.activeList = append(s.activeList, c.id)
	}

	if len(candidates) == 0 {
		s.activeSet = make(map[scheduler.CustomerID]struct{})
		return "", 0, false, nil
	}

	var maxIdx int
	for i, c := range candidates {
		e := s.entryFor(c.id)
		e.currentWeight += int64(c.weight)
		candidates[i].currentWeight = e.currentWeight
		if i == 0 || e.currentWeight > candidates[maxIdx].currentWeight {
			maxIdx = i
		}
	}

	winner := candidates[maxIdx]
	s.entries[winner.id].currentWeight -= totalWeight

	delete(s.activeSet, winner.id)
	newList := make([]scheduler.CustomerID, 0, len(candidates)-1)
	for _, c := range candidates {
		if c.id != winner.id {
			newList = append(newList, c.id)
		}
	}
	s.activeList = newList

	return winner.id, winner.weight, true, nil
}

// Notify registers the Customer for weighted rotation if not already present
// and if its weight is > 0. Idempotent.
func (s *swrr) Notify(customerID scheduler.CustomerID) {
	if customerID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.weights.Weight(customerID) == 0 {
		return
	}

	if _, ok := s.activeSet[customerID]; ok {
		return
	}

	s.entryFor(customerID)

	s.activeSet[customerID] = struct{}{}
	s.activeList = append(s.activeList, customerID)
}

func (s *swrr) entryFor(customerID scheduler.CustomerID) *entry {
	e, ok := s.entries[customerID]
	if ok {
		return e
	}

	e = &entry{order: s.nextOrder}
	s.nextOrder++
	s.entries[customerID] = e
	return e
}
