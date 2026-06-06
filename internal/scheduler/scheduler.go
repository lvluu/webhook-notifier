package scheduler

import "context"

// CustomerID identifies the Customer whose Delivery a worker should serve next.
// Declared here so the Scheduler contract does not depend on any other
// internal package.
type CustomerID = string

// Scheduler decides which Customer's Delivery a free worker serves next.
//
// The interface is the load-bearing fairness contract (ADR-0001). It must not
// leak round-robin assumptions — no "next index" in the contract, no Customer
// ordering in returns. Implementations decide how to honour fairness.
//
// Pick returns the next Customer to serve, a batchHint equal to that Customer's
// weight (the worker MAY claim up to batchHint Deliveries before yielding back),
// or ok=false when no Customer has pending work registered with this Scheduler.
// batchHint == 0 means no work. Pick must respect ctx cancellation.
//
// WaitForWork blocks until at least one Customer is registered or ctx is
// cancelled. Workers call this instead of polling with time-based backoff
// when Pick returns ok=false.
//
// Notify informs the Scheduler that the given Customer has new pending work.
// Implementations must be safe for concurrent use.
//
// Remove tells the Scheduler that the given Customer has no more pending work
// and should be removed from the rotation. It is a no-op if the Customer is
// not currently registered.
type Scheduler interface {
	Pick(ctx context.Context) (customerID CustomerID, batchHint uint32, ok bool, err error)
	WaitForWork(ctx context.Context) error
	Notify(customerID CustomerID)
	Remove(customerID CustomerID)
}

// WeightSource resolves a Customer's fairness weight. A weight of 0 means the
// Customer is paused and should never be returned by Pick. The Scheduler
// depends on this interface, not on any particular storage backend.
type WeightSource interface {
	Weight(customerID CustomerID) uint32
}

// ConstantWeights returns the same weight for every Customer. It is the PoC
// default: all Customers get weight 1, which makes SWRR degenerate to plain
// round-robin.
type ConstantWeights struct {
	Value uint32
}

// Weight implements WeightSource. The weight is constant regardless of customer.
func (c ConstantWeights) Weight(customerID CustomerID) uint32 {
	_ = customerID
	if c.Value == 0 {
		return 1
	}
	return c.Value
}
