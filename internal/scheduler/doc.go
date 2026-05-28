// Package scheduler decides which Customer's Delivery a free worker serves next.
//
// The Scheduler interface is the load-bearing fairness contract; the
// round-robin implementation is the PoC's policy (ADR-0001). Future
// policies (Deficit Round Robin, weighted) slot in behind the same
// interface without touching workers.
package scheduler
