// Package scheduler decides which Customer's Delivery a free worker serves next.
//
// The Scheduler interface is the load-bearing fairness contract; the
// Smooth Weighted Round Robin implementation is the PoC's policy (ADR-0001).
// Future policies, such as Deficit Round Robin, slot in behind the same
// interface without touching workers.
package scheduler
