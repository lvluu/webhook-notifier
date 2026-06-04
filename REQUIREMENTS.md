# Webhook Notifier — Requirements

## Problem Statement

A multi-tenant SaaS platform exposes webhook integration so that Integration
Partners can receive HTTP POSTs for Events occurring in a Customer's account.
All webhook Events carry JSON payloads.

The platform already provides APIs to manage webhook registrations (create,
update, delete). What is missing is the **Webhook Notifier** — the component
that listens for business Events and delivers them as HTTP POSTs to the
configured webhook URLs.

## Challenges

1. **Reliability** — A retry mechanism must handle transient system failures
   and networking issues from Integration Partners. No Event should be lost
   between emission and terminal delivery state.

2. **Scalability** — Workers must scale horizontally to handle variable Event
   volume and traffic. Adding workers should increase throughput without
   changing claim or coordination semantics.

3. **Fairness** — Customers range from a few hundred Subscribers to hundreds
   of thousands. A single large Customer ("Whale") emitting a burst of Events
   must not starve smaller Customers from timely delivery. Scheduling
   decisions must be per-Customer, not per-Event.

## Tasks

### System Design

Produce design documentation covering:

- A high-level architecture diagram showing component wiring.
- A document describing the solution and technology choices.
- Scope: the design must address the Reliability and Scalability challenges.

### Implementation

Deliver a working proof-of-concept that demonstrates:

- Core functionality: listen for Events, process them, and send HTTP POST
  requests to configured webhook URLs.
- Balanced event processing across small and large Customers — small
  Customers must not experience disproportionate delays when Whale Customers
  are active.
- A way to test and benchmark the system.

### Discussion Topics (pair session)

- Monitoring: metrics for performance, latency, and system health.
- Further improvements and production considerations.

## Non-Requirements (explicitly out of scope)

- Webhook management APIs (create, update, delete) — assumed implemented
  elsewhere.
- Strict per-Customer FIFO ordering (per-Customer queues drain in arrival
  order, but multiple workers may cause some interleaving).
- Multi-region delivery.
- Production-grade authentication on the ingest path.
- Partner-facing replay UI (the DLQ is a queryable table).
