# Webhook Notifier - Requirements

This document restates the supplied backend home-assignment brief in generic,
de-branded language. It records assignment requirements separately from the
scope decisions made for this proof of concept.

## Problem Statement

A multi-tenant SaaS platform supports Webhooks so Integration Partners can
receive HTTP POST requests when Events occur in a Customer's account. Webhook
Event payloads are JSON.

The platform already provides APIs for creating, updating, and deleting
Webhook registrations. The missing component is the **Webhook Notifier**,
which accepts business Events and POSTs them to the matching configured
Webhook URLs.

## Required Challenges

1. **Reliability** - Retry transient system and Integration Partner network
   failures so accepted Events are not silently lost.
2. **Scalability** - Allow delivery workers to scale horizontally as Event
   volume and traffic change.
3. **Fairness** - Prevent a Whale Customer's Event burst from causing excessive
   delay for Small Customers. Fairness must be based on Customer scheduling,
   rather than a single global FIFO of Events.

## Required Deliverables

### System Design

- A high-level architecture diagram showing how the modules are wired.
- A design document explaining the solution and technology choices.
- The design must address Reliability and Scalability. This project also
  documents Fairness because the implementation is explicitly required to
  demonstrate balanced processing.

### Implementation

- A working proof of concept that accepts Events, processes them, and sends
  HTTP POST requests to configured Webhook URLs.
- Balanced processing for Small and Whale Customers so Whale traffic does not
  disproportionately delay Small Customers.
- A repeatable way to test and benchmark the system.

### Pair-Session Discussion

- Walk through the solution and its trade-offs.
- Review the code.
- Discuss monitoring metrics for system performance and latency.
- Discuss production improvements and remaining risks.

## Submission Constraints

- Submitted code and documentation must not contain company branding.
- AI-assisted work is permitted, but the candidate must understand and take
  ownership of all submitted code and documentation.

## Project Scope Decisions

The following are PoC scope decisions, not additional statements from the
assignment brief:

- Webhook management APIs are assumed to exist elsewhere; local registration
  routes are test and demo helpers only.
- Strict per-Customer FIFO ordering is not guaranteed when multiple workers
  process Deliveries concurrently.
- Multi-region delivery is out of scope.
- Production-grade authentication on the ingest path is out of scope.
- A partner-facing replay UI is out of scope; terminal failures are retained
  in queryable database tables.

## Traceability

| Assignment requirement | Project evidence |
|---|---|
| High-level architecture and technology choices | [`docs/DESIGN.md`](docs/DESIGN.md) |
| Reliable delivery and retry behavior | ADR-0002, ADR-0003, ADR-0009, ADR-0011; reliability benchmark |
| Horizontal worker scaling | ADR-0002, ADR-0006, ADR-0013; scalability benchmark |
| Balanced Small/Whale processing | ADR-0001; fairness benchmark |
| Test and benchmark path | [`bench/README.md`](bench/README.md) and `just bench-*` recipes |
| Monitoring discussion artifacts | [`docs/observability/`](docs/observability/) |
