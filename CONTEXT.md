# Webhook Notifier

The Webhook Notifier delivers event notifications from a multi-tenant platform to integration partners' configured URLs over HTTP POST, with reliability, horizontal scalability, and fair scheduling across tenants of vastly different sizes.

## Language

**Customer**:
A user of the platform who owns a list of Subscribers and configures Webhook destinations. The unit of fairness — scheduling decisions are made per-Customer.
_Avoid_: account, tenant, user, client

**Subscriber**:
A person on a Customer's list. Operations on Subscribers (create, update, delete) are the primary source of Events.
_Avoid_: contact, list member, lead

**Integration Partner**:
An external system that receives Webhook POSTs at URLs the Customer configured. The "partner" is not modelled in our system beyond a URL + secret.
_Avoid_: consumer, third party, receiver

**Whale Customer / Small Customer**:
Size classes used in fairness discussions. A Whale Customer has many Subscribers (hundreds of thousands) and emits Event bursts large enough to monopolise naive queues. A Small Customer has few Subscribers and is the party fairness protects.
_Avoid_: large account, heavy tenant

**Event**:
A business fact that occurred in a Customer's account (e.g. `subscriber.created`). One Event per business action. Immutable, has a stable `event_id` propagated to every Delivery for partner-side deduplication.
_Avoid_: message, notification, signal

**Webhook**:
A Customer-owned configuration row holding a destination URL, a signing secret, and the set of Event types it's interested in. One Customer can own many Webhooks. Not to be confused with the HTTP request itself — that's an Attempt.
_Avoid_: subscription (collides with Subscriber), endpoint, hook config

**Delivery**:
The unit of scheduled work: an `(Event × Webhook)` pair. One Event fans out to one Delivery per matching Webhook. Deliveries are what the FairScheduler enqueues per Customer and what the WorkerPool drains. Terminal failure of a Delivery puts it in the DLQ.
_Avoid_: job, dispatch, notification

**Attempt**:
One HTTP POST against a Delivery's URL. A Delivery typically has 1 Attempt on the happy path and up to N Attempts when retries are needed. Attempts carry the timing, status code, and error class — they are the audit trail.
_Avoid_: try, request, send

**DLQ (Dead-Letter Queue)**:
The terminal resting place for a Delivery whose Attempts exhausted the RetryPolicy. Not a real queue — a table that ops can inspect and manually replay.
_Avoid_: failure store, error log

## Example dialogue

> Dev: "What if a Customer with 500k Subscribers does a bulk import?"
>
> Domain expert: "That's exactly the Whale Customer scenario the fair scheduler exists for. The 500k `subscriber.created` Events go through the same per-Customer queue as a Small Customer's three Events, and the scheduler gives each active Customer an equal share per cycle. The Whale's burst gets drained over time without starving anyone else's Integration Partners."
