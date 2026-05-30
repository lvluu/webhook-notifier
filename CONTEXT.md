# Webhook Notifier

The Webhook Notifier delivers event notifications from a multi-tenant platform to integration partners' configured URLs over HTTP POST, with reliability, horizontal scalability, and fair scheduling across tenants of vastly different sizes.

## Language

**Customer**:
A user of the platform who owns a Subscriber list and one or more Webhooks. The unit of fairness — scheduling decisions are made per-Customer, not per-Subscriber or per-Webhook.
_Avoid_: account, tenant, user, client

**Subscriber**:
A person on a Customer's list. Subscriber activity, such as creation, segment membership changes, or unsubscribe actions, is the primary source of Events.
_Avoid_: contact, list member, lead

**Integration Partner**:
An external system that receives Webhook POSTs at URLs the Customer configured. An Integration Partner is the recipient behind a Webhook URL, not the Customer who owns that Webhook.
_Avoid_: consumer, third party, receiver

**Whale Customer / Small Customer**:
Size classes used in fairness discussions. A Whale Customer has many Subscribers (hundreds of thousands) and emits Event bursts large enough to monopolise naive queues. A Small Customer has few Subscribers and is the party fairness protects.
_Avoid_: large account, heavy tenant

**Event**:
A business fact that occurred for a Customer, usually about a Subscriber (e.g. `subscriber.created`). One Event represents one business action and may fan out to multiple Deliveries when multiple matching Webhooks exist.
_Avoid_: message, notification, signal

**Webhook**:
A Customer-owned destination configuration: a POST URL plus the Event types it is interested in. One Customer can own many Webhooks; a Webhook is not the HTTP request itself — that is an Attempt.
_Avoid_: subscription (collides with Subscriber), endpoint, hook config

**Delivery**:
The unit of scheduled work: an `(Event × Webhook)` pair. One Event fans out to one Delivery per matching Webhook, so a Customer with many Subscribers and many Webhooks can produce a large Delivery backlog.
_Avoid_: job, dispatch, notification

**Attempt**:
One HTTP POST against a Delivery's URL. A Delivery typically has 1 Attempt on the happy path and up to N Attempts when retries are needed. Attempts carry the timing, status code, and error class — they are the audit trail.
_Avoid_: try, request, send

**Host Concurrency Cap**:
The maximum number of in-flight Attempts a single process may issue concurrently against one Integration Partner hostname. When the cap is reached, further Deliveries to that host are requeued with a short delay so the worker can serve other Customers. Orthogonal to per-Customer fairness — it protects partners, not Customers.
_Avoid_: rate limit, throttle, backpressure

**Requeue**:
Returning a claimed (in-flight) Delivery to `failed_retry` with a bumped `next_attempt_at`, without recording an Attempt row or incrementing the attempt counter. Used when a Delivery cannot proceed for a non-partner reason (e.g. host concurrency cap hit). A Requeue is not an Attempt — the partner never saw the POST.
_Avoid_: retry (implies an Attempt was made), release (ambiguous with claim release)

**Delivery Expiration**:
The point at which a Delivery is considered too old to be useful and is moved to the DLQ, regardless of remaining retry attempts. Controlled by the configured MaxAge.
_Avoid_: ttl, timeout, drop

**DLQ (Dead-Letter Queue)**:
The terminal resting place for a Delivery that can no longer be retried, either because its Attempts exhausted the RetryPolicy, it reached its Delivery Expiration, or the outcome is non-retryable. Not a real queue — a table that ops can inspect and manually replay.
_Avoid_: failure store, error log

**Webhook Circuit Breaker**:
The health state of one Webhook destination. When a Webhook is repeatedly failing, its circuit breaker can pause further Deliveries for that Webhook until a later probe shows the Integration Partner endpoint is healthy again.
_Avoid_: host breaker, customer breaker, rate limit

## Example dialogue

> Dev: "What if a Customer with 500k Subscribers and three matching Webhooks does a bulk import?"
>
> Domain expert: "That creates 500k `subscriber.created` Events and up to 1.5M Deliveries — one per `(Event × Webhook)` pair. The fair scheduler still treats the Customer as the fairness unit, so that Whale Customer's fanout backlog drains over time without starving Small Customers."
