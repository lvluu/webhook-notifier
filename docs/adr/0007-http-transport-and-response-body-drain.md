# Shared HTTP transport and response body drain

Status: Accepted. Runtime sharing was widened from one client per Worker to one
client per notifier process without changing the decision.

The notifier uses one tuned `http.Client` shared by all Workers in a process
instead of `http.DefaultClient` or clients created per Delivery. This gives
high-concurrency Delivery bursts a common connection pool. Per-Attempt
deadlines are enforced with request contexts, while transport timeouts bound
connection setup and response headers separately. Every response body is fully
drained and closed before classification so Go can reuse the connection.

The current transport sets `MaxConnsPerHost=500` as a backstop and
`MaxIdleConnsPerHost=500`. The lower process-local HostLimiter default of 20 is
the normal PoC concurrency control. Benchmarks may override transport and
request-timeout configuration through Worker options. A horizontally scaled
production deployment still needs fleet-wide Host Concurrency Cap enforcement.
