# Terminal delivery retention uses an archive table

The live `deliveries` table is the hot queue and should stay small enough for claim and discovery queries to remain predictable. Terminal Deliveries will be copied to a time-partitioned archive table, then removed from `deliveries` after a bounded operational retention window; old archive history can be dropped by partition instead of deleted row-by-row.

This keeps the queue path focused on claimable work while preserving terminal history for debugging and audit. PostgreSQL deletes still create dead tuples that depend on vacuum cleanup, so deletion remains acceptable for the live queue's short retention window but is not the long-term history retention mechanism.

Retention runs as scheduled maintenance, not as a background goroutine inside the notifier worker runtime. The PoC exposes this as the `delivery-history-retention` command so operators can run it manually, from cron, or as a Kubernetes CronJob without coupling retention failures to live webhook delivery.

To reduce cache pressure during high-throughput retention passes, the command
supports deferred enrichment mode. In deferred mode, archival keeps core
terminal fields on the critical path and defers heavy per-Delivery attempt
summary enrichment.

Operationally, this pairs with aggressive autovacuum settings on the hot `deliveries` table so reclaimed pages become reusable quickly during steady terminal-row churn.

Start with a configurable 24-hour live-table retention window. This leaves a short operational debugging window in `deliveries` while bounding hot-queue growth. Validate the production window and archive cadence under load using database size, claim latency, and vacuum evidence.

Operational defaults should start at `batch-size=500` and a `1m` schedule, then
be tuned with `EXPLAIN (ANALYZE, BUFFERS)` and autovacuum evidence.

Delivery History retention is an operator policy, not a hard-coded notifier
behavior. Remove old history by dropping time partitions. If a concrete initial
value is needed, use a provisional 30-day archive-retention window and align it
later with support, audit, and privacy requirements.

Archive the Attempt summary needed for support and reporting, then allow old
`delivery_attempts` rows to be removed with the live Delivery. Unbounded
per-Attempt detail would defeat the retention goal. Preserve detailed Attempt
history only when a concrete audit requirement justifies the additional
storage.

This does not remove the known Postgres queue ceiling at high throughput; it delays bloat-driven degradation and keeps the claim path healthier, but sustained very high TPS still eventually pushes toward queue partitioning or broker-backed designs.

References:

- PostgreSQL table partitioning: https://www.postgresql.org/docs/17/ddl-partitioning.html
- PostgreSQL `VACUUM`: https://www.postgresql.org/docs/17/sql-vacuum.html
- PostgreSQL routine vacuuming: https://www.postgresql.org/docs/17/routine-vacuuming.html
