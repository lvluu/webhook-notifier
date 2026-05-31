# Terminal delivery retention uses an archive table

The live `deliveries` table is the hot queue and should stay small enough for claim and discovery queries to remain predictable. Terminal Deliveries will be copied to a time-partitioned archive table, then removed from `deliveries` after a bounded operational retention window; old archive history can be dropped by partition instead of deleted row-by-row.

This keeps the queue path focused on claimable work while preserving terminal history for debugging and audit. PostgreSQL deletes still create dead tuples that depend on vacuum cleanup, so deletion remains acceptable for the live queue's short retention window but is not the long-term history retention mechanism.

Retention runs as scheduled maintenance, not as a background goroutine inside the notifier worker runtime. The PoC exposes this as the `archive-deliveries` command so operators can run it manually, from cron, or as a Kubernetes CronJob without coupling archive failures to live webhook delivery.

References:

- PostgreSQL table partitioning: https://www.postgresql.org/docs/17/ddl-partitioning.html
- PostgreSQL `VACUUM`: https://www.postgresql.org/docs/17/sql-vacuum.html
- PostgreSQL routine vacuuming: https://www.postgresql.org/docs/17/routine-vacuuming.html
