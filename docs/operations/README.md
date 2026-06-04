# Operations Runbooks

Operational docs are intentionally small and task-focused:

- [DB query monitoring](db-query-monitoring.md) - sustained-load queue health,
  Postgres wait/lock checks, bloat thresholds, and failure triage.
- [Delivery History retention](delivery-history-retention.md) - archive command
  defaults, query-shape evidence, and vacuum checks for terminal Delivery
  retention.

Use [docs/CONFIGURATION.md](../CONFIGURATION.md) for flag defaults and
[docs/WORKFLOW.md](../WORKFLOW.md) for the end-to-end runtime path.
