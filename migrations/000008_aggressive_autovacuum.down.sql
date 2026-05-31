ALTER TABLE deliveries RESET (
    autovacuum_vacuum_threshold,
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_cost_limit,
    autovacuum_vacuum_cost_delay
);
