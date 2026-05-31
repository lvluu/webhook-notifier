ALTER TABLE deliveries SET (
    autovacuum_vacuum_threshold = 50,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 2
);
