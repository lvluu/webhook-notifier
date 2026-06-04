ALTER TABLE delivery_claims SET (
    autovacuum_vacuum_threshold = 20,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 1
);