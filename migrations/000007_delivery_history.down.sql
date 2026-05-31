DROP TABLE IF EXISTS delivery_history;

ALTER TABLE deliveries RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_analyze_scale_factor
);
