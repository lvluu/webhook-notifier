ALTER TABLE deliveries
    ALTER CONSTRAINT deliveries_event_id_fkey NOT DEFERRABLE;

ALTER TABLE deliveries
    ALTER CONSTRAINT deliveries_webhook_id_fkey NOT DEFERRABLE;
