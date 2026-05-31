DELETE FROM webhooks w
USING webhooks dup
WHERE w.customer_id = dup.customer_id
  AND w.url = dup.url
  AND w.id > dup.id;

ALTER TABLE webhooks
    ADD CONSTRAINT webhooks_customer_id_url_key UNIQUE (customer_id, url);
