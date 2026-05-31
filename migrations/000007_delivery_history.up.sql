CREATE TABLE delivery_history (
    delivery_id uuid NOT NULL,
    event_id uuid NOT NULL,
    webhook_id uuid NOT NULL,
    customer_id text NOT NULL,
    event_type text NOT NULL,
    url_host text NOT NULL,
    final_status text NOT NULL CHECK (final_status IN ('succeeded', 'dead')),
    terminal_reason text,
    attempt_count integer NOT NULL CHECK (attempt_count >= 0),
    first_attempt_at timestamptz,
    last_attempt_at timestamptz,
    last_status_code integer,
    last_error_class text,
    last_error text,
    delivery_created_at timestamptz NOT NULL,
    final_at timestamptz NOT NULL,
    archived_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (delivery_id, final_at)
) PARTITION BY RANGE (final_at);

CREATE INDEX idx_delivery_history_customer_final_at
    ON delivery_history (customer_id, final_at DESC);

