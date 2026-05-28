CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id text NOT NULL,
    type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE webhooks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id text NOT NULL,
    url text NOT NULL,
    secret_encrypted text NOT NULL,
    event_types text[] NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE deliveries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id uuid NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    webhook_id uuid NOT NULL REFERENCES webhooks(id) ON DELETE RESTRICT,
    customer_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'in_flight', 'failed_retry', 'succeeded', 'dead')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    claimed_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE delivery_attempts (
    delivery_id uuid NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    status_code integer,
    error_class text,
    duration_ms integer NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
    sent_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (delivery_id, attempt_number)
);

CREATE TABLE dead_letters (
    delivery_id uuid PRIMARY KEY REFERENCES deliveries(id) ON DELETE CASCADE,
    last_error text,
    attempts integer NOT NULL CHECK (attempts >= 0),
    reason text NOT NULL,
    final_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_deliveries_customer_status_next_attempt_at
    ON deliveries (customer_id, status, next_attempt_at);

CREATE INDEX idx_delivery_attempts_delivery_id_attempt_number
    ON delivery_attempts (delivery_id, attempt_number);

CREATE INDEX idx_dead_letters_final_at_desc
    ON dead_letters (final_at DESC);
