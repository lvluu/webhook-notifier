ALTER TABLE webhooks
    ADD COLUMN secret_previous text,
    ADD COLUMN secret_rotated_at timestamptz;
