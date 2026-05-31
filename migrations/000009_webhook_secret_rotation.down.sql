ALTER TABLE webhooks
    DROP COLUMN IF EXISTS secret_rotated_at,
    DROP COLUMN IF EXISTS secret_previous;
