CREATE TABLE customers (
    id text PRIMARY KEY,
    weight integer NOT NULL DEFAULT 1 CHECK (weight >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO customers (id, weight) VALUES
    ('whale', 3),
    ('small', 1)
ON CONFLICT (id) DO NOTHING;
