CREATE TABLE customers (
    id text PRIMARY KEY,
    class integer NOT NULL DEFAULT 1 CHECK (class >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO customers (id, class) VALUES
    ('whale', 3),
    ('small', 1)
ON CONFLICT (id) DO NOTHING;
