CREATE TABLE host_concurrency_leases (
    hostname    text        NOT NULL,
    lease_id    uuid        NOT NULL DEFAULT gen_random_uuid(),
    acquired_at timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL
);

CREATE INDEX idx_host_concurrency_leases_hostname_expires
    ON host_concurrency_leases (hostname, expires_at);
