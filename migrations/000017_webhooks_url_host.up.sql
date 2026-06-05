-- Project the delivery target host once per webhook so the worker hot path
-- (classifyDelivery) reads a column instead of re-parsing w.url with
-- net/url.Parse on every Attempt (issue #115, finding B-7).
--
-- A STORED generated column keeps the value correct for every insert/update of
-- webhooks.url with no application write-path change, and Postgres backfills
-- existing rows when the column is added. The extraction matches the host
-- semantics of (*url.URL).Hostname() for normal http(s) URLs: everything after
-- the scheme separator up to the first '/', ':', '?' or '#'. The same regex
-- already populates delivery_history.url_host (migration 000007, archive.go),
-- so the projected host stays consistent between the live and archived paths.
-- COALESCE falls back to the raw url for malformed values that lack a scheme.
ALTER TABLE webhooks
    ADD COLUMN url_host text
    GENERATED ALWAYS AS (
        COALESCE(substring(url from '^[a-zA-Z][a-zA-Z0-9+.-]*://([^/:?#]+)'), url)
    ) STORED;
