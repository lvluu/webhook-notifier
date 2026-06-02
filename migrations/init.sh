#!/bin/bash
set -e

echo "Running migrations..."
echo "  enabling pg_stat_statements"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"

latest_version=""
for f in /migrations/*.up.sql; do
    echo "  applying $(basename "$f")"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$f"
    version="$(basename "$f" | cut -d_ -f1)"
    latest_version="$version"
done

if [ -n "$latest_version" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE TABLE IF NOT EXISTS schema_migrations (
    version bigint NOT NULL PRIMARY KEY,
    dirty boolean NOT NULL
);
DELETE FROM schema_migrations;
INSERT INTO schema_migrations (version, dirty) VALUES ($latest_version, false);
SQL
fi
echo "Migrations complete."
