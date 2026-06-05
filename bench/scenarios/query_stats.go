package scenarios

import (
	"context"
	"database/sql"
	"log/slog"
	"math"
	"strings"
)

// setupQueryStats enables pg_stat_statements and resets its counters so the
// subsequent dump reflects only this bench run. Requires the extension library
// to be preloaded (shared_preload_libraries); the throwaway bench container sets
// that, but an external override DSN may not, so a failure here just disables
// the hotspot dump rather than failing the run. Returns true when stats are
// available.
func setupQueryStats(ctx context.Context, db *sql.DB) bool {
	if _, err := db.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`); err != nil {
		slog.Warn("pg_stat_statements unavailable; query hotspot dump disabled",
			"component", "bench-db", "error", err)
		return false
	}
	if _, err := db.ExecContext(ctx, `SELECT pg_stat_statements_reset()`); err != nil {
		slog.Warn("pg_stat_statements reset failed; query hotspot dump disabled",
			"component", "bench-db", "error", err)
		return false
	}
	return true
}

// dumpQueryHotspots logs the top queries by total execution time from
// pg_stat_statements. This is the EXPLAIN-adjacent signal agents use to spot the
// dominant queries of a bench run (calls, mean latency, rows, buffer traffic)
// without instrumenting each call site. Printed to the run log only.
func dumpQueryHotspots(ctx context.Context, db *sql.DB) {
	rows, err := db.QueryContext(ctx, `
		SELECT calls, total_exec_time, mean_exec_time, rows,
		       shared_blks_hit, shared_blks_read, query
		FROM pg_stat_statements
		WHERE query NOT ILIKE '%pg_stat_statements%'
		  AND query NOT ILIKE 'SET %'
		  AND query NOT ILIKE 'SELECT pg_stat_statements_reset%'
		ORDER BY total_exec_time DESC
		LIMIT 20
	`)
	if err != nil {
		slog.Warn("query hotspot dump failed", "component", "bench-db", "error", err)
		return
	}
	defer func() { _ = rows.Close() }()

	slog.Info("bench query hotspots: pg_stat_statements top 20 by total exec time",
		"component", "bench-db")

	rank := 0
	for rows.Next() {
		rank++
		var (
			calls           int64
			totalMs, meanMs float64
			nrows           int64
			hit, read       int64
			query           string
		)
		if err := rows.Scan(&calls, &totalMs, &meanMs, &nrows, &hit, &read, &query); err != nil {
			slog.Warn("scan query hotspot", "component", "bench-db", "error", err)
			return
		}
		slog.Info("query hotspot",
			"component", "bench-db",
			"rank", rank,
			"calls", calls,
			"total_ms", round(totalMs, 2),
			"mean_ms", round(meanMs, 4),
			"rows", nrows,
			"shared_hit", hit,
			"shared_read", read,
			"query", singleLine(query),
		)
	}
	if err := rows.Err(); err != nil {
		slog.Warn("iterate query hotspots", "component", "bench-db", "error", err)
	}
}

func round(v float64, places int) float64 {
	scale := math.Pow(10, float64(places))
	return math.Round(v*scale) / scale
}

// singleLine collapses whitespace and truncates a normalized query so each
// hotspot fits one log line.
func singleLine(q string) string {
	q = strings.Join(strings.Fields(q), " ")
	const max = 200
	if len(q) > max {
		return q[:max] + " …"
	}
	return q
}
