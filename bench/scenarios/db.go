package scenarios

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lvluu/webhook-notifier/internal/pgdsn"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// OpenBenchDatabase returns an isolated Postgres database for benchmark runs.
// BENCH_DATABASE_URL or DATABASE_URL can override the container when needed,
// but the default path is a fresh throwaway container with the repo migrations
// applied.
func OpenBenchDatabase(ctx context.Context) (*sql.DB, func(), error) {
	if dsn := os.Getenv("BENCH_DATABASE_URL"); dsn != "" {
		slog.Info("using bench database override", "component", "bench-db", "source", "BENCH_DATABASE_URL")
		return openBenchDatabaseAtDSN(ctx, dsn, false)
	}
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		slog.Info("using bench database override", "component", "bench-db", "source", "DATABASE_URL")
		return openBenchDatabaseAtDSN(ctx, dsn, false)
	}

	ctr, err := postgres.RunContainer(
		ctx,
		testcontainers.WithImage("postgres:16-alpine"),
		postgres.WithDatabase("notifier"),
		postgres.WithUsername("notifier"),
		postgres.WithPassword("notifier"),
		// Preload pg_stat_statements so each bench run can dump its query
		// hotspots (the EXPLAIN-adjacent signal agents use to find dominant
		// queries). The extension is created and reset in openBenchDatabaseAtDSN.
		// The postgres entrypoint prepends "postgres" when the first arg is a flag.
		testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) {
			req.Cmd = append(req.Cmd, "-c", "shared_preload_libraries=pg_stat_statements")
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("start postgres container: %w", err)
	}

	connString, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = ctr.Terminate(context.Background())
		return nil, nil, fmt.Errorf("postgres connection string: %w", err)
	}

	if inspectPath, err := writeBenchDSNFile(connString); err != nil {
		_ = ctr.Terminate(context.Background())
		return nil, nil, err
	} else if inspectPath != "" {
		slog.Info("bench postgres connection string written", "component", "bench-db", "path", inspectPath)
	}

	db, cleanup, err := openBenchDatabaseAtDSN(ctx, connString, true)
	if err != nil {
		_ = ctr.Terminate(context.Background())
		return nil, nil, err
	}

	slog.Info("bench postgres ready", "component", "bench-db", "dsn", redactBenchDSN(connString), "keep", benchKeepDatabase())

	return db, func() {
		cleanup()
		if benchKeepDatabase() {
			slog.Info("leaving bench postgres container running for inspection", "component", "bench-db", "dsn", redactBenchDSN(connString))
			return
		}
		if err := ctr.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "terminate bench postgres container: %v\n", err)
		}
	}, nil
}

func openBenchDatabaseAtDSN(ctx context.Context, dsn string, migrate bool) (*sql.DB, func(), error) {
	// Keep JIT off for bench runs so timings stay comparable across repeated
	// invocations. Runtime service connections do not use this setting.
	dsn = pgdsn.WithSessionSetting(dsn, "jit", "off")

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("ping postgres: %w", err)
	}
	if migrate {
		if err := ApplyBenchMigrations(ctx, db); err != nil {
			_ = db.Close()
			return nil, nil, err
		}
	}
	// Reset query stats after migrations so the end-of-run hotspot dump reflects
	// the bench workload (and seeding), not schema DDL.
	hasQueryStats := setupQueryStats(ctx, db)
	return db, func() {
		if hasQueryStats {
			dumpQueryHotspots(ctx, db)
		}
		if err := db.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close bench postgres db: %v\n", err)
		}
	}, nil
}

func ApplyBenchMigrations(ctx context.Context, db *sql.DB) error {
	files, err := filepath.Glob(filepath.Join(repoRoot(), "migrations", "*.up.sql"))
	if err != nil {
		return fmt.Errorf("find migrations: %w", err)
	}
	sort.Strings(files)
	for _, file := range files {
		sqlBytes, err := os.ReadFile(file) //nolint:gosec // migration paths come from the repo migrations directory
		if err != nil {
			return fmt.Errorf("read migration %s: %w", file, err)
		}
		if _, err := db.ExecContext(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", file, err)
		}
	}
	return nil
}

func repoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func benchKeepDatabase() bool {
	switch strings.ToLower(os.Getenv("BENCH_KEEP_DATABASE")) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func redactBenchDSN(dsn string) string {
	if dsn == "" {
		return dsn
	}
	// Preserve enough information for manual psql access while hiding the
	// password in logs.
	return strings.Replace(dsn, "://notifier:notifier@", "://notifier:******@", 1)
}

func writeBenchDSNFile(dsn string) (string, error) {
	path := os.Getenv("BENCH_DATABASE_INFO_FILE")
	if path == "" {
		path = filepath.Join(os.TempDir(), "bench-postgres.dsn")
	}
	if err := os.WriteFile(path, []byte(dsn+"\n"), 0o600); err != nil { //nolint:gosec // path is either fixed temp dir or caller-provided benchmark info file
		return "", fmt.Errorf("write bench database info file: %w", err)
	}
	return path, nil
}
