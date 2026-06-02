// Command delivery-history-retention moves terminal Deliveries into Delivery History.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/lvluu/webhook-notifier/internal/outbox"
)

type config struct {
	DatabaseURL string
	Retention   time.Duration
	BatchSize   int
	Enrichment  outbox.ArchiveEnrichmentMode
	DB          *sql.DB
	Out         io.Writer
}

func main() {
	cfg := config{
		DatabaseURL: envDefault("DATABASE_URL", "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable"),
		Retention:   24 * time.Hour,
		BatchSize:   1000,
		Enrichment:  outbox.ArchiveEnrichmentFull,
		Out:         os.Stdout,
	}

	flag.StringVar(&cfg.DatabaseURL, "database-url", cfg.DatabaseURL, "Postgres connection string")
	flag.DurationVar(&cfg.Retention, "retention", cfg.Retention, "terminal delivery retention before archiving")
	flag.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "maximum deliveries to retain into history per pass")
	flag.Func("enrichment", "archive enrichment mode: full or deferred", func(v string) error {
		mode := outbox.ArchiveEnrichmentMode(v)
		if mode != outbox.ArchiveEnrichmentFull && mode != outbox.ArchiveEnrichmentDeferred {
			return fmt.Errorf("invalid enrichment %q: must be one of %q or %q", v, outbox.ArchiveEnrichmentFull, outbox.ArchiveEnrichmentDeferred)
		}
		cfg.Enrichment = mode
		return nil
	})
	flag.Parse()

	ctx := context.Background()
	total, err := runDeliveryHistoryRetention(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := fmt.Fprintf(cfg.Out, "archived %d deliveries\n", total); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runDeliveryHistoryRetention(ctx context.Context, cfg config) (int, error) {
	if cfg.Out == nil {
		cfg.Out = io.Discard
	}
	db := cfg.DB
	if db == nil {
		var err error
		db, err = openArchiveDB(cfg.DatabaseURL)
		if err != nil {
			return 0, err
		}
		defer func() { _ = db.Close() }()
	}

	store := outbox.New(db)
	var total int
	for {
		archived, err := store.ArchiveTerminalDeliveriesWithMode(ctx, cfg.Retention, cfg.BatchSize, cfg.Enrichment)
		if err != nil {
			return total, err
		}
		total += archived
		if archived == 0 {
			return total, nil
		}
	}
}

func openArchiveDB(databaseURL string) (*sql.DB, error) {
	if databaseURL == "" {
		databaseURL = envDefault("DATABASE_URL", "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable")
	}
	connConfig, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database config: %w", err)
	}
	connConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	db := stdlib.OpenDB(*connConfig)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return db, nil
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
