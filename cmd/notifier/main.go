// Command notifier runs the webhook notifier service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/registry"
)

func main() {
	db := flag.String("db", "", "DATABASE_URL")
	addr := flag.String("addr", ":8080", "business HTTP server address")
	metricsAddr := flag.String("metrics-addr", ":9090", "metrics HTTP server address")
	workerCount := flag.Int("workers", 64, "number of delivery workers")
	minBatch := flag.Int("min-batch", 32, "minimum claim batch size per customer")
	multiPick := flag.Int("multi-pick", 1, "customers to batch per claim query (1 = single-customer ClaimNextN, >1 = ClaimNextBatch)")
	hostCap := flag.Int("host-cap", 20, "maximum concurrent in-flight requests per Integration Partner hostname")
	discoveryInterval := flag.Duration("discovery-interval", time.Second, "claimable customer discovery interval")
	emptyClaimBackoff := flag.Duration("empty-claim-backoff", 20*time.Millisecond, "pause after an empty claim cycle")
	backlogSampleInterval := flag.Duration("pending-sample-interval", 5*time.Second, "sample interval for pending-delivery backlog gauge (0 disables)")
	enablePgNotifyWakeups := flag.Bool("enable-pg-notify-wakeups", true, "use LISTEN/NOTIFY wakeups in addition to polling")
	pgNotifyDSN := flag.String("pg-notify-db", "", "optional direct DATABASE_URL for LISTEN/NOTIFY connections")
	pgNotifyChannel := flag.String("pg-notify-channel", "webhook_customer_ready", "LISTEN/NOTIFY channel for customer wakeups")
	claimQueryMode := flag.String("claim-query-mode", "split", "claim query mode: split or legacy")
	maxConns := flag.Int("max-conns", 0, "max DB connections (0 = workerCount + 10)")
	flag.Parse()

	logger := observe.NewLogger(os.Stderr)
	logger = logger.With("service", "webhook-notifier")
	logger.Info("starting webhook-notifier",
		"addr", *addr,
		"metrics_addr", *metricsAddr,
		"workers", *workerCount,
		"min_batch", *minBatch,
		"host_cap", *hostCap,
		"discovery_interval", *discoveryInterval,
		"empty_claim_backoff", *emptyClaimBackoff,
		"pending_sample_interval", *backlogSampleInterval,
		"pg_notify_wakeups", *enablePgNotifyWakeups,
		"pg_notify_channel", *pgNotifyChannel,
		"claim_query_mode", *claimQueryMode,
	)

	dsn := *db
	if dsn == "" {
		// #nosec G101 -- local Docker Compose default only; production supplies DATABASE_URL.
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		// #nosec G101 -- local Docker Compose default only; production supplies DATABASE_URL.
		dsn = "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable"
	}
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		logger.Error("failed to parse database config", "error", err)
		os.Exit(1)
	}
	dbConn := stdlib.OpenDB(*connConfig)
	if *maxConns > 0 {
		dbConn.SetMaxOpenConns(*maxConns)
		dbConn.SetMaxIdleConns(*maxConns)
	} else {
		dbConn.SetMaxOpenConns(*workerCount + 10)
		dbConn.SetMaxIdleConns(*workerCount + 5)
	}
	dbConn.SetConnMaxLifetime(30 * time.Minute)

	promRegistry := prometheus.NewRegistry()
	observe.RegisterMetrics(promRegistry)
	observe.RegisterPoolGauges(promRegistry, dbConn)
	observe.SetConfiguredWorkers(*workerCount)

	infra := observe.NewInfraServer(observe.InfraServerConfig{
		Addr:      *metricsAddr,
		DB:        dbConn,
		Pprof:     false,
		Collector: promRegistry,
	})
	infraErrCh := infra.Start()
	logger.Info("infra server started", "addr", *metricsAddr)

	runCtx, cancel := context.WithCancel(context.Background())

	runtimeCfg := loadWorkerRuntimeConfig()
	runtimeCfg.WorkerCount = *workerCount
	runtimeCfg.MinBatchHint = *minBatch
	runtimeCfg.MultiPickCount = *multiPick
	runtimeCfg.HostConcurrencyCap = *hostCap
	runtimeCfg.EmptyClaimBackoff = *emptyClaimBackoff
	runtimeCfg.BacklogSampleInterval = *backlogSampleInterval
	runtimeCfg.EnablePgNotifyWakeups = *enablePgNotifyWakeups
	runtimeCfg.PgNotifyDSN = *pgNotifyDSN
	runtimeCfg.PgNotifyChannel = *pgNotifyChannel
	runtimeCfg.ClaimQueryMode = *claimQueryMode
	runtimeCfg.Discovery.Interval = *discoveryInterval
	observe.StartPendingDeliveriesSampler(runCtx, dbConn, runtimeCfg.BacklogSampleInterval, logger.With("component", "pending-backlog"))

	// Ingest only needs a notifier handle; this object also broadcasts
	// pg_notify wakeups so other notifier replicas can wake immediately.
	deliveryRuntime, runtimeErrCh, err := startDeliveryRuntime(runCtx, logger, dbConn, dsn, runtimeCfg)
	if err != nil {
		logger.Error("failed to start delivery runtime", "error", err)
		os.Exit(1)
	}
	defer cancel()

	ingestStore := ingest.New(dbConn, registry.New(dbConn), deliveryRuntime.notifier)
	business := &http.Server{
		Addr:              *addr,
		Handler:           newBusinessHandler(logger.With("component", "business-http"), ingestStore, dbConn),
		ReadHeaderTimeout: 5 * time.Second,
	}
	businessErrCh := make(chan error, 1)
	go func() {
		if err := business.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			businessErrCh <- fmt.Errorf("business server: %w", err)
		}
		close(businessErrCh)
	}()
	logger.Info("business server started", "addr", *addr)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-shutdown:
		logger.Info("shutting down", "signal", sig)
	case err := <-infraErrCh:
		logger.Error("infra server error", "error", err)
	case err := <-businessErrCh:
		logger.Error("business server error", "error", err)
	case err, ok := <-runtimeErrCh:
		if ok && err != nil {
			logger.Error("delivery runtime error", "error", err)
		}
	}

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := infra.Shutdown(shutdownCtx); err != nil {
		logger.Error("infra server shutdown error", "error", err)
	}
	if err := business.Shutdown(shutdownCtx); err != nil {
		logger.Error("business server shutdown error", "error", err)
	}
	if err := deliveryRuntime.Stop(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("delivery runtime shutdown error", "error", err)
	}
	if err := dbConn.Close(); err != nil {
		logger.Error("database close error", "error", err)
	}
	logger.Info("shutdown complete")
}
