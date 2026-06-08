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
	cliCfg, err := parseCLIConfig(flag.CommandLine, os.Args[1:])
	if err != nil {
		// flag.CommandLine uses ExitOnError, so Parse already exited; this
		// guards future error-handling modes.
		os.Exit(2)
	}
	runtimeCfg := cliCfg.Runtime

	logger := observe.NewLogger(os.Stderr)
	logger = logger.With("service", "webhook-notifier")
	logger.Info("starting webhook-notifier",
		"addr", cliCfg.Addr,
		"metrics_addr", cliCfg.MetricsAddr,
		"workers", runtimeCfg.WorkerCount,
		"min_batch", runtimeCfg.Worker.MinBatchHint,
		"host_cap", runtimeCfg.HostConcurrencyCap,
		"request_timeout", runtimeCfg.Worker.RequestTimeout,
		"delivery_concurrency", runtimeCfg.Worker.DeliveryConcurrency,
		"discovery_interval", runtimeCfg.Discovery.Interval,
		"empty_claim_backoff", runtimeCfg.Worker.EmptyClaimBackoff,
		"pending_sample_interval", runtimeCfg.BacklogSampleInterval,
		"pg_notify_wakeups", runtimeCfg.EnablePgNotifyWakeups,
		"pg_notify_channel", runtimeCfg.PgNotifyChannel,
	)

	dsn := cliCfg.DBDSN
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
	if cliCfg.MaxConns > 0 {
		dbConn.SetMaxOpenConns(cliCfg.MaxConns)
		dbConn.SetMaxIdleConns(cliCfg.MaxConns)
	} else {
		// The pool serves workers (claim + mark), ingest HTTP goroutines, discovery,
		// backlog sampler, breaker persister, and archive — so WorkerCount+10 is
		// too small under load. Cap at 50 to avoid connection storms on Postgres.
		maxConns := runtimeCfg.WorkerCount * 3
		if maxConns < 25 {
			maxConns = 25
		}
		if maxConns > 50 {
			maxConns = 50
		}
		dbConn.SetMaxOpenConns(maxConns)
		dbConn.SetMaxIdleConns(maxConns / 2)
	}
	dbConn.SetConnMaxLifetime(30 * time.Minute)

	promRegistry := prometheus.NewRegistry()
	observe.RegisterMetrics(promRegistry)
	observe.RegisterPoolGauges(promRegistry, dbConn)
	observe.SetConfiguredWorkers(runtimeCfg.WorkerCount)

	infra := observe.NewInfraServer(observe.InfraServerConfig{
		Addr:      cliCfg.MetricsAddr,
		DB:        dbConn,
		Pprof:     false,
		Collector: promRegistry,
	})
	infraErrCh := infra.Start()
	logger.Info("infra server started", "addr", cliCfg.MetricsAddr)

	runCtx, cancel := context.WithCancel(context.Background())

	if cliCfg.ReplicaID != "" {
		if err := os.Setenv("NOTIFIER_REPLICA_ID", cliCfg.ReplicaID); err != nil {
			logger.Error("failed to set notifier replica id env", "error", err)
			os.Exit(1)
		}
	}

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
		Addr:              cliCfg.Addr,
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
	logger.Info("business server started", "addr", cliCfg.Addr)

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
