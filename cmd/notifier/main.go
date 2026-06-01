// Command notifier runs the webhook notifier service.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
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
	concurrency := flag.Int("concurrency", 16, "Host Concurrency Cap")
	discoveryInterval := flag.Duration("discovery-interval", time.Second, "claimable customer discovery interval")
	maxConns := flag.Int("max-conns", 0, "max DB connections (0 = workerCount + 10)")
	pgNotify := flag.Bool("pg-notify", true, "enable cross-replica Postgres LISTEN/NOTIFY wakeups")
	flag.Parse()

	logger := observe.NewLogger(os.Stderr)
	logger = logger.With("service", "webhook-notifier")
	logger.Info("starting webhook-notifier",
		"addr", *addr,
		"metrics_addr", *metricsAddr,
		"workers", *workerCount,
		"min_batch", *minBatch,
		"concurrency", *concurrency,
		"discovery_interval", *discoveryInterval,
		"pg_notify", *pgNotify,
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
	dbConn, err := sql.Open("postgres", dsn)
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
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
	runtimeCfg.HostConcurrencyCap = *concurrency
	runtimeCfg.Discovery.Interval = *discoveryInterval
	runtimeCfg.DatabaseURL = dsn
	runtimeCfg.EnablePGNotify = *pgNotify

	deliveryRuntime, runtimeErrCh, err := startDeliveryRuntime(runCtx, logger, dbConn, runtimeCfg)
	if err != nil {
		logger.Error("failed to start delivery runtime", "error", err)
		os.Exit(1)
	}
	defer cancel()

	ingestStore := ingest.New(
		dbConn,
		registry.New(dbConn),
		deliveryRuntime.scheduler,
		ingest.WithClaimableCustomerNotify(*pgNotify),
	)
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
