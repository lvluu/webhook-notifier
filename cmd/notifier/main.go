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

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lvluu/webhook-notifier/internal/observe"
)

func main() {
	addr := envDefault("ADDR", ":8080")
	infraAddr := envDefault("INFRA_ADDR", ":9090")
	pprof := envBool("PPROF_ENABLED", false)

	flag.StringVar(&addr, "addr", addr, "business HTTP server address")
	flag.StringVar(&infraAddr, "infra-addr", infraAddr, "infra HTTP server address (metrics, health, pprof)")
	flag.BoolVar(&pprof, "pprof", pprof, "enable /debug/pprof endpoints on infra server")
	flag.Parse()

	logger := observe.NewLogger(os.Stderr)
	logger = logger.With("service", "webhook-notifier")
	logger.Info("starting webhook-notifier", "addr", addr, "infra_addr", infraAddr, "pprof", pprof)

	db, err := openDB()
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	registry := prometheus.NewRegistry()
	observe.RegisterMetrics(registry)

	infra := observe.NewInfraServer(observe.InfraServerConfig{
		Addr:      infraAddr,
		DB:        db,
		Pprof:     pprof,
		Collector: registry,
	})
	infraErrCh := infra.Start()
	logger.Info("infra server started", "addr", infraAddr)

	// Stub business server that responds 200 for readiness checks.
	business := &http.Server{
		Addr:    addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}
	businessErrCh := make(chan error, 1)
	go func() {
		if err := business.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			businessErrCh <- fmt.Errorf("business server: %w", err)
		}
		close(businessErrCh)
	}()
	logger.Info("business server started", "addr", addr)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-shutdown:
		logger.Info("shutting down", "signal", sig)
	case err := <-infraErrCh:
		logger.Error("infra server error", "error", err)
	case err := <-businessErrCh:
		logger.Error("business server error", "error", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := infra.Shutdown(ctx); err != nil {
		logger.Error("infra server shutdown error", "error", err)
	}
	if err := business.Shutdown(ctx); err != nil {
		logger.Error("business server shutdown error", "error", err)
	}
	logger.Info("shutdown complete")
}

func openDB() (*sql.DB, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	return db, nil
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	switch os.Getenv(key) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return def
}
