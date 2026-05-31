// Command notifier runs the Webhook Notifier service.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
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
	closeDB := func() {
		if err := db.Close(); err != nil {
			logger.Error("database close error", "error", err)
		}
	}

	promRegistry := prometheus.NewRegistry()
	observe.RegisterMetrics(promRegistry)
	observe.RegisterPoolGauges(promRegistry, db)
	observe.RegisterPendingCountGauges(promRegistry, db)

	infra := observe.NewInfraServer(observe.InfraServerConfig{
		Addr:      infraAddr,
		DB:        db,
		Pprof:     pprof,
		Collector: promRegistry,
	})
	infraErrCh := infra.Start()
	logger.Info("infra server started", "addr", infraAddr)

	runtimeCfg := loadWorkerRuntimeConfig()
	runtime, runtimeErrCh, err := startDeliveryRuntime(context.Background(), logger, db, runtimeCfg)
	if err != nil {
		logger.Error("failed to start delivery runtime", "error", err)
		closeDB()
		os.Exit(1)
	}

	ingestStore := ingest.New(db, registry.New(db), runtime.scheduler)
	business := &http.Server{
		Addr:              addr,
		Handler:           newBusinessHandler(logger.With("component", "business-http"), ingestStore),
		ReadHeaderTimeout: 5 * time.Second,
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
	case err := <-runtimeErrCh:
		logger.Error("delivery runtime error", "error", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := runtime.Stop(ctx); err != nil {
		logger.Error("delivery runtime shutdown error", "error", err)
	}
	if err := infra.Shutdown(ctx); err != nil {
		logger.Error("infra server shutdown error", "error", err)
	}
	if err := business.Shutdown(ctx); err != nil {
		logger.Error("business server shutdown error", "error", err)
	}
	closeDB()
	logger.Info("shutdown complete")
}

func openDB() (*sql.DB, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		// #nosec G101 -- local Docker Compose default only; production supplies DATABASE_URL.
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

func envInt(key string, def int) int {
	value := os.Getenv(key)
	if value == "" {
		return def
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil || parsed <= 0 {
		return def
	}
	return parsed
}

type workerRuntimeConfig struct {
	WorkerCount    int
	RequestTimeout time.Duration
	Discovery      worker.DiscoveryConfig
}

func loadWorkerRuntimeConfig() workerRuntimeConfig {
	return workerRuntimeConfig{
		WorkerCount:    envInt("WORKER_COUNT", 16),
		RequestTimeout: 30 * time.Second,
		Discovery: worker.DiscoveryConfig{
			Interval:     time.Second,
			EmptyBackoff: 5 * time.Second,
			ErrorBackoff: time.Second,
			MaxBackoff:   30 * time.Second,
			Limit:        1000,
		},
	}
}

type deliveryRuntime struct {
	scheduler roundrobinScheduler
	cancel    context.CancelFunc
	done      chan struct{}
}

type roundrobinScheduler interface {
	Notify(string)
	Remove(string)
	Pick(context.Context) (string, uint32, bool, error)
}

func (r *deliveryRuntime) Stop(ctx context.Context) error {
	r.cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return nil
	}
}

func startDeliveryRuntime(
	parent context.Context,
	logger *slog.Logger,
	db *sql.DB,
	cfg workerRuntimeConfig,
) (*deliveryRuntime, <-chan error, error) {
	if cfg.WorkerCount < 1 {
		return nil, nil, fmt.Errorf("worker count must be >= 1")
	}

	runCtx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	errCh := make(chan error, cfg.WorkerCount+1)

	sched := roundrobin.New()
	outboxStore := outbox.New(db)
	limiter := worker.NewHostLimiter(worker.HostLimiterConfig{})

	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			w := worker.New(
				sched,
				worker.WithHTTPClient(worker.NewHTTPClient(worker.DefaultHTTPConfig())),
				worker.WithHostLimiter(limiter),
				worker.WithRequestTimeout(cfg.RequestTimeout),
				worker.WithLogger(logger.With("component", "worker", "worker_id", workerID)),
			)
			err := w.Run(runCtx, outboxStore)
			if err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("worker %d: %w", workerID, err)
			}
		}(i + 1)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.RunClaimableCustomerDiscovery(runCtx, outboxStore, sched, cfg.Discovery)
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("discovery: %w", err)
		}
	}()

	go func() {
		wg.Wait()
		close(done)
		close(errCh)
	}()

	return &deliveryRuntime{
		scheduler: sched,
		cancel:    cancel,
		done:      done,
	}, errCh, nil
}

func newBusinessHandler(logger *slog.Logger, ingestStore *ingest.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			CustomerID string          `json:"customer_id"`
			Type       string          `json:"type"`
			Payload    json.RawMessage `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}

		submitted, err := ingestStore.Submit(r.Context(), ingest.Event{
			CustomerID: req.CustomerID,
			Type:       req.Type,
			Payload:    req.Payload,
		})
		if err != nil {
			logger.Warn("ingest submit failed", "error", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(submitted)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
