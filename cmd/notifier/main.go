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

func envDuration(key string, def time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return def
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
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
		RequestTimeout: envDuration("WORKER_REQUEST_TIMEOUT", 30*time.Second),
		Discovery: worker.DiscoveryConfig{
			Interval:     envDuration("DISCOVERY_INTERVAL", time.Second),
			EmptyBackoff: envDuration("DISCOVERY_EMPTY_BACKOFF", 5*time.Second),
			ErrorBackoff: envDuration("DISCOVERY_ERROR_BACKOFF", time.Second),
			MaxBackoff:   envDuration("DISCOVERY_MAX_BACKOFF", 30*time.Second),
			Limit:        envInt("DISCOVERY_LIMIT", 1000),
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
				trySendRuntimeError(errCh, fmt.Errorf("worker %d: %w", workerID, err))
			}
		}(i + 1)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.RunClaimableCustomerDiscovery(runCtx, outboxStore, sched, cfg.Discovery)
		if err != nil && !errors.Is(err, context.Canceled) {
			trySendRuntimeError(errCh, fmt.Errorf("discovery: %w", err))
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

func trySendRuntimeError(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

type ingestSubmitter interface {
	Submit(context.Context, ingest.Event) (ingest.SubmittedEvent, error)
}

func newBusinessHandler(logger *slog.Logger, ingestStore ingestSubmitter) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if ingestStore == nil {
			logger.Error("ingest store is not configured")
			http.Error(w, "service unavailable", http.StatusInternalServerError)
			return
		}

		var raw map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		customerID, ok := jsonStringField(raw, "customer_id")
		if !ok || customerID == "" {
			http.Error(w, "customer_id is required", http.StatusBadRequest)
			return
		}
		eventName, ok := jsonStringField(raw, "event_name")
		if !ok || eventName == "" {
			http.Error(w, "event_name is required", http.StatusBadRequest)
			return
		}
		eventTime, ok := jsonStringField(raw, "event_time")
		if !ok || eventTime == "" {
			http.Error(w, "event_time is required", http.StatusBadRequest)
			return
		}
		// event_time is contract-required for the Flodesk envelope and remains
		// part of the persisted event payload after customer_id is removed.
		delete(raw, "customer_id")
		payload, err := json.Marshal(raw)
		if err != nil {
			http.Error(w, "invalid event payload", http.StatusBadRequest)
			return
		}

		submitted, err := ingestStore.Submit(r.Context(), ingest.Event{
			CustomerID: customerID,
			Type:       eventName,
			Payload:    payload,
		})
		if err != nil {
			logger.Warn("ingest submit failed", "error", err)
			http.Error(w, "invalid event payload", http.StatusBadRequest)
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

func jsonStringField(raw map[string]json.RawMessage, field string) (string, bool) {
	rawValue, ok := raw[field]
	if !ok {
		return "", false
	}
	var value string
	if err := json.Unmarshal(rawValue, &value); err != nil {
		return "", false
	}
	return value, true
}
