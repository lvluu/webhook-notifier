// Command notifier runs the webhook notifier service.
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
	"github.com/lvluu/webhook-notifier/internal/pgdsn"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
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
	dsn = pgdsn.WithSessionSetting(dsn, "jit", "off")

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

	deliveryRuntime, runtimeErrCh, err := startDeliveryRuntime(runCtx, logger, dbConn, runtimeCfg)
	if err != nil {
		logger.Error("failed to start delivery runtime", "error", err)
		os.Exit(1)
	}
	defer cancel()

	ingestStore := ingest.New(dbConn, registry.New(dbConn), deliveryRuntime.scheduler)
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

type ingestSubmitter interface {
	Submit(context.Context, ingest.Event) (ingest.SubmittedEvent, error)
}

func newBusinessHandler(logger *slog.Logger, ing ingestSubmitter, db *sql.DB) http.Handler {
	mux := http.NewServeMux()

	eventsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if ing == nil {
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
		eventType, ok := jsonStringField(raw, "event_name")
		if !ok || eventType == "" {
			http.Error(w, "event_name is required", http.StatusBadRequest)
			return
		}
		_, ok = jsonStringField(raw, "event_time")
		if !ok {
			http.Error(w, "timestamp is required", http.StatusBadRequest)
			return
		}
		delete(raw, "customer_id")
		payload, err := json.Marshal(raw)
		if err != nil {
			http.Error(w, "invalid event payload", http.StatusBadRequest)
			return
		}

		submitted, err := ing.Submit(r.Context(), ingest.Event{
			CustomerID: customerID,
			Type:       eventType,
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

	mux.Handle("/events", eventsHandler)
	mux.Handle("/api/events", eventsHandler)

	mux.HandleFunc("/api/customers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var cmd struct {
			ID    string `json:"id"`
			Class uint32 `json:"class"`
			URL   string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		if cmd.ID == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}
		if cmd.URL == "" {
			http.Error(w, "url is required", http.StatusBadRequest)
			return
		}
		if db == nil {
			http.Error(w, "database not available", http.StatusInternalServerError)
			return
		}

		tx, err := db.BeginTx(r.Context(), nil)
		if err != nil {
			logger.Error("begin customer registration transaction", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		defer func() { _ = tx.Rollback() }()

		_, err = tx.ExecContext(r.Context(), `
			INSERT INTO customers (id, class)
			VALUES ($1, $2)
			ON CONFLICT (id) DO UPDATE SET class = EXCLUDED.class
		`, cmd.ID, cmd.Class)
		if err != nil {
			logger.Error("upsert customer", "customer_id", cmd.ID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		_, err = tx.ExecContext(r.Context(), `
			INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active)
			VALUES ($1, $2, '', ARRAY['subscriber.created', 'order.created', 'payment.received'], true)
			ON CONFLICT (customer_id, url) DO UPDATE
			SET active = true,
			    event_types = EXCLUDED.event_types
		`, cmd.ID, cmd.URL)
		if err != nil {
			logger.Error("upsert webhook", "customer_id", cmd.ID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(); err != nil {
			logger.Error("commit customer registration", "customer_id", cmd.ID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		logger.Info("customer registered", "customer_id", cmd.ID, "class", cmd.Class, "url", cmd.URL)
		w.WriteHeader(http.StatusCreated)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return mux
}

type workerRuntimeConfig struct {
	WorkerCount        int
	MinBatchHint       int
	HostConcurrencyCap int
	RequestTimeout     time.Duration
	Discovery          worker.DiscoveryConfig
}

func loadWorkerRuntimeConfig() workerRuntimeConfig {
	return workerRuntimeConfig{
		WorkerCount:        64,
		MinBatchHint:       32,
		HostConcurrencyCap: 16,
		RequestTimeout:     30 * time.Second,
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
	limiter := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost: cfg.HostConcurrencyCap,
	})

	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			w := worker.New(
				sched,
				worker.WithHTTPClient(worker.NewHTTPClient(worker.DefaultHTTPConfig())),
				worker.WithHostLimiter(limiter),
				worker.WithMinBatchHint(cfg.MinBatchHint),
				worker.WithRequestTimeout(cfg.RequestTimeout),
				worker.WithLogger(logger.With("component", "worker", "worker_id", workerID)),
			)
			err := w.Run(runCtx, outboxStore)
			if err != nil && !errors.Is(err, context.Canceled) {
				trySendRuntimeError(logger, errCh, fmt.Errorf("worker %d: %w", workerID, err))
			}
		}(i + 1)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.RunClaimableCustomerDiscovery(runCtx, outboxStore, sched, cfg.Discovery)
		if err != nil && !errors.Is(err, context.Canceled) {
			trySendRuntimeError(logger, errCh, fmt.Errorf("discovery: %w", err))
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

func trySendRuntimeError(logger *slog.Logger, errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
		if logger != nil {
			logger.Error("runtime error channel full; dropping runtime error", "error", err)
		}
	}
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
