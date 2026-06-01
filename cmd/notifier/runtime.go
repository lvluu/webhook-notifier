package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

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

type runtimeDeps struct {
	newScheduler func() roundrobinScheduler
	newStore     func(*sql.DB) *outbox.Store
	newLimiter   func(int) *worker.HostLimiter
	runDiscovery func(context.Context, *outbox.Store, roundrobinScheduler, worker.DiscoveryConfig) error
}

func defaultRuntimeDeps() runtimeDeps {
	return runtimeDeps{
		newScheduler: func() roundrobinScheduler { return roundrobin.New() },
		newStore:     func(db *sql.DB) *outbox.Store { return outbox.New(db) },
		newLimiter: func(maxPerHost int) *worker.HostLimiter {
			return worker.NewHostLimiter(worker.HostLimiterConfig{MaxPerHost: maxPerHost})
		},
		runDiscovery: func(ctx context.Context, store *outbox.Store, sched roundrobinScheduler, cfg worker.DiscoveryConfig) error {
			return worker.RunClaimableCustomerDiscovery(ctx, store, sched, cfg)
		},
	}
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
	return startDeliveryRuntimeWithDeps(parent, logger, db, cfg, defaultRuntimeDeps())
}

func startDeliveryRuntimeWithDeps(
	parent context.Context,
	logger *slog.Logger,
	db *sql.DB,
	cfg workerRuntimeConfig,
	deps runtimeDeps,
) (*deliveryRuntime, <-chan error, error) {
	if cfg.WorkerCount < 1 {
		return nil, nil, fmt.Errorf("worker count must be >= 1")
	}

	runCtx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	errCh := make(chan error, cfg.WorkerCount+1)

	sched := deps.newScheduler()
	outboxStore := deps.newStore(db)
	limiter := deps.newLimiter(cfg.HostConcurrencyCap)

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
		err := deps.runDiscovery(runCtx, outboxStore, sched, cfg.Discovery)
		if err != nil && !errors.Is(err, context.Canceled) {
			trySendRuntimeError(logger, errCh, fmt.Errorf("discovery: %w", err))
		}
	}()

	if cfg.EnablePGNotify && cfg.DatabaseURL != "" && cfg.NotifyChannel != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runNotifyListener(runCtx, cfg.DatabaseURL, cfg.NotifyChannel, sched, logger); err != nil && !errors.Is(err, context.Canceled) {
				trySendRuntimeError(logger, errCh, fmt.Errorf("notify listener: %w", err))
			}
		}()
	}

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

func runNotifyListener(
	ctx context.Context,
	dsn string,
	channel string,
	sched roundrobinScheduler,
	logger *slog.Logger,
) error {
	listener := pq.NewListener(dsn, 2*time.Second, 30*time.Second, nil)
	defer func() { _ = listener.Close() }()

	if err := listener.Listen(channel); err != nil {
		return fmt.Errorf("listen on channel %q: %w", channel, err)
	}

	if logger != nil {
		logger.Info("postgres notify listener started", "channel", channel)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case n := <-listener.Notify:
			if n == nil {
				continue
			}
			if n.Extra != "" {
				sched.Notify(n.Extra)
			}
		}
	}
}
