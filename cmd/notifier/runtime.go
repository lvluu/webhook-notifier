package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/scheduler/dbweights"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/scheduler/swrr"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

type deliveryRuntime struct {
	scheduler roundrobinScheduler
	notifier  schedulerNotifier
	cancel    context.CancelFunc
	done      chan struct{}
}

type roundrobinScheduler interface {
	Notify(string)
	Remove(string)
	Pick(context.Context) (string, uint32, bool, error)
}

type runtimeDeps struct {
	newScheduler func(*sql.DB, workerRuntimeConfig) roundrobinScheduler
	newStore     func(*sql.DB, workerRuntimeConfig) *outbox.Store
	newLimiter   func(int) *worker.HostLimiter
	runDiscovery func(context.Context, *outbox.Store, roundrobinScheduler, worker.DiscoveryConfig) error
}

const pgNotifyQueueSize = 1024

func defaultRuntimeDeps() runtimeDeps {
	return runtimeDeps{
		newScheduler: func(db *sql.DB, cfg workerRuntimeConfig) roundrobinScheduler {
			if cfg.EnableWeightedScheduler {
				return swrr.New(dbweights.New(db))
			}
			return roundrobin.New(roundrobin.WithBatchHint(cfg.ClaimBatchHint))
		},
		newStore: func(db *sql.DB, cfg workerRuntimeConfig) *outbox.Store {
			opts := []outbox.Option{
				outbox.WithClaimQueryMode(outbox.ClaimQueryMode(cfg.ClaimQueryMode)),
			}
			return outbox.New(db, opts...)
		},
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
	dsn string,
	cfg workerRuntimeConfig,
) (*deliveryRuntime, <-chan error, error) {
	return startDeliveryRuntimeWithDeps(parent, logger, db, dsn, cfg, defaultRuntimeDeps())
}

func startDeliveryRuntimeWithDeps(
	parent context.Context,
	logger *slog.Logger,
	db *sql.DB,
	dsn string,
	cfg workerRuntimeConfig,
	deps runtimeDeps,
) (*deliveryRuntime, <-chan error, error) {
	if cfg.WorkerCount < 1 {
		return nil, nil, fmt.Errorf("worker count must be >= 1")
	}

	runCtx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	errCh := make(chan error, cfg.WorkerCount+1)

	sched := deps.newScheduler(db, cfg)
	notify := schedulerNotifier{
		scheduler: sched,
	}
	outboxStore := deps.newStore(db, cfg)
	limiter := deps.newLimiter(cfg.HostConcurrencyCap)
	httpClient := worker.NewHTTPClient(worker.DefaultHTTPConfig())

	var wg sync.WaitGroup
	var broadcastDB *sql.DB
	pgNotifyDSN := cfg.PgNotifyDSN
	if pgNotifyDSN == "" {
		pgNotifyDSN = dsn
	}
	if cfg.EnablePgNotifyWakeups && pgNotifyDSN != "" {
		var err error
		broadcastDB, err = sql.Open("pgx", pgNotifyDSN)
		if err != nil {
			cancel()
			return nil, nil, fmt.Errorf("open pg_notify database: %w", err)
		}
		broadcastDB.SetMaxOpenConns(1)
		broadcastDB.SetMaxIdleConns(1)
		broadcaster := newPgNotifyBroadcaster(
			broadcastDB,
			logger.With("component", "pg-notify"),
			cfg.PgNotifyChannel,
			pgNotifyQueueSize,
		)
		notify.broadcaster = broadcaster
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := broadcaster.Run(runCtx)
			if err != nil && !errors.Is(err, context.Canceled) {
				trySendRuntimeError(logger, errCh, fmt.Errorf("pg_notify broadcaster: %w", err))
			}
		}()
	}

	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			w := worker.New(
				sched,
				worker.WithHTTPClient(httpClient),
				worker.WithHostLimiter(limiter),
				worker.WithMinBatchHint(cfg.MinBatchHint),
				worker.WithMultiPickCount(cfg.MultiPickCount),
				worker.WithRequestTimeout(cfg.RequestTimeout),
				worker.WithEmptyClaimBackoff(cfg.EmptyClaimBackoff),
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

	if cfg.EnablePgNotifyWakeups && pgNotifyDSN != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := runCustomerWakeListener(runCtx, logger.With("component", "pg-listener"), pgNotifyDSN, cfg.PgNotifyChannel, sched)
			if err != nil && !errors.Is(err, context.Canceled) {
				trySendRuntimeError(logger, errCh, fmt.Errorf("wake listener: %w", err))
			}
		}()
	}

	go func() {
		wg.Wait()
		if broadcastDB != nil {
			if err := broadcastDB.Close(); err != nil && logger != nil {
				logger.Warn("pg_notify database close failed", "error", err)
			}
		}
		close(done)
		close(errCh)
	}()

	return &deliveryRuntime{
		scheduler: sched,
		notifier:  notify,
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
