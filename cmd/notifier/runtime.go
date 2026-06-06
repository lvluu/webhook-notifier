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
	group     *runtimeGroup
}

type roundrobinScheduler interface {
	Notify(string)
	Remove(string)
	Pick(context.Context) (string, uint32, bool, error)
	WaitForWork(context.Context) error
}

type runtimeDeps struct {
	newScheduler func(*sql.DB, workerRuntimeConfig) roundrobinScheduler
	newStore     func(*sql.DB, workerRuntimeConfig) *outbox.Store
	newLimiter   func(int) *worker.HostLimiter
	runDiscovery func(context.Context, *outbox.Store, roundrobinScheduler, worker.DiscoveryConfig) error
}

const pgNotifyQueueSize = 1024

type runtimeGroup struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
	errCh  chan error
	done   chan struct{}

	wg       sync.WaitGroup
	cleanups []func()
}

func newRuntimeGroup(parent context.Context, logger *slog.Logger, errBuffer int) *runtimeGroup {
	ctx, cancel := context.WithCancel(parent)
	return &runtimeGroup{
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
		errCh:  make(chan error, errBuffer),
		done:   make(chan struct{}),
	}
}

func (g *runtimeGroup) Go(label string, run func(context.Context) error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := run(g.ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			g.report(fmt.Errorf("%s: %w", label, err))
		}
	}()
}

func (g *runtimeGroup) Cleanup(fn func()) {
	g.cleanups = append(g.cleanups, fn)
}

func (g *runtimeGroup) Finish() {
	go func() {
		g.wg.Wait()
		for _, cleanup := range g.cleanups {
			cleanup()
		}
		close(g.done)
		close(g.errCh)
	}()
}

func (g *runtimeGroup) Stop(ctx context.Context) error {
	g.cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.done:
		return nil
	}
}

func (g *runtimeGroup) report(err error) {
	select {
	case g.errCh <- err:
	default:
		if g.logger != nil {
			g.logger.Error("runtime error channel full; dropping runtime error", "error", err)
		}
	}
}

func defaultRuntimeDeps() runtimeDeps {
	return runtimeDeps{
		newScheduler: func(db *sql.DB, cfg workerRuntimeConfig) roundrobinScheduler {
			if cfg.EnableWeightedScheduler {
				return swrr.New(dbweights.New(db))
			}
			return roundrobin.New()
		},
		newStore: func(db *sql.DB, cfg workerRuntimeConfig) *outbox.Store {
			return outbox.New(db, outbox.WithDeferredBreaker())
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
	return r.group.Stop(ctx)
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
	if err := cfg.Worker.Validate(); err != nil {
		return nil, nil, fmt.Errorf("worker config: %w", err)
	}

	group := newRuntimeGroup(parent, logger, cfg.WorkerCount+1)

	sched := deps.newScheduler(db, cfg)
	notify := schedulerNotifier{
		scheduler: sched,
	}
	outboxStore := deps.newStore(db, cfg)
	limiter := deps.newLimiter(cfg.HostConcurrencyCap)
	httpClient := worker.NewHTTPClient(worker.DefaultHTTPConfig())

	// Single per-process consumer of breaker outcomes. The store is built with
	// WithDeferredBreaker, so workers enqueue outcomes here instead of updating
	// the hot webhook breaker rows inside their mark transactions (ADR-0015).
	breakerCoalescer := outbox.NewBreakerCoalescer(outboxStore, logger.With("component", "breaker-coalescer"))

	var broadcastDB *sql.DB
	pgNotifyDSN := cfg.PgNotifyDSN
	if pgNotifyDSN == "" {
		pgNotifyDSN = dsn
	}
	if cfg.EnablePgNotifyWakeups && pgNotifyDSN != "" {
		var err error
		broadcastDB, err = sql.Open("pgx", pgNotifyDSN)
		if err != nil {
			group.cancel()
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
		group.Go("pg_notify broadcaster", broadcaster.Run)
		group.Cleanup(func() {
			if err := broadcastDB.Close(); err != nil && logger != nil {
				logger.Warn("pg_notify database close failed", "error", err)
			}
		})
	}

	for i := 0; i < cfg.WorkerCount; i++ {
		workerID := i + 1
		group.Go(fmt.Sprintf("worker %d", workerID), func(ctx context.Context) error {
			w, err := worker.NewWithConfig(
				sched,
				cfg.Worker,
				worker.WithHTTPClient(httpClient),
				worker.WithHostLimiter(limiter),
				worker.WithLogger(logger.With("component", "worker", "worker_id", workerID)),
				worker.WithBreakerSink(breakerCoalescer),
			)
			if err != nil {
				return fmt.Errorf("config: %w", err)
			}
			return w.Run(ctx, outboxStore)
		})
	}

	group.Go("breaker coalescer", breakerCoalescer.Run)

	group.Go("discovery", func(ctx context.Context) error {
		return deps.runDiscovery(ctx, outboxStore, sched, cfg.Discovery)
	})

	if cfg.EnablePgNotifyWakeups && pgNotifyDSN != "" {
		group.Go("wake listener", func(ctx context.Context) error {
			return runCustomerWakeListener(ctx, logger.With("component", "pg-listener"), pgNotifyDSN, cfg.PgNotifyChannel, sched)
		})
	}

	group.Finish()

	return &deliveryRuntime{
		scheduler: sched,
		notifier:  notify,
		group:     group,
	}, group.errCh, nil
}
