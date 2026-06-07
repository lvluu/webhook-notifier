package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lvluu/webhook-notifier/internal/observe"
)

const (
	pgNotifyBroadcastCooldown = 100 * time.Millisecond
	pgNotifyBroadcastTimeout  = 250 * time.Millisecond
)

type schedulerNotifier struct {
	scheduler   roundrobinScheduler
	broadcaster customerWakeBroadcaster
}

type customerWakeBroadcaster interface {
	Notify(string)
}

type pgNotifyExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type pgNotifyBroadcaster struct {
	executor   pgNotifyExecutor
	logger     *slog.Logger
	notifyChan string
	timeout    time.Duration
	cooldown   time.Duration
	queue      chan string
	now        func() time.Time

	mu                      sync.Mutex
	lastBroadcastByCustomer map[string]time.Time
	pending                 map[string]struct{}
}

func (n schedulerNotifier) Notify(customerID string) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return
	}

	n.scheduler.Notify(customerID)
	if n.broadcaster != nil {
		n.broadcaster.Notify(customerID)
	}
}

func newPgNotifyBroadcaster(executor pgNotifyExecutor, logger *slog.Logger, notifyChan string, queueSize int) *pgNotifyBroadcaster {
	return &pgNotifyBroadcaster{
		executor:   executor,
		logger:     logger,
		notifyChan: strings.TrimSpace(notifyChan),
		timeout:    pgNotifyBroadcastTimeout,
		cooldown:   pgNotifyBroadcastCooldown,
		queue:      make(chan string, queueSize),
		now:        time.Now,

		lastBroadcastByCustomer: make(map[string]time.Time),
		pending:                 make(map[string]struct{}),
	}
}

func (b *pgNotifyBroadcaster) Notify(customerID string) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" || b.executor == nil || b.notifyChan == "" {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.pending[customerID]; ok {
		observe.IncPgNotifyBroadcast("coalesced")
		return
	}
	now := b.now()
	if lastBroadcast := b.lastBroadcastByCustomer[customerID]; now.Sub(lastBroadcast) < b.cooldown {
		observe.IncPgNotifyBroadcast("coalesced")
		return
	}

	select {
	case b.queue <- customerID:
		b.pending[customerID] = struct{}{}
	default:
		observe.IncPgNotifyBroadcast("dropped")
	}
}

func (b *pgNotifyBroadcaster) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case customerID := <-b.queue:
			b.broadcast(ctx, customerID)
		}
	}
}

func (b *pgNotifyBroadcaster) broadcast(parent context.Context, customerID string) {
	defer func() {
		// Update pending state and record cooldown timestamp — brief, no iteration.
		b.mu.Lock()
		delete(b.pending, customerID)
		b.lastBroadcastByCustomer[customerID] = b.now()
		overLimit := len(b.lastBroadcastByCustomer) > 1024
		b.mu.Unlock()

		// Prune stale entries after releasing the lock so that concurrent Notify
		// callers are not blocked for the O(n) scan duration.
		if overLimit {
			b.pruneStaleEntries()
		}
	}()

	ctx, cancel := context.WithTimeout(parent, b.timeout)
	defer cancel()
	if _, err := b.executor.ExecContext(ctx, `SELECT pg_notify($1, $2)`, b.notifyChan, customerID); err != nil {
		observe.IncPgNotifyBroadcast("failed")
		if b.logger != nil {
			b.logger.Warn("pg_notify failed", "customer_id", customerID, "error", err)
		}
		return
	}
	observe.IncPgNotifyBroadcast("sent")
}

// pruneStaleEntries removes cooldown map entries that are older than
// 10x the cooldown window. Called from the broadcast defer after the
// primary mutex section so Notify callers are not blocked for the scan.
func (b *pgNotifyBroadcaster) pruneStaleEntries() {
	deadline := b.now().Add(-10 * b.cooldown)
	b.mu.Lock()
	for id, last := range b.lastBroadcastByCustomer {
		if last.Before(deadline) {
			delete(b.lastBroadcastByCustomer, id)
		}
	}
	b.mu.Unlock()
}

func runCustomerWakeListener(ctx context.Context, logger *slog.Logger, dsn string, notifyChan string, sched roundrobinScheduler) error {
	notifyChan = strings.TrimSpace(notifyChan)
	if notifyChan == "" {
		return nil
	}

	const reconnectDelay = time.Second

	for {
		if err := listenOnce(ctx, logger, dsn, notifyChan, sched); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if logger != nil {
				logger.Warn("listener disconnected, reconnecting", "error", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(reconnectDelay):
			}
		}
	}
}

func listenOnce(ctx context.Context, logger *slog.Logger, dsn string, notifyChan string, sched roundrobinScheduler) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil && logger != nil {
			logger.Warn("close pg_notify connection", "error", err)
		}
	}()

	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{notifyChan}.Sanitize()); err != nil {
		return fmt.Errorf("listen %s: %w", notifyChan, err)
	}

	if logger != nil {
		logger.Info("listening for postgres notifications", "channel", notifyChan)
	}

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("wait for notification: %w", err)
		}
		customerID := strings.TrimSpace(notification.Payload)
		if customerID != "" {
			sched.Notify(customerID)
		}
	}
}
