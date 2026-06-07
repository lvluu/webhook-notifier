package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
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
	lastBroadcastByCustomer sync.Map
	cooldownEntryCount      atomic.Int64
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

		pending: make(map[string]struct{}),
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
	if last, ok := b.lastBroadcastByCustomer.Load(customerID); ok {
		if now.Sub(last.(time.Time)) < b.cooldown {
			observe.IncPgNotifyBroadcast("coalesced")
			return
		}
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
		b.storeCooldown(customerID, b.now())
		b.mu.Unlock()

		// sync.Map iteration does not hold the pending mutex, so concurrent Notify
		// calls can continue while stale cooldown entries are scanned.
		if b.cooldownEntryCount.Load() > 1024 {
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
	b.lastBroadcastByCustomer.Range(func(id, last any) bool {
		if last.(time.Time).Before(deadline) && b.lastBroadcastByCustomer.CompareAndDelete(id, last) {
			b.cooldownEntryCount.Add(-1)
		}
		return true
	})
}

func (b *pgNotifyBroadcaster) storeCooldown(customerID string, timestamp time.Time) {
	for {
		last, loaded := b.lastBroadcastByCustomer.Load(customerID)
		if !loaded {
			if _, loaded = b.lastBroadcastByCustomer.LoadOrStore(customerID, timestamp); !loaded {
				b.cooldownEntryCount.Add(1)
				return
			}
			continue
		}
		if b.lastBroadcastByCustomer.CompareAndSwap(customerID, last, timestamp) {
			return
		}
	}
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
