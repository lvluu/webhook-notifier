package main

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
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

	mu                   sync.Mutex
	lastQueuedByCustomer map[string]time.Time
	pending              map[string]struct{}
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

		lastQueuedByCustomer: make(map[string]time.Time),
		pending:              make(map[string]struct{}),
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
	if lastQueued := b.lastQueuedByCustomer[customerID]; now.Sub(lastQueued) < b.cooldown {
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
		b.mu.Lock()
		delete(b.pending, customerID)
		b.lastQueuedByCustomer[customerID] = b.now()
		if len(b.lastQueuedByCustomer) > 1024 {
			deadline := b.now().Add(-10 * b.cooldown)
			for id, last := range b.lastQueuedByCustomer {
				if last.Before(deadline) {
					delete(b.lastQueuedByCustomer, id)
				}
			}
		}
		b.mu.Unlock()
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

func runCustomerWakeListener(ctx context.Context, logger *slog.Logger, dsn string, notifyChan string, sched roundrobinScheduler) error {
	notifyChan = strings.TrimSpace(notifyChan)
	if notifyChan == "" {
		return nil
	}

	reconnectMin := 2 * time.Second
	reconnectMax := 30 * time.Second
	listener := pq.NewListener(dsn, reconnectMin, reconnectMax, func(event pq.ListenerEventType, err error) {
		if err != nil && logger != nil {
			logger.Warn("postgres listener event", "event", event, "error", err)
		}
	})
	defer func() {
		if err := listener.Close(); err != nil && logger != nil {
			logger.Warn("postgres listener close failed", "error", err)
		}
	}()

	if err := listener.Listen(notifyChan); err != nil {
		return err
	}

	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case n := <-listener.Notify:
			if n == nil {
				continue
			}
			customerID := strings.TrimSpace(n.Extra)
			if customerID != "" {
				sched.Notify(customerID)
			}
		case <-pingTicker.C:
			if err := listener.Ping(); err != nil && logger != nil {
				logger.Warn("postgres listener ping failed", "error", err)
			}
		}
	}
}
