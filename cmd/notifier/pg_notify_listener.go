package main

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"time"

	"github.com/lib/pq"
)

type schedulerNotifier struct {
	scheduler  roundrobinScheduler
	db         *sql.DB
	logger     *slog.Logger
	notifyChan string
	enabled    bool
}

func (n schedulerNotifier) Notify(customerID string) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return
	}

	n.scheduler.Notify(customerID)
	if !n.enabled || n.db == nil || strings.TrimSpace(n.notifyChan) == "" {
		return
	}

	// scheduler.Scheduler.Notify does not accept a context; this best-effort
	// wakeup uses a short timeout so it cannot block the ingest path for long.
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := n.db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, n.notifyChan, customerID); err != nil && n.logger != nil {
		n.logger.Warn("pg_notify failed", "customer_id", customerID, "error", err)
	}
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
