package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
)

// BreakerOutcome is one delivery attempt result relevant to the circuit
// breaker, produced by the worker after a batch mark commits and applied out of
// band by a BreakerCoalescer. WebhookID lets the coalescer lock the involved
// webhook rows in a deterministic order without a lookup.
type BreakerOutcome struct {
	WebhookID  string
	DeliveryID string
	Success    bool
	Reason     string
}

// lockWebhooksOrdered acquires the given webhook rows in one id-sorted
// FOR NO KEY UPDATE statement so concurrent breaker-applying transactions take
// them in the same order and cannot deadlock. FOR NO KEY UPDATE (not FOR UPDATE)
// is compatible with the FOR KEY SHARE lock that delivery inserts take on their
// parent webhook, so ordering breaker writes never blocks ingest. With a single
// coalescer per process the only contenders here are the few per-process flush
// transactions, where this ordering is cheap and effective (ADR-0015).
func lockWebhooksOrdered(ctx context.Context, tx *sql.Tx, webhookIDs []string) error {
	if len(webhookIDs) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		SELECT id
		FROM webhooks
		WHERE id = ANY($1::uuid[])
		ORDER BY id
		FOR NO KEY UPDATE
	`, webhookIDs); err != nil {
		return fmt.Errorf("lock webhooks in order: %w", err)
	}
	return nil
}

// ApplyBreakerOutcomes applies a batch of delivery outcomes to webhook breaker
// state in a single transaction: successes reset their breaker, failures
// increment the persisted counter and open breakers that reach the threshold.
// It is the out-of-band counterpart to the inline breaker writes that
// WithDeferredBreaker disables on the mark path. Coalescing many mark cycles
// from all of a process's workers into one transaction — and running only one
// coalescer per process — collapses the breaker write concurrency from
// (workers x replicas) down to (replicas), removing the hot-row contention that
// caused partner-outage deadlocks. Breaker open/close events are emitted after
// commit.
func (s *Store) ApplyBreakerOutcomes(ctx context.Context, outcomes []BreakerOutcome) error {
	if s.breakerConfig.FailureThreshold == 0 || len(outcomes) == 0 {
		return nil
	}

	var successIDs, failIDs []string
	var failItems []BatchMarkItem
	widSet := make(map[string]struct{}, len(outcomes))
	for _, o := range outcomes {
		if o.WebhookID != "" {
			widSet[o.WebhookID] = struct{}{}
		}
		if o.Success {
			successIDs = append(successIDs, o.DeliveryID)
		} else {
			failIDs = append(failIDs, o.DeliveryID)
			failItems = append(failItems, BatchMarkItem{DeliveryID: o.DeliveryID, ErrorClass: o.Reason, Reason: o.Reason})
		}
	}
	webhookIDs := make([]string, 0, len(widSet))
	for wid := range widSet {
		webhookIDs = append(webhookIDs, wid)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin breaker outcomes transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := lockWebhooksOrdered(ctx, tx, webhookIDs); err != nil {
		return err
	}

	// Successes first so a success and a later failure for the same webhook in
	// this batch are applied in order (success resets, failure then increments),
	// matching the per-cycle ordering the inline mark path used.
	var closed []string
	if len(successIDs) > 0 {
		closed, err = s.batchRecordBreakerSuccess(ctx, tx, successIDs)
		if err != nil {
			return err
		}
	}
	var tripped []trippedBreaker
	if len(failItems) > 0 {
		tripped, err = s.batchRecordBreakerFailure(ctx, tx, failIDs, failItems, func(i BatchMarkItem) string { return i.ErrorClass })
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit breaker outcomes: %w", err)
	}

	for _, wid := range closed {
		emitBreakerClosed(wid)
	}
	for _, ev := range tripped {
		emitBreakerOpened(ev.webhookID, ev.reason)
	}
	return nil
}

const (
	defaultBreakerCoalesceBuffer = 4096
	defaultBreakerCoalesceMax    = 512
	defaultBreakerCoalesceWindow = 100 * time.Millisecond
	breakerFlushTimeout          = 15 * time.Second
)

// BreakerCoalescer is the single per-process consumer of BreakerOutcomes. It
// buffers outcomes from all workers and flushes them in batched transactions,
// so breaker state is maintained off the synchronous mark path without every
// worker contending on the hot webhook rows.
type BreakerCoalescer struct {
	store    *Store
	logger   *slog.Logger
	ch       chan BreakerOutcome
	maxBatch int
	window   time.Duration
}

// NewBreakerCoalescer builds a coalescer for store. A nil logger is tolerated.
func NewBreakerCoalescer(store *Store, logger *slog.Logger) *BreakerCoalescer {
	return &BreakerCoalescer{
		store:    store,
		logger:   logger,
		ch:       make(chan BreakerOutcome, defaultBreakerCoalesceBuffer),
		maxBatch: defaultBreakerCoalesceMax,
		window:   defaultBreakerCoalesceWindow,
	}
}

// Enqueue offers outcomes to the coalescer without blocking the worker. If the
// buffer is full the outcome is dropped: the breaker is a self-correcting
// heuristic, so a missed sample at saturation only slightly delays a transition
// and is preferable to stalling delivery.
func (c *BreakerCoalescer) Enqueue(outcomes []BreakerOutcome) {
	for _, o := range outcomes {
		select {
		case c.ch <- o:
		default:
			observe.IncBreakerCoalescerDropped()
			if c.logger != nil {
				c.logger.Warn("breaker outcome buffer full; dropping breaker sample",
					"webhook_id", o.WebhookID, "delivery_id", o.DeliveryID)
			}
		}
	}
}

// Run consumes outcomes until ctx is cancelled, flushing on a full batch or the
// coalesce window, whichever comes first.
func (c *BreakerCoalescer) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.window)
	defer ticker.Stop()

	batch := make([]BreakerOutcome, 0, c.maxBatch)
	for {
		select {
		case <-ctx.Done():
			c.drainInto(&batch)
			c.flush(context.Background(), batch)
			return ctx.Err()
		case o := <-c.ch:
			batch = append(batch, o)
			if len(batch) >= c.maxBatch {
				c.flush(ctx, batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				c.flush(ctx, batch)
				batch = batch[:0]
			}
		}
	}
}

// drainInto appends any immediately-available buffered outcomes to batch,
// bounded by maxBatch, for a best-effort flush on shutdown.
func (c *BreakerCoalescer) drainInto(batch *[]BreakerOutcome) {
	for len(*batch) < c.maxBatch {
		select {
		case o := <-c.ch:
			*batch = append(*batch, o)
		default:
			return
		}
	}
}

func (c *BreakerCoalescer) flush(ctx context.Context, batch []BreakerOutcome) {
	if len(batch) == 0 {
		return
	}
	flushCtx, cancel := context.WithTimeout(ctx, breakerFlushTimeout)
	defer cancel()
	if err := c.store.ApplyBreakerOutcomes(flushCtx, batch); err != nil {
		if c.logger != nil {
			c.logger.Warn("apply breaker outcomes failed; dropping batch",
				"count", len(batch), "error", err)
		}
	}
}
