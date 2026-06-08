package breaker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lvluu/webhook-notifier/internal/dberr"
	"github.com/lvluu/webhook-notifier/internal/observe"
)

// LoadFromDB loads persisted Webhook Circuit Breaker state.
func LoadFromDB(ctx context.Context, db *sql.DB) (snapshots map[string]Snapshot, err error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id::text, breaker_state, breaker_failure_count,
		       breaker_cooldown_ms, breaker_next_probe_at,
		       breaker_probe_delivery_id::text
		FROM webhooks
	`)
	if err != nil {
		return nil, fmt.Errorf("load breaker states: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close rows: %w", closeErr))
		}
	}()

	snapshots = make(map[string]Snapshot)
	for rows.Next() {
		var snapshot Snapshot
		var cooldownMillis int64
		var nextProbeAt sql.NullTime
		var probeDeliveryID sql.NullString
		if err := rows.Scan(
			&snapshot.WebhookID,
			&snapshot.State,
			&snapshot.FailureCount,
			&cooldownMillis,
			&nextProbeAt,
			&probeDeliveryID,
		); err != nil {
			return nil, fmt.Errorf("scan breaker state: %w", err)
		}
		snapshot.Cooldown = time.Duration(cooldownMillis) * time.Millisecond
		if nextProbeAt.Valid {
			snapshot.NextProbeAt = nextProbeAt.Time
		}
		if probeDeliveryID.Valid {
			snapshot.ProbeDeliveryID = probeDeliveryID.String
		}
		snapshots[snapshot.WebhookID] = snapshot
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate breaker states: %w", err)
	}
	return snapshots, nil
}

// PersistTransition writes one breaker lifecycle transition to Postgres.
func PersistTransition(ctx context.Context, db *sql.DB, transition Transition) error {
	switch transition.State {
	case Closed:
		if err := persistClosed(ctx, db, transition); err != nil {
			return err
		}
	case Open:
		if err := persistOpen(ctx, db, transition); err != nil {
			return err
		}
		observe.IncBreakerTrip(transition.Reason)
	case HalfOpen:
		if err := persistHalfOpen(ctx, db, transition); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown breaker state %q", transition.State)
	}
	observe.SetBreakerState(transition.WebhookID, string(transition.State))
	return nil
}

// AsyncPersister persists breaker transitions from a bounded queue.
type AsyncPersister struct {
	db      *sql.DB
	logger  *slog.Logger
	ch      chan Transition
	persist func(context.Context, *sql.DB, Transition) error
}

// NewAsyncPersister creates a bounded asynchronous transition persister.
func NewAsyncPersister(db *sql.DB, logger *slog.Logger, capacity int) *AsyncPersister {
	if capacity < 1 {
		capacity = 1
	}
	return &AsyncPersister{
		db:      db,
		logger:  logger,
		ch:      make(chan Transition, capacity),
		persist: PersistTransition,
	}
}

// Sink returns a transition sink suitable for passing to NewMemory.
func (p *AsyncPersister) Sink() TransitionSink {
	return func(transition Transition) {
		select {
		case p.ch <- transition:
		default:
			observe.IncBreakerSyncFallback()
			if p.logger != nil {
				p.logger.Warn("breaker transition persistence queue full; applying transition synchronously",
					"webhook_id", transition.WebhookID,
					"state", transition.State,
				)
			}
			if err := p.persist(context.Background(), p.db, transition); err != nil && p.logger != nil {
				p.logger.Error("persist breaker transition failed",
					"webhook_id", transition.WebhookID,
					"state", transition.State,
					"error", err,
				)
			}
		}
	}
}

// Run persists queued transitions until ctx is canceled.
func (p *AsyncPersister) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case transition := <-p.ch:
			if err := p.persist(ctx, p.db, transition); err != nil && p.logger != nil {
				p.logger.Error("persist breaker transition failed",
					"webhook_id", transition.WebhookID,
					"state", transition.State,
					"error", err,
				)
			}
		}
	}
}

func persistClosed(ctx context.Context, db *sql.DB, transition Transition) error {
	_, err := db.ExecContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'closed',
		    breaker_failure_count = 0,
		    breaker_opened_at = NULL,
		    breaker_next_probe_at = NULL,
		    breaker_cooldown_ms = $2,
		    breaker_probe_delivery_id = NULL
		WHERE id = $1::uuid
		  AND breaker_state <> 'closed'
	`, transition.WebhookID, transition.Cooldown.Milliseconds())
	if err != nil {
		return fmt.Errorf("persist breaker closed: %w", err)
	}
	return nil
}

func persistOpen(ctx context.Context, db *sql.DB, transition Transition) error {
	// Split into two sequential standalone transactions to avoid deadlock with
	// concurrent worker claim queries.
	//
	// Phase 1 — mark the webhook as 'open'. Fast, no delivery-row locks needed.
	// After this commits, worker claim eligibility filters out this webhook's
	// deliveries immediately (breaker_state = 'open' in DB), so no new claims
	// acquire locks on them.
	nextProbeAt := transition.NextProbeAt
	if nextProbeAt.IsZero() {
		nextProbeAt = time.Now().Add(transition.Cooldown)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'open',
		    breaker_failure_count = $2,
		    breaker_opened_at = now(),
		    breaker_next_probe_at = $3,
		    breaker_cooldown_ms = $4,
		    breaker_probe_delivery_id = NULL
		WHERE id = $1::uuid
	`, transition.WebhookID, transition.FailureCount, nextProbeAt, transition.Cooldown.Milliseconds()); err != nil {
		return fmt.Errorf("persist breaker open: %w", err)
	}

	// Phase 2 — defer pending deliveries for this webhook. Runs in its own
	// transaction WITHOUT a webhook-row lock, so there is no lock-order cycle
	// with in-flight worker claims (workers hold delivery locks, want webhooks;
	// we want delivery locks but don't hold webhooks → no cycle).
	//
	// Phase 2 is an optimization, not a correctness requirement: after Phase 1
	// commits, the claim-eligibility filter already excludes this webhook's
	// deliveries until breaker_next_probe_at (open deliveries are claimable only
	// when breaker_next_probe_at <= now()). Deferring next_attempt_at merely
	// stops the scheduler from waking on rows it cannot claim. A Phase 2 failure
	// is therefore benign — the breaker is already open and enforced — so the
	// returned error is advisory for observability rather than a rollback signal.
	return deferBlockedDeliveries(ctx, db, []string{transition.WebhookID})
}

func persistHalfOpen(ctx context.Context, db *sql.DB, transition Transition) error {
	if transition.ProbeDeliveryID == "" {
		return nil
	}
	_, err := db.ExecContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'half_open',
		    breaker_probe_delivery_id = $2::uuid
		WHERE id = $1::uuid
		  AND breaker_state <> 'closed'
	`, transition.WebhookID, transition.ProbeDeliveryID)
	if err != nil {
		return fmt.Errorf("persist breaker half-open: %w", err)
	}
	return nil
}

func deferBlockedDeliveries(ctx context.Context, db *sql.DB, webhookIDs []string) error {
	if len(webhookIDs) == 0 {
		return nil
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE deliveries d
		SET next_attempt_at = GREATEST(d.next_attempt_at, w.breaker_next_probe_at),
		    updated_at = now()
		FROM webhooks w
		WHERE w.id = ANY($1::uuid[])
		  AND d.webhook_id = w.id
		  AND d.status IN ('pending', 'failed_retry')
		  AND w.breaker_next_probe_at IS NOT NULL
		  AND d.next_attempt_at < w.breaker_next_probe_at
	`, webhookIDs); err != nil {
		// Lock contention here is benign and not retried. This UPDATE is a Phase 2
		// optimization that merely pushes next_attempt_at past breaker_next_probe_at
		// so the scheduler stops waking on rows it cannot claim. Phase 1 already
		// committed breaker_state='open', and the claim-eligibility filter blocks
		// these deliveries until breaker_next_probe_at <= now() regardless. So on a
		// deadlock/lock-not-available we log and move on rather than retrying on the
		// breaker-open hot path; worst case is a few wasted scheduler polls that
		// self-correct on the next breaker transition.
		if dberr.IsRetryableLock(err) {
			slog.Default().Warn("defer blocked deliveries skipped on lock contention; breaker filter still enforced",
				"webhook_ids", webhookIDs,
				"error", err,
			)
			return nil
		}
		return fmt.Errorf("defer blocked deliveries: %w", err)
	}
	return nil
}
