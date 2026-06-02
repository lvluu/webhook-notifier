package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type batchBase struct {
	ids         []string
	statusCodes []int
	durationsMs []int64
}

type batchRetry struct {
	batchBase
	errorClasses []string
	delaysMs     []int64
}

type batchDead struct {
	batchBase
	errorClasses []string
	reasons      []string
}

// BatchMarkItem is one delivery outcome to be applied in a batched mark operation.
type BatchMarkItem struct {
	DeliveryID string
	StatusCode int
	Duration   time.Duration
	ErrorClass string
	Delay      time.Duration
	Reason     string

	// Observation-only fields (not used in batch SQL).
	Lifetime      time.Duration
	AttemptNumber int
}

// MarkSucceededBatch marks multiple deliveries as succeeded in one round trip.
// Updates delivery status and inserts attempt records via a single CTE.
func (s *Store) MarkSucceededBatch(ctx context.Context, items []BatchMarkItem) error {
	if len(items) == 0 {
		return nil
	}

	base, err := prepareBatchBase(items)
	if err != nil {
		return err
	}

	tx, err := s.beginBatchTx(ctx, "success")
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var marked int
	err = tx.QueryRowContext(ctx, `
		WITH batch AS (
			SELECT * FROM unnest(
				$1::text[], $2::int[], $3::bigint[]
			) AS t(id, status_code, duration_ms)
		),
		locked_deliveries AS (
			SELECT d.id
			FROM deliveries d
			JOIN batch b ON d.id = b.id::uuid
			ORDER BY d.id
			FOR UPDATE OF d
		),
		deleted_claims AS (
			DELETE FROM delivery_claims dc
			USING batch b
			JOIN locked_deliveries ld ON ld.id = b.id::uuid
			WHERE dc.delivery_id = ld.id
			RETURNING dc.delivery_id, dc.attempt_number, b.status_code, b.duration_ms
		),
		updated AS (
			UPDATE deliveries d
			SET status = 'succeeded',
			    attempt_count = dc.attempt_number,
			    updated_at = now(),
			    last_error = NULL
			FROM deleted_claims dc
			WHERE d.id = dc.delivery_id
			RETURNING d.id, dc.attempt_number, dc.status_code, dc.duration_ms
		),
		inserted AS (
			INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, duration_ms, sent_at)
			SELECT id, attempt_number, status_code, duration_ms, now() FROM updated
			RETURNING delivery_id
		)
		SELECT COUNT(*) FROM inserted
	`, base.ids, base.statusCodes, base.durationsMs).Scan(&marked)
	if err != nil {
		return fmt.Errorf("batch mark succeeded: %w", err)
	}
	if marked != len(items) {
		return fmt.Errorf("batch mark succeeded: marked %d of %d deliveries", marked, len(items))
	}
	if s.breakerConfig.FailureThreshold == 0 {
		return s.commitBatchTx(tx, "success")
	}
	webhookIDs, err := batchDeliveryWebhookIDs(ctx, tx, base.ids)
	if err != nil {
		return err
	}
	var closedBreakers []string
	for _, item := range items {
		wid, ok := webhookIDs[item.DeliveryID]
		if !ok {
			return fmt.Errorf("delivery %s not found", item.DeliveryID)
		}
		closed, err := s.recordBreakerSuccess(ctx, tx, wid, item.DeliveryID)
		if err != nil {
			return err
		}
		if closed {
			closedBreakers = append(closedBreakers, wid)
		}
	}
	if err := s.commitBatchTx(tx, "success"); err != nil {
		return err
	}
	for _, wid := range closedBreakers {
		emitBreakerClosed(wid)
	}
	return nil
}

// MarkRetryBatch schedules multiple deliveries for retry in one round trip.
func (s *Store) MarkRetryBatch(ctx context.Context, items []BatchMarkItem) error {
	if len(items) == 0 {
		return nil
	}

	retryBatch, err := prepareBatchRetry(items)
	if err != nil {
		return err
	}

	tx, err := s.beginBatchTx(ctx, "retry")
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var marked int
	err = tx.QueryRowContext(ctx, `
		WITH batch AS (
			SELECT * FROM unnest(
				$1::text[], $2::int[], $3::bigint[], $4::text[], $5::bigint[]
			) AS t(id, status_code, duration_ms, error_class, delay_ms)
		),
		locked_deliveries AS (
			SELECT d.id
			FROM deliveries d
			JOIN batch b ON d.id = b.id::uuid
			ORDER BY d.id
			FOR UPDATE OF d
		),
		deleted_claims AS (
			DELETE FROM delivery_claims dc
			USING batch b
			JOIN locked_deliveries ld ON ld.id = b.id::uuid
			WHERE dc.delivery_id = ld.id
			RETURNING dc.delivery_id, dc.attempt_number, b.status_code, b.duration_ms, b.error_class, b.delay_ms
		),
		updated AS (
			UPDATE deliveries d
			SET status = 'failed_retry',
			    attempt_count = dc.attempt_number,
			    next_attempt_at = now() + (dc.delay_ms * interval '1 millisecond'),
			    last_error = NULLIF(dc.error_class, ''),
			    updated_at = now()
			FROM deleted_claims dc
			WHERE d.id = dc.delivery_id
			RETURNING d.id, dc.attempt_number, dc.status_code, dc.duration_ms, dc.error_class
		),
		inserted AS (
			INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, error_class, duration_ms, sent_at)
			SELECT id, attempt_number, status_code, NULLIF(error_class, ''), duration_ms, now() FROM updated
			RETURNING delivery_id
		)
		SELECT COUNT(*) FROM inserted
	`, retryBatch.ids, retryBatch.statusCodes, retryBatch.durationsMs, retryBatch.errorClasses, retryBatch.delaysMs).Scan(&marked)
	if err != nil {
		return fmt.Errorf("batch mark retry: %w", err)
	}
	if marked != len(items) {
		return fmt.Errorf("batch mark retry: marked %d of %d deliveries", marked, len(items))
	}
	if s.breakerConfig.FailureThreshold == 0 {
		return s.commitBatchTx(tx, "retry")
	}
	webhookIDs, err := batchDeliveryWebhookIDs(ctx, tx, retryBatch.ids)
	if err != nil {
		return err
	}
	var trippedBreakers []struct {
		webhookID string
		reason    string
	}
	for _, item := range items {
		wid, ok := webhookIDs[item.DeliveryID]
		if !ok {
			return fmt.Errorf("delivery %s not found", item.DeliveryID)
		}
		tripped, err := s.recordBreakerFailure(ctx, tx, wid, item.ErrorClass)
		if err != nil {
			return err
		}
		if tripped {
			trippedBreakers = append(trippedBreakers, struct {
				webhookID string
				reason    string
			}{wid, item.ErrorClass})
		}
	}
	if err := s.commitBatchTx(tx, "retry"); err != nil {
		return err
	}
	for _, ev := range trippedBreakers {
		emitBreakerOpened(ev.webhookID, ev.reason)
	}
	return nil
}

// MarkDeadBatch marks multiple deliveries as dead in one round trip, recording
// dead_letter rows for each.
func (s *Store) MarkDeadBatch(ctx context.Context, items []BatchMarkItem) error {
	if len(items) == 0 {
		return nil
	}

	deadBatch, err := prepareBatchDead(items)
	if err != nil {
		return err
	}

	tx, err := s.beginBatchTx(ctx, "dead")
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var attemptsMarked, deadLettersMarked int
	err = tx.QueryRowContext(ctx, `
		WITH batch AS (
			SELECT * FROM unnest(
				$1::text[], $2::int[], $3::bigint[], $4::text[], $5::text[]
			) AS t(id, status_code, duration_ms, error_class, reason)
		),
		locked_deliveries AS (
			SELECT d.id
			FROM deliveries d
			JOIN batch b ON d.id = b.id::uuid
			ORDER BY d.id
			FOR UPDATE OF d
		),
		deleted_claims AS (
			DELETE FROM delivery_claims dc
			USING batch b
			JOIN locked_deliveries ld ON ld.id = b.id::uuid
			WHERE dc.delivery_id = ld.id
			RETURNING dc.delivery_id, dc.attempt_number, b.status_code, b.duration_ms, b.error_class, b.reason
		),
		updated AS (
			UPDATE deliveries d
			SET status = 'dead',
			    attempt_count = dc.attempt_number,
			    last_error = NULLIF(dc.error_class, ''),
			    updated_at = now()
			FROM deleted_claims dc
			WHERE d.id = dc.delivery_id
			RETURNING d.id, dc.attempt_number, dc.status_code, dc.duration_ms, dc.error_class, dc.reason
		),
		attempts_inserted AS (
			INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, error_class, duration_ms, sent_at)
			SELECT id, attempt_number, status_code, NULLIF(error_class, ''), duration_ms, now() FROM updated
			RETURNING delivery_id
		),
		dead_letters_inserted AS (
			INSERT INTO dead_letters (delivery_id, last_error, attempts, reason, final_at)
			SELECT id, NULLIF(error_class, ''), attempt_number, reason, now() FROM updated
			RETURNING delivery_id
		)
		SELECT
			(SELECT COUNT(*) FROM attempts_inserted),
			(SELECT COUNT(*) FROM dead_letters_inserted)
	`, deadBatch.ids, deadBatch.statusCodes, deadBatch.durationsMs, deadBatch.errorClasses, deadBatch.reasons).Scan(&attemptsMarked, &deadLettersMarked)
	if err != nil {
		return fmt.Errorf("batch mark dead: %w", err)
	}
	if attemptsMarked != len(items) || deadLettersMarked != len(items) {
		return fmt.Errorf("batch mark dead: marked %d attempts and %d dead letters of %d deliveries", attemptsMarked, deadLettersMarked, len(items))
	}
	if s.breakerConfig.FailureThreshold == 0 {
		return s.commitBatchTx(tx, "dead")
	}
	webhookIDs, err := batchDeliveryWebhookIDs(ctx, tx, deadBatch.ids)
	if err != nil {
		return err
	}
	var trippedBreakers []struct {
		webhookID string
		reason    string
	}
	for _, item := range items {
		wid, ok := webhookIDs[item.DeliveryID]
		if !ok {
			return fmt.Errorf("delivery %s not found", item.DeliveryID)
		}
		tripped, err := s.recordBreakerFailure(ctx, tx, wid, item.Reason)
		if err != nil {
			return err
		}
		if tripped {
			trippedBreakers = append(trippedBreakers, struct {
				webhookID string
				reason    string
			}{wid, item.Reason})
		}
	}
	if err := s.commitBatchTx(tx, "dead"); err != nil {
		return err
	}
	for _, ev := range trippedBreakers {
		emitBreakerOpened(ev.webhookID, ev.reason)
	}
	return nil
}

func prepareBatchBase(items []BatchMarkItem) (batchBase, error) {
	ids := make([]string, len(items))
	statusCodes := make([]int, len(items))
	durationsMs := make([]int64, len(items))
	for i, item := range items {
		if item.DeliveryID == "" {
			return batchBase{}, fmt.Errorf("delivery_id is required at index %d", i)
		}
		ids[i] = item.DeliveryID
		statusCodes[i] = item.StatusCode
		durationsMs[i] = item.Duration.Milliseconds()
	}
	return batchBase{ids: ids, statusCodes: statusCodes, durationsMs: durationsMs}, nil
}

func prepareBatchRetry(items []BatchMarkItem) (batchRetry, error) {
	base, err := prepareBatchBase(items)
	if err != nil {
		return batchRetry{}, err
	}
	errorClasses := make([]string, len(items))
	delaysMs := make([]int64, len(items))
	for i, item := range items {
		errorClasses[i] = item.ErrorClass
		delaysMs[i] = item.Delay.Milliseconds()
	}
	return batchRetry{
		batchBase:    base,
		errorClasses: errorClasses,
		delaysMs:     delaysMs,
	}, nil
}

func prepareBatchDead(items []BatchMarkItem) (batchDead, error) {
	base, err := prepareBatchBase(items)
	if err != nil {
		return batchDead{}, err
	}
	errorClasses := make([]string, len(items))
	reasons := make([]string, len(items))
	for i, item := range items {
		if item.Reason == "" {
			return batchDead{}, fmt.Errorf("reason is required at index %d", i)
		}
		errorClasses[i] = item.ErrorClass
		reasons[i] = item.Reason
	}
	return batchDead{
		batchBase:    base,
		errorClasses: errorClasses,
		reasons:      reasons,
	}, nil
}

func (s *Store) beginBatchTx(ctx context.Context, kind string) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin batch %s transaction: %w", kind, err)
	}
	return tx, nil
}

func (s *Store) commitBatchTx(tx *sql.Tx, kind string) error {
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch %s transaction: %w", kind, err)
	}
	return nil
}
