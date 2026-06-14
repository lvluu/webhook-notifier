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
	CustomerClass uint32
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
			FOR UPDATE OF d SKIP LOCKED
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
	return s.commitBatchTx(tx, "success")
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
			FOR UPDATE OF d SKIP LOCKED
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
			    last_delay_millis = dc.delay_ms,
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
	return s.commitBatchTx(tx, "retry")
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
			FOR UPDATE OF d SKIP LOCKED
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
	return s.commitBatchTx(tx, "dead")
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

// MarkOutcomesBatch writes all three terminal outcome types — succeeded, retry,
// and dead — in a single database transaction, cutting the post-classification
// DB round trips from three sequential BEGIN/COMMIT pairs to one.
//
// RequeueBatch is deliberately excluded: requeueing does not write a
// delivery_attempt row and has different semantics (no attempt counter advance).
// Any of the three slices may be empty; empty slices contribute zero rows.
// The method is a no-op when all three are empty.
func (s *Store) MarkOutcomesBatch(ctx context.Context, succeeded, retries, dead []BatchMarkItem) error {
	if len(succeeded) == 0 && len(retries) == 0 && len(dead) == 0 {
		return nil
	}

	for i, it := range succeeded {
		if it.DeliveryID == "" {
			return fmt.Errorf("succeeded[%d]: delivery_id is required", i)
		}
	}
	for i, it := range retries {
		if it.DeliveryID == "" {
			return fmt.Errorf("retries[%d]: delivery_id is required", i)
		}
	}
	for i, it := range dead {
		if it.DeliveryID == "" {
			return fmt.Errorf("dead[%d]: delivery_id is required", i)
		}
		if it.Reason == "" {
			return fmt.Errorf("dead[%d]: reason is required", i)
		}
	}

	sIDs, sCodes, sDurations := outcomesBase(succeeded)
	rIDs, rCodes, rDurations, rErrClasses, rDelays := outcomesRetryFields(retries)
	dIDs, dCodes, dDurations, dErrClasses, dReasons := outcomesDeadFields(dead)

	tx, err := s.beginBatchTx(ctx, "outcomes")
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var attemptsMarked, deadLettersMarked int
	err = tx.QueryRowContext(ctx, `
		WITH succeeded_rows AS (
			SELECT id::uuid AS id, 'succeeded'::text AS outcome,
			       status_code, duration_ms,
			       NULL::text  AS error_class,
			       NULL::bigint AS delay_ms,
			       NULL::text  AS reason
			FROM unnest($1::text[], $2::int[], $3::bigint[])
			     AS t(id, status_code, duration_ms)
		),
		retry_rows AS (
			SELECT id::uuid, 'retry'::text,
			       status_code, duration_ms, error_class, delay_ms, NULL::text
			FROM unnest($4::text[], $5::int[], $6::bigint[], $7::text[], $8::bigint[])
			     AS t(id, status_code, duration_ms, error_class, delay_ms)
		),
		dead_rows AS (
			SELECT id::uuid, 'dead'::text,
			       status_code, duration_ms, error_class, NULL::bigint, reason
			FROM unnest($9::text[], $10::int[], $11::bigint[], $12::text[], $13::text[])
			     AS t(id, status_code, duration_ms, error_class, reason)
		),
		batch AS (
			SELECT * FROM succeeded_rows
			UNION ALL SELECT * FROM retry_rows
			UNION ALL SELECT * FROM dead_rows
		),
		locked_deliveries AS (
			SELECT d.id
			FROM deliveries d
			JOIN batch b ON d.id = b.id
			ORDER BY d.id
			FOR UPDATE OF d SKIP LOCKED
		),
		deleted_claims AS (
			DELETE FROM delivery_claims dc
			USING batch b
			JOIN locked_deliveries ld ON ld.id = b.id
			WHERE dc.delivery_id = ld.id
			RETURNING dc.delivery_id,
			          dc.attempt_number,
			          b.outcome,
			          b.status_code,
			          b.duration_ms,
			          b.error_class,
			          b.delay_ms,
			          b.reason
		),
		updated AS (
			UPDATE deliveries d
			SET status = CASE dc.outcome
			                 WHEN 'succeeded' THEN 'succeeded'
			                 WHEN 'retry'     THEN 'failed_retry'
			                 WHEN 'dead'      THEN 'dead'
			             END,
			    attempt_count     = dc.attempt_number,
			    updated_at        = now(),
			    last_error        = CASE dc.outcome
			                           WHEN 'succeeded' THEN NULL
			                           ELSE NULLIF(dc.error_class, '')
			                       END,
			    next_attempt_at   = CASE dc.outcome
			                           WHEN 'retry' THEN now() + (dc.delay_ms * interval '1 millisecond')
			                           ELSE d.next_attempt_at
			                       END,
			    last_delay_millis = CASE dc.outcome
			                           WHEN 'retry' THEN dc.delay_ms
			                           ELSE d.last_delay_millis
			                       END
			FROM deleted_claims dc
			WHERE d.id = dc.delivery_id
			RETURNING d.id, dc.outcome, dc.attempt_number, dc.status_code, dc.duration_ms, dc.error_class, dc.reason
		),
		attempts_inserted AS (
			INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, error_class, duration_ms, sent_at)
			SELECT id, attempt_number, status_code, NULLIF(error_class, ''), duration_ms, now()
			FROM updated
			RETURNING delivery_id
		),
		dead_letters_inserted AS (
			INSERT INTO dead_letters (delivery_id, last_error, attempts, reason, final_at)
			SELECT id, NULLIF(error_class, ''), attempt_number, reason, now()
			FROM updated
			WHERE outcome = 'dead'
			RETURNING delivery_id
		)
		SELECT
			(SELECT COUNT(*) FROM attempts_inserted)      AS attempts,
			(SELECT COUNT(*) FROM dead_letters_inserted)  AS dead_letters
	`,
		sIDs, sCodes, sDurations,
		rIDs, rCodes, rDurations, rErrClasses, rDelays,
		dIDs, dCodes, dDurations, dErrClasses, dReasons,
	).Scan(&attemptsMarked, &deadLettersMarked)
	if err != nil {
		return fmt.Errorf("mark outcomes batch: %w", err)
	}

	total := len(succeeded) + len(retries) + len(dead)
	if attemptsMarked != total {
		return fmt.Errorf("mark outcomes batch: inserted %d attempts for %d deliveries", attemptsMarked, total)
	}
	if deadLettersMarked != len(dead) {
		return fmt.Errorf("mark outcomes batch: inserted %d dead letters for %d dead deliveries", deadLettersMarked, len(dead))
	}

	return s.commitBatchTx(tx, "outcomes")
}

// outcomesBase extracts the common fields from a BatchMarkItem slice.
func outcomesBase(items []BatchMarkItem) (ids []string, statusCodes []int, durationsMs []int64) {
	ids = make([]string, len(items))
	statusCodes = make([]int, len(items))
	durationsMs = make([]int64, len(items))
	for i, it := range items {
		ids[i] = it.DeliveryID
		statusCodes[i] = it.StatusCode
		durationsMs[i] = it.Duration.Milliseconds()
	}
	return
}

// outcomesRetryFields extracts retry-specific fields.
func outcomesRetryFields(items []BatchMarkItem) (ids []string, statusCodes []int, durationsMs []int64, errorClasses []string, delaysMs []int64) {
	ids, statusCodes, durationsMs = outcomesBase(items)
	errorClasses = make([]string, len(items))
	delaysMs = make([]int64, len(items))
	for i, it := range items {
		errorClasses[i] = it.ErrorClass
		delaysMs[i] = it.Delay.Milliseconds()
	}
	return
}

// outcomesDeadFields extracts dead-specific fields.
func outcomesDeadFields(items []BatchMarkItem) (ids []string, statusCodes []int, durationsMs []int64, errorClasses []string, reasons []string) {
	ids, statusCodes, durationsMs = outcomesBase(items)
	errorClasses = make([]string, len(items))
	reasons = make([]string, len(items))
	for i, it := range items {
		errorClasses[i] = it.ErrorClass
		reasons[i] = it.Reason
	}
	return
}
