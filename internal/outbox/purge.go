package outbox

import (
	"context"
	"fmt"
	"time"
)

// PurgeOldDeadLetters deletes dead_letters rows whose final_at is older than
// maxAge, in batches of up to limit rows per call. Returns the number of rows
// deleted.
func (s *Store) PurgeOldDeadLetters(ctx context.Context, maxAge time.Duration, limit int) (int, error) {
	if maxAge <= 0 {
		return 0, fmt.Errorf("max-age must be > 0")
	}
	if limit < 1 {
		return 0, fmt.Errorf("limit must be >= 1")
	}

	const query = `
		DELETE FROM dead_letters
		WHERE delivery_id IN (
			SELECT delivery_id FROM dead_letters
			WHERE final_at < now() - ($1 * interval '1 millisecond')
			ORDER BY final_at
			LIMIT $2
		)
	`

	result, err := s.db.ExecContext(ctx, query, maxAge.Milliseconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge dead letters: %w", err)
	}

	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("purge dead letters rows affected: %w", err)
	}

	return int(n), nil
}

// PurgeOldDeliveryHistory deletes delivery_history rows whose final_at is older
// than maxAge, in batches of up to limit rows per call. Returns the number of
// rows deleted.
func (s *Store) PurgeOldDeliveryHistory(ctx context.Context, maxAge time.Duration, limit int) (int, error) {
	if maxAge <= 0 {
		return 0, fmt.Errorf("max-age must be > 0")
	}
	if limit < 1 {
		return 0, fmt.Errorf("limit must be >= 1")
	}

	const query = `
		DELETE FROM delivery_history
		WHERE ctid IN (
			SELECT ctid FROM delivery_history
			WHERE final_at < now() - ($1 * interval '1 millisecond')
			ORDER BY final_at
			LIMIT $2
		)
	`

	result, err := s.db.ExecContext(ctx, query, maxAge.Milliseconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge delivery history: %w", err)
	}

	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("purge delivery history rows affected: %w", err)
	}

	return int(n), nil
}
