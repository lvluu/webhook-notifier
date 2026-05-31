// Package retention provides shared helpers for Delivery History retention tests.
package retention

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// NoopNotifier satisfies the ingest notifier dependency in tests.
type NoopNotifier struct{}

// Notify is a no-op for integration tests.
func (NoopNotifier) Notify(string) {}

// AgeTerminalDelivery shifts timestamps backward so a terminal Delivery is older than retention.
func AgeTerminalDelivery(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string, age time.Duration) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		UPDATE deliveries
		SET updated_at = now() - ($2 * interval '1 millisecond')
		WHERE id = $1
	`, deliveryID, age.Milliseconds())
	if err != nil {
		t.Fatalf("age delivery: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE delivery_attempts
		SET sent_at = now() - ($2 * interval '1 millisecond')
		WHERE delivery_id = $1
	`, deliveryID, age.Milliseconds())
	if err != nil {
		t.Fatalf("age delivery attempts: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE dead_letters
		SET final_at = now() - ($2 * interval '1 millisecond')
		WHERE delivery_id = $1
	`, deliveryID, age.Milliseconds())
	if err != nil {
		t.Fatalf("age dead letter: %v", err)
	}
}
