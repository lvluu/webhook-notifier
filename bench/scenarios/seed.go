package scenarios

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lvluu/webhook-notifier/internal/ingest"
)

// resetBenchData truncates all bench-managed tables so the scenario is
// idempotent across aborted runs.  The order respects FK constraints:
// child tables (delivery_attempts, dead_letters) are dropped before/during
// the CASCADE from deliveries; webhooks and events are truncated directly.
func resetBenchData(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		TRUNCATE
			webhooks,
			events,
			deliveries,
			delivery_attempts,
			dead_letters,
			customers
		CASCADE
	`)
	if err != nil {
		return fmt.Errorf("reset bench data: %w", err)
	}
	return nil
}

const (
	whaleCustomerID = "whale"
	lateSmallID     = "small-late"
	eventType       = "bench.test"
	whaleDeliveries = 50000
	smallCount      = 100
	smallDeliveries = 5
	lateDeliveries  = 3
	workerCount     = 8
)

func smallCustomerID(i int) string {
	return fmt.Sprintf("small-%03d", i)
}

func seedCustomers(ctx context.Context, db *sql.DB) error {
	all := []struct {
		id     string
		weight int
	}{
		{whaleCustomerID, 1},
		{lateSmallID, 1},
	}
	for i := 0; i < smallCount; i++ {
		all = append(all, struct {
			id     string
			weight int
		}{smallCustomerID(i), 1})
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO customers (id, weight) VALUES ")
	args := make([]any, 0, len(all)*2)
	for i, c := range all {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		args = append(args, c.id, c.weight)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")

	_, err := db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("seed customers: %w", err)
	}
	return nil
}

func seedWebhooks(ctx context.Context, db *sql.DB, receiverBaseURL string) error {
	customerIDs := []string{whaleCustomerID, lateSmallID}
	for i := 0; i < smallCount; i++ {
		customerIDs = append(customerIDs, smallCustomerID(i))
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")
	args := make([]any, 0, len(customerIDs)*4)
	for i, cid := range customerIDs {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * 4
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, true)", base+1, base+2, base+3, base+4))
		args = append(args, cid, receiverBaseURL+"/hooks/"+cid, "", fmt.Sprintf("{%s}", eventType))
	}
	sb.WriteString(" ON CONFLICT DO NOTHING")

	_, err := db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("seed webhooks: %w", err)
	}
	return nil
}

func seedWhaleDeliveries(ctx context.Context, db *sql.DB) (string, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var eventID string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO events (customer_id, type, payload)
		VALUES ($1, $2, '{}')
		RETURNING id
	`, whaleCustomerID, eventType).Scan(&eventID)
	if err != nil {
		return "", fmt.Errorf("insert whale event: %w", err)
	}

	webhookIDs, err := lookupCustomerWebhookIDs(ctx, tx, whaleCustomerID)
	if err != nil {
		return "", err
	}
	if len(webhookIDs) == 0 {
		return "", fmt.Errorf("no webhooks found for whale customer")
	}
	webhookID := webhookIDs[0]

	_, err = tx.ExecContext(ctx, `
		INSERT INTO deliveries (event_id, webhook_id, customer_id, status, attempt_count, next_attempt_at)
		SELECT $1, $2, $3, 'pending', 0, now()
		FROM generate_series(1, $4)
	`, eventID, webhookID, whaleCustomerID, whaleDeliveries)
	if err != nil {
		return "", fmt.Errorf("bulk insert whale deliveries: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit whale seed: %w", err)
	}

	return eventID, nil
}

func lookupCustomerWebhookIDs(ctx context.Context, tx *sql.Tx, customerID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id FROM webhooks WHERE customer_id = $1`, customerID)
	if err != nil {
		return nil, fmt.Errorf("lookup webhooks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan webhook id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate webhooks: %w", err)
	}
	return ids, nil
}

func ingestSmallDeliveries(ctx context.Context, store *ingest.Store, customerID string, count int) error {
	for range count {
		_, err := store.Submit(ctx, ingest.Event{
			CustomerID: customerID,
			Type:       eventType,
			Payload:    []byte("{}"),
		})
		if err != nil {
			return fmt.Errorf("ingest for %s: %w", customerID, err)
		}
	}
	return nil
}

func countPendingDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM deliveries
		WHERE status NOT IN ('succeeded', 'dead')
	`).Scan(&n)
	return n, err
}
