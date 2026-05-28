package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// Event is a business fact that should be fanned out to matching Webhooks.
type Event struct {
	CustomerID string
	Type       string
	Payload    json.RawMessage
}

// SubmittedEvent describes the durable rows created for an Event.
type SubmittedEvent struct {
	EventID     string
	DeliveryIDs []string
}

// Store persists Events and their derived Delivery rows in Postgres.
type Store struct {
	db *sql.DB
}

// New returns an Event ingest store backed by db.
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// Submit writes the Event and one Delivery per matching active Webhook in
// one transaction.
func (s *Store) Submit(ctx context.Context, event Event) (SubmittedEvent, error) {
	if event.CustomerID == "" {
		return SubmittedEvent{}, fmt.Errorf("customer_id is required")
	}
	if event.Type == "" {
		return SubmittedEvent{}, fmt.Errorf("type is required")
	}
	if !json.Valid(event.Payload) {
		return SubmittedEvent{}, fmt.Errorf("payload must be valid JSON")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SubmittedEvent{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var submitted SubmittedEvent
	err = tx.QueryRowContext(ctx, `
		INSERT INTO events (customer_id, type, payload)
		VALUES ($1, $2, $3)
		RETURNING id
	`, event.CustomerID, event.Type, []byte(event.Payload)).Scan(&submitted.EventID)
	if err != nil {
		return SubmittedEvent{}, fmt.Errorf("insert event: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
		INSERT INTO deliveries (event_id, webhook_id, customer_id, status, attempt_count, next_attempt_at)
		SELECT $1, webhooks.id, webhooks.customer_id, 'pending', 0, now()
		FROM webhooks
		WHERE webhooks.customer_id = $2
		  AND webhooks.active = true
		  AND $3 = ANY(webhooks.event_types)
		ORDER BY webhooks.id
		RETURNING id
	`, submitted.EventID, event.CustomerID, event.Type)
	if err != nil {
		return SubmittedEvent{}, fmt.Errorf("insert deliveries: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return SubmittedEvent{}, fmt.Errorf("scan delivery id: %w", err)
		}
		submitted.DeliveryIDs = append(submitted.DeliveryIDs, id)
	}
	if err := rows.Err(); err != nil {
		return SubmittedEvent{}, fmt.Errorf("scan delivery ids: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return SubmittedEvent{}, fmt.Errorf("commit transaction: %w", err)
	}

	return submitted, nil
}
