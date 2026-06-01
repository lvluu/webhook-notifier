package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/lvluu/webhook-notifier/internal/ingest"
)

type ingestSubmitter interface {
	Submit(context.Context, ingest.Event) (ingest.SubmittedEvent, error)
}

func newBusinessHandler(logger *slog.Logger, ing ingestSubmitter, db *sql.DB) http.Handler {
	mux := http.NewServeMux()

	eventsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if ing == nil {
			logger.Error("ingest store is not configured")
			http.Error(w, "service unavailable", http.StatusInternalServerError)
			return
		}

		var raw map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}

		customerID, ok := jsonStringField(raw, "customer_id")
		if !ok || customerID == "" {
			http.Error(w, "customer_id is required", http.StatusBadRequest)
			return
		}
		eventType, ok := jsonStringField(raw, "event_name")
		if !ok || eventType == "" {
			http.Error(w, "event_name is required", http.StatusBadRequest)
			return
		}
		_, ok = jsonStringField(raw, "event_time")
		if !ok {
			http.Error(w, "timestamp is required", http.StatusBadRequest)
			return
		}
		delete(raw, "customer_id")
		payload, err := json.Marshal(raw)
		if err != nil {
			http.Error(w, "invalid event payload", http.StatusBadRequest)
			return
		}

		submitted, err := ing.Submit(r.Context(), ingest.Event{
			CustomerID: customerID,
			Type:       eventType,
			Payload:    payload,
		})
		if err != nil {
			logger.Warn("ingest submit failed", "error", err)
			http.Error(w, "invalid event payload", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(submitted)
	})

	mux.Handle("/events", eventsHandler)
	mux.Handle("/api/events", eventsHandler)

	mux.HandleFunc("/api/customers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var cmd struct {
			ID    string `json:"id"`
			Class uint32 `json:"class"`
			URL   string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		if cmd.ID == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}
		if cmd.URL == "" {
			http.Error(w, "url is required", http.StatusBadRequest)
			return
		}
		if db == nil {
			http.Error(w, "database not available", http.StatusInternalServerError)
			return
		}

		tx, err := db.BeginTx(r.Context(), nil)
		if err != nil {
			logger.Error("begin customer registration transaction", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		defer func() { _ = tx.Rollback() }()

		_, err = tx.ExecContext(r.Context(), `
			INSERT INTO customers (id, class)
			VALUES ($1, $2)
			ON CONFLICT (id) DO UPDATE SET class = EXCLUDED.class
		`, cmd.ID, cmd.Class)
		if err != nil {
			logger.Error("upsert customer", "customer_id", cmd.ID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		_, err = tx.ExecContext(r.Context(), `
			INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active)
			VALUES ($1, $2, '', ARRAY['subscriber.created', 'order.created', 'payment.received'], true)
			ON CONFLICT (customer_id, url) DO UPDATE
			SET active = true,
			    event_types = EXCLUDED.event_types
		`, cmd.ID, cmd.URL)
		if err != nil {
			logger.Error("upsert webhook", "customer_id", cmd.ID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(); err != nil {
			logger.Error("commit customer registration", "customer_id", cmd.ID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		logger.Info("customer registered", "customer_id", cmd.ID, "class", cmd.Class, "url", cmd.URL)
		w.WriteHeader(http.StatusCreated)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return mux
}

func jsonStringField(raw map[string]json.RawMessage, field string) (string, bool) {
	rawValue, ok := raw[field]
	if !ok {
		return "", false
	}
	var value string
	if err := json.Unmarshal(rawValue, &value); err != nil {
		return "", false
	}
	return value, true
}
