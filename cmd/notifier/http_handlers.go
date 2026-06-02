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
		if err := upsertCustomer(r.Context(), db, cmd.ID, cmd.Class, cmd.URL); err != nil {
			logger.Error("registration failed", "customer_id", cmd.ID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		logger.Info("customer registered", "customer_id", cmd.ID, "class", cmd.Class, "url", cmd.URL)
		w.WriteHeader(http.StatusCreated)
	})

	mux.HandleFunc("/api/customers/batch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if db == nil {
			http.Error(w, "database not available", http.StatusInternalServerError)
			return
		}

		var cmds []struct {
			ID    string `json:"id"`
			Class uint32 `json:"class"`
			URL   string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&cmds); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}

		tx, err := db.BeginTx(r.Context(), nil)
		if err != nil {
			logger.Error("begin batch registration transaction", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		defer func() { _ = tx.Rollback() }()

		var ok int
		for _, cmd := range cmds {
			if cmd.ID == "" || cmd.URL == "" {
				continue
			}
			if err := upsertCustomerTx(r.Context(), tx, cmd.ID, cmd.Class, cmd.URL); err != nil {
				logger.Warn("batch registration skipped", "customer_id", cmd.ID, "error", err)
				continue
			}
			ok++
		}

		if err := tx.Commit(); err != nil {
			logger.Error("commit batch registration", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		logger.Info("batch customers registered", "ok", ok, "total", len(cmds))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]int{"registered": ok})
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

func upsertCustomer(ctx context.Context, db *sql.DB, id string, class uint32, url string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := upsertCustomerTx(ctx, tx, id, class, url); err != nil {
		return err
	}
	return tx.Commit()
}

func upsertCustomerTx(ctx context.Context, tx *sql.Tx, id string, class uint32, url string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO customers (id, class)
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET class = EXCLUDED.class
	`, id, class)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active)
		VALUES ($1, $2, '', ARRAY['subscriber.created', 'order.created', 'payment.received'], true)
		ON CONFLICT (customer_id, url) DO UPDATE
		SET active = true,
		    event_types = EXCLUDED.event_types
	`, id, url)
	return err
}
