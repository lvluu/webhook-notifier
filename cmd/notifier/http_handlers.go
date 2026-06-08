package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/lvluu/webhook-notifier/internal/ingest"
)

const maxEventBodySize = 1 << 20 // 1 MB

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

		r.Body = http.MaxBytesReader(w, r.Body, maxEventBodySize)
		var raw map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
				return
			}
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

	mux.HandleFunc("/setup-customers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if db == nil {
			http.Error(w, "database not available", http.StatusInternalServerError)
			return
		}

		var cmd struct {
			CustomerBase string `json:"customer_base"`
			Count        int    `json:"count"`
			Class        uint32 `json:"class"`
			ReceiverURL  string `json:"receiver_url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		if cmd.Count < 1 || cmd.Count > 50000 {
			http.Error(w, "count must be 1-50000", http.StatusBadRequest)
			return
		}
		if cmd.CustomerBase == "" || cmd.ReceiverURL == "" {
			http.Error(w, "customer_base and receiver_url are required", http.StatusBadRequest)
			return
		}

		const txBatch = 500
		var totalOK int
		paths := []string{"/hooks/fast", "/hooks/slow", "/hooks/flaky"}
		for start := 0; start < cmd.Count; start += txBatch {
			end := min(start+txBatch, cmd.Count)

			tx, err := db.BeginTx(r.Context(), nil)
			if err != nil {
				logger.Error("begin setup transaction", "error", err)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}

			var sb strings.Builder
			sb.WriteString("INSERT INTO customers (id, class) VALUES ")
			custArgs := make([]any, 0, (end-start)*2)
			for i := start; i < end; i++ {
				if i > start {
					sb.WriteString(", ")
				}
				base := (i - start) * 2
				_, _ = fmt.Fprintf(&sb, "($%d, $%d)", base+1, base+2)
				cid := fmt.Sprintf("%s-%04d", cmd.CustomerBase, i+1)
				custArgs = append(custArgs, cid, cmd.Class)
			}
			sb.WriteString(" ON CONFLICT (id) DO UPDATE SET class = EXCLUDED.class")
			if _, err := tx.ExecContext(r.Context(), sb.String(), custArgs...); err != nil {
				_ = tx.Rollback()
				logger.Error("batch insert customers", "error", err)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}

			var whSb strings.Builder
			whSb.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")
			custCount := end - start
			whArgs := make([]any, 0, custCount*len(paths)*4)
			row := 0
			for i := start; i < end; i++ {
				cid := fmt.Sprintf("%s-%04d", cmd.CustomerBase, i+1)
				for _, path := range paths {
					if row > 0 {
						whSb.WriteString(", ")
					}
					base := row * 4
					_, _ = fmt.Fprintf(&whSb, "($%d, $%d, $%d, $%d, true)", base+1, base+2, base+3, base+4)
					whArgs = append(whArgs, cid, cmd.ReceiverURL+path, "", fmt.Sprintf("{%s}", "subscriber.created"))
					row++
				}
			}
			whSb.WriteString(" ON CONFLICT (customer_id, url) DO UPDATE SET active = true, event_types = EXCLUDED.event_types")
			if _, err := tx.ExecContext(r.Context(), whSb.String(), whArgs...); err != nil {
				_ = tx.Rollback()
				logger.Error("batch insert webhooks", "error", err)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}

			if err := tx.Commit(); err != nil {
				_ = tx.Rollback()
				logger.Error("commit setup batch", "error", err)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			totalOK += custCount
		}

		logger.Info("setup-customers complete", "base", cmd.CustomerBase, "count", totalOK, "class", cmd.Class)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]int{"created": totalOK})
	})

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

func upsertCustomerTx(ctx context.Context, tx *sql.Tx, id string, class uint32, baseURL string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO customers (id, class)
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET class = EXCLUDED.class
	`, id, class)
	if err != nil {
		return err
	}

	paths := []string{"/hooks/fast", "/hooks/slow", "/hooks/flaky"}
	for _, path := range paths {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active)
			VALUES ($1, $2, '', ARRAY['subscriber.created', 'order.created', 'payment.received'], true)
			ON CONFLICT (customer_id, url) DO UPDATE
			SET active = true,
			    event_types = EXCLUDED.event_types
		`, id, baseURL+path)
		if err != nil {
			return err
		}
	}
	return nil
}
