package outbox_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/worker"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestIngestClaimDeliverHappyPath(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	var got struct {
		eventID    string
		deliveryID string
		body       string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		got.eventID = r.Header.Get("X-Event-Id")
		got.deliveryID = r.Header.Get("X-Delivery-Id")
		got.body = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_1", srv.URL, true, "subscriber.created")
	insertWebhook(t, ctx, db, "cus_1", srv.URL+"/inactive", false, "subscriber.created")
	insertWebhook(t, ctx, db, "cus_1", srv.URL+"/wrong-type", true, "subscriber.deleted")

	submitted, err := ingest.New(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_1",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_123"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(submitted.DeliveryIDs) != 1 {
		t.Fatalf("delivery count: got %d, want 1", len(submitted.DeliveryIDs))
	}

	claimed, ok, err := outbox.New(db).ClaimNext(ctx, "cus_1")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: got no delivery, want one")
	}
	if claimed.Status != outbox.StatusInFlight {
		t.Fatalf("claimed status: got %q, want %q", claimed.Status, outbox.StatusInFlight)
	}
	if claimed.AttemptNumber != 1 {
		t.Fatalf("attempt number: got %d, want 1", claimed.AttemptNumber)
	}

	started := time.Now()
	statusCode, err := worker.New().Deliver(ctx, worker.Delivery{
		EventID:    claimed.EventID,
		DeliveryID: claimed.ID,
		CustomerID: claimed.CustomerID,
		URL:        claimed.URL,
		Body:       claimed.Payload,
		EventType:  claimed.EventType,
	})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if statusCode != http.StatusOK {
		t.Fatalf("status code: got %d, want 200", statusCode)
	}
	if err := outbox.New(db).MarkSucceeded(ctx, claimed.ID, statusCode, time.Since(started)); err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}

	if got.eventID != submitted.EventID {
		t.Errorf("X-Event-Id: got %q, want %q", got.eventID, submitted.EventID)
	}
	if got.deliveryID != claimed.ID {
		t.Errorf("X-Delivery-Id: got %q, want %q", got.deliveryID, claimed.ID)
	}
	var gotBody map[string]string
	if err := json.Unmarshal([]byte(got.body), &gotBody); err != nil {
		t.Fatalf("unmarshal received body: %v", err)
	}
	if gotBody["subscriber_id"] != "sub_123" {
		t.Errorf("body subscriber_id: got %q, want sub_123", gotBody["subscriber_id"])
	}

	assertDeliveryStatus(t, ctx, db, claimed.ID, string(outbox.StatusSucceeded))
	assertAttemptCount(t, ctx, db, claimed.ID, 1)
}

func TestClaimNextConcurrentWorkersDoNotDoubleClaim(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const deliveryCount = 50
	const workerCount = 12

	for i := 0; i < deliveryCount; i++ {
		insertWebhook(t, ctx, db, "cus_race", "https://partner.example/webhook", true, "subscriber.created")
	}
	submitted, err := ingest.New(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_race",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_race"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(submitted.DeliveryIDs) != deliveryCount {
		t.Fatalf("delivery count: got %d, want %d", len(submitted.DeliveryIDs), deliveryCount)
	}

	store := outbox.New(db)
	claimed := make(map[string]int, deliveryCount)
	var claimedMu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, workerCount)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				delivery, ok, err := store.ClaimNext(ctx, "cus_race")
				if err != nil {
					errs <- err
					return
				}
				if !ok {
					return
				}

				claimedMu.Lock()
				claimed[delivery.ID]++
				claimedMu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("ClaimNext: %v", err)
	}

	if len(claimed) != deliveryCount {
		t.Fatalf("claimed unique deliveries: got %d, want %d", len(claimed), deliveryCount)
	}

	var duplicates []string
	for id, count := range claimed {
		if count > 1 {
			duplicates = append(duplicates, id)
		}
	}
	sort.Strings(duplicates)
	if len(duplicates) > 0 {
		t.Fatalf("double-claimed deliveries: %v", duplicates)
	}

	assertStatusCount(t, ctx, db, string(outbox.StatusInFlight), deliveryCount)
	assertStatusCount(t, ctx, db, string(outbox.StatusPending), 0)
}

func TestClaimNextRecoversStaleInFlightDelivery(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	insertWebhook(t, ctx, db, "cus_recover", "https://partner.example/webhook", true, "subscriber.created")
	submitted, err := ingest.New(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_recover",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_recover"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(submitted.DeliveryIDs) != 1 {
		t.Fatalf("delivery count: got %d, want 1", len(submitted.DeliveryIDs))
	}

	store := outbox.New(db, outbox.WithClaimLease(time.Hour))
	first, ok, err := store.ClaimNext(ctx, "cus_recover")
	if err != nil {
		t.Fatalf("first ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("first ClaimNext: got no delivery, want one")
	}

	second, ok, err := store.ClaimNext(ctx, "cus_recover")
	if err != nil {
		t.Fatalf("second ClaimNext: %v", err)
	}
	if ok {
		t.Fatalf("second ClaimNext claimed fresh in-flight delivery %s", second.ID)
	}

	_, err = db.ExecContext(ctx, `
		UPDATE deliveries
		SET claimed_at = now() - interval '2 hours'
		WHERE id = $1
	`, first.ID)
	if err != nil {
		t.Fatalf("age in-flight claim: %v", err)
	}

	recovered, ok, err := store.ClaimNext(ctx, "cus_recover")
	if err != nil {
		t.Fatalf("recovery ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("recovery ClaimNext: got no delivery, want stale in-flight delivery")
	}
	if recovered.ID != first.ID {
		t.Fatalf("recovered delivery: got %s, want %s", recovered.ID, first.ID)
	}
	if recovered.AttemptNumber != 2 {
		t.Fatalf("recovered attempt number: got %d, want 2", recovered.AttemptNumber)
	}
}

func TestProcessRetryableFailureSchedulesRetry(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_retry", srv.URL, true, "subscriber.created")
	submitted, err := ingest.New(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_retry",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_retry"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	store := outbox.New(db)
	claimed, ok, err := store.ClaimNext(ctx, "cus_retry")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: got no delivery, want one")
	}

	w := worker.New()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: time.Millisecond, CapDelay: time.Millisecond})
	if err := w.Process(ctx, store, claimed); err != nil {
		t.Fatalf("Process: %v", err)
	}

	assertDeliveryStatus(t, ctx, db, submitted.DeliveryIDs[0], string(outbox.StatusFailedRetry))
	assertAttemptCount(t, ctx, db, submitted.DeliveryIDs[0], 1)
	assertLastError(t, ctx, db, submitted.DeliveryIDs[0], retry.ReasonRetryable5xx)
}

func TestProcessRetryableFailureUsesPreviousRetryDelay(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_backoff", srv.URL, true, "subscriber.created")
	submitted, err := ingest.New(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_backoff",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_backoff"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	store := outbox.New(db)
	w := worker.New()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: 10 * time.Millisecond, CapDelay: 250 * time.Millisecond})

	first, ok, err := store.ClaimNext(ctx, "cus_backoff")
	if err != nil {
		t.Fatalf("first ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("first ClaimNext: got no delivery, want one")
	}
	if err := w.Process(ctx, store, first); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	firstDelay := scheduledRetryDelay(t, ctx, db, submitted.DeliveryIDs[0])
	if firstDelay < 10*time.Millisecond {
		t.Fatalf("first retry delay: got %s, want at least 10ms", firstDelay)
	}

	second := waitForClaim(t, ctx, store, "cus_backoff", 2*time.Second)
	if second.PreviousDelay <= 0 {
		t.Fatalf("second previous delay: got %s, want positive", second.PreviousDelay)
	}
	if err := w.Process(ctx, store, second); err != nil {
		t.Fatalf("second Process: %v", err)
	}

	secondDelay := scheduledRetryDelay(t, ctx, db, submitted.DeliveryIDs[0])
	if secondDelay <= firstDelay {
		t.Fatalf("second retry delay: got %s, want greater than first delay %s", secondDelay, firstDelay)
	}
}

func TestProcessTerminalFailureWritesDeadLetter(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_dead", srv.URL, true, "subscriber.created")
	submitted, err := ingest.New(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_dead",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_dead"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	store := outbox.New(db)
	claimed, ok, err := store.ClaimNext(ctx, "cus_dead")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: got no delivery, want one")
	}
	if err := worker.New().Process(ctx, store, claimed); err != nil {
		t.Fatalf("Process: %v", err)
	}

	deliveryID := submitted.DeliveryIDs[0]
	assertDeliveryStatus(t, ctx, db, deliveryID, string(outbox.StatusDead))
	assertAttemptCount(t, ctx, db, deliveryID, 1)
	assertDeadLetter(t, ctx, db, deliveryID, retry.ReasonTerminal4xx, 1)
}

func TestProcessFlakyReceiverEventuallySucceedsAllDeliveries(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	var totalRequests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestNumber := totalRequests.Add(1)
		attemptNumber, _ := strconv.Atoi(r.Header.Get("X-Attempt-Number"))
		if attemptNumber == 1 && requestNumber%10 < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	const deliveryCount = 40
	for i := 0; i < deliveryCount; i++ {
		insertWebhook(t, ctx, db, "cus_flaky", srv.URL, true, "subscriber.created")
	}
	submitted, err := ingest.New(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_flaky",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_flaky"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(submitted.DeliveryIDs) != deliveryCount {
		t.Fatalf("delivery count: got %d, want %d", len(submitted.DeliveryIDs), deliveryCount)
	}

	store := outbox.New(db)
	w := worker.New()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: time.Millisecond, CapDelay: time.Millisecond})

	deadline := time.Now().Add(5 * time.Second)
	for succeededCount(t, ctx, db) < deliveryCount {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for all deliveries to succeed")
		}

		claimed, ok, err := store.ClaimNext(ctx, "cus_flaky")
		if err != nil {
			t.Fatalf("ClaimNext: %v", err)
		}
		if !ok {
			time.Sleep(time.Millisecond)
			continue
		}
		if err := w.Process(ctx, store, claimed); err != nil {
			t.Fatalf("Process delivery %s: %v", claimed.ID, err)
		}
	}

	assertStatusCount(t, ctx, db, string(outbox.StatusSucceeded), deliveryCount)
	assertStatusCount(t, ctx, db, string(outbox.StatusDead), 0)
	if totalRequests.Load() <= deliveryCount {
		t.Fatalf("receiver request count: got %d, want retries above %d", totalRequests.Load(), deliveryCount)
	}
}

func TestStoreValidatesRequiredIDs(t *testing.T) {
	t.Parallel()

	store := outbox.New(nil)
	tests := []struct {
		name string
		run  func(context.Context) error
	}{
		{
			name: "claim requires customer ID",
			run: func(ctx context.Context) error {
				_, _, err := store.ClaimNext(ctx, "")
				return err
			},
		},
		{
			name: "mark succeeded requires delivery ID",
			run: func(ctx context.Context) error {
				return store.MarkSucceeded(ctx, "", http.StatusOK, time.Millisecond)
			},
		},
		{
			name: "mark retry requires delivery ID",
			run: func(ctx context.Context) error {
				return store.MarkRetry(ctx, "", http.StatusInternalServerError, retry.ReasonRetryable5xx, time.Millisecond, time.Millisecond)
			},
		},
		{
			name: "mark dead requires delivery ID",
			run: func(ctx context.Context) error {
				return store.MarkDead(ctx, "", http.StatusNotFound, retry.ReasonTerminal4xx, retry.ReasonTerminal4xx, time.Millisecond)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := tt.run(context.Background()); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func newPostgresDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()

	ctr, err := postgres.RunContainer(
		ctx,
		testcontainers.WithImage("postgres:16-alpine"),
		postgres.WithDatabase("notifier"),
		postgres.WithUsername("notifier"),
		postgres.WithPassword("notifier"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Errorf("terminate postgres container: %v", err)
		}
	})

	connString, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("postgres", connString)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close postgres: %v", err)
		}
	})

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	applyMigrations(t, ctx, db)

	return db
}

func applyMigrations(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	files, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("find migrations: %v", err)
	}
	sort.Strings(files)
	for _, file := range files {
		sqlBytes, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read migration %s: %v", file, err)
		}
		if _, err := db.ExecContext(ctx, string(sqlBytes)); err != nil {
			t.Fatalf("apply migration %s: %v", file, err)
		}
	}
}

func insertWebhook(t *testing.T, ctx context.Context, db *sql.DB, customerID, url string, active bool, eventTypes ...string) {
	t.Helper()

	if len(eventTypes) == 0 {
		t.Fatal("insertWebhook requires at least one event type")
	}

	_, err := db.ExecContext(ctx, `
		INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active)
		VALUES ($1, $2, 'test-secret', $3, $4)
	`, customerID, url, pq.Array(eventTypes), active)
	if err != nil {
		t.Fatalf("insert webhook: %v", err)
	}
}

func assertDeliveryStatus(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, want string) {
	t.Helper()

	var got string
	err := db.QueryRowContext(ctx, `SELECT status FROM deliveries WHERE id = $1`, deliveryID).Scan(&got)
	if err != nil {
		t.Fatalf("select delivery status: %v", err)
	}
	if got != want {
		t.Fatalf("delivery status: got %q, want %q", got, want)
	}
}

func assertAttemptCount(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string, want int) {
	t.Helper()

	var got int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_attempts WHERE delivery_id = $1`, deliveryID).Scan(&got)
	if err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if got != want {
		t.Fatalf("attempt count: got %d, want %d", got, want)
	}
}

func assertStatusCount(t *testing.T, ctx context.Context, db *sql.DB, status string, want int) {
	t.Helper()

	var got int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM deliveries WHERE status = $1`, status).Scan(&got)
	if err != nil {
		t.Fatalf("count status %s: %v", status, err)
	}
	if got != want {
		t.Fatalf("status %s count: got %d, want %d", status, got, want)
	}
}

func assertLastError(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, want string) {
	t.Helper()

	var got sql.NullString
	err := db.QueryRowContext(ctx, `SELECT last_error FROM deliveries WHERE id = $1`, deliveryID).Scan(&got)
	if err != nil {
		t.Fatalf("select last_error: %v", err)
	}
	if !got.Valid || got.String != want {
		t.Fatalf("last_error: got %q valid=%v, want %q", got.String, got.Valid, want)
	}
}

func assertDeadLetter(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, wantReason string, wantAttempts int) {
	t.Helper()

	var gotReason string
	var gotAttempts int
	err := db.QueryRowContext(ctx, `
		SELECT reason, attempts
		FROM dead_letters
		WHERE delivery_id = $1
	`, deliveryID).Scan(&gotReason, &gotAttempts)
	if err != nil {
		t.Fatalf("select dead letter: %v", err)
	}
	if gotReason != wantReason {
		t.Fatalf("dead letter reason: got %q, want %q", gotReason, wantReason)
	}
	if gotAttempts != wantAttempts {
		t.Fatalf("dead letter attempts: got %d, want %d", gotAttempts, wantAttempts)
	}
}

func scheduledRetryDelay(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string) time.Duration {
	t.Helper()

	var delayMillis int64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(
		    GREATEST(EXTRACT(EPOCH FROM (deliveries.next_attempt_at - delivery_attempts.sent_at)) * 1000, 0)::bigint,
		    0
		)
		FROM deliveries
		JOIN delivery_attempts ON delivery_attempts.delivery_id = deliveries.id
		WHERE deliveries.id = $1
		ORDER BY delivery_attempts.attempt_number DESC
		LIMIT 1
	`, deliveryID).Scan(&delayMillis)
	if err != nil {
		t.Fatalf("select scheduled retry delay: %v", err)
	}

	return time.Duration(delayMillis) * time.Millisecond
}

func waitForClaim(t *testing.T, ctx context.Context, store *outbox.Store, customerID string, timeout time.Duration) outbox.Delivery {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		delivery, ok, err := store.ClaimNext(ctx, customerID)
		if err != nil {
			t.Fatalf("ClaimNext: %v", err)
		}
		if ok {
			return delivery
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("timed out waiting to claim delivery for %s", customerID)
	return outbox.Delivery{}
}

func succeededCount(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()

	var got int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM deliveries WHERE status = 'succeeded'`).Scan(&got)
	if err != nil {
		t.Fatalf("count succeeded: %v", err)
	}
	return got
}
