package outbox_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/testutil/retention"
	"github.com/lvluu/webhook-notifier/internal/worker"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type integrationStubScheduler struct{}

func (integrationStubScheduler) Pick(_ context.Context) (scheduler.CustomerID, uint32, bool, error) {
	return "", 0, false, nil
}
func (integrationStubScheduler) Notify(scheduler.CustomerID) {}
func (integrationStubScheduler) Remove(scheduler.CustomerID) {}

func newIntegrationWorker(opts ...worker.Option) *worker.Worker {
	return worker.New(integrationStubScheduler{}, opts...)
}

func newIngestStore(db *sql.DB) *ingest.Store {
	return ingest.New(db, registry.New(db), retention.NoopNotifier{})
}

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

	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
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
	statusCode, err := newIntegrationWorker().Deliver(ctx, worker.Delivery{
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
		insertWebhook(t, ctx, db, "cus_race", fmt.Sprintf("https://partner-%d.example/webhook", i), true, "subscriber.created")
	}
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
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

	var claimCount int
	err = db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_claims`).Scan(&claimCount)
	if err != nil {
		t.Fatalf("count delivery_claims: %v", err)
	}
	if claimCount != deliveryCount {
		t.Fatalf("delivery_claims count: got %d, want %d", claimCount, deliveryCount)
	}
}

func TestClaimNextBatchDeduplicatesInputCustomers(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	deliveryID := submitOneDelivery(t, ctx, db, "cus_duplicate_batch")
	store := outbox.New(db)

	deliveries, err := store.ClaimNextBatch(ctx, []string{
		"cus_duplicate_batch",
		"cus_duplicate_batch",
		"cus_duplicate_batch",
	}, 4)
	if err != nil {
		t.Fatalf("ClaimNextBatch: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("claimed deliveries: got %d, want 1", len(deliveries))
	}
	if deliveries[0].ID != deliveryID {
		t.Fatalf("claimed delivery ID: got %s, want %s", deliveries[0].ID, deliveryID)
	}

	var claimCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_claims WHERE delivery_id = $1`, deliveryID).Scan(&claimCount); err != nil {
		t.Fatalf("count delivery claims: %v", err)
	}
	if claimCount != 1 {
		t.Fatalf("delivery claim count: got %d, want 1", claimCount)
	}
}

// TestClaimNextBatchPerCustomerLimitPreventsWhaleMonopoly proves that a single
// high-volume Customer cannot fill an entire multi-customer claim batch and
// starve a co-claimed sparse Customer. The whale's Deliveries are submitted
// first, so they sort older than the small Customer's single Delivery on the
// (next_attempt_at, created_at, id) claim ordering. Without a per-Customer cap
// on the candidate pool, the whale's oldest rows would consume the whole batch
// limit at the final ORDER BY ... LIMIT cut and the small Customer would claim
// nothing. With the per-Customer cap the whale contributes at most its fair
// share, leaving room for the small Customer in the same batch.
func TestClaimNextBatchPerCustomerLimitPreventsWhaleMonopoly(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const whaleDeliveries = 8
	const batchLimit = 4

	whaleFirst := submitOneDelivery(t, ctx, db, "cus_whale")
	for range whaleDeliveries - 1 {
		if _, err := appendDeliveryForExistingWebhook(t, ctx, db, whaleFirst); err != nil {
			t.Fatalf("append whale delivery: %v", err)
		}
	}

	smallID := submitOneDelivery(t, ctx, db, "cus_small")

	store := outbox.New(db)
	deliveries, err := store.ClaimNextBatch(ctx, []string{"cus_whale", "cus_small"}, batchLimit)
	if err != nil {
		t.Fatalf("ClaimNextBatch: %v", err)
	}

	whaleClaimed := 0
	smallClaimed := false
	for _, d := range deliveries {
		switch d.CustomerID {
		case "cus_whale":
			whaleClaimed++
		case "cus_small":
			smallClaimed = true
			if d.ID != smallID {
				t.Fatalf("small delivery ID: got %s, want %s", d.ID, smallID)
			}
		default:
			t.Fatalf("unexpected customer in batch: %s", d.CustomerID)
		}
	}

	if !smallClaimed {
		t.Fatalf("small Customer was starved: claimed %d deliveries, all from whale; want the small Customer's delivery included", len(deliveries))
	}

	// Fair share with 2 input customers and a batch limit of 4 is 2 per customer.
	if maxWhalePerBatch := batchLimit / 2; whaleClaimed > maxWhalePerBatch {
		t.Fatalf("whale claimed %d in one batch; per-customer cap should hold it to %d", whaleClaimed, maxWhalePerBatch)
	}
}

// TestClaimNextBatchAndRequeueConcurrentNoDeadlock stresses the claim and
// requeue paths together to guard against lock-order inversion. Claiming locks
// deliveries (FOR UPDATE SKIP LOCKED) before inserting delivery_claims; requeue
// goes through withInFlightDelivery, which must acquire the same
// deliveries-then-claims order. With the breaker enabled the claim path also
// touches webhooks. If any of these acquire row locks in a conflicting order,
// Postgres aborts a transaction with SQLSTATE 40P01 (deadlock detected). The
// test fails on any deadlock error.
func TestClaimNextBatchAndRequeueConcurrentNoDeadlock(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const customerCount = 4
	const deliveriesPerCustomer = 6

	customerIDs := make([]string, 0, customerCount)
	for c := range customerCount {
		customerID := fmt.Sprintf("cus_deadlock_%d", c)
		customerIDs = append(customerIDs, customerID)
		first := submitOneDelivery(t, ctx, db, customerID)
		for range deliveriesPerCustomer - 1 {
			if _, err := appendDeliveryForExistingWebhook(t, ctx, db, first); err != nil {
				t.Fatalf("append delivery for %s: %v", customerID, err)
			}
		}
	}

	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 3,
		InitialCooldown:  time.Second,
		MaxCooldown:      time.Second,
	}), outbox.WithClaimLease(50*time.Millisecond))

	deadline := time.Now().Add(2 * time.Second)
	var deadlocks atomic.Int64
	var wg sync.WaitGroup

	assertNoDeadlock := func(label string, err error) {
		if err == nil {
			return
		}
		if strings.Contains(err.Error(), "40P01") || strings.Contains(err.Error(), "deadlock detected") {
			deadlocks.Add(1)
			t.Errorf("%s: deadlock: %v", label, err)
		}
	}

	// Claimers race to claim the whole set repeatedly.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				deliveries, err := store.ClaimNextBatch(ctx, customerIDs, customerCount*deliveriesPerCustomer)
				assertNoDeadlock("claim", err)
				// Requeue everything claimed so the rows churn back into the pool.
				for _, d := range deliveries {
					assertNoDeadlock("requeue", store.Requeue(ctx, d.ID, 0))
				}
			}
		}()
	}

	wg.Wait()

	if n := deadlocks.Load(); n > 0 {
		t.Fatalf("observed %d deadlock(s) across concurrent claim/requeue", n)
	}
}

func TestClaimNextRecoversStaleInFlightDelivery(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	insertWebhook(t, ctx, db, "cus_recover", "https://partner.example/webhook", true, "subscriber.created")
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
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
		UPDATE delivery_claims
		SET claimed_at = now() - interval '2 hours'
		WHERE delivery_id = $1
	`, first.ID)
	if err != nil {
		t.Fatalf("age claim: %v", err)
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
	if recovered.AttemptNumber != 1 {
		t.Fatalf("recovered attempt number: got %d, want 1 (no POST was made on the first claim, so attempt_number resets to 1)", recovered.AttemptNumber)
	}
}

func TestDiscoverClaimableCustomersMatchesClaimEligibility(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	store := outbox.New(db, outbox.WithClaimLease(time.Hour))

	submitOneDelivery(t, ctx, db, "cus_pending")
	claimableRetry := submitOneDelivery(t, ctx, db, "cus_retry_due")
	futureRetry := submitOneDelivery(t, ctx, db, "cus_retry_future")
	staleInFlight := submitOneDelivery(t, ctx, db, "cus_stale_inflight")
	freshInFlight := submitOneDelivery(t, ctx, db, "cus_fresh_inflight")

	_, err := db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'failed_retry', next_attempt_at = now() - interval '1 second'
		WHERE id = $1
	`, claimableRetry)
	if err != nil {
		t.Fatalf("make retry due: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'failed_retry', next_attempt_at = now() + interval '1 hour'
		WHERE id = $1
	`, futureRetry)
	if err != nil {
		t.Fatalf("make retry future: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO delivery_claims (delivery_id, attempt_number, claimed_at)
		VALUES ($1, 1, now() - interval '2 hours')
	`, staleInFlight)
	if err != nil {
		t.Fatalf("make stale claim: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO delivery_claims (delivery_id, attempt_number, claimed_at)
		VALUES ($1, 1, now())
	`, freshInFlight)
	if err != nil {
		t.Fatalf("make fresh claim: %v", err)
	}

	got, err := store.DiscoverClaimableCustomers(ctx, "", 10)
	if err != nil {
		t.Fatalf("DiscoverClaimableCustomers: %v", err)
	}

	want := []string{"cus_pending", "cus_retry_due", "cus_stale_inflight"}
	if !slices.Equal(got, want) {
		t.Fatalf("claimable customers: got %v, want %v", got, want)
	}

	afterPending, err := store.DiscoverClaimableCustomers(ctx, "cus_pending", 10)
	if err != nil {
		t.Fatalf("DiscoverClaimableCustomers after cursor: %v", err)
	}
	wantAfterPending := []string{"cus_retry_due", "cus_stale_inflight"}
	if !slices.Equal(afterPending, wantAfterPending) {
		t.Fatalf("claimable customers after cursor: got %v, want %v", afterPending, wantAfterPending)
	}

	limited, err := store.DiscoverClaimableCustomers(ctx, "", 2)
	if err != nil {
		t.Fatalf("DiscoverClaimableCustomers limited: %v", err)
	}
	wantLimited := []string{"cus_pending", "cus_retry_due"}
	if !slices.Equal(limited, wantLimited) {
		t.Fatalf("limited claimable customers: got %v, want %v", limited, wantLimited)
	}

	if _, err := store.DiscoverClaimableCustomers(ctx, "", 0); err == nil {
		t.Fatal("DiscoverClaimableCustomers limit 0: got nil error, want error")
	}
}

func TestHasClaimableWorkMatchesClaimEligibility(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db, outbox.WithClaimLease(time.Hour))

	futureRetry := submitOneDelivery(t, ctx, db, "cus_future_retry")
	freshInFlight := submitOneDelivery(t, ctx, db, "cus_fresh_inflight")
	dueRetry := submitOneDelivery(t, ctx, db, "cus_due_retry")

	_, err := db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'failed_retry', next_attempt_at = now() + interval '1 hour'
		WHERE id = $1
	`, futureRetry)
	if err != nil {
		t.Fatalf("set future retry: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO delivery_claims (delivery_id, attempt_number, claimed_at)
		VALUES ($1, 1, now())
	`, freshInFlight)
	if err != nil {
		t.Fatalf("set fresh claim: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'failed_retry', next_attempt_at = now() - interval '1 second'
		WHERE id = $1
	`, dueRetry)
	if err != nil {
		t.Fatalf("set due retry: %v", err)
	}

	tests := []struct {
		customerID string
		want       bool
	}{
		{customerID: "cus_future_retry", want: false},
		{customerID: "cus_fresh_inflight", want: false},
		{customerID: "cus_due_retry", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.customerID, func(t *testing.T) {
			got, err := store.HasClaimableWork(ctx, tt.customerID)
			if err != nil {
				t.Fatalf("HasClaimableWork: %v", err)
			}
			if got != tt.want {
				t.Fatalf("HasClaimableWork(%s): got %v, want %v", tt.customerID, got, tt.want)
			}
		})
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
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
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

	w := newIntegrationWorker()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: time.Millisecond, CapDelay: time.Millisecond})
	if result := w.Process(ctx, store, claimed); result.Err != nil {
		t.Fatalf("Process: %v", result.Err)
	}

	assertDeliveryStatus(t, ctx, db, submitted.DeliveryIDs[0], string(outbox.StatusFailedRetry))
	assertAttemptCount(t, ctx, db, submitted.DeliveryIDs[0], 1)
	assertLastError(t, ctx, db, submitted.DeliveryIDs[0], retry.ReasonRetryable5xx)
}

func TestProcessRetryableFailuresOpenWebhookBreaker(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_breaker", srv.URL, true, "subscriber.created")
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_breaker",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_breaker"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 2,
		InitialCooldown:  time.Hour,
		MaxCooldown:      time.Hour,
	}))
	w := newIntegrationWorker()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: time.Millisecond, CapDelay: time.Millisecond})
	for i := 0; i < 2; i++ {
		claimed := waitForClaim(t, ctx, store, "cus_breaker", time.Second)
		if result := w.Process(ctx, store, claimed); result.Err != nil {
			t.Fatalf("Process %d: %v", i+1, result.Err)
		}
	}

	assertWebhookBreaker(t, ctx, db, submitted.DeliveryIDs[0], "open", 2)
}

func TestBatchRetryOpensBreakerWithMultipleDeliveries(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	deliveryID := submitOneDelivery(t, ctx, db, "cus_batch_breaker")
	delivery2, err := appendDeliveryForExistingWebhook(t, ctx, db, deliveryID)
	if err != nil {
		t.Fatalf("append delivery 2: %v", err)
	}
	delivery3, err := appendDeliveryForExistingWebhook(t, ctx, db, deliveryID)
	if err != nil {
		t.Fatalf("append delivery 3: %v", err)
	}
	delivery4, err := appendDeliveryForExistingWebhook(t, ctx, db, deliveryID)
	if err != nil {
		t.Fatalf("append delivery 4: %v", err)
	}

	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 3,
		InitialCooldown:  time.Hour,
		MaxCooldown:      time.Hour,
	}))

	_ = claimDelivery(t, ctx, store, "cus_batch_breaker", deliveryID)
	_ = claimDelivery(t, ctx, store, "cus_batch_breaker", delivery2)
	_ = claimDelivery(t, ctx, store, "cus_batch_breaker", delivery3)
	_ = claimDelivery(t, ctx, store, "cus_batch_breaker", delivery4)

	err = store.MarkRetryBatch(ctx, []outbox.BatchMarkItem{
		{DeliveryID: deliveryID, StatusCode: 500, ErrorClass: retry.ReasonRetryable5xx, Delay: time.Millisecond},
		{DeliveryID: delivery2, StatusCode: 500, ErrorClass: retry.ReasonRetryable5xx, Delay: time.Millisecond},
		{DeliveryID: delivery3, StatusCode: 500, ErrorClass: retry.ReasonRetryable5xx, Delay: time.Millisecond},
		{DeliveryID: delivery4, StatusCode: 500, ErrorClass: retry.ReasonRetryable5xx, Delay: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("MarkRetryBatch: %v", err)
	}

	assertWebhookBreaker(t, ctx, db, deliveryID, "open", 4)
}

func claimDelivery(t *testing.T, ctx context.Context, store *outbox.Store, customerID, deliveryID string) outbox.Delivery {
	t.Helper()

	claimed, ok, err := store.ClaimNext(ctx, customerID)
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatalf("ClaimNext: no delivery available")
	}
	if claimed.ID != deliveryID {
		t.Fatalf("ClaimNext: got delivery %s, want %s", claimed.ID, deliveryID)
	}
	return claimed
}

func TestProcessRetryableFailuresCanDisableWebhookBreaker(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_breaker_disabled", srv.URL, true, "subscriber.created")
	_, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_breaker_disabled",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_breaker_disabled"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 0,
	}))
	w := newIntegrationWorker()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: time.Millisecond, CapDelay: time.Millisecond})

	claimed := waitForClaim(t, ctx, store, "cus_breaker_disabled", time.Second)
	if result := w.Process(ctx, store, claimed); result.Err != nil {
		t.Fatalf("Process: %v", result.Err)
	}

	assertWebhookBreaker(t, ctx, db, claimed.ID, "closed", 0)
}

func TestClaimNextSkipsOpenWebhookUntilHalfOpenProbe(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	first := submitOneDelivery(t, ctx, db, "cus_open_breaker")
	second, err := appendDeliveryForExistingWebhook(t, ctx, db, first)
	if err != nil {
		t.Fatalf("append delivery: %v", err)
	}
	store := outbox.New(db)

	_, err = db.ExecContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'open',
		    breaker_opened_at = now(),
		    breaker_next_probe_at = now() + interval '1 hour',
		    breaker_failure_count = 5
		WHERE id = (SELECT webhook_id FROM deliveries WHERE id = $1)
	`, first)
	if err != nil {
		t.Fatalf("open breaker: %v", err)
	}

	if got, err := store.DiscoverClaimableCustomers(ctx, "", 10); err != nil {
		t.Fatalf("DiscoverClaimableCustomers: %v", err)
	} else if len(got) != 0 {
		t.Fatalf("claimable customers with open breaker: got %v, want none", got)
	}
	if claimed, ok, err := store.ClaimNext(ctx, "cus_open_breaker"); err != nil {
		t.Fatalf("ClaimNext while open: %v", err)
	} else if ok {
		t.Fatalf("ClaimNext while open claimed %s", claimed.ID)
	}

	_, err = db.ExecContext(ctx, `
		UPDATE webhooks
		SET breaker_next_probe_at = now() - interval '1 second'
		WHERE id = (SELECT webhook_id FROM deliveries WHERE id = $1)
	`, first)
	if err != nil {
		t.Fatalf("expire breaker cooldown: %v", err)
	}

	claimed, ok, err := store.ClaimNext(ctx, "cus_open_breaker")
	if err != nil {
		t.Fatalf("ClaimNext half-open: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext half-open: got no delivery, want probe")
	}
	if claimed.ID != first {
		t.Fatalf("half-open probe delivery: got %s, want %s", claimed.ID, first)
	}
	assertWebhookProbe(t, ctx, db, first, "half_open", first)

	if claimed, ok, err := store.ClaimNext(ctx, "cus_open_breaker"); err != nil {
		t.Fatalf("ClaimNext with probe in flight: %v", err)
	} else if ok {
		t.Fatalf("ClaimNext with probe in flight claimed %s, want no second probe; other pending delivery is %s", claimed.ID, second)
	}
}

func TestHalfOpenProbeSuccessClosesWebhookBreaker(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	deliveryID := submitOneDeliveryToURL(t, ctx, db, "cus_probe_success", srv.URL)
	store := outbox.New(db)
	makeWebhookHalfOpen(t, ctx, db, deliveryID)

	claimed, ok, err := store.ClaimNext(ctx, "cus_probe_success")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: got no delivery, want probe")
	}
	if result := newIntegrationWorker().Process(ctx, store, claimed); result.Err != nil {
		t.Fatalf("Process: %v", result.Err)
	}

	assertWebhookBreaker(t, ctx, db, deliveryID, "closed", 0)
}

func TestHalfOpenNonProbeSuccessDoesNotCloseWebhookBreaker(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	probeDeliveryID := submitOneDelivery(t, ctx, db, "cus_probe_identity")
	otherDeliveryID, err := appendDeliveryForExistingWebhook(t, ctx, db, probeDeliveryID)
	if err != nil {
		t.Fatalf("append delivery: %v", err)
	}
	makeWebhookHalfOpen(t, ctx, db, probeDeliveryID)
	_, err = db.ExecContext(ctx, `
		INSERT INTO delivery_claims (delivery_id, attempt_number, claimed_at)
		VALUES ($1, 1, now())
	`, otherDeliveryID)
	if err != nil {
		t.Fatalf("force non-probe in-flight: %v", err)
	}

	if err := outbox.New(db).MarkSucceeded(ctx, otherDeliveryID, http.StatusOK, time.Millisecond); err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}

	assertDeliveryStatus(t, ctx, db, otherDeliveryID, string(outbox.StatusSucceeded))
	assertWebhookProbe(t, ctx, db, probeDeliveryID, "half_open", probeDeliveryID)
}

func TestHalfOpenProbeFailureReopensWebhookBreaker(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	deliveryID := submitOneDeliveryToURL(t, ctx, db, "cus_probe_failure", srv.URL)
	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 2,
		InitialCooldown:  time.Hour,
		MaxCooldown:      2 * time.Hour,
	}))
	makeWebhookHalfOpen(t, ctx, db, deliveryID)

	claimed, ok, err := store.ClaimNext(ctx, "cus_probe_failure")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: got no delivery, want probe")
	}
	w := newIntegrationWorker()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: time.Millisecond, CapDelay: time.Millisecond})
	if result := w.Process(ctx, store, claimed); result.Err != nil {
		t.Fatalf("Process: %v", result.Err)
	}

	assertWebhookBreaker(t, ctx, db, deliveryID, "open", 4)
	assertWebhookNextProbeAfter(t, ctx, db, deliveryID, time.Now().Add(90*time.Minute))
}

func TestProcessRetryableFailureUsesPreviousRetryDelay(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_backoff", srv.URL, true, "subscriber.created")
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_backoff",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_backoff"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	store := outbox.New(db)
	w := newIntegrationWorker()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: 10 * time.Millisecond, CapDelay: 250 * time.Millisecond})

	first, ok, err := store.ClaimNext(ctx, "cus_backoff")
	if err != nil {
		t.Fatalf("first ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("first ClaimNext: got no delivery, want one")
	}
	if result := w.Process(ctx, store, first); result.Err != nil {
		t.Fatalf("first Process: %v", result.Err)
	}
	firstDelay := scheduledRetryDelay(t, ctx, db, submitted.DeliveryIDs[0])
	if firstDelay < 10*time.Millisecond {
		t.Fatalf("first retry delay: got %s, want at least 10ms", firstDelay)
	}

	second := waitForClaim(t, ctx, store, "cus_backoff", 2*time.Second)
	if second.PreviousDelay <= 0 {
		t.Fatalf("second previous delay: got %s, want positive", second.PreviousDelay)
	}
	if !durationNear(second.PreviousDelay, firstDelay, time.Millisecond) {
		t.Fatalf("second previous delay: got %s, want previous scheduled delay %s", second.PreviousDelay, firstDelay)
	}
	expectedSecondDelay, terminal, reason := w.RetryPolicy.NextWithKey(
		second.AttemptNumber,
		second.PreviousDelay,
		&http.Response{StatusCode: http.StatusInternalServerError},
		nil,
		second.ID,
		time.Since(second.CreatedAt),
	)
	if terminal {
		t.Fatalf("second retry policy terminal: got true with reason %s, want retry", reason)
	}
	if result := w.Process(ctx, store, second); result.Err != nil {
		t.Fatalf("second Process: %v", result.Err)
	}

	secondDelay := scheduledRetryDelay(t, ctx, db, submitted.DeliveryIDs[0])
	expectedSecondDelay = time.Duration(expectedSecondDelay.Milliseconds()) * time.Millisecond
	if !durationNear(secondDelay, expectedSecondDelay, time.Millisecond) {
		t.Fatalf("second retry delay: got %s, want policy delay %s", secondDelay, expectedSecondDelay)
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
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
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
	if result := newIntegrationWorker().Process(ctx, store, claimed); result.Err != nil {
		t.Fatalf("Process: %v", result.Err)
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
		insertWebhook(t, ctx, db, "cus_flaky", fmt.Sprintf("%s/%d", srv.URL, i), true, "subscriber.created")
	}
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
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
	w := newIntegrationWorker()
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
		if result := w.Process(ctx, store, claimed); result.Err != nil {
			t.Fatalf("Process delivery %s: %v", claimed.ID, result.Err)
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
		{
			name: "archive requires positive retention",
			run: func(ctx context.Context) error {
				_, err := store.ArchiveTerminalDeliveries(ctx, -time.Millisecond, 10)
				return err
			},
		},
		{
			name: "archive rejects zero retention",
			run: func(ctx context.Context) error {
				_, err := store.ArchiveTerminalDeliveries(ctx, 0, 10)
				return err
			},
		},
		{
			name: "archive requires positive limit",
			run: func(ctx context.Context) error {
				_, err := store.ArchiveTerminalDeliveries(ctx, time.Hour, 0)
				return err
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

func TestArchiveTerminalDeliveriesMovesOldTerminalRowsToHistory(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	oldSucceededID := submitOneDeliveryToURL(t, ctx, db, "cus_history_success", srv.URL)
	claimedSucceeded, ok, err := store.ClaimNext(ctx, "cus_history_success")
	if err != nil {
		t.Fatalf("ClaimNext succeeded delivery: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext succeeded delivery: got no delivery, want one")
	}
	if err := store.MarkSucceeded(ctx, claimedSucceeded.ID, http.StatusOK, 10*time.Millisecond); err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}

	oldDeadID := submitOneDeliveryToURL(t, ctx, db, "cus_history_dead", srv.URL)
	claimedDead, ok, err := store.ClaimNext(ctx, "cus_history_dead")
	if err != nil {
		t.Fatalf("ClaimNext dead delivery: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext dead delivery: got no delivery, want one")
	}
	if err := store.MarkDead(ctx, claimedDead.ID, http.StatusNotFound, retry.ReasonTerminal4xx, retry.ReasonTerminal4xx, 10*time.Millisecond); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	recentID := submitOneDeliveryToURL(t, ctx, db, "cus_history_recent", srv.URL)
	claimedRecent, ok, err := store.ClaimNext(ctx, "cus_history_recent")
	if err != nil {
		t.Fatalf("ClaimNext recent delivery: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext recent delivery: got no delivery, want one")
	}
	if err := store.MarkSucceeded(ctx, claimedRecent.ID, http.StatusOK, 10*time.Millisecond); err != nil {
		t.Fatalf("MarkSucceeded recent: %v", err)
	}

	retention.AgeTerminalDelivery(t, ctx, db, oldSucceededID, 48*time.Hour)
	retention.AgeTerminalDelivery(t, ctx, db, oldDeadID, 48*time.Hour)

	archived, err := store.ArchiveTerminalDeliveries(ctx, 24*time.Hour, 10)
	if err != nil {
		t.Fatalf("ArchiveTerminalDeliveries: %v", err)
	}
	if archived != 2 {
		t.Fatalf("archived count: got %d, want 2", archived)
	}

	assertDeliveryMissing(t, ctx, db, oldSucceededID)
	assertDeliveryMissing(t, ctx, db, oldDeadID)
	assertAttemptCount(t, ctx, db, oldSucceededID, 0)
	assertAttemptCount(t, ctx, db, oldDeadID, 0)
	assertDeadLetterMissing(t, ctx, db, oldDeadID)
	assertDeliveryStatus(t, ctx, db, recentID, string(outbox.StatusSucceeded))

	assertDeliveryHistory(t, ctx, db, oldSucceededID, "succeeded", sql.NullString{}, 1)
	assertDeliveryHistory(t, ctx, db, oldDeadID, "dead", sql.NullString{String: retry.ReasonTerminal4xx, Valid: true}, 1)
}

func TestArchiveTerminalDeliveriesOrdersByUpdatedAt(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db)

	var oldIDs []string
	for i := 0; i < 3; i++ {
		customerID := fmt.Sprintf("cus_ord_old_%d", i)
		deliveryID := submitOneDelivery(t, ctx, db, customerID)
		claimed, ok, err := store.ClaimNext(ctx, customerID)
		if err != nil {
			t.Fatalf("ClaimNext old %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("ClaimNext old %d: no delivery", i)
		}
		if i < 2 {
			if err := store.MarkSucceeded(ctx, claimed.ID, http.StatusOK, 10*time.Millisecond); err != nil {
				t.Fatalf("MarkSucceeded old %d: %v", i, err)
			}
		} else {
			if err := store.MarkDead(ctx, claimed.ID, http.StatusNotFound, retry.ReasonTerminal4xx, retry.ReasonTerminal4xx, 10*time.Millisecond); err != nil {
				t.Fatalf("MarkDead old %d: %v", i, err)
			}
		}
		oldIDs = append(oldIDs, deliveryID)
		retention.AgeTerminalDelivery(t, ctx, db, deliveryID, 48*time.Hour)
	}

	var recentIDs []string
	for i := 0; i < 2; i++ {
		customerID := fmt.Sprintf("cus_ord_recent_%d", i)
		deliveryID := submitOneDelivery(t, ctx, db, customerID)
		claimed, ok, err := store.ClaimNext(ctx, customerID)
		if err != nil {
			t.Fatalf("ClaimNext recent %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("ClaimNext recent %d: no delivery", i)
		}
		if err := store.MarkSucceeded(ctx, claimed.ID, http.StatusOK, 10*time.Millisecond); err != nil {
			t.Fatalf("MarkSucceeded recent %d: %v", i, err)
		}
		recentIDs = append(recentIDs, deliveryID)
		retention.AgeTerminalDelivery(t, ctx, db, deliveryID, 1*time.Hour)
	}

	archived, err := store.ArchiveTerminalDeliveries(ctx, 24*time.Hour, 3)
	if err != nil {
		t.Fatalf("ArchiveTerminalDeliveries: %v", err)
	}
	if archived != 3 {
		t.Fatalf("archived count: got %d, want 3 (oldest 3 eligible rows)", archived)
	}

	for _, id := range oldIDs {
		assertDeliveryMissing(t, ctx, db, id)
	}
	for _, id := range recentIDs {
		assertDeliveryStatus(t, ctx, db, id, string(outbox.StatusSucceeded))
	}
}

func TestBatchMarkErrorsWhenDeliveryIsNotInFlight(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db)

	tests := []struct {
		name string
		run  func(outbox.BatchMarkItem) error
	}{
		{
			name: "succeeded",
			run: func(item outbox.BatchMarkItem) error {
				return store.MarkSucceededBatch(ctx, []outbox.BatchMarkItem{item})
			},
		},
		{
			name: "retry",
			run: func(item outbox.BatchMarkItem) error {
				item.ErrorClass = retry.ReasonRetryable5xx
				item.Delay = time.Millisecond
				return store.MarkRetryBatch(ctx, []outbox.BatchMarkItem{item})
			},
		},
		{
			name: "dead",
			run: func(item outbox.BatchMarkItem) error {
				item.ErrorClass = retry.ReasonTerminal4xx
				item.Reason = retry.ReasonTerminal4xx
				return store.MarkDeadBatch(ctx, []outbox.BatchMarkItem{item})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deliveryID := submitOneDelivery(t, ctx, db, "cus_batch_"+tt.name)
			claimed, ok, err := store.ClaimNext(ctx, "cus_batch_"+tt.name)
			if err != nil {
				t.Fatalf("ClaimNext: %v", err)
			}
			if !ok {
				t.Fatal("ClaimNext: got no delivery, want one")
			}
			if claimed.ID != deliveryID {
				t.Fatalf("claimed ID: got %s, want %s", claimed.ID, deliveryID)
			}
			if err := store.MarkSucceeded(ctx, deliveryID, http.StatusOK, time.Millisecond); err != nil {
				t.Fatalf("MarkSucceeded setup: %v", err)
			}

			err = tt.run(outbox.BatchMarkItem{
				DeliveryID: deliveryID,
				StatusCode: http.StatusOK,
				Duration:   time.Millisecond,
			})
			if err == nil {
				t.Fatal("expected batch mark error, got nil")
			}
			if !strings.Contains(err.Error(), "marked 0 of 1") &&
				!strings.Contains(err.Error(), "marked 0 attempts and 0 dead letters of 1") {
				t.Fatalf("error: got %q, want partial batch mark error", err.Error())
			}
		})
	}
}

// TestWorkerRun_SchedulerFairnessAcrossCustomers exercises the full worker
// loop (Scheduler → ClaimNext → Process → re-Notify) against a real Postgres
// outbox. A Whale Customer and a Small Customer each enqueue Deliveries; the
// test asserts the worker interleaves them fairly rather than draining the
// Whale first.
func TestWorkerRun_SchedulerFairnessAcrossCustomers(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Whale: 10 deliveries, Small: 10 deliveries.
	const deliveriesPerCustomer = 10
	for i := 0; i < deliveriesPerCustomer; i++ {
		insertWebhook(t, ctx, db, "whale", fmt.Sprintf("%s/whale/%d", srv.URL, i), true, "subscriber.created")
		insertWebhook(t, ctx, db, "small", fmt.Sprintf("%s/small/%d", srv.URL, i), true, "subscriber.created")
	}

	whaleSubmitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "whale",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"whale_sub"}`),
	})
	if err != nil {
		t.Fatalf("Submit whale: %v", err)
	}
	if len(whaleSubmitted.DeliveryIDs) != deliveriesPerCustomer {
		t.Fatalf("whale delivery count: got %d, want %d", len(whaleSubmitted.DeliveryIDs), deliveriesPerCustomer)
	}

	smallSubmitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "small",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"small_sub"}`),
	})
	if err != nil {
		t.Fatalf("Submit small: %v", err)
	}
	if len(smallSubmitted.DeliveryIDs) != deliveriesPerCustomer {
		t.Fatalf("small delivery count: got %d, want %d", len(smallSubmitted.DeliveryIDs), deliveriesPerCustomer)
	}

	store := outbox.New(db)
	sched := roundrobin.New()
	w := newIntegrationWorker()
	w.Scheduler = sched

	// Notify both Customers to seed the scheduler.
	sched.Notify("whale")
	sched.Notify("small")

	// Run worker in background until all deliveries succeed.
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(runCtx, store)
	}()

	// Poll until all deliveries succeed or timeout.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		whaleSucceeded := countSucceededForCustomer(t, ctx, db, "whale")
		smallSucceeded := countSucceededForCustomer(t, ctx, db, "small")
		if whaleSucceeded == deliveriesPerCustomer && smallSucceeded == deliveriesPerCustomer {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	<-errCh

	whaleSucceeded := countSucceededForCustomer(t, ctx, db, "whale")
	smallSucceeded := countSucceededForCustomer(t, ctx, db, "small")
	if whaleSucceeded != deliveriesPerCustomer {
		t.Errorf("whale succeeded: got %d, want %d", whaleSucceeded, deliveriesPerCustomer)
	}
	if smallSucceeded != deliveriesPerCustomer {
		t.Errorf("small succeeded: got %d, want %d", smallSucceeded, deliveriesPerCustomer)
	}
}

func TestWorkerRun_DBDiscoveryFeedsIndependentSchedulers(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const deliveriesPerCustomer = 5
	const totalDeliveries = deliveriesPerCustomer * 2

	var (
		seenMu sync.Mutex
		seen   = make(map[string]int, totalDeliveries)
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Delivery-Id") + ":" + r.Header.Get("X-Attempt-Number")
		seenMu.Lock()
		seen[key]++
		seenMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	for i := 0; i < deliveriesPerCustomer; i++ {
		insertWebhook(t, ctx, db, "cus_a", fmt.Sprintf("%s/cus_a/%d", srv.URL, i), true, "subscriber.created")
		insertWebhook(t, ctx, db, "cus_b", fmt.Sprintf("%s/cus_b/%d", srv.URL, i), true, "subscriber.created")
	}

	submit := func(customerID string) {
		t.Helper()
		submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
			CustomerID: customerID,
			Type:       "subscriber.created",
			Payload:    []byte(`{"subscriber_id":"sub_123"}`),
		})
		if err != nil {
			t.Fatalf("Submit %s: %v", customerID, err)
		}
		if len(submitted.DeliveryIDs) != deliveriesPerCustomer {
			t.Fatalf("%s delivery count: got %d, want %d", customerID, len(submitted.DeliveryIDs), deliveriesPerCustomer)
		}
	}
	submit("cus_a")
	submit("cus_b")

	store := outbox.New(db)
	schedulers := []scheduler.Scheduler{roundrobin.New(), roundrobin.New()}

	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	errCh := make(chan error, len(schedulers)*2)
	for _, sched := range schedulers {
		sched := sched
		go func() {
			errCh <- worker.RunClaimableCustomerDiscovery(runCtx, store, sched, worker.DiscoveryConfig{
				Interval: time.Millisecond,
				Limit:    1,
			})
		}()

		w := newIntegrationWorker()
		w.Scheduler = sched
		go func() {
			errCh <- w.Run(runCtx, store)
		}()
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cusASucceeded := countSucceededForCustomer(t, ctx, db, "cus_a")
		cusBSucceeded := countSucceededForCustomer(t, ctx, db, "cus_b")
		if cusASucceeded == deliveriesPerCustomer && cusBSucceeded == deliveriesPerCustomer {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	for range cap(errCh) {
		err := <-errCh
		if err != nil && err != context.Canceled {
			t.Fatalf("background loop: %v", err)
		}
	}

	if got := countSucceededForCustomer(t, ctx, db, "cus_a"); got != deliveriesPerCustomer {
		t.Fatalf("cus_a succeeded: got %d, want %d", got, deliveriesPerCustomer)
	}
	if got := countSucceededForCustomer(t, ctx, db, "cus_b"); got != deliveriesPerCustomer {
		t.Fatalf("cus_b succeeded: got %d, want %d", got, deliveriesPerCustomer)
	}

	seenMu.Lock()
	defer seenMu.Unlock()
	if len(seen) != totalDeliveries {
		t.Fatalf("receiver unique attempts: got %d, want %d", len(seen), totalDeliveries)
	}
	var duplicates []string
	for key, count := range seen {
		if count > 1 {
			duplicates = append(duplicates, key)
		}
	}
	sort.Strings(duplicates)
	if len(duplicates) > 0 {
		t.Fatalf("duplicate attempts: %v", duplicates)
	}
}

func TestRequeue_ReturnsInFlightToFailedRetryWithoutAttemptRow(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db)

	insertWebhook(t, ctx, db, "cus_1", "https://partner.example.com/hook", true, "subscriber.created")

	_, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_1",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_1"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	claimed, ok, err := store.ClaimNext(ctx, "cus_1")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: no delivery")
	}

	var claimExists bool
	err = db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM delivery_claims WHERE delivery_id = $1)`, claimed.ID).Scan(&claimExists)
	if err != nil {
		t.Fatalf("check claim exists: %v", err)
	}
	if !claimExists {
		t.Fatal("delivery_claims row not found after claim")
	}
	if claimed.AttemptNumber != 1 {
		t.Fatalf("attempt_number after claim: got %d, want 1", claimed.AttemptNumber)
	}

	if err := store.Requeue(ctx, claimed.ID, 200*time.Millisecond); err != nil {
		t.Fatalf("Requeue: %v", err)
	}

	assertDeliveryStatus(t, ctx, db, claimed.ID, "failed_retry")
	assertAttemptCount(t, ctx, db, claimed.ID, 0)

	var attemptCount int
	err = db.QueryRowContext(ctx, `SELECT attempt_count FROM deliveries WHERE id = $1`, claimed.ID).Scan(&attemptCount)
	if err != nil {
		t.Fatalf("select attempt_count: %v", err)
	}
	if attemptCount != 0 {
		t.Errorf("attempt_count after requeue: got %d, want 0 (claim-time increment cancelled)", attemptCount)
	}

	time.Sleep(250 * time.Millisecond)

	reclaimed, ok, err := store.ClaimNext(ctx, "cus_1")
	if err != nil {
		t.Fatalf("ClaimNext after requeue: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext after requeue: no delivery")
	}
	if reclaimed.AttemptNumber != 1 {
		t.Fatalf("attempt_number after re-claim: got %d, want 1 (decrement + re-increment)", reclaimed.AttemptNumber)
	}

	if err := store.MarkSucceeded(ctx, reclaimed.ID, 200, 10*time.Millisecond); err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}

	assertDeliveryStatus(t, ctx, db, reclaimed.ID, "succeeded")
	assertAttemptCount(t, ctx, db, reclaimed.ID, 1)
}

// TestHasClaimableWorkBatchExcludesOpenBreaker proves the batch claimability
// check applies the same breaker eligibility as the claim path (issue #105,
// Fix A). A customer whose only work sits behind an open breaker that is not
// yet in its probe window must be reported as having no claimable work, so the
// worker drops it from rotation instead of busy-spinning empty claims.
func TestHasClaimableWorkBatchExcludesOpenBreaker(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 2,
		InitialCooldown:  time.Hour,
		MaxCooldown:      time.Hour,
	}))

	openBreaker := submitOneDelivery(t, ctx, db, "cus_open_breaker")
	eligible := submitOneDelivery(t, ctx, db, "cus_eligible")

	// Force the eligible customer's delivery to be due now.
	if _, err := db.ExecContext(ctx, `
		UPDATE deliveries SET next_attempt_at = now() - interval '1 second' WHERE id = $1
	`, eligible); err != nil {
		t.Fatalf("make eligible due: %v", err)
	}

	// Open the breaker on the other customer's webhook with the probe window
	// still in the future, mirroring TestClaimNextSkipsOpenWebhookUntilHalfOpenProbe.
	if _, err := db.ExecContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'open',
		    breaker_opened_at = now(),
		    breaker_next_probe_at = now() + interval '1 hour',
		    breaker_failure_count = 5
		WHERE id = (SELECT webhook_id FROM deliveries WHERE id = $1)
	`, openBreaker); err != nil {
		t.Fatalf("open breaker: %v", err)
	}

	got, err := store.HasClaimableWorkBatch(ctx, []string{"cus_open_breaker", "cus_eligible"})
	if err != nil {
		t.Fatalf("HasClaimableWorkBatch: %v", err)
	}
	if got["cus_open_breaker"] {
		t.Errorf("cus_open_breaker: got hasWork=true, want false (work is behind an open breaker)")
	}
	if !got["cus_eligible"] {
		t.Errorf("cus_eligible: got hasWork=false, want true")
	}
}

// TestRequeueBatch_ReturnsInFlightToFailedRetryWithoutAttemptRows proves
// RequeueBatch (issue #105, Fix C) requeues many claimed deliveries in one
// transaction with the same semantics as the per-delivery Requeue: claims are
// deleted, status moves to failed_retry, and attempt_count is not advanced.
func TestRequeueBatch_ReturnsInFlightToFailedRetryWithoutAttemptRows(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db)

	first := submitOneDelivery(t, ctx, db, "cus_batch_requeue")
	second, err := appendDeliveryForExistingWebhook(t, ctx, db, first)
	if err != nil {
		t.Fatalf("append delivery: %v", err)
	}

	claimed, err := store.ClaimNextN(ctx, "cus_batch_requeue", 10)
	if err != nil {
		t.Fatalf("ClaimNextN: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed count: got %d, want 2", len(claimed))
	}

	ids := []string{first, second}
	if err := store.RequeueBatch(ctx, ids, 200*time.Millisecond); err != nil {
		t.Fatalf("RequeueBatch: %v", err)
	}

	for _, id := range ids {
		assertDeliveryStatus(t, ctx, db, id, "failed_retry")
		assertAttemptCount(t, ctx, db, id, 0)

		var claimExists bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM delivery_claims WHERE delivery_id = $1)`, id).Scan(&claimExists); err != nil {
			t.Fatalf("check claim exists: %v", err)
		}
		if claimExists {
			t.Errorf("delivery %s: claim row still present after RequeueBatch", id)
		}

		var attemptRows int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_attempts WHERE delivery_id = $1`, id).Scan(&attemptRows); err != nil {
			t.Fatalf("count attempts: %v", err)
		}
		if attemptRows != 0 {
			t.Errorf("delivery %s: got %d delivery_attempts rows, want 0", id, attemptRows)
		}
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

	db, err := sql.Open("pgx", connString)
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
	`, customerID, url, eventTypes, active)
	if err != nil {
		t.Fatalf("insert webhook: %v", err)
	}
}

func submitOneDelivery(t *testing.T, ctx context.Context, db *sql.DB, customerID string) string {
	t.Helper()

	return submitOneDeliveryToURL(t, ctx, db, customerID, "https://partner.example/webhook")
}

func submitOneDeliveryToURL(t *testing.T, ctx context.Context, db *sql.DB, customerID, url string) string {
	t.Helper()

	insertWebhook(t, ctx, db, customerID, url, true, "subscriber.created")
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: customerID,
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_123"}`),
	})
	if err != nil {
		t.Fatalf("Submit %s: %v", customerID, err)
	}
	if len(submitted.DeliveryIDs) != 1 {
		t.Fatalf("delivery count for %s: got %d, want 1", customerID, len(submitted.DeliveryIDs))
	}
	return submitted.DeliveryIDs[0]
}

func makeWebhookHalfOpen(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'half_open',
		    breaker_failure_count = 3,
		    breaker_opened_at = now() - interval '1 hour',
		    breaker_next_probe_at = now() - interval '1 second',
		    breaker_cooldown_ms = 3600000,
		    breaker_probe_delivery_id = $1
		WHERE id = (SELECT webhook_id FROM deliveries WHERE id = $1)
	`, deliveryID)
	if err != nil {
		t.Fatalf("make webhook half-open: %v", err)
	}
}

func appendDeliveryForExistingWebhook(t *testing.T, ctx context.Context, db *sql.DB, existingDeliveryID string) (string, error) {
	t.Helper()

	var customerID, webhookID string
	err := db.QueryRowContext(ctx, `
		SELECT customer_id, webhook_id
		FROM deliveries
		WHERE id = $1
	`, existingDeliveryID).Scan(&customerID, &webhookID)
	if err != nil {
		return "", fmt.Errorf("load existing delivery: %w", err)
	}

	var eventID string
	err = db.QueryRowContext(ctx, `
		INSERT INTO events (customer_id, type, payload)
		VALUES ($1, 'subscriber.created', '{"subscriber_id":"sub_extra"}'::jsonb)
		RETURNING id
	`, customerID).Scan(&eventID)
	if err != nil {
		return "", fmt.Errorf("insert event: %w", err)
	}

	var deliveryID string
	err = db.QueryRowContext(ctx, `
		INSERT INTO deliveries (event_id, webhook_id, customer_id, status)
		VALUES ($1, $2, $3, 'pending')
		RETURNING id
	`, eventID, webhookID, customerID).Scan(&deliveryID)
	if err != nil {
		return "", fmt.Errorf("insert delivery: %w", err)
	}

	return deliveryID, nil
}

func assertWebhookBreaker(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, wantState string, wantFailures int) {
	t.Helper()

	var gotState string
	var gotFailures int
	err := db.QueryRowContext(ctx, `
		SELECT w.breaker_state, w.breaker_failure_count
		FROM webhooks w
		JOIN deliveries d ON d.webhook_id = w.id
		WHERE d.id = $1
	`, deliveryID).Scan(&gotState, &gotFailures)
	if err != nil {
		t.Fatalf("select webhook breaker: %v", err)
	}
	if gotState != wantState || gotFailures != wantFailures {
		t.Fatalf("webhook breaker: got state=%q failures=%d, want state=%q failures=%d", gotState, gotFailures, wantState, wantFailures)
	}
}

func assertWebhookProbe(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, wantState, wantProbeDeliveryID string) {
	t.Helper()

	var gotState string
	var gotProbeDeliveryID sql.NullString
	err := db.QueryRowContext(ctx, `
		SELECT w.breaker_state, w.breaker_probe_delivery_id::text
		FROM webhooks w
		JOIN deliveries d ON d.webhook_id = w.id
		WHERE d.id = $1
	`, deliveryID).Scan(&gotState, &gotProbeDeliveryID)
	if err != nil {
		t.Fatalf("select webhook probe: %v", err)
	}
	if gotState != wantState || !gotProbeDeliveryID.Valid || gotProbeDeliveryID.String != wantProbeDeliveryID {
		t.Fatalf("webhook probe: got state=%q probe=%q valid=%v, want state=%q probe=%q", gotState, gotProbeDeliveryID.String, gotProbeDeliveryID.Valid, wantState, wantProbeDeliveryID)
	}
}

func assertWebhookNextProbeAfter(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string, threshold time.Time) {
	t.Helper()

	var got time.Time
	err := db.QueryRowContext(ctx, `
		SELECT w.breaker_next_probe_at
		FROM webhooks w
		JOIN deliveries d ON d.webhook_id = w.id
		WHERE d.id = $1
	`, deliveryID).Scan(&got)
	if err != nil {
		t.Fatalf("select next probe: %v", err)
	}
	if !got.After(threshold) {
		t.Fatalf("next probe: got %s, want after %s", got, threshold)
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

func assertDeliveryMissing(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string) {
	t.Helper()

	var exists bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM deliveries WHERE id = $1)`, deliveryID).Scan(&exists)
	if err != nil {
		t.Fatalf("check delivery missing: %v", err)
	}
	if exists {
		t.Fatalf("delivery %s still exists", deliveryID)
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

func assertDeadLetterMissing(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string) {
	t.Helper()

	var exists bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM dead_letters WHERE delivery_id = $1)`, deliveryID).Scan(&exists)
	if err != nil {
		t.Fatalf("check dead letter missing: %v", err)
	}
	if exists {
		t.Fatalf("dead letter %s still exists", deliveryID)
	}
}

func assertDeliveryHistory(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, wantStatus string, wantReason sql.NullString, wantAttempts int) {
	t.Helper()

	var gotStatus string
	var gotReason sql.NullString
	var gotAttempts int
	var eventID, webhookID, customerID, eventType, urlHost string
	var finalAt time.Time
	err := db.QueryRowContext(ctx, `
		SELECT
		    event_id::text,
		    webhook_id::text,
		    customer_id,
		    event_type,
		    url_host,
		    final_status,
		    terminal_reason,
		    attempt_count,
		    final_at
		FROM delivery_history
		WHERE delivery_id = $1
	`, deliveryID).Scan(&eventID, &webhookID, &customerID, &eventType, &urlHost, &gotStatus, &gotReason, &gotAttempts, &finalAt)
	if err != nil {
		t.Fatalf("select delivery history: %v", err)
	}
	if eventID == "" || webhookID == "" || customerID == "" || eventType == "" || urlHost == "" || finalAt.IsZero() {
		t.Fatalf("delivery history has empty required fields: event=%q webhook=%q customer=%q type=%q host=%q final=%s", eventID, webhookID, customerID, eventType, urlHost, finalAt)
	}
	if gotStatus != wantStatus {
		t.Fatalf("history status: got %q, want %q", gotStatus, wantStatus)
	}
	if gotReason.Valid != wantReason.Valid || gotReason.String != wantReason.String {
		t.Fatalf("history reason: got %q valid=%v, want %q valid=%v", gotReason.String, gotReason.Valid, wantReason.String, wantReason.Valid)
	}
	if gotAttempts != wantAttempts {
		t.Fatalf("history attempts: got %d, want %d", gotAttempts, wantAttempts)
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

	var delaySeconds float64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(
		    GREATEST(EXTRACT(EPOCH FROM (deliveries.next_attempt_at - delivery_attempts.sent_at)), 0),
		    0
		)
		FROM deliveries
		JOIN delivery_attempts ON delivery_attempts.delivery_id = deliveries.id
		WHERE deliveries.id = $1
		ORDER BY delivery_attempts.attempt_number DESC
		LIMIT 1
	`, deliveryID).Scan(&delaySeconds)
	if err != nil {
		t.Fatalf("select scheduled retry delay: %v", err)
	}

	return time.Duration(delaySeconds * float64(time.Second))
}

func durationNear(got, want, tolerance time.Duration) bool {
	delta := got - want
	if delta < 0 {
		delta = -delta
	}
	return delta <= tolerance
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

func countSucceededForCustomer(t *testing.T, ctx context.Context, db *sql.DB, customerID string) int {
	t.Helper()

	var got int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM deliveries WHERE status = 'succeeded' AND customer_id = $1`, customerID).Scan(&got)
	if err != nil {
		t.Fatalf("count succeeded for customer %s: %v", customerID, err)
	}
	return got
}

func TestPurgeOldDeadLettersRemovesRowsOlderThanMaxAge(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	oldDeadID := submitOneDeliveryToURL(t, ctx, db, "cus_purge_dl_old", srv.URL)
	claimedOld, ok, err := store.ClaimNext(ctx, "cus_purge_dl_old")
	if err != nil {
		t.Fatalf("ClaimNext old dead: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext old dead: got no delivery, want one")
	}
	if err := store.MarkDead(ctx, claimedOld.ID, http.StatusNotFound, retry.ReasonTerminal4xx, retry.ReasonTerminal4xx, 10*time.Millisecond); err != nil {
		t.Fatalf("MarkDead old: %v", err)
	}

	recentDeadID := submitOneDeliveryToURL(t, ctx, db, "cus_purge_dl_recent", srv.URL)
	claimedRecent, ok, err := store.ClaimNext(ctx, "cus_purge_dl_recent")
	if err != nil {
		t.Fatalf("ClaimNext recent dead: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext recent dead: got no delivery, want one")
	}
	if err := store.MarkDead(ctx, claimedRecent.ID, http.StatusNotFound, retry.ReasonTerminal4xx, retry.ReasonTerminal4xx, 10*time.Millisecond); err != nil {
		t.Fatalf("MarkDead recent: %v", err)
	}

	retention.AgeTerminalDelivery(t, ctx, db, oldDeadID, 48*time.Hour)

	purged, err := store.PurgeOldDeadLetters(ctx, 24*time.Hour, 10)
	if err != nil {
		t.Fatalf("PurgeOldDeadLetters: %v", err)
	}
	if purged != 1 {
		t.Fatalf("purged count: got %d, want 1", purged)
	}

	assertDeadLetterMissing(t, ctx, db, oldDeadID)
	assertDeadLetter(t, ctx, db, recentDeadID, retry.ReasonTerminal4xx, 1)
}

func TestPurgeOldDeliveryHistoryRemovesRowsOlderThanMaxAge(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	oldID := submitOneDeliveryToURL(t, ctx, db, "cus_purge_dh_old", srv.URL)
	claimedOld, ok, err := store.ClaimNext(ctx, "cus_purge_dh_old")
	if err != nil {
		t.Fatalf("ClaimNext old: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext old: got no delivery, want one")
	}
	if err := store.MarkSucceeded(ctx, claimedOld.ID, http.StatusOK, 10*time.Millisecond); err != nil {
		t.Fatalf("MarkSucceeded old: %v", err)
	}
	retention.AgeTerminalDelivery(t, ctx, db, oldID, 48*time.Hour)

	recentID := submitOneDeliveryToURL(t, ctx, db, "cus_purge_dh_recent", srv.URL)
	claimedRecent, ok, err := store.ClaimNext(ctx, "cus_purge_dh_recent")
	if err != nil {
		t.Fatalf("ClaimNext recent: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext recent: got no delivery, want one")
	}
	if err := store.MarkSucceeded(ctx, claimedRecent.ID, http.StatusOK, 10*time.Millisecond); err != nil {
		t.Fatalf("MarkSucceeded recent: %v", err)
	}
	retention.AgeTerminalDelivery(t, ctx, db, recentID, 25*time.Hour)

	archived, err := store.ArchiveTerminalDeliveries(ctx, 24*time.Hour, 10)
	if err != nil {
		t.Fatalf("ArchiveTerminalDeliveries: %v", err)
	}
	if archived != 2 {
		t.Fatalf("archived count: got %d, want 2", archived)
	}

	purged, err := store.PurgeOldDeliveryHistory(ctx, 26*time.Hour, 10)
	if err != nil {
		t.Fatalf("PurgeOldDeliveryHistory: %v", err)
	}
	if purged != 1 {
		t.Fatalf("purged count: got %d, want 1", purged)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_history WHERE delivery_id = $1`, oldID).Scan(&count); err != nil {
		t.Fatalf("count old history: %v", err)
	}
	if count != 0 {
		t.Fatalf("old delivery_history count: got %d, want 0", count)
	}

	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_history WHERE delivery_id = $1`, recentID).Scan(&count); err != nil {
		t.Fatalf("count recent history: %v", err)
	}
	if count != 1 {
		t.Fatalf("recent delivery_history count: got %d, want 1", count)
	}
}
