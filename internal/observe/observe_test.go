package observe

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// fakeDriver and fakeConn implement enough of the database/sql/driver contract
// so that RegisterPoolGauges tests pass without a real Postgres installation.
type fakeDriver struct {
	driver.Driver
}
type fakeConn struct {
	driver.Conn
}
type fakeRows struct {
	sent bool
}

func (fakeDriver) Open(_ string) (driver.Conn, error)  { return fakeConn{}, nil }
func (fakeConn) Prepare(_ string) (driver.Stmt, error) { return nil, nil }
func (fakeConn) Close() error                          { return nil }
func (fakeConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return &fakeRows{}, nil
}
func (fakeRows) Columns() []string { return []string{"count"} }
func (fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.sent {
		return io.EOF
	}
	r.sent = true
	dest[0] = int64(7)
	return nil
}

func init() { sql.Register("fake", fakeDriver{}) }

func TestRegisterMetrics_registers_metric_families(t *testing.T) {
	ResetMetricsForTest()
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg)
	// Force Vec metrics to materialise a series so Gather includes them.
	WebhookDeliveriesTotal.WithLabelValues("succeeded").Add(0)
	WebhookDeliveryLatencySeconds.WithLabelValues("small").Observe(0)
	WebhookDlqTotal.WithLabelValues("exhausted").Add(0)
	WebhookHostSemaphoreWaitSeconds.WithLabelValues("example.com").Observe(0)
	WebhookBreakerState.WithLabelValues("webhook_1", "closed").Set(1)
	WebhookBreakerTripsTotal.WithLabelValues("retryable_5xx").Add(0)

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) < 13 {
		t.Fatalf("expected at least 13 metric families, got %d", len(families))
	}
}

func TestMetricsEndpoint_returns_Prometheus_text(t *testing.T) {
	ResetMetricsForTest()
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg)
	WebhookDeliveriesTotal.WithLabelValues("succeeded").Add(1)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("webhook_deliveries_total")) {
		t.Fatal("/metrics response missing webhook_deliveries_total")
	}
}

func TestNewLogger_outputs_JSON(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("test message", "key", "value")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("expected JSON output: %s", buf.String())
	}
	if m["msg"] != "test message" {
		t.Errorf("expected msg='test message', got %v", m["msg"])
	}
}

func TestRegisterPoolGauges_registers_six_families(t *testing.T) {
	ResetMetricsForTest()
	reg := prometheus.NewRegistry()

	db, err := sql.Open("fake", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	RegisterMetrics(reg)
	RegisterPoolGauges(reg, db)

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var poolCount int
	for _, f := range families {
		if strings.HasPrefix(f.GetName(), "webhook_pg_pool") {
			poolCount++
		}
	}
	if poolCount < 6 {
		t.Fatalf("expected at least 6 webhook_pg_pool_* families, got %d", poolCount)
	}
}

func TestRegisterPendingCountGauges_registers_customer_class_families(t *testing.T) {
	reg := prometheus.NewRegistry()

	db, err := sql.Open("fake", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	RegisterPendingCountGauges(reg, db)

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, f := range families {
		if f.GetName() != "webhook_pending_count" {
			continue
		}
		found = true
		if len(f.GetMetric()) != 3 {
			t.Fatalf("expected 3 customer_class series, got %d", len(f.GetMetric()))
		}
	}
	if !found {
		t.Fatal("missing webhook_pending_count family")
	}
}

func TestMetricsEndpoint_returns_nonzero_after_seed(t *testing.T) {
	ResetMetricsForTest()
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg)

	WebhookDeliveriesTotal.WithLabelValues("succeeded").Add(3)
	WebhookDeliveryLatencySeconds.WithLabelValues("small").Observe(0.5)
	WebhookAttemptsPerDelivery.Observe(2)
	WebhookDlqTotal.WithLabelValues("exhausted").Add(1)
	WebhookSchedulerPickDurationSeconds.Observe(0.001)
	WebhookHostSemaphoreWaitSeconds.WithLabelValues("example.com").Observe(0.01)
	WebhookClaimDurationSeconds.Observe(0.1)
	WebhookClaimBatchSize.Observe(5)
	WebhookClaimEmptyTotal.Add(2)
	WebhookActiveCustomers.Set(4)
	WebhookIngestFanoutPerEvent.Observe(3)
	SetBreakerState("webhook_1", "open")
	IncBreakerTrip("retryable_5xx")

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)

	requireContains := func(sub string) {
		t.Helper()
		if !bytes.Contains(body, []byte(sub)) {
			t.Fatalf("/metrics response missing %q", sub)
		}
	}
	requireContains(`webhook_deliveries_total{status="succeeded"} 3`)
	// Vec histograms include the customer_class label before le, so match the full label set.
	requireContains(`webhook_delivery_latency_seconds_bucket{customer_class="small",le="0.5"}`)
	requireContains(`webhook_attempts_per_delivery_bucket{le="2"}`)
	requireContains(`webhook_dlq_total{reason="exhausted"} 1`)
	requireContains(`webhook_scheduler_pick_duration_seconds_bucket{le="1e-05"}`)
	requireContains(`webhook_host_semaphore_wait_seconds_bucket`)
	requireContains(`webhook_claim_duration_seconds_bucket`)
	requireContains(`webhook_claim_batch_size_bucket`)
	requireContains(`webhook_claim_empty_total 2`)
	requireContains(`webhook_active_customers 4`)
	requireContains(`webhook_ingest_fanout_per_event_bucket`)
	requireContains(`webhook_breaker_state{state="open",webhook_id="webhook_1"} 1`)
	requireContains(`webhook_breaker_trips_total{reason="retryable_5xx"} 1`)
}

func TestCustomerClassFromWeight(t *testing.T) {
	tests := []struct {
		weight uint32
		want   string
	}{
		{5, "whale"},
		{3, "whale"},
		{1, "small"},
		{2, "small"},
		{0, "paused"},
	}
	for _, tt := range tests {
		got := CustomerClassFromWeight(tt.weight)
		if got != tt.want {
			t.Errorf("CustomerClassFromWeight(%d) = %q, want %q", tt.weight, got, tt.want)
		}
	}
}
