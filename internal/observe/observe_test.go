package observe

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestRegisterMetrics_registers_metric_families(t *testing.T) {
	ResetMetricsForTest()
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg)
	// Force Vec metrics to materialise a series so Gather includes them.
	WebhookDeliveriesTotal.WithLabelValues("succeeded").Add(0)
	WebhookDeliveryLatencySeconds.WithLabelValues("small").Observe(0)
	WebhookDlqTotal.WithLabelValues("exhausted").Add(0)
	WebhookHostSemaphoreWaitSeconds.WithLabelValues("example.com").Observe(0)

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) < 11 {
		t.Fatalf("expected at least 11 metric families, got %d", len(families))
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
