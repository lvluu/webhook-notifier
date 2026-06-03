package chaos

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
)

func TestNew_ValidatesRequiredConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "notifier URL is required",
			cfg: Config{
				CustomerIDs: []string{"chaos-0001"},
			},
		},
		{
			name: "customer IDs are required",
			cfg: Config{
				NotifierURL: "http://example.test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := New(tt.cfg); err == nil {
				t.Fatal("New() error = nil, want validation error")
			}
		})
	}
}

func TestNew_AppliesDefaults(t *testing.T) {
	t.Parallel()

	emitter, err := New(Config{
		NotifierURL: "http://example.test",
		CustomerIDs: []string{"chaos-0001"},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if emitter.cfg.Rate != 1 {
		t.Fatalf("default rate = %v, want 1", emitter.cfg.Rate)
	}
	if emitter.cfg.MaxInFlight != 10 {
		t.Fatalf("default max in-flight = %d, want 10", emitter.cfg.MaxInFlight)
	}
	if emitter.cfg.HTTPClient != http.DefaultClient {
		t.Fatal("default HTTP client was not applied")
	}
}

func TestEmitter_PostCountsRequestBuildFailure(t *testing.T) {
	t.Parallel()

	emitter, err := New(Config{
		NotifierURL: ":",
		CustomerIDs: []string{"chaos-0001"},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	emitter.post(context.Background(), logger, []byte(`{}`))

	if got := emitter.Failed(); got != 1 {
		t.Fatalf("failed = %d, want 1", got)
	}
}
