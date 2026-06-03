// Command chaos emits synthetic webhook events to load test notifier.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lvluu/webhook-notifier/internal/chaos"
)

func main() {
	notifierURL := flag.String("notifier", "http://localhost:8080", "notifier base URL")
	receiverURL := flag.String("receiver", "http://localhost:8081", "receiver base URL")
	customerBase := flag.String("customer-base", "chaos", "customer ID prefix")
	customerCount := flag.Int("customers", 10, "number of customers")
	customerClass := flag.Int("class", 1, "customer class tier (3=whale, 1=small)")
	rate := flag.Float64("rate", 100, "events per second")
	burst := flag.Int("burst", 10, "maximum concurrent in-flight HTTP requests")
	duration := flag.Duration("duration", 0, "run duration (0 = forever)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	logger = logger.With("service", "chaos-loader")

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
	}

	customerIDs := make([]string, *customerCount)
	for i := range *customerCount {
		customerIDs[i] = fmt.Sprintf("%s-%04d", *customerBase, i+1)
	}

	logger.Info("seeding customers", "count", *customerCount, "receiver", *receiverURL)
	body, err := json.Marshal(map[string]interface{}{
		"customer_base": *customerBase,
		"count":         *customerCount,
		"class":         *customerClass,
		"receiver_url":  *receiverURL,
	})
	if err != nil {
		logger.Error("marshal setup-customers request", "error", err)
		os.Exit(1)
	}

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	req, err := http.NewRequestWithContext(
		setupCtx,
		http.MethodPost,
		*notifierURL+"/setup-customers",
		bytes.NewReader(body),
	)
	if err != nil {
		setupCancel()
		logger.Error("build setup-customers request", "error", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	setupCancel()
	if err != nil {
		logger.Error("setup-customers failed", "error", err)
		os.Exit(1)
	}
	closeSetupResponse := func() {
		if err := resp.Body.Close(); err != nil {
			logger.Warn("close setup-customers response body", "error", err)
		}
	}

	if resp.StatusCode != http.StatusCreated {
		logger.Error("setup-customers rejected", "status", resp.StatusCode)
		closeSetupResponse()
		os.Exit(1)
	}
	var result struct {
		Created int `json:"created"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		logger.Error("decode setup-customers response", "error", err)
		closeSetupResponse()
		os.Exit(1)
	}
	closeSetupResponse()
	logger.Info("customers seeded", "created", result.Created)

	logger.Info("customers registered, starting event emission",
		"rate", *rate, "burst", *burst, "duration", duration, "class", *customerClass,
	)

	emitter, err := chaos.New(chaos.Config{
		NotifierURL:  *notifierURL,
		Rate:         *rate,
		MaxInFlight:  *burst,
		Duration:     *duration,
		CustomerIDs:  customerIDs,
		CustomerBase: *customerBase,
		HTTPClient:   client,
	})
	if err != nil {
		logger.Error("configure chaos emitter", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case <-done:
			logger.Info("shutting down")
		case <-ctx.Done():
			logger.Info("duration reached")
		}
		cancel()
	}()

	startTime := time.Now()
	emitter.Run(ctx)
	if ctx.Err() == nil && *duration > 0 && time.Since(startTime) >= *duration {
		logger.Info("duration reached")
	}

	logger.Info("shutdown complete",
		"total_sent", emitter.Sent(),
		"total_failed", emitter.Failed(),
		"total_shed", emitter.Shed(),
		"elapsed", time.Since(startTime).Round(time.Second),
	)
}
