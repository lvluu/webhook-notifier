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

	logger.Info("registering customers", "count", *customerCount, "receiver", *receiverURL, "class", *customerClass)
	for _, cid := range customerIDs {
		body, _ := json.Marshal(map[string]interface{}{
			"id":    cid,
			"class": *customerClass,
			"url":   *receiverURL + "/hooks/" + cid,
		})
		resp, err := client.Post(*notifierURL+"/api/customers", "application/json", bytes.NewReader(body))
		if err != nil {
			logger.Error("register customer failed", "customer", cid, "error", err)
			continue
		}
		if err := resp.Body.Close(); err != nil {
			logger.Warn("close register response body", "customer", cid, "error", err)
		}
	}

	logger.Info("customers registered, starting event emission",
		"rate", *rate, "burst", *burst, "duration", duration, "class", *customerClass,
	)

	emitter := chaos.New(chaos.Config{
		NotifierURL:  *notifierURL,
		Rate:         *rate,
		MaxInFlight:  *burst,
		Duration:     *duration,
		CustomerIDs:  customerIDs,
		CustomerBase: *customerBase,
		HTTPClient:   client,
	})

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

	logger.Info("shutdown complete",
		"total_sent", emitter.Sent(),
		"total_failed", emitter.Failed(),
		"total_shed", emitter.Shed(),
		"elapsed", time.Since(startTime).Round(time.Second),
	)
}
