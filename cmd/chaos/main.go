package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type eventRequest struct {
	CustomerID string          `json:"customer_id"`
	Type       string          `json:"event_name"`
	EventTime  string          `json:"event_time"`
	Data       json.RawMessage `json:"data,omitempty"`
}

func main() {
	notifierURL := flag.String("notifier", "http://localhost:8080", "notifier base URL")
	receiverURL := flag.String("receiver", "http://localhost:8081", "receiver base URL")
	customerBase := flag.String("customer-base", "chaos", "customer ID prefix")
	customerCount := flag.Int("customers", 10, "number of customers")
	rate := flag.Int("rate", 100, "events per second")
	burst := flag.Int("burst", 10, "burst size per second")
	duration := flag.Duration("duration", 0, "run duration (0 = forever)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger = logger.With("service", "chaos-emitter")

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

	logger.Info("registering customers", "count", *customerCount, "receiver", *receiverURL)
	for _, cid := range customerIDs {
		body, _ := json.Marshal(map[string]interface{}{
			"id":     cid,
			"weight": 1,
			"url":    *receiverURL + "/hooks/" + cid,
		})
		resp, err := client.Post(*notifierURL+"/api/customers", "application/json", bytes.NewReader(body))
		if err != nil {
			logger.Error("register customer failed", "customer", cid, "error", err)
			continue
		}
		resp.Body.Close()
	}

	logger.Info("customers registered, starting event emission",
		"rate", rate, "burst", burst, "duration", duration,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sent atomic.Int64
	var wg sync.WaitGroup
	startTime := time.Now()

	ticker := time.NewTicker(time.Second)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if *duration > 0 && time.Since(startTime) >= *duration {
					cancel()
					return
				}

				batchStart := time.Now()
				thisBurst := *rate
				if *burst > 0 && *burst < thisBurst {
					thisBurst = *burst
				}

				var batchWg sync.WaitGroup
				for range thisBurst {
					batchWg.Add(1)
					go func() {
						defer batchWg.Done()
						cid := customerIDs[rand.Intn(len(customerIDs))]
						eventType := "order.created"
						if rand.Intn(3) == 0 {
							eventType = "payment.received"
						}

						data, _ := json.Marshal(map[string]interface{}{
							"id":     fmt.Sprintf("evt-%d", time.Now().UnixNano()),
							"amount": rand.Float64() * 1000,
						})

						body, _ := json.Marshal(eventRequest{
							CustomerID: cid,
							Type:       eventType,
							EventTime:  time.Now().UTC().Format(time.RFC3339),
							Data:       data,
						})

						resp, err := client.Post(*notifierURL+"/api/events", "application/json", bytes.NewReader(body))
						if err != nil {
							sent.Add(1)
							return
						}
						resp.Body.Close()
						sent.Add(1)
					}()
				}
				batchWg.Wait()

				logger.Info("batch complete",
					"sent", sent.Load(),
					"rate", float64(sent.Load())/time.Since(startTime).Seconds(),
					"batch_time_ms", time.Since(batchStart).Milliseconds(),
				)
			}
		}
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-done:
		logger.Info("shutting down")
	case <-ctx.Done():
		logger.Info("duration reached")
	}

	cancel()
	wg.Wait()
	logger.Info("shutdown complete",
		"total_sent", sent.Load(),
		"elapsed", time.Since(startTime).Round(time.Second),
	)
}
