package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	metricsAddr := flag.String("metrics-addr", ":9091", "metrics listen address")
	latencyMin := flag.Duration("latency-min", 5*time.Millisecond, "minimum response latency")
	latencyMax := flag.Duration("latency-max", 15*time.Millisecond, "maximum response latency")
	failureRate := flag.Float64("failure-rate", 0.0, "failure rate (0.0-1.0)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger = logger.With("service", "webhook-receiver")

	var received atomic.Uint64

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		latency := *latencyMin
		if *latencyMax > *latencyMin {
			latency += time.Duration(rand.Int63n(int64(*latencyMax - *latencyMin)))
		}
		time.Sleep(latency)

		received.Add(1)

		if *failureRate > 0 && rand.Float64() < *failureRate {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "webhook_requests_received_total %d\n", received.Load())
	})

	server := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	metricsMux := http.NewServeMux()
	metricsMux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "webhook_requests_received_total %d\n", received.Load())
	})
	metricsServer := &http.Server{
		Addr:              *metricsAddr,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("listening", "addr", *addr, "metrics_addr", *metricsAddr, "latency_min", *latencyMin, "latency_max", *latencyMax, "failure_rate", *failureRate)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "error", err)
		}
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done

	logger.Info("shutting down", "received", received.Load())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}
