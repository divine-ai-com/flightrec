// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Command demo runs a reproducible comparison between plain slog JSON logging
// and flightrec across 1,000 requests with a 1% failure rate (10 failed
// requests and 990 healthy requests, each emitting 20 debug/info log records).
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/divine-ai-com/flightrec"
)

const (
	totalRequests  = 1000
	logsPerRequest = 20
	failEveryN     = 100 // 1% error rate (10 failed requests out of 1,000)
)

func runWorkload(handler http.Handler) {
	for i := 1; i <= totalRequests; i++ {
		path := "/api/v1/checkout"
		if i%failEveryN == 0 {
			path = "/api/v1/checkout?fail=1"
		}
		req := httptest.NewRequest(http.MethodPost, path, nil)
		req.Header.Set("X-Trace-Id", fmt.Sprintf("trace-%04d", i))
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
	}
}

func makeAppHandler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		for step := 1; step <= logsPerRequest; step++ {
			logger.DebugContext(ctx, "checkout pipeline step",
				"step", step,
				"component", "inventory-validator",
				"sku", "SKU-99481-A",
				"warehouse", "eu-central-1",
			)
		}
		if r.URL.Query().Get("fail") == "1" {
			logger.ErrorContext(ctx, "payment gateway timeout", "provider", "stripe", "attempt", 3)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func main() {
	// 1. Baseline: plain slog.JSONHandler at LevelDebug
	var plainBuf bytes.Buffer
	plainLogger := slog.New(slog.NewJSONHandler(&plainBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	startPlain := time.Now()
	runWorkload(makeAppHandler(plainLogger))
	plainDur := time.Since(startPlain)
	plainBytes := plainBuf.Len()
	plainLines := bytes.Count(plainBuf.Bytes(), []byte{'\n'})

	// 2. flightrec: buffers Debug/Info, flushes on failure, 1 summary line on healthy
	var flightBuf bytes.Buffer
	rec := flightrec.New(flightrec.Options{
		TrustRequestTraceIDs: true, // requests carry their trace ID in X-Trace-Id
		Next:                 slog.NewJSONHandler(&flightBuf, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLeveler:          slog.LevelWarn,
		BufferLeveler:        slog.LevelDebug,
		SlowAfter:            2 * time.Second,
		MaxPerScope:          256 << 10,
		MaxTotal:             64 << 20,
	})
	flightLogger := slog.New(rec)
	wrappedHandler := flightrec.HTTPMiddleware(rec)(makeAppHandler(flightLogger))

	startFlight := time.Now()
	runWorkload(wrappedHandler)
	flightDur := time.Since(startFlight)
	flightBytes := flightBuf.Len()
	flightLines := bytes.Count(flightBuf.Bytes(), []byte{'\n'})

	// 3. Verify that failed request trace-0100 has all 20 debug steps in order + the error log
	var trace100Steps []int
	var trace100ErrorSeen bool
	for _, line := range strings.Split(strings.TrimSpace(flightBuf.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			fmt.Fprintf(os.Stderr, "invalid JSON line: %v\n", err)
			os.Exit(1)
		}
		if m["flightrec.trace_id"] == "trace-0100" {
			if m["msg"] == "payment gateway timeout" {
				trace100ErrorSeen = true
			}
			if step, ok := m["step"].(float64); ok {
				trace100Steps = append(trace100Steps, int(step))
			}
		}
	}
	for idx, step := range trace100Steps {
		if step != idx+1 {
			fmt.Fprintf(os.Stderr, "out-of-order step at index %d: got %d\n", idx, step)
			os.Exit(1)
		}
	}
	if !trace100ErrorSeen || len(trace100Steps) != logsPerRequest {
		fmt.Fprintf(os.Stderr, "incomplete failed trace-0100 logs: errorSeen=%v steps=%d\n", trace100ErrorSeen, len(trace100Steps))
		os.Exit(1)
	}

	reductionPct := (1.0 - float64(flightBytes)/float64(plainBytes)) * 100.0
	st := rec.Stats()

	fmt.Println("=== flightrec volume and fidelity demo ===")
	fmt.Printf("Workload: %d requests (%d healthy, %d failed [1%%]), %d debug logs/request\n\n",
		totalRequests, totalRequests-totalRequests/failEveryN, totalRequests/failEveryN, logsPerRequest)
	fmt.Printf("%-26s %12s %12s %12s\n", "Mode", "Log Lines", "Log Bytes", "Wall Time")
	fmt.Printf("%-26s %12d %12d %12s\n", "Plain slog (LevelDebug)", plainLines, plainBytes, plainDur.Round(time.Microsecond))
	fmt.Printf("%-26s %12d %12d %12s\n", "flightrec", flightLines, flightBytes, flightDur.Round(time.Microsecond))
	fmt.Printf("\nLog byte reduction:   %.1f%% (%d B -> %d B)\n", reductionPct, plainBytes, flightBytes)
	fmt.Printf("Log line reduction:   %.1f%% (%d -> %d lines)\n", (1.0-float64(flightLines)/float64(plainLines))*100.0, plainLines, flightLines)
	fmt.Printf("Failed trace check:   PASS (trace-0100 retained all %d debug steps in order + error record)\n", len(trace100Steps))
	fmt.Printf("Recorder stats:       started=%d flushed=%d healthy_dropped=%d buffered_records_dropped=%d active_bytes=%d\n",
		st.ScopesStarted, st.ScopesFlushed, st.ScopesHealthyDropped, st.RecordsHealthyDropped, st.BufferedBytes)
}
