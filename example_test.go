// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/divine-ai-com/flightrec"
	"github.com/divine-ai-com/flightrec/notifier/memnotifier"
)

func ExampleNew() {
	var buf bytes.Buffer
	rec := flightrec.New(flightrec.Options{
		TrustRequestTraceIDs: true,
		EnableFlushMarker:    true,
		Next:                 slog.NewJSONHandler(&buf, nil),
		PassLeveler:          slog.LevelWarn,
		BufferLeveler:        slog.LevelDebug,
		SlowAfter:            2 * time.Second,
		MaxPerScope:          256 << 10,
		MaxTotal:             64 << 20,
	})
	logger := slog.New(rec)

	mux := http.NewServeMux()
	mux.HandleFunc("/pay", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger.DebugContext(ctx, "loaded cart", "items", 3)
		if r.URL.Query().Get("fail") == "1" {
			flightrec.Flush(ctx, "payment mismatch")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	handler := flightrec.HTTPMiddleware(rec)(mux)

	// 1. Healthy request -> debug log is dropped; 1 summary line emitted.
	reqOk := httptest.NewRequest(http.MethodPost, "/pay", nil)
	reqOk.Header.Set("X-Trace-Id", "req-healthy")
	handler.ServeHTTP(httptest.NewRecorder(), reqOk)

	// 2. Triggered request -> debug log is flushed with reason.
	reqFail := httptest.NewRequest(http.MethodPost, "/pay?fail=1", nil)
	reqFail.Header.Set("X-Trace-Id", "req-failed")
	handler.ServeHTTP(httptest.NewRecorder(), reqFail)

	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		_ = json.Unmarshal([]byte(line), &m)
		fmt.Printf("trace=%v msg=%q reason=%v dropped=%v\n",
			m["flightrec.trace_id"], m["msg"], m["flightrec.flush_reason"], m["flightrec.dropped_records"])
	}
	// Output:
	// trace=req-healthy msg="request completed" reason=<nil> dropped=1
	// trace=req-failed msg="loaded cart" reason=payment mismatch dropped=<nil>
}

func ExampleStartScope() {
	var buf bytes.Buffer
	rec := flightrec.New(flightrec.Options{
		Next:                  slog.NewJSONHandler(&buf, nil),
		PassLeveler:           slog.LevelWarn,
		BufferLeveler:         slog.LevelDebug,
		DisableHealthySummary: true,
	})
	logger := slog.New(rec)

	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{
		TraceID: "job-42",
		Method:  "JOB",
		Path:    "sync-invoices",
	})
	logger.DebugContext(ctx, "fetched batch", "count", 10)
	logger.ErrorContext(ctx, "upstream timeout")
	sc.End(ctx, 500)

	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		_ = json.Unmarshal([]byte(line), &m)
		fmt.Printf("level=%v msg=%q reason=%v\n", m["level"], m["msg"], m["flightrec.flush_reason"])
	}
	// Output:
	// level=ERROR msg="upstream timeout" reason=<nil>
	// level=DEBUG msg="fetched batch" reason=error_log
}

func ExampleFlush() {
	var buf bytes.Buffer
	rec := flightrec.New(flightrec.Options{
		Next:                  slog.NewJSONHandler(&buf, nil),
		PassLeveler:           slog.LevelWarn,
		BufferLeveler:         slog.LevelDebug,
		DisableHealthySummary: true,
	})
	logger := slog.New(rec)

	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "tx-99"})
	logger.DebugContext(ctx, "computed ledger delta", "cents", -500)
	flightrec.Flush(ctx, "invariant_check_failed")
	sc.End(ctx, http.StatusOK)

	var m map[string]any
	_ = json.Unmarshal(buf.Bytes(), &m)
	fmt.Printf("trace=%v msg=%q reason=%v\n", m["flightrec.trace_id"], m["msg"], m["flightrec.flush_reason"])
	// Output:
	// trace=tx-99 msg="computed ledger delta" reason=invariant_check_failed
}

func ExampleInvokeGRPCUnary() {
	var buf bytes.Buffer
	rec := flightrec.New(flightrec.Options{
		TrustRequestTraceIDs: true,
		EnableFlushMarker:    true,
		Next:                 slog.NewJSONHandler(&buf, nil),
		PassLeveler:          slog.LevelWarn,
		BufferLeveler:        slog.LevelDebug,
	})
	logger := slog.New(rec)

	_, _ = flightrec.InvokeGRPCUnary(
		flightrec.WithIncomingMetadata(context.Background(), map[string][]string{"x-trace-id": {"grpc-1"}}),
		rec,
		"/billing.Service/Charge",
		nil,
		"req",
		func(ctx context.Context, req any) (any, error) {
			logger.DebugContext(ctx, "validating card", "req", req)
			return nil, errors.New("gateway unreachable")
		},
	)

	var m map[string]any
	_ = json.Unmarshal(buf.Bytes(), &m)
	fmt.Printf("trace=%v msg=%q reason=%v\n", m["flightrec.trace_id"], m["msg"], m["flightrec.flush_reason"])
	// Output:
	// trace=grpc-1 msg="validating card" reason=grpc_error
}

// A service that called a failing service keeps its own logs for the request:
// the response's Flightrec-Flush header triggers the caller's scope.
func ExampleNewTransport() {
	downstream := httptest.NewServer(flightrec.HTTPMiddleware(flightrec.New(flightrec.Options{Next: slog.DiscardHandler, TrustRequestTraceIDs: true, EnableFlushMarker: true}))(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) })))
	defer downstream.Close()

	var buf bytes.Buffer
	rec := flightrec.New(flightrec.Options{Next: slog.NewJSONHandler(&buf, nil), BufferLeveler: slog.LevelDebug, TrustRequestTraceIDs: true, EnableFlushMarker: true})
	client := &http.Client{Transport: flightrec.NewTransport(http.DefaultTransport)}

	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "req-1"})
	slog.New(rec).DebugContext(ctx, "calling inventory")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, downstream.URL, nil)
	if resp, err := client.Do(req); err == nil {
		_ = resp.Body.Close()
	}
	sc.End(ctx, http.StatusOK) // this service answered 200, but kept its logs

	var m map[string]any
	_ = json.Unmarshal(buf.Bytes(), &m)
	fmt.Println(m["msg"], m["flightrec.flush_reason"])
	// Output: calling inventory downstream:http_503
}

// With Retain, a healthy request's logs are kept for a while; FlushTrace
// writes them when a failure elsewhere turns out to belong to the same trace.
func ExampleRecorder_FlushTrace() {
	var buf bytes.Buffer
	rec := flightrec.New(flightrec.Options{
		Next:                  slog.NewJSONHandler(&buf, nil),
		BufferLeveler:         slog.LevelDebug,
		Retain:                30 * time.Second,
		DisableHealthySummary: true,
	})
	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "req-7"})
	slog.New(rec).DebugContext(ctx, "queued confirmation email")
	sc.End(ctx, http.StatusOK)

	n := rec.FlushTrace(context.Background(), "req-7", "payment reversed")
	var m map[string]any
	_ = json.Unmarshal(buf.Bytes(), &m)
	fmt.Println(n, m["msg"], m["flightrec.flush_reason"])
	// Output: 1 queued confirmation email payment reversed
}

// Services connected through a notifier keep each other's logs: a failure in
// one flushes the retained logs of the others for the same trace. Sign the
// messages with a key every service shares.
func ExampleRecorder_StartNotifier() {
	rec := flightrec.New(flightrec.Options{Next: slog.NewJSONHandler(os.Stdout, nil), Retain: 30 * time.Second})
	bus := memnotifier.New() // redisnotifier or natsnotifier across processes
	key := []byte(os.Getenv("FLIGHTREC_NOTIFIER_KEY"))
	if len(key) < 32 {
		key = bytes.Repeat([]byte("k"), 32) // example only: use 32 random bytes
	}
	stop := rec.StartNotifier(context.Background(), bus, flightrec.WithSigningKey(key))
	defer stop()
}

// A journal on a volume that outlives the process lets the next start write
// the debug logs of requests that were in flight when the process was killed.
func ExampleRecorder_StartJournal() {
	rec := flightrec.New(flightrec.Options{Next: slog.NewJSONHandler(os.Stdout, nil)})
	stop, err := rec.StartJournal(filepath.Join(os.TempDir(), "flightrec.journal"))
	if errors.Is(err, flightrec.ErrJournalUnsupported) {
		return // Windows: run without a journal
	}
	if err != nil {
		panic(err)
	}
	defer func() { _ = stop() }() // after the server has drained its requests
}
