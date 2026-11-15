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
	"strings"
	"time"

	"github.com/divine-ai-com/flightrec"
)

func ExampleNew() {
	var buf bytes.Buffer
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(&buf, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
		SlowAfter:     2 * time.Second,
		MaxPerScope:   256 << 10,
		MaxTotal:      64 << 20,
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
		Next:          slog.NewJSONHandler(&buf, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
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
