// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"bytes"
	"encoding/json"
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
		Next:        slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLevel:   slog.LevelWarn,
		BufferLevel: slog.LevelDebug,
		SlowAfter:   2 * time.Second,
		MaxPerScope: 256 << 10,
		MaxTotal:    64 << 20,
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
