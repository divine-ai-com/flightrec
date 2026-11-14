// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/slogtest"
	"time"

	"github.com/divine-ai-com/flightrec"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	raw := b.buf.String()
	b.mu.Unlock()

	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	lines := strings.Split(raw, "\n")
	out := make([]map[string]any, 0, len(lines))
	for _, ln := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			t.Fatalf("unmarshal log line %q: %v", ln, err)
		}
		out = append(out, m)
	}
	return out
}

func TestHealthyRequestDropsBufferAndWritesSummary(t *testing.T) {
	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		Next:        slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLevel:   slog.LevelWarn,
		BufferLevel: slog.LevelDebug,
	})
	logger := slog.New(rec)

	h := flightrec.HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger.DebugContext(ctx, "step 1: parsed input", "user_id", 42)
		logger.InfoContext(ctx, "step 2: queried cache", "hit", true)
		logger.DebugContext(ctx, "step 3: prepared response")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/orders", nil)
	req.Header.Set("X-Trace-Id", "trace-healthy-01")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	lines := out.Lines(t)
	if len(lines) != 1 {
		t.Fatalf("expected exactly 1 summary line, got %d: %+v", len(lines), lines)
	}
	summary := lines[0]
	if summary["msg"] != "request completed" {
		t.Errorf("unexpected summary msg: %v", summary["msg"])
	}
	if summary["flightrec.trace_id"] != "trace-healthy-01" {
		t.Errorf("unexpected trace_id: %v", summary["flightrec.trace_id"])
	}
	if summary["flightrec.dropped_records"] != float64(3) {
		t.Errorf("expected dropped_records=3, got %v", summary["flightrec.dropped_records"])
	}

	st := rec.Stats()
	if st.ScopesHealthyDropped != 1 || st.RecordsHealthyDropped != 3 || st.BufferedBytes != 0 {
		t.Errorf("unexpected stats: %+v", st)
	}
}

func TestTriggersFlushBufferedRecordsInOrder(t *testing.T) {
	tests := []struct {
		name       string
		slowAfter  time.Duration
		advanceBy  time.Duration
		statusCode int
		handlerFn  func(ctx context.Context, logger *slog.Logger)
		wantPanic  bool
		wantReason string
	}{
		{
			name:       "error_log trigger",
			statusCode: http.StatusOK,
			handlerFn: func(ctx context.Context, logger *slog.Logger) {
				logger.DebugContext(ctx, "first debug")
				logger.InfoContext(ctx, "second info")
				logger.ErrorContext(ctx, "db query failed", "err", "connection reset")
			},
			wantReason: "error_log",
		},
		{
			name:       "http_5xx trigger",
			statusCode: http.StatusServiceUnavailable,
			handlerFn: func(ctx context.Context, logger *slog.Logger) {
				logger.DebugContext(ctx, "first debug")
				logger.InfoContext(ctx, "second info")
			},
			wantReason: "http_503",
		},
		{
			name:       "panic trigger",
			statusCode: http.StatusOK,
			handlerFn: func(ctx context.Context, logger *slog.Logger) {
				logger.DebugContext(ctx, "first debug")
				logger.InfoContext(ctx, "second info")
				panic("nil pointer dereference")
			},
			wantPanic:  true,
			wantReason: "panic",
		},
		{
			name:       "slow_request trigger",
			slowAfter:  500 * time.Millisecond,
			advanceBy:  600 * time.Millisecond,
			statusCode: http.StatusOK,
			handlerFn: func(ctx context.Context, logger *slog.Logger) {
				logger.DebugContext(ctx, "first debug")
				logger.InfoContext(ctx, "second info")
			},
			wantReason: "slow_request",
		},
		{
			name:       "explicit Flush trigger",
			statusCode: http.StatusOK,
			handlerFn: func(ctx context.Context, logger *slog.Logger) {
				logger.DebugContext(ctx, "first debug")
				logger.InfoContext(ctx, "second info")
				flightrec.Flush(ctx, "payment mismatch")
			},
			wantReason: "payment mismatch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := &syncBuffer{}
			now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
			var nowMu sync.Mutex

			rec := flightrec.New(flightrec.Options{
				Next:        slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug}),
				PassLevel:   slog.LevelWarn,
				BufferLevel: slog.LevelDebug,
				SlowAfter:   tc.slowAfter,
				Now: func() time.Time {
					nowMu.Lock()
					defer nowMu.Unlock()
					return now
				},
			})
			logger := slog.New(rec)

			h := flightrec.HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tc.handlerFn(r.Context(), logger)
				if tc.advanceBy > 0 {
					nowMu.Lock()
					now = now.Add(tc.advanceBy)
					nowMu.Unlock()
				}
				w.WriteHeader(tc.statusCode)
			}))

			req := httptest.NewRequest(http.MethodPost, "/checkout", nil)
			req.Header.Set("Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			rr := httptest.NewRecorder()

			didPanic := false
			func() {
				defer func() {
					if recover() != nil {
						didPanic = true
					}
				}()
				h.ServeHTTP(rr, req)
			}()

			if didPanic != tc.wantPanic {
				t.Fatalf("didPanic=%v, want %v", didPanic, tc.wantPanic)
			}

			lines := out.Lines(t)
			var flushedMsgs []string
			for _, ln := range lines {
				if ln["flightrec.flush_reason"] == tc.wantReason {
					flushedMsgs = append(flushedMsgs, ln["msg"].(string))
					if ln["flightrec.trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
						t.Errorf("expected W3C trace_id, got %v", ln["flightrec.trace_id"])
					}
				}
			}

			if len(flushedMsgs) != 2 || flushedMsgs[0] != "first debug" || flushedMsgs[1] != "second info" {
				t.Fatalf("expected flushed messages [first debug, second info] with reason %q, got %v (all lines: %+v)", tc.wantReason, flushedMsgs, lines)
			}
		})
	}
}

func TestMemoryCapsAndEvictionMetrics(t *testing.T) {
	t.Run("MaxPerScope drops oldest records and marks truncated", func(t *testing.T) {
		out := &syncBuffer{}
		rec := flightrec.New(flightrec.Options{
			Next:        slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug}),
			PassLevel:   slog.LevelWarn,
			BufferLevel: slog.LevelDebug,
			MaxPerScope: 450, // room for roughly 2 small records (~180 bytes each)
			MaxTotal:    64 << 10,
		})
		logger := slog.New(rec)

		ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "cap-scope"})
		logger.DebugContext(ctx, "msg-1")
		logger.DebugContext(ctx, "msg-2")
		logger.DebugContext(ctx, "msg-3")
		logger.DebugContext(ctx, "msg-4")
		flightrec.Flush(ctx, "test_cap")
		sc.End(ctx, http.StatusOK)

		lines := out.Lines(t)
		if len(lines) == 0 || len(lines) >= 4 {
			t.Fatalf("expected partial truncated flush (1..3 lines), got %d: %+v", len(lines), lines)
		}
		// Last message must be msg-4 (newest kept, oldest dropped).
		last := lines[len(lines)-1]
		if last["msg"] != "msg-4" {
			t.Errorf("expected newest record msg-4 to survive, got %v", last["msg"])
		}
		if lines[0]["flightrec.truncated"] != true {
			t.Errorf("expected first flushed line to carry flightrec.truncated=true, got %+v", lines[0])
		}

		st := rec.Stats()
		if st.RecordsDroppedPerScopeCap == 0 {
			t.Errorf("expected RecordsDroppedPerScopeCap > 0, got %+v", st)
		}
		if st.BufferedBytes != 0 {
			t.Errorf("expected BufferedBytes=0 after End, got %d", st.BufferedBytes)
		}
	})

	t.Run("MaxTotal evicts oldest scope when process limit is reached", func(t *testing.T) {
		out := &syncBuffer{}
		rec := flightrec.New(flightrec.Options{
			Next:        slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug}),
			PassLevel:   slog.LevelWarn,
			BufferLevel: slog.LevelDebug,
			MaxPerScope: 350,
			MaxTotal:    350, // Only enough room for one scope's records
		})
		logger := slog.New(rec)

		ctx1, sc1 := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "old-scope"})
		logger.DebugContext(ctx1, "old-1")
		logger.DebugContext(ctx1, "old-2")

		ctx2, sc2 := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "new-scope"})
		logger.DebugContext(ctx2, "new-1")
		logger.DebugContext(ctx2, "new-2")

		flightrec.Flush(ctx1, "flush_old")
		sc1.End(ctx1, 500)
		flightrec.Flush(ctx2, "flush_new")
		sc2.End(ctx2, 500)

		st := rec.Stats()
		if st.ScopesEvictedTotalCap != 1 || st.RecordsDroppedTotalCap == 0 {
			t.Fatalf("expected 1 evicted scope and dropped records > 0, got %+v", st)
		}
		if st.BufferedBytes != 0 {
			t.Fatalf("expected BufferedBytes=0 after both scopes end, got %d", st.BufferedBytes)
		}
	})
}

func TestWithAttrsAndWithGroupPreservedOnFlush(t *testing.T) {
	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		Next:        slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLevel:   slog.LevelWarn,
		BufferLevel: slog.LevelDebug,
	})
	child := slog.New(rec).With("service", "billing").WithGroup("stripe")

	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "grp-1"})
	child.DebugContext(ctx, "charging card", "amount_cents", 1999)
	flightrec.Flush(ctx, "charge_declined")
	sc.End(ctx, http.StatusPaymentRequired)

	lines := out.Lines(t)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	ln := lines[0]
	if ln["service"] != "billing" {
		t.Errorf("expected top-level service=billing, got %v", ln["service"])
	}
	if ln["flightrec.flush_reason"] != "charge_declined" {
		t.Errorf("expected top-level flush_reason=charge_declined, got %v", ln["flightrec.flush_reason"])
	}
	stripeGrp, ok := ln["stripe"].(map[string]any)
	if !ok || stripeGrp["amount_cents"] != float64(1999) {
		t.Errorf("expected stripe.amount_cents=1999, got %+v", ln["stripe"])
	}
}

func TestGRPCUnaryHelper(t *testing.T) {
	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		Next: slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug}),
	})
	logger := slog.New(rec)

	_, err := flightrec.InvokeGRPCUnary(
		context.Background(),
		rec,
		"/orders.OrderService/CreateOrder",
		func(context.Context) string { return "grpc-trace-77" },
		"req-payload",
		func(ctx context.Context, req any) (any, error) {
			logger.DebugContext(ctx, "validating gRPC order", "req", req)
			return nil, errors.New("inventory exhausted")
		},
	)
	if err == nil {
		t.Fatal("expected error")
	}

	lines := out.Lines(t)
	if len(lines) != 1 || lines[0]["flightrec.flush_reason"] != "grpc_error" || lines[0]["flightrec.trace_id"] != "grpc-trace-77" {
		t.Fatalf("unexpected gRPC flush lines: %+v", lines)
	}
}

func TestSlogtestCompliancePassThrough(t *testing.T) {
	var buf bytes.Buffer
	rec := flightrec.New(flightrec.Options{
		Next:        slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLevel:   slog.LevelDebug,
		BufferLevel: slog.LevelDebug,
	})

	results := func() []map[string]any {
		var ms []map[string]any
		for _, line := range bytes.Split(buf.Bytes(), []byte{'\n'}) {
			if len(line) == 0 {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal(line, &m); err != nil {
				t.Fatal(err)
			}
			ms = append(ms, m)
		}
		return ms
	}

	if err := slogtest.TestHandler(rec, results); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentStressWithRaceDetector(t *testing.T) {
	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		Next:        slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLevel:   slog.LevelWarn,
		BufferLevel: slog.LevelDebug,
		MaxPerScope: 4 << 10,
		MaxTotal:    64 << 10,
	})
	logger := slog.New(rec)

	h := flightrec.HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		for i := 0; i < 25; i++ {
			logger.DebugContext(ctx, "processing item", "idx", i)
		}
		if r.Header.Get("X-Fail") == "1" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				req := httptest.NewRequest(http.MethodGet, "/work", nil)
				if (id+i)%10 == 0 {
					req.Header.Set("X-Fail", "1")
				}
				rr := httptest.NewRecorder()
				h.ServeHTTP(rr, req)
			}
		}(g)
	}
	wg.Wait()

	st := rec.Stats()
	if st.ActiveScopes != 0 || st.BufferedBytes != 0 {
		t.Fatalf("leaked active scopes or bytes: %+v", st)
	}
}
