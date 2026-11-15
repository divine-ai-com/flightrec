// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
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
		Next:          slog.NewJSONHandler(out, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
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
				Next:          slog.NewJSONHandler(out, nil),
				PassLeveler:   slog.LevelWarn,
				BufferLeveler: slog.LevelDebug,
				SlowAfter:     tc.slowAfter,
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
			Next:          slog.NewJSONHandler(out, nil),
			PassLeveler:   slog.LevelWarn,
			BufferLeveler: slog.LevelDebug,
			MaxPerScope:   450,
			MaxTotal:      64 << 10,
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
			Next:          slog.NewJSONHandler(out, nil),
			PassLeveler:   slog.LevelWarn,
			BufferLeveler: slog.LevelDebug,
			MaxPerScope:   350,
			MaxTotal:      350,
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
		Next:          slog.NewJSONHandler(out, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
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
		Next: slog.NewJSONHandler(out, nil),
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

func TestNestedGroupsMatchSlog(t *testing.T) {
	var buf bytes.Buffer
	rec := flightrec.New(flightrec.Options{
		Next:                  slog.NewJSONHandler(&buf, nil),
		PassLeveler:           slog.LevelWarn,
		BufferLeveler:         slog.LevelDebug,
		DisableHealthySummary: true,
	})
	l := slog.New(rec).With("a", "b").WithGroup("G").With("c", "d").WithGroup("H")

	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "t1"})
	l.WarnContext(ctx, "immediate warn", "e", "f")
	l.DebugContext(ctx, "buffered debug", "e", "f")
	flightrec.Flush(ctx, "force")
	sc.End(ctx, 200)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines (1 warn + 1 flushed debug), got %d: %s", len(lines), buf.String())
	}
	for idx, raw := range lines {
		if strings.Count(raw, `"G":{`) != 1 {
			t.Errorf("line %d has duplicate G key in raw JSON: %s", idx, raw)
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatalf("line %d invalid JSON: %v", idx, err)
		}
		g, _ := m["G"].(map[string]any)
		if g == nil || g["c"] != "d" {
			t.Errorf("line %d lost G.c='d': parsed G=%+v (raw=%s)", idx, g, raw)
		}
		h, _ := g["H"].(map[string]any)
		if h == nil || h["e"] != "f" {
			t.Errorf("line %d lost G.H.e='f': parsed G=%+v (raw=%s)", idx, g, raw)
		}
	}
}

type scopedTestHandler struct {
	rec   *flightrec.Recorder
	sub   slog.Handler
	flush bool
}

func (s *scopedTestHandler) target() slog.Handler {
	if s.sub != nil {
		return s.sub
	}
	return s.rec
}

func (s *scopedTestHandler) Enabled(ctx context.Context, level slog.Level) bool {
	ctx, sc := flightrec.StartScope(ctx, s.rec, flightrec.ScopeMeta{TraceID: "slogtest"})
	defer sc.End(ctx, 200)
	return s.target().Enabled(ctx, level)
}

func (s *scopedTestHandler) Handle(ctx context.Context, r slog.Record) error {
	ctx, sc := flightrec.StartScope(ctx, s.rec, flightrec.ScopeMeta{TraceID: "slogtest"})
	err := s.target().Handle(ctx, r)
	if s.flush {
		flightrec.Flush(ctx, "slogtest_flush")
	}
	sc.End(ctx, 200)
	return err
}

func (s *scopedTestHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &scopedTestHandler{
		rec:   s.rec,
		sub:   s.target().WithAttrs(attrs),
		flush: s.flush,
	}
}

func (s *scopedTestHandler) WithGroup(name string) slog.Handler {
	return &scopedTestHandler{
		rec:   s.rec,
		sub:   s.target().WithGroup(name),
		flush: s.flush,
	}
}

func parseJSONLines(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	var ms []map[string]any
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("unmarshal %q: %v", line, err)
		}
		ms = append(ms, m)
	}
	return ms
}

func TestSlogtestCompliancePassThrough(t *testing.T) {
	t.Run("out-of-scope pass-through", func(t *testing.T) {
		var buf bytes.Buffer
		rec := flightrec.New(flightrec.Options{
			Next:          slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}),
			PassLeveler:   slog.LevelDebug,
			BufferLeveler: slog.LevelDebug,
		})
		results := func() []map[string]any {
			return parseJSONLines(t, buf.Bytes())
		}
		if err := slogtest.TestHandler(rec, results); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("in-scope immediate pass-through", func(t *testing.T) {
		var buf bytes.Buffer
		rec := flightrec.New(flightrec.Options{
			Next:                  slog.NewJSONHandler(&buf, nil),
			PassLeveler:           slog.LevelDebug,
			BufferLeveler:         slog.LevelDebug,
			DisableHealthySummary: true,
		})
		h := &scopedTestHandler{rec: rec, flush: false}
		results := func() []map[string]any {
			return parseJSONLines(t, buf.Bytes())
		}
		if err := slogtest.TestHandler(h, results); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("in-scope buffered and flushed", func(t *testing.T) {
		var buf bytes.Buffer
		rec := flightrec.New(flightrec.Options{
			Next:                  slog.NewJSONHandler(&buf, nil),
			PassLeveler:           slog.LevelError + 4,
			BufferLeveler:         slog.LevelDebug,
			DisableHealthySummary: true,
		})
		h := &scopedTestHandler{rec: rec, flush: true}
		results := func() []map[string]any {
			return parseJSONLines(t, buf.Bytes())
		}
		if err := slogtest.TestHandler(h, results); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMaxTotalLongLivedScopes(t *testing.T) {
	const maxPerScope = 4 << 10 // 4 KB
	const maxTotal = 16 << 10   // 16 KB
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(io.Discard, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
		MaxPerScope:   maxPerScope,
		MaxTotal:      maxTotal,
	})
	logger := slog.New(rec)

	const numScopes = 50
	ctxs := make([]context.Context, numScopes)
	scopes := make([]*flightrec.Scope, numScopes)
	for i := 0; i < numScopes; i++ {
		ctxs[i], scopes[i] = flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
	}
	defer func() {
		for i := range scopes {
			scopes[i].End(ctxs[i], 200)
		}
	}()

	for round := 0; round < 40; round++ {
		for i := 0; i < numScopes; i++ {
			logger.DebugContext(ctxs[i], "streaming chunk", "round", round, "scope", i, "payload", "0123456789abcdef0123456789abcdef")
			st := rec.Stats()
			if st.BufferedBytes > maxTotal {
				t.Fatalf("round %d scope %d: MaxTotal violated: BufferedBytes=%d > MaxTotal=%d",
					round, i, st.BufferedBytes, maxTotal)
			}
		}
	}
}

func TestIdleScopeMemoryAndSlotAccounting(t *testing.T) {
	t.Run("idle scopes allocate lazily", func(t *testing.T) {
		rec := flightrec.New(flightrec.Options{
			Next:          slog.NewJSONHandler(io.Discard, nil),
			PassLeveler:   slog.LevelWarn,
			BufferLeveler: slog.LevelDebug,
			MaxPerScope:   256 << 10,
			MaxTotal:      64 << 20,
		})

		runtime.GC()
		var mBefore runtime.MemStats
		runtime.ReadMemStats(&mBefore)

		const idleScopes = 2000
		ctxs := make([]context.Context, idleScopes)
		scopes := make([]*flightrec.Scope, idleScopes)
		for i := 0; i < idleScopes; i++ {
			ctxs[i], scopes[i] = flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "idle"})
		}

		runtime.GC()
		var mAfter runtime.MemStats
		runtime.ReadMemStats(&mAfter)

		var heapDelta int64
		if mAfter.HeapAlloc > mBefore.HeapAlloc {
			heapDelta = int64(mAfter.HeapAlloc - mBefore.HeapAlloc)
		}
		runtime.KeepAlive(ctxs)
		runtime.KeepAlive(scopes)

		for i := range scopes {
			scopes[i].End(ctxs[i], 200)
		}

		bytesPerIdleScope := heapDelta / idleScopes
		if bytesPerIdleScope > 1024 {
			t.Fatalf("idle StartScope allocated ~%d B/scope (total %d KB for %d idle scopes); ring buffer is not lazily allocated",
				bytesPerIdleScope, heapDelta>>10, idleScopes)
		}
	})

	t.Run("active scopes account for ring slot capacity", func(t *testing.T) {
		rec := flightrec.New(flightrec.Options{
			Next:          slog.NewJSONHandler(io.Discard, nil),
			PassLeveler:   slog.LevelWarn,
			BufferLeveler: slog.LevelDebug,
			MaxPerScope:   256 << 10,
			MaxTotal:      64 << 20,
		})
		logger := slog.New(rec)

		runtime.GC()
		var mBefore runtime.MemStats
		runtime.ReadMemStats(&mBefore)

		const activeScopes = 200
		ctxs := make([]context.Context, activeScopes)
		scopes := make([]*flightrec.Scope, activeScopes)
		for i := 0; i < activeScopes; i++ {
			ctxs[i], scopes[i] = flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "active"})
			for j := 0; j < 20; j++ {
				logger.DebugContext(ctxs[i], "db query executed", "table", "orders", "rows", j, "cached", true)
			}
		}

		runtime.GC()
		var mAfter runtime.MemStats
		runtime.ReadMemStats(&mAfter)

		accounted := rec.Stats().BufferedBytes
		var realDelta int64
		if mAfter.HeapAlloc > mBefore.HeapAlloc {
			realDelta = int64(mAfter.HeapAlloc - mBefore.HeapAlloc)
		}
		runtime.KeepAlive(ctxs)
		runtime.KeepAlive(scopes)

		for i := range scopes {
			scopes[i].End(ctxs[i], 200)
		}

		if accounted == 0 || realDelta > accounted*3/2 {
			t.Fatalf("real heap delta (%d B) exceeds 1.5x accounted BufferedBytes (%d B)", realDelta, accounted)
		}
	})
}

func TestLogAfterScopeEndPassedThrough(t *testing.T) {
	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		Next:                  slog.NewJSONHandler(out, nil),
		PassLeveler:           slog.LevelWarn,
		BufferLeveler:         slog.LevelDebug,
		DisableHealthySummary: true,
	})
	logger := slog.New(rec)

	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "closed-scope"})
	sc.End(ctx, 200)

	beforeStats := rec.Stats()
	logger.InfoContext(ctx, "late goroutine info")
	logger.DebugContext(ctx, "late goroutine debug")
	afterStats := rec.Stats()

	lines := out.Lines(t)
	passedThroughDelta := afterStats.RecordsPassedThrough - beforeStats.RecordsPassedThrough
	if len(lines) != 2 || passedThroughDelta != 2 {
		t.Fatalf("records logged after Scope.End were not passed through and counted: lines=%d, delta=%d",
			len(lines), passedThroughDelta)
	}
	if lines[0]["flightrec.trace_id"] != "closed-scope" {
		t.Fatalf("expected flightrec.trace_id='closed-scope' on post-End log, got %+v", lines[0])
	}
}

func TestLevelFilteringInAndOutOfScope(t *testing.T) {
	t.Run("out-of-scope Info log reaches Next at default Info level", func(t *testing.T) {
		out := &syncBuffer{}
		rec := flightrec.New(flightrec.Options{
			Next:          slog.NewJSONHandler(out, nil), // default LevelInfo
			PassLeveler:   slog.LevelWarn,
			BufferLeveler: slog.LevelDebug,
		})
		l := slog.New(rec)

		l.Info("server listening", "addr", ":8080")
		l.InfoContext(context.Background(), "nightly job finished", "rows", 1200)
		l.Debug("out-of-scope debug filtered by Next")

		lines := out.Lines(t)
		if len(lines) != 2 {
			t.Fatalf("expected 2 out-of-scope Info lines, got %d: %+v", len(lines), lines)
		}
		if lines[0]["msg"] != "server listening" || lines[1]["msg"] != "nightly job finished" {
			t.Fatalf("unexpected out-of-scope lines: %+v", lines)
		}
	})

	t.Run("in-scope flushed Debug records are written even when Next is at Info", func(t *testing.T) {
		out := &syncBuffer{}
		rec := flightrec.New(flightrec.Options{
			Next:                  slog.NewJSONHandler(out, nil), // default LevelInfo
			PassLeveler:           slog.LevelWarn,
			BufferLeveler:         slog.LevelDebug,
			DisableHealthySummary: true,
		})
		l := slog.New(rec)

		ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "s1"})
		l.DebugContext(ctx, "step detail")
		sc.End(ctx, 500)

		lines := out.Lines(t)
		if len(lines) != 1 || lines[0]["msg"] != "step detail" {
			t.Fatalf("expected flushed Debug record when Next is at Info, got %+v", lines)
		}
	})
}

func TestLevelerOptionsAndDefaults(t *testing.T) {
	t.Run("setting only BufferLevel defaults PassLevel to Warn", func(t *testing.T) {
		out := &syncBuffer{}
		rec := flightrec.New(flightrec.Options{
			Next:                  slog.NewJSONHandler(out, nil),
			BufferLevel:           slog.LevelDebug,
			DisableHealthySummary: true,
		})
		logger := slog.New(rec)
		ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "t6a"})
		logger.InfoContext(ctx, "info should be buffered, not passed through immediately")
		if len(out.Lines(t)) != 0 {
			t.Fatalf("expected Info to be buffered when PassLevel is unset (default Warn), but it was written immediately: %+v", out.Lines(t))
		}
		sc.End(ctx, 200)
	})

	t.Run("BufferLeveler set to LevelInfo drops Debug and buffers Info", func(t *testing.T) {
		out := &syncBuffer{}
		rec := flightrec.New(flightrec.Options{
			Next:                  slog.NewJSONHandler(out, nil),
			PassLeveler:           slog.LevelWarn,
			BufferLeveler:         slog.LevelInfo,
			DisableHealthySummary: true,
		})
		logger := slog.New(rec)
		ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "t6b"})
		logger.DebugContext(ctx, "debug should be ignored below BufferLeveler=Info")
		logger.InfoContext(ctx, "info should be buffered")
		flightrec.Flush(ctx, "force")
		sc.End(ctx, 200)

		lines := out.Lines(t)
		if len(lines) != 1 || lines[0]["msg"] != "info should be buffered" {
			t.Fatalf("expected only Info record to be buffered and flushed, got %+v", lines)
		}
	})
}

type mutableOrder struct {
	State string
}

func (o *mutableOrder) LogValue() slog.Value {
	return slog.StringValue(o.State)
}

func TestLogValuerResolvedAtBufferTime(t *testing.T) {
	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		Next:                  slog.NewJSONHandler(out, nil),
		PassLeveler:           slog.LevelWarn,
		BufferLeveler:         slog.LevelDebug,
		DisableHealthySummary: true,
	})

	ordAttr := &mutableOrder{State: "created"}
	logger := slog.New(rec).With("with_order", ordAttr)
	ordAttr.State = "mutated_after_with"

	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "t7"})
	ord := &mutableOrder{State: "pending"}
	grpOrd := &mutableOrder{State: "queued"}
	logger.DebugContext(ctx, "order created", "order", ord, slog.Group("nested", "grp_order", grpOrd))
	ord.State = "paid"
	grpOrd.State = "shipped"
	flightrec.Flush(ctx, "force")
	sc.End(ctx, 200)

	lines := out.Lines(t)
	if len(lines) != 1 || lines[0]["order"] != "pending" || lines[0]["with_order"] != "created" {
		t.Fatalf("expected order='pending' and with_order='created' captured at log time, got %+v", lines)
	}
	nested, _ := lines[0]["nested"].(map[string]any)
	if nested == nil || nested["grp_order"] != "queued" {
		t.Fatalf("expected nested.grp_order='queued' captured at log time, got %+v", lines[0])
	}
}

type fakeGRPCStatus struct{ code uint32 }

func (s *fakeGRPCStatus) Code() uint32 { return s.code }

type fakeGRPCError struct{ code uint32 }

func (e *fakeGRPCError) Error() string               { return "grpc error" }
func (e *fakeGRPCError) GRPCStatus() *fakeGRPCStatus { return &fakeGRPCStatus{code: e.code} }

func TestGRPCErrorClassificationAndStream(t *testing.T) {
	t.Run("NotFound client error does not flush and logs grpc.code=5", func(t *testing.T) {
		out := &syncBuffer{}
		rec := flightrec.New(flightrec.Options{
			Next:          slog.NewJSONHandler(out, nil),
			PassLeveler:   slog.LevelWarn,
			BufferLeveler: slog.LevelDebug,
		})
		logger := slog.New(rec)

		inCtx := flightrec.WithIncomingMetadata(context.Background(), map[string][]string{
			"x-trace-id": {"grpc-meta-trace-42"},
		})

		_, err := flightrec.InvokeGRPCUnary(
			inCtx,
			rec,
			"/orders.Service/GetOrder",
			nil,
			nil,
			func(ctx context.Context, _ any) (any, error) {
				logger.DebugContext(ctx, "lookup order in cache")
				return nil, &fakeGRPCError{code: 5} // codes.NotFound = 5
			},
		)
		if err == nil {
			t.Fatal("expected error")
		}
		lines := out.Lines(t)
		if len(lines) != 1 {
			t.Fatalf("expected 1 healthy summary line, got %+v", lines)
		}
		if lines[0]["msg"] != "request completed" || lines[0]["grpc.code"] != float64(5) || lines[0]["flightrec.trace_id"] != "grpc-meta-trace-42" {
			t.Fatalf("unexpected gRPC summary line: %+v", lines[0])
		}
		if _, hasHTTPStatus := lines[0]["status"]; hasHTTPStatus {
			t.Fatalf("gRPC summary should not emit HTTP status attribute: %+v", lines[0])
		}
	})

	t.Run("streaming server fault flushes buffer", func(t *testing.T) {
		out := &syncBuffer{}
		rec := flightrec.New(flightrec.Options{
			Next:          slog.NewJSONHandler(out, nil),
			PassLeveler:   slog.LevelWarn,
			BufferLeveler: slog.LevelDebug,
		})
		logger := slog.New(rec)

		err := flightrec.InvokeGRPCStream(
			flightrec.WithIncomingMetadata(context.Background(), map[string][]string{"x-request-id": {"stream-99"}}),
			rec,
			"/orders.Service/WatchOrders",
			nil,
			func(ctx context.Context) error {
				logger.DebugContext(ctx, "stream frame 1")
				return &fakeGRPCError{code: 13} // codes.Internal = 13
			},
		)
		if err == nil {
			t.Fatal("expected error")
		}
		lines := out.Lines(t)
		if len(lines) != 1 || lines[0]["msg"] != "stream frame 1" || lines[0]["flightrec.trace_id"] != "stream-99" {
			t.Fatalf("expected flushed stream log, got %+v", lines)
		}
	})
}

type readerFromRecorder struct {
	*httptest.ResponseRecorder
	readFromCalled bool
}

func (r *readerFromRecorder) ReadFrom(src io.Reader) (int64, error) {
	r.readFromCalled = true
	return io.Copy(r.ResponseRecorder, src)
}

type hijackRecorder struct {
	*httptest.ResponseRecorder
}

func (h *hijackRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, nil
}

func TestResponseWriterInterfacePreservation(t *testing.T) {
	rec := flightrec.New(flightrec.Options{
		Next: slog.NewJSONHandler(io.Discard, nil),
	})

	t.Run("ResponseRecorder does not claim Hijacker or Pusher", func(t *testing.T) {
		var claimedHijacker, claimedPusher bool
		mw := flightrec.HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, claimedHijacker = w.(http.Hijacker)
			_, claimedPusher = w.(http.Pusher)
			w.WriteHeader(http.StatusOK)
		}))
		mw.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/test", nil))
		if claimedHijacker || claimedPusher {
			t.Fatalf("ResponseWriter falsely claimed Hijacker=%v Pusher=%v", claimedHijacker, claimedPusher)
		}
	})

	t.Run("preserves io.ReaderFrom and http.Hijacker when present", func(t *testing.T) {
		rfRec := &readerFromRecorder{ResponseRecorder: httptest.NewRecorder()}
		mw := flightrec.HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rf, ok := w.(io.ReaderFrom)
			if !ok {
				t.Fatal("expected io.ReaderFrom to be preserved")
			}
			_, _ = rf.ReadFrom(strings.NewReader("hello sendfile"))
		}))
		mw.ServeHTTP(rfRec, httptest.NewRequest(http.MethodGet, "/file", nil))
		if !rfRec.readFromCalled || rfRec.Body.String() != "hello sendfile" {
			t.Fatalf("io.ReaderFrom was not delegated properly: called=%v body=%q", rfRec.readFromCalled, rfRec.Body.String())
		}

		hjRec := &hijackRecorder{ResponseRecorder: httptest.NewRecorder()}
		var hasHijacker bool
		mwHj := flightrec.HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, hasHijacker = w.(http.Hijacker)
		}))
		mwHj.ServeHTTP(hjRec, httptest.NewRequest(http.MethodGet, "/ws", nil))
		if !hasHijacker {
			t.Fatal("expected http.Hijacker to be preserved when underlying ResponseWriter implements it")
		}
	})
}

func TestErrAbortHandlerNotPanicFlush(t *testing.T) {
	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		Next:                  slog.NewJSONHandler(out, nil),
		PassLeveler:           slog.LevelWarn,
		BufferLeveler:         slog.LevelDebug,
		DisableHealthySummary: true,
	})
	logger := slog.New(rec)

	mw := flightrec.HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.DebugContext(r.Context(), "streaming chunk before abort")
		panic(http.ErrAbortHandler)
	}))

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		mw.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/stream", nil))
	}()

	if recovered != http.ErrAbortHandler {
		t.Fatalf("expected http.ErrAbortHandler to be re-panicked, got %v", recovered)
	}
	if len(out.Lines(t)) != 0 {
		t.Fatalf("http.ErrAbortHandler triggered a panic flush: %+v", out.Lines(t))
	}
}

func TestConcurrentStressWithRaceDetector(t *testing.T) {
	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(out, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
		MaxPerScope:   4 << 10,
		MaxTotal:      64 << 10,
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

type testObserver struct {
	mu        sync.Mutex
	started   []string
	triggered []string
	ended     []string
}

func (o *testObserver) OnScopeStart(_ context.Context, _ *flightrec.Scope, traceID string) {
	o.mu.Lock()
	o.started = append(o.started, traceID)
	o.mu.Unlock()
}

func (o *testObserver) OnScopeTrigger(_ *flightrec.Scope, traceID, reason string) {
	o.mu.Lock()
	o.triggered = append(o.triggered, traceID+":"+reason)
	o.mu.Unlock()
}

func (o *testObserver) OnScopeEnd(_ *flightrec.Scope, traceID string, triggered bool, reason string) {
	o.mu.Lock()
	o.ended = append(o.ended, traceID+":"+reason)
	o.mu.Unlock()
}

func TestTraceBridgeAndScopeObserver(t *testing.T) {
	type spanCtxKey struct{}
	wantTraceID, ok := flightrec.ParseTraceIDHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if !ok {
		t.Fatal("expected valid 32-hex TraceID")
	}
	wantSpanID := [8]byte{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7}

	var injectedTrace [16]byte
	var injectedSpan [8]byte

	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(out, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
		TraceExtractor: func(ctx context.Context) flightrec.TraceContext {
			if tc, ok := ctx.Value(spanCtxKey{}).(flightrec.TraceContext); ok {
				return tc
			}
			return flightrec.TraceContext{}
		},
		TraceInjector: func(ctx context.Context, tc flightrec.TraceContext) context.Context {
			injectedTrace = tc.TraceID
			injectedSpan = tc.SpanID
			return ctx
		},
	})
	obs := &testObserver{}
	rec.AttachObserver(obs)
	rec.AttachObserver(obs) // duplicate registration must be ignored

	logger := slog.New(rec)

	baseCtx := context.WithValue(context.Background(), spanCtxKey{}, flightrec.TraceContext{
		TraceID: wantTraceID,
		SpanID:  wantSpanID,
	})

	ctx, sc := flightrec.StartScope(baseCtx, rec, flightrec.ScopeMeta{})
	if sc.TraceID() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("expected scope to extract TraceID from TraceExtractor, got %q", sc.TraceID())
	}

	logger.DebugContext(ctx, "buffered with span", "step", 1)
	flightrec.Flush(ctx, "custom_reason")
	sc.End(ctx, 200)

	lines := out.Lines(t)
	if len(lines) != 1 {
		t.Fatalf("expected 1 flushed line, got %d", len(lines))
	}
	if lines[0]["flightrec.trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("unexpected trace_id: %v", lines[0]["flightrec.trace_id"])
	}
	if lines[0]["flightrec.span_id"] != "00f067aa0ba902b7" {
		t.Fatalf("expected flightrec.span_id=00f067aa0ba902b7, got %v", lines[0]["flightrec.span_id"])
	}
	if injectedTrace != wantTraceID || injectedSpan != wantSpanID {
		t.Fatalf("expected TraceInjector to receive log-time TraceID/SpanID")
	}

	if len(obs.started) != 1 || len(obs.triggered) != 1 || len(obs.ended) != 1 {
		t.Fatalf("unexpected observer counts: started=%v triggered=%v ended=%v", obs.started, obs.triggered, obs.ended)
	}
}
