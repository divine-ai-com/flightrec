// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package otelflightrec_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/slogtest"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/divine-ai-com/flightrec"
	"github.com/divine-ai-com/flightrec/otelflightrec"
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

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func parseJSONLines(t *testing.T, raw string) []map[string]any {
	t.Helper()
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	lines := strings.Split(raw, "\n")
	out := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("failed to unmarshal JSON line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func spanAttrMap(s tracetest.SpanStub) map[string]any {
	m := make(map[string]any, len(s.Attributes))
	for _, kv := range s.Attributes {
		m[string(kv.Key)] = kv.Value.AsInterface()
	}
	return m
}

func TestHealthyTracePolicies(t *testing.T) {
	t.Run("HealthyExportRootOnly exports only root span with dropped count", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
			Next:          sdktrace.NewSimpleSpanProcessor(exporter),
			HealthyPolicy: otelflightrec.HealthyExportRootOnly,
		})
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
		defer func() { _ = tp.Shutdown(context.Background()) }()
		tracer := tp.Tracer("test")

		ctx, root := tracer.Start(context.Background(), "HTTP GET /items")
		for i := 0; i < 4; i++ {
			_, child := tracer.Start(ctx, fmt.Sprintf("db.query.%d", i))
			child.SetAttributes(attribute.Int("query.index", i))
			child.End()
		}
		root.End()

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 exported span (root only), got %d", len(spans))
		}
		if spans[0].Name != "HTTP GET /items" {
			t.Fatalf("expected root span, got %q", spans[0].Name)
		}
		attrs := spanAttrMap(spans[0])
		if got, _ := attrs["flightrec.dropped_spans"].(int64); got != 4 {
			t.Fatalf("expected flightrec.dropped_spans=4 on root span, got %v", attrs["flightrec.dropped_spans"])
		}

		st := sp.Stats()
		if st.ActiveTraces != 0 || st.BufferedBytes != 0 {
			t.Fatalf("expected 0 active traces and 0 buffered bytes, got %+v", st)
		}
		if st.TracesStarted != 1 || st.TracesHealthyDropped != 1 || st.TracesFlushed != 0 {
			t.Fatalf("unexpected trace stats: %+v", st)
		}
		if st.SpansBuffered != 5 || st.SpansExported != 1 || st.SpansHealthyDropped != 4 {
			t.Fatalf("unexpected span stats: %+v", st)
		}
	})

	t.Run("HealthyExportNone exports zero spans", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
			Next:          sdktrace.NewSimpleSpanProcessor(exporter),
			HealthyPolicy: otelflightrec.HealthyExportNone,
		})
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
		defer func() { _ = tp.Shutdown(context.Background()) }()
		tracer := tp.Tracer("test")

		ctx, root := tracer.Start(context.Background(), "HTTP GET /health")
		for i := 0; i < 3; i++ {
			_, child := tracer.Start(ctx, "cache.get")
			child.End()
		}
		root.End()

		spans := exporter.GetSpans()
		if len(spans) != 0 {
			t.Fatalf("expected 0 exported spans under HealthyExportNone, got %d", len(spans))
		}
		st := sp.Stats()
		if st.SpansExported != 0 || st.SpansHealthyDropped != 4 || st.TracesHealthyDropped != 1 {
			t.Fatalf("unexpected stats under HealthyExportNone: %+v", st)
		}
	})
}

func TestSpanErrorFlushesTraceAndLogsWithChildSpanIDs(t *testing.T) {
	logBuf := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(logBuf, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})
	logger := slog.New(rec)

	exporter := tracetest.NewInMemoryExporter()
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
		Next:     sdktrace.NewSimpleSpanProcessor(exporter),
		Recorder: rec,
	})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("checkout")

	ctx, root := tracer.Start(context.Background(), "POST /checkout")
	ctx, scope := flightrec.StartScope(ctx, rec, flightrec.ScopeMeta{
		Method: "POST",
		Path:   "/checkout",
	})

	rootTraceID := root.SpanContext().TraceID().String()
	if scope.TraceID() != rootTraceID {
		t.Fatalf("expected scope TraceID %q to match OTel root TraceID %q", scope.TraceID(), rootTraceID)
	}

	c1Ctx, child1 := tracer.Start(ctx, "cart.load")
	c1SpanID := child1.SpanContext().SpanID().String()
	logger.DebugContext(c1Ctx, "loaded cart items", "count", 3)
	child1.End()

	c2Ctx, child2 := tracer.Start(ctx, "db.reserve")
	c2SpanID := child2.SpanContext().SpanID().String()
	logger.InfoContext(c2Ctx, "reserving inventory", "sku", "ABC-1")
	child2.SetStatus(codes.Error, "deadlock detected")
	child2.End()

	c3Ctx, child3 := tracer.Start(ctx, "rollback")
	c3SpanID := child3.SpanContext().SpanID().String()
	logger.DebugContext(c3Ctx, "rolling back reservation")
	child3.End()

	// Scope ends with HTTP 200, but child2 triggered "span_error" so both
	// logs and spans must flush!
	scope.End(ctx, http.StatusOK)
	root.End()

	spans := exporter.GetSpans()
	if len(spans) != 4 {
		t.Fatalf("expected all 4 spans exported, got %d", len(spans))
	}
	wantNames := []string{"cart.load", "db.reserve", "rollback", "POST /checkout"}
	for i, want := range wantNames {
		if spans[i].Name != want {
			t.Fatalf("span[%d] name = %q, want %q", i, spans[i].Name, want)
		}
		attrs := spanAttrMap(spans[i])
		if attrs["flightrec.flush_reason"] != "span_error" {
			t.Fatalf("span[%d] flightrec.flush_reason = %v, want span_error", i, attrs["flightrec.flush_reason"])
		}
	}

	logs := parseJSONLines(t, logBuf.String())
	if len(logs) != 3 {
		t.Fatalf("expected 3 flushed log records, got %d: %s", len(logs), logBuf.String())
	}
	wantSpanIDs := []string{c1SpanID, c2SpanID, c3SpanID}
	for i, l := range logs {
		if l["flightrec.trace_id"] != rootTraceID {
			t.Fatalf("log[%d] flightrec.trace_id = %v, want %q", i, l["flightrec.trace_id"], rootTraceID)
		}
		if l["flightrec.span_id"] != wantSpanIDs[i] {
			t.Fatalf("log[%d] flightrec.span_id = %v, want child span ID %q", i, l["flightrec.span_id"], wantSpanIDs[i])
		}
		if l["flightrec.flush_reason"] != "span_error" {
			t.Fatalf("log[%d] flightrec.flush_reason = %v, want span_error", i, l["flightrec.flush_reason"])
		}
	}
}

func TestExceptionAndSlowTraceTriggers(t *testing.T) {
	t.Run("exception event triggers trace flush", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
			Next: sdktrace.NewSimpleSpanProcessor(exporter),
		})
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
		defer func() { _ = tp.Shutdown(context.Background()) }()
		tracer := tp.Tracer("test")

		ctx, root := tracer.Start(context.Background(), "root")
		_, child := tracer.Start(ctx, "child")
		child.RecordError(errors.New("unexpected EOF"))
		child.End()
		root.End()

		spans := exporter.GetSpans()
		if len(spans) != 2 {
			t.Fatalf("expected 2 spans flushed on exception event, got %d", len(spans))
		}
		if got := spanAttrMap(spans[0])["flightrec.flush_reason"]; got != "span_exception" {
			t.Fatalf("expected flush_reason=span_exception, got %v", got)
		}
	})

	t.Run("slow trace triggers flush", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
			Next:      sdktrace.NewSimpleSpanProcessor(exporter),
			SlowAfter: 10 * time.Millisecond,
		})
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
		defer func() { _ = tp.Shutdown(context.Background()) }()
		tracer := tp.Tracer("test")

		t0 := time.Now()
		ctx, root := tracer.Start(context.Background(), "root", trace.WithTimestamp(t0))
		_, child := tracer.Start(ctx, "child", trace.WithTimestamp(t0))
		child.End(trace.WithTimestamp(t0.Add(2 * time.Millisecond)))
		root.End(trace.WithTimestamp(t0.Add(25 * time.Millisecond)))

		spans := exporter.GetSpans()
		if len(spans) != 2 {
			t.Fatalf("expected 2 spans flushed on slow trace, got %d", len(spans))
		}
		if got := spanAttrMap(spans[1])["flightrec.flush_reason"]; got != "slow_trace" {
			t.Fatalf("expected flush_reason=slow_trace, got %v", got)
		}
	})
}

// TestOutboundSpanErrors checks which errors on outbound spans trigger: a
// remote that answered with a client error does not, while a server fault, a
// failure with no answer, and errors on other span kinds do.
func TestOutboundSpanErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		kind    trace.SpanKind
		attrs   []attribute.KeyValue
		status  bool // set codes.Error
		event   bool // record an exception event
		trigger bool
	}{
		{"grpc NotFound", trace.SpanKindClient, []attribute.KeyValue{attribute.Int("rpc.grpc.status_code", 5)}, true, true, false},
		{"grpc NotFound status only", trace.SpanKindClient, []attribute.KeyValue{attribute.Int("rpc.grpc.status_code", 5)}, true, false, false},
		{"grpc NotFound event only", trace.SpanKindClient, []attribute.KeyValue{attribute.Int("rpc.grpc.status_code", 5)}, false, true, false},
		{"http 404", trace.SpanKindClient, []attribute.KeyValue{attribute.Int("http.response.status_code", 404)}, true, false, false},
		{"grpc Unavailable", trace.SpanKindClient, []attribute.KeyValue{attribute.Int("rpc.grpc.status_code", 14)}, true, false, true},
		{"http 503", trace.SpanKindClient, []attribute.KeyValue{attribute.Int("http.response.status_code", 503)}, true, false, true},
		{"no answer (connection refused, database)", trace.SpanKindClient, nil, true, true, true},
		{"producer publish failed", trace.SpanKindProducer, nil, true, false, true},
		{"internal error", trace.SpanKindInternal, []attribute.KeyValue{attribute.Int("rpc.grpc.status_code", 5)}, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
				Next: sdktrace.NewSimpleSpanProcessor(exporter),
			})
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
			defer func() { _ = tp.Shutdown(context.Background()) }()
			tracer := tp.Tracer("test")

			ctx, root := tracer.Start(context.Background(), "root", trace.WithSpanKind(trace.SpanKindServer))
			_, call := tracer.Start(ctx, "call", trace.WithSpanKind(tc.kind), trace.WithAttributes(tc.attrs...))
			if tc.event {
				call.RecordError(errors.New("call failed"))
			}
			if tc.status {
				call.SetStatus(codes.Error, "call failed")
			}
			call.End()
			root.End()

			want := 1 // healthy: the root only
			if tc.trigger {
				want = 2
			}
			if got := len(exporter.GetSpans()); got != want {
				t.Fatalf("%d spans exported, want %d", got, want)
			}
		})
	}
}

func TestBothHTTPMiddlewareOrdersCoordinateLogsAndTraces(t *testing.T) {
	for _, tc := range []struct{ order, requestID string }{
		{"otel_outer", ""},
		{"flightrec_outer", ""},
		// A proxy-added X-Request-Id must not split logs from spans (#96).
		{"otel_outer", "lb-req-1"},
		{"flightrec_outer", "lb-req-1"},
	} {
		order := tc.order
		t.Run(order+"/"+tc.requestID, func(t *testing.T) {
			logBuf := &syncBuffer{}
			rec := flightrec.New(flightrec.Options{
				Next:          slog.NewJSONHandler(logBuf, nil),
				PassLeveler:   slog.LevelWarn,
				BufferLeveler: slog.LevelDebug,
			})
			logger := slog.New(rec)

			exporter := tracetest.NewInMemoryExporter()
			sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
				Next:     sdktrace.NewSimpleSpanProcessor(exporter),
				Recorder: rec,
			})
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
			defer func() { _ = tp.Shutdown(context.Background()) }()
			tracer := tp.Tracer("http-test")

			otelMiddleware := func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ctx, span := tracer.Start(r.Context(), r.Method+" "+r.URL.Path)
					defer span.End()
					next.ServeHTTP(w, r.WithContext(ctx))
				})
			}

			baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := r.Context()
				cCtx, child := tracer.Start(ctx, "service.step")
				logger.DebugContext(cCtx, "executing step", "order", order)
				child.End()
				// Trigger via HTTP 503 without setting OTel span status to Error.
				w.WriteHeader(http.StatusServiceUnavailable)
			})

			var handler http.Handler
			if order == "otel_outer" {
				handler = otelMiddleware(flightrec.HTTPMiddleware(rec)(baseHandler))
			} else {
				handler = flightrec.HTTPMiddleware(rec)(otelMiddleware(baseHandler))
			}

			req := httptest.NewRequest(http.MethodPost, "/pay", nil)
			if tc.requestID != "" {
				req.Header.Set("X-Request-Id", tc.requestID)
			}
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503, got %d", rr.Code)
			}

			spans := exporter.GetSpans()
			if len(spans) != 2 {
				t.Fatalf("expected both child and root spans exported in %s, got %d", order, len(spans))
			}
			rootTraceID := spans[1].SpanContext.TraceID().String()
			for i, s := range spans {
				if got := spanAttrMap(s)["flightrec.flush_reason"]; got != "http_503" {
					t.Fatalf("span[%d] flush_reason = %v, want http_503", i, got)
				}
			}

			logs := parseJSONLines(t, logBuf.String())
			if len(logs) != 1 {
				t.Fatalf("expected 1 flushed debug log in %s, got %d: %s", order, len(logs), logBuf.String())
			}
			if logs[0]["flightrec.trace_id"] != rootTraceID {
				t.Fatalf("log trace_id = %v, want OTel root trace_id %q in %s", logs[0]["flightrec.trace_id"], rootTraceID, order)
			}
			if logs[0]["flightrec.flush_reason"] != "http_503" {
				t.Fatalf("log flush_reason = %v, want http_503", logs[0]["flightrec.flush_reason"])
			}
		})
	}
}

func TestPerTraceAndTotalMemoryCaps(t *testing.T) {
	t.Run("per-trace cap drops oldest child spans and preserves root span", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
			Next:             sdktrace.NewSimpleSpanProcessor(exporter),
			MaxBytesPerTrace: 2048,
			MaxTotalBytes:    64 << 20,
		})
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
		defer func() { _ = tp.Shutdown(context.Background()) }()
		tracer := tp.Tracer("cap-test")

		ctx, root := tracer.Start(context.Background(), "root-op")
		for i := 0; i < 40; i++ {
			_, ch := tracer.Start(ctx, fmt.Sprintf("child-%02d", i))
			ch.SetAttributes(attribute.String("payload", strings.Repeat("x", 120)))
			ch.End()
		}
		otelflightrec.FlushTrace(ctx, sp, "force_check")
		root.End()

		spans := exporter.GetSpans()
		if len(spans) == 0 || len(spans) >= 41 {
			t.Fatalf("expected truncated span list (1..40), got %d", len(spans))
		}
		lastSpan := spans[len(spans)-1]
		if lastSpan.Name != "root-op" {
			t.Fatalf("expected root span to be preserved as last exported span, got %q", lastSpan.Name)
		}
		rootAttrs := spanAttrMap(lastSpan)
		if rootAttrs["flightrec.truncated"] != true {
			t.Fatalf("expected flightrec.truncated=true on root span, got %v", rootAttrs)
		}
		dropped, _ := rootAttrs["flightrec.dropped_spans"].(int64)
		if dropped <= 0 {
			t.Fatalf("expected positive flightrec.dropped_spans, got %v", rootAttrs["flightrec.dropped_spans"])
		}
		st := sp.Stats()
		if st.SpansDroppedPerTraceCap != uint64(dropped) {
			t.Fatalf("expected SpansDroppedPerTraceCap=%d, got %d", dropped, st.SpansDroppedPerTraceCap)
		}
	})

	t.Run("MaxTotalBytes enforced across long-lived traces", func(t *testing.T) {
		const (
			maxPerTrace = 16 << 10
			maxTotal    = 32 << 10
			numTraces   = 12
		)
		exporter := tracetest.NewInMemoryExporter()
		sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
			Next:             sdktrace.NewSimpleSpanProcessor(exporter),
			MaxBytesPerTrace: maxPerTrace,
			MaxTotalBytes:    maxTotal,
		})
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
		defer func() { _ = tp.Shutdown(context.Background()) }()
		tracer := tp.Tracer("total-cap-test")

		ctxs := make([]context.Context, numTraces)
		roots := make([]trace.Span, numTraces)
		for i := 0; i < numTraces; i++ {
			ctxs[i], roots[i] = tracer.Start(context.Background(), fmt.Sprintf("root-%d", i))
		}

		for round := 0; round < 3; round++ {
			for i := 0; i < numTraces; i++ {
				for j := 0; j < 15; j++ {
					_, ch := tracer.Start(ctxs[i], "work")
					ch.SetAttributes(attribute.String("data", strings.Repeat("y", 200)))
					ch.End()
				}
				if got := sp.Stats().BufferedBytes; got > maxTotal {
					t.Fatalf("round %d trace %d: BufferedBytes=%d exceeded MaxTotalBytes=%d", round, i, got, maxTotal)
				}
			}
		}

		st := sp.Stats()
		if st.TracesEvictedTotalCap == 0 || st.SpansDroppedTotalCap == 0 {
			t.Fatalf("expected total-cap evictions, got %+v", st)
		}

		for i := range roots {
			roots[i].End()
		}
		if after := sp.Stats(); after.BufferedBytes != 0 || after.ActiveTraces != 0 {
			t.Fatalf("expected 0 BufferedBytes and 0 ActiveTraces after ending all roots, got %+v", after)
		}
	})
}

func TestStaleTraceSweep(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	var nowMu sync.Mutex
	nowFn := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		now = now.Add(d)
		nowMu.Unlock()
	}

	exporter := tracetest.NewInMemoryExporter()
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
		Next:        sdktrace.NewSimpleSpanProcessor(exporter),
		MaxTraceAge: 30 * time.Second,
		Now:         nowFn,
	})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("stale-test")

	// Start a leaked trace whose root span is never ended.
	leakCtx, _ := tracer.Start(context.Background(), "abandoned-root")
	_, leakChild := tracer.Start(leakCtx, "abandoned-child")
	leakChild.End()

	if st := sp.Stats(); st.ActiveTraces != 1 || st.BufferedBytes == 0 {
		t.Fatalf("expected 1 active trace with buffered bytes, got %+v", st)
	}

	advance(45 * time.Second)

	// Starting a new trace triggers the lazy stale sweep.
	_, freshRoot := tracer.Start(context.Background(), "fresh-root")
	freshRoot.End()

	st := sp.Stats()
	if st.ActiveTraces != 0 || st.BufferedBytes != 0 {
		t.Fatalf("expected abandoned trace to be swept and freed, got %+v", st)
	}
	if st.TracesEvictedTotalCap != 1 || st.SpansDroppedTotalCap != 1 {
		t.Fatalf("expected stale eviction to be counted in stats, got %+v", st)
	}
}

type capturedLogRecord struct {
	Timestamp  time.Time
	Severity   otellog.Severity
	Body       string
	TraceID    trace.TraceID
	SpanID     trace.SpanID
	TraceFlags trace.TraceFlags
	Attrs      map[string]any
}

type memoryLogExporter struct {
	mu      sync.Mutex
	records []capturedLogRecord
}

func (e *memoryLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range records {
		attrs := make(map[string]any, r.AttributesLen())
		r.WalkAttributes(func(kv attribute.KeyValue) bool {
			attrs[string(kv.Key)] = attrValueToAny(kv.Value)
			return true
		})
		e.records = append(e.records, capturedLogRecord{
			Timestamp:  r.Timestamp(),
			Severity:   r.Severity(),
			Body:       r.Body().AsString(),
			TraceID:    r.TraceID(),
			SpanID:     r.SpanID(),
			TraceFlags: r.TraceFlags(),
			Attrs:      attrs,
		})
	}
	return nil
}

func (e *memoryLogExporter) Shutdown(context.Context) error   { return nil }
func (e *memoryLogExporter) ForceFlush(context.Context) error { return nil }

func (e *memoryLogExporter) Snapshot() []capturedLogRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]capturedLogRecord, len(e.records))
	copy(out, e.records)
	return out
}

func attrValueToAny(v attribute.Value) any {
	if v.Type() == attribute.MAP {
		m := make(map[string]any)
		for _, kv := range v.AsMap() {
			m[string(kv.Key)] = attrValueToAny(kv.Value)
		}
		return m
	}
	return v.AsInterface()
}

func TestOTelLogHandlerBridgePreservesTraceAndSpanIDs(t *testing.T) {
	logExp := &memoryLogExporter{}
	logProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(logExp)),
	)
	defer func() { _ = logProvider.Shutdown(context.Background()) }()

	otelHandler := otelflightrec.NewLogHandler(otelflightrec.LogHandlerOptions{
		LoggerProvider: logProvider,
	})
	rec := flightrec.New(flightrec.Options{
		Next:          otelHandler,
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})
	logger := slog.New(rec)

	spanExp := tracetest.NewInMemoryExporter()
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
		Next:     sdktrace.NewSimpleSpanProcessor(spanExp),
		Recorder: rec,
	})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("bridge-test")

	ctx, root := tracer.Start(context.Background(), "POST /orders")
	ctx, scope := flightrec.StartScope(ctx, rec, flightrec.ScopeMeta{
		Method: "POST",
		Path:   "/orders",
	})

	s1Ctx, s1 := tracer.Start(ctx, "auth.verify")
	s1SpanID := s1.SpanContext().SpanID()
	logger.WithGroup("auth").With("tenant", "acme").DebugContext(s1Ctx, "token verified", "user_id", 42)
	s1.End()

	s2Ctx, s2 := tracer.Start(ctx, "payment.charge")
	s2SpanID := s2.SpanContext().SpanID()
	logger.ErrorContext(s2Ctx, "card declined", "code", "insufficient_funds")
	s2.End()

	scope.End(ctx, http.StatusPaymentRequired)
	root.End()

	records := logExp.Snapshot()
	if len(records) != 2 {
		t.Fatalf("expected 2 OTel log records (immediate error + flushed debug), got %d", len(records))
	}

	rootTraceID := root.SpanContext().TraceID()

	// Record 0: immediate error log emitted during s2
	if records[0].Body != "card declined" || records[0].Severity != otellog.SeverityError1 {
		t.Fatalf("unexpected record[0]: %+v", records[0])
	}
	if records[0].TraceID != rootTraceID || records[0].SpanID != s2SpanID {
		t.Fatalf("record[0] TraceID/SpanID = %s/%s, want %s/%s",
			records[0].TraceID, records[0].SpanID, rootTraceID, s2SpanID)
	}

	// Record 1: buffered debug log recorded during s1 and flushed at scope.End
	if records[1].Body != "token verified" || records[1].Severity != otellog.SeverityDebug1 {
		t.Fatalf("unexpected record[1]: %+v", records[1])
	}
	if records[1].TraceID != rootTraceID || records[1].SpanID != s1SpanID {
		t.Fatalf("record[1] TraceID/SpanID = %s/%s, want %s/%s (must preserve child span active at log time)",
			records[1].TraceID, records[1].SpanID, rootTraceID, s1SpanID)
	}
	if !records[1].TraceFlags.IsSampled() {
		t.Fatalf("expected flushed record[1] TraceFlags to have FlagsSampled set")
	}
	authGroup, ok := records[1].Attrs["auth"].(map[string]any)
	if !ok || authGroup["tenant"] != "acme" || authGroup["user_id"] != int64(42) {
		t.Fatalf("expected nested auth group with tenant=acme and user_id=42, got %#v", records[1].Attrs)
	}
}

func TestLogHandlerSlogtestCompliance(t *testing.T) {
	logExp := &memoryLogExporter{}
	logProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(logExp)),
	)
	defer func() { _ = logProvider.Shutdown(context.Background()) }()

	var buf bytes.Buffer
	handler := otelflightrec.NewLogHandler(otelflightrec.LogHandlerOptions{
		LoggerProvider: logProvider,
		Level:          slog.LevelDebug,
		Next:           slog.NewJSONHandler(&buf, nil),
	})

	err := slogtest.TestHandler(handler, func() []map[string]any {
		return parseJSONLines(t, buf.String())
	})
	if err != nil {
		t.Fatalf("slogtest compliance failed for LogHandler: %v", err)
	}
}

func TestZeroAllocBufferedLogWithActiveSpan(t *testing.T) {
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(io.Discard, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})
	logger := slog.New(rec)

	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
		Recorder: rec,
	})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("alloc-test")

	ctx, root := tracer.Start(context.Background(), "root")
	defer root.End()
	ctx, scope := flightrec.StartScope(ctx, rec, flightrec.ScopeMeta{})
	defer scope.End(ctx, 200)
	childCtx, child := tracer.Start(ctx, "child")
	defer child.End()

	// Warm up the scope ring buffer.
	for i := 0; i < 30; i++ {
		logger.DebugContext(childCtx, "warmup", "k", "v", "n", i)
	}

	allocs := testing.AllocsPerRun(100, func() {
		logger.DebugContext(childCtx, "buffered with span", "k", "v", "n", 1)
	})
	if allocs > 0 {
		t.Fatalf("expected 0 allocs/op when buffering log inside active OTel span, got %v", allocs)
	}
}

type recordOnlySampler struct{}

func (recordOnlySampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	return sdktrace.SamplingResult{
		Decision:   sdktrace.RecordOnly,
		Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
	}
}

func (recordOnlySampler) Description() string { return "RecordOnly" }

type countingProcessor struct {
	mu     sync.Mutex
	starts int
	ends   int
}

func (c *countingProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {
	c.mu.Lock()
	c.starts++
	c.mu.Unlock()
}

func (c *countingProcessor) OnEnd(sdktrace.ReadOnlySpan) {
	c.mu.Lock()
	c.ends++
	c.mu.Unlock()
}

func (c *countingProcessor) Shutdown(context.Context) error   { return nil }
func (c *countingProcessor) ForceFlush(context.Context) error { return nil }

func TestUnsampledSpansAreNotTracked(t *testing.T) {
	next := &countingProcessor{}
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{Next: next})
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(recordOnlySampler{}),
		sdktrace.WithSpanProcessor(sp),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := tp.Tracer("test")

	const traces = 1000
	for i := 0; i < traces; i++ {
		ctx, root := tr.Start(context.Background(), "root")
		_, child := tr.Start(ctx, "child")
		child.End()
		root.End()
	}

	st := sp.Stats()
	if st.ActiveTraces != 0 || st.TracesStarted != 0 || st.BufferedBytes != 0 {
		t.Fatalf("unsampled traces must not be tracked: %+v", st)
	}
	next.mu.Lock()
	defer next.mu.Unlock()
	if next.starts != 2*traces || next.ends != 2*traces {
		t.Fatalf("unsampled spans must pass through to Next: starts=%d ends=%d, want %d each", next.starts, next.ends, 2*traces)
	}
}
