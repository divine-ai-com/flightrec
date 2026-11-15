// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package otelflightrec_test

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/divine-ai-com/flightrec"
	"github.com/divine-ai-com/flightrec/otelflightrec"
)

type stringer struct{}

func (stringer) String() string { return "via Stringer" }

func TestLogHandlerConvertsEveryValueKind(t *testing.T) {
	exp := &memoryLogExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	t.Cleanup(func() { _ = lp.Shutdown(context.Background()) })
	log := slog.New(otelflightrec.NewLogHandler(otelflightrec.LogHandlerOptions{LoggerProvider: lp, Level: slog.LevelDebug}))

	when := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	log.Info("kinds",
		"s", "text", "i", -3, "u", uint64(7), "big", uint64(math.MaxUint64), "f", 1.5, "b", true,
		"d", 2*time.Second, "t", when, "err", errors.New("boom"), "bytes", []byte("raw"),
		"str", stringer{}, "other", struct{ A int }{1}, "nil", nil,
		slog.Group("g", "x", 1),
	)
	recs := exp.Snapshot()
	if len(recs) != 1 {
		t.Fatalf("got %d records", len(recs))
	}
	a := recs[0].Attrs
	for key, want := range map[string]any{
		"s": "text", "i": int64(-3), "u": int64(7), "big": "18446744073709551615", "f": 1.5, "b": true,
		"d": int64(2 * time.Second), "t": when.Format(time.RFC3339Nano), "err": "boom",
		"str": "via Stringer", "other": "{1}",
	} {
		if a[key] != want {
			t.Errorf("attr %q = %#v (%T), want %#v (%T)", key, a[key], a[key], want, want)
		}
	}
	if g, ok := a["g"].(map[string]any); !ok || g["x"] != int64(1) {
		t.Errorf("group = %#v", a["g"])
	}
	if b, ok := a["bytes"].([]byte); !ok || string(b) != "raw" {
		t.Errorf("bytes = %#v, want the byte slice", a["bytes"])
	}
	if v, present := a["nil"]; present && v != nil {
		t.Errorf("nil = %#v, want an empty value", v)
	}
}

func TestLogHandlerSeverityAndEnabled(t *testing.T) {
	exp := &memoryLogExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	t.Cleanup(func() { _ = lp.Shutdown(context.Background()) })
	h := otelflightrec.NewLogHandler(otelflightrec.LogHandlerOptions{LoggerProvider: lp, Level: slog.LevelInfo})

	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("Debug enabled below the configured Info level")
	}
	if !h.Enabled(context.Background(), slog.LevelWarn) {
		t.Error("Warn not enabled")
	}
	log := slog.New(h)
	for _, l := range []slog.Level{slog.LevelInfo, slog.LevelWarn, slog.LevelError, slog.LevelError + 4} {
		log.Log(context.Background(), l, "m")
	}
	var got []otellog.Severity
	for _, r := range exp.Snapshot() {
		got = append(got, r.Severity)
	}
	want := []otellog.Severity{otellog.SeverityInfo1, otellog.SeverityWarn1, otellog.SeverityError1, otellog.SeverityFatal1}
	if len(got) != len(want) {
		t.Fatalf("severities %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("severity %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestInjectTraceContext(t *testing.T) {
	tid := [16]byte{1, 2, 3}
	ctx := otelflightrec.InjectTraceContext(context.Background(), flightrec.TraceContext{TraceID: tid})
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() || sc.TraceID() != trace.TraceID(tid) || !sc.IsSampled() {
		t.Fatalf("trace ID only: span context %+v should be valid, sampled, with the trace ID", sc)
	}

	sid := [8]byte{9}
	parent := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID(tid), SpanID: trace.SpanID(sid), TraceFlags: trace.FlagsSampled,
	}))
	ctx = otelflightrec.InjectTraceContext(parent, flightrec.TraceContext{TraceID: tid})
	if got := trace.SpanContextFromContext(ctx).SpanID(); got != trace.SpanID(sid) {
		t.Errorf("span ID = %s, want the existing span's %s", got, trace.SpanID(sid))
	}

	if got := otelflightrec.InjectTraceContext(nil, flightrec.TraceContext{}); got == nil { //nolint:staticcheck // nil context is part of the contract
		t.Error("nil context returned nil")
	}
}

func TestTriggerContextFromScopeWithoutSpan(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{Next: sdktrace.NewSimpleSpanProcessor(exp)})
	sp.AttachRecorder(rec)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, root := tp.Tracer("t").Start(context.Background(), "root")
	_, child := tp.Tracer("t").Start(ctx, "child")
	child.End()

	// A scope that only knows the trace ID, as in code that did not keep the span context.
	scopeCtx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: root.SpanContext().TraceID().String()})
	sp.TriggerContext(scopeCtx, "explicit")
	sc.End(scopeCtx, 200)
	root.End()

	if n := len(exp.GetSpans()); n != 2 {
		t.Errorf("exported %d spans, want root and child after TriggerContext", n)
	}
	if err := sp.ForceFlush(context.Background()); err != nil {
		t.Errorf("ForceFlush: %v", err)
	}
}

func TestSpanRingGrowsForLargeTraces(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{Next: sdktrace.NewSimpleSpanProcessor(exp)})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := tp.Tracer("t")

	ctx, root := tr.Start(context.Background(), "root")
	for i := 0; i < 100; i++ { // well past the initial ring capacity
		_, s := tr.Start(ctx, "child")
		s.End()
	}
	sp.TriggerContext(ctx, "explicit")
	root.End()

	if got := len(exp.GetSpans()); got != 101 {
		t.Errorf("exported %d spans, want all 101", got)
	}
	if st := sp.Stats(); st.ActiveTraces != 0 || st.BufferedBytes != 0 {
		t.Errorf("trace not released: %+v", st)
	}
}

func TestScopeFailingAtEndFlushesItsTrace(t *testing.T) {
	for _, tc := range []struct {
		status    int
		wantSpans int
	}{
		{503, 2}, // failed at End: the whole trace is exported
		{200, 1}, // healthy: only the root, under the default policy
	} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) { endTraceWithStatus(t, tc.status, tc.wantSpans) })
	}
}

func endTraceWithStatus(t *testing.T, status, wantSpans int) {
	exp := tracetest.NewInMemoryExporter()
	rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{Next: sdktrace.NewSimpleSpanProcessor(exp), Recorder: rec})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := tp.Tracer("t")

	ctx, root := tr.Start(context.Background(), "GET /orders")
	ctx, sc := flightrec.StartScope(ctx, rec, flightrec.ScopeMeta{})
	_, child := tr.Start(ctx, "db.query",
		trace.WithLinks(trace.Link{SpanContext: root.SpanContext(), Attributes: []attribute.KeyValue{attribute.String("why", "retry")}}))
	child.SetAttributes(
		attribute.StringSlice("tables", []string{"orders", "items"}),
		attribute.Int64Slice("ids", []int64{1, 2}),
		attribute.Bool("cached", false),
	)
	child.AddEvent("rows", trace.WithAttributes(attribute.Int("n", 3)))
	child.End()
	root.End()          // the local root ends first, as with otelhttp wrapping flightrec
	sc.End(ctx, status) // the status decides at End whether the request failed

	spans := exp.GetSpans()
	if len(spans) != wantSpans {
		t.Fatalf("status %d: exported %d spans, want %d", status, len(spans), wantSpans)
	}
	if sp.Stats().ActiveTraces != 0 {
		t.Errorf("trace not released: %+v", sp.Stats())
	}
	if wantSpans < 2 {
		return
	}
	var got tracetest.SpanStub
	for _, s := range spans {
		if s.Name == "db.query" {
			got = s
		}
	}
	attrs := map[string]string{}
	for _, kv := range got.Attributes {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs["tables"] != `["orders","items"]` || attrs["ids"] != "[1,2]" || attrs["cached"] != "false" {
		t.Errorf("child attributes = %v", attrs)
	}
	if len(got.Events) != 1 || got.Events[0].Name != "rows" {
		t.Errorf("child events = %+v", got.Events)
	}
	if len(got.Links) != 1 || got.Links[0].SpanContext.SpanID() != root.SpanContext().SpanID() {
		t.Errorf("child links = %+v", got.Links)
	}
}
