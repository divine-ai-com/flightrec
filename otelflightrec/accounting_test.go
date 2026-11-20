// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package otelflightrec_test

import (
	"context"
	"runtime"
	"testing"
	"unsafe"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/haawklabs/flightrec/otelflightrec"
)

var attrKeys = []string{"db.statement", "db.system", "db.name", "db.user"}

// TestSpanAccountingMatchesHeap checks that the bytes counted for buffered
// spans are close to the heap they keep alive, so the byte limits mean what
// they say.
func TestSpanAccountingMatchesHeap(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("calibrated on 64-bit platforms")
	}
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{MaxBytesPerTrace: 1 << 30, MaxTotalBytes: 1 << 30})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := tp.Tracer("t")
	ctx, root := tr.Start(context.Background(), "root")
	defer root.End()

	for _, attrs := range []int{0, 4} {
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		before := sp.Stats().BufferedBytes
		const n = 20000
		for i := 0; i < n; i++ {
			_, s := tr.Start(ctx, "db.query")
			for a := 0; a < attrs; a++ {
				s.SetAttributes(attribute.String(attrKeys[a], "SELECT 1"))
			}
			s.End()
		}
		runtime.GC()
		runtime.ReadMemStats(&m1)
		real := float64(int64(m1.HeapAlloc)-int64(m0.HeapAlloc)) / n
		counted := float64(sp.Stats().BufferedBytes-before) / n
		t.Logf("%d attributes: %.0f B real, %.0f B counted per span", attrs, real, counted)
		if counted < real*0.75 || counted > real*1.25 {
			t.Errorf("%d attributes: counted %.0f B per span, real %.0f B: more than 25%% off", attrs, counted, real)
		}
	}
}

func TestMaxBytesPerTraceDropsSpans(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	const limit = 16 << 10
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
		Next:             sdktrace.NewSimpleSpanProcessor(exp),
		MaxBytesPerTrace: limit,
	})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := tp.Tracer("t")

	ctx, root := tr.Start(context.Background(), "root")
	for i := 0; i < 100; i++ {
		_, s := tr.Start(ctx, "child")
		s.End()
	}
	if st := sp.Stats(); st.BufferedBytes > limit {
		t.Errorf("BufferedBytes %d over MaxBytesPerTrace %d", st.BufferedBytes, limit)
	}
	sp.TriggerContext(ctx, "explicit")
	root.End()

	st := sp.Stats()
	if st.SpansDroppedPerTraceCap == 0 || len(exp.GetSpans()) >= 101 {
		t.Errorf("exported %d spans with %d dropped; a 16 KiB limit must drop some of 100 children", len(exp.GetSpans()), st.SpansDroppedPerTraceCap)
	}
}
