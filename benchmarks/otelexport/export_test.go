// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package otelexport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/divine-ai-com/flightrec/otelflightrec"
)

// collector is a local OTLP/HTTP endpoint that counts the bytes it receives.
type collector struct {
	srv   *httptest.Server
	bytes atomic.Int64
}

func newCollector(b *testing.B) *collector {
	c := &collector{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		c.bytes.Add(n)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	b.Cleanup(c.srv.Close)
	return c
}

// countingExporter counts the spans that reach the exporter.
type countingExporter struct {
	sdktrace.SpanExporter
	spans atomic.Int64
}

func (e *countingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.spans.Add(int64(len(spans)))
	return e.SpanExporter.ExportSpans(ctx, spans)
}

// exportPipeline returns a BatchSpanProcessor feeding the OTLP/HTTP exporter
// and the collector it sends to. The processor blocks instead of dropping
// spans when its queue is full, so every export is paid for in the
// measurement.
func exportPipeline(b *testing.B) (sdktrace.SpanProcessor, *countingExporter, *collector) {
	c := newCollector(b)
	exp, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpointURL(c.srv.URL+"/v1/traces"),
		otlptracehttp.WithCompression(otlptracehttp.NoCompression),
	)
	if err != nil {
		b.Fatal(err)
	}
	ce := &countingExporter{SpanExporter: exp}
	return sdktrace.NewBatchSpanProcessor(ce, sdktrace.WithBlocking()), ce, c
}

// runTraces creates b.N traces of one root span and ten child spans, the
// last child failing when fail is set, then flushes the pipeline inside the
// timed region and reports what was exported per trace.
//
// ns/op is wall time: the exporter runs on other cores while spans are
// created. cpu-ns/op is the process's CPU time (user and system, all
// threads, including the local collector's handler) per trace, which counts
// the export work in full.
func runTraces(b *testing.B, sp sdktrace.SpanProcessor, ce *countingExporter, c *collector, fail bool) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("bench")
	bg := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	cpu0, cpuOK := processCPU()
	for range b.N {
		ctx, root := tracer.Start(bg, "HTTP POST /checkout")
		for j := range 10 {
			_, ch := tracer.Start(ctx, "db.query")
			ch.SetAttributes(
				attribute.Int("iter", j),
				attribute.String("db.system", "postgresql"),
				attribute.String("db.statement", "SELECT id, total FROM orders WHERE customer_id = $1"),
			)
			if fail && j == 9 {
				ch.SetStatus(codes.Error, "db error")
			}
			ch.End()
		}
		root.End()
	}
	if err := tp.ForceFlush(bg); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	if cpu1, ok := processCPU(); ok && cpuOK {
		b.ReportMetric(float64(cpu1-cpu0)/float64(b.N), "cpu-ns/op")
	}
	b.ReportMetric(float64(ce.spans.Load())/float64(b.N), "spans/op")
	b.ReportMetric(float64(c.bytes.Load())/float64(b.N), "exported-B/op")
}

// BenchmarkExportAll is the baseline: plain OpenTelemetry exporting every
// span of a healthy trace.
func BenchmarkExportAll(b *testing.B) {
	bsp, ce, c := exportPipeline(b)
	runTraces(b, bsp, ce, c, false)
}

// BenchmarkHealthyRootOnly exports only the root span of a healthy trace
// (the default policy).
func BenchmarkHealthyRootOnly(b *testing.B) {
	bsp, ce, c := exportPipeline(b)
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{Next: bsp, HealthyPolicy: otelflightrec.HealthyExportRootOnly})
	runTraces(b, sp, ce, c, false)
}

// BenchmarkHealthyNone exports nothing for a healthy trace.
func BenchmarkHealthyNone(b *testing.B) {
	bsp, ce, c := exportPipeline(b)
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{Next: bsp, HealthyPolicy: otelflightrec.HealthyExportNone})
	runTraces(b, sp, ce, c, false)
}

// BenchmarkTriggered exports every span of a trace whose last child fails.
func BenchmarkTriggered(b *testing.B) {
	bsp, ce, c := exportPipeline(b)
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{Next: bsp})
	runTraces(b, sp, ce, c, true)
}
