// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package otelflightrec_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/divine-ai-com/flightrec/otelflightrec"
)

type noopSpanProcessor struct{}

func (noopSpanProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (noopSpanProcessor) OnEnd(sdktrace.ReadOnlySpan)                     {}
func (noopSpanProcessor) Shutdown(context.Context) error                  { return nil }
func (noopSpanProcessor) ForceFlush(context.Context) error                { return nil }

// BenchmarkPlainOTel_AlwaysExport measures baseline OpenTelemetry tracing where
// a request creates 1 root span and 10 child spans and exports all 11 spans.
func BenchmarkPlainOTel_AlwaysExport(b *testing.B) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(noopSpanProcessor{}))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("bench")
	bg := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, root := tracer.Start(bg, "HTTP GET /orders")
		for j := 0; j < 10; j++ {
			_, ch := tracer.Start(ctx, "db.query")
			ch.SetAttributes(attribute.Int("iter", j))
			ch.End()
		}
		root.End()
	}
}

// BenchmarkSpanProcessor_HealthyRootOnly measures otelflightrec.SpanProcessor
// on a healthy trace with 1 root span and 10 child spans (exporting only root).
func BenchmarkSpanProcessor_HealthyRootOnly(b *testing.B) {
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
		Next:          noopSpanProcessor{},
		HealthyPolicy: otelflightrec.HealthyExportRootOnly,
	})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("bench")
	bg := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, root := tracer.Start(bg, "HTTP GET /orders")
		for j := 0; j < 10; j++ {
			_, ch := tracer.Start(ctx, "db.query")
			ch.SetAttributes(attribute.Int("iter", j))
			ch.End()
		}
		root.End()
	}
}

// BenchmarkSpanProcessor_TriggeredFlush measures otelflightrec.SpanProcessor
// when a child span fails and triggers export of all 11 spans.
func BenchmarkSpanProcessor_TriggeredFlush(b *testing.B) {
	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
		Next: noopSpanProcessor{},
	})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("bench")
	bg := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, root := tracer.Start(bg, "HTTP POST /checkout")
		for j := 0; j < 10; j++ {
			_, ch := tracer.Start(ctx, "db.query")
			ch.SetAttributes(attribute.Int("iter", j))
			if j == 9 {
				ch.SetStatus(codes.Error, "db error")
			}
			ch.End()
		}
		root.End()
	}
}
