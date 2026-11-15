// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package otelflightrec_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/divine-ai-com/flightrec"
	"github.com/divine-ai-com/flightrec/otelflightrec"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func ExampleNewSpanProcessor() {
	exporter := tracetest.NewInMemoryExporter()
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(io.Discard, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})

	sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
		Next:          sdktrace.NewSimpleSpanProcessor(exporter),
		Recorder:      rec,
		HealthyPolicy: otelflightrec.HealthyExportRootOnly,
	})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("checkout")

	// Healthy trace: 3 child spans are discarded, only the root span is exported.
	hCtx, hRoot := tracer.Start(context.Background(), "GET /catalog")
	for i := 0; i < 3; i++ {
		_, ch := tracer.Start(hCtx, "db.query")
		ch.End()
	}
	hRoot.End()

	// Failed trace: child span error triggers export of all spans in the trace.
	fCtx, fRoot := tracer.Start(context.Background(), "POST /checkout")
	_, fChild := tracer.Start(fCtx, "payment.charge")
	fChild.SetStatus(codes.Error, "gateway timeout")
	fChild.End()
	fRoot.End()

	st := sp.Stats()
	fmt.Printf("exported=%d dropped_healthy=%d flushed_traces=%d\n",
		st.SpansExported, st.SpansHealthyDropped, st.TracesFlushed)
	// Output:
	// exported=3 dropped_healthy=3 flushed_traces=1
}
