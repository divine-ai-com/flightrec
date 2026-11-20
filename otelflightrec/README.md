# otelflightrec

OpenTelemetry integration for [flightrec](https://github.com/haawklabs/flightrec): a span processor that exports a healthy trace's root span only and a failed trace in full, sharing triggers with flightrec's logs, and a bridge to OpenTelemetry Logs.

```bash
go get github.com/haawklabs/flightrec/otelflightrec
```

<p align="center"><img src="https://raw.githubusercontent.com/haawklabs/flightrec/main/docs/media/otel.svg" alt="SpanProcessor exports only the root span of a healthy trace and every span of a failed one" width="100%"></p>

Use the `github.com/haawklabs/flightrec/otelflightrec` submodule to buffer finished child spans per trace and export them only when the trace or its linked `flightrec.Scope` triggers:

```go
import "github.com/haawklabs/flightrec/otelflightrec"

sp := otelflightrec.NewSpanProcessor(otelflightrec.SpanProcessorOptions{
	Next:          sdktrace.NewBatchSpanProcessor(spanExporter),
	Recorder:      rec,                                  // shares triggers between logs and traces
	HealthyPolicy: otelflightrec.HealthyExportRootOnly, // or HealthyExportNone
	SlowAfter:     2 * time.Second,
})
tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp))
```

To bridge `slog` records directly to OpenTelemetry Logs (`go.opentelemetry.io/otel/log`) with log-time `TraceID` and child `SpanID` correlation:

```go
rec := flightrec.New(flightrec.Options{
	Next: otelflightrec.NewLogHandler(otelflightrec.LogHandlerOptions{
		LoggerProvider: logProvider,
		Next:           slog.NewJSONHandler(os.Stdout, nil), // optional stdout mirror
	}),
})
```

Measured against a real OTLP export path, a healthy trace uses 56% less CPU than exporting it; see [benchmarks](https://github.com/haawklabs/flightrec/blob/main/docs/benchmarks.md#tracing-with-otelflightrec).
