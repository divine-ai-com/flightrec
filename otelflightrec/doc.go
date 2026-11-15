// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package otelflightrec integrates flightrec with OpenTelemetry traces and logs.
//
// It provides three capabilities while keeping the root github.com/divine-ai-com/flightrec
// module free of external dependencies:
//
//  1. SpanProcessor (implementing go.opentelemetry.io/otel/sdk/trace.SpanProcessor)
//     buffers finished child spans per trace in bounded memory and exports them
//     only when the trace is triggered (by a span error, an exception event, a
//     slow trace duration, an explicit flush, or any linked flightrec.Scope
//     trigger such as an error log, HTTP 5xx, gRPC server fault, or panic).
//     For healthy traces, SpanProcessor exports either the local root span only
//     (HealthyExportRootOnly, the default) or nothing (HealthyExportNone).
//
//  2. Automatic log-time TraceID and SpanID correlation. Importing otelflightrec
//     registers a default TraceBridge with flightrec so buffered log records
//     capture the active OpenTelemetry TraceID and SpanID at log time with zero
//     heap allocations, emit "flightrec.span_id" when flushed, and restore the
//     log-time SpanContext into ctx when calling downstream slog handlers.
//
//  3. LogHandler (implementing log/slog.Handler) bridges log/slog and
//     flightrec.Recorder to go.opentelemetry.io/otel/log so flushed records
//     carry their original timestamps, attributes, TraceID, and SpanID to any
//     OpenTelemetry Logs exporter.
package otelflightrec
