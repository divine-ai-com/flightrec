// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package otelflightrec

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/divine-ai-com/flightrec"
)

func init() {
	flightrec.RegisterDefaultTraceBridge(ExtractTraceContext, InjectTraceContext)
}

// ExtractTraceContext extracts the active OpenTelemetry TraceID, SpanID, and
// TraceFlags from ctx without allocating.
func ExtractTraceContext(ctx context.Context) flightrec.TraceContext {
	if ctx == nil {
		return flightrec.TraceContext{}
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return flightrec.TraceContext{}
	}
	return flightrec.TraceContext{
		TraceID:    sc.TraceID(),
		SpanID:     sc.SpanID(),
		TraceFlags: byte(sc.TraceFlags()),
		Remote:     sc.IsRemote(),
	}
}

// InjectTraceContext returns a derived context containing an OpenTelemetry
// SpanContext constructed from tc. It sets FlagsSampled so downstream
// OpenTelemetry log processors with TraceBased filtering never discard a
// record that flightrec has chosen to flush.
func InjectTraceContext(ctx context.Context, tc flightrec.TraceContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if !tc.HasTraceID() {
		return ctx
	}
	spanID := tc.SpanID
	if !tc.HasSpanID() {
		// Use a deterministic non-zero sentinel SpanID if only a TraceID was
		// known so trace.SpanContext.IsValid() evaluates to true for OTel log
		// processors that require a valid SpanContext to record TraceID.
		if existing := trace.SpanContextFromContext(ctx); existing.IsValid() && existing.TraceID() == tc.TraceID {
			spanID = existing.SpanID()
		} else {
			spanID[7] = 1
		}
	}
	cfg := trace.SpanContextConfig{
		TraceID:    tc.TraceID,
		SpanID:     spanID,
		TraceFlags: trace.TraceFlags(tc.TraceFlags) | trace.FlagsSampled,
		Remote:     tc.Remote,
	}
	return trace.ContextWithSpanContext(ctx, trace.NewSpanContext(cfg))
}

// FlushTrace marks the active trace (and any active flightrec.Scope in ctx) as
// triggered with the given reason so all buffered spans and logs for the trace
// will be exported when the trace completes.
func FlushTrace(ctx context.Context, sp *SpanProcessor, reason string) {
	if reason == "" {
		reason = "explicit_flush"
	}
	flightrec.Flush(ctx, reason)
	if sp != nil {
		sp.TriggerContext(ctx, reason)
	}
}
