// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/divine-ai-com/flightrec"
)

const logsPerRequest = 20

// BenchmarkPlainSlog_JSON measures standard slog.JSONHandler serializing all
// 20 debug/info log calls per request directly to io.Discard.
func BenchmarkPlainSlog_JSON(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < logsPerRequest; j++ {
			logger.DebugContext(ctx, "db query executed", "table", "orders", "rows", j, "cached", true)
		}
	}
}

// BenchmarkFlightRec_BufferedAndDropped measures a healthy request with 20
// buffered debug logs that are dropped at request end (emitting 1 summary record).
func BenchmarkFlightRec_BufferedAndDropped(b *testing.B) {
	rec := flightrec.New(flightrec.Options{
		Next:        slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLevel:   slog.LevelWarn,
		BufferLevel: slog.LevelDebug,
	})
	logger := slog.New(rec)
	baseCtx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx, sc := flightrec.StartScope(baseCtx, rec, flightrec.ScopeMeta{
			TraceID: "0123456789abcdef0123456789abcdef",
			Method:  "GET",
			Path:    "/api/orders",
		})
		for j := 0; j < logsPerRequest; j++ {
			logger.DebugContext(ctx, "db query executed", "table", "orders", "rows", j, "cached", true)
		}
		sc.End(ctx, 200)
	}
}

// BenchmarkFlightRec_PassThrough measures immediate pass-through logging
// (Warn level >= PassLevel).
func BenchmarkFlightRec_PassThrough(b *testing.B) {
	rec := flightrec.New(flightrec.Options{
		Next:        slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLevel:   slog.LevelWarn,
		BufferLevel: slog.LevelDebug,
	})
	logger := slog.New(rec)
	baseCtx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx, sc := flightrec.StartScope(baseCtx, rec, flightrec.ScopeMeta{
			TraceID: "0123456789abcdef0123456789abcdef",
			Method:  "GET",
			Path:    "/api/orders",
		})
		for j := 0; j < logsPerRequest; j++ {
			logger.WarnContext(ctx, "rate limit approaching", "tenant", "acme", "remaining", j)
		}
		sc.End(ctx, 200)
	}
}

// BenchmarkFlightRec_TriggeredAndFlushed measures a failed request (500) where
// all 20 buffered debug logs are flushed to the underlying JSONHandler.
func BenchmarkFlightRec_TriggeredAndFlushed(b *testing.B) {
	rec := flightrec.New(flightrec.Options{
		Next:        slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLevel:   slog.LevelWarn,
		BufferLevel: slog.LevelDebug,
	})
	logger := slog.New(rec)
	baseCtx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx, sc := flightrec.StartScope(baseCtx, rec, flightrec.ScopeMeta{
			TraceID: "0123456789abcdef0123456789abcdef",
			Method:  "POST",
			Path:    "/api/checkout",
		})
		for j := 0; j < logsPerRequest; j++ {
			logger.DebugContext(ctx, "db query executed", "table", "orders", "rows", j, "cached", true)
		}
		sc.End(ctx, 500)
	}
}
