// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/divine-ai-com/flightrec"
	"github.com/divine-ai-com/flightrec/notifier/memnotifier"
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
		Next:          slog.NewJSONHandler(io.Discard, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
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
// (Warn level >= PassLeveler).
func BenchmarkFlightRec_PassThrough(b *testing.B) {
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(io.Discard, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
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
		Next:          slog.NewJSONHandler(io.Discard, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
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

// BenchmarkPlainSlog_DebugOff measures 20 debug calls against a plain
// slog.JSONHandler at its default Info level, where every call is discarded.
// This is the baseline for services that run with debug logging turned off.
func BenchmarkPlainSlog_DebugOff(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < logsPerRequest; j++ {
			logger.DebugContext(ctx, "db query executed", "table", "orders", "rows", j, "cached", true)
		}
	}
}

// BenchmarkPlainSlog_JSON_SixAttrs measures plain slog.JSONHandler with a child
// logger and six attributes per call, one more than slog.Record stores inline.
func BenchmarkPlainSlog_JSON_SixAttrs(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})).With("service", "checkout", "region", "eu")
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < logsPerRequest; j++ {
			logger.DebugContext(ctx, "db query executed", "table", "orders", "rows", j, "cached", true, "shard", 3, "dur_ms", 12, "user", "u-42")
		}
	}
}

// BenchmarkFlightRec_BufferedAndDropped_SixAttrs measures a healthy request
// with a child logger and six attributes per call.
func BenchmarkFlightRec_BufferedAndDropped_SixAttrs(b *testing.B) {
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(io.Discard, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})
	logger := slog.New(rec).With("service", "checkout", "region", "eu")
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
			logger.DebugContext(ctx, "db query executed", "table", "orders", "rows", j, "cached", true, "shard", 3, "dur_ms", 12, "user", "u-42")
		}
		sc.End(ctx, 200)
	}
}

// BenchmarkFlightRec_OutOfScope_WithAttrs measures logging outside a scope
// using a child logger with five pre-formatted .With attributes.
func BenchmarkFlightRec_OutOfScope_WithAttrs(b *testing.B) {
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(io.Discard, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})
	logger := slog.New(rec).With(
		"service", "checkout",
		"region", "eu-central-1",
		"env", "prod",
		"version", "1.4.2",
		"instance", "pod-09",
	)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		logger.WarnContext(ctx, "background tick", "iter", i)
	}
}

// BenchmarkFlightRec_BufferedAndRetained measures a healthy request with 20
// buffered debug logs kept for Options.Retain instead of discarded.
func BenchmarkFlightRec_BufferedAndRetained(b *testing.B) {
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
		Retain:        30 * time.Second,
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

type benchOrder struct {
	ID    int    `json:"id"`
	State string `json:"state"`
	Total int    `json:"total"`
}

// benchAnyValues runs healthy requests whose 20 log calls each carry a
// struct passed with slog.Any, with or without retention.
func benchAnyValues(b *testing.B, retain time.Duration) {
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
		Retain:        retain,
	})
	logger := slog.New(rec)
	o := &benchOrder{ID: 42, State: "pending", Total: 1999}
	baseCtx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx, sc := flightrec.StartScope(baseCtx, rec, flightrec.ScopeMeta{TraceID: "0123456789abcdef0123456789abcdef"})
		for j := 0; j < logsPerRequest; j++ {
			logger.DebugContext(ctx, "order step", "step", j, "order", o)
		}
		sc.End(ctx, 200)
	}
}

// BenchmarkFlightRec_AnyValues_Dropped is the discarded baseline for
// BenchmarkFlightRec_AnyValues_Retained.
func BenchmarkFlightRec_AnyValues_Dropped(b *testing.B) { benchAnyValues(b, 0) }

// BenchmarkFlightRec_AnyValues_Retained includes copying each slog.Any value
// when the scope is retained.
func BenchmarkFlightRec_AnyValues_Retained(b *testing.B) { benchAnyValues(b, 30*time.Second) }

// benchParallelRequests runs healthy requests on all CPUs, optionally with a
// notifier attached, to measure the cost of the active-scope index.
func benchParallelRequests(b *testing.B, withNotifier bool) {
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(io.Discard, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})
	if withNotifier {
		defer rec.StartNotifier(context.Background(), memnotifier.New())()
	}
	logger := slog.New(rec)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "trace-" + strconv.Itoa(i)})
			logger.DebugContext(ctx, "step", "i", i)
			sc.End(ctx, 200)
		}
	})
}

// BenchmarkParallel_NoNotifier is the baseline for BenchmarkParallel_Notifier.
func BenchmarkParallel_NoNotifier(b *testing.B) { benchParallelRequests(b, false) }

// BenchmarkParallel_Notifier adds an attached in-memory notifier.
func BenchmarkParallel_Notifier(b *testing.B) { benchParallelRequests(b, true) }

// BenchmarkFlightRec_Journaled is BenchmarkFlightRec_BufferedAndDropped with
// StartJournal on: every buffered record is also encoded into the journal.
func BenchmarkFlightRec_Journaled(b *testing.B) {
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(io.Discard, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})
	stop, err := rec.StartJournal(filepath.Join(b.TempDir(), "journal"))
	if errors.Is(err, flightrec.ErrJournalUnsupported) {
		b.Skip("journal not supported on this platform")
	}
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = stop() }()
	logger := slog.New(rec)
	bg := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, sc := flightrec.StartScope(bg, rec, flightrec.ScopeMeta{
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
