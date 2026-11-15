// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package flightrec implements an in-memory log flight recorder for Go's
// standard log/slog package.
//
// It keeps detailed debug and info logs in a bounded per-request memory buffer
// and writes them to the underlying slog.Handler only when a request fails
// (error log, HTTP 5xx, gRPC server fault, panic, slow response, or an explicit
// call to Flush). Healthy requests discard their buffered debug logs and
// emit a single summary record.
//
// # Level Filtering
//
// flightrec applies one rule depending on whether the context carries an
// active request Scope:
//
//   - Outside a scope: records are delegated to Options.Next.Enabled and
//     Options.Next.Handle unchanged. Startup logs, background job logs, and
//     slog.Info calls without a request context work the same as without
//     flightrec.
//   - Inside a scope: records at or above PassLeveler (default slog.LevelWarn)
//     are written to Options.Next immediately. Records at or above
//     BufferLeveler (default slog.LevelDebug) and below PassLeveler are stored
//     in the scope's ring buffer; when the scope triggers, buffered records are
//     flushed through Options.Next.Handle regardless of Options.Next's own
//     level so debug detail for a failed request is never lost.
//
// Basic usage:
//
//	rec := flightrec.New(flightrec.Options{
//	    Next:          slog.NewJSONHandler(os.Stdout, nil),
//	    PassLeveler:   slog.LevelWarn,
//	    BufferLeveler: slog.LevelDebug,
//	    SlowAfter:     2 * time.Second,
//	    MaxPerScope:   256 << 10, // 256 KB per request
//	    MaxTotal:      64 << 20,  // 64 MB process-wide
//	})
//	slog.SetDefault(slog.New(rec))
//
//	mux := http.NewServeMux()
//	handler := flightrec.HTTPMiddleware(rec)(mux)
//
//	// Force the current request buffer to flush with a custom reason:
//	flightrec.Flush(ctx, "payment mismatch")
package flightrec
