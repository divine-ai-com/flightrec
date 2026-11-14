// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package flightrec implements an in-memory log flight recorder for Go's
// standard log/slog package.
//
// It keeps detailed debug and info logs in a bounded per-request memory buffer
// and writes them to the underlying slog.Handler only when a request fails
// (error log, HTTP 5xx, gRPC error, panic, slow response, or an explicit
// call to Flush). Healthy requests discard their buffered debug logs and
// emit a single summary record.
//
// Basic usage:
//
//	rec := flightrec.New(flightrec.Options{
//	    Next:        slog.NewJSONHandler(os.Stdout, nil),
//	    PassLevel:   slog.LevelWarn,
//	    BufferLevel: slog.LevelDebug,
//	    SlowAfter:   2 * time.Second,
//	    MaxPerScope: 256 << 10, // 256 KB per request
//	    MaxTotal:    64 << 20,  // 64 MB process-wide
//	})
//	slog.SetDefault(slog.New(rec))
//
//	mux := http.NewServeMux()
//	handler := flightrec.HTTPMiddleware(rec)(mux)
//
//	// Force the current request buffer to flush with a custom reason:
//	flightrec.Flush(ctx, "payment mismatch")
package flightrec
