// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"log/slog"
	"time"
	"unsafe"
)

const (
	// DefaultMaxPerScope is the default per-request buffer cap (256 KB).
	DefaultMaxPerScope int64 = 256 << 10
	// DefaultMaxTotal is the default process-wide buffer cap (64 MB).
	DefaultMaxTotal int64 = 64 << 20
	// DefaultMaxRetained is the default cap on retained scope bytes (16 MB).
	DefaultMaxRetained int64 = 16 << 20

	initialRingCapacity = 24
	maxPooledRingCap    = 512

	entrySlotSize = int64(unsafe.Sizeof(bufferedEntry{}))
)

// Options configures a Recorder.
type Options struct {
	// Next is the underlying slog.Handler where passed-through, flushed,
	// and healthy summary records are written. If nil, a default JSONHandler
	// writing to io.Discard is used.
	//
	// Outside a request scope, Next.Enabled and Next.Handle are called
	// directly. Inside a request scope, PassLeveler and BufferLeveler govern
	// buffering, and flushed records are sent directly to Next.Handle
	// regardless of Next's configured level.
	Next slog.Handler

	// PassLeveler is the minimum level written immediately to Next without
	// buffering inside a scope. Takes precedence over PassLevel when non-nil.
	// Defaults to slog.LevelWarn when both PassLeveler and PassLevel are unset.
	PassLeveler slog.Leveler

	// BufferLeveler is the minimum level stored in a request scope's buffer
	// and emitted only if the scope triggers. Takes precedence over
	// BufferLevel when non-nil. Defaults to slog.LevelDebug when both
	// BufferLeveler and BufferLevel are unset.
	BufferLeveler slog.Leveler

	// PassLevel is the minimum level written immediately to Next without
	// buffering inside a scope. Defaults to slog.LevelWarn when unset (zero).
	//
	// Deprecated: Use PassLeveler instead, which distinguishes slog.LevelInfo
	// (zero) from an unset value.
	PassLevel slog.Level

	// BufferLevel is the minimum level stored in a request scope's buffer
	// and emitted only if the scope triggers. Defaults to slog.LevelDebug
	// when unset (zero).
	//
	// Deprecated: Use BufferLeveler instead, which distinguishes slog.LevelInfo
	// (zero) from an unset value.
	BufferLevel slog.Level

	// SlowAfter triggers a scope flush when a request's total duration
	// meets or exceeds this threshold. Zero disables slow-request triggering.
	SlowAfter time.Duration

	// MaxPerScope caps buffered bytes (including ring-buffer slot capacity)
	// for a single request scope. Defaults to DefaultMaxPerScope (256 KB)
	// when <= 0.
	MaxPerScope int64

	// MaxTotal caps buffered bytes (including ring-buffer slot capacity)
	// across all active scopes in the process. Defaults to DefaultMaxTotal
	// (64 MB) when <= 0.
	MaxTotal int64

	// IgnoreRequestTraceIDs stops HTTPMiddleware and the gRPC helpers
	// (including any GRPCMetadataExtractor passed to them) from taking trace
	// IDs from traceparent, X-Trace-Id and X-Request-Id on incoming requests.
	// Set it on services that receive requests from clients outside your
	// system: a client could otherwise choose the trace ID that groups its
	// requests. The trace ID then comes from an active OpenTelemetry span
	// when one is present, or is generated.
	IgnoreRequestTraceIDs bool

	// DisableFlushMarker stops HTTPMiddleware from setting the FlushHeader
	// response header. Set it on services whose responses go to clients
	// outside your system, such as a public API or an edge gateway.
	DisableFlushMarker bool

	// Retain keeps the buffer of a healthy scope for this long after the scope
	// ends, so a failure reported later for the same trace ID can still write
	// it with Recorder.FlushTrace (or, with a notifier, from another service).
	// Zero disables retention. See also MaxRetained.
	Retain time.Duration

	// MaxRetained caps the bytes held by retained scopes, separately from
	// MaxTotal. When it is reached the oldest retained scopes are dropped.
	// Defaults to DefaultMaxRetained (16 MB) when Retain is set and this is
	// <= 0.
	MaxRetained int64

	// DisableHealthySummary disables writing the single summary log line when
	// a healthy (untriggered) scope completes.
	DisableHealthySummary bool

	// TraceExtractor optionally extracts a W3C TraceID and SpanID from ctx at
	// log and scope-start time. When nil, the bridge registered via
	// RegisterDefaultTraceBridge (automatically set by importing otelflightrec)
	// is used.
	TraceExtractor TraceExtractor

	// TraceInjector optionally injects the log-time TraceContext back into ctx
	// when flushing buffered records to Next.Handle. When nil, the bridge
	// registered via RegisterDefaultTraceBridge is used.
	TraceInjector TraceInjector

	// Now returns the current time. Defaults to time.Now.
	Now func() time.Time
}

func (st *recorderState) passLevel() slog.Level {
	return st.passLeveler.Level()
}

func (st *recorderState) bufferLevel() slog.Level {
	return st.bufferLeveler.Level()
}
