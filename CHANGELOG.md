# Changelog

All notable changes to `flightrec` are documented in this file. This project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- Trace ID headers with whitespace, quotes, control characters, non-ASCII bytes or more than 128 bytes are no longer used; such requests get a generated trace ID instead. IDs such as UUIDs, ULIDs, base64 and `host/sequence` request IDs are unaffected (#49).

### Added
- `HTTPMiddleware` sets the `Flightrec-Flush` response header (`flightrec.FlushHeader`) when a scope is triggered before its headers are written, so callers can keep their own logs for the same request. The value is a built-in reason such as `http_502` or `slow_request`; custom `Flush` reasons are sent as `explicit_flush`. `Options.DisableFlushMarker` turns the header off for services that answer external clients. The header is removed from incoming requests. `Stats` gains `FlushMarkersSent` and `FlushMarkersMissed` (#39).
- `NewTransport` wraps an `http.RoundTripper` so that a `Flightrec-Flush` header on a response triggers the caller's scope with reason `downstream:<reason>`; the flush then travels up the call chain. A value that is not exactly one built-in reason is ignored and counted, and the header is removed from the response. `MarkerValue` and `ValidMarker` expose the sending and checking rules for integrations. `Stats` gains `DownstreamTriggers` and `MarkersRejected` (#40).
- `Options.Retain` keeps the buffer of a healthy scope for a window after it ends, within its own byte limit `Options.MaxRetained` (default 16 MB) and at most 8 scopes per trace ID. `Recorder.FlushTrace(ctx, traceID, reason)` writes the retained scopes for a trace. `slog.Any` values, including those added with `Logger.With`, are copied when a scope is retained, so later changes to the objects do not show; `slog.JSONHandler` writes the copy as the original JSON and `slog.TextHandler` as JSON text. Expired scopes are released by a timer even without traffic. The summary line of a retained scope carries `flightrec.retained_records`. `Stats` gains `RetainedScopes`, `RetainedBytes`, `RetainedOldestAge`, `RetainedFlushed`, `RetainedExpired` and `RetainedEvicted` (#42).
- `Notifier` and `Recorder.StartNotifier(ctx, n)` connect Recorders in different processes: a local failure announces its trace ID once, and other processes flush their retained scopes and trigger their running scopes for that trace with reason `remote:<reason>`. Scopes triggered by a remote notice or a downstream marker do not announce, so notices cannot echo. Publishing is queued, batched and rate limited, delivery is rate limited, malformed notices are dropped, and the subscription is retried with backoff. A local failure also flushes retained scopes for the same trace in the same process. `notifier/memnotifier` provides an in-process `Notifier` for tests and examples. `Stats` gains `RemoteFlushes` and the `Notifier*` counters (#43).

### Fixed
- An informational response such as `103 Early Hints` no longer becomes the recorded status of the request; the final status is used for the 5xx trigger and the summary line (#47).

### Security
- Trace IDs taken from `X-Trace-Id`, `X-Request-Id` and the matching gRPC metadata, and from a custom `GRPCMetadataExtractor`, must be 1 to 128 bytes of letters, digits and `_ . : - / + = @`. An invalid value is skipped: the next header is tried, then an active OpenTelemetry span, then a generated ID. When a header repeats, the first valid value wins (#49).
- `Options.IgnoreRequestTraceIDs` stops `HTTPMiddleware` and the gRPC helpers from taking trace IDs from incoming requests, for services that face clients outside your system (#49).

## [grpcflightrec/v0.2.1, otelflightrec/v0.2.1] - 2026-11-15

The core module is unchanged at `v0.2.0`.

### Fixed
- `otelflightrec` and `grpcflightrec` now require core `v0.2.0` instead of a zero pseudo-version and `v0.1.0`, so they install from the module proxy. `otelflightrec/v0.2.0`, `grpcflightrec/v0.1.1` and `grpcflightrec/v0.2.0` are retracted (#23).
- `otelflightrec.SpanProcessor` no longer tracks spans that are recorded but not sampled. They pass through to `Next` unchanged instead of holding a trace entry until `MaxTraceAge` (#24).

### Security
- `grpcflightrec` requires `golang.org/x/net` v0.60.0, which fixes GO-2026-6603, GO-2026-6611, GO-2026-6612 and GO-2026-6617 in the HTTP/2 code that gRPC uses. Previously the minimum was v0.58.0 (#31).

## [v0.2.0] - 2026-11-15

### Added
- `otelflightrec` submodule (`github.com/divine-ai-com/flightrec/otelflightrec`) providing `SpanProcessor` (`go.opentelemetry.io/otel/sdk/trace.SpanProcessor`) that buffers finished child spans per trace in bounded memory (`MaxBytesPerTrace`, `MaxTotalBytes`) and exports them only when the trace or its linked `flightrec.Scope` triggers (#18).
- Configurable healthy-trace export policies on `otelflightrec.SpanProcessor`: `HealthyExportRootOnly` (default: exports only the local root span annotated with `flightrec.dropped_spans` and discards child spans) and `HealthyExportNone` (exports nothing for healthy traces), plus `flightrec.flush_reason` and `flightrec.truncated` attributes on triggered traces (#19).
- `otelflightrec.NewLogHandler` (`log/slog.Handler`) bridging `log/slog` and `flightrec.Recorder` to OpenTelemetry Logs (`go.opentelemetry.io/otel/log`) with original timestamps, structured group/attribute mapping, and `slogtest.TestHandler` compliance (#20).
- Zero-allocation log-time `TraceContext` (`TraceID`, `SpanID`, `TraceFlags`) capture in `flightrec.Recorder`, emitting `flightrec.span_id` on output records when an OpenTelemetry span was active at log time and restoring the log-time `SpanContext` into `ctx` when flushing buffered records to `Next.Handle` (#20).
- Bidirectional `ScopeObserver` hooks (`Recorder.AttachObserver`, `ScopeFromContext`, `Scope.AdoptGeneratedTraceID`) so `otelflightrec.SpanProcessor` and `flightrec.Recorder` coordinate triggers and trace IDs regardless of middleware wrapping order (#18, #20).

## [v0.1.1] - 2026-11-15

### Changed
- Raise minimum Go version in `go.mod` from `1.24` to `1.26` and update CI to test Go `1.26` and `1.27` (#13).
- Deprecate `Options.PassLevel` and `Options.BufferLevel` in favor of `Options.PassLeveler` and `Options.BufferLeveler` (`slog.Leveler`) (#7).

### Added
- `Options.PassLeveler` and `Options.BufferLeveler` (`slog.Leveler`) so `BufferLeveler` can be set to `slog.LevelInfo` (`0`) (#7).
- `grpcflightrec` submodule (`github.com/divine-ai-com/flightrec/grpcflightrec`) providing `UnaryServerInterceptor` and `StreamServerInterceptor` with incoming gRPC metadata trace ID extraction, plus `InvokeGRPCStream`, `WithIncomingMetadata`, and `ExtractTraceIDFromMetadata` in the root package (#10).

### Fixed
- Rebuild a single nested group/attribute tree per record so interleaved `WithGroup` and `WithAttrs` calls never emit duplicate JSON keys or drop attributes; pass `testing/slogtest` out-of-scope, in-scope pass-through, and in-scope buffered-and-flushed (#2).
- Enforce `MaxTotal` across long-lived, initially empty, and previously evicted scopes, and link/unlink scopes atomically under `Scope.mu` so closed scopes cannot remain linked in the eviction list (#3).
- Allocate per-scope ring buffers lazily on the first buffered record and account for ring-buffer slot capacity against `MaxPerScope`, `MaxTotal`, and `Stats().BufferedBytes` (#4).
- Pass through and count records logged against a request context after `Scope.End` instead of dropping them silently (#5).
- Preserve `Options.Next` delegation for out-of-scope logs (keeping startup/background `Info` logs working and `.With` loggers zero-alloc) while documenting the in-scope vs. out-of-scope level rule (#6).
- Default `PassLevel` to `slog.LevelWarn` when only `BufferLevel` is set (#7).
- Resolve `slog.LogValuer` attributes at log/buffer time rather than flush time (#8).
- Trigger `grpc_error` flushes only on gRPC server-fault codes (`Unknown`, `DeadlineExceeded`, `Unimplemented`, `Internal`, `Unavailable`, `DataLoss`) or non-gRPC errors (excluding `context.Canceled`), and emit `grpc.code` instead of HTTP `status` on gRPC summary records (#9).
- Conditionally preserve `http.Flusher`, `http.Hijacker`, and `io.ReaderFrom` on the wrapped `http.ResponseWriter` without falsely claiming `http.Hijacker` or `http.Pusher` (#11).
- Treat `http.ErrAbortHandler` as a normal response abort rather than a panic flush (#12).

## [v0.1.0] - 2026-11-15

### Added
- `Recorder` implementing `log/slog.Handler` with `PassLevel` and `BufferLevel` splitting.
- Per-request `Scope` management with five automatic flush triggers (`error_log`, `http_5xx` / `grpc_error`, `panic` with re-panic, `slow_request`, and explicit `flightrec.Flush(ctx, reason)`).
- Bounded memory management: per-scope ring buffer cap (`MaxPerScope`, default 256 KB) and process-wide oldest-scope eviction cap (`MaxTotal`, default 64 MB).
- `Stats()` snapshot exposing active scopes, buffered bytes, and drop/eviction counters.
- `HTTPMiddleware` for `net/http` with W3C `traceparent`, `X-Trace-Id`, and `X-Request-Id` extraction.
- `InvokeGRPCUnary` zero-dependency gRPC server interceptor helper.
- Reproducible benchmark suite (`benchmark_test.go`) and 1,000-request volume/fidelity demo (`cmd/demo`).
