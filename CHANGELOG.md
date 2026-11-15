# Changelog

All notable changes to `flightrec` are documented in this file. This project adheres to [Semantic Versioning](https://semver.org/).

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
