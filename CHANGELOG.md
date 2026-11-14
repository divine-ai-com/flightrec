# Changelog

All notable changes to `flightrec` are documented in this file. This project adheres to [Semantic Versioning](https://semver.org/).

## [v0.1.0] - 2026-11-15

### Added
- `Recorder` implementing `log/slog.Handler` with `PassLevel` and `BufferLevel` splitting.
- Per-request `Scope` management with five automatic flush triggers (`error_log`, `http_5xx` / `grpc_error`, `panic` with re-panic, `slow_request`, and explicit `flightrec.Flush(ctx, reason)`).
- Bounded memory management: per-scope ring buffer cap (`MaxPerScope`, default 256 KB) and process-wide oldest-scope eviction cap (`MaxTotal`, default 64 MB).
- `Stats()` snapshot exposing active scopes, buffered bytes, and drop/eviction counters.
- `HTTPMiddleware` for `net/http` (preserving `Flusher`, `Hijacker`, `Pusher`, and `Unwrap`) with W3C `traceparent`, `X-Trace-Id`, and `X-Request-Id` extraction.
- `InvokeGRPCUnary` zero-dependency gRPC server interceptor helper.
- Reproducible benchmark suite (`benchmark_test.go`) and 1,000-request volume/fidelity demo (`cmd/demo`).
