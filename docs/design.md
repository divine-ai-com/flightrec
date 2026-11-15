# Architecture & Design

This page describes the core buffering design. Cross-service flush (`Retain`, notifiers, flush markers) is designed in [proposal 0001](proposals/0001-cross-service-flush.md) and the crash journal in [proposal 0002](proposals/0002-crash-recovery.md).

## Goals

1. **Skip JSON formatting and I/O for healthy debug logs:** Do not serialize or write `DEBUG`/`INFO` records to `io.Writer` unless a request triggers.
2. **Strict memory bounds:** Enforce both per-scope (`MaxPerScope`) and process-wide (`MaxTotal`) byte caps, accounting for both ring-buffer slot capacity and record payloads.
3. **Zero external dependencies:** Rely strictly on the Go standard library (`log/slog`, `net/http`, `sync/atomic`).

## Request Scope Lifecycle

1. **StartScope:** `HTTPMiddleware` takes the trace ID from a trace already in the request context (an OpenTelemetry span), else from W3C `traceparent`, `X-Trace-Id`, or `X-Request-Id` (or generates a 128-bit random hex ID via a pooled `math/rand/v2.ChaCha8` generator). It attaches a `*Scope` to `r.Context()`. The underlying `[]bufferedEntry` ring buffer is allocated lazily on the first buffered record so idle scopes do not pre-allocate ring memory.
2. **Handle:**
   - **No Scope in `ctx`:** Delegated to `Next.Enabled` and `Next.Handle` unchanged, including attributes and groups added with `WithAttrs` and `WithGroup`. Startup and background logs behave exactly as they would without flightrec.
   - **`level >= PassLeveler` (`WARN`, `ERROR`):** Emitted immediately to `Next.Handle` with `flightrec.trace_id` attached. If `level >= slog.LevelError`, the scope is marked triggered (`reason = "error_log"`).
   - **`BufferLeveler <= level < PassLeveler` (`DEBUG`, `INFO`):** Resolves any `slog.LogValuer` attributes, clones the `slog.Record` into the scope's circular buffer slice (`[]bufferedEntry`), and links the scope into the eviction list. If the scope has already ended, the late record is passed through to `Next.Handle` and counted in `RecordsPassedThrough`.
3. **Memory Enforcement:**
   - **Per-Scope (`MaxPerScope`):** `scope.bytes` tracks the allocated ring-buffer slot capacity (`len(buf) * sizeof(bufferedEntry)`) plus attribute and message payloads. When a new record would exceed `MaxPerScope`, the scope pops its oldest records from the ring buffer head, marks `flightrec.truncated = true`, and increments `RecordsDroppedPerScopeCap`.
   - **Process-Wide (`MaxTotal`):** `totalBytes` is tracked via an `atomic.Int64`. When `totalBytes > MaxTotal`, the recorder acquires `listMu` to evict the oldest non-empty active scopes from the head of the doubly-linked list, incrementing `ScopesEvictedTotalCap` and `RecordsDroppedTotalCap`. If an evicted scope later buffers new records, it is re-linked to the tail of the eviction list so `MaxTotal` remains enforced across long-lived requests and streams.
4. **End:**
   - Checks HTTP status (`>= 500`), gRPC server-fault codes, and elapsed duration (`>= SlowAfter`).
   - **If triggered:** Drains the ring buffer, returns the backing slice to `sync.Pool`, and emits all buffered records in original chronological order with their original timestamps, single nested `WithAttrs`/`WithGroup` tree, `flightrec.trace_id`, and `flightrec.flush_reason`.
   - **If healthy:** Clears and returns the backing slice to `sync.Pool` without formatting any buffered records, and emits one summary record (`msg="request completed"`, `flightrec.dropped_records`, `duration`, `status` or `grpc.code`).

## OpenTelemetry Trace & Log Correlation (`otelflightrec`)

1. **Zero-Allocation Log-Time Span Capture:** When `otelflightrec` is imported, it registers an OpenTelemetry `TraceExtractor` and `TraceInjector` with `flightrec`. On every `Recorder.Handle(ctx, rec)` call inside a scope, `flightrec` extracts the 16-byte `TraceID`, 8-byte `SpanID`, and `TraceFlags` from `ctx` into fixed-size value arrays on `bufferedEntry` (0 heap allocations). When a triggered scope flushes, `flightrec` attaches `flightrec.span_id` and restores the log-time `SpanContext` (with `FlagsSampled` set) into the `context.Context` passed to `Next.Handle`.
2. **Per-Trace Span Buffering (`SpanProcessor`):** `otelflightrec.SpanProcessor` buffers finished child spans in a lazily allocated per-trace ring buffer (`MaxBytesPerTrace`, default 256 KB) while keeping the local root span in a dedicated slot so child span volume can never evict the root span. Process-wide span memory is bounded by `MaxTotalBytes` (default 64 MB) using oldest-trace eviction, and abandoned unclosed traces are swept after `MaxTraceAge` (default 5 minutes).
3. **Bidirectional Trigger Coordination:** Linking `SpanProcessor` with `Recorder` via `ScopeObserver` ensures that a span error (`codes.Error`), recorded exception event, or slow trace triggers the active log `Scope`, and conversely an `error_log`, HTTP 5xx, gRPC server fault, panic, or `flightrec.Flush` triggers the trace buffer regardless of whether `otelhttp` wraps `flightrec.HTTPMiddleware` or vice versa.

## Code map and invariants

The root package holds the public API, one concern per file: `options.go`, `recorder.go` (the `slog.Handler`, stats and output records), `scope.go` (request lifecycle, per-scope ring and `MaxTotal` eviction), `trace.go`, `http.go` (middleware, transport and flush markers), `grpc.go`, `retain.go`, `notifier.go` and `journal.go` (journal, record encoding and recovery). Machinery with no dependency on those types lives in `internal/`: `traceid` (validation, `traceparent`, generation), `snapshot` (copies of `slog.Any` values), `notify` (wire format, signing, limiters) and `journal` (file format, writer, reader).

Invariants the code relies on:

- **Lock order:** `Scope.mu` before `recorderState.listMu`; the notifier hub's shard locks are never held while calling into a scope.
- **A scope writes once:** `End` closes the scope under `Scope.mu`; records that arrive later pass through to `Next`, and `trigger` reports a change only once, so notices and stats count each scope once.
- **Crash journal ordering:** a triggered scope writes its end entry only after its records reached `Next`, so a crash during a flush repeats records rather than losing them (at least once).
- **Untrusted input is bounded before use:** trace IDs (charset and length), flush markers (an allowlist), notifier messages (size, count, signature) and journal files (checksums, ownership) are validated where they enter.
