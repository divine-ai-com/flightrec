# Reference

Options, trace ID rules, log attributes, flush reasons and limits. The API itself is documented on [pkg.go.dev](https://pkg.go.dev/github.com/divine-ai-com/flightrec).

## Choose what is buffered

| Option | Default | Effect |
| --- | --- | --- |
| `PassLeveler` | `slog.LevelWarn` | Records at or above this level inside a scope are written immediately |
| `BufferLeveler` | `slog.LevelDebug` | Records at or above this level, and below `PassLeveler`, are buffered |
| `SlowAfter` | off | Requests that take this long or longer are kept |
| `DisableHealthySummary` | `false` | Turns off the summary line for healthy requests |

Set both levels explicitly, as in the quick start. Outside a scope, records are delegated to `Next.Enabled` and `Next.Handle` unchanged; inside a scope, flushed records bypass `Next`'s level so debug detail for failed requests is never lost (see [Level Filtering](https://pkg.go.dev/github.com/divine-ai-com/flightrec#hdr-Level_Filtering)).

## Limit memory

| Option | Default | When the limit is reached |
| --- | --- | --- |
| `MaxPerScope` | 256 KB | The request drops its oldest records, and a single record larger than the limit is dropped by itself. Its flushed output is marked `flightrec.truncated` |
| `MaxTotal` | 64 MB | The oldest requests lose their buffers |

A failed request that lost every buffered record writes one `buffered records dropped` line at Warn level, with `flightrec.truncated` and `flightrec.dropped_records`, so the loss is visible in the log and not only in `Stats`.

## Trace IDs from requests

The trace ID is taken from the trace already in the request context if there is one (OpenTelemetry middleware or interceptors that run before flightrec, or `Options.TraceExtractor`), so logs, spans and cross-service flush all use the same trace. Either middleware order works with `otelflightrec`: when OpenTelemetry middleware runs inside flightrec's, the server span's trace replaces a generated or header ID as soon as the span starts, and a header ID stays an alias, so services that forwarded it still match the request for cross-service flush. Otherwise it is taken from `traceparent`, then `X-Trace-Id`, then `X-Request-Id`, and finally generated. `traceparent` must be valid as the W3C Trace Context specification defines it (lowercase hex, non-zero IDs). `X-Trace-Id` and `X-Request-Id` values are used only when they are 1 to 128 bytes of letters, digits and `_ . : - / + = @`; otherwise the next source is tried.

A client can choose these headers. On services that receive requests from outside your system, such as a public API, set `IgnoreRequestTraceIDs: true` so trace IDs come only from your own tracing or are generated. The same applies to the `Flightrec-Flush` response header, which `DisableFlushMarker: true` turns off.

## Monitor it

`Recorder.Stats()` returns a snapshot you can export to your metrics system.

```go
st := rec.Stats()
slog.Info("flightrec",
	"active_scopes", st.ActiveScopes,
	"buffered_bytes", st.BufferedBytes,
	"scopes_flushed", st.ScopesFlushed,
	"dropped_per_scope_cap", st.RecordsDroppedPerScopeCap,
	"dropped_total_cap", st.RecordsDroppedTotalCap,
)
```

## What appears in your logs

| Attribute | On | Meaning |
| --- | --- | --- |
| `flightrec.trace_id` | every record in a request | The request's trace ID |
| `flightrec.span_id` | records logged inside an active OpenTelemetry span | The 16-hex-char ID of the span active at log time |
| `flightrec.flush_reason` | flushed records | Why the request was kept (see below) |
| `flightrec.truncated`, `flightrec.dropped_records` | first flushed record if records were lost, or a `buffered records dropped` line if none is left; after a crash, the first recovered record and the summary line | Records are missing: a memory limit was hit, the journal ring overwrote them, or they were too large to journal. `dropped_records` counts them where known |
| `flightrec.recovered`, `flightrec.recovered_host`, `flightrec.recovered_process_start` | records recovered after a crash | Written by the next start of a killed process (`StartJournal`) |
| `flightrec.recovered_scope` | records recovered for a request with no trace ID | Groups the lines of that request |
| `duration`, `status` (or `grpc.code`), `method`, `path`, `flightrec.dropped_records` | the summary line | Outcome of a healthy request |
| `flightrec.dropped_spans` | the exported root span of a healthy trace (`otelflightrec`, `HealthyExportRootOnly`) | Child spans dropped because the trace was healthy |
| `flightrec.request_id` | records and the summary line of a request whose header ID was replaced by its OpenTelemetry span's trace | The request header ID the request arrived with |
| `flightrec.retained_records` | the summary line, with `Retain` | Records kept for a later remote flush instead of being dropped |

| Trigger | `flightrec.flush_reason` |
| --- | --- |
| A record at Error level or above was logged | `error_log` |
| Response status 500 or above | `http_500`, `http_502`, `http_503`, `http_504`, `http_5xx` |
| A gRPC server fault (`Unknown`, `DeadlineExceeded`, `Unimplemented`, `Internal`, `Unavailable`, `DataLoss`) or non-gRPC error (client codes like `NotFound`, `InvalidArgument`, `Canceled` do not flush; `DeadlineExceeded` does flush) | `grpc_error` |
| The handler panicked, except `http.ErrAbortHandler` (the panic is re-raised after the flush) | `panic` |
| The request took `SlowAfter` or longer | `slow_request` |
| A span in the trace ended with `codes.Error` or recorded an `"exception"` event, or exceeded `SlowAfter` (`otelflightrec`). A client span whose remote answered with a client error (a gRPC code such as `NotFound`, or an HTTP status below 500) does not trigger: that service decided the request was not a failure | `span_error`, `span_exception`, `slow_trace` |
| The code called `flightrec.Flush(ctx, reason)` or `otelflightrec.FlushTrace(ctx, sp, reason)` | the reason given |
| A service this request called kept its logs (`NewTransport`, gRPC client interceptors) | `downstream:<reason>` |
| Another service, or another scope in this process, kept its logs for the same trace (`StartNotifier`, `Retain`) | `remote:<reason>` |
| The code called `Recorder.FlushTrace(ctx, traceID, reason)` | the reason given |
| The process was killed while the request was in flight, and the next start recovered it (`StartJournal`) | `crash_recovery` |

## Limits

- **Spans stay in one process.** Cross-service flush keeps logs in other services. `otelflightrec` keeps spans only in the process where the scope was triggered; a service flushed by a remote notice has already exported or dropped its spans.
- **Cross-service flush is best effort.** See [Keep logs across services](cross-service.md).
- **A hard kill needs the journal.** Panics are flushed. After `SIGKILL` or an out-of-memory kill, buffered records are recovered on the next start only with [`StartJournal`](crash-recovery.md), and only those still in its ring.
- **Logs after a request ends are written at once.** A record logged with the request's context after its scope ended (a goroutine using `context.WithoutCancel(r.Context())`, say) is written immediately if it is at or above `BufferLeveler`, even below `PassLeveler`, since there is no buffer left to hold it.
- **The context must be passed.** `slog.Debug(...)` without a context has no request to attach to and is handled as a log outside a request.
- **Values are captured by reference until a request ends.** Attributes implementing `slog.LogValuer` are resolved when buffered. A pointer, map or slice logged with `slog.Any` shows its state when the request ends; with `Retain`, a copy is taken then, which costs time on healthy requests that log many distinct pointers.
