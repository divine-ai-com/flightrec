# Architecture & Design

## Goals

1. **Zero network/serialization cost for healthy debug logs:** Do not format JSON or write to `io.Writer` for `DEBUG`/`INFO` records unless a request triggers.
2. **Strict memory bounds:** Never allow a slow loris or traffic spike to exhaust heap memory (`MaxPerScope` + `MaxTotal`).
3. **Zero external dependencies:** Rely strictly on the Go standard library (`log/slog`, `net/http`, `sync/atomic`).

## Request Scope Lifecycle

1. **StartScope:** `HTTPMiddleware` extracts a trace ID from W3C `traceparent`, `X-Trace-Id`, or `X-Request-Id` (or generates a 128-bit random hex ID via a pooled `math/rand/v2.ChaCha8` generator). It attaches a `*Scope` to `r.Context()` and links the scope to the tail of the recorder's active-scope doubly-linked list.
2. **Handle:**
   - **No Scope in `ctx`:** Passes straight through to `Next`.
   - **`level >= PassLevel` (`WARN`, `ERROR`):** Emitted immediately to `Next` with `flightrec.trace_id` attached. If `level >= slog.LevelError`, the scope is marked triggered (`reason = "error_log"`).
   - **`BufferLevel <= level < PassLevel` (`DEBUG`, `INFO`):** Clones the `slog.Record` into a pooled circular buffer slice (`[]bufferedEntry`) inside the `Scope`.
3. **Memory Enforcement:**
   - **Per-Scope (`MaxPerScope`):** If `scope.bytes + recordSize > MaxPerScope`, the scope pops its oldest records from the ring buffer head, marks `flightrec.truncated = true`, and increments `RecordsDroppedPerScopeCap`.
   - **Process-Wide (`MaxTotal`):** `totalBytes` is tracked via an `atomic.Int64`. Only when `totalBytes > MaxTotal` does the recorder acquire `listMu` to evict the oldest active scopes from the head of the doubly-linked list, incrementing `ScopesEvictedTotalCap` and `RecordsDroppedTotalCap`.
4. **End:**
   - Checks HTTP status (`>= 500`) and elapsed duration (`>= SlowAfter`).
   - **If triggered:** Drains the ring buffer, returns the backing slice to `sync.Pool`, and emits all buffered records in original chronological order with their original timestamps, `WithAttrs`/`WithGroup` hierarchy, `flightrec.trace_id`, and `flightrec.flush_reason`.
   - **If healthy:** Clears and returns the backing slice to `sync.Pool` without formatting any buffered records, and emits one summary record (`msg="request completed"`, `flightrec.dropped_records`, `duration`, `status`).
