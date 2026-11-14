# flightrec

**Full debug detail for failed requests, near-zero cost for healthy ones.**

`flightrec` is a zero-dependency Go library that wraps any standard `log/slog.Handler`. It keeps per-request `DEBUG` and `INFO` logs in a bounded in-memory ring buffer and flushes them only when a request fails or runs slowly. Healthy requests discard their buffered debug logs and emit a single summary line.

---

## 60-Second Example

```go
package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/divine-ai-com/flightrec"
)

func main() {
	rec := flightrec.New(flightrec.Options{
		Next:        slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLevel:   slog.LevelWarn,  // WARN and ERROR are written immediately
		BufferLevel: slog.LevelDebug, // DEBUG and INFO are buffered per request
		SlowAfter:   2 * time.Second, // flush if a request takes >= 2s
		MaxPerScope: 256 << 10,       // 256 KB cap per request
		MaxTotal:    64 << 20,        // 64 MB cap across the whole process
	})
	slog.SetDefault(slog.New(rec))

	mux := http.NewServeMux()
	mux.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		slog.DebugContext(ctx, "loaded cart", "items", 3)

		// Force this request's buffer to flush on a domain condition:
		if r.Header.Get("X-Force-Audit") == "1" {
			flightrec.Flush(ctx, "payment mismatch")
		}
		w.WriteHeader(http.StatusOK)
	})

	http.ListenAndServe(":8080", flightrec.HTTPMiddleware(rec)(mux))
}
```

---

## Measured Numbers

Reproduce both tables below with one command:

```bash
go run ./cmd/demo && go test -bench=. -benchmem ./...
```

### 1. Log Volume & Wall-Time Reduction (`cmd/demo`, 1,000 requests, 1% error rate, 20 debug logs/req)

| Mode | Log Lines | Log Bytes | Total Wall Time | Failed Request Fidelity |
| :--- | ---: | ---: | ---: | :--- |
| Plain `slog.JSONHandler` (`LevelDebug`) | 20,010 | 3,690,174 B (3.69 MB) | 58.36 ms | 100% |
| **`flightrec` (`v0.1.0`)** | **1,200 (-94.0%)** | **275,351 B (-92.5%)** | **25.46 ms (2.29× faster)** | **100% (all 20 steps in order + error)** |

### 2. Per-Request & Per-Log Overhead (`go test -bench=. -benchmem`, 20 logs/request on Intel Xeon 2.20 GHz)

| Benchmark Path (20 log calls per op) | Time / op (20 logs) | Time / log call | Bytes / op | Allocs / op |
| :--- | ---: | ---: | ---: | ---: |
| Plain `slog.JSONHandler` (serializes all 20 logs) | 25,083 ns | ~1,254 ns | 0 B | 0 |
| **`flightrec` Buffered & Dropped (healthy request)** | **15,819 ns** | **~791 ns (-37%)** | **328 B** | **4 (0 per log)** |
| `flightrec` Immediate Pass-Through (`WARN+`) | 33,273 ns | ~1,664 ns (+33%) | 328 B | 4 |
| `flightrec` Triggered & Flushed (failed request) | 43,387 ns | ~2,169 ns (+73%) | 7,208 B | 4 |

*Note on the numbers:* On the 99% healthy path, `flightrec` is faster than plain `slog.JSONHandler` because dropped debug records skip JSON formatting and I/O. On the 1% failed path (or when every log is `WARN+`), `flightrec` pays the cost of cloning records into the ring buffer before flushing them (`~2.17 µs/log` vs `~1.25 µs/log`).

---

## How Triggers Work

A request scope is flushed when the request ends if any of these five triggers occurred:

1. **Error log (`error_log`):** Any record at or above `slog.LevelError` was logged in the request context.
2. **HTTP 5xx or gRPC error (`http_5xx` / `grpc_error`):** The handler returned HTTP status `>= 500` or a non-nil gRPC error.
3. **Panic (`panic`):** The handler panicked. `flightrec` flushes the buffered records tagged with `flightrec.flush_reason="panic"` and re-panics so your crash recovery middleware still runs.
4. **Slow request (`slow_request`):** Request duration met or exceeded `Options.SlowAfter`.
5. **Explicit flush:** Application code called `flightrec.Flush(ctx, "reason")`.

---

## How This Compares

| Tool | What it does well | Where `flightrec` differs |
| :--- | :--- | :--- |
| [`alexrios/slogbox`](https://github.com/alexrios/slogbox) & [`hectorj/slog-fingerscrossed`](https://github.com/hectorj/slog-fingerscrossed) | Clean single-buffer or fingers-crossed `slog.Handler` implementations for Go | `flightrec` adds per-request trace-ID isolation, dual memory caps (`MaxPerScope` + `MaxTotal`) with eviction metrics, slow-request & HTTP/gRPC status triggers, and (in v0.2–v0.4) OpenTelemetry span buffering, cross-service flush, and `mmap` crash recovery |
| **AWS Lambda Powertools Logger** | Built-in request log buffering and flush-on-error for serverless functions | Tied to AWS Lambda; `flightrec` works in any Go HTTP/gRPC server or background worker |
| **OpenTelemetry Collector Tail Sampling** | Full trace-aware sampling across services | Application still serializes and ships 100% of telemetry over the network to the collector first; `flightrec` drops healthy debug noise in-process before serialization |
| **Go 1.25+ `runtime/trace.FlightRecorder`** | Low-overhead ring buffer for Go runtime scheduler/GC execution traces | Captures Go runtime events only, not application `slog` logs or OpenTelemetry spans |

---

## Current Limits (v0.1.0)

We state our current boundaries plainly:

1. **Logs only in v0.1:** OpenTelemetry `SpanProcessor` integration arrives in **v0.2**.
2. **Single-service in v0.1:** Cross-service flush propagation (flushing upstream/sibling services when a downstream call fails) arrives in **v0.3**.
3. **Hard process kills (`SIGKILL` / OOM) lose in-flight buffers in v0.1:** Panics are recovered, flushed, and re-raised, but an uncatchable `kill -9` or OOM kill before a request finishes loses that request's in-memory buffer until **v0.4** (optional `mmap` file recovery on restart).
4. **Context propagation required:** Like OpenTelemetry, buffered logs must use `slog.DebugContext(ctx, ...)` / `slog.InfoContext(ctx, ...)`. Calls without `ctx` (such as `slog.Debug(...)`) have no request scope and pass straight through to `Next`.

---

## Documentation

- [Quickstart Guide](docs/quickstart.md)
- [Architecture & Design](docs/design.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Changelog](CHANGELOG.md)

## License

Apache-2.0 — see [LICENSE](LICENSE).
