# flightrec

**Debug logs for the requests that fail. One line for the ones that don't.**

[![CI](https://github.com/divine-ai-com/flightrec/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/divine-ai-com/flightrec/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/divine-ai-com/flightrec.svg)](https://pkg.go.dev/github.com/divine-ai-com/flightrec)
[![Release](https://img.shields.io/github/v/release/divine-ai-com/flightrec?sort=semver)](https://github.com/divine-ai-com/flightrec/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

flightrec is a `log/slog` handler that works like an aircraft flight recorder. It keeps each request's debug and info logs in memory and writes them only if that request fails or runs slowly. A healthy request leaves a single summary line.

A healthy request and a failed one, with the same handler code:

```jsonc
// POST /checkout -> 200. Three debug and info records were buffered, then discarded.
{"level":"INFO","msg":"request completed","flightrec.trace_id":"req-1041","duration":37660,"flightrec.dropped_records":3,"status":200,"method":"POST","path":"/checkout"}

// POST /checkout -> 502. The error is written at once, then everything that led up to it.
{"level":"ERROR","msg":"payment provider timed out","flightrec.trace_id":"req-1042","attempt":3}
{"level":"DEBUG","msg":"loaded cart","flightrec.trace_id":"req-1042","flightrec.flush_reason":"error_log","items":3}
{"level":"DEBUG","msg":"reserved inventory","flightrec.trace_id":"req-1042","flightrec.flush_reason":"error_log","warehouse":"eu-central-1"}
{"level":"INFO","msg":"calling payment provider","flightrec.trace_id":"req-1042","flightrec.flush_reason":"error_log","provider":"stripe"}
```

This is real output with the `time` field removed. Every record keeps its original timestamp.

- Standard library only. The module has no dependencies.
- Three lines to adopt. Existing `slog` calls that pass a context stay as they are.
- Memory is capped per request and for the whole process, and every drop is counted.

## Install

```bash
go get github.com/divine-ai-com/flightrec
```

Requires Go 1.24 or later.

## Quick start

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
		Next:        slog.NewJSONHandler(os.Stdout, nil), // where logs are written
		PassLevel:   slog.LevelWarn,                      // written immediately
		BufferLevel: slog.LevelDebug,                     // buffered per request
		SlowAfter:   2 * time.Second,                     // slow requests are kept too
	})
	slog.SetDefault(slog.New(rec))

	mux := http.NewServeMux()
	mux.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		slog.DebugContext(r.Context(), "loaded cart", "items", 3)
		w.WriteHeader(http.StatusOK)
	})

	http.ListenAndServe(":8080", flightrec.HTTPMiddleware(rec)(mux))
}
```

To keep the logs of a request that did not fail, call `flightrec.Flush(ctx, "payment mismatch")` anywhere in the request.

For background jobs and gRPC, see the [quickstart](docs/quickstart.md).

## When a request is kept

A request's buffer is written when the request ends if any of these happened:

| Trigger | `flightrec.flush_reason` |
| --- | --- |
| A record at Error level or above was logged | `error_log` |
| The response status was 500 or above, or the gRPC handler returned an error | `http_500`, `http_502`, `http_503`, `http_504`, `http_5xx`, `grpc_error` |
| The handler panicked (the panic is re-raised after the flush) | `panic` |
| The request took `SlowAfter` or longer | `slow_request` |
| The code called `flightrec.Flush(ctx, reason)` | the reason given |

Records at or above `PassLevel` are never buffered. Logs outside a request, such as startup messages, go straight to `Next`.

## Memory limits

| Option | Default | When the limit is reached |
| --- | --- | --- |
| `MaxPerScope` | 256 KB | The request drops its oldest records and its flushed output is marked `flightrec.truncated` |
| `MaxTotal` | 64 MB | The oldest requests lose their buffers |

`Recorder.Stats()` reports active scopes, buffered bytes, and a counter for every kind of drop.

## Performance

Measured on v0.1.0 with Go 1.27.4, linux/amd64, Intel Xeon 2.20 GHz, 4 vCPU, median of 5 runs. Reproduce with:

```bash
go test -bench=. -benchmem -run='^$' . && go run ./cmd/demo
```

Cost of one request that makes 20 log calls with 3 attributes each:

| Path | Time per log call | Allocations per request |
| --- | ---: | ---: |
| Plain slog, debug logging off | 16 ns | 0 |
| **flightrec, healthy request (buffered, then discarded)** | **790 ns** | **4** |
| Plain slog, debug logging on (JSON to `io.Discard`) | 1,260 ns | 0 |
| flightrec, records at Warn or above (written immediately) | 1,650 ns | 4 |
| flightrec, failed request (buffered, then written) | 2,170 ns | 4 |

How to read this:

- Against always-on debug logging, a healthy request costs about 37% less CPU and writes one line.
- Against debug logging turned off, buffering is not free. Each buffered call costs about 790 ns, most of it the timestamp and caller lookup that `slog` performs for any enabled record. That is the price of having the detail when a request fails.
- Calls with more than 5 attributes allocate once per call, as they do in `slog` itself (24 allocations per request against 20 for plain slog, measured with a child logger and 6 attributes).

Log volume in the bundled demo: 1,000 requests, 1% failing, 20 debug records each.

| | Lines | Bytes |
| --- | ---: | ---: |
| Plain slog at Debug | 20,010 | 3.69 MB |
| flightrec | 1,200 | 275 KB |

The demo is synthetic: every record is at Debug level, so the 92.5% reduction follows from the 1% failure rate. All 10 failed requests keep their 20 records in order.

## How it compares

| Tool | What it does well | How flightrec differs |
| --- | --- | --- |
| [slogbox](https://github.com/alexrios/slogbox), [slog-fingerscrossed](https://github.com/hectorj/slog-fingerscrossed) | Small, focused buffering handlers for `slog` | Per-request scopes with trace IDs, two memory caps with drop counters, and triggers for status, panics and slow requests |
| AWS Lambda Powertools | Per-invocation log buffering with flush on error | Runs in any Go HTTP or gRPC server, not only on Lambda |
| OpenTelemetry Collector tail sampling | Trace-aware decisions across services | Drops healthy detail inside the process, before it is serialized or sent |
| `runtime/trace.FlightRecorder` | Low-overhead recording of Go runtime events | Records application logs, not scheduler and GC events |

## Limits

- **Logs only.** Traces are planned for v0.2.
- **One service.** A failure does not yet flush the buffers of upstream or downstream services. Planned for v0.3.
- **A hard kill loses the buffer.** Panics are flushed. `SIGKILL` and out-of-memory kills are not, until v0.4.
- **The context must be passed.** Only `slog.DebugContext(ctx, ...)` style calls are buffered. `slog.Debug(...)` has no request to attach to and is handled as a log outside a request.
- **Values are captured by reference.** A pointer logged with `slog.Any` shows its state at flush time.

Known defects in v0.1.0 are tracked in the [v0.1.1 milestone](https://github.com/divine-ai-com/flightrec/milestone/1).

## Stability

flightrec follows semantic versioning. Before 1.0, a minor release may change the API; such changes are listed first in the [changelog](CHANGELOG.md). 

## Documentation

- [Quickstart](docs/quickstart.md)
- [Design](docs/design.md)
- [Troubleshooting](docs/troubleshooting.md)
- [API reference](https://pkg.go.dev/github.com/divine-ai-com/flightrec)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). To report a vulnerability, follow the [security policy](SECURITY.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
