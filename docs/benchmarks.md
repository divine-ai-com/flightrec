# Benchmarks

Measured on `main` at 010c339 (the code released as v0.3.0) by the [Benchmarks workflow](https://github.com/divine-ai-com/flightrec/actions/runs/43714183457) on a GitHub Actions runner (AMD EPYC 9V74, 4 vCPU, linux/amd64, Go 1.27.4): 10 alternating runs, benchstat medians. Absolute numbers depend on the machine; the ratios are what carry over. Reproduce locally with:

```bash
go test -bench=. -benchmem -run='^$' . && go run ./examples/demo   # from the repository root
```

Cost of one request that makes 20 log calls with 3 attributes each:

| Path | Time per log call | Allocations per request |
| --- | ---: | ---: |
| Plain slog, debug logging off | 7 ns | 0 |
| **flightrec, healthy request (buffered, then discarded)** | **459 ns** | **4** |
| flightrec, healthy request kept for `Retain` | 545 ns | 8 |
| Plain slog, debug logging on (JSON to `io.Discard`) | 675 ns | 0 |
| flightrec, records at Warn or above (written immediately) | 955 ns | 4 |
| flightrec, failed request (buffered, then written) | 1,195 ns | 4 |

How to read this:

- Against always-on debug logging, a healthy request costs about 32% less CPU and writes one line.
- Against debug logging turned off, buffering is not free. Each buffered call costs about 460 ns, most of it the timestamp and caller lookup that `slog` performs for any enabled record. That is the price of having the detail when a request fails.
- Calls with more than 5 attributes allocate once per call, as they do in `slog` itself (24 allocations per request against 20 for plain slog, measured with a child logger and 6 attributes).
- `Retain` copies `slog.Any` values when a request ends. With one struct pointer per call (logged 20 times, encoded once) a retained request takes 13.3 µs against 8.6 µs discarded; many distinct pointers per request cost more.
- With a notifier attached, starting and ending a scope costs roughly 100 to 120 ns more under parallel load (759 ns against 641 ns per minimal request, with about 6% run-to-run noise).

Log volume in the bundled demo, 1,000 requests with 1% failing and 20 debug records each:

| | Lines | Bytes |
| --- | ---: | ---: |
| Plain slog at Debug | 20,010 | 3.69 MB |
| flightrec | 1,200 | 275 KB |

The [end-to-end test of v0.4.0](e2e-report.md) ran three services for 10 minutes at 300 req/s with flat memory (101 to 106 MB per service) and recovered every in-flight request after `kill -9`.

The demo is synthetic: every record is at Debug level, so the 92.5% reduction follows from the 1% failure rate. All 10 failed requests keep their 20 records in order.

## Tracing with otelflightrec

A trace of one root span and ten child spans, through `BatchSpanProcessor` and the OTLP/HTTP exporter (protobuf, no compression) to a local server. Measured by the Benchmarks workflow on a GitHub Actions runner ([run](https://github.com/divine-ai-com/flightrec/actions/runs/43732735249): AMD EPYC 7763, 4 vCPU, Go 1.27.4), 10 runs, benchstat medians. CPU is the process's CPU time per trace across all threads, export included; wall time is lower because the exporter runs on other cores:

| Path | CPU per trace | Wall time per trace | Spans exported | Bytes exported |
| --- | ---: | ---: | ---: | ---: |
| Plain OpenTelemetry, every span exported | 79.5 µs | 33.1 µs | 11 | 1,974 |
| **otelflightrec, healthy trace, `HealthyExportRootOnly` (default)** | **34.9 µs** | **20.3 µs** | **1** | **110** |
| otelflightrec, healthy trace, `HealthyExportNone` | 21.5 µs | 15.5 µs | 0 | 0 |
| otelflightrec, failed trace (every span exported) | 99.9 µs | 39.7 µs | 11 | 2,426 |

A healthy trace uses 56% less CPU than exporting it and sends 94% fewer bytes; a failed trace uses 26% more, since its spans are buffered before they are exported and carry the flush attributes. With a share `f` of traces failing, the default policy costs about `35 + 65f` µs against 79.5 µs, so it saves CPU while fewer than about two thirds of traces fail. The baseline exports every span; against head sampling, which drops whole traces at random, the saving is smaller, but sampling also drops the traces that fail.

Against a no-op exporter, which measures the processor alone, flightrec adds 3 to 4 µs per trace ([run](https://github.com/divine-ai-com/flightrec/actions/runs/43732004424): 6.2 µs plain, 9.5 µs healthy, 10.4 µs failed); that work pays for itself only when export is real. Reproduce with `cd benchmarks/otelexport && go test -bench=. -benchmem -run='^$'`.
