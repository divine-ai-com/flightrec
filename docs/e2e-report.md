# End-to-end test of v0.4.0

On 2026-11-15 the released modules were tested together in a three-service system under load: core `v0.4.0`, `grpcflightrec v0.3.0`, `otelflightrec v0.2.2`, `notifier/redisnotifier v0.3.0` and `notifier/natsnotifier v0.3.0`. They were installed from the module proxy, with no `replace` directives. This page records what was run, what held, and what was found. Every bug it found was fixed in [v0.5.0](https://github.com/haawklabs/flightrec/releases/tag/v0.5.0).

## Setup

- **Services:** gateway (HTTP), then orders (HTTP server, gRPC client), then inventory (gRPC unary and server streaming), plus an async background job in orders. Each runs as a separate process.
- **Wiring:** as the README describes it.
  - `HTTPMiddleware` and the `grpcflightrec` interceptors.
  - `NewTransport` on HTTP clients.
  - `Retain` (30 s) with `StartNotifier`, over Redis or NATS.
  - `otelflightrec.SpanProcessor` and `LogHandler`, exporting with OTLP/HTTP to a local receiver.
  - `StartJournal`.
- **Modes:**
  - OpenTelemetry propagation (otelhttp, otelgrpc).
  - Plain, with `X-Request-Id` forwarded as in `examples/cross-service`.
- **Load and checks:** a load generator with a mix of 30 scenarios: 5xx, error log, panic, slow, explicit `Flush`, gRPC `Internal`, `Unavailable` and deadline, streaming, and async failure. A verifier checks every request in every service's JSON output, and the spans and OTLP log records.
- **Machine:** Linux, 4 vCPU, 31 GB, Go 1.27.4. It was shared with other work, so latency-sensitive results are noted as such.

## What held

| Check | Result |
| --- | --- |
| Failed requests, every scenario | Every buffered line written exactly once, in order, with one flush reason and the right trace ID. Healthy requests wrote exactly one summary line |
| Cross-service flush | Header and trailer path (`downstream:`), notifier path (`remote:`) and the async case all worked, over Redis and NATS |
| Soak: 10 minutes, 300 req/s, 1,000 concurrent clients | 180,001 requests, 0 shed. RSS per service held between 101 and 106 MB from the first minute to the last (40 MB at start). 0 garbled lines among 678,000 |
| Higher load | 60,001 requests at 1,000 req/s and 52,696 at 3,000 req/s offered: 0 garbled lines, no deadlock, `RecordsFlushed` exact |
| `Stats` | `RecordsFlushed` equalled the flushed lines in the logs in every run. `ScopesStarted` equalled the requests sent |
| Spans | Healthy traces exported 1 span per service. Failed traces exported every span. Every flushed log line had exactly one OTLP log record with the same trace and span ID |
| `kill -9` under 200 req/s | orders: 21 requests recovered of 21 in flight. inventory: 24 of 24. Each had the full prefix of its records, in order, tagged `crash_recovery`. No request the client saw succeed was recovered. Three kills 1.2 s apart: 70 requests recovered, 0 duplicates. Recovery took 40 to 72 ms |
| Memory limits | `MaxPerScope` truncation marked on the first flushed line, with exact `dropped_records`. `MaxTotal` eviction kept RSS at 82 MB with 200 KB per request at 300 req/s |
| Broker outages (20 s) | Redis: flushing resumed about 8 s after Redis returned. NATS: within about 3 s. No goroutine growth, request latency unaffected |
| Hostile input | Forged `Flightrec-Flush` request headers ignored. Invalid `X-Request-Id` values rejected. 8 kinds of malformed marker from a downstream rejected. Malformed notifier messages rejected and counted |
| slog equivalence | For 13 call shapes (`With`, `WithGroup`, nested groups, `LogValuer`, duplicate keys, `AddSource`, `ReplaceAttr`), flushed output equalled plain `slog` output apart from the `flightrec.*` attributes |

## What it found

| Issue | Finding |
| --- | --- |
| [#83](https://github.com/haawklabs/flightrec/issues/83) | A failure in the service that generated the trace ID never reached the notifier, even when the ID was forwarded |
| [#84](https://github.com/haawklabs/flightrec/issues/84) | A request header trace ID (a proxy's `X-Request-Id`, or a malformed `traceparent`) overrode the active OpenTelemetry span |
| [#85](https://github.com/haawklabs/flightrec/issues/85) | otelflightrec triggered on client span errors such as `NotFound`, which flushed every service for a 404 |
| [#86](https://github.com/haawklabs/flightrec/issues/86) | A failed request that lost every buffered record wrote nothing |
| [#87](https://github.com/haawklabs/flightrec/issues/87) | Crash recovery omitted records too large for the journal without marking the request truncated |
| [#88](https://github.com/haawklabs/flightrec/issues/88) | Stale versions in the quickstart, and limits the README did not state |

Also observed but not confirmed as a flightrec problem: with `SlowAfter` set, a downstream stall of a few seconds makes every in-flight request slow, and writing all of their buffers adds load while the system recovers. Two runs at 400 to 500 req/s did not recover while another job saturated the machine; a replicate recovered.
