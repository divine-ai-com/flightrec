# Proposal 0001: Cross-service flush (v0.3)

| | |
| --- | --- |
| Status | Implemented in v0.3.0 (#33, #48) |
| Milestone | v0.3.0 |
| Scope | core module, `grpcflightrec`, two new notifier modules |

## Problem

flightrec decides per process. When service C fails a request, C keeps its debug logs, but services A and B, which handled the same request upstream, see a successful downstream call or a generic error and discard theirs. The person debugging the incident gets the last hop only.

Two cases need different mechanisms:

- **Case A, the failure is visible to the caller.** C returns an error or a 5xx, and B is still inside its own request when the response arrives.
- **Case B, the caller already finished.** B called C asynchronously, or B returned before C failed (a queue consumer, a fire-and-forget call, a parallel call to a sibling). B's scope has already ended.

## Goals

- In a chain A to B to C, one failure in C produces complete logs from all three, in both cases, when the services share a trace ID.
- No new dependency in the core module.
- Nothing a client sends can force a server to flush. Only responses from services you call, and a notifier you run, can trigger a flush.
- Memory for retained scopes is bounded and counted, like the existing buffers.
- Best effort, and documented as such: a scope that has left its retention window is gone.

## Non-goals

- Guaranteed delivery. A crashed service, a dropped notifier message or an expired scope means missing logs, not an error.
- Flushing spans across services. In Case A, `otelflightrec` follows the log scope it is linked to, so spans are kept in-process when a downstream marker triggers the scope. In Case B, only logs are flushed: the span processor has already finalized and dropped a healthy trace when its scope ended. Cross-process span retention is a later step.
- Propagating trace context. flightrec reads the trace ID that OpenTelemetry or your own headers already carry.
- Failures decided after the response headers are sent, and requests that time out with no response at all, are not covered by Case A. They are counted, and Case B covers them when a notifier is configured.

## Design

### Case A: flush marker on responses (shipped in #47)

1. When a scope is triggered before its response headers are written, the server middleware sets:

   ```
   Flightrec-Flush: http_502
   ```

   The value is a built-in reason (`error_log`, `http_5xx` family, `panic`, `slow_request`, `grpc_error`, `span_error`, `span_exception`, `slow_trace`, `explicit_flush`). Custom reasons passed to `Flush` are sent as `explicit_flush`, so application text never leaves the process. `downstream:` and `remote:` prefixes are removed, so the value names the original failure and does not grow at each hop. A request already past `SlowAfter` when its headers are sent carries `slow_request`.

2. `Options.DisableFlushMarker` turns the header off for services whose responses go to clients outside your system. Gateways in front of internal services should strip `Flightrec-Flush` from responses to external clients; the README documents this.

3. A client wrapper reads it:

   ```go
   client := &http.Client{Transport: flightrec.NewTransport(http.DefaultTransport)}
   ```

   `NewTransport(next, opts ...TransportOption)` returns an `http.RoundTripper`. When a response carries `Flightrec-Flush` with a value from the built-in list, it triggers the scope in the request's context with reason `downstream:<value>`. Any other value is ignored and counted, because the header comes from another service and reaches logs. The header is removed from the response before it is returned, so application code that proxies the response cannot forward it. A request without a scope in its context is unaffected. The variadic options leave room for a later retry-aware policy (a successful retry should eventually not flush upstream); v0.3 ships with none.

4. Because B's scope is now triggered, B's own response to A carries the marker, and the flush travels up the chain.

5. **gRPC.** `grpcflightrec` server interceptors set `flightrec-flush` as a response trailer only (never a header), with the same value rules. `UnaryClientInterceptor` reads the trailer through a `grpc.Trailer` call option. `StreamClientInterceptor` wraps the `ClientStream` and reads `Trailer()` when the stream ends. Incoming `flightrec-flush` metadata is ignored.

6. **Security.** The server middleware never reads `Flightrec-Flush` from incoming requests and removes it from a clone of the request before the handler runs. Operators who do not trust a downstream service can omit `NewTransport` for that client.

### Trace ID hygiene (prerequisite for Case B)

Case B looks scopes up by trace ID, and today `X-Trace-Id` and `X-Request-Id` are accepted as sent. A client could send many healthy requests under one ID, then one failing request, and flush them all across the fleet. Before retention ships:

- trace IDs taken from `X-Trace-Id` and `X-Request-Id` are limited to 128 bytes of `[A-Za-z0-9_.:-]`; others are replaced with a generated ID;
- at most 8 retained scopes are kept per trace ID; older ones are evicted and counted;
- the README states that edge services should not take trace IDs from untrusted clients.

### Case B: retention and notifier

1. **Retention.** With `Options.Retain` set (for example `30 * time.Second`), a healthy scope that ends is not discarded. Its buffer moves to a retained set with its own byte limit `Options.MaxRetained` (default 16 MB):
   - the ring is compacted to exact size and the pooled ring returned, so a retained scope costs only its records;
   - values of kind `slog.KindAny` are rendered to strings when the scope is retained, because the objects they point to may be reused by later requests within the window (`LogValuer`s are already resolved at log time);
   - the request context is not kept; a later flush uses `context.Background()` with the scope's trace context injected;
   - the healthy summary line is written at the end of the request as today, and `ScopesHealthyDropped` and `RecordsHealthyDropped` are counted when a retained scope expires or is evicted, not when it is retained.

   The effective window under load is `MaxRetained / (requests per second x bytes per retained scope)`; the docs state the formula and the stats needed to check it.

2. **Index.** Retained scopes are held in a map from trace ID to a short list (a service can be called more than once in one trace), plus an age-ordered list for expiry and eviction. Active scopes are indexed by trace ID only while a notifier is attached, and `Scope.AdoptGeneratedTraceID` re-keys the entry. `StartScope` and `End` contention is benchmarked against v0.2.

3. **Notifier.** The core owns the message format; a backend is only a byte bus:

   ```go
   // Notifier carries flush notices between processes. Publish must not block
   // for long; Subscribe blocks until ctx is done or the connection fails.
   type Notifier interface {
       Publish(ctx context.Context, msg []byte) error
       Subscribe(ctx context.Context, deliver func(msg []byte)) error
   }
   ```

   The core encodes a versioned message with the origin process ID, the reason and one or more trace IDs, so backends need no knowledge of flightrec and messages can be batched.

4. **Lifecycle.** The notifier is attached explicitly, not through `Options`:

   ```go
   stop := rec.StartNotifier(ctx, n) // runs until ctx is done or stop is called
   defer stop()
   ```

   `StartNotifier` starts one publisher goroutine and one subscriber goroutine. The subscriber calls `Subscribe` and re-subscribes with exponential backoff (100 ms to 30 s) when it returns an error.

5. **Publishing.**
   - Only locally originated triggers publish. A scope triggered with a `downstream:` or `remote:` reason does not, which prevents echo storms between services.
   - The trace ID is queued when the scope is triggered, not when it ends, so long requests do not publish after upstream windows have expired.
   - The queue is bounded (1,024 entries), drained by the publisher goroutine with a 1 s timeout per `Publish`, and rate limited to 100 messages per second with a burst of 200. Drops and errors are counted. The rate is internal in v0.3.

6. **Delivery.**
   - Messages from this process are ignored as remote notices, but the process still flushes its own other retained scopes for that trace through a direct local lookup when a scope is triggered.
   - A delivered trace ID flushes matching retained scopes and triggers matching active scopes, with reason `remote:<reason>`.
   - Delivered trace IDs are kept in a small recent set (4,096 entries, 30 s). It deduplicates repeated notices, and a scope that starts or ends shortly after a notice for its trace is triggered too.
   - Delivery work runs on the subscriber goroutine through a bounded queue, with a per-process budget of remote flushes per second (default 50); excess notices are dropped and counted.

7. **Backends**, each a separate module so the core stays dependency-free. Both take a channel or subject prefix option so environments sharing a server do not cross-talk:
   - `notifier/redisnotifier` using Redis pub/sub (`github.com/redis/go-redis/v9`);
   - `notifier/natsnotifier` using NATS core subjects (`github.com/nats-io/nats.go`).

   An in-memory notifier for tests and the demo ships in the core module as `notifier/memnotifier`.

### Order and timestamps

Flushed records keep their original timestamps and are written in order within a scope. Records from different services are interleaved by the log backend using those timestamps, as today. `downstream:` and `remote:` reasons show why a service that succeeded kept its logs.

## Public API changes

All additions; nothing existing changes behavior unless the new options are used.

| Addition | Module |
| --- | --- |
| `FlushHeader`, `Options.DisableFlushMarker` (shipped in #47) | core |
| `NewTransport(next, ...TransportOption) http.RoundTripper`, `TransportOption` | core |
| `MarkerValue(reason) (string, bool)` for senders and `ValidMarker(value) bool` for receivers | core |
| `Options.Retain`, `Options.MaxRetained` | core |
| `Notifier`, `Recorder.StartNotifier(ctx, n) (stop func())` | core |
| `notifier/memnotifier` | core module, new package |
| `Stats` fields: `FlushMarkersSent`, `FlushMarkersMissed` (shipped), `DownstreamTriggers`, `MarkersRejected`, `RetainedScopes`, `RetainedBytes`, `RetainedOldestAge`, `RetainedExpired`, `RetainedEvicted`, `RemoteFlushes`, `NotifierPublished`, `NotifierPublishDropped`, `NotifierPublishErrors`, `NotifierDeliveryDropped` | core |
| `UnaryClientInterceptor`, `StreamClientInterceptor`; trailer on server responses | `grpcflightrec` |
| `redisnotifier.New`, `natsnotifier.New` | new modules |

## Proof

- A three-service integration test in CI (A calls B calls C over HTTP, in one process with real listeners) from the `NewTransport` PR onward, extended for Case B once the notifier lands. One failure in C produces complete, ordered debug logs from all three.
- `examples/cross-service/` with `docker compose` and Redis, for people to run.
- Tests for: marker propagation through two hops, rejected marker values, header removed from responses, retention expiry and eviction under the byte limit and the per-trace cap, `KindAny` rendering at retention, notifier self-delivery, echo suppression for `remote:` triggers, publish rate limit and queue drops, notice-before-scope via the recent set. All with `-race`.
- Fuzz tests for marker values and trace ID parsing.
- Benchmarks against v0.2: `NewTransport` on a healthy call, `StartScope`/`End` with retention on and off, under parallel load.

## Risks

| Risk | Response |
| --- | --- |
| Retention raises memory use on healthy traffic | Off by default; separate byte limit; compaction; counters and an oldest-age gauge |
| A misbehaving downstream triggers flushes upstream | Markers only honored by clients you wrap; values restricted to the built-in list; reason prefixed `downstream:` |
| Client-chosen trace IDs used to amplify flushes | ID length and charset limits, per-trace retained cap, remote flush budget, documentation |
| Notifier storms | Only local triggers publish; dedupe set; rate limits on both sides |
| Trace IDs do not match across services | Documented requirement; the integration test covers OpenTelemetry propagation and the plain `X-Request-Id` path |
| Proxies strip unknown response headers | Case B covers it; documented |

## Delivery plan

Core changes land first so the submodules can require a tagged core version (see [RELEASING.md](../RELEASING.md)). Each step is one PR closing one issue.

1. `feat(http): set a flush marker on responses of triggered scopes` (#39, PR #47)
2. `feat(http): add NewTransport that triggers the caller scope on a flush marker` (#40), with the three-service integration test
3. `fix(http): limit the length and characters of trace IDs from request headers` (#49)
4. `feat(core): retain healthy scopes for a configurable window` (#42)
5. `feat(core): add the Notifier interface, StartNotifier and an in-memory notifier` (#43)
6. Tag core `v0.3.0-rc.1`
7. `feat(grpc): add client interceptors and a flush trailer on server responses` (#41), requiring core `v0.3.0-rc.1`
8. `feat(notifier): add a Redis pub/sub notifier module` (#44)
9. `feat(notifier): add a NATS notifier module` (#45); may move to v0.3.1 if it holds up the release
10. `docs: add the cross-service example and README section` (#46)
11. Release core `v0.3.0`, then the submodules requiring it
