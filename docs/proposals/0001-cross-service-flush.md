# Proposal 0001: Cross-service flush (v0.3)

| | |
| --- | --- |
| Status | Accepted with the defaults below (#33) |
| Milestone | v0.3.0 |
| Scope | core module, `grpcflightrec`, two new notifier modules |

## Problem

flightrec decides per process. When service C fails a request, C keeps its debug logs, but services A and B, which handled the same request upstream, see a successful downstream call or a generic error and discard theirs. The person debugging the incident gets the last hop only.

Two cases need different mechanisms:

- **Case A, the failure is visible to the caller.** C returns an error or a 5xx, and B is still inside its own request when the response arrives.
- **Case B, the caller already finished.** B called C asynchronously, or B returned before C failed (a queue consumer, a fire-and-forget call, a retry that eventually failed). B's scope has already ended.

## Goals

- In a chain A to B to C, one failure in C produces complete logs from all three, in both cases, when the services share a trace ID.
- No new dependency in the core module.
- Nothing a client sends can force a server to flush. Only responses from services you call, and a notifier you run, can trigger a flush.
- Memory for retained scopes is bounded and counted, like the existing buffers.
- Best effort, and documented as such: a scope that has left its retention window is gone.

## Non-goals

- Guaranteed delivery. A crashed service, a dropped notifier message or an expired scope means missing logs, not an error.
- Flushing spans across services. `otelflightrec` follows the log scope it is linked to, so it inherits this feature for free in-process; cross-process span flush is a later step.
- Propagating trace context. flightrec reads the trace ID that OpenTelemetry or your own headers already carry. The HTTP client wrapper only fills in a request ID when none is present.

## Design

### Case A: flush marker on responses

1. When a scope is triggered, the server middleware sets a response header before the status line is written:

   ```
   Flightrec-Flush: http_502
   ```

   The value is the flush reason, truncated to 64 bytes and limited to `[a-z0-9_.-]`. For gRPC, `grpcflightrec` sets the same key as response metadata (header if the scope triggered before the first message, trailer otherwise).

2. A new client wrapper reads it:

   ```go
   client := &http.Client{Transport: flightrec.Transport(rec, http.DefaultTransport)}
   ```

   `Transport` returns an `http.RoundTripper`. When a response carries `Flightrec-Flush`, it triggers the scope in the request's context with reason `downstream:<reason>`, for example `downstream:http_502`. A request without a scope in its context is unaffected. `grpcflightrec` gets the matching `UnaryClientInterceptor` and `StreamClientInterceptor`.

3. Because B's scope is now triggered, B's own response to A carries the marker, and the flush travels up the whole chain without any configuration beyond the two wrappers.

4. **Headers already sent.** If the scope is triggered after the response headers are written, the marker cannot be added. HTTP trailers are not used, because many proxies and clients drop them. The middleware counts these in `Stats().FlushMarkersMissed`, and Case B covers them when a notifier is configured.

5. **Security.** The server middleware never reads `Flightrec-Flush` from incoming requests. The header is stripped from incoming requests before the handler runs, so a handler that proxies requests cannot forward a client-supplied marker by accident. Only `Transport` and the gRPC client interceptors read it, from responses. Operators who do not trust a downstream service can omit `Transport` for that client.

### Case B: retention window and notifier

1. **Retention.** With `Options.Retain` set (for example `30 * time.Second`), a healthy scope that ends is not discarded. Its buffer moves to a retained set, indexed by trace ID, with its own byte limit `Options.MaxRetained` (default 16 MB). The healthy summary line is still written at the end of the request. When the window passes or the limit is reached, the oldest retained scopes are discarded and counted in `Stats().RetainedExpired` and `Stats().RetainedEvicted`.

2. **Notifier.** A small interface in the core module:

   ```go
   type Notifier interface {
       // Publish announces that the trace was flushed in this process.
       Publish(ctx context.Context, traceID, reason string) error
       // Subscribe delivers trace IDs flushed in other processes until ctx is done.
       Subscribe(ctx context.Context, deliver func(traceID, reason string)) error
   }
   ```

   `Options.Notifier` is optional. When set:

   - every triggered scope publishes its trace ID once, after its own flush;
   - every delivered trace ID that matches a retained scope flushes it with reason `remote:<reason>`, and a matching active scope is triggered the same way;
   - a process ignores its own messages.

3. **Backends**, each a separate module so the core stays dependency-free:

   - `notifier/redisnotifier` using Redis pub/sub (`github.com/redis/go-redis/v9`);
   - `notifier/natsnotifier` using NATS core subjects (`github.com/nats-io/nats.go`).

4. **Load control.** Publishing is rate limited per process (`Options.NotifierRate`, default 100 per second, burst 200); excess publishes are dropped and counted. Delivery work is done on one goroutine per subscription with a bounded queue, so a burst of remote flushes cannot block request handling.

### Order and timestamps

Flushed records keep their original timestamps and are written in order within a scope. Records from different services are interleaved by the log backend using those timestamps, as today. A flushed record carries `flightrec.flush_reason`, so `downstream:` and `remote:` reasons show why a service that succeeded kept its logs.

## Public API changes

All additions; nothing existing changes behavior unless the new options are set.

| Addition | Module |
| --- | --- |
| `Transport(rec, next http.RoundTripper) http.RoundTripper` | core |
| `Options.Retain`, `Options.MaxRetained`, `Options.Notifier`, `Options.NotifierRate` | core |
| `Notifier` interface | core |
| `Stats` fields: `FlushMarkersSent`, `FlushMarkersMissed`, `DownstreamTriggers`, `RetainedScopes`, `RetainedBytes`, `RetainedExpired`, `RetainedEvicted`, `RemoteFlushes`, `NotifierPublishDropped` | core |
| `UnaryClientInterceptor`, `StreamClientInterceptor`; marker on server responses | `grpcflightrec` |
| `redisnotifier.New`, `natsnotifier.New` | new modules |

## Proof

- A three-service demo under `examples/cross-service/` (A calls B calls C over HTTP, with `docker compose` for Redis): one failure in C produces complete, ordered debug logs from all three services, for both an in-band error and an asynchronous failure.
- Tests for: marker propagation through two hops, marker ignored on incoming requests, marker missed after headers are written, retention expiry and eviction under the byte limit, notifier self-delivery ignored, publish rate limit.
- Benchmarks: overhead of `Transport` on a healthy call, and of retention on the healthy path, against v0.2.

## Risks

| Risk | Response |
| --- | --- |
| Retention raises memory use on healthy traffic | Off by default; separate byte limit; counters for every drop |
| A misbehaving downstream triggers flushes upstream | Markers only honored by clients you wrap; reason prefixed `downstream:` so it is visible; per-process rate limit applies to notifier |
| Trace IDs do not match across services | Documented requirement; the demo uses OpenTelemetry propagation and the plain `X-Request-Id` path |
| Proxies strip unknown response headers | Case B covers it; documented |

## Delivery plan

Each step is one PR closing one issue:

1. `feat(http): set a flush marker on responses of triggered scopes`
2. `feat(http): add Transport that triggers the caller scope on a flush marker`
3. `feat(grpc): add client interceptors and server flush marker`
4. `feat(core): retain healthy scopes for a configurable window`
5. `feat(core): add Notifier interface and remote flush handling`
6. `feat(notifier): add Redis pub/sub notifier module`
7. `feat(notifier): add NATS notifier module`
8. `docs: add three-service cross-service flush example`
