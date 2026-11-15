# Running flightrec safely

flightrec sits on the request path and holds your services' debug logs, so treat its inputs as untrusted and its buffers as sensitive. This page lists what to set, and why. To report a vulnerability, follow the [security policy](../SECURITY.md).

## Services that answer clients outside your system

A client controls its request headers. By default flightrec takes the trace ID from `traceparent`, `X-Trace-Id` or `X-Request-Id`, and marks kept responses with a `Flightrec-Flush` header, which suits services that only other services of yours call. On a public API, a client that knows or guesses another request's ID (sequential request IDs are guessable) could make that request's retained logs flush or be evicted, and the marker tells it when your service logged an error. Set:

```go
rec := flightrec.New(flightrec.Options{
	// ...
	IgnoreRequestTraceIDs: true, // trace IDs come from your own tracing, or are generated
	DisableFlushMarker:    true, // no Flightrec-Flush header or gRPC trailer on responses
})
```

With OpenTelemetry middleware in front, also mark it public (`otelhttp.WithPublicEndpoint()`), or the client's `traceparent` still becomes the span's trace, and so flightrec's.

A client can also make its own request slow (by sending the body slowly), which triggers `SlowAfter`: that request's buffer is written, bounded by `MaxPerScope`, and a notice is published within the notifier's rate limits. Set `SlowAfter` above the durations clients can reasonably cause, and watch `Stats().ScopesFlushed`.

## Notifier channel

A notice carries only a trace ID and a reason, but anyone who can publish on the channel can make every connected service flush or trigger its scopes for any trace ID, or flood the channel so real notices are dropped. Sign messages, and restrict who can publish:

```go
key := []byte(os.Getenv("FLIGHTREC_NOTIFIER_KEY")) // at least 32 random bytes, the same in every service
stop := rec.StartNotifier(ctx, n, flightrec.WithSigningKey(key))
```

- Messages are signed with HMAC-SHA256 and a timestamp; unsigned, forged and messages older than two minutes are dropped and counted in `Stats().NotifierRejected`. Use 32 random bytes, not a 32-character password.
- Signed and unsigned services reject each other's messages, so turn signing on for every service of a channel in one deploy. To rotate keys with rolling deploys, roll out each step everywhere before the next: `WithSigningKey(old, new)`, then `WithSigningKey(new, old)`, then `WithSigningKey(new)`.
- Give the channel its own credentials, over TLS, read from the environment or a secret store. For Redis, an ACL user limited to the channel; `+ping` is needed for the notifier's health checks:

  ```
  ACL SETUSER flightrec on >PASSWORD resetchannels &flightrec:prod +publish +subscribe +ping
  ```

  For NATS, publish and subscribe permissions on the notifier's subject only.

## Sensitive data in logs

flightrec writes what your code logs, through the handler you pass as `Next`, so redaction you do there (`HandlerOptions.ReplaceAttr`, `slog.LogValuer` on your types) applies to flushed records as it does to any other. Two features copy values instead of keeping references:

- `Retain` and `StartJournal` copy `slog.Any` values. The copy keeps two renderings made as slog's handlers make them: JSON for JSON handlers (`MarshalJSON` is honoured), and text for text handlers (`MarshalText`, else `Format`, `Error` or `String`), so types that redact themselves in those methods stay redacted. A type that redacts only in `String` is still written in full by `slog.JSONHandler`, with or without flightrec; implement `slog.LogValuer` or `MarshalJSON` for redaction that holds everywhere.
- Each rendering of a copy is capped at 64 KB (32 KB for both together in the journal), and copying for `Retain` stops once a request's copies exceed `MaxRetained`.

Do not change a map that was logged with `slog.Any` from another goroutine while the request runs: the handler (or the copy taken for `Retain`) reads it when the request ends, and a concurrent write makes the Go runtime abort the process.

## The crash journal

The journal file holds recent debug records of all requests, healthy ones included, before your handler's redaction runs on them at recovery.

- Put it on a volume only your service can read, ideally in memory: `emptyDir: {medium: Memory}` in Kubernetes. On disks, especially SSDs and copy-on-write filesystems, overwriting a file with zeros does not guarantee the old data is gone.
- The file is created with mode 0600 and locked through `path.lock`. A lock file, journal or `path.recovering` that is not a private regular file of this user (a link, a FIFO, another user's file) is refused rather than used or deleted.

## Resource limits

Every buffer is bounded: `MaxPerScope` per request, `MaxTotal` per process, `MaxRetained` for retained requests and at most 8 retained requests per trace ID, `WithJournalSize` for the journal, and fixed rate limits and queue sizes for the notifier. Export `Stats()` and alert on the drop and eviction counters; they tell you when a limit is shaping what you keep.
