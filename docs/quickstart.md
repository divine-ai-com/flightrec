# Quickstart Guide

Get `flightrec` running in an existing Go service in under 10 minutes.

## 1. Install

```bash
go get github.com/divine-ai-com/flightrec@v0.1.0
```

## 2. Wrap Your `slog.Handler` (3 Changes)

```go
// 1. Create the recorder wrapping your existing slog.Handler
//    (Make sure the inner handler allows LevelDebug so flushed debug logs are emitted!)
inner := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
rec := flightrec.New(flightrec.Options{
    Next:        inner,
    PassLevel:   slog.LevelWarn,
    BufferLevel: slog.LevelDebug,
    SlowAfter:   2 * time.Second,
    MaxPerScope: 256 << 10, // 256 KB per request
    MaxTotal:    64 << 20,  // 64 MB process-wide
})

// 2. Set as your default logger
slog.SetDefault(slog.New(rec))

// 3. Wrap your HTTP router with HTTPMiddleware
handler = flightrec.HTTPMiddleware(rec)(handler)
```

## 3. Use Context-Aware Logging in Handlers

```go
func handleOrder(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()

    // Buffered in memory; dropped if the request succeeds quickly:
    slog.DebugContext(ctx, "parsed order payload", "items", 4)
    slog.InfoContext(ctx, "reserved inventory", "warehouse", "us-east-1")

    // Written immediately (WARN >= PassLevel):
    slog.WarnContext(ctx, "carrier rate-limit near threshold")

    // Optional: explicitly force this request's buffer to flush even on HTTP 200:
    if isSuspiciousOrder() {
        flightrec.Flush(ctx, "fraud_score_review")
    }

    w.WriteHeader(http.StatusOK)
}
```

## 4. Background Jobs (Manual Scopes)

Logs outside any HTTP/gRPC request pass straight through to `Next`. To buffer logs for a background job or worker iteration, open a scope manually:

```go
func processJob(parentCtx context.Context, rec *flightrec.Recorder, jobID string) (err error) {
    ctx, scope := flightrec.StartScope(parentCtx, rec, flightrec.ScopeMeta{
        TraceID: jobID,
        Method:  "JOB",
        Path:    "sync-invoices",
    })
    defer func() {
        status := 200
        if err != nil {
            status = 500
        }
        scope.End(ctx, status)
    }()

    slog.DebugContext(ctx, "fetched invoice batch")
    return nil
}
```
