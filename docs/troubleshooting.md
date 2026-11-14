# Troubleshooting

## 1. Buffered `DEBUG` logs are not appearing even when a request fails

**Cause:** Your underlying `slog.Handler` (`Options.Next`) is configured with its default minimum level (`slog.LevelInfo`), so it drops `slog.LevelDebug` records when `flightrec` flushes them.

**Fix:** Configure `Options.Next` with `Level: slog.LevelDebug`:

```go
inner := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
})
rec := flightrec.New(flightrec.Options{Next: inner})
```

## 2. `DEBUG` logs are being written immediately for every healthy request

**Cause:** The log call did not pass the request's `context.Context` (for example calling `slog.Debug("msg")` instead of `slog.DebugContext(r.Context(), "msg")`). Logs without an active scope in `ctx` pass straight through to `Next`.

**Fix:** Always use `slog.DebugContext(ctx, ...)` and `slog.InfoContext(ctx, ...)` inside request handlers.

## 3. Flushed logs show `flightrec.truncated: true`

**Cause:** The single request produced more than `Options.MaxPerScope` bytes (default 256 KB) of buffered records, or process-wide buffered memory reached `Options.MaxTotal` (default 64 MB).

**Fix:** Check `rec.Stats()` (`RecordsDroppedPerScopeCap` vs `ScopesEvictedTotalCap`) and either reduce overly large log attributes in hot loops or raise `MaxPerScope` / `MaxTotal`.
