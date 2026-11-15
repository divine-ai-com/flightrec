# Troubleshooting

## 1. Buffered `DEBUG` logs are not appearing when expected

**Causes & Fixes:**
- **Missing request context:** Ensure log calls inside handlers pass the request context (`slog.DebugContext(r.Context(), "msg")`). Calling `slog.Debug("msg")` without `ctx` has no request scope and is delegated directly to `Options.Next` (see the package [Level Filtering](https://pkg.go.dev/github.com/divine-ai-com/flightrec#hdr-Level_Filtering) docs).
- **`BufferLeveler` set above `slog.LevelDebug`:** If `Options.BufferLeveler` is set to `slog.LevelInfo`, `DEBUG` records are ignored. Leave `BufferLeveler` unset or set it to `slog.LevelDebug`. When a scope triggers, `flightrec` flushes buffered records directly through `Options.Next.Handle` regardless of `Options.Next`'s own level, so `Options.Next` does not need `Level: slog.LevelDebug` for flushed records to appear.
- **Request did not trigger:** Verify that the request logged an `ERROR`, returned HTTP `>= 500` (or a gRPC server-fault code), exceeded `Options.SlowAfter`, or called `flightrec.Flush(ctx, "reason")`.

## 2. Logs outside a request (`slog.Debug` / `slog.Info` at startup) are filtered

**Cause:** Outside an active request scope, `flightrec` delegates `Enabled` and `Handle` directly to `Options.Next`.

**Fix:** Configure `Options.Next` with the minimum level you want for non-request logs (defaults to `slog.LevelInfo` on standard library handlers).

## 3. Flushed logs show `flightrec.truncated: true`

**Cause:** The single request produced more than `Options.MaxPerScope` bytes (default 256 KB, including ring-buffer slot capacity) of buffered records, or process-wide buffered memory reached `Options.MaxTotal` (default 64 MB).

**Fix:** Check `rec.Stats()` (`RecordsDroppedPerScopeCap` vs `ScopesEvictedTotalCap`) and either reduce overly large log attributes in hot loops or raise `MaxPerScope` / `MaxTotal`.
