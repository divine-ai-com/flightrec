// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite testdata/golden files")

// normalize replaces the values that change between runs: times, durations
// and the host name.
func normalize(b []byte) []byte {
	for _, r := range []struct {
		re   *regexp.Regexp
		repl string
	}{
		{regexp.MustCompile(`"(time|start|flightrec.recovered_process_start)":"[^"]*"`), `"$1":"<time>"`},
		{regexp.MustCompile(`"duration":\d+`), `"duration":<duration>`},
		{regexp.MustCompile(`"flightrec.recovered_host":"[^"]*"`), `"flightrec.recovered_host":"<host>"`},
	} {
		b = r.re.ReplaceAll(b, []byte(r.repl))
	}
	return b
}

// checkGolden compares out with testdata/golden/name.jsonl.
func checkGolden(t *testing.T, name string, out []byte) {
	t.Helper()
	path := filepath.Join("testdata", "golden", name+".jsonl")
	got := normalize(out)
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run go test -run TestGolden -update)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s output changed:\n got:\n%s\nwant:\n%s", name, got, want)
	}
}

type goldenOrder struct {
	ID    int    `json:"id"`
	State string `json:"state"`
}

// TestGolden pins the exact lines flightrec writes in each situation.
func TestGolden(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
		run  func(rec *Recorder, log *slog.Logger)
	}{
		{"healthy", Options{}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-1", Method: "GET", Path: "/orders"})
			log.DebugContext(ctx, "loaded cart", "items", 3)
			sc.End(ctx, 200)
		}},
		{"error_log", Options{}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-2", Method: "POST", Path: "/checkout"})
			log.DebugContext(ctx, "loaded cart", "items", 3)
			log.InfoContext(ctx, "calling provider", "provider", "acme")
			log.ErrorContext(ctx, "provider timed out", "err", errors.New("deadline exceeded"))
			sc.End(ctx, 200)
		}},
		{"status_5xx", Options{}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-3"})
			log.With("service", "checkout").WithGroup("req").DebugContext(ctx, "step", "n", 1, "order", goldenOrder{ID: 7, State: "new"})
			sc.End(ctx, 503)
		}},
		{"explicit_flush", Options{}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-4"})
			log.DebugContext(ctx, "totals differ")
			Flush(ctx, "payment mismatch")
			sc.End(ctx, 200)
		}},
		{"truncated", Options{MaxPerScope: 2 << 10}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-5"})
			for i := range 40 {
				log.DebugContext(ctx, "step", "n", i)
			}
			sc.End(ctx, 500)
		}},
		{"retained_flush", Options{Retain: time.Minute}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-6"})
			log.DebugContext(ctx, "queued email", "order", &goldenOrder{ID: 9, State: "paid"})
			sc.End(ctx, 200)
			rec.FlushTrace(context.Background(), "req-6", "downstream failure")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			tc.opts.Next = slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})
			tc.opts.PassLeveler = slog.LevelWarn
			tc.opts.BufferLeveler = slog.LevelDebug
			rec := New(tc.opts)
			tc.run(rec, slog.New(rec))
			checkGolden(t, tc.name, out.Bytes())
		})
	}
}
