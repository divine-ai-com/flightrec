// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package flightrec

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// TestGoldenRecovery pins the lines crash recovery writes, for a journal of
// the current format and for one written by v0.5.0.
func TestGoldenRecovery(t *testing.T) {
	handler := func(out *bytes.Buffer) slog.Handler {
		return slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug})
	}

	t.Run("current", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "j")
		r := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug})
		if _, err := r.StartJournal(path, WithJournalSize(1<<20)); err != nil {
			t.Fatal(err)
		}
		log := slog.New(r).With("service", "checkout").WithGroup("req")
		ctx, _ := StartScope(context.Background(), r, ScopeMeta{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", Method: "POST", Path: "/pay"})
		log.DebugContext(ctx, "charging", "amount", 4598, "order", map[string]any{"id": 7, "state": "new"})
		log.InfoContext(ctx, "waiting for bank")
		crash(t, r)

		var out bytes.Buffer
		if _, err := recoverInto(t, path, handler(&out)); err != nil {
			t.Fatal(err)
		}
		checkGolden(t, "recovery", out.Bytes())
	})

	t.Run("v0.5.0", func(t *testing.T) {
		f, err := os.Open(filepath.Join("testdata", "journal-v0.5.0.gz"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		zr, err := gzip.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(zr)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "j")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		rec, err := recoverInto(t, path, handler(&out))
		if err != nil {
			t.Fatal(err)
		}
		if st := rec.Stats(); st.JournalRecoveredScopes != 1 || st.JournalCorruptEntries != 0 {
			t.Errorf("stats %+v", st)
		}
		checkGolden(t, "recovery_v0.5.0", out.Bytes())
	})
}
