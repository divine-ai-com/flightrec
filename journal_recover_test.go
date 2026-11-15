// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package flightrec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/divine-ai-com/flightrec/internal/journal"
)

// crash leaves rec's journal as a killed process would: written, not
// zeroed, and unlocked.
func crash(t *testing.T, rec *Recorder) {
	t.Helper()
	js := rec.state.journal.Swap(nil)
	if js == nil {
		t.Fatal("no journal running")
	}
	if err := js.file.Close(false); err != nil {
		t.Fatal(err)
	}
	js.unlock()
}

// lineBuffer collects JSON log lines.
type lineBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lineBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lineBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		if l == "" {
			continue
		}
		m := map[string]any{}
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("bad line %q: %v", l, err)
		}
		out = append(out, m)
	}
	return out
}

func TestRecoverInterruptedScopes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	old := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug})
	if _, err := old.StartJournal(path, WithJournalSize(2<<20)); err != nil {
		t.Fatal(err)
	}
	log := slog.New(old).With("service", "checkout").WithGroup("req")

	doneCtx, done := StartScope(context.Background(), old, ScopeMeta{TraceID: "t-done"})
	log.DebugContext(doneCtx, "healthy")
	done.End(doneCtx, 200)
	failedCtx, failed := StartScope(context.Background(), old, ScopeMeta{TraceID: "t-failed"})
	log.DebugContext(failedCtx, "flushed already")
	failed.End(failedCtx, 500)

	tid := "4bf92f3577b34da6a3ce929d0e0e4736"
	openCtx, _ := StartScope(context.Background(), old, ScopeMeta{TraceID: tid, Method: "POST", Path: "/pay"})
	log.DebugContext(openCtx, "charging", "amount", 4598, "err", errors.New("slow bank"))
	log.InfoContext(openCtx, "waiting")
	crash(t, old)

	var out lineBuffer
	rec := New(Options{Next: slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})})
	stop, err := rec.StartJournal(path) // default size: the old file's layout is read from its header
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()

	lines := out.lines(t)
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %v", len(lines), lines)
	}
	host, _ := os.Hostname()
	for _, l := range lines {
		if l["flightrec.trace_id"] != tid || l["flightrec.flush_reason"] != "crash_recovery" ||
			l["flightrec.recovered"] != true || l["flightrec.recovered_host"] != host {
			t.Errorf("line not tagged: %v", l)
		}
	}
	first := lines[0]
	req, _ := first["req"].(map[string]any)
	if first["msg"] != "charging" || first["level"] != "DEBUG" || first["service"] != "checkout" ||
		req["amount"] != float64(4598) || req["err"] != "slow bank" {
		t.Errorf("first record %v", first)
	}
	if lines[1]["msg"] != "waiting" {
		t.Errorf("second record %v", lines[1])
	}
	sum := lines[2]
	if sum["msg"] != "request interrupted by crash" || sum["level"] != "ERROR" || sum["method"] != "POST" ||
		sum["path"] != "/pay" || sum["flightrec.recovered_records"] != float64(2) || sum["start"] == nil {
		t.Errorf("summary %v", sum)
	}
	st := rec.Stats()
	if st.JournalRecoveredScopes != 1 || st.JournalRecoveredRecords != 2 || st.JournalCorruptEntries != 0 ||
		st.JournalDiscardedFiles != 0 || st.JournalAbandonedRecoveries != 0 {
		t.Errorf("stats %+v", st)
	}
	if _, err := os.Lstat(path + ".recovering"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("recovering file left: %v", err)
	}
	if st.JournalBytesWritten == 0 && rec.state.journal.Load() == nil {
		t.Error("no journal running after recovery")
	}
}

func TestRecoverTruncatedScope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	f, err := journal.Create(path, journal.Header{Size: journal.MinSize(2), Stripes: 2, Host: "old-host", StartedAt: 1})
	if err != nil {
		t.Fatal(err)
	}
	rec0 := New(Options{Next: slog.DiscardHandler})
	r := slog.NewRecord(time.Unix(100, 0), slog.LevelDebug, "late", 0)
	tc := TraceContext{TraceID: [16]byte{1, 2, 3}, SpanID: [8]byte{4}}
	// A record whose scope's start entry was overwritten.
	f.Append(7, journal.KindRecord, 7, appendJournalRecord(nil, rec0, r, tc))
	if err := f.Close(false); err != nil {
		t.Fatal(err)
	}

	var out lineBuffer
	rec := New(Options{Next: slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})})
	stop, err := rec.StartJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()
	lines := out.lines(t)
	if len(lines) != 2 {
		t.Fatalf("lines %v", lines)
	}
	if lines[0]["flightrec.truncated"] != true || lines[0]["flightrec.trace_id"] != tc.TraceIDHex() ||
		lines[0]["flightrec.span_id"] != tc.SpanIDHex() || lines[0]["flightrec.recovered_host"] != "old-host" {
		t.Errorf("record %v", lines[0])
	}
	if lines[1]["flightrec.truncated"] != true || lines[1]["start"] != nil {
		t.Errorf("summary %v", lines[1])
	}
}

func TestRecoveryAttemptsAreLimited(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	old := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug})
	if _, err := old.StartJournal(path, WithJournalSize(1<<20)); err != nil {
		t.Fatal(err)
	}
	ctx, _ := StartScope(context.Background(), old, ScopeMeta{})
	slog.New(old).DebugContext(ctx, "x")
	crash(t, old)
	// Three recoveries that crashed part way.
	if err := os.Rename(path, path+".recovering"); err != nil {
		t.Fatal(err)
	}
	for range maxRecoveryAttempts {
		if _, err := journal.IncrementAttempts(path + ".recovering"); err != nil {
			t.Fatal(err)
		}
	}
	var out lineBuffer
	rec := New(Options{Next: slog.NewJSONHandler(&out, nil)})
	stop, err := rec.StartJournal(path, WithJournalSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()
	if out.buf.Len() != 0 {
		t.Errorf("abandoned file recovered: %s", out.buf.String())
	}
	if st := rec.Stats(); st.JournalAbandonedRecoveries != 1 || st.JournalRecoveredScopes != 0 {
		t.Errorf("stats %+v", st)
	}
	if _, err := os.Lstat(path + ".recovering"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("abandoned file left: %v", err)
	}
}

func TestRecoverBothPendingFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	write := func(trace string) {
		r := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug})
		if _, err := r.StartJournal(path, WithJournalSize(1<<20)); err != nil {
			t.Fatal(err)
		}
		ctx, _ := StartScope(context.Background(), r, ScopeMeta{TraceID: trace})
		slog.New(r).DebugContext(ctx, "x")
		crash(t, r)
	}
	write("first")
	// A recovery of the first file crashed; the process then wrote a second
	// journal and was killed too.
	if err := os.Rename(path, path+".first"); err != nil {
		t.Fatal(err)
	}
	write("second")
	if err := os.Rename(path+".first", path+".recovering"); err != nil {
		t.Fatal(err)
	}

	var out lineBuffer
	rec := New(Options{Next: slog.NewJSONHandler(&out, nil)})
	stop, err := rec.StartJournal(path, WithJournalSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()
	var traces []any
	for _, l := range out.lines(t) {
		if l["msg"] == "request interrupted by crash" {
			traces = append(traces, l["flightrec.trace_id"])
		}
	}
	if len(traces) != 2 || traces[0] != "first" || traces[1] != "second" {
		t.Errorf("recovered %v", traces)
	}
}

func TestRecoverDiscardsBadFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "j")
	if err := os.WriteFile(path, bytes.Repeat([]byte("not a journal "), 1000), 0o600); err != nil {
		t.Fatal(err)
	}
	rec := New(Options{Next: slog.DiscardHandler})
	stop, err := rec.StartJournal(path, WithJournalSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	_ = stop()
	if st := rec.Stats(); st.JournalDiscardedFiles != 1 {
		t.Errorf("stats %+v", st)
	}

	// A symlink is removed, never followed.
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	stop, err = rec.StartJournal(path, WithJournalSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	_ = stop()
	if b, _ := os.ReadFile(target); string(b) != "keep" {
		t.Errorf("symlink target changed: %q", b)
	}
	if st := rec.Stats(); st.JournalDiscardedFiles != 2 {
		t.Errorf("stats %+v", st)
	}
}
