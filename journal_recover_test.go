// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package flightrec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	if rec.state.journal.Load() == nil {
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
	bumpAttempts(t, path+".recovering", maxRecoveryAttempts)
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

// bumpAttempts counts n recovery attempts in the journal file at path.
func bumpAttempts(t *testing.T, path string, n int) {
	t.Helper()
	r, err := journal.OpenRecovering(path, maxJournalSize)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for range n {
		if _, err := r.IncrementAttempts(); err != nil {
			t.Fatal(err)
		}
	}
}

// crashedJournal leaves a journal at path with one open scope per trace ID,
// started in order, each holding one record.
func crashedJournal(t *testing.T, path string, traces ...string) {
	t.Helper()
	r := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug})
	if _, err := r.StartJournal(path, WithJournalSize(1<<20)); err != nil {
		t.Fatal(err)
	}
	for _, tr := range traces {
		ctx, _ := StartScope(context.Background(), r, ScopeMeta{TraceID: tr})
		slog.New(r).DebugContext(ctx, "x", "trace", tr)
	}
	crash(t, r)
}

func recoverInto(t *testing.T, path string, h slog.Handler) (*Recorder, error) {
	t.Helper()
	rec := New(Options{Next: h})
	stop, err := rec.StartJournal(path, WithJournalSize(1<<20))
	if err == nil {
		t.Cleanup(func() { _ = stop() })
	}
	return rec, err
}

func TestRecoveryRetriesBeforeTheLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	crashedJournal(t, path, "a")
	if err := os.Rename(path, path+".recovering"); err != nil {
		t.Fatal(err)
	}
	bumpAttempts(t, path+".recovering", maxRecoveryAttempts-1)
	var out lineBuffer
	rec, err := recoverInto(t, path, slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if err != nil {
		t.Fatal(err)
	}
	if st := rec.Stats(); st.JournalRecoveredScopes != 1 || st.JournalRecoveredRecords != 1 || st.JournalAbandonedRecoveries != 0 {
		t.Errorf("stats %+v", st)
	}
}

func TestRecoveryOrderAndEmptyScopes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	r := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug})
	if _, err := r.StartJournal(path, WithJournalSize(1<<20)); err != nil {
		t.Fatal(err)
	}
	// Eight scopes land in different stripes; their start order must hold.
	var want []any
	for i := range 8 {
		tr := fmt.Sprintf("scope-%d", i)
		want = append(want, tr)
		ctx, _ := StartScope(context.Background(), r, ScopeMeta{TraceID: tr})
		if i != 3 { // scope-3 is interrupted before logging anything
			slog.New(r).DebugContext(ctx, "x")
		}
		time.Sleep(time.Millisecond)
	}
	crash(t, r)

	var out lineBuffer
	rec, err := recoverInto(t, path, slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if err != nil {
		t.Fatal(err)
	}
	var got []any
	for _, l := range out.lines(t) {
		if l["msg"] == "request interrupted by crash" {
			got = append(got, l["flightrec.trace_id"])
			if l["flightrec.trace_id"] == "scope-3" && l["flightrec.recovered_records"] != float64(0) {
				t.Errorf("empty scope summary %v", l)
			}
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("order %v, want %v", got, want)
	}
	if st := rec.Stats(); st.JournalRecoveredScopes != 8 || st.JournalRecoveredRecords != 7 {
		t.Errorf("stats %+v", st)
	}
}

// panicky panics on its second record.
type panicky struct{ n int }

func (h *panicky) Enabled(context.Context, slog.Level) bool { return true }
func (h *panicky) Handle(context.Context, slog.Record) error {
	if h.n++; h.n == 2 {
		panic("boom")
	}
	return nil
}
func (h *panicky) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *panicky) WithGroup(string) slog.Handler      { return h }

func TestRecoverySurvivesAPanickingHandler(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	crashedJournal(t, path, "a", "b", "c")
	h := &panicky{}
	rec, err := recoverInto(t, path, h)
	if err != nil {
		t.Fatal(err)
	}
	// Three records and three summaries; the first summary panicked.
	if st := rec.Stats(); h.n != 6 || st.JournalRecoveryErrors != 1 || st.JournalRecoveredScopes != 3 || st.JournalRecoveredRecords != 3 {
		t.Errorf("handled %d; stats %+v", h.n, rec.Stats())
	}
}

func TestRecoverLeavesForeignFiles(t *testing.T) {
	t.Run("not a journal", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "j")
		data := bytes.Repeat([]byte("precious "), 1000)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := recoverInto(t, path, slog.DiscardHandler); err == nil {
			t.Error("StartJournal over a foreign file succeeded")
		}
		if b, _ := os.ReadFile(path); !bytes.Equal(b, data) {
			t.Error("foreign file changed")
		}
	})
	t.Run("hard link", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "j")
		crashedJournal(t, path, "a")
		keep := filepath.Join(dir, "keep")
		if err := os.Link(path, keep); err != nil {
			t.Fatal(err)
		}
		before, _ := os.ReadFile(keep)
		if _, err := recoverInto(t, path, slog.DiscardHandler); err == nil {
			t.Error("StartJournal over a hard link succeeded")
		}
		if after, _ := os.ReadFile(keep); !bytes.Equal(before, after) {
			t.Error("hard-linked file changed")
		}
	})
	t.Run("directory at recovering", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "j")
		crashedJournal(t, path, "a")
		if err := os.MkdirAll(filepath.Join(path+".recovering", "x"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := recoverInto(t, path, slog.DiscardHandler); err == nil {
			t.Error("StartJournal with a directory in the way succeeded")
		}
		if journal.Classify(path) != journal.ClassJournal {
			t.Error("the crashed journal was not kept for a later recovery")
		}
	})
}

func TestRecoverRemovesDeadFilesAndSymlinks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "j")
	// A journal whose creation was cut short: zeros only.
	if err := os.WriteFile(path, make([]byte, 1<<16), 0o600); err != nil {
		t.Fatal(err)
	}
	rec, err := recoverInto(t, path, slog.DiscardHandler)
	if err != nil {
		t.Fatal(err)
	}
	if st := rec.Stats(); st.JournalDiscardedFiles != 0 || st.JournalRecoveredScopes != 0 {
		t.Errorf("stats %+v", st)
	}

	path = filepath.Join(dir, "j2")
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	rec, err = recoverInto(t, path, slog.DiscardHandler)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(target); string(b) != "keep" {
		t.Errorf("symlink target changed: %q", b)
	}
	if st := rec.Stats(); st.JournalDiscardedFiles != 1 {
		t.Errorf("stats %+v", st)
	}
}

func FuzzRecoverJournal(f *testing.F) {
	dir := f.TempDir()
	seed := filepath.Join(dir, "seed")
	jf, err := journal.Create(seed, journal.Header{Size: journal.MinSize(1), Stripes: 1, Host: "h"})
	if err != nil {
		f.Fatal(err)
	}
	rec0 := New(Options{Next: slog.DiscardHandler}).WithGroup("g").(*Recorder)
	r := slog.NewRecord(time.Unix(1, 0), slog.LevelDebug, "m", 0)
	r.AddAttrs(slog.Int("n", 1), slog.Float64("f", 2), slog.Group("x", slog.String("y", "z")))
	jf.Append(1, journal.KindStart, 1, appendScopeStart(nil, scopeStart{start: time.Unix(1, 0), traceID: "t", method: "GET", path: "/"}))
	jf.Append(1, journal.KindRecord, 1, appendJournalRecord(nil, rec0, r, TraceContext{}))
	jf.Append(2, journal.KindRecord, 2, appendJournalRecord(nil, rec0, r, TraceContext{}))
	jf.Append(2, journal.KindEnd, 2, nil)
	if err := jf.Close(false); err != nil {
		f.Fatal(err)
	}
	data, err := os.ReadFile(seed)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data[:journal.HeaderSize+2*1024])
	f.Fuzz(func(t *testing.T, b []byte) {
		path := filepath.Join(t.TempDir(), "j")
		// Fuzz the start of a valid journal, keeping its size.
		full := append([]byte(nil), data...)
		copy(full, b)
		if err := os.WriteFile(path, full, 0o600); err != nil {
			t.Fatal(err)
		}
		rec := New(Options{Next: slog.NewJSONHandler(io.Discard, nil)})
		stop, err := rec.StartJournal(path, WithJournalSize(1<<20))
		if err != nil {
			if journal.Classify(path) != journal.ClassOther {
				t.Fatalf("StartJournal: %v", err)
			}
			return
		}
		_ = stop()
	})
}

// TestRecoverMarksRecordsTooLargeToJournal checks that a record too large for
// a journal entry leaves the recovered request marked truncated.
func TestRecoverMarksRecordsTooLargeToJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	r := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug, MaxPerScope: 1 << 20})
	if _, err := r.StartJournal(path, WithJournalSize(1<<20)); err != nil {
		t.Fatal(err)
	}
	ctx, _ := StartScope(context.Background(), r, ScopeMeta{TraceID: "t-big"})
	log := slog.New(r)
	log.DebugContext(ctx, "small", "seq", 0)
	log.DebugContext(ctx, "huge", "blob", strings.Repeat("x", 70<<10))
	log.DebugContext(ctx, "small", "seq", 2)
	if got := r.Stats().JournalDropped; got != 1 {
		t.Errorf("JournalDropped = %d, want 1", got)
	}
	crash(t, r)

	var out lineBuffer
	if _, err := recoverInto(t, path, slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})); err != nil {
		t.Fatal(err)
	}
	lines := out.lines(t)
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 2 records and a summary: %v", len(lines), lines)
	}
	if lines[0]["flightrec.truncated"] != true || lines[0]["flightrec.dropped_records"] != float64(1) {
		t.Errorf("first recovered record not marked truncated: %v", lines[0])
	}
	sum := lines[2]
	if sum["flightrec.truncated"] != true || sum["flightrec.dropped_records"] != float64(1) || sum["flightrec.recovered_records"] != float64(2) {
		t.Errorf("summary %v", sum)
	}
}

// TestRecoveryLabelsAdoptedAndExplicitScopes checks that recovery labels
// records as a live flush would: an explicit trace ID is kept even when
// records carry a span's trace, and an adopted trace ID is used with the
// header ID as flightrec.request_id.
func TestRecoveryLabelsAdoptedAndExplicitScopes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	span := TraceContext{TraceID: [16]byte{0xab, 0xcd}, SpanID: [8]byte{1}}
	extract := func(ctx context.Context) TraceContext {
		if ctx.Value(spanKey{}) != nil {
			return span
		}
		return TraceContext{}
	}
	r := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug, TraceExtractor: extract})
	if _, err := r.StartJournal(path, WithJournalSize(1<<20)); err != nil {
		t.Fatal(err)
	}
	log := slog.New(r)
	jctx, _ := StartScope(context.Background(), r, ScopeMeta{TraceID: "job-123"})
	log.DebugContext(context.WithValue(jctx, spanKey{}, true), "job step")

	hctx, hs := startScope(context.Background(), r, ScopeMeta{TraceID: "req-W"}, idHeader)
	hs.AdoptGeneratedTraceID(span.TraceIDHex())
	log.DebugContext(hctx, "request step")
	crash(t, r)

	var out lineBuffer
	if _, err := recoverInto(t, path, slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})); err != nil {
		t.Fatal(err)
	}
	for _, l := range out.lines(t) {
		switch l["msg"] {
		case "job step":
			if l["flightrec.trace_id"] != "job-123" {
				t.Errorf("explicit scope relabelled: %v", l)
			}
		case "request step":
			if l["flightrec.trace_id"] != span.TraceIDHex() || l["flightrec.request_id"] != "req-W" {
				t.Errorf("adopted scope: %v", l)
			}
		}
	}
}

type spanKey struct{}

// pass redacts itself when printed.
type pass string

func (pass) String() string { return "***" }

// TestJournalKeepsStringRedaction checks that a value that redacts itself in
// String is recovered redacted for text handlers, as the original would print.
func TestJournalKeepsStringRedaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	r := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug})
	if _, err := r.StartJournal(path, WithJournalSize(1<<20)); err != nil {
		t.Fatal(err)
	}
	ctx, _ := StartScope(context.Background(), r, ScopeMeta{TraceID: "t"})
	slog.New(r).DebugContext(ctx, "login", "pw", struct{ P pass }{"hunter2"})
	crash(t, r)

	var out bytes.Buffer
	if _, err := recoverInto(t, path, slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "pw={P:***}") {
		t.Errorf("recovered text output: %s", out.String())
	}
}

// TestJournalFitsMidSizeValues checks that a record with one value of a few
// tens of KB is journaled, not dropped for having two renderings.
func TestJournalFitsMidSizeValues(t *testing.T) {
	type blob struct{ Data string }
	path := filepath.Join(t.TempDir(), "j")
	r := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug, MaxPerScope: 1 << 20})
	if _, err := r.StartJournal(path, WithJournalSize(1<<20)); err != nil {
		t.Fatal(err)
	}
	ctx, _ := StartScope(context.Background(), r, ScopeMeta{TraceID: "t"})
	slog.New(r).DebugContext(ctx, "mid", "b", blob{Data: strings.Repeat("x", 40<<10)})
	if st := r.Stats(); st.JournalDropped != 0 || st.JournalRecordsWritten != 1 {
		t.Errorf("stats %+v", st)
	}
	crash(t, r)
}
