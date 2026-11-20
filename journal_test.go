// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package flightrec

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/haawklabs/flightrec/internal/journal"
)

func readJournal(t *testing.T, path string) []journal.Entry {
	t.Helper()
	data, err := journal.ReadFile(path, maxJournalSize+journal.HeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	_, entries, _, err := journal.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestStartJournalWritesScopeEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flightrec.journal")
	rec := New(Options{Next: slog.NewJSONHandler(io.Discard, nil), PassLeveler: slog.LevelWarn, BufferLeveler: slog.LevelDebug})
	stop, err := rec.StartJournal(path, WithJournalSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(rec).With("service", "checkout").WithGroup("req")

	healthyCtx, healthy := StartScope(context.Background(), rec, ScopeMeta{TraceID: "t-healthy", Method: "GET", Path: "/ok"})
	log.DebugContext(healthyCtx, "step", "n", 1)
	healthy.End(healthyCtx, 200)

	failedCtx, failed := StartScope(context.Background(), rec, ScopeMeta{TraceID: "t-failed"})
	log.DebugContext(failedCtx, "about to fail")
	failed.End(failedCtx, 502)

	openCtx, open := StartScope(context.Background(), rec, ScopeMeta{TraceID: "t-open", Method: "POST", Path: "/pay"})
	when := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	log.DebugContext(openCtx, "charging", "amount", 4598, "ok", true, "at", when, "tags", []string{"a"})
	log.InfoContext(openCtx, "waiting for bank")

	kinds := map[uint64][]journal.Kind{}
	var openRecords []journalRecord
	var openStart scopeStart
	for _, e := range readJournal(t, path) {
		kinds[e.Scope] = append(kinds[e.Scope], e.Kind)
		if e.Scope != open.jid {
			continue
		}
		switch e.Kind {
		case journal.KindStart:
			if openStart, err = decodeScopeStart(e.Payload); err != nil {
				t.Fatal(err)
			}
		case journal.KindRecord:
			jr, err := decodeJournalRecord(e.Payload)
			if err != nil {
				t.Fatal(err)
			}
			openRecords = append(openRecords, jr)
		}
	}
	want := map[uint64][]journal.Kind{
		healthy.jid: {journal.KindStart, journal.KindRecord, journal.KindEnd},
		failed.jid:  {journal.KindStart, journal.KindRecord, journal.KindEnd},
		open.jid:    {journal.KindStart, journal.KindRecord, journal.KindRecord},
	}
	for id, w := range want {
		if len(kinds[id]) != len(w) {
			t.Errorf("scope %d entries %v, want %v", id, kinds[id], w)
			continue
		}
		for i := range w {
			if kinds[id][i] != w[i] {
				t.Errorf("scope %d entries %v, want %v", id, kinds[id], w)
			}
		}
	}
	if openStart.traceID != "t-open" || openStart.method != "POST" || openStart.path != "/pay" {
		t.Errorf("start entry %+v", openStart)
	}
	if len(openRecords) != 2 || openRecords[0].message != "charging" || openRecords[1].level != slog.LevelInfo {
		t.Fatalf("records %+v", openRecords)
	}
	r0 := openRecords[0]
	if len(r0.rootAttrs) != 1 || r0.rootAttrs[0].Key != "service" || len(r0.frames) != 1 || r0.frames[0].name != "req" {
		t.Errorf("logger attributes not kept: root %v frames %v", r0.rootAttrs, r0.frames)
	}
	got := map[string]slog.Value{}
	for _, a := range r0.attrs {
		got[a.Key] = a.Value
	}
	if got["amount"].Int64() != 4598 || !got["ok"].Bool() || !got["at"].Time().Equal(when) || got["tags"].String() != "[a]" {
		t.Errorf("record attributes %v", r0.attrs)
	}

	if st := rec.Stats(); st.JournalRecordsWritten != 4 || st.JournalBytesWritten == 0 || st.JournalOldestAge <= 0 {
		t.Errorf("stats %+v", st)
	}
	open.End(openCtx, 200)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("journal file left after stop: %v", err)
	}
	if err := stop(); err != nil {
		t.Errorf("second stop: %v", err)
	}
}

func TestStartJournalOptionsAndState(t *testing.T) {
	dir := t.TempDir()
	rec := New(Options{Next: slog.DiscardHandler})
	if _, err := rec.StartJournal(filepath.Join(dir, "small"), WithJournalSize(1<<10)); err == nil {
		t.Error("size below 1 MiB accepted")
	}
	if _, err := rec.StartJournal(filepath.Join(dir, "big"), WithJournalSize(2<<30)); err == nil {
		t.Error("size above 1 GiB accepted")
	}
	stop, err := rec.StartJournal(filepath.Join(dir, "j"), WithJournalSize(1<<20+1))
	if err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(filepath.Join(dir, "j")); fi.Size()%int64(os.Getpagesize()) != 0 {
		t.Errorf("size %d not rounded to the page size", fi.Size())
	}
	if _, err := rec.StartJournal(filepath.Join(dir, "j2")); !errors.Is(err, ErrJournalRunning) {
		t.Errorf("second StartJournal: %v, want ErrJournalRunning", err)
	}
	other := New(Options{Next: slog.DiscardHandler})
	if _, err := other.StartJournal(filepath.Join(dir, "j")); err == nil {
		t.Error("a second Recorder opened the same journal path")
	}
	_ = stop()
	stop, err = rec.StartJournal(filepath.Join(dir, "j"))
	if err != nil {
		t.Fatalf("StartJournal after stop: %v", err)
	}
	_ = stop()
}

func TestNoJournalWithoutStartJournal(t *testing.T) {
	rec := New(Options{Next: slog.DiscardHandler})
	ctx, sc := StartScope(context.Background(), rec, ScopeMeta{})
	slog.New(rec).DebugContext(ctx, "x")
	sc.End(ctx, 200)
	if sc.journal != nil || rec.Stats().JournalRecordsWritten != 0 {
		t.Error("journal used without StartJournal")
	}
}

func TestJournalCodecRoundTrip(t *testing.T) {
	rec := New(Options{Next: slog.DiscardHandler})
	r := rec.WithAttrs([]slog.Attr{slog.Float64("rootf", 0.25)}).WithGroup("g").WithAttrs([]slog.Attr{slog.String("ga", "v")}).(*Recorder)
	far := time.Date(3000, 1, 2, 3, 4, 5, 6, time.UTC)
	deep := slog.String("leaf", "x")
	for range maxGroupDepth + 5 {
		deep = slog.Group("deep", deep)
	}
	record := slog.NewRecord(time.Time{}, slog.LevelWarn, "msg", 0)
	record.AddAttrs(
		slog.Float64("f", 1.5),
		slog.String("after", "s"),
		slog.Int("n", -7),
		slog.Uint64("u", 1<<63),
		slog.Bool("b", true),
		slog.Duration("d", time.Second),
		slog.Time("far", far),
		slog.Time("zero", time.Time{}),
		slog.Any("err", errors.New("boom")),
		slog.Any("m", map[string]int{"k": 1}),
		deep,
	)
	payload := appendJournalRecord(nil, r, record, TraceContext{})
	orig := append([]byte(nil), payload...)
	jr, err := decodeJournalRecord(payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != string(orig) {
		t.Error("decoding changed its input")
	}
	if !jr.time.IsZero() || jr.level != slog.LevelWarn || jr.message != "msg" {
		t.Errorf("record header %v %v %q", jr.time, jr.level, jr.message)
	}
	if jr.rootAttrs[0].Value.Float64() != 0.25 || jr.frames[0].name != "g" || jr.frames[0].attrs[0].Value.String() != "v" {
		t.Errorf("logger attributes %v %v", jr.rootAttrs, jr.frames)
	}
	got := map[string]slog.Value{}
	for _, a := range jr.attrs {
		got[a.Key] = a.Value
	}
	if got["f"].Float64() != 1.5 || got["after"].String() != "s" || got["n"].Int64() != -7 ||
		got["u"].Uint64() != 1<<63 || !got["b"].Bool() || got["d"].Duration() != time.Second ||
		!got["far"].Time().Equal(far) || !got["zero"].Time().IsZero() {
		t.Errorf("attributes %v", jr.attrs)
	}
	if v := got["err"]; v.String() != "boom" {
		t.Errorf("error attribute %v (%v)", v, v.Kind())
	}
	if b, _ := json.Marshal(got["m"].Any()); string(b) != `{"k":1}` || got["m"].String() != "map[k:1]" {
		t.Errorf("any attribute %v", got["m"])
	}
	if got["deep"].Kind() != slog.KindGroup {
		t.Errorf("deep group kind %v", got["deep"].Kind())
	}

	s := scopeStart{start: far, traceID: "t", method: "GET", path: "/"}
	ds, err := decodeScopeStart(appendScopeStart(nil, s))
	if err != nil || !ds.start.Equal(far) || ds.traceID != "t" || ds.method != "GET" || ds.path != "/" {
		t.Errorf("scope start %+v, %v", ds, err)
	}
}

func TestStopUnderLoadDoesNotFault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j")
	rec := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug})
	log := slog.New(rec)
	done := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				ctx, sc := StartScope(context.Background(), rec, ScopeMeta{})
				log.DebugContext(ctx, "x", "n", 1)
				sc.End(ctx, 200)
			}
		}()
	}
	var faults uint64
	for range 100 {
		stop, err := rec.StartJournal(path, WithJournalSize(1<<20))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Microsecond)
		faults += rec.Stats().JournalFaults
		js := rec.state.journal.Load()
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		faults += js.file.Stats().Faults
	}
	close(done)
	wg.Wait()
	if faults != 0 {
		t.Errorf("%d memory faults while stopping under load", faults)
	}
}

func FuzzDecodeJournalPayloads(f *testing.F) {
	rec := New(Options{Next: slog.DiscardHandler})
	r := rec.WithAttrs([]slog.Attr{slog.String("a", "b")}).WithGroup("g").(*Recorder)
	record := slog.NewRecord(time.Now(), slog.LevelDebug, "m", 0)
	record.AddAttrs(slog.Int("n", 1), slog.Group("x", slog.Bool("y", true)), slog.Any("z", map[string]int{"k": 1}))
	f.Add(appendJournalRecord(nil, r, record, TraceContext{}))
	f.Add(appendScopeStart(nil, scopeStart{start: time.Now(), traceID: "t", method: "GET", path: "/"}))
	f.Fuzz(func(t *testing.T, b []byte) {
		orig := string(b)
		_, _ = decodeJournalRecord(b)
		_, _ = decodeScopeStart(b)
		if string(b) != orig {
			t.Fatal("decoding changed its input")
		}
	})
}

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

func (h *panicky) WithGroup(string) slog.Handler { return h }

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

const crashChildEnv = "FLIGHTREC_CRASH_CHILD"

// crashPayload is the attribute value written with record seq of worker w,
// so the parent can check that every recovered record is intact.
func crashPayload(w, seq int) string {
	return strings.Repeat(strconv.Itoa(w*1_000_003+seq), 1+seq%7)
}

// TestMain runs the crash child when the parent test starts this binary
// again with crashChildEnv set.
func TestMain(m *testing.M) {
	if path := os.Getenv(crashChildEnv); path != "" {
		runCrashChild(path)
		return
	}
	os.Exit(m.Run())
}

// runCrashChild journals scopes from several goroutines until it is killed.
// A kill between a scope's start and its end leaves that scope interrupted,
// so at most one per worker. It prints "working" once every worker has ended
// a scope, and exits by itself if it outlives the test.
func runCrashChild(path string) {
	time.AfterFunc(60*time.Second, func() { os.Exit(3) })
	rec := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug})
	if _, err := rec.StartJournal(path, WithJournalSize(1<<20)); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
	log := slog.New(rec).With("child", true).WithGroup("work")
	var working sync.WaitGroup
	working.Add(8)
	for w := range 8 {
		go func() {
			seq := 0
			var once sync.Once
			for {
				ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: fmt.Sprintf("w%d-s%d", w, seq), Method: "GET", Path: "/crash"})
				n := 1 + seq%5
				for range n {
					log.DebugContext(ctx, "step", "w", w, "seq", seq, "payload", crashPayload(w, seq))
					seq++
				}
				if seq%3 == 0 {
					sc.Trigger("test")
				}
				sc.End(ctx, 200)
				once.Do(working.Done)
			}
		}()
	}
	working.Wait()
	fmt.Println("working")
	select {}
}

func TestCrashRecoveryAfterKill(t *testing.T) {
	if testing.Short() {
		t.Skip("starts child processes")
	}
	path := filepath.Join(t.TempDir(), "j")
	for round := range 5 {
		cmd := exec.Command(os.Args[0])
		cmd.Env = append(os.Environ(), crashChildEnv+"="+path)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		ready := make(chan string, 1)
		go func() {
			line, _ := bufio.NewReader(stdout).ReadString('\n')
			ready <- strings.TrimSpace(line)
		}()
		select {
		case line := <-ready:
			if line != "working" {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatalf("child: %q; stderr: %s", line, stderr.String())
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("child not working after 20s; stderr: %s", stderr.String())
		}
		time.Sleep(time.Duration(rand.IntN(200)) * time.Millisecond)
		if err := cmd.Process.Kill(); err != nil { // SIGKILL
			t.Fatal(err)
		}
		_ = cmd.Wait()

		var out lineBuffer
		rec := New(Options{Next: slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})})
		stop, err := rec.StartJournal(path, WithJournalSize(2<<20))
		if err != nil {
			t.Fatal(err)
		}
		st := rec.Stats()
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if st.JournalRecoveredScopes == 0 || st.JournalDiscardedFiles != 0 || st.JournalAbandonedRecoveries != 0 {
			t.Fatalf("round %d: stats %+v", round, st)
		}
		records, summaries := 0, 0
		seen := map[[2]int]bool{}
		for _, l := range out.lines(t) {
			if l["flightrec.recovered"] != true || l["flightrec.flush_reason"] != "crash_recovery" {
				t.Fatalf("round %d: untagged line %v", round, l)
			}
			if l["msg"] == "request interrupted by crash" {
				summaries++
				continue
			}
			records++
			work, _ := l["work"].(map[string]any)
			w, _ := work["w"].(float64)
			seq, _ := work["seq"].(float64)
			if l["msg"] != "step" || l["child"] != true || work["payload"] != crashPayload(int(w), int(seq)) {
				t.Fatalf("round %d: damaged record %v", round, l)
			}
			if k := [2]int{int(w), int(seq)}; seen[k] {
				t.Fatalf("round %d: record recovered twice: %v", round, l)
			} else {
				seen[k] = true
			}
			// A scope whose start entry the ring overwrote has no trace ID:
			// these IDs are not W3C trace IDs, so records do not carry them.
			if tid, ok := l["flightrec.trace_id"]; ok && !strings.HasPrefix(fmt.Sprint(tid), fmt.Sprintf("w%d-s", int(w))) {
				t.Fatalf("round %d: record of worker %d under trace %v", round, int(w), l["flightrec.trace_id"])
			}
		}
		if uint64(summaries) != st.JournalRecoveredScopes || uint64(records) != st.JournalRecoveredRecords {
			t.Fatalf("round %d: %d summaries, %d records; stats %+v", round, summaries, records, st)
		}
		// At most one open scope per worker; a scope may also look open if the
		// kill came between its last record and its end entry.
		if summaries > 8 {
			t.Errorf("round %d: %d interrupted scopes from 8 workers", round, summaries)
		}
		t.Logf("round %d: recovered %d scopes, %d records, %d corrupt", round, summaries, records, st.JournalCorruptEntries)
	}
}

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
