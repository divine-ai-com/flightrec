// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package flightrec

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/divine-ai-com/flightrec/internal/journal"
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
	if v := got["err"]; v.Kind() != slog.KindString || v.String() != "boom" {
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
