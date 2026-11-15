// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/divine-ai-com/flightrec"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newRetainRecorder(t *testing.T, opts flightrec.Options) (*flightrec.Recorder, *slog.Logger, *syncBuffer) {
	t.Helper()
	out := &syncBuffer{}
	opts.Next = slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug})
	opts.PassLeveler = slog.LevelWarn
	opts.BufferLeveler = slog.LevelDebug
	rec := flightrec.New(opts)
	return rec, slog.New(rec), out
}

// healthyRequest runs one scope that logs msgs and ends with status 200.
func healthyRequest(rec *flightrec.Recorder, log *slog.Logger, traceID string, msgs ...string) {
	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: traceID})
	for _, m := range msgs {
		log.DebugContext(ctx, m)
	}
	sc.End(ctx, 200)
}

func TestRetainedScopeIsFlushedLater(t *testing.T) {
	clock := newFakeClock()
	rec, log, out := newRetainRecorder(t, flightrec.Options{Retain: 30 * time.Second, Now: clock.Now})

	healthyRequest(rec, log, "trace-a", "step 1", "step 2")
	lines := out.Lines(t)
	if len(lines) != 1 || lines[0]["msg"] != "request completed" || lines[0]["flightrec.retained"] != true {
		t.Fatalf("want only a summary marked retained, got %+v", lines)
	}
	if st := rec.Stats(); st.RetainedScopes != 1 || st.ScopesHealthyDropped != 0 || st.RetainedBytes <= 0 {
		t.Fatalf("after End: %+v", st)
	}

	clock.Advance(10 * time.Second)
	if n := rec.FlushTrace("trace-a", "remote:http_502"); n != 1 {
		t.Fatalf("FlushTrace = %d, want 1", n)
	}
	var msgs []string
	for _, ln := range out.Lines(t)[1:] {
		if ln["flightrec.flush_reason"] != "remote:http_502" || ln["flightrec.trace_id"] != "trace-a" {
			t.Errorf("flushed record %+v", ln)
		}
		msgs = append(msgs, ln["msg"].(string))
	}
	if strings.Join(msgs, ",") != "step 1,step 2" {
		t.Errorf("flushed %v, want [step 1 step 2] in order", msgs)
	}
	st := rec.Stats()
	if st.RetainedScopes != 0 || st.RetainedBytes != 0 || st.RetainedFlushed != 1 || st.ScopesFlushed != 1 || st.RecordsFlushed != 2 {
		t.Errorf("after FlushTrace: %+v", st)
	}
	if n := rec.FlushTrace("trace-a", "again"); n != 0 {
		t.Errorf("second FlushTrace = %d, want 0", n)
	}
}

func TestRetainedScopeExpires(t *testing.T) {
	clock := newFakeClock()
	rec, log, _ := newRetainRecorder(t, flightrec.Options{Retain: 30 * time.Second, Now: clock.Now})

	healthyRequest(rec, log, "trace-a", "one", "two")
	clock.Advance(20 * time.Second)
	if age := rec.Stats().RetainedOldestAge; age != 20*time.Second {
		t.Errorf("RetainedOldestAge = %v, want 20s", age)
	}
	clock.Advance(11 * time.Second)

	if n := rec.FlushTrace("trace-a", "late"); n != 0 {
		t.Errorf("FlushTrace after expiry = %d, want 0", n)
	}
	st := rec.Stats()
	if st.RetainedExpired != 1 || st.RetainedScopes != 0 || st.ScopesHealthyDropped != 1 || st.RecordsHealthyDropped != 2 {
		t.Errorf("after expiry: %+v", st)
	}
}

func TestRetainedBytesStayWithinMaxRetained(t *testing.T) {
	const limit = 8 << 10
	rec, log, _ := newRetainRecorder(t, flightrec.Options{Retain: time.Minute, MaxRetained: limit})

	for i := 0; i < 100; i++ {
		healthyRequest(rec, log, fmt.Sprintf("trace-%03d", i), strings.Repeat("x", 200))
	}
	st := rec.Stats()
	if st.RetainedBytes > limit {
		t.Fatalf("RetainedBytes = %d, over MaxRetained %d", st.RetainedBytes, limit)
	}
	if st.RetainedEvicted == 0 || st.RetainedScopes == 0 {
		t.Fatalf("expected evictions and some retained scopes: %+v", st)
	}
	if rec.FlushTrace("trace-000", "x") != 0 {
		t.Error("oldest scope should have been evicted first")
	}
	if rec.FlushTrace("trace-099", "x") != 1 {
		t.Error("newest scope should still be retained")
	}
}

func TestOversizedScopeDoesNotEvictOthers(t *testing.T) {
	rec, log, _ := newRetainRecorder(t, flightrec.Options{Retain: time.Minute, MaxRetained: 4 << 10})
	healthyRequest(rec, log, "small", "ok")
	healthyRequest(rec, log, "huge", strings.Repeat("y", 8<<10))

	if rec.FlushTrace("small", "x") != 1 {
		t.Error("an oversized scope evicted a smaller retained scope")
	}
	if st := rec.Stats(); st.RetainedEvicted != 1 {
		t.Errorf("RetainedEvicted = %d, want 1 (the oversized scope)", st.RetainedEvicted)
	}
}

func TestRetainedScopesPerTraceAreLimited(t *testing.T) {
	rec, log, _ := newRetainRecorder(t, flightrec.Options{Retain: time.Minute})
	for i := 0; i < 12; i++ {
		healthyRequest(rec, log, "shared-trace", fmt.Sprintf("request %d", i))
	}
	st := rec.Stats()
	if st.RetainedScopes != 8 || st.RetainedEvicted != 4 {
		t.Fatalf("RetainedScopes=%d RetainedEvicted=%d, want 8 and 4", st.RetainedScopes, st.RetainedEvicted)
	}
	if n := rec.FlushTrace("shared-trace", "x"); n != 8 {
		t.Errorf("FlushTrace = %d, want 8", n)
	}
}

type order struct {
	ID    int    `json:"id"`
	State string `json:"state"`
}

func TestRetainedAnyValuesAreSnapshotted(t *testing.T) {
	rec, log, out := newRetainRecorder(t, flightrec.Options{Retain: time.Minute})

	o := &order{ID: 7, State: "pending"}
	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "t"})
	log.DebugContext(ctx, "order loaded", "order", o, "err", errors.New("card declined"),
		slog.Group("g", slog.Any("tags", []string{"a", "b"})))
	sc.End(ctx, 200)

	o.State = "paid" // the object is reused after the request ended
	rec.FlushTrace("t", "x")

	ln := out.Lines(t)[1]
	got, ok := ln["order"].(map[string]any)
	if !ok || got["state"] != "pending" || got["id"] != float64(7) {
		t.Errorf("order = %#v, want the JSON object as it was at log time", ln["order"])
	}
	if ln["err"] != "card declined" {
		t.Errorf("err = %#v, want the error message", ln["err"])
	}
	if g, ok := ln["g"].(map[string]any); !ok || fmt.Sprint(g["tags"]) != "[a b]" {
		t.Errorf("g = %#v, want tags inside the group", ln["g"])
	}
}

func TestRetentionDisabledByDefault(t *testing.T) {
	rec, log, _ := newRetainRecorder(t, flightrec.Options{})
	healthyRequest(rec, log, "t", "x")
	if n := rec.FlushTrace("t", "x"); n != 0 {
		t.Errorf("FlushTrace without Retain = %d", n)
	}
	if st := rec.Stats(); st.ScopesHealthyDropped != 1 || st.RetainedScopes != 0 {
		t.Errorf("%+v", st)
	}
}

func TestTriggeredScopeIsNotRetained(t *testing.T) {
	rec, log, _ := newRetainRecorder(t, flightrec.Options{Retain: time.Minute})
	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "t"})
	log.DebugContext(ctx, "x")
	sc.End(ctx, 500)
	if st := rec.Stats(); st.RetainedScopes != 0 || st.ScopesFlushed != 1 {
		t.Errorf("%+v", st)
	}
}

func TestRetentionConcurrent(t *testing.T) {
	rec, log, _ := newRetainRecorder(t, flightrec.Options{Retain: time.Minute, MaxRetained: 64 << 10})
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id := fmt.Sprintf("trace-%d", i%20)
				healthyRequest(rec, log, id, "a", "b")
				if i%7 == 0 {
					rec.FlushTrace(id, "x")
				}
				_ = rec.Stats()
			}
		}(g)
	}
	wg.Wait()
	st := rec.Stats()
	if st.RetainedBytes < 0 || st.RetainedBytes > 64<<10 || st.ActiveScopes != 0 {
		t.Fatalf("inconsistent stats after concurrent use: %+v", st)
	}
}
