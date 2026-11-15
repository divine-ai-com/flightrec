// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/divine-ai-com/flightrec"
	"github.com/divine-ai-com/flightrec/notifier/memnotifier"
)

// node is one process connected to a shared notifier.
type node struct {
	rec *flightrec.Recorder
	log *slog.Logger
	out *syncBuffer
}

func newNode(t *testing.T, n flightrec.Notifier, opts flightrec.Options) *node {
	t.Helper()
	nd := &node{out: &syncBuffer{}}
	opts.Next = slog.NewJSONHandler(nd.out, &slog.HandlerOptions{Level: slog.LevelDebug})
	opts.PassLeveler = slog.LevelWarn
	opts.BufferLeveler = slog.LevelDebug
	nd.rec = flightrec.New(opts)
	nd.log = slog.New(nd.rec)
	stop := nd.rec.StartNotifier(context.Background(), n)
	t.Cleanup(stop)
	return nd
}

// eventually fails the test if cond is not true within two seconds.
// listening waits until bus has n subscribers, so no notice is lost to a
// subscription that has not started yet.
func listening(t *testing.T, bus *memnotifier.Bus, n int) {
	t.Helper()
	eventually(t, "subscriptions to start", func() bool { return bus.Subscribers() == n })
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func flushedReasons(t *testing.T, out *syncBuffer) map[string]string {
	t.Helper()
	got := map[string]string{}
	for _, ln := range out.Lines(t) {
		if r, ok := ln["flightrec.flush_reason"].(string); ok {
			got[ln["msg"].(string)] = r
		}
	}
	return got
}

func failRequest(n *node, traceID string) {
	ctx, sc := flightrec.StartScope(context.Background(), n.rec, flightrec.ScopeMeta{TraceID: traceID})
	n.log.DebugContext(ctx, "failing step")
	sc.End(ctx, 502)
}

func TestNotifierFlushesRetainedScopeInAnotherProcess(t *testing.T) {
	bus := memnotifier.New()
	b := newNode(t, bus, flightrec.Options{Retain: time.Minute})
	c := newNode(t, bus, flightrec.Options{})
	listening(t, bus, 2)

	healthyRequest(b.rec, b.log, "trace-1", "b: queued email")
	failRequest(c, "trace-1")

	eventually(t, "b to flush its retained scope", func() bool {
		return flushedReasons(t, b.out)["b: queued email"] == "remote:http_502"
	})
	eventually(t, "c to count its publish", func() bool { return c.rec.Stats().NotifierPublished == 1 })
	eventually(t, "b to count its remote flush", func() bool { return b.rec.Stats().RemoteFlushes == 1 })
	time.Sleep(20 * time.Millisecond)
	if st := b.rec.Stats(); st.RemoteFlushes != 1 || st.NotifierPublished != 0 {
		t.Errorf("b RemoteFlushes=%d NotifierPublished=%d, want 1 and 0 (no echo)", st.RemoteFlushes, st.NotifierPublished)
	}
}

func TestNotifierTriggersActiveScope(t *testing.T) {
	bus := memnotifier.New()
	b := newNode(t, bus, flightrec.Options{})
	c := newNode(t, bus, flightrec.Options{})
	listening(t, bus, 2)

	ctx, sc := flightrec.StartScope(context.Background(), b.rec, flightrec.ScopeMeta{TraceID: "trace-2"})
	b.log.DebugContext(ctx, "b: still working")
	failRequest(c, "trace-2")
	eventually(t, "b's active scope to be triggered", func() bool {
		triggered, _ := sc.TriggerState()
		return triggered
	})
	sc.End(ctx, 200)

	if got := flushedReasons(t, b.out)["b: still working"]; got != "remote:http_502" {
		t.Errorf("flush reason = %q, want remote:http_502", got)
	}
}

func TestNotifierTriggersScopeStartedAfterNotice(t *testing.T) {
	bus := memnotifier.New()
	b := newNode(t, bus, flightrec.Options{})
	c := newNode(t, bus, flightrec.Options{})

	listening(t, bus, 2)
	failRequest(c, "trace-3")
	// Start scopes for the trace until delivery has recorded the notice.
	eventually(t, "a late scope to be triggered", func() bool {
		ctx, sc := flightrec.StartScope(context.Background(), b.rec, flightrec.ScopeMeta{TraceID: "trace-3"})
		triggered, _ := sc.TriggerState()
		sc.End(ctx, 200)
		return triggered
	})
}

func TestNotifierRekeyedScope(t *testing.T) {
	bus := memnotifier.New()
	b := newNode(t, bus, flightrec.Options{})
	c := newNode(t, bus, flightrec.Options{})
	listening(t, bus, 2)

	ctx, sc := flightrec.StartScope(context.Background(), b.rec, flightrec.ScopeMeta{})
	const otelID = "4bf92f3577b34da6a3ce929d0e0e4736"
	if _, ok := sc.AdoptGeneratedTraceID(otelID); !ok {
		t.Fatal("AdoptGeneratedTraceID failed")
	}
	failRequest(c, otelID)
	eventually(t, "the re-keyed scope to be triggered", func() bool {
		triggered, _ := sc.TriggerState()
		return triggered
	})
	sc.End(ctx, 200)
}

func TestNotifierLocalFailureFlushesRetainedSiblings(t *testing.T) {
	n := newNode(t, memnotifier.New(), flightrec.Options{Retain: time.Minute})
	healthyRequest(n.rec, n.log, "trace-4", "earlier async task")
	failRequest(n, "trace-4")

	if got := flushedReasons(t, n.out)["earlier async task"]; got != "remote:http_502" {
		t.Errorf("sibling flush reason = %q, want remote:http_502", got)
	}
	if st := n.rec.Stats(); st.RemoteFlushes != 1 {
		t.Errorf("RemoteFlushes = %d, want 1 (own notice must not apply twice)", st.RemoteFlushes)
	}
}

func TestNotifierPublishesOncePerTrace(t *testing.T) {
	bus := memnotifier.New()
	c := newNode(t, bus, flightrec.Options{})
	for i := 0; i < 5; i++ {
		failRequest(c, "trace-5")
	}
	eventually(t, "one publish", func() bool { return c.rec.Stats().NotifierPublished == 1 })
	time.Sleep(20 * time.Millisecond)
	if got := c.rec.Stats().NotifierPublished; got != 1 {
		t.Errorf("NotifierPublished = %d for five failures of one trace, want 1", got)
	}
}

func TestNotifierIgnoresMalformedMessages(t *testing.T) {
	bus := memnotifier.New()
	b := newNode(t, bus, flightrec.Options{})
	listening(t, bus, 1)
	for _, msg := range []string{
		`not json`,
		`{"v":2,"o":"x","n":[{"t":"trace-6","r":"http_502"}]}`,
		`{"v":1,"o":"x","n":[{"t":"bad id","r":"http_502"}]}`,
		`{"v":1,"o":"x","n":[{"t":"trace-6","r":"payment mismatch"}]}`,
	} {
		_ = bus.Publish(context.Background(), []byte(msg))
	}
	_ = bus.Publish(context.Background(), make([]byte, 70<<10)) // too large
	eventually(t, "malformed notices to be counted", func() bool { return b.rec.Stats().NotifierRejected == 5 })
	if got := b.rec.Stats().NotifierDeliveryDropped; got != 0 {
		t.Errorf("malformed input counted as load shedding: NotifierDeliveryDropped=%d", got)
	}

	ctx, sc := flightrec.StartScope(context.Background(), b.rec, flightrec.ScopeMeta{TraceID: "trace-6"})
	if triggered, _ := sc.TriggerState(); triggered {
		t.Error("a malformed notice triggered a scope")
	}
	sc.End(ctx, 200)
}

// flakyNotifier fails the first Subscribe call, then behaves like its Bus.
type flakyNotifier struct {
	*memnotifier.Bus
	calls atomic.Int32
}

func (f *flakyNotifier) Subscribe(ctx context.Context, deliver func([]byte)) error {
	if f.calls.Add(1) == 1 {
		return errors.New("connection refused")
	}
	return f.Bus.Subscribe(ctx, deliver)
}

func TestNotifierResubscribesAfterError(t *testing.T) {
	bus := memnotifier.New()
	flaky := &flakyNotifier{Bus: bus}
	b := newNode(t, flaky, flightrec.Options{})
	c := newNode(t, bus, flightrec.Options{})

	eventually(t, "a second Subscribe call", func() bool { return flaky.calls.Load() >= 2 })
	listening(t, bus, 2)
	if got := b.rec.Stats().NotifierSubscribeErrors; got != 1 {
		t.Errorf("NotifierSubscribeErrors = %d, want 1", got)
	}
	ctx, sc := flightrec.StartScope(context.Background(), b.rec, flightrec.ScopeMeta{TraceID: "trace-7"})
	failRequest(c, "trace-7")
	eventually(t, "delivery after resubscribing", func() bool {
		triggered, _ := sc.TriggerState()
		return triggered
	})
	sc.End(ctx, 200)
}

func TestNotifierStop(t *testing.T) {
	bus := memnotifier.New()
	rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
	stop := rec.StartNotifier(context.Background(), bus)
	listening(t, bus, 1)
	stop()
	stop() // idempotent
	if n := bus.Subscribers(); n != 0 {
		t.Fatalf("subscription still active after stop: %d", n)
	}

	c := newNode(t, bus, flightrec.Options{})
	listening(t, bus, 1)
	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "trace-8"})
	failRequest(c, "trace-8")
	time.Sleep(30 * time.Millisecond)
	if triggered, _ := sc.TriggerState(); triggered {
		t.Error("a stopped notifier still delivered")
	}
	sc.End(ctx, 200)
}

func TestNotifierConcurrent(t *testing.T) {
	bus := memnotifier.New()
	nodes := []*node{
		newNode(t, bus, flightrec.Options{Retain: time.Minute}),
		newNode(t, bus, flightrec.Options{Retain: time.Minute}),
		newNode(t, bus, flightrec.Options{Retain: time.Minute}),
	}
	listening(t, bus, 3)
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, n *node) {
			defer wg.Done()
			for j := 0; j < 300; j++ {
				id := "trace-c-" + string(rune('a'+j%26))
				if (i+j)%10 == 0 {
					failRequest(n, id)
				} else {
					healthyRequest(n.rec, n.log, id, "x")
				}
			}
		}(i, n)
	}
	wg.Wait()
	for _, n := range nodes {
		if st := n.rec.Stats(); st.ActiveScopes != 0 {
			t.Errorf("leaked active scopes: %+v", st)
		}
	}
}

func TestNotifierDoesNotPublishGeneratedTraceIDs(t *testing.T) {
	bus := memnotifier.New()
	c := newNode(t, bus, flightrec.Options{})
	listening(t, bus, 1)
	failRequest(c, "") // no trace ID: one is generated
	time.Sleep(30 * time.Millisecond)
	if got := c.rec.Stats().NotifierPublished; got != 0 {
		t.Errorf("NotifierPublished = %d for a generated trace ID, want 0", got)
	}
}

// TestNotifierNoticeRacingRetain ends a healthy scope at the same time as a
// remote failure for its trace arrives. Whatever the interleaving, the
// scope's records must be written.
func TestNotifierNoticeRacingRetain(t *testing.T) {
	bus := memnotifier.New()
	b := newNode(t, bus, flightrec.Options{Retain: time.Minute})
	c := newNode(t, bus, flightrec.Options{})
	listening(t, bus, 2)

	// Stay within the remote flush burst (100), so any missing scope is a
	// lost notice, not deliberate load shedding.
	const n = 90
	for i := 0; i < n; i++ {
		id := "race-" + strconv.Itoa(i)
		ctx, sc := flightrec.StartScope(context.Background(), b.rec, flightrec.ScopeMeta{TraceID: id})
		b.log.DebugContext(ctx, id)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); sc.End(ctx, 200) }()
		go func() { defer wg.Done(); failRequest(c, id) }()
		wg.Wait()
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(flushedReasons(t, b.out)) != n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(flushedReasons(t, b.out)); got != n {
		t.Fatalf("%d of %d racing scopes written; stats %+v", got, n, b.rec.Stats())
	}
	if st := b.rec.Stats(); st.NotifierDeliveryDropped != 0 || st.RetainedScopes != 0 {
		t.Errorf("unexpected drops or leftovers: %+v", st)
	}
}
