// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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

// TestNotifierPublishesForwardedGeneratedTraceIDs covers a service that
// generates the trace ID and forwards it to the services it calls: its
// failure must reach them.
func TestNotifierPublishesForwardedGeneratedTraceIDs(t *testing.T) {
	bus := memnotifier.New()
	b := newNode(t, bus, flightrec.Options{Retain: time.Minute})
	c := newNode(t, bus, flightrec.Options{})
	listening(t, bus, 2)

	ctx, sc := flightrec.StartScope(context.Background(), c.rec, flightrec.ScopeMeta{}) // no trace ID: one is generated
	tid := flightrec.TraceID(ctx)
	healthyRequest(b.rec, b.log, tid, "b: called with the forwarded ID")
	c.log.DebugContext(ctx, "failing step")
	sc.End(ctx, 502)

	eventually(t, "b to flush the retained scope", func() bool { return b.rec.Stats().RemoteFlushes == 1 })
	// The publisher counts after Publish returns, which can be after b
	// received the notice.
	eventually(t, "c to count its publish", func() bool { return c.rec.Stats().NotifierPublished == 1 })
	if !strings.Contains(b.out.String(), "remote:http_502") {
		t.Errorf("b output lacks remote:http_502:\n%s", b.out.String())
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

// TestAdoptedHeaderIDStaysAnAlias covers a service that receives a forwarded
// X-Request-Id, then adopts its OpenTelemetry span's trace (flightrec outside
// the OTel middleware). Other services know the request only by the header:
// their notices must still reach it, and its failures must reach them.
func TestAdoptedHeaderIDStaysAnAlias(t *testing.T) {
	const spanTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	serve := func(n *node, requestID string, status int) {
		h := flightrec.HTTPMiddleware(n.rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sc := flightrec.ScopeFromContext(r.Context())
			if old, ok := sc.AdoptGeneratedTraceID(spanTrace); !ok || old != requestID {
				t.Errorf("AdoptGeneratedTraceID = %q, %v; want %q, true", old, ok, requestID)
			}
			n.log.DebugContext(r.Context(), "handled", "id", requestID)
			w.WriteHeader(status)
		}))
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Request-Id", requestID)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}

	t.Run("notice for the header ID flushes the adopted scope", func(t *testing.T) {
		bus := memnotifier.New()
		b := newNode(t, bus, flightrec.Options{Retain: time.Minute})
		c := newNode(t, bus, flightrec.Options{})
		listening(t, bus, 2)
		serve(b, "req-T", http.StatusOK)
		failRequest(c, "req-T")
		eventually(t, "b to flush its retained scope", func() bool { return b.rec.Stats().RemoteFlushes == 1 })
		if out := b.out.String(); !strings.Contains(out, "remote:http_502") || !strings.Contains(out, spanTrace) || !strings.Contains(out, `"flightrec.request_id":"req-T"`) {
			t.Errorf("b output:\n%s", out)
		}
	})

	t.Run("failure is announced under the header ID too", func(t *testing.T) {
		bus := memnotifier.New()
		a := newNode(t, bus, flightrec.Options{Retain: time.Minute})
		b := newNode(t, bus, flightrec.Options{})
		listening(t, bus, 2)
		healthyRequest(a.rec, a.log, "req-U", "a: forwarded req-U")
		serve(b, "req-U", http.StatusBadGateway)
		eventually(t, "a to flush its retained scope", func() bool { return a.rec.Stats().RemoteFlushes == 1 })
	})
}

func TestExplicitTraceIDIsNeverAdopted(t *testing.T) {
	rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{TraceID: "job-7"})
	defer sc.End(ctx, 200)
	if _, ok := sc.AdoptGeneratedTraceID("4bf92f3577b34da6a3ce929d0e0e4736"); ok || sc.TraceID() != "job-7" {
		t.Errorf("explicit trace ID replaced: %q", sc.TraceID())
	}
	gctx, gen := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
	defer gen.End(gctx, 200)
	id := gen.TraceID()
	if _, ok := gen.AdoptGeneratedTraceID(id); ok {
		t.Error("adopting the same ID reported a change")
	}
}

// TestAliasRetainedCap checks that the per-trace cap of retained scopes holds
// for a forwarded request ID shared by many scopes with their own span traces.
func TestAliasRetainedCap(t *testing.T) {
	rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug, Retain: time.Minute})
	log := slog.New(rec)
	for i := range 20 {
		h := flightrec.HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			flightrec.ScopeFromContext(r.Context()).AdoptGeneratedTraceID(fmt.Sprintf("%016x0000000000000001", uint64(0x1000000000000000)+uint64(i)))
			log.DebugContext(r.Context(), "x")
		}))
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Request-Id", "req-same")
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	if n := rec.FlushTrace(context.Background(), "req-same", "test"); n != 8 {
		t.Errorf("FlushTrace(alias) wrote %d scopes, want the cap of 8", n)
	}
}

func TestChildScopeInheritsAlias(t *testing.T) {
	bus := memnotifier.New()
	b := newNode(t, bus, flightrec.Options{})
	c := newNode(t, bus, flightrec.Options{})
	listening(t, bus, 2)
	done := make(chan struct{})
	h := flightrec.HTTPMiddleware(b.rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flightrec.ScopeFromContext(r.Context()).AdoptGeneratedTraceID("4bf92f3577b34da6a3ce929d0e0e4736")
		ctx, child := flightrec.StartScope(r.Context(), b.rec, flightrec.ScopeMeta{})
		b.log.DebugContext(ctx, "child work")
		failRequest(c, "req-V")
		eventually(t, "the child to be triggered", func() bool { ok, _ := child.TriggerState(); return ok })
		child.End(ctx, 200)
		close(done)
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-Id", "req-V")
	h.ServeHTTP(httptest.NewRecorder(), req)
	<-done
}

// TestSignedNotifier checks that processes sharing a signing key exchange
// notices, and that a process with another key, or none, cannot trigger them.
func TestSignedNotifier(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	start := func(bus *memnotifier.Bus, opts ...flightrec.NotifierOption) *node {
		nd := &node{out: &syncBuffer{}}
		nd.rec = flightrec.New(flightrec.Options{Next: slog.NewJSONHandler(nd.out, nil), BufferLeveler: slog.LevelDebug, Retain: time.Minute})
		nd.log = slog.New(nd.rec)
		t.Cleanup(nd.rec.StartNotifier(context.Background(), bus, opts...))
		return nd
	}
	bus := memnotifier.New()
	b := start(bus, flightrec.WithSigningKey(key))
	forger := start(bus, flightrec.WithSigningKey([]byte("ffffffffffffffffffffffffffffffff")))
	plain := start(bus)
	c := start(bus, flightrec.WithSigningKey(key))
	listening(t, bus, 4)

	healthyRequest(b.rec, b.log, "t-sig", "b: retained")
	failRequest(forger, "t-sig")
	failRequest(plain, "t-sig")
	eventually(t, "b to reject both", func() bool { return b.rec.Stats().NotifierRejected >= 2 })
	if got := b.rec.Stats().RemoteFlushes; got != 0 {
		t.Fatalf("unsigned or wrongly signed notice flushed b (%d)", got)
	}
	failRequest(c, "t-sig")
	eventually(t, "b to flush on the signed notice", func() bool { return b.rec.Stats().RemoteFlushes == 1 })
}
