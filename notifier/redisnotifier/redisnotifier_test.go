// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package redisnotifier_test

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/divine-ai-com/flightrec"
	"github.com/divine-ai-com/flightrec/notifier/redisnotifier"
)

func mustNew(t *testing.T, c redis.UniversalClient, opts ...redisnotifier.Option) *redisnotifier.Notifier {
	t.Helper()
	n, err := redisnotifier.New(c, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func newClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// collector records delivered messages.
type collector struct {
	mu   sync.Mutex
	msgs []string
}

func (c *collector) deliver(msg []byte) {
	c.mu.Lock()
	c.msgs = append(c.msgs, string(msg))
	c.mu.Unlock()
}

func (c *collector) got() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.msgs...)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPublishSubscribe(t *testing.T) {
	srv := miniredis.RunT(t)
	sub := mustNew(t, newClient(t, srv.Addr()), redisnotifier.WithChannel("flightrec:test"))
	pub := mustNew(t, newClient(t, srv.Addr()), redisnotifier.WithChannel("flightrec:test"))
	other := mustNew(t, newClient(t, srv.Addr()), redisnotifier.WithChannel("flightrec:other"))

	ctx, cancel := context.WithCancel(context.Background())
	var c collector
	done := make(chan error, 1)
	go func() { done <- sub.Subscribe(ctx, c.deliver) }()
	waitFor(t, "the subscription", func() bool { return len(srv.PubSubChannels("flightrec:*")) == 1 })

	if err := other.Publish(context.Background(), []byte("wrong channel")); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(context.Background(), []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "delivery", func() bool { return len(c.got()) == 1 })
	if got := c.got(); got[0] != `{"v":1}` {
		t.Errorf("delivered %q", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Subscribe returned %v after cancel, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Subscribe did not return after cancel")
	}
}

func TestSubscribeReturnsErrorWhenServerFails(t *testing.T) {
	srv := miniredis.RunT(t)
	n := mustNew(t, newClient(t, srv.Addr()))
	done := make(chan error, 1)
	go func() { done <- n.Subscribe(context.Background(), func([]byte) {}) }()
	waitFor(t, "the subscription", func() bool { return len(srv.PubSubChannels("")) == 1 })

	srv.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Error("Subscribe returned nil after the server went away; flightrec would not resubscribe")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not return after the server went away")
	}
}

func TestSubscribeUnreachableServer(t *testing.T) {
	n := mustNew(t, newClient(t, "127.0.0.1:1"))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := n.Subscribe(ctx, func([]byte) {}); err == nil {
		t.Error("Subscribe to an unreachable server returned nil")
	}
}

func TestOptionsAreValidated(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer func() { _ = c.Close() }()
	if _, err := redisnotifier.New(c, redisnotifier.WithChannel("")); err == nil {
		t.Error("empty channel name accepted")
	}
	if _, err := redisnotifier.New(c, redisnotifier.WithHealthCheckInterval(0)); err == nil {
		t.Error("zero health check interval accepted")
	}
	if _, err := redisnotifier.New(nil); err == nil {
		t.Error("nil client accepted")
	}
}

func TestDefaultChannel(t *testing.T) {
	srv := miniredis.RunT(t)
	n := mustNew(t, newClient(t, srv.Addr()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = n.Subscribe(ctx, func([]byte) {}) }()
	waitFor(t, "a subscription on the default channel", func() bool {
		return len(srv.PubSubChannels(redisnotifier.DefaultChannel)) == 1
	})
}

// TestCrossServiceFlushOverRedis connects two Recorders through Redis: a
// failure in "checkout" flushes the logs "email" retained for the same trace.
func TestCrossServiceFlushOverRedis(t *testing.T) {
	srv := miniredis.RunT(t)
	var mu sync.Mutex
	var flushed []string
	capture := &captureHandler{onFlush: func(msg, reason string) {
		mu.Lock()
		flushed = append(flushed, msg+" "+reason)
		mu.Unlock()
	}}

	email := flightrec.New(flightrec.Options{Next: capture, Retain: time.Minute})
	checkout := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
	ctx := context.Background()
	defer email.StartNotifier(ctx, mustNew(t, newClient(t, srv.Addr())), flightrec.WithoutSigning())()
	defer checkout.StartNotifier(ctx, mustNew(t, newClient(t, srv.Addr())), flightrec.WithoutSigning())()
	waitFor(t, "both subscriptions", func() bool {
		return srv.PubSubNumSub(redisnotifier.DefaultChannel)[redisnotifier.DefaultChannel] == 2
	})

	sctx, sc := flightrec.StartScope(ctx, email, flightrec.ScopeMeta{TraceID: "order-7"})
	slog.New(email).DebugContext(sctx, "receipt queued")
	sc.End(sctx, 200)

	fctx, fc := flightrec.StartScope(ctx, checkout, flightrec.ScopeMeta{TraceID: "order-7"})
	fc.End(fctx, 502)

	waitFor(t, "email to flush its retained scope", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(flushed) == 1
	})
	if flushed[0] != "receipt queued remote:http_502" {
		t.Errorf("flushed %q", flushed[0])
	}
}

// captureHandler reports flushed records.
type captureHandler struct {
	onFlush func(msg, reason string)
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *captureHandler) WithGroup(string) slog.Handler            { return h }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "flightrec.flush_reason" {
			h.onFlush(r.Message, a.Value.String())
		}
		return true
	})
	return nil
}

var _ flightrec.Notifier = (*redisnotifier.Notifier)(nil)

// stallingProxy forwards TCP traffic until stall is called, then stops
// forwarding without closing the connections, like a NAT that silently drops
// an idle connection.
type stallingProxy struct {
	ln      net.Listener
	target  string
	stalled atomic.Bool
}

func newStallingProxy(t *testing.T, target string) *stallingProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallingProxy{ln: ln, target: target}
	t.Cleanup(func() { _ = ln.Close() })
	go p.serve()
	return p
}

func (p *stallingProxy) addr() string { return p.ln.Addr().String() }
func (p *stallingProxy) stall()       { p.stalled.Store(true) }

func (p *stallingProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		go p.pipe(c, up)
		go p.pipe(up, c)
	}
}

func (p *stallingProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		if p.stalled.Load() {
			continue // swallow: the peer sees nothing, not even a close
		}
		if _, err := dst.Write(buf[:n]); err != nil {
			return
		}
	}
}

func TestSubscribeDetectsDeadConnection(t *testing.T) {
	srv := miniredis.RunT(t)
	proxy := newStallingProxy(t, srv.Addr())
	n := mustNew(t, newClient(t, proxy.addr()), redisnotifier.WithHealthCheckInterval(100*time.Millisecond))

	done := make(chan error, 1)
	go func() { done <- n.Subscribe(context.Background(), func([]byte) {}) }()
	waitFor(t, "the subscription", func() bool { return len(srv.PubSubChannels("")) == 1 })

	// Healthy and idle for a few intervals: pings are answered, no error.
	select {
	case err := <-done:
		t.Fatalf("Subscribe returned %v on a healthy idle connection", err)
	case <-time.After(350 * time.Millisecond):
	}

	proxy.stall()
	select {
	case err := <-done:
		if err == nil {
			t.Error("Subscribe returned nil for a dead connection")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Subscribe did not detect a dead connection")
	}
}
