// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package natsnotifier_test

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	"github.com/haawklabs/flightrec"
	"github.com/haawklabs/flightrec/notifier/natsnotifier"
)

var _ flightrec.Notifier = (*natsnotifier.Notifier)(nil)

func startServer(t *testing.T) string {
	t.Helper()
	s := natsserver.RunRandClientPortServer()
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}

func connect(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func mustNew(t *testing.T, nc *nats.Conn, opts ...natsnotifier.Option) *natsnotifier.Notifier {
	t.Helper()
	n, err := natsnotifier.New(nc, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return n
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

type collector struct {
	mu   sync.Mutex
	msgs []string
}

func (c *collector) deliver(m []byte) {
	c.mu.Lock()
	c.msgs = append(c.msgs, string(m))
	c.mu.Unlock()
}

func (c *collector) got() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.msgs...)
}

func TestPublishSubscribe(t *testing.T) {
	url := startServer(t)
	subConn := connect(t, url)
	sub := mustNew(t, subConn, natsnotifier.WithSubject("flightrec.test"))
	pub := mustNew(t, connect(t, url), natsnotifier.WithSubject("flightrec.test"))
	other := mustNew(t, connect(t, url), natsnotifier.WithSubject("flightrec.other"))

	ctx, cancel := context.WithCancel(context.Background())
	var c collector
	done := make(chan error, 1)
	go func() { done <- sub.Subscribe(ctx, c.deliver) }()
	waitFor(t, "the subscription", func() bool { return subConn.NumSubscriptions() == 1 })
	_ = subConn.Flush() // the subscribe round trip may still be in flight

	if err := other.Publish(context.Background(), []byte("wrong subject")); err != nil {
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
	waitFor(t, "unsubscribe", func() bool { return subConn.NumSubscriptions() == 0 })
}

func TestSubscribeReturnsErrorWhenConnectionCloses(t *testing.T) {
	nc := connect(t, startServer(t))
	n := mustNew(t, nc)
	done := make(chan error, 1)
	go func() { done <- n.Subscribe(context.Background(), func([]byte) {}) }()
	waitFor(t, "the subscription", func() bool { return nc.NumSubscriptions() == 1 })

	nc.Close()
	select {
	case err := <-done:
		if !errors.Is(err, nats.ErrConnectionClosed) {
			t.Errorf("Subscribe returned %v, want ErrConnectionClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Subscribe did not return after the connection closed")
	}
	if err := n.Subscribe(context.Background(), func([]byte) {}); !errors.Is(err, nats.ErrConnectionClosed) {
		t.Errorf("Subscribe on a closed connection returned %v", err)
	}
}

func TestOptionsAreValidated(t *testing.T) {
	nc := connect(t, startServer(t))
	if _, err := natsnotifier.New(nc, natsnotifier.WithSubject("")); err == nil {
		t.Error("empty subject accepted")
	}
	if _, err := natsnotifier.New(nil); err == nil {
		t.Error("nil connection accepted")
	}
}

func TestCrossServiceFlushOverNATS(t *testing.T) {
	url := startServer(t)
	var mu sync.Mutex
	var flushed []string
	capture := &captureHandler{onFlush: func(msg, reason string) {
		mu.Lock()
		flushed = append(flushed, msg+" "+reason)
		mu.Unlock()
	}}

	email := flightrec.New(flightrec.Options{Next: capture, Retain: time.Minute})
	checkout := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
	emailConn, checkoutConn := connect(t, url), connect(t, url)
	ctx := context.Background()
	defer email.StartNotifier(ctx, mustNew(t, emailConn), flightrec.WithoutSigning())()
	defer checkout.StartNotifier(ctx, mustNew(t, checkoutConn), flightrec.WithoutSigning())()
	waitFor(t, "both subscriptions", func() bool {
		return emailConn.NumSubscriptions() == 1 && checkoutConn.NumSubscriptions() == 1
	})
	_ = emailConn.Flush()

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

type captureHandler struct{ onFlush func(msg, reason string) }

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

func TestSubscribeSurvivesReconnect(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	srv := natsserver.RunServer(&opts)
	port := srv.Addr().(*net.TCPAddr).Port
	url := srv.ClientURL()

	nc, err := nats.Connect(url, nats.ReconnectWait(20*time.Millisecond), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	n := mustNew(t, nc)

	var c collector
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- n.Subscribe(ctx, c.deliver) }()
	waitFor(t, "the subscription", func() bool { return nc.NumSubscriptions() == 1 })

	srv.Shutdown()
	opts.Port = port
	srv = natsserver.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	waitFor(t, "the client to reconnect", nc.IsConnected)

	pub := mustNew(t, connect(t, url))
	waitFor(t, "delivery after the reconnect", func() bool {
		_ = pub.Publish(context.Background(), []byte("after"))
		return len(c.got()) > 0
	})
	select {
	case err := <-done:
		t.Fatalf("Subscribe returned %v across a reconnect", err)
	default:
	}
}

func TestSubscribeRefusedByPermissions(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.Users = []*server.User{{
		Username: "svc", Password: "pw",
		Permissions: &server.Permissions{Subscribe: &server.SubjectPermission{Deny: []string{"flightrec.>"}}},
	}}
	srv := natsserver.RunServer(&opts)
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL(), nats.UserInfo("svc", "pw"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	done := make(chan error, 1)
	go func() { done <- mustNew(t, nc).Subscribe(context.Background(), func([]byte) {}) }()
	select {
	case err := <-done:
		if !errors.Is(err, nats.ErrPermissionViolation) {
			t.Errorf("Subscribe returned %v, want a permissions violation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe blocked on a refused subscription")
	}
}
