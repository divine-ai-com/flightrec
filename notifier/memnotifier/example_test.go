// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package memnotifier_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/divine-ai-com/flightrec"
	"github.com/divine-ai-com/flightrec/notifier/memnotifier"
)

// Two Recorders share a Bus. A failure in "checkout" flushes the logs that
// "email" kept for the same trace after its own request had succeeded.
func Example() {
	bus := memnotifier.New()
	ctx := context.Background()

	email := flightrec.New(flightrec.Options{Next: printer("email"), Retain: time.Minute})
	checkout := flightrec.New(flightrec.Options{Next: printer("checkout")})
	defer email.StartNotifier(ctx, bus)()
	defer checkout.StartNotifier(ctx, bus)()
	for bus.Subscribers() < 2 {
		time.Sleep(time.Millisecond)
	}

	sctx, sc := flightrec.StartScope(ctx, email, flightrec.ScopeMeta{TraceID: "order-7"})
	slog.New(email).DebugContext(sctx, "receipt queued")
	sc.End(sctx, 200) // healthy: retained, not written

	fctx, fc := flightrec.StartScope(ctx, checkout, flightrec.ScopeMeta{TraceID: "order-7"})
	slog.New(checkout).DebugContext(fctx, "card declined")
	fc.End(fctx, 502) // failed: written, and announced to email

	for email.Stats().RemoteFlushes == 0 {
		time.Sleep(time.Millisecond)
	}
	// Output:
	// checkout: card declined (http_502)
	// email: receipt queued (remote:http_502)
}

// printer returns a handler that prints flushed records only.
func printer(service string) slog.Handler {
	return &printHandler{service: service}
}

type printHandler struct{ service string }

func (h *printHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *printHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *printHandler) WithGroup(string) slog.Handler            { return h }
func (h *printHandler) Handle(_ context.Context, r slog.Record) error {
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "flightrec.flush_reason" {
			fmt.Printf("%s: %s (%s)\n", h.service, r.Message, a.Value)
		}
		return true
	})
	return nil
}
