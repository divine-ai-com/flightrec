// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/divine-ai-com/flightrec"
)

func TestScopeSendFlushMarker(t *testing.T) {
	record := func(dst *string) func(string) error {
		return func(v string) error { *dst = v; return nil }
	}

	rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
	var sent string
	if sc.SendFlushMarker(record(&sent)) || sent != "" {
		t.Errorf("untriggered scope sent %q", sent)
	}
	flightrec.Flush(ctx, "downstream:http_503")
	if !sc.SendFlushMarker(record(&sent)) || sent != "http_503" {
		t.Errorf("sent %q, want http_503 with the prefix removed", sent)
	}
	if sc.SendFlushMarker(func(string) error { return errors.New("trailer already sent") }) {
		t.Error("SendFlushMarker reported success for a failed send")
	}
	sc.End(ctx, 200)
	if st := rec.Stats(); st.FlushMarkersSent != 1 || st.FlushMarkersMissed != 1 {
		t.Errorf("FlushMarkersSent=%d FlushMarkersMissed=%d, want 1 and 1", st.FlushMarkersSent, st.FlushMarkersMissed)
	}

	ctx, sc = flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
	flightrec.Flush(ctx, "payment mismatch")
	if sc.SendFlushMarker(record(&sent)); sent != "explicit_flush" {
		t.Errorf("custom reason sent as %q, want explicit_flush", sent)
	}
	sc.End(ctx, 200)

	off := flightrec.New(flightrec.Options{Next: slog.DiscardHandler, DisableFlushMarker: true})
	ctx, sc = flightrec.StartScope(context.Background(), off, flightrec.ScopeMeta{})
	flightrec.Flush(ctx, "x")
	called := false
	if sc.SendFlushMarker(func(string) error { called = true; return nil }) || called {
		t.Error("SendFlushMarker sent with DisableFlushMarker")
	}
	sc.End(ctx, 200)
	if st := off.Stats(); st.FlushMarkersSent != 0 || st.FlushMarkersMissed != 0 {
		t.Errorf("counters moved with DisableFlushMarker: %+v", st)
	}

	var nilScope *flightrec.Scope
	if nilScope.SendFlushMarker(record(&sent)) {
		t.Error("nil scope sent a marker")
	}
}

func TestReceiveMarker(t *testing.T) {
	for _, tc := range []struct {
		name       string
		values     []string
		want       bool
		wantReason string
		rejected   uint64
	}{
		{"http_502", []string{"http_502"}, true, "downstream:http_502", 0},
		{"slow_request", []string{"slow_request"}, true, "downstream:slow_request", 0},
		{"none", nil, false, "", 0},
		{"unknown", []string{"nope"}, false, "", 1},
		{"several", []string{"http_502", "http_502"}, false, "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
			ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
			if got := flightrec.ReceiveMarker(ctx, tc.values); got != tc.want {
				t.Errorf("ReceiveMarker = %v, want %v", got, tc.want)
			}
			triggered, reason := sc.TriggerState()
			if triggered != tc.want || reason != tc.wantReason {
				t.Errorf("TriggerState = %v %q, want %v %q", triggered, reason, tc.want, tc.wantReason)
			}
			st := rec.Stats()
			wantDown := uint64(0)
			if tc.want {
				wantDown = 1
			}
			if st.MarkersRejected != tc.rejected || st.DownstreamTriggers != wantDown {
				t.Errorf("MarkersRejected=%d DownstreamTriggers=%d, want %d and %d", st.MarkersRejected, st.DownstreamTriggers, tc.rejected, wantDown)
			}
			sc.End(ctx, 200)
		})
	}

	t.Run("already triggered", func(t *testing.T) {
		rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
		ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
		flightrec.Flush(ctx, "error_log")
		if flightrec.ReceiveMarker(ctx, []string{"http_502"}) {
			t.Error("ReceiveMarker reported a trigger on an already triggered scope")
		}
		if st := rec.Stats(); st.DownstreamTriggers != 0 {
			t.Errorf("DownstreamTriggers = %d, want 0", st.DownstreamTriggers)
		}
		sc.End(ctx, 200)
	})

	if flightrec.ReceiveMarker(context.Background(), []string{"http_502"}) {
		t.Error("ReceiveMarker without a scope reported a trigger")
	}
}
