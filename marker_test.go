// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/divine-ai-com/flightrec"
)

func TestScopeFlushMarker(t *testing.T) {
	rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
	if v, ok := sc.FlushMarker(); ok {
		t.Errorf("untriggered scope returned marker %q", v)
	}
	flightrec.Flush(ctx, "downstream:http_503")
	if v, ok := sc.FlushMarker(); !ok || v != "http_503" {
		t.Errorf("FlushMarker = %q, %v; want http_503, true", v, ok)
	}
	sc.End(ctx, 200)
	if got := rec.Stats().FlushMarkersSent; got != 1 {
		t.Errorf("FlushMarkersSent = %d, want 1", got)
	}

	off := flightrec.New(flightrec.Options{Next: slog.DiscardHandler, DisableFlushMarker: true})
	ctx, sc = flightrec.StartScope(context.Background(), off, flightrec.ScopeMeta{})
	flightrec.Flush(ctx, "x")
	if _, ok := sc.FlushMarker(); ok {
		t.Error("FlushMarker returned a value with DisableFlushMarker")
	}
	sc.End(ctx, 200)
}

func TestReceiveMarker(t *testing.T) {
	rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
	for _, tc := range []struct {
		name     string
		values   []string
		want     bool
		rejected uint64
	}{
		{"valid", []string{"http_502"}, true, 0},
		{"none", nil, false, 0},
		{"unknown", []string{"nope"}, false, 1},
		{"several", []string{"http_502", "http_502"}, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := rec.Stats().MarkersRejected
			ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
			if got := flightrec.ReceiveMarker(ctx, tc.values); got != tc.want {
				t.Errorf("ReceiveMarker = %v, want %v", got, tc.want)
			}
			if got := rec.Stats().MarkersRejected - before; got != tc.rejected {
				t.Errorf("MarkersRejected grew by %d, want %d", got, tc.rejected)
			}
			if triggered, reason := sc.TriggerState(); triggered != tc.want || (tc.want && reason != "downstream:http_502") {
				t.Errorf("TriggerState = %v %q", triggered, reason)
			}
			sc.End(ctx, 200)
		})
	}
	if flightrec.ReceiveMarker(context.Background(), []string{"http_502"}) {
		t.Error("ReceiveMarker without a scope reported a trigger")
	}
}
