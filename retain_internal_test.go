// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestRetainedScopesExpireWithoutTraffic(t *testing.T) {
	rec := New(Options{Next: slog.NewJSONHandler(io.Discard, nil), Retain: 30 * time.Millisecond})
	log := slog.New(rec)
	ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "t"})
	log.DebugContext(ctx, "x")
	sc.End(ctx, 200)

	set := &rec.state.retained
	deadline := time.Now().Add(2 * time.Second)
	for {
		set.mu.Lock()
		n, timerSet := set.order.Len(), set.timer != nil
		set.mu.Unlock()
		if n == 0 && !timerSet {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained scope still held after its window with no other calls: n=%d timer=%v", n, timerSet)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
