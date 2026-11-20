// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"context"
	"encoding/hex"
	"sync/atomic"
)

// TraceContext holds a W3C 16-byte TraceID, 8-byte SpanID, and trace flags
// extracted from a context.Context at log or scope-start time.
//
// Storing TraceContext as fixed-size byte arrays inside buffered entries avoids
// heap allocations on buffered log calls that are later discarded.
type TraceContext struct {
	TraceID    [16]byte
	SpanID     [8]byte
	TraceFlags byte
	Remote     bool
}

// HasTraceID reports whether TraceID is non-zero.
func (tc TraceContext) HasTraceID() bool {
	return tc.TraceID != ([16]byte{})
}

// HasSpanID reports whether SpanID is non-zero.
func (tc TraceContext) HasSpanID() bool {
	return tc.SpanID != ([8]byte{})
}

// TraceIDHex returns the 32-character lowercase hex encoding of TraceID,
// or "" if TraceID is all zeros.
func (tc TraceContext) TraceIDHex() string {
	if !tc.HasTraceID() {
		return ""
	}
	return hex.EncodeToString(tc.TraceID[:])
}

// SpanIDHex returns the 16-character lowercase hex encoding of SpanID,
// or "" if SpanID is all zeros.
func (tc TraceContext) SpanIDHex() string {
	if !tc.HasSpanID() {
		return ""
	}
	return hex.EncodeToString(tc.SpanID[:])
}

// ParseTraceIDHex parses a 32-character hex string into a non-zero 16-byte TraceID.
func ParseTraceIDHex(s string) ([16]byte, bool) {
	var out [16]byte
	if len(s) != 32 {
		return out, false
	}
	n, err := hex.Decode(out[:], []byte(s))
	if err != nil || n != 16 || out == ([16]byte{}) {
		return [16]byte{}, false
	}
	return out, true
}

// TraceExtractor extracts a TraceContext from ctx without allocating.
type TraceExtractor func(ctx context.Context) TraceContext

// TraceInjector returns a derived context carrying tc so downstream log
// handlers (such as OpenTelemetry log bridges) see the log-time TraceID and
// SpanID when buffered records are flushed.
type TraceInjector func(ctx context.Context, tc TraceContext) context.Context

type traceBridgePair struct {
	extract TraceExtractor
	inject  TraceInjector
}

var defaultTraceBridge atomic.Pointer[traceBridgePair]

// RegisterDefaultTraceBridge registers the process-wide default TraceExtractor
// and TraceInjector used when Options.TraceExtractor or Options.TraceInjector
// are not set explicitly. Importing github.com/haawklabs/flightrec/otelflightrec
// registers an OpenTelemetry bridge automatically.
func RegisterDefaultTraceBridge(extract TraceExtractor, inject TraceInjector) {
	defaultTraceBridge.Store(&traceBridgePair{
		extract: extract,
		inject:  inject,
	})
}

func extractDefaultTraceContext(ctx context.Context) TraceContext {
	if ctx == nil {
		return TraceContext{}
	}
	if bp := defaultTraceBridge.Load(); bp != nil && bp.extract != nil {
		return bp.extract(ctx)
	}
	return TraceContext{}
}

// ScopeObserver receives lifecycle notifications from Scopes created by a
// Recorder so an attached trace processor (such as otelflightrec.SpanProcessor)
// can coordinate trace and log flushing bidirectionally.
type ScopeObserver interface {
	OnScopeStart(ctx context.Context, sc *Scope, traceID string)
	OnScopeTrigger(sc *Scope, traceID, reason string)
	OnScopeEnd(sc *Scope, traceID string, triggered bool, reason string)
}

// AttachObserver registers a ScopeObserver with the Recorder. Multiple
// observers may be attached; duplicate registrations of the same observer are
// ignored.
func (r *Recorder) AttachObserver(obs ScopeObserver) {
	if r == nil || r.state == nil || obs == nil {
		return
	}
	st := r.state
	st.obsMu.Lock()
	for _, existing := range st.observers {
		if existing == obs {
			st.obsMu.Unlock()
			return
		}
	}
	copied := make([]ScopeObserver, len(st.observers)+1)
	copy(copied, st.observers)
	copied[len(st.observers)] = obs
	st.observers = copied
	st.obsSnapshot.Store(&copied)
	st.obsMu.Unlock()
}

func (st *recorderState) loadObservers() []ScopeObserver {
	if p := st.obsSnapshot.Load(); p != nil {
		return *p
	}
	return nil
}

func (st *recorderState) notifyScopeStart(ctx context.Context, sc *Scope, traceID string) {
	for _, obs := range st.loadObservers() {
		obs.OnScopeStart(ctx, sc, traceID)
	}
}

func (st *recorderState) notifyScopeTrigger(sc *Scope, traceID, reason string) {
	for _, obs := range st.loadObservers() {
		obs.OnScopeTrigger(sc, traceID, reason)
	}
}

func (st *recorderState) notifyScopeEnd(sc *Scope, traceID string, triggered bool, reason string) {
	for _, obs := range st.loadObservers() {
		obs.OnScopeEnd(sc, traceID, triggered, reason)
	}
}
