// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// maxRetainedPerTrace limits how many retained scopes one trace ID can hold,
// so a client that reuses a trace ID cannot fill the retained set.
const maxRetainedPerTrace = 8

// retainedScope is the buffer of a healthy scope kept for Options.Retain.
type retainedScope struct {
	traceID    string
	entries    []bufferedEntry
	bytes      int64
	truncated  bool
	dropped    uint64
	retainedAt time.Time
	elem       *list.Element
}

// retainedSet holds retained scopes by trace ID and in age order.
type retainedSet struct {
	mu      sync.Mutex
	byTrace map[string][]*retainedScope
	order   list.List // of *retainedScope, oldest first
	bytes   int64
}

// retain moves the entries of a healthy scope into the retained set.
func (st *recorderState) retain(traceID string, entries []bufferedEntry, truncated bool, dropped uint64, now time.Time) {
	snapshotBytes := snapshotAnyValues(entries)
	rs := &retainedScope{
		traceID:    traceID,
		entries:    entries,
		truncated:  truncated,
		dropped:    dropped,
		retainedAt: now,
	}
	rs.bytes = int64(len(entries))*entrySlotSize + snapshotBytes
	for i := range entries {
		rs.bytes += entries[i].size
	}
	if rs.bytes > st.opts.MaxRetained {
		// Larger than the whole budget: drop it rather than evict everything else.
		st.retainedEvicted.Add(1)
		st.scopesHealthyDropped.Add(1)
		st.recordsHealthyDropped.Add(uint64(len(entries)))
		return
	}

	set := &st.retained
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.byTrace == nil {
		set.byTrace = make(map[string][]*retainedScope)
	}
	st.expireRetainedLocked(now)

	if same := set.byTrace[traceID]; len(same) >= maxRetainedPerTrace {
		st.dropRetainedLocked(same[0], false)
	}
	rs.elem = set.order.PushBack(rs)
	set.byTrace[traceID] = append(set.byTrace[traceID], rs)
	set.bytes += rs.bytes
	for set.bytes > st.opts.MaxRetained && set.order.Len() > 0 {
		st.dropRetainedLocked(set.order.Front().Value.(*retainedScope), false)
	}
}

// expireRetainedLocked drops retained scopes older than Options.Retain.
func (st *recorderState) expireRetainedLocked(now time.Time) {
	set := &st.retained
	cutoff := now.Add(-st.opts.Retain)
	for e := set.order.Front(); e != nil; e = set.order.Front() {
		rs := e.Value.(*retainedScope)
		if rs.retainedAt.After(cutoff) {
			return
		}
		st.dropRetainedLocked(rs, true)
	}
}

// dropRetainedLocked removes rs without writing it and counts the drop.
func (st *recorderState) dropRetainedLocked(rs *retainedScope, expired bool) {
	st.removeRetainedLocked(rs)
	if expired {
		st.retainedExpired.Add(1)
	} else {
		st.retainedEvicted.Add(1)
	}
	st.scopesHealthyDropped.Add(1)
	st.recordsHealthyDropped.Add(uint64(len(rs.entries)))
}

func (st *recorderState) removeRetainedLocked(rs *retainedScope) {
	set := &st.retained
	set.order.Remove(rs.elem)
	set.bytes -= rs.bytes
	same := set.byTrace[rs.traceID]
	for i, other := range same {
		if other == rs {
			same = append(same[:i], same[i+1:]...)
			break
		}
	}
	if len(same) == 0 {
		delete(set.byTrace, rs.traceID)
	} else {
		set.byTrace[rs.traceID] = same
	}
}

// retainedGauges returns the current retained scope count, bytes and the age
// of the oldest retained scope.
func (st *recorderState) retainedGauges() (scopes, bytes int64, oldest time.Duration) {
	set := &st.retained
	set.mu.Lock()
	defer set.mu.Unlock()
	if st.opts.Retain > 0 && set.byTrace != nil {
		st.expireRetainedLocked(st.now())
	}
	if e := set.order.Front(); e != nil {
		oldest = st.now().Sub(e.Value.(*retainedScope).retainedAt)
	}
	return int64(set.order.Len()), set.bytes, oldest
}

// FlushTrace writes every retained scope for traceID, as if each had been
// triggered with reason, and returns how many it wrote. A scope is retained
// only when Options.Retain is set; scopes that have expired or been evicted
// are gone. An empty reason is recorded as explicit_flush.
//
// Records are written with a background context carrying the trace context
// captured at log time; the original request context is not kept.
func (r *Recorder) FlushTrace(traceID, reason string) int {
	st := r.state
	if st.opts.Retain <= 0 || traceID == "" {
		return 0
	}
	if reason == "" {
		reason = "explicit_flush"
	}

	set := &st.retained
	set.mu.Lock()
	if set.byTrace == nil {
		set.mu.Unlock()
		return 0
	}
	st.expireRetainedLocked(st.now())
	scopes := append([]*retainedScope(nil), set.byTrace[traceID]...)
	for _, rs := range scopes {
		st.removeRetainedLocked(rs)
	}
	set.mu.Unlock()

	for _, rs := range scopes {
		st.scopesFlushed.Add(1)
		st.retainedFlushed.Add(1)
		st.writeFlushed(context.Background(), rs.entries, rs.traceID, reason, rs.truncated, rs.dropped)
	}
	return len(scopes)
}

// writeFlushed writes buffered entries to Next in order, tagged with the
// flush reason, restoring each entry's log-time trace context.
func (st *recorderState) writeFlushed(ctx context.Context, entries []bufferedEntry, traceID, reason string, truncated bool, dropped uint64) {
	scopeTraceBytes, hasScopeTraceBytes := ParseTraceIDHex(traceID)
	for i, e := range entries {
		tc := e.traceCtx
		if !tc.HasTraceID() && hasScopeTraceBytes {
			tc.TraceID = scopeTraceBytes
		}
		outRec := e.recorder.buildOutputRecord(e.rec, traceID, tc.SpanIDHex(), reason, truncated && i == 0, dropped)
		_ = st.baseNext.Handle(st.injectTraceContext(ctx, tc), outRec)
	}
	st.recordsFlushed.Add(uint64(len(entries)))
}

// snapshotAnyValues replaces slog.KindAny attribute values in entries with a
// copy taken now. A retained scope can be written long after its request
// ended, by which time the objects those values point to may have changed or
// been reused by another request. Values are encoded the way slog's
// JSONHandler encodes them, as raw JSON, so the output looks the same; errors
// become their message, and values JSON cannot encode become a string.
//
// It returns the bytes the snapshots add.
func snapshotAnyValues(entries []bufferedEntry) int64 {
	var added int64
	for i := range entries {
		rec := &entries[i].rec
		changed := false
		rec.Attrs(func(a slog.Attr) bool {
			if hasAnyValue(a.Value) {
				changed = true
				return false
			}
			return true
		})
		if !changed {
			continue
		}
		out := slog.NewRecord(rec.Time, rec.Level, rec.Message, rec.PC)
		rec.Attrs(func(a slog.Attr) bool {
			out.AddAttrs(snapshotAttr(a, &added))
			return true
		})
		*rec = out
	}
	return added
}

func hasAnyValue(v slog.Value) bool {
	switch v.Kind() {
	case slog.KindAny:
		return true
	case slog.KindGroup:
		for _, a := range v.Group() {
			if hasAnyValue(a.Value) {
				return true
			}
		}
	}
	return false
}

func snapshotAttr(a slog.Attr, added *int64) slog.Attr {
	switch a.Value.Kind() {
	case slog.KindAny:
		a.Value = snapshotAny(a.Value.Any())
		if raw, ok := a.Value.Any().(json.RawMessage); ok {
			*added += int64(len(raw))
		} else {
			*added += int64(len(a.Value.String()))
		}
	case slog.KindGroup:
		group := a.Value.Group()
		attrs := make([]slog.Attr, len(group))
		for i, ga := range group {
			attrs[i] = snapshotAttr(ga, added)
		}
		a.Value = slog.GroupValue(attrs...)
	}
	return a
}

func snapshotAny(v any) slog.Value {
	if _, isMarshaler := v.(json.Marshaler); !isMarshaler {
		if err, ok := v.(error); ok {
			return slog.StringValue(err.Error())
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return slog.StringValue("!ERROR:" + err.Error())
	}
	return slog.AnyValue(json.RawMessage(bytes.TrimRight(buf.Bytes(), "\n")))
}
