// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"bytes"
	"container/list"
	"context"
	"encoding"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"
)

// maxRetainedPerTrace limits how many retained scopes one trace ID can hold,
// so a client that reuses a trace ID cannot fill the retained set.
const maxRetainedPerTrace = 8

// retainedScope is the buffer of a healthy scope kept for Options.Retain.
type retainedScope struct {
	traceID    string
	alias      string // a request header ID the scope also answers to
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
	timer   *time.Timer // releases expired scopes when no other call does
}

// retain moves the entries of a healthy scope into the retained set.
func (st *recorderState) retain(traceID, alias string, entries []bufferedEntry, truncated bool, dropped uint64, now time.Time) {
	snapshotBytes := snapshotAnyValues(entries, st.opts.MaxRetained)
	rs := &retainedScope{
		traceID:    traceID,
		alias:      alias,
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
	if alias != "" {
		// The cap applies to the alias as well: many scopes can share one
		// forwarded request ID while each has its own span trace.
		if same := set.byTrace[alias]; len(same) >= maxRetainedPerTrace {
			st.dropRetainedLocked(same[0], false)
		}
	}
	rs.elem = set.order.PushBack(rs)
	set.byTrace[traceID] = append(set.byTrace[traceID], rs)
	if alias != "" {
		set.byTrace[alias] = append(set.byTrace[alias], rs)
	}
	set.bytes += rs.bytes
	for set.bytes > st.opts.MaxRetained && set.order.Len() > 0 {
		st.dropRetainedLocked(set.order.Front().Value.(*retainedScope), false)
	}
	if set.timer == nil {
		set.timer = time.AfterFunc(st.opts.Retain, st.sweepRetained)
	}
}

// sweepRetained runs from a timer so that retained scopes are released after
// Options.Retain even when no request, FlushTrace or Stats call follows.
func (st *recorderState) sweepRetained() {
	set := &st.retained
	set.mu.Lock()
	defer set.mu.Unlock()
	now := st.now()
	st.expireRetainedLocked(now)
	front := set.order.Front()
	if front == nil {
		set.timer = nil
		return
	}
	next := front.Value.(*retainedScope).retainedAt.Add(st.opts.Retain).Sub(now)
	if next < 10*time.Millisecond {
		next = 10 * time.Millisecond
	}
	set.timer = time.AfterFunc(next, st.sweepRetained)
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
	for _, key := range []string{rs.traceID, rs.alias} {
		if key == "" {
			continue
		}
		same := set.byTrace[key]
		for i, other := range same {
			if other == rs {
				same = append(same[:i], same[i+1:]...)
				break
			}
		}
		if len(same) == 0 {
			delete(set.byTrace, key)
		} else {
			set.byTrace[key] = same
		}
	}
}

// retainedGauges expires old retained scopes, then returns the retained scope
// count, bytes and the age of the oldest retained scope.
func (st *recorderState) retainedGauges() (scopes, bytes int64, oldest time.Duration) {
	if st.opts.Retain <= 0 {
		return 0, 0, 0
	}
	set := &st.retained
	set.mu.Lock()
	defer set.mu.Unlock()
	now := st.now()
	st.expireRetainedLocked(now)
	if e := set.order.Front(); e != nil {
		oldest = now.Sub(e.Value.(*retainedScope).retainedAt)
	}
	return int64(set.order.Len()), set.bytes, oldest
}

// hasRetained reports whether any scope is retained for traceID.
func (st *recorderState) hasRetained(traceID string) bool {
	if st.opts.Retain <= 0 {
		return false
	}
	set := &st.retained
	set.mu.Lock()
	defer set.mu.Unlock()
	return len(set.byTrace[traceID]) > 0
}

// FlushTrace writes every retained scope for traceID, as if each had been
// triggered with reason, and returns how many it wrote. A scope is retained
// only when Options.Retain is set; scopes that have expired or been evicted
// are gone. An empty reason is recorded as explicit_flush.
//
// FlushTrace does not affect scopes for the trace that are still running;
// trigger those with Flush on their context, or let a notifier do it (see
// StartNotifier).
//
// Records are written to Next with ctx carrying the trace context captured
// at log time; the original request context is not kept.
func (r *Recorder) FlushTrace(ctx context.Context, traceID, reason string) int {
	if reason == "" {
		reason = "explicit_flush"
	}
	return r.state.flushRetained(ctx, traceID, reason)
}

// flushRetained implements FlushTrace.
func (st *recorderState) flushRetained(ctx context.Context, traceID, reason string) int {
	if st.opts.Retain <= 0 || traceID == "" {
		return 0
	}
	if ctx == nil {
		ctx = context.Background()
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
		st.writeFlushed(ctx, rs.entries, rs.traceID, rs.alias, reason, rs.truncated, rs.dropped)
	}
	return len(scopes)
}

// writeFlushed writes buffered entries to Next in order, tagged with the
// flush reason, restoring each entry's log-time trace context.
//
// alias, when set, is the request header ID the scope was known by before it
// adopted a span's trace; it is written as flightrec.request_id so the logs
// can still be found by it.
func (st *recorderState) writeFlushed(ctx context.Context, entries []bufferedEntry, traceID, alias, reason string, truncated bool, dropped uint64) {
	scopeTraceBytes, hasScopeTraceBytes := ParseTraceIDHex(traceID)
	if len(entries) == 0 && truncated {
		// Every buffered record was dropped (memory limits, or one record
		// larger than MaxPerScope): say so, or the failed request leaves no
		// trace in the log.
		rec := slog.NewRecord(st.now(), slog.LevelWarn, "buffered records dropped", 0)
		rec.AddAttrs(
			slog.String("flightrec.trace_id", traceID),
			slog.String("flightrec.flush_reason", reason),
			slog.Bool("flightrec.truncated", true),
			slog.Uint64("flightrec.dropped_records", dropped),
		)
		if alias != "" {
			rec.AddAttrs(slog.String("flightrec.request_id", alias))
		}
		tctx := ctx
		if hasScopeTraceBytes {
			tctx = st.injectTraceContext(ctx, TraceContext{TraceID: scopeTraceBytes})
		}
		_ = st.baseNext.Handle(tctx, rec)
		return
	}
	for i, e := range entries {
		tc := e.traceCtx
		if !tc.HasTraceID() && hasScopeTraceBytes {
			tc.TraceID = scopeTraceBytes
		}
		outRec := e.recorder.buildOutputRecord(e.rec, traceID, tc.SpanIDHex(), reason, truncated && i == 0, dropped)
		if alias != "" {
			outRec.AddAttrs(slog.String("flightrec.request_id", alias))
		}
		_ = st.baseNext.Handle(st.injectTraceContext(ctx, tc), outRec)
	}
	st.recordsFlushed.Add(uint64(len(entries)))
}

// snapshotAnyValues replaces slog.KindAny attribute values in entries, and in
// attributes added with Logger.With, with a copy taken now. A retained scope
// can be written long after its request ended, by which time the objects
// those values point to may have changed or been reused by another request.
//
// The copy is a snapshotValue: the value as JSON, written unchanged by
// slog.JSONHandler and as text by slog.TextHandler. Errors become their
// message, and values JSON cannot encode become a string describing the
// error. It returns the bytes the copies add.
func snapshotAnyValues(entries []bufferedEntry, budget int64) int64 {
	sn := snapshotter{budget: budget}
	copied := make(map[*Recorder]*Recorder)
	for i := range entries {
		e := &entries[i]
		if r, ok := copied[e.recorder]; ok {
			e.recorder = r
		} else {
			r := e.recorder.snapshotAttrs(&sn)
			copied[e.recorder] = r
			e.recorder = r
		}

		rec := &e.rec
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
			out.AddAttrs(sn.attr(a))
			return true
		})
		*rec = out
	}
	return sn.added
}

// snapshotter copies values for one retained scope. The same pointer is
// often logged many times in one request, so pointer values are encoded once.
type snapshotter struct {
	budget   int64 // stop copying once added exceeds it; 0 means no limit
	added    int64
	pointers map[any]slog.Value
}

// snapshotAttrs returns r, or a copy of r whose Logger.With attributes have
// their slog.KindAny values copied.
func (r *Recorder) snapshotAttrs(sn *snapshotter) *Recorder {
	needs := false
	for _, a := range r.rootAttrs {
		needs = needs || hasAnyValue(a.Value)
	}
	for _, f := range r.frames {
		for _, a := range f.attrs {
			needs = needs || hasAnyValue(a.Value)
		}
	}
	if !needs {
		return r
	}
	cp := *r
	cp.rootAttrs = make([]slog.Attr, len(r.rootAttrs))
	for i, a := range r.rootAttrs {
		cp.rootAttrs[i] = sn.attr(a)
	}
	cp.frames = make([]groupFrame, len(r.frames))
	for i, f := range r.frames {
		attrs := make([]slog.Attr, len(f.attrs))
		for j, a := range f.attrs {
			attrs[j] = sn.attr(a)
		}
		cp.frames[i] = groupFrame{name: f.name, attrs: attrs}
	}
	return &cp
}

func hasAnyValue(v slog.Value) bool {
	switch v.Kind() {
	case slog.KindAny:
		_, done := v.Any().(snapshotValue)
		return !done
	case slog.KindGroup:
		for _, a := range v.Group() {
			if hasAnyValue(a.Value) {
				return true
			}
		}
	}
	return false
}

func (sn *snapshotter) attr(a slog.Attr) slog.Attr {
	switch a.Value.Kind() {
	case slog.KindAny:
		v := a.Value.Any()
		if _, done := v.(snapshotValue); done {
			return a
		}
		isPointer := v != nil && reflect.TypeOf(v).Kind() == reflect.Pointer
		if isPointer {
			if cached, ok := sn.pointers[v]; ok {
				a.Value = cached
				return a
			}
		}
		if sn.budget > 0 && sn.added > sn.budget {
			// Copying more could not be retained anyway: stop paying for it.
			a.Value = slog.StringValue("!TRUNCATED: retained budget exceeded")
			return a
		}
		a.Value = snapshotAny(v)
		if s, ok := a.Value.Any().(snapshotValue); ok {
			sn.added += s.size()
		} else {
			sn.added += int64(len(a.Value.String()))
		}
		if isPointer {
			if sn.pointers == nil {
				sn.pointers = make(map[any]slog.Value)
			}
			sn.pointers[v] = a.Value
		}
	case slog.KindGroup:
		group := a.Value.Group()
		attrs := make([]slog.Attr, len(group))
		for i, ga := range group {
			attrs[i] = sn.attr(ga)
		}
		a.Value = slog.GroupValue(attrs...)
	}
	return a
}

// maxSnapshotValue bounds one copied slog.Any value, in each of its two
// renderings. Larger values are replaced by a short marker.
const maxSnapshotValue = 64 << 10

// snapshotTruncated replaces a value too large to copy.
const snapshotTruncated = "!TRUNCATED: value larger than 64 KB"

// snapshotAny copies v as the two renderings handlers use: JSON, as
// slog.JSONHandler writes values (json.Marshal, honouring MarshalJSON), and
// text, as slog.TextHandler writes them (MarshalText if v has it, otherwise
// fmt's %+v, which honours String). Keeping the text rendering preserves
// redaction that a type does in String or MarshalText.
func snapshotAny(v any) slog.Value {
	if _, isMarshaler := v.(json.Marshaler); !isMarshaler {
		if err, ok := v.(error); ok {
			return slog.StringValue(err.Error())
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	var raw []byte
	if err := enc.Encode(v); err != nil {
		raw, _ = json.Marshal("!ERROR:" + err.Error())
	} else {
		raw = bytes.TrimRight(buf.Bytes(), "\n")
	}
	var text string
	if tm, ok := v.(encoding.TextMarshaler); ok {
		if b, err := tm.MarshalText(); err == nil {
			text = string(b)
		} else {
			text = "!ERROR:" + err.Error()
		}
	} else {
		text = fmt.Sprintf("%+v", v)
	}
	if len(raw) > maxSnapshotValue {
		raw, _ = json.Marshal(snapshotTruncated)
	}
	if len(text) > maxSnapshotValue {
		text = snapshotTruncated
	}
	return slog.AnyValue(snapshotValue{raw: raw, text: text})
}

// snapshotValue is a copy of a slog.Any value taken when its scope was
// retained or journaled. slog.JSONHandler writes it as the original JSON;
// slog.TextHandler and fmt write the text the original value produced.
type snapshotValue struct {
	raw  []byte
	text string
}

// MarshalJSON implements json.Marshaler.
func (v snapshotValue) MarshalJSON() ([]byte, error) { return v.raw, nil }

// MarshalText implements encoding.TextMarshaler.
func (v snapshotValue) MarshalText() ([]byte, error) { return []byte(v.text), nil }

// String implements fmt.Stringer.
func (v snapshotValue) String() string { return v.text }

// size is the memory the copy holds.
func (v snapshotValue) size() int64 { return int64(len(v.raw) + len(v.text)) }
