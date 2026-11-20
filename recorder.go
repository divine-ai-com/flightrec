// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type groupFrame struct {
	name  string
	attrs []slog.Attr
}

// Recorder is a slog.Handler that buffers records below PassLeveler per
// request scope and flushes them to Next only when the scope triggers.
//
// Values implementing slog.LogValuer are resolved at log time before
// buffering; raw pointers, slices, and maps that do not implement
// slog.LogValuer are captured by reference.
type Recorder struct {
	state     *recorderState
	next      slog.Handler
	rootAttrs []slog.Attr
	frames    []groupFrame
}

type recorderState struct {
	opts          Options
	baseNext      slog.Handler
	passLeveler   slog.Leveler
	bufferLeveler slog.Leveler
	now           func() time.Time

	totalBytes   atomic.Int64
	activeScopes atomic.Int64

	scopesStarted             atomic.Uint64
	scopesFlushed             atomic.Uint64
	scopesHealthyDropped      atomic.Uint64
	scopesEvictedTotalCap     atomic.Uint64
	recordsPassedThrough      atomic.Uint64
	recordsBuffered           atomic.Uint64
	recordsFlushed            atomic.Uint64
	recordsHealthyDropped     atomic.Uint64
	recordsDroppedPerScopeCap atomic.Uint64
	recordsDroppedTotalCap    atomic.Uint64
	flushMarkersSent          atomic.Uint64
	flushMarkersMissed        atomic.Uint64
	downstreamTriggers        atomic.Uint64
	markersRejected           atomic.Uint64
	retainedFlushed           atomic.Uint64
	retainedExpired           atomic.Uint64
	retainedEvicted           atomic.Uint64
	remoteFlushes             atomic.Uint64
	notifierPublished         atomic.Uint64
	notifierPublishDropped    atomic.Uint64
	notifierPublishErrors     atomic.Uint64
	notifierDeliveryDropped   atomic.Uint64
	notifierRejected          atomic.Uint64
	notifierSubscribeErrors   atomic.Uint64

	journal    atomic.Pointer[journalState]
	journalMu  sync.Mutex // serializes StartJournal and stop
	journalIDs atomic.Uint64

	journalRecoveredScopes  atomic.Uint64
	journalRecoveredRecords atomic.Uint64
	journalCorrupt          atomic.Uint64
	journalDiscarded        atomic.Uint64
	journalAbandoned        atomic.Uint64
	journalRecoveryErrors   atomic.Uint64

	hub   atomic.Pointer[notifierHub]
	hubMu sync.Mutex // serializes StartNotifier and stop

	retained retainedSet

	listMu sync.Mutex
	oldest *Scope
	newest *Scope

	obsMu       sync.Mutex
	observers   []ScopeObserver
	obsSnapshot atomic.Pointer[[]ScopeObserver]
}

func (st *recorderState) extractTraceContext(ctx context.Context) TraceContext {
	if ctx == nil {
		return TraceContext{}
	}
	if st.opts.TraceExtractor != nil {
		return st.opts.TraceExtractor(ctx)
	}
	return extractDefaultTraceContext(ctx)
}

func (st *recorderState) injectTraceContext(ctx context.Context, tc TraceContext) context.Context {
	if !tc.HasTraceID() && !tc.HasSpanID() {
		return ctx
	}
	if st.opts.TraceInjector != nil {
		return st.opts.TraceInjector(ctx, tc)
	}
	if bp := defaultTraceBridge.Load(); bp != nil && bp.inject != nil {
		return bp.inject(ctx, tc)
	}
	return ctx
}

// New creates a new Recorder with the given Options.
func New(opts Options) *Recorder {
	if opts.Next == nil {
		opts.Next = slog.NewJSONHandler(io.Discard, nil)
	}

	passLeveler := opts.PassLeveler
	if passLeveler == nil {
		passLeveler = slog.LevelWarn
	}
	bufferLeveler := opts.BufferLeveler
	if bufferLeveler == nil {
		bufferLeveler = slog.LevelDebug
	}

	if opts.MaxPerScope <= 0 {
		opts.MaxPerScope = DefaultMaxPerScope
	}
	if opts.MaxTotal <= 0 {
		opts.MaxTotal = DefaultMaxTotal
	}
	if opts.MaxPerScope > opts.MaxTotal {
		opts.MaxPerScope = opts.MaxTotal
	}
	if opts.Retain > 0 && opts.MaxRetained <= 0 {
		opts.MaxRetained = DefaultMaxRetained
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	st := &recorderState{
		opts:          opts,
		baseNext:      opts.Next,
		passLeveler:   passLeveler,
		bufferLeveler: bufferLeveler,
		now:           nowFn,
	}
	return &Recorder{
		state: st,
		next:  opts.Next,
	}
}

// Enabled implements slog.Handler.
//
// Inside a scope, records at or above BufferLeveler or PassLeveler are enabled.
// Outside a scope, Enabled delegates to Options.Next.Enabled.
func (r *Recorder) Enabled(ctx context.Context, level slog.Level) bool {
	st := r.state
	sc := scopeFromContext(ctx)
	if sc != nil && sc.state == st {
		return level >= st.bufferLevel() || level >= st.passLevel()
	}
	return r.next.Enabled(ctx, level)
}

// Handle implements slog.Handler.
func (r *Recorder) Handle(ctx context.Context, rec slog.Record) error {
	st := r.state
	sc := scopeFromContext(ctx)
	if sc == nil || sc.state != st {
		st.recordsPassedThrough.Add(1)
		return r.next.Handle(ctx, rec)
	}

	tc := st.extractTraceContext(ctx)

	if rec.Level >= st.passLevel() {
		if rec.Level >= slog.LevelError {
			sc.Trigger("error_log")
		}
		st.recordsPassedThrough.Add(1)
		outRec := r.buildOutputRecord(rec, sc.TraceID(), tc.SpanIDHex(), "", false, 0)
		return st.baseNext.Handle(ctx, outRec)
	}

	if rec.Level < st.bufferLevel() {
		return nil
	}

	cloned, payloadSize := cloneAndSizeRecord(rec)
	if !sc.bufferRecord(cloned, r, payloadSize, tc) {
		// The scope has already ended; pass the late record through so it is
		// neither silently lost nor uncounted.
		st.recordsPassedThrough.Add(1)
		outRec := r.buildOutputRecord(cloned, sc.TraceID(), tc.SpanIDHex(), "", false, 0)
		return st.baseNext.Handle(ctx, outRec)
	}
	if sc.journal != nil {
		st.journalRecord(sc, r, cloned, tc)
	}
	return nil
}

// WithAttrs implements slog.Handler.
func (r *Recorder) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return r
	}
	cleaned := cleanAttrs(attrs)
	nextHandler := r.next.WithAttrs(attrs)
	if len(cleaned) == 0 {
		return &Recorder{
			state:     r.state,
			next:      nextHandler,
			rootAttrs: r.rootAttrs,
			frames:    r.frames,
		}
	}
	if len(r.frames) == 0 {
		newRoot := make([]slog.Attr, len(r.rootAttrs), len(r.rootAttrs)+len(cleaned))
		copy(newRoot, r.rootAttrs)
		newRoot = append(newRoot, cleaned...)
		return &Recorder{
			state:     r.state,
			next:      nextHandler,
			rootAttrs: newRoot,
			frames:    r.frames,
		}
	}
	newFrames := make([]groupFrame, len(r.frames))
	copy(newFrames, r.frames)
	last := &newFrames[len(newFrames)-1]
	merged := make([]slog.Attr, len(last.attrs), len(last.attrs)+len(cleaned))
	copy(merged, last.attrs)
	merged = append(merged, cleaned...)
	last.attrs = merged
	return &Recorder{
		state:     r.state,
		next:      nextHandler,
		rootAttrs: r.rootAttrs,
		frames:    newFrames,
	}
}

// WithGroup implements slog.Handler.
func (r *Recorder) WithGroup(name string) slog.Handler {
	if name == "" {
		return r
	}
	newFrames := make([]groupFrame, len(r.frames)+1)
	copy(newFrames, r.frames)
	newFrames[len(r.frames)] = groupFrame{name: name}
	return &Recorder{
		state:     r.state,
		next:      r.next.WithGroup(name),
		rootAttrs: r.rootAttrs,
		frames:    newFrames,
	}
}

// Stats reports cumulative and point-in-time metrics for a Recorder.
type Stats struct {
	ActiveScopes              int64
	BufferedBytes             int64
	ScopesStarted             uint64
	ScopesFlushed             uint64
	ScopesHealthyDropped      uint64
	ScopesEvictedTotalCap     uint64
	RecordsPassedThrough      uint64
	RecordsBuffered           uint64
	RecordsFlushed            uint64
	RecordsHealthyDropped     uint64
	RecordsDroppedPerScopeCap uint64
	RecordsDroppedTotalCap    uint64

	// FlushMarkersSent counts responses that carried the Flightrec-Flush
	// header because their scope was triggered before headers were written.
	FlushMarkersSent uint64
	// FlushMarkersMissed counts triggered scopes whose response headers were
	// already written, so the Flightrec-Flush header could not be added.
	FlushMarkersMissed uint64
	// DownstreamTriggers counts open scopes that a FlushHeader on a response
	// read through NewTransport triggered. Responses for scopes that were
	// already triggered or had ended are not counted.
	DownstreamTriggers uint64
	// MarkersRejected counts FlushHeader values from responses that were not
	// a built-in reason and were ignored.
	MarkersRejected uint64

	// RetainedScopes and RetainedBytes are the healthy scopes currently held
	// for Options.Retain and the bytes they use.
	RetainedScopes int64
	RetainedBytes  int64
	// RetainedOldestAge is the age of the oldest retained scope, which is the
	// effective retention window under the current load.
	RetainedOldestAge time.Duration
	// RetainedFlushed counts retained scopes written by FlushTrace.
	RetainedFlushed uint64
	// RetainedExpired counts retained scopes dropped after Options.Retain.
	RetainedExpired uint64
	// RetainedEvicted counts retained scopes dropped early, to stay within
	// MaxRetained or the per-trace limit, including scopes larger than
	// MaxRetained on their own, which are dropped instead of retained.
	RetainedEvicted uint64

	// RemoteFlushes counts scopes flushed or triggered because of a failure
	// elsewhere in the same trace: in another process, reported through
	// StartNotifier, or in another scope of this process.
	RemoteFlushes uint64
	// NotifierPublished counts trace IDs announced through the notifier.
	NotifierPublished uint64
	// NotifierPublishDropped counts announcements dropped because the queue
	// was full or the publish rate limit was reached.
	NotifierPublishDropped uint64
	// NotifierPublishErrors counts announcements in failed Notifier.Publish
	// calls. Like NotifierPublished, it counts trace IDs, not calls.
	NotifierPublishErrors uint64
	// NotifierDeliveryDropped counts received notices dropped under load:
	// the delivery queue was full or the remote flush rate limit was reached.
	NotifierDeliveryDropped uint64
	// NotifierRejected counts received messages or notices that were
	// malformed (invalid encoding or version, too large, an invalid trace ID
	// or a reason that is not built in) or, with WithSigningKey, unsigned,
	// wrongly signed or older than two minutes.
	NotifierRejected uint64
	// NotifierSubscribeErrors counts Notifier.Subscribe calls that failed
	// and were retried.
	NotifierSubscribeErrors uint64

	// JournalRecordsWritten counts buffered records written to the journal
	// started with StartJournal; JournalBytesWritten counts all journal
	// bytes, start and end entries included.
	JournalRecordsWritten uint64
	JournalBytesWritten   uint64
	// JournalWraps counts times the journal ring reused its oldest block.
	JournalWraps uint64
	// JournalDropped counts entries not written: larger than a journal
	// block, or written after a fault disabled the journal.
	JournalDropped uint64
	// JournalFaults counts memory faults on the journal file (for example
	// after it was truncated or the disk failed); the journal stops after one.
	JournalFaults uint64
	// JournalOldestAge is the time span the journal currently covers: older
	// records have been overwritten.
	JournalOldestAge time.Duration

	// The counters below describe recovery at StartJournal and add up over
	// the life of the Recorder.
	//
	// JournalRecoveredScopes counts interrupted requests recovered from a
	// journal left by a killed process; JournalRecoveredRecords counts their
	// records Next accepted.
	JournalRecoveredScopes  uint64
	JournalRecoveredRecords uint64
	// JournalCorruptEntries counts journal entries or blocks that failed
	// their checks during recovery: torn writes, or damage to the file.
	JournalCorruptEntries uint64
	// JournalDiscardedFiles counts journal files deleted without recovery:
	// a damaged header, a symlink, or a file not private to this user.
	JournalDiscardedFiles uint64
	// JournalAbandonedRecoveries counts journal files deleted after three
	// recovery attempts that did not finish, such as a crash during recovery.
	JournalAbandonedRecoveries uint64
	// JournalRecoveryErrors counts recovered records that Next failed to
	// write, by returning an error or panicking.
	JournalRecoveryErrors uint64
}

// Stats returns a snapshot of the recorder's current and cumulative metrics.
func (r *Recorder) Stats() Stats {
	st := r.state
	// Gauges first: computing them expires old retained scopes, which the
	// counters below must include.
	retainedScopes, retainedBytes, retainedOldest := st.retainedGauges()
	stats := Stats{
		ActiveScopes:              st.activeScopes.Load(),
		BufferedBytes:             st.totalBytes.Load(),
		ScopesStarted:             st.scopesStarted.Load(),
		ScopesFlushed:             st.scopesFlushed.Load(),
		ScopesHealthyDropped:      st.scopesHealthyDropped.Load(),
		ScopesEvictedTotalCap:     st.scopesEvictedTotalCap.Load(),
		RecordsPassedThrough:      st.recordsPassedThrough.Load(),
		RecordsBuffered:           st.recordsBuffered.Load(),
		RecordsFlushed:            st.recordsFlushed.Load(),
		RecordsHealthyDropped:     st.recordsHealthyDropped.Load(),
		RecordsDroppedPerScopeCap: st.recordsDroppedPerScopeCap.Load(),
		RecordsDroppedTotalCap:    st.recordsDroppedTotalCap.Load(),
		FlushMarkersSent:          st.flushMarkersSent.Load(),
		FlushMarkersMissed:        st.flushMarkersMissed.Load(),
		DownstreamTriggers:        st.downstreamTriggers.Load(),
		MarkersRejected:           st.markersRejected.Load(),
		RetainedFlushed:           st.retainedFlushed.Load(),
		RetainedExpired:           st.retainedExpired.Load(),
		RetainedEvicted:           st.retainedEvicted.Load(),
		RemoteFlushes:             st.remoteFlushes.Load(),
		NotifierPublished:         st.notifierPublished.Load(),
		NotifierPublishDropped:    st.notifierPublishDropped.Load(),
		NotifierPublishErrors:     st.notifierPublishErrors.Load(),
		NotifierDeliveryDropped:   st.notifierDeliveryDropped.Load(),
		NotifierRejected:          st.notifierRejected.Load(),
		NotifierSubscribeErrors:   st.notifierSubscribeErrors.Load(),
	}
	stats.RetainedScopes, stats.RetainedBytes, stats.RetainedOldestAge = retainedScopes, retainedBytes, retainedOldest
	st.journalStats(&stats)
	return stats
}

func (r *Recorder) buildOutputRecord(rec slog.Record, traceID, spanID, flushReason string, truncated bool, droppedInScope uint64) slog.Record {
	out := slog.NewRecord(rec.Time, rec.Level, rec.Message, rec.PC)
	if traceID != "" {
		out.AddAttrs(slog.String("flightrec.trace_id", traceID))
	}
	if spanID != "" {
		out.AddAttrs(slog.String("flightrec.span_id", spanID))
	}
	if flushReason != "" {
		out.AddAttrs(slog.String("flightrec.flush_reason", flushReason))
	}
	if truncated {
		out.AddAttrs(
			slog.Bool("flightrec.truncated", true),
			slog.Uint64("flightrec.dropped_records", droppedInScope),
		)
	}

	if len(r.rootAttrs) > 0 {
		out.AddAttrs(r.rootAttrs...)
	}

	if len(r.frames) == 0 {
		if rec.NumAttrs() > 0 {
			rec.Attrs(func(a slog.Attr) bool {
				a.Value = a.Value.Resolve()
				if a.Equal(slog.Attr{}) {
					return true
				}
				if a.Value.Kind() == slog.KindGroup {
					sub := cleanAttrs(a.Value.Group())
					if len(sub) == 0 {
						return true
					}
					if a.Key == "" {
						out.AddAttrs(sub...)
						return true
					}
					a.Value = slog.GroupValue(sub...)
				}
				out.AddAttrs(a)
				return true
			})
		}
		return out
	}

	var recAttrs []slog.Attr
	if n := rec.NumAttrs(); n > 0 {
		raw := make([]slog.Attr, 0, n)
		rec.Attrs(func(a slog.Attr) bool {
			raw = append(raw, a)
			return true
		})
		recAttrs = cleanAttrs(raw)
	}

	inner := recAttrs
	for i := len(r.frames) - 1; i >= 0; i-- {
		f := r.frames[i]
		if len(f.attrs) == 0 && len(inner) == 0 {
			continue
		}
		combined := make([]slog.Attr, 0, len(f.attrs)+len(inner))
		combined = append(combined, f.attrs...)
		combined = append(combined, inner...)
		inner = []slog.Attr{{Key: f.name, Value: slog.GroupValue(combined...)}}
	}
	if len(inner) > 0 {
		out.AddAttrs(inner...)
	}
	return out
}

// cloneAndSizeRecord copies rec for buffering, resolving LogValuer and group
// attributes, and estimates the bytes it holds beyond its slot. One pass over
// the attributes covers the common case of plain values.
//
// Attributes added with Logger.With are not counted: they live on the shared
// child Recorder each entry points to, not in the record.
func cloneAndSizeRecord(rec slog.Record) (slog.Record, int64) {
	needsResolve := false
	sz := int64(len(rec.Message))
	rec.Attrs(func(a slog.Attr) bool {
		k := a.Value.Kind()
		if k == slog.KindLogValuer || k == slog.KindGroup {
			needsResolve = true
			return false
		}
		sz += estimateAttrPayloadSize(a)
		return true
	})
	if !needsResolve {
		if n := rec.NumAttrs(); n > 5 {
			sz += int64(n) * int64(unsafe.Sizeof(slog.Attr{}))
		}
		return rec.Clone(), sz
	}
	out := slog.NewRecord(rec.Time, rec.Level, rec.Message, rec.PC)
	raw := make([]slog.Attr, 0, rec.NumAttrs())
	rec.Attrs(func(a slog.Attr) bool {
		raw = append(raw, a)
		return true
	})
	cleaned := cleanAttrs(raw)
	if len(cleaned) > 0 {
		out.AddAttrs(cleaned...)
	}
	return out, estimateRecordPayloadSize(&out)
}

func cleanAttrs(attrs []slog.Attr) []slog.Attr {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		a.Value = a.Value.Resolve()
		if a.Equal(slog.Attr{}) {
			continue
		}
		if a.Value.Kind() == slog.KindGroup {
			sub := cleanAttrs(a.Value.Group())
			if len(sub) == 0 {
				continue
			}
			if a.Key == "" {
				out = append(out, sub...)
				continue
			}
			a.Value = slog.GroupValue(sub...)
		}
		out = append(out, a)
	}
	return out
}

func estimateRecordPayloadSize(rec *slog.Record) int64 {
	sz := int64(len(rec.Message))
	if n := rec.NumAttrs(); n > 5 {
		sz += int64(n) * int64(unsafe.Sizeof(slog.Attr{}))
	}
	rec.Attrs(func(a slog.Attr) bool {
		sz += estimateAttrPayloadSize(a)
		return true
	})
	return sz
}

func estimateAttrPayloadSize(a slog.Attr) int64 {
	sz := int64(len(a.Key))
	v := a.Value
	switch v.Kind() {
	case slog.KindString:
		sz += int64(len(v.String()))
	case slog.KindGroup:
		for _, ga := range v.Group() {
			sz += int64(unsafe.Sizeof(slog.Attr{})) + estimateAttrPayloadSize(ga)
		}
	case slog.KindAny:
		if s, ok := v.Any().(string); ok {
			sz += int64(len(s))
		} else if b, ok := v.Any().([]byte); ok {
			sz += int64(len(b))
		} else {
			sz += 16
		}
	}
	return sz
}
