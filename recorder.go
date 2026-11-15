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
		if opts.PassLevel != 0 {
			passLeveler = opts.PassLevel
		} else {
			passLeveler = slog.LevelWarn
		}
	}

	bufferLeveler := opts.BufferLeveler
	if bufferLeveler == nil {
		if opts.BufferLevel != 0 {
			bufferLeveler = opts.BufferLevel
		} else {
			bufferLeveler = slog.LevelDebug
		}
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
