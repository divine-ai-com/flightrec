// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
	"log/slog"
	mrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// DefaultMaxPerScope is the default per-request buffer cap (256 KB).
	DefaultMaxPerScope int64 = 256 << 10
	// DefaultMaxTotal is the default process-wide buffer cap (64 MB).
	DefaultMaxTotal int64 = 64 << 20
	// DefaultMaxRetained is the default cap on retained scope bytes (16 MB).
	DefaultMaxRetained int64 = 16 << 20

	initialRingCapacity = 24
	maxPooledRingCap    = 512

	entrySlotSize = int64(unsafe.Sizeof(bufferedEntry{}))
)

var ringBufPool = sync.Pool{
	New: func() any {
		s := make([]bufferedEntry, initialRingCapacity)
		return &s
	},
}

// Options configures a Recorder.
type Options struct {
	// Next is the underlying slog.Handler where passed-through, flushed,
	// and healthy summary records are written. If nil, a default JSONHandler
	// writing to io.Discard is used.
	//
	// Outside a request scope, Next.Enabled and Next.Handle are called
	// directly. Inside a request scope, PassLeveler and BufferLeveler govern
	// buffering, and flushed records are sent directly to Next.Handle
	// regardless of Next's configured level.
	Next slog.Handler

	// PassLeveler is the minimum level written immediately to Next without
	// buffering inside a scope. Takes precedence over PassLevel when non-nil.
	// Defaults to slog.LevelWarn when both PassLeveler and PassLevel are unset.
	PassLeveler slog.Leveler

	// BufferLeveler is the minimum level stored in a request scope's buffer
	// and emitted only if the scope triggers. Takes precedence over
	// BufferLevel when non-nil. Defaults to slog.LevelDebug when both
	// BufferLeveler and BufferLevel are unset.
	BufferLeveler slog.Leveler

	// PassLevel is the minimum level written immediately to Next without
	// buffering inside a scope. Defaults to slog.LevelWarn when unset (zero).
	//
	// Deprecated: Use PassLeveler instead, which distinguishes slog.LevelInfo
	// (zero) from an unset value.
	PassLevel slog.Level

	// BufferLevel is the minimum level stored in a request scope's buffer
	// and emitted only if the scope triggers. Defaults to slog.LevelDebug
	// when unset (zero).
	//
	// Deprecated: Use BufferLeveler instead, which distinguishes slog.LevelInfo
	// (zero) from an unset value.
	BufferLevel slog.Level

	// SlowAfter triggers a scope flush when a request's total duration
	// meets or exceeds this threshold. Zero disables slow-request triggering.
	SlowAfter time.Duration

	// MaxPerScope caps buffered bytes (including ring-buffer slot capacity)
	// for a single request scope. Defaults to DefaultMaxPerScope (256 KB)
	// when <= 0.
	MaxPerScope int64

	// MaxTotal caps buffered bytes (including ring-buffer slot capacity)
	// across all active scopes in the process. Defaults to DefaultMaxTotal
	// (64 MB) when <= 0.
	MaxTotal int64

	// IgnoreRequestTraceIDs stops HTTPMiddleware and the gRPC helpers
	// (including any GRPCMetadataExtractor passed to them) from taking trace
	// IDs from traceparent, X-Trace-Id and X-Request-Id on incoming requests.
	// Set it on services that receive requests from clients outside your
	// system: a client could otherwise choose the trace ID that groups its
	// requests. The trace ID then comes from an active OpenTelemetry span
	// when one is present, or is generated.
	IgnoreRequestTraceIDs bool

	// DisableFlushMarker stops HTTPMiddleware from setting the FlushHeader
	// response header. Set it on services whose responses go to clients
	// outside your system, such as a public API or an edge gateway.
	DisableFlushMarker bool

	// Retain keeps the buffer of a healthy scope for this long after the scope
	// ends, so a failure reported later for the same trace ID can still write
	// it with Recorder.FlushTrace (or, with a notifier, from another service).
	// Zero disables retention. See also MaxRetained.
	Retain time.Duration

	// MaxRetained caps the bytes held by retained scopes, separately from
	// MaxTotal. When it is reached the oldest retained scopes are dropped.
	// Defaults to DefaultMaxRetained (16 MB) when Retain is set and this is
	// <= 0.
	MaxRetained int64

	// DisableHealthySummary disables writing the single summary log line when
	// a healthy (untriggered) scope completes.
	DisableHealthySummary bool

	// TraceExtractor optionally extracts a W3C TraceID and SpanID from ctx at
	// log and scope-start time. When nil, the bridge registered via
	// RegisterDefaultTraceBridge (automatically set by importing otelflightrec)
	// is used.
	TraceExtractor TraceExtractor

	// TraceInjector optionally injects the log-time TraceContext back into ctx
	// when flushing buffered records to Next.Handle. When nil, the bridge
	// registered via RegisterDefaultTraceBridge is used.
	TraceInjector TraceInjector

	// Now returns the current time. Defaults to time.Now.
	Now func() time.Time
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
	// malformed: invalid encoding or version, too large, an invalid trace ID
	// or a reason that is not built in.
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

func (st *recorderState) passLevel() slog.Level {
	return st.passLeveler.Level()
}

func (st *recorderState) bufferLevel() slog.Level {
	return st.bufferLeveler.Level()
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

type scopeContextKey struct{}

// ScopeMeta holds request metadata associated with a Scope.
type ScopeMeta struct {
	TraceID string
	Method  string
	Path    string
}

type bufferedEntry struct {
	rec      slog.Record
	recorder *Recorder
	size     int64
	traceCtx TraceContext
}

// Scope represents a single request or unit-of-work buffer.
type Scope struct {
	state     *recorderState
	meta      ScopeMeta
	startTime time.Time

	mu                sync.Mutex
	buf               []bufferedEntry
	head              int
	count             int
	ringBytes         int64
	payloadBytes      int64
	bytes             int64
	triggered         bool
	triggerReason     string
	truncated         bool
	evictedByTotalCap bool
	droppedInScope    uint64
	generatedTraceID  bool

	// jid and journal are set at StartScope when a journal is running and
	// never change afterwards.
	jid     uint64
	journal *journalState
	closed  bool

	// Doubly-linked list pointers guarded by state.listMu.
	prev   *Scope
	next   *Scope
	inList bool
}

// StartScope opens a new request Scope associated with rec and returns a
// derived context along with the Scope. Callers must call Scope.End when the
// unit of work finishes.
//
// The underlying ring buffer is allocated lazily on the first buffered record.
func StartScope(ctx context.Context, rec *Recorder, meta ScopeMeta) (context.Context, *Scope) {
	st := rec.state
	if meta.TraceID == "" {
		if sc := scopeFromContext(ctx); sc != nil {
			meta.TraceID = sc.TraceID()
		} else if tc := st.extractTraceContext(ctx); tc.HasTraceID() {
			meta.TraceID = tc.TraceIDHex()
		}
	}
	generated := false
	if meta.TraceID == "" {
		meta.TraceID = generateTraceID()
		generated = true
	}
	sc := &Scope{
		state:            st,
		meta:             meta,
		startTime:        st.now(),
		generatedTraceID: generated,
	}
	st.activeScopes.Add(1)
	st.scopesStarted.Add(1)
	st.journalStart(sc)
	scopeCtx := context.WithValue(ctx, scopeContextKey{}, sc)
	st.notifyScopeStart(scopeCtx, sc, meta.TraceID)
	if h := st.hub.Load(); h != nil {
		h.scopeStarted(sc, meta.TraceID)
	}
	return scopeCtx, sc
}

// Flush marks the active Scope in ctx as triggered with the provided reason so
// all buffered records for the request will be written when the scope ends.
func Flush(ctx context.Context, reason string) {
	sc := scopeFromContext(ctx)
	if sc == nil {
		return
	}
	if reason == "" {
		reason = "explicit_flush"
	}
	sc.Trigger(reason)
}

// TraceID returns the trace ID of the active Scope or OpenTelemetry span in
// ctx, or "" if none is present.
func TraceID(ctx context.Context) string {
	sc := scopeFromContext(ctx)
	if sc != nil {
		return sc.TraceID()
	}
	if tc := extractDefaultTraceContext(ctx); tc.HasTraceID() {
		return tc.TraceIDHex()
	}
	return ""
}

func scopeFromContext(ctx context.Context) *Scope {
	if ctx == nil {
		return nil
	}
	sc, _ := ctx.Value(scopeContextKey{}).(*Scope)
	return sc
}

// Trigger marks the scope to be flushed on End. The first non-empty reason is kept.
func (s *Scope) Trigger(reason string) {
	s.trigger(reason)
}

// trigger marks the scope as triggered and reports whether this call changed
// an open, untriggered scope.
func (s *Scope) trigger(reason string) bool {
	if reason == "" {
		reason = "triggered"
	}
	s.mu.Lock()
	var notify, generated bool
	var tid string
	if !s.triggered {
		s.triggered = true
		s.triggerReason = reason
		tid = s.meta.TraceID
		generated = s.generatedTraceID
		notify = !s.closed
	}
	s.mu.Unlock()

	if notify && s.state != nil {
		s.state.notifyScopeTrigger(s, tid, reason)
		if h := s.state.hub.Load(); h != nil {
			h.localTrigger(tid, reason, generated)
		}
	}
	return notify
}

// triggerIfSlow triggers the scope with reason slow_request if it has run for
// at least Options.SlowAfter. It lets the HTTP middleware decide before the
// response headers are sent, instead of only at End.
func (s *Scope) triggerIfSlow() {
	st := s.state
	if st == nil || st.opts.SlowAfter <= 0 {
		return
	}
	if st.now().Sub(s.startTime) >= st.opts.SlowAfter {
		s.Trigger("slow_request")
	}
}

func (s *Scope) bufferRecord(rec slog.Record, recorder *Recorder, payloadSize int64, tc TraceContext) bool {
	st := s.state
	maxPerScope := st.opts.MaxPerScope

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}

	if entrySlotSize+payloadSize > maxPerScope {
		s.truncated = true
		s.droppedInScope++
		s.mu.Unlock()
		st.recordsDroppedPerScopeCap.Add(1)
		return true
	}

	prevBytes := s.bytes
	var evictedCount uint64

	if len(s.buf) == 0 {
		s.initRingLocked(payloadSize, maxPerScope)
	} else if s.count == len(s.buf) {
		if !s.tryGrowRingLocked(payloadSize, maxPerScope) {
			_ = s.popOldestLocked()
			evictedCount++
		}
	}

	for s.count > 0 && s.ringBytes+s.payloadBytes+payloadSize > maxPerScope {
		_ = s.popOldestLocked()
		evictedCount++
	}

	if evictedCount > 0 {
		s.truncated = true
		s.droppedInScope += evictedCount
		st.recordsDroppedPerScopeCap.Add(evictedCount)
	}

	idx := (s.head + s.count) % len(s.buf)
	s.buf[idx] = bufferedEntry{
		rec:      rec,
		recorder: recorder,
		size:     payloadSize,
		traceCtx: tc,
	}
	s.count++
	s.payloadBytes += payloadSize
	s.bytes = s.ringBytes + s.payloadBytes
	delta := s.bytes - prevBytes
	if s.bytes > 0 {
		// Hold s.mu while linking so a scope that is closing concurrently
		// can never be re-linked after End. Lock order is always s.mu -> listMu.
		st.ensureLinked(s)
	}
	s.mu.Unlock()

	st.recordsBuffered.Add(1)
	newTotal := st.totalBytes.Add(delta)
	if newTotal > st.opts.MaxTotal {
		st.enforceTotalCap(s)
	}
	return true
}

func (s *Scope) initRingLocked(payloadSize, maxPerScope int64) {
	if int64(initialRingCapacity)*entrySlotSize+payloadSize <= maxPerScope {
		bp := ringBufPool.Get().(*[]bufferedEntry)
		s.buf = *bp
		s.ringBytes = int64(len(s.buf)) * entrySlotSize
		s.head = 0
		s.count = 0
		return
	}
	maxSlots := int((maxPerScope - payloadSize) / entrySlotSize)
	if maxSlots < 1 {
		maxSlots = 1
	}
	if maxSlots > initialRingCapacity {
		maxSlots = initialRingCapacity
	}
	s.buf = make([]bufferedEntry, maxSlots)
	s.ringBytes = int64(maxSlots) * entrySlotSize
	s.head = 0
	s.count = 0
}

func (s *Scope) tryGrowRingLocked(payloadSize, maxPerScope int64) bool {
	curCap := len(s.buf)
	nextCap := curCap * 2
	if curCap < initialRingCapacity {
		nextCap = initialRingCapacity
		if int64(nextCap)*entrySlotSize+s.payloadBytes+payloadSize > maxPerScope {
			nextCap = curCap + 1
		}
	}
	if int64(nextCap)*entrySlotSize+s.payloadBytes+payloadSize > maxPerScope {
		return false
	}

	var nb []bufferedEntry
	if nextCap == initialRingCapacity {
		bp := ringBufPool.Get().(*[]bufferedEntry)
		nb = *bp
	} else {
		nb = make([]bufferedEntry, nextCap)
	}

	for i := 0; i < s.count; i++ {
		idx := (s.head + i) % curCap
		nb[i] = s.buf[idx]
		s.buf[idx] = bufferedEntry{}
	}
	releaseRingSlice(s.buf)
	s.buf = nb
	s.head = 0
	s.ringBytes = int64(len(nb)) * entrySlotSize
	return true
}

func releaseRingSlice(b []bufferedEntry) {
	if len(b) == initialRingCapacity {
		ringBufPool.Put(&b)
	}
}

func (s *Scope) popOldestLocked() bufferedEntry {
	e := s.buf[s.head]
	s.buf[s.head] = bufferedEntry{}
	s.head = (s.head + 1) % len(s.buf)
	s.count--
	if s.count == 0 {
		s.head = 0
	}
	s.payloadBytes -= e.size
	s.bytes = s.ringBytes + s.payloadBytes
	return e
}

func (s *Scope) clearAndReleasePoolLocked() (droppedCount int, freedBytes int64) {
	droppedCount = s.count
	freedBytes = s.bytes
	for i := 0; i < s.count; i++ {
		idx := (s.head + i) % len(s.buf)
		s.buf[idx] = bufferedEntry{}
	}
	releaseRingSlice(s.buf)
	s.buf = nil
	s.head = 0
	s.count = 0
	s.ringBytes = 0
	s.payloadBytes = 0
	s.bytes = 0
	return droppedCount, freedBytes
}

func (s *Scope) drainAndReleasePoolLocked() ([]bufferedEntry, int64) {
	freed := s.bytes
	if s.count == 0 {
		releaseRingSlice(s.buf)
		s.buf = nil
		s.head = 0
		s.ringBytes = 0
		s.payloadBytes = 0
		s.bytes = 0
		return nil, freed
	}
	out := make([]bufferedEntry, s.count)
	for i := 0; i < s.count; i++ {
		idx := (s.head + i) % len(s.buf)
		out[i] = s.buf[idx]
		s.buf[idx] = bufferedEntry{}
	}
	releaseRingSlice(s.buf)
	s.buf = nil
	s.head = 0
	s.count = 0
	s.ringBytes = 0
	s.payloadBytes = 0
	s.bytes = 0
	return out, freed
}

// End finalizes the Scope. It evaluates HTTP status and slow-request thresholds,
// flushes all buffered records in chronological order if triggered, or discards
// the buffer and writes a single summary record if healthy.
func (s *Scope) End(ctx context.Context, statusCode int) {
	s.endWithSummaryAttr(ctx, statusCode, slog.Attr{})
}

func (s *Scope) endWithSummaryAttr(ctx context.Context, statusCode int, extraSummaryAttr slog.Attr) {
	st := s.state
	now := st.now()
	duration := now.Sub(s.startTime)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	st.unlinkScope(s)

	if statusCode >= 500 && !s.triggered {
		s.triggered = true
		s.triggerReason = httpStatusReason(statusCode)
	}
	if st.opts.SlowAfter > 0 && duration >= st.opts.SlowAfter && !s.triggered {
		s.triggered = true
		s.triggerReason = "slow_request"
	}
	if !s.triggered {
		// A notice for this trace may have arrived from another process after
		// the scope stopped being reachable for remote triggers.
		if h := st.hub.Load(); h != nil {
			if r, ok := h.announced(s.meta.TraceID); ok {
				s.triggered = true
				s.triggerReason = "remote:" + r
				st.remoteFlushes.Add(1)
			}
		}
	}

	traceID := s.meta.TraceID
	generatedID := s.generatedTraceID
	method := s.meta.Method
	path := s.meta.Path
	triggered := s.triggered
	reason := s.triggerReason
	truncated := s.truncated
	droppedInScope := s.droppedInScope

	var entries []bufferedEntry
	var freedBytes int64
	var healthyDropped int
	retain := !triggered && st.opts.Retain > 0 && s.count > 0
	if triggered || retain {
		entries, freedBytes = s.drainAndReleasePoolLocked()
	} else {
		healthyDropped, freedBytes = s.clearAndReleasePoolLocked()
	}
	s.mu.Unlock()

	if freedBytes > 0 {
		st.totalBytes.Add(-freedBytes)
	}
	st.activeScopes.Add(-1)
	if h := st.hub.Load(); h != nil {
		h.scopeEnded(s, traceID, triggered, reason, generatedID)
	}

	scopeTraceBytes, hasScopeTraceBytes := ParseTraceIDHex(traceID)

	if triggered {
		st.scopesFlushed.Add(1)
		st.writeFlushed(ctx, entries, traceID, reason, truncated, droppedInScope)
		// After the records reached Next: a crash while writing them
		// recovers them again rather than losing them.
		st.journalEnd(s)
		st.notifyScopeEnd(s, traceID, true, reason)
		return
	}
	st.journalEnd(s)

	droppedCount := uint64(healthyDropped) + droppedInScope
	retainedCount := len(entries)
	if retain {
		// Retained records are not written now. They count as healthy drops
		// only if the retained scope expires or is evicted.
		droppedCount = droppedInScope
		st.retain(traceID, entries, truncated, droppedInScope, now)
		if h := st.hub.Load(); h != nil {
			// A notice may have arrived after the check above but before
			// the scope was retained, when nothing could find it.
			h.afterRetain(traceID)
		}
	} else {
		st.scopesHealthyDropped.Add(1)
		st.recordsHealthyDropped.Add(uint64(healthyDropped))
	}

	if !st.opts.DisableHealthySummary {
		summary := slog.NewRecord(now, slog.LevelInfo, "request completed", 0)
		summary.AddAttrs(
			slog.String("flightrec.trace_id", traceID),
			slog.Duration("duration", duration),
			slog.Uint64("flightrec.dropped_records", droppedCount),
		)
		if !extraSummaryAttr.Equal(slog.Attr{}) {
			summary.AddAttrs(extraSummaryAttr)
		} else if statusCode > 0 {
			summary.AddAttrs(slog.Int("status", statusCode))
		}
		if method != "" {
			summary.AddAttrs(slog.String("method", method))
		}
		if path != "" {
			summary.AddAttrs(slog.String("path", path))
		}
		if retain {
			summary.AddAttrs(slog.Int("flightrec.retained_records", retainedCount))
		}
		summaryCtx := ctx
		if tc := st.extractTraceContext(ctx); !tc.HasTraceID() && hasScopeTraceBytes {
			summaryCtx = st.injectTraceContext(ctx, TraceContext{TraceID: scopeTraceBytes})
		}
		_ = st.baseNext.Handle(summaryCtx, summary)
	}
	st.notifyScopeEnd(s, traceID, false, "")
}

func (st *recorderState) ensureLinked(sc *Scope) {
	st.listMu.Lock()
	if !sc.inList && !sc.closed {
		sc.prev = st.newest
		sc.next = nil
		if st.newest != nil {
			st.newest.next = sc
		} else {
			st.oldest = sc
		}
		st.newest = sc
		sc.inList = true
	}
	st.listMu.Unlock()
}

func (st *recorderState) unlinkScope(sc *Scope) {
	st.listMu.Lock()
	if sc.inList {
		if sc.prev != nil {
			sc.prev.next = sc.next
		} else {
			st.oldest = sc.next
		}
		if sc.next != nil {
			sc.next.prev = sc.prev
		} else {
			st.newest = sc.prev
		}
		sc.prev = nil
		sc.next = nil
		sc.inList = false
	}
	st.listMu.Unlock()
}

func (st *recorderState) enforceTotalCap(current *Scope) {
	for st.totalBytes.Load() > st.opts.MaxTotal {
		st.listMu.Lock()
		victim := st.oldest
		for victim != nil && victim == current && victim.next != nil {
			victim = victim.next
		}
		if victim == nil {
			st.listMu.Unlock()
			return
		}
		st.listMu.Unlock()

		victim.mu.Lock()
		if victim.closed {
			st.unlinkScope(victim)
			victim.mu.Unlock()
			continue
		}
		var dropped uint64
		var freed int64
		if victim.count > 0 || victim.bytes > 0 {
			c, b := victim.clearAndReleasePoolLocked()
			dropped = uint64(c)
			freed = b
			victim.truncated = true
			victim.droppedInScope += dropped
		}
		alreadyEvicted := victim.evictedByTotalCap
		victim.evictedByTotalCap = true
		st.unlinkScope(victim)
		victim.mu.Unlock()

		if freed > 0 {
			st.totalBytes.Add(-freed)
		}
		if dropped > 0 {
			st.recordsDroppedTotalCap.Add(dropped)
		}
		if !alreadyEvicted {
			st.scopesEvictedTotalCap.Add(1)
		}
		if victim == current {
			return
		}
	}
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

func httpStatusReason(code int) string {
	switch code {
	case 500:
		return "http_500"
	case 502:
		return "http_502"
	case 503:
		return "http_503"
	case 504:
		return "http_504"
	default:
		return "http_5xx"
	}
}

var rngPool = sync.Pool{
	New: func() any {
		var seed [32]byte
		_, _ = rand.Read(seed[:])
		return mrand.NewChaCha8(seed)
	},
}

func generateTraceID() string {
	ch := rngPool.Get().(*mrand.ChaCha8)
	var b [16]byte
	binary.LittleEndian.PutUint64(b[:8], ch.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], ch.Uint64())
	rngPool.Put(ch)
	return hex.EncodeToString(b[:])
}
