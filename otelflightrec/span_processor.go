// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package otelflightrec

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/divine-ai-com/flightrec"
)

const (
	// DefaultMaxTraceAge is the default retention ceiling for an unclosed trace
	// before it is evicted as stale (5 minutes).
	DefaultMaxTraceAge = 5 * time.Minute

	initialSpanRingCap = 16
	spanSlotSize       = int64(unsafe.Sizeof(bufferedSpan{}))
)

var spanRingPool = sync.Pool{
	New: func() any {
		s := make([]bufferedSpan, initialSpanRingCap)
		return &s
	},
}

// HealthySpanPolicy controls what SpanProcessor exports when a trace finishes
// without triggering.
type HealthySpanPolicy int

const (
	// HealthyExportRootOnly discards buffered child spans for healthy traces
	// and exports only the local root span (annotated with
	// "flightrec.dropped_spans"). This is the default policy.
	HealthyExportRootOnly HealthySpanPolicy = iota

	// HealthyExportNone discards all spans (including the root span) for
	// healthy traces.
	HealthyExportNone
)

// SpanProcessorOptions configures a SpanProcessor.
type SpanProcessorOptions struct {
	// Next is the downstream sdktrace.SpanProcessor that receives spans when a
	// trace is flushed (or the root span under HealthyExportRootOnly).
	Next sdktrace.SpanProcessor

	// Recorder optionally links this SpanProcessor with a flightrec.Recorder
	// so log scopes and trace buffers share triggers bidirectionally.
	Recorder *flightrec.Recorder

	// HealthyPolicy controls whether healthy traces export only their root
	// span (HealthyExportRootOnly, the default) or nothing (HealthyExportNone).
	HealthyPolicy HealthySpanPolicy

	// SlowAfter triggers a trace flush when a span's duration meets or exceeds
	// this threshold. Zero disables slow-trace triggering.
	SlowAfter time.Duration

	// MaxBytesPerTrace caps buffered span bytes (including ring-buffer slot
	// capacity) for a single trace. Defaults to flightrec.DefaultMaxPerScope
	// (256 KB) when <= 0.
	MaxBytesPerTrace int64

	// MaxTotalBytes caps buffered span bytes (including ring-buffer slot
	// capacity) across all active traces in the process. Defaults to
	// flightrec.DefaultMaxTotal (64 MB) when <= 0.
	MaxTotalBytes int64

	// MaxTraceAge is the maximum duration an unclosed trace is retained before
	// being evicted as stale. Defaults to DefaultMaxTraceAge (5 minutes) when <= 0.
	MaxTraceAge time.Duration

	// DisableExceptionTrigger disables automatic triggering when a span records
	// an "exception" event. By default, exception events trigger a trace flush.
	//
	// Error status and exception events on client and producer spans never
	// trigger: they describe a call to another service, which decides whether
	// its own request failed.
	DisableExceptionTrigger bool

	// Now returns the current time. Defaults to time.Now.
	Now func() time.Time
}

// SpanStats reports cumulative and point-in-time metrics for a SpanProcessor.
type SpanStats struct {
	ActiveTraces            int64
	BufferedBytes           int64
	TracesStarted           uint64
	TracesFlushed           uint64
	TracesHealthyDropped    uint64
	TracesEvictedTotalCap   uint64
	SpansBuffered           uint64
	SpansExported           uint64
	SpansHealthyDropped     uint64
	SpansDroppedPerTraceCap uint64
	SpansDroppedTotalCap    uint64
}

type bufferedSpan struct {
	span sdktrace.ReadOnlySpan
	size int64
}

type traceBuffer struct {
	traceID    trace.TraceID
	createdAt  time.Time
	lastActive int64 // unix nano, updated under mu

	mu                sync.Mutex
	buf               []bufferedSpan
	head              int
	count             int
	ringBytes         int64
	payloadBytes      int64
	rootSpan          sdktrace.ReadOnlySpan
	rootBytes         int64
	bytes             int64
	openRootSpans     int
	openSpans         int
	rootEnded         bool
	scope             *flightrec.Scope
	scopeEnded        bool
	triggered         bool
	triggerReason     string
	truncated         bool
	evictedByTotalCap bool
	droppedInTrace    uint64
	closed            bool

	// Doubly-linked eviction list pointers guarded by SpanProcessor.listMu.
	prev   *traceBuffer
	next   *traceBuffer
	inList bool
}

// SpanProcessor is an OpenTelemetry sdktrace.SpanProcessor that buffers
// finished child spans per trace and exports them only when the trace triggers.
type SpanProcessor struct {
	opts SpanProcessorOptions
	next sdktrace.SpanProcessor
	now  func() time.Time

	tracesMu sync.Mutex
	traces   map[trace.TraceID]*traceBuffer
	byScope  map[*flightrec.Scope]*traceBuffer

	listMu sync.Mutex
	oldest *traceBuffer
	newest *traceBuffer

	lastStaleSweepNano atomic.Int64

	totalBytes   atomic.Int64
	activeTraces atomic.Int64

	tracesStarted           atomic.Uint64
	tracesFlushed           atomic.Uint64
	tracesHealthyDropped    atomic.Uint64
	tracesEvictedTotalCap   atomic.Uint64
	spansBuffered           atomic.Uint64
	spansExported           atomic.Uint64
	spansHealthyDropped     atomic.Uint64
	spansDroppedPerTraceCap atomic.Uint64
	spansDroppedTotalCap    atomic.Uint64
}

var (
	_ sdktrace.SpanProcessor  = (*SpanProcessor)(nil)
	_ flightrec.ScopeObserver = (*SpanProcessor)(nil)
)

// NewSpanProcessor creates a new SpanProcessor with the given options.
func NewSpanProcessor(opts SpanProcessorOptions) *SpanProcessor {
	if opts.MaxBytesPerTrace <= 0 {
		opts.MaxBytesPerTrace = flightrec.DefaultMaxPerScope
	}
	if opts.MaxTotalBytes <= 0 {
		opts.MaxTotalBytes = flightrec.DefaultMaxTotal
	}
	if opts.MaxBytesPerTrace > opts.MaxTotalBytes {
		opts.MaxBytesPerTrace = opts.MaxTotalBytes
	}
	if opts.MaxTraceAge <= 0 {
		opts.MaxTraceAge = DefaultMaxTraceAge
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	sp := &SpanProcessor{
		opts:    opts,
		next:    opts.Next,
		now:     nowFn,
		traces:  make(map[trace.TraceID]*traceBuffer),
		byScope: make(map[*flightrec.Scope]*traceBuffer),
	}
	if opts.Recorder != nil {
		opts.Recorder.AttachObserver(sp)
	}
	return sp
}

// AttachRecorder links this SpanProcessor with rec so triggers on either
// side propagate bidirectionally.
func (sp *SpanProcessor) AttachRecorder(rec *flightrec.Recorder) {
	if sp == nil || rec == nil {
		return
	}
	rec.AttachObserver(sp)
}

// Stats returns a snapshot of the SpanProcessor's current and cumulative metrics.
func (sp *SpanProcessor) Stats() SpanStats {
	return SpanStats{
		ActiveTraces:            sp.activeTraces.Load(),
		BufferedBytes:           sp.totalBytes.Load(),
		TracesStarted:           sp.tracesStarted.Load(),
		TracesFlushed:           sp.tracesFlushed.Load(),
		TracesHealthyDropped:    sp.tracesHealthyDropped.Load(),
		TracesEvictedTotalCap:   sp.tracesEvictedTotalCap.Load(),
		SpansBuffered:           sp.spansBuffered.Load(),
		SpansExported:           sp.spansExported.Load(),
		SpansHealthyDropped:     sp.spansHealthyDropped.Load(),
		SpansDroppedPerTraceCap: sp.spansDroppedPerTraceCap.Load(),
		SpansDroppedTotalCap:    sp.spansDroppedTotalCap.Load(),
	}
}

// TriggerTrace marks the trace with the given TraceID to be flushed when it
// completes.
func (sp *SpanProcessor) TriggerTrace(traceID trace.TraceID, reason string) {
	if !traceID.IsValid() {
		return
	}
	if reason == "" {
		reason = "explicit_flush"
	}
	sp.tracesMu.Lock()
	tb := sp.traces[traceID]
	sp.tracesMu.Unlock()
	if tb == nil {
		return
	}

	var scopeToTrigger *flightrec.Scope
	tb.mu.Lock()
	if !tb.closed && !tb.triggered {
		tb.triggered = true
		tb.triggerReason = reason
		scopeToTrigger = tb.scope
	}
	tb.mu.Unlock()

	if scopeToTrigger != nil {
		scopeToTrigger.Trigger(reason)
	}
}

// TriggerContext marks the active OpenTelemetry trace in ctx to be flushed when
// it completes.
func (sp *SpanProcessor) TriggerContext(ctx context.Context, reason string) {
	if sp == nil || ctx == nil {
		return
	}
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		sp.TriggerTrace(sc.TraceID(), reason)
		return
	}
	if fScope := flightrec.ScopeFromContext(ctx); fScope != nil {
		if tid, ok := flightrec.ParseTraceIDHex(fScope.TraceID()); ok {
			sp.TriggerTrace(trace.TraceID(tid), reason)
		}
	}
}

// OnStart implements sdktrace.SpanProcessor.
func (sp *SpanProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	sc := s.SpanContext()
	if !sc.IsValid() || !sc.IsSampled() {
		// Spans that are recorded but not sampled are never exported, so they
		// are not buffered or tracked. They pass through to Next unchanged.
		if sp.next != nil {
			sp.next.OnStart(parent, s)
		}
		return
	}

	now := sp.now()
	tid := sc.TraceID()
	isLocalRoot := !s.Parent().IsValid() || s.Parent().IsRemote()
	fScope := flightrec.ScopeFromContext(parent)

	if fScope != nil && isLocalRoot {
		_, _ = fScope.AdoptGeneratedTraceID(tid.String())
	}

	var scopeTriggered bool
	var scopeReason string
	if fScope != nil {
		scopeTriggered, scopeReason = fScope.TriggerState()
	}

	for {
		var created bool
		sp.tracesMu.Lock()
		tb := sp.traces[tid]
		if tb == nil {
			tb = &traceBuffer{
				traceID:    tid,
				createdAt:  now,
				lastActive: now.UnixNano(),
			}
			sp.traces[tid] = tb
			created = true
		}
		if fScope != nil {
			sp.byScope[fScope] = tb
		}
		sp.tracesMu.Unlock()

		if created {
			sp.activeTraces.Add(1)
			sp.tracesStarted.Add(1)
		}

		tb.mu.Lock()
		if tb.closed {
			tb.mu.Unlock()
			sp.tracesMu.Lock()
			if sp.traces[tid] == tb {
				delete(sp.traces, tid)
			}
			sp.tracesMu.Unlock()
			continue
		}
		tb.lastActive = now.UnixNano()
		tb.openSpans++
		if isLocalRoot {
			tb.openRootSpans++
		}
		if fScope != nil && tb.scope == nil {
			tb.scope = fScope
		}
		if scopeTriggered && !tb.triggered {
			tb.triggered = true
			tb.triggerReason = scopeReason
		}
		tb.mu.Unlock()
		break
	}

	sp.maybeSweepStaleTraces(now)

	if sp.next != nil {
		sp.next.OnStart(parent, s)
	}
}

// OnEnd implements sdktrace.SpanProcessor.
func (sp *SpanProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	sc := s.SpanContext()
	if !sc.IsValid() || !sc.IsSampled() {
		if sp.next != nil {
			sp.next.OnEnd(s)
		}
		return
	}

	tid := sc.TraceID()
	isLocalRoot := !s.Parent().IsValid() || s.Parent().IsRemote()
	spanReason := sp.checkSpanTrigger(s)

	sp.tracesMu.Lock()
	tb := sp.traces[tid]
	sp.tracesMu.Unlock()

	if tb == nil {
		// Late span arriving after the trace already finalized.
		if spanReason != "" {
			if sp.next != nil {
				sp.next.OnEnd(enrichSpan(s, attribute.String("flightrec.flush_reason", spanReason)))
			}
			sp.spansExported.Add(1)
		} else {
			sp.spansHealthyDropped.Add(1)
		}
		return
	}

	payloadSize := estimateSpanPayloadSize(s)
	maxPerTrace := sp.opts.MaxBytesPerTrace

	tb.mu.Lock()
	if tb.closed {
		wasTriggered := tb.triggered
		reason := tb.triggerReason
		tb.mu.Unlock()
		if wasTriggered || spanReason != "" {
			if reason == "" {
				reason = spanReason
			}
			if sp.next != nil {
				sp.next.OnEnd(enrichSpan(s, attribute.String("flightrec.flush_reason", reason)))
			}
			sp.spansExported.Add(1)
		} else {
			sp.spansHealthyDropped.Add(1)
		}
		return
	}

	tb.lastActive = sp.now().UnixNano()
	if tb.openSpans > 0 {
		tb.openSpans--
	}
	if isLocalRoot && tb.openRootSpans > 0 {
		tb.openRootSpans--
		tb.rootEnded = true
	}

	var scopeToTrigger *flightrec.Scope
	if spanReason != "" && !tb.triggered {
		tb.triggered = true
		tb.triggerReason = spanReason
		scopeToTrigger = tb.scope
	}

	prevBytes := tb.bytes
	var droppedByCap uint64

	if isLocalRoot && tb.rootSpan == nil {
		rootCost := spanSlotSize + payloadSize
		if rootCost > maxPerTrace {
			tb.truncated = true
			tb.droppedInTrace++
			droppedByCap++
		} else {
			for tb.count > 0 && tb.ringBytes+tb.payloadBytes+rootCost > maxPerTrace {
				_ = tb.popOldestChildLocked()
				droppedByCap++
			}
			tb.rootSpan = s
			tb.rootBytes = rootCost
			tb.bytes = tb.ringBytes + tb.payloadBytes + tb.rootBytes
		}
	} else {
		droppedByCap = tb.bufferChildLocked(s, payloadSize, maxPerTrace)
	}

	if droppedByCap > 0 {
		tb.truncated = true
		tb.droppedInTrace += droppedByCap
		sp.spansDroppedPerTraceCap.Add(droppedByCap)
	}

	delta := tb.bytes - prevBytes
	if tb.bytes > 0 {
		// Hold tb.mu while linking into the eviction list.
		// Lock order is strictly tb.mu -> sp.listMu.
		sp.ensureLinked(tb)
	}

	shouldFinalize := tb.shouldFinalizeLocked()
	tb.mu.Unlock()

	sp.spansBuffered.Add(1)
	if delta != 0 {
		newTotal := sp.totalBytes.Add(delta)
		if newTotal > sp.opts.MaxTotalBytes {
			sp.enforceTotalCap(tb)
		}
	}

	if scopeToTrigger != nil {
		scopeToTrigger.Trigger(spanReason)
	}

	if shouldFinalize {
		sp.finalizeTrace(tb)
	}
}

// OnScopeStart implements flightrec.ScopeObserver.
func (sp *SpanProcessor) OnScopeStart(ctx context.Context, sc *flightrec.Scope, traceID string) {
	tidBytes, ok := flightrec.ParseTraceIDHex(traceID)
	if !ok {
		return
	}
	tid := trace.TraceID(tidBytes)

	sp.tracesMu.Lock()
	tb := sp.traces[tid]
	if tb != nil {
		sp.byScope[sc] = tb
	}
	sp.tracesMu.Unlock()

	if tb == nil {
		return
	}

	var triggerScopeReason string
	tb.mu.Lock()
	if !tb.closed {
		if tb.scope == nil {
			tb.scope = sc
		}
		if tb.triggered {
			triggerScopeReason = tb.triggerReason
		}
	}
	tb.mu.Unlock()

	if triggerScopeReason != "" {
		sc.Trigger(triggerScopeReason)
	}
}

// OnScopeTrigger implements flightrec.ScopeObserver.
func (sp *SpanProcessor) OnScopeTrigger(sc *flightrec.Scope, traceID, reason string) {
	tb := sp.lookupByScopeOrTraceID(sc, traceID, false)
	if tb == nil {
		return
	}
	if reason == "" {
		reason = "triggered"
	}
	tb.mu.Lock()
	if !tb.closed && !tb.triggered {
		tb.triggered = true
		tb.triggerReason = reason
	}
	tb.mu.Unlock()
}

// OnScopeEnd implements flightrec.ScopeObserver.
func (sp *SpanProcessor) OnScopeEnd(sc *flightrec.Scope, traceID string, triggered bool, reason string) {
	tb := sp.lookupByScopeOrTraceID(sc, traceID, true)
	if tb == nil {
		return
	}

	tb.mu.Lock()
	if tb.closed {
		tb.mu.Unlock()
		return
	}
	tb.scopeEnded = true
	if triggered && !tb.triggered {
		tb.triggered = true
		if reason == "" {
			reason = "triggered"
		}
		tb.triggerReason = reason
	}
	shouldFinalize := tb.shouldFinalizeLocked()
	tb.mu.Unlock()

	if shouldFinalize {
		sp.finalizeTrace(tb)
	}
}

func (sp *SpanProcessor) lookupByScopeOrTraceID(sc *flightrec.Scope, traceID string, removeScope bool) *traceBuffer {
	var tid trace.TraceID
	var hasTID bool
	if b, ok := flightrec.ParseTraceIDHex(traceID); ok {
		tid = trace.TraceID(b)
		hasTID = true
	}

	sp.tracesMu.Lock()
	var tb *traceBuffer
	if sc != nil {
		tb = sp.byScope[sc]
		if removeScope {
			delete(sp.byScope, sc)
		}
	}
	if tb == nil && hasTID {
		tb = sp.traces[tid]
	}
	sp.tracesMu.Unlock()
	return tb
}

func (tb *traceBuffer) shouldFinalizeLocked() bool {
	if tb.closed {
		return false
	}
	if tb.scope != nil && !tb.scopeEnded {
		return false
	}
	if tb.rootEnded && tb.openRootSpans == 0 {
		return true
	}
	if tb.scopeEnded && tb.openSpans == 0 && (tb.count > 0 || tb.rootSpan != nil || tb.droppedInTrace > 0) {
		return true
	}
	return false
}

func (tb *traceBuffer) bufferChildLocked(s sdktrace.ReadOnlySpan, payloadSize, maxPerTrace int64) uint64 {
	if spanSlotSize+payloadSize+tb.rootBytes > maxPerTrace {
		return 1
	}

	var evicted uint64
	if len(tb.buf) == 0 {
		tb.initRingLocked(payloadSize, maxPerTrace)
	} else if tb.count == len(tb.buf) {
		if !tb.tryGrowRingLocked(payloadSize, maxPerTrace) {
			_ = tb.popOldestChildLocked()
			evicted++
		}
	}

	for tb.count > 0 && tb.ringBytes+tb.payloadBytes+tb.rootBytes+payloadSize > maxPerTrace {
		_ = tb.popOldestChildLocked()
		evicted++
	}

	idx := (tb.head + tb.count) % len(tb.buf)
	tb.buf[idx] = bufferedSpan{
		span: s,
		size: payloadSize,
	}
	tb.count++
	tb.payloadBytes += payloadSize
	tb.bytes = tb.ringBytes + tb.payloadBytes + tb.rootBytes
	return evicted
}

func (tb *traceBuffer) initRingLocked(payloadSize, maxPerTrace int64) {
	avail := maxPerTrace - tb.rootBytes
	if int64(initialSpanRingCap)*spanSlotSize+payloadSize <= avail {
		bp := spanRingPool.Get().(*[]bufferedSpan)
		tb.buf = *bp
		tb.ringBytes = int64(len(tb.buf)) * spanSlotSize
		tb.head = 0
		tb.count = 0
		return
	}
	maxSlots := int((avail - payloadSize) / spanSlotSize)
	if maxSlots < 1 {
		maxSlots = 1
	}
	if maxSlots > initialSpanRingCap {
		maxSlots = initialSpanRingCap
	}
	tb.buf = make([]bufferedSpan, maxSlots)
	tb.ringBytes = int64(maxSlots) * spanSlotSize
	tb.head = 0
	tb.count = 0
}

func (tb *traceBuffer) tryGrowRingLocked(payloadSize, maxPerTrace int64) bool {
	avail := maxPerTrace - tb.rootBytes
	curCap := len(tb.buf)
	nextCap := curCap * 2
	if curCap < initialSpanRingCap {
		nextCap = initialSpanRingCap
		if int64(nextCap)*spanSlotSize+tb.payloadBytes+payloadSize > avail {
			nextCap = curCap + 1
		}
	}
	if int64(nextCap)*spanSlotSize+tb.payloadBytes+payloadSize > avail {
		return false
	}

	var nb []bufferedSpan
	if nextCap == initialSpanRingCap {
		bp := spanRingPool.Get().(*[]bufferedSpan)
		nb = *bp
	} else {
		nb = make([]bufferedSpan, nextCap)
	}
	for i := 0; i < tb.count; i++ {
		idx := (tb.head + i) % curCap
		nb[i] = tb.buf[idx]
		tb.buf[idx] = bufferedSpan{}
	}
	releaseSpanRing(tb.buf)
	tb.buf = nb
	tb.head = 0
	tb.ringBytes = int64(len(nb)) * spanSlotSize
	return true
}

func releaseSpanRing(b []bufferedSpan) {
	if len(b) == initialSpanRingCap {
		spanRingPool.Put(&b)
	}
}

func (tb *traceBuffer) popOldestChildLocked() bufferedSpan {
	e := tb.buf[tb.head]
	tb.buf[tb.head] = bufferedSpan{}
	tb.head = (tb.head + 1) % len(tb.buf)
	tb.count--
	if tb.count == 0 {
		tb.head = 0
	}
	tb.payloadBytes -= e.size
	tb.bytes = tb.ringBytes + tb.payloadBytes + tb.rootBytes
	return e
}

func (tb *traceBuffer) clearAndReleaseLocked(clearRoot bool) (droppedChildren int, droppedRoot bool, freedBytes int64) {
	droppedChildren = tb.count
	for i := 0; i < tb.count; i++ {
		idx := (tb.head + i) % len(tb.buf)
		tb.buf[idx] = bufferedSpan{}
	}
	releaseSpanRing(tb.buf)
	tb.buf = nil
	tb.head = 0
	tb.count = 0
	freed := tb.ringBytes + tb.payloadBytes
	tb.ringBytes = 0
	tb.payloadBytes = 0
	if clearRoot && tb.rootSpan != nil {
		droppedRoot = true
		freed += tb.rootBytes
		tb.rootSpan = nil
		tb.rootBytes = 0
	}
	tb.bytes = tb.rootBytes
	return droppedChildren, droppedRoot, freed
}

func (tb *traceBuffer) drainAndReleaseLocked() (children []sdktrace.ReadOnlySpan, root sdktrace.ReadOnlySpan, freedBytes int64) {
	freedBytes = tb.bytes
	if tb.count > 0 {
		children = make([]sdktrace.ReadOnlySpan, tb.count)
		for i := 0; i < tb.count; i++ {
			idx := (tb.head + i) % len(tb.buf)
			children[i] = tb.buf[idx].span
			tb.buf[idx] = bufferedSpan{}
		}
	}
	releaseSpanRing(tb.buf)
	tb.buf = nil
	tb.head = 0
	tb.count = 0
	tb.ringBytes = 0
	tb.payloadBytes = 0
	root = tb.rootSpan
	tb.rootSpan = nil
	tb.rootBytes = 0
	tb.bytes = 0
	return children, root, freedBytes
}

func (sp *SpanProcessor) finalizeTrace(tb *traceBuffer) {
	tb.mu.Lock()
	if tb.closed {
		tb.mu.Unlock()
		return
	}
	tb.closed = true
	sp.unlinkTrace(tb)

	triggered := tb.triggered
	reason := tb.triggerReason
	truncated := tb.truncated
	droppedInTrace := tb.droppedInTrace
	fScope := tb.scope

	var children []sdktrace.ReadOnlySpan
	var root sdktrace.ReadOnlySpan
	var freedBytes int64
	var healthyChildDropped int
	var healthyRootDropped bool

	if triggered {
		children, root, freedBytes = tb.drainAndReleaseLocked()
	} else {
		clearRoot := sp.opts.HealthyPolicy == HealthyExportNone
		healthyChildDropped, healthyRootDropped, freedBytes = tb.clearAndReleaseLocked(clearRoot)
		if !clearRoot && tb.rootSpan != nil {
			root = tb.rootSpan
			freedBytes += tb.rootBytes
			tb.rootSpan = nil
			tb.rootBytes = 0
			tb.bytes = 0
		}
	}
	tb.mu.Unlock()

	sp.tracesMu.Lock()
	if sp.traces[tb.traceID] == tb {
		delete(sp.traces, tb.traceID)
	}
	if fScope != nil && sp.byScope[fScope] == tb {
		delete(sp.byScope, fScope)
	}
	sp.tracesMu.Unlock()

	if freedBytes > 0 {
		sp.totalBytes.Add(-freedBytes)
	}
	sp.activeTraces.Add(-1)

	if triggered {
		sp.tracesFlushed.Add(1)
		if reason == "" {
			reason = "triggered"
		}
		totalExported := uint64(0)
		for i, ch := range children {
			extra := []attribute.KeyValue{attribute.String("flightrec.flush_reason", reason)}
			if truncated && i == 0 && root == nil {
				extra = append(extra,
					attribute.Bool("flightrec.truncated", true),
					attribute.Int64("flightrec.dropped_spans", int64(droppedInTrace)),
				)
			}
			if sp.next != nil {
				sp.next.OnEnd(enrichSpan(ch, extra...))
			}
			totalExported++
		}
		if root != nil {
			extra := []attribute.KeyValue{attribute.String("flightrec.flush_reason", reason)}
			if truncated {
				extra = append(extra,
					attribute.Bool("flightrec.truncated", true),
					attribute.Int64("flightrec.dropped_spans", int64(droppedInTrace)),
				)
			}
			if sp.next != nil {
				sp.next.OnEnd(enrichSpan(root, extra...))
			}
			totalExported++
		}
		sp.spansExported.Add(totalExported)
		return
	}

	sp.tracesHealthyDropped.Add(1)
	if sp.opts.HealthyPolicy == HealthyExportNone {
		dropped := uint64(healthyChildDropped)
		if healthyRootDropped {
			dropped++
		}
		sp.spansHealthyDropped.Add(dropped)
		return
	}

	// HealthyExportRootOnly: discard children, export only root with dropped count.
	sp.spansHealthyDropped.Add(uint64(healthyChildDropped))
	if root != nil {
		totalDropped := uint64(healthyChildDropped) + droppedInTrace
		if sp.next != nil {
			sp.next.OnEnd(enrichSpan(
				root,
				attribute.Int64("flightrec.dropped_spans", int64(totalDropped)),
			))
		}
		sp.spansExported.Add(1)
	}
}

func (sp *SpanProcessor) ensureLinked(tb *traceBuffer) {
	sp.listMu.Lock()
	if !tb.inList && !tb.closed {
		tb.prev = sp.newest
		tb.next = nil
		if sp.newest != nil {
			sp.newest.next = tb
		} else {
			sp.oldest = tb
		}
		sp.newest = tb
		tb.inList = true
	}
	sp.listMu.Unlock()
}

func (sp *SpanProcessor) unlinkTrace(tb *traceBuffer) {
	sp.listMu.Lock()
	if tb.inList {
		if tb.prev != nil {
			tb.prev.next = tb.next
		} else {
			sp.oldest = tb.next
		}
		if tb.next != nil {
			tb.next.prev = tb.prev
		} else {
			sp.newest = tb.prev
		}
		tb.prev = nil
		tb.next = nil
		tb.inList = false
	}
	sp.listMu.Unlock()
}

func (sp *SpanProcessor) enforceTotalCap(current *traceBuffer) {
	for sp.totalBytes.Load() > sp.opts.MaxTotalBytes {
		sp.listMu.Lock()
		victim := sp.oldest
		for victim != nil && victim == current && victim.next != nil {
			victim = victim.next
		}
		if victim == nil {
			sp.listMu.Unlock()
			return
		}
		sp.listMu.Unlock()

		victim.mu.Lock()
		if victim.closed {
			sp.unlinkTrace(victim)
			victim.mu.Unlock()
			continue
		}
		droppedChildren, droppedRoot, freed := victim.clearAndReleaseLocked(true)
		dropped := uint64(droppedChildren)
		if droppedRoot {
			dropped++
		}
		if dropped > 0 || freed > 0 {
			victim.truncated = true
			victim.droppedInTrace += dropped
		}
		alreadyEvicted := victim.evictedByTotalCap
		victim.evictedByTotalCap = true
		sp.unlinkTrace(victim)
		victim.mu.Unlock()

		if freed > 0 {
			sp.totalBytes.Add(-freed)
		}
		if dropped > 0 {
			sp.spansDroppedTotalCap.Add(dropped)
		}
		if !alreadyEvicted {
			sp.tracesEvictedTotalCap.Add(1)
		}
		if victim == current {
			return
		}
	}
}

func (sp *SpanProcessor) maybeSweepStaleTraces(now time.Time) {
	if sp.activeTraces.Load() == 0 {
		return
	}
	nowNano := now.UnixNano()
	last := sp.lastStaleSweepNano.Load()
	if nowNano-last < int64(time.Second) {
		return
	}
	if !sp.lastStaleSweepNano.CompareAndSwap(last, nowNano) {
		return
	}

	cutoffNano := now.Add(-sp.opts.MaxTraceAge).UnixNano()
	var stale []*traceBuffer

	sp.tracesMu.Lock()
	for _, tb := range sp.traces {
		stale = append(stale, tb)
	}
	sp.tracesMu.Unlock()

	for _, tb := range stale {
		tb.mu.Lock()
		isStale := !tb.closed && tb.lastActive <= cutoffNano
		tb.mu.Unlock()
		if isStale {
			sp.evictStaleTrace(tb)
		}
	}
}

func (sp *SpanProcessor) evictStaleTrace(tb *traceBuffer) {
	tb.mu.Lock()
	if tb.closed {
		tb.mu.Unlock()
		return
	}
	tb.closed = true
	sp.unlinkTrace(tb)
	droppedChildren, droppedRoot, freed := tb.clearAndReleaseLocked(true)
	fScope := tb.scope
	tb.mu.Unlock()

	sp.tracesMu.Lock()
	if sp.traces[tb.traceID] == tb {
		delete(sp.traces, tb.traceID)
	}
	if fScope != nil && sp.byScope[fScope] == tb {
		delete(sp.byScope, fScope)
	}
	sp.tracesMu.Unlock()

	if freed > 0 {
		sp.totalBytes.Add(-freed)
	}
	dropped := uint64(droppedChildren)
	if droppedRoot {
		dropped++
	}
	if dropped > 0 {
		sp.spansDroppedTotalCap.Add(dropped)
	}
	sp.tracesEvictedTotalCap.Add(1)
	sp.activeTraces.Add(-1)
}

// ForceFlush implements sdktrace.SpanProcessor.
func (sp *SpanProcessor) ForceFlush(ctx context.Context) error {
	if sp.next != nil {
		return sp.next.ForceFlush(ctx)
	}
	return nil
}

// Shutdown implements sdktrace.SpanProcessor.
func (sp *SpanProcessor) Shutdown(ctx context.Context) error {
	sp.tracesMu.Lock()
	remaining := make([]*traceBuffer, 0, len(sp.traces))
	for _, tb := range sp.traces {
		remaining = append(remaining, tb)
	}
	sp.tracesMu.Unlock()

	for _, tb := range remaining {
		sp.finalizeTrace(tb)
	}
	if sp.next != nil {
		return sp.next.Shutdown(ctx)
	}
	return nil
}

func (sp *SpanProcessor) checkSpanTrigger(s sdktrace.ReadOnlySpan) string {
	// A client or producer span records the outcome of a call to another
	// service; instrumentation marks it as an error for answers such as
	// NotFound. That service decides whether its request failed, and its
	// flush marker or notice reaches this one as downstream: or remote:.
	outbound := s.SpanKind() == trace.SpanKindClient || s.SpanKind() == trace.SpanKindProducer
	if s.Status().Code == codes.Error && !outbound {
		return "span_error"
	}
	if !sp.opts.DisableExceptionTrigger && !outbound {
		for _, ev := range s.Events() {
			if ev.Name == "exception" {
				return "span_exception"
			}
		}
	}
	if sp.opts.SlowAfter > 0 && s.EndTime().Sub(s.StartTime()) >= sp.opts.SlowAfter {
		return "slow_trace"
	}
	return ""
}

type enrichedSpan struct {
	sdktrace.ReadOnlySpan
	attrs []attribute.KeyValue
}

func (e *enrichedSpan) Attributes() []attribute.KeyValue {
	return e.attrs
}

func enrichSpan(s sdktrace.ReadOnlySpan, extra ...attribute.KeyValue) sdktrace.ReadOnlySpan {
	if len(extra) == 0 {
		return s
	}
	base := s.Attributes()
	merged := make([]attribute.KeyValue, 0, len(base)+len(extra))
	merged = append(merged, base...)
	merged = append(merged, extra...)
	return &enrichedSpan{
		ReadOnlySpan: s,
		attrs:        merged,
	}
}

// spanBaseCost is the heap held by one ended span apart from its attributes,
// events and links: the SDK's span struct with its contexts, times and status.
// Measured at about 400 bytes on linux/amd64 with go.opentelemetry.io/otel/sdk
// v1.47.0 (heap growth over 20,000 buffered spans, see
// TestSpanAccountingMatchesHeap) and scaled by pointer size for 32-bit
// platforms, where most of the struct is pointers and words.
const spanBaseCost = 400 * int(unsafe.Sizeof(uintptr(0))) / 8

var (
	attrValueSize = int64(unsafe.Sizeof(attribute.KeyValue{}))
	eventSize     = int64(unsafe.Sizeof(sdktrace.Event{}))
	linkSize      = int64(unsafe.Sizeof(sdktrace.Link{}))
)

// estimateSpanPayloadSize estimates the heap a buffered span keeps alive,
// beyond its slot in the ring.
func estimateSpanPayloadSize(s sdktrace.ReadOnlySpan) int64 {
	sz := int64(spanBaseCost) + int64(len(s.Name())) + int64(len(s.Status().Description))
	for _, a := range s.Attributes() {
		sz += estimateOTelAttrSize(a)
	}
	for _, ev := range s.Events() {
		sz += eventSize + int64(len(ev.Name))
		for _, a := range ev.Attributes {
			sz += estimateOTelAttrSize(a)
		}
	}
	for _, ln := range s.Links() {
		sz += linkSize
		for _, a := range ln.Attributes {
			sz += estimateOTelAttrSize(a)
		}
	}
	return sz
}

// estimateOTelAttrSize counts the attribute.KeyValue itself plus the strings
// and slices it references.
func estimateOTelAttrSize(a attribute.KeyValue) int64 {
	sz := attrValueSize + int64(len(a.Key))
	switch a.Value.Type() {
	case attribute.STRING:
		sz += int64(len(a.Value.AsString()))
	case attribute.STRINGSLICE:
		for _, s := range a.Value.AsStringSlice() {
			sz += int64(len(s)) + 16
		}
	case attribute.BOOLSLICE:
		sz += int64(len(a.Value.AsBoolSlice()))
	case attribute.INT64SLICE:
		sz += 8 * int64(len(a.Value.AsInt64Slice()))
	case attribute.FLOAT64SLICE:
		sz += 8 * int64(len(a.Value.AsFloat64Slice()))
	}
	return sz
}
