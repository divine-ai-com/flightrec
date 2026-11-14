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
	"math/big"
	mrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultMaxPerScope is the default per-request buffer cap (256 KB).
	DefaultMaxPerScope int64 = 256 << 10
	// DefaultMaxTotal is the default process-wide buffer cap (64 MB).
	DefaultMaxTotal int64 = 64 << 20

	// baseRecordOverhead estimates the in-memory footprint of a slog.Record
	// struct and ring-buffer slot before message and attribute payloads.
	baseRecordOverhead int64 = 128

	initialRingCapacity = 32
	maxPooledRingCap    = 512
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
	Next slog.Handler

	// PassLevel is the minimum level written immediately to Next without
	// buffering. Defaults to slog.LevelWarn when both PassLevel and
	// BufferLevel are zero.
	PassLevel slog.Level

	// BufferLevel is the minimum level stored in a request scope's buffer
	// and emitted only if the scope triggers. Defaults to slog.LevelDebug
	// when zero.
	BufferLevel slog.Level

	// SlowAfter triggers a scope flush when a request's total duration
	// meets or exceeds this threshold. Zero disables slow-request triggering.
	SlowAfter time.Duration

	// MaxPerScope caps buffered bytes for a single request scope.
	// Defaults to DefaultMaxPerScope (256 KB) when <= 0.
	MaxPerScope int64

	// MaxTotal caps buffered bytes across all active scopes in the process.
	// Defaults to DefaultMaxTotal (64 MB) when <= 0.
	MaxTotal int64

	// DisableHealthySummary disables writing the single summary log line when
	// a healthy (untriggered) scope completes.
	DisableHealthySummary bool

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
}

// Recorder is a slog.Handler that buffers records below PassLevel per request
// scope and flushes them to Next only when the scope triggers.
type Recorder struct {
	state  *recorderState
	next   slog.Handler
	groups []string
	attrs  []slog.Attr
}

type recorderState struct {
	opts     Options
	baseNext slog.Handler
	now      func() time.Time

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

	listMu sync.Mutex
	oldest *Scope
	newest *Scope
}

// New creates a new Recorder with the given Options.
func New(opts Options) *Recorder {
	if opts.Next == nil {
		opts.Next = slog.NewJSONHandler(io.Discard, nil)
	}
	if opts.PassLevel == 0 && opts.BufferLevel == 0 {
		opts.PassLevel = slog.LevelWarn
		opts.BufferLevel = slog.LevelDebug
	} else if opts.BufferLevel == 0 && opts.PassLevel > slog.LevelDebug {
		opts.BufferLevel = slog.LevelDebug
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
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	st := &recorderState{
		opts:     opts,
		baseNext: opts.Next,
		now:      nowFn,
	}
	return &Recorder{
		state: st,
		next:  opts.Next,
	}
}

// Stats returns a snapshot of the recorder's current and cumulative metrics.
func (r *Recorder) Stats() Stats {
	st := r.state
	return Stats{
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
	}
}

// Enabled implements slog.Handler.
func (r *Recorder) Enabled(ctx context.Context, level slog.Level) bool {
	if scopeFromContext(ctx) != nil {
		return level >= r.state.opts.BufferLevel || level >= r.state.opts.PassLevel
	}
	return r.next.Enabled(ctx, level)
}

// Handle implements slog.Handler.
func (r *Recorder) Handle(ctx context.Context, rec slog.Record) error {
	sc := scopeFromContext(ctx)
	if sc == nil || sc.state != r.state {
		r.state.recordsPassedThrough.Add(1)
		return r.next.Handle(ctx, rec)
	}

	st := r.state
	if rec.Level >= st.opts.PassLevel {
		if rec.Level >= slog.LevelError {
			sc.Trigger("error_log")
		}
		st.recordsPassedThrough.Add(1)
		outRec := buildOutputRecord(rec, r.groups, r.attrs, sc.meta.TraceID, "", false, 0)
		return st.baseNext.Handle(ctx, outRec)
	}

	if rec.Level < st.opts.BufferLevel {
		return nil
	}

	cloned := rec.Clone()
	size := estimateRecordSize(&cloned, r.attrs)
	sc.bufferRecord(cloned, r.groups, r.attrs, size)
	return nil
}

// WithAttrs implements slog.Handler.
func (r *Recorder) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return r
	}
	newAttrs := make([]slog.Attr, len(r.attrs), len(r.attrs)+len(attrs))
	copy(newAttrs, r.attrs)
	if len(r.groups) == 0 {
		newAttrs = append(newAttrs, attrs...)
	} else {
		newAttrs = append(newAttrs, wrapAttrsInGroups(r.groups, attrs))
	}
	return &Recorder{
		state:  r.state,
		next:   r.next.WithAttrs(attrs),
		groups: r.groups,
		attrs:  newAttrs,
	}
}

// WithGroup implements slog.Handler.
func (r *Recorder) WithGroup(name string) slog.Handler {
	if name == "" {
		return r
	}
	newGroups := make([]string, len(r.groups)+1)
	copy(newGroups, r.groups)
	newGroups[len(r.groups)] = name
	return &Recorder{
		state:  r.state,
		next:   r.next.WithGroup(name),
		groups: newGroups,
		attrs:  r.attrs,
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
	rec    slog.Record
	groups []string
	attrs  []slog.Attr
	size   int64
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
	bytes             int64
	triggered         bool
	triggerReason     string
	truncated         bool
	evictedByTotalCap bool
	droppedInScope    uint64
	closed            bool

	// Doubly-linked list pointers guarded by state.listMu.
	prev   *Scope
	next   *Scope
	inList bool
}

// StartScope opens a new request Scope associated with rec and returns a
// derived context along with the Scope. Callers must call Scope.End when the
// unit of work finishes.
func StartScope(ctx context.Context, rec *Recorder, meta ScopeMeta) (context.Context, *Scope) {
	if meta.TraceID == "" {
		meta.TraceID = generateTraceID()
	}
	st := rec.state
	bp := ringBufPool.Get().(*[]bufferedEntry)
	sc := &Scope{
		state:     st,
		meta:      meta,
		startTime: st.now(),
		buf:       *bp,
	}
	st.activeScopes.Add(1)
	st.scopesStarted.Add(1)
	st.linkScope(sc)
	return context.WithValue(ctx, scopeContextKey{}, sc), sc
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

// TraceID returns the trace ID of the active Scope in ctx, or "" if none.
func TraceID(ctx context.Context) string {
	sc := scopeFromContext(ctx)
	if sc == nil {
		return ""
	}
	return sc.meta.TraceID
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
	s.mu.Lock()
	if !s.triggered {
		s.triggered = true
		if reason == "" {
			reason = "triggered"
		}
		s.triggerReason = reason
	}
	s.mu.Unlock()
}

func (s *Scope) bufferRecord(rec slog.Record, groups []string, attrs []slog.Attr, size int64) {
	st := s.state
	maxPerScope := st.opts.MaxPerScope

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}

	if size > maxPerScope {
		s.truncated = true
		s.droppedInScope++
		s.mu.Unlock()
		st.recordsDroppedPerScopeCap.Add(1)
		return
	}

	var freedBytes int64
	var evictedCount uint64
	for s.count > 0 && s.bytes+size > maxPerScope {
		old := s.popOldestLocked()
		freedBytes += old.size
		evictedCount++
	}
	if evictedCount > 0 {
		s.truncated = true
		s.droppedInScope += evictedCount
		st.recordsDroppedPerScopeCap.Add(evictedCount)
	}

	s.pushBackLocked(bufferedEntry{
		rec:    rec,
		groups: groups,
		attrs:  attrs,
		size:   size,
	})
	s.bytes += size
	s.mu.Unlock()

	st.recordsBuffered.Add(1)
	delta := size - freedBytes
	newTotal := st.totalBytes.Add(delta)
	if newTotal > st.opts.MaxTotal {
		st.enforceTotalCap(s)
	}
}

func (s *Scope) pushBackLocked(e bufferedEntry) {
	if len(s.buf) == 0 {
		bp := ringBufPool.Get().(*[]bufferedEntry)
		s.buf = *bp
		s.head = 0
		s.count = 0
	}
	if s.count == len(s.buf) {
		newLen := len(s.buf) * 2
		if newLen < initialRingCapacity {
			newLen = initialRingCapacity
		}
		nb := make([]bufferedEntry, newLen)
		for i := 0; i < s.count; i++ {
			nb[i] = s.buf[(s.head+i)%len(s.buf)]
			s.buf[(s.head+i)%len(s.buf)] = bufferedEntry{}
		}
		if len(s.buf) <= maxPooledRingCap {
			old := s.buf
			ringBufPool.Put(&old)
		}
		s.buf = nb
		s.head = 0
	}
	idx := (s.head + s.count) % len(s.buf)
	s.buf[idx] = e
	s.count++
}

func (s *Scope) popOldestLocked() bufferedEntry {
	e := s.buf[s.head]
	s.buf[s.head] = bufferedEntry{}
	s.head = (s.head + 1) % len(s.buf)
	s.count--
	if s.count == 0 {
		s.head = 0
	}
	s.bytes -= e.size
	return e
}

func (s *Scope) clearAndReleasePoolLocked() (droppedCount int, freedBytes int64) {
	droppedCount = s.count
	freedBytes = s.bytes
	for i := 0; i < s.count; i++ {
		idx := (s.head + i) % len(s.buf)
		s.buf[idx] = bufferedEntry{}
	}
	if len(s.buf) >= initialRingCapacity && len(s.buf) <= maxPooledRingCap {
		b := s.buf
		ringBufPool.Put(&b)
	}
	s.buf = nil
	s.head = 0
	s.count = 0
	s.bytes = 0
	return droppedCount, freedBytes
}

func (s *Scope) drainAndReleasePoolLocked() ([]bufferedEntry, int64) {
	if s.count == 0 {
		if len(s.buf) >= initialRingCapacity && len(s.buf) <= maxPooledRingCap {
			b := s.buf
			ringBufPool.Put(&b)
		}
		s.buf = nil
		return nil, 0
	}
	out := make([]bufferedEntry, s.count)
	for i := 0; i < s.count; i++ {
		idx := (s.head + i) % len(s.buf)
		out[i] = s.buf[idx]
		s.buf[idx] = bufferedEntry{}
	}
	freed := s.bytes
	if len(s.buf) >= initialRingCapacity && len(s.buf) <= maxPooledRingCap {
		b := s.buf
		ringBufPool.Put(&b)
	}
	s.buf = nil
	s.head = 0
	s.count = 0
	s.bytes = 0
	return out, freed
}

// End finalizes the Scope. It evaluates HTTP status and slow-request thresholds,
// flushes all buffered records in chronological order if triggered, or discards
// the buffer and writes a single summary record if healthy.
func (s *Scope) End(ctx context.Context, statusCode int) {
	st := s.state
	now := st.now()
	duration := now.Sub(s.startTime)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true

	if statusCode >= 500 && !s.triggered {
		s.triggered = true
		s.triggerReason = httpStatusReason(statusCode)
	}
	if st.opts.SlowAfter > 0 && duration >= st.opts.SlowAfter && !s.triggered {
		s.triggered = true
		s.triggerReason = "slow_request"
	}

	triggered := s.triggered
	reason := s.triggerReason
	truncated := s.truncated
	droppedInScope := s.droppedInScope

	var entries []bufferedEntry
	var freedBytes int64
	var healthyDropped int
	if triggered {
		entries, freedBytes = s.drainAndReleasePoolLocked()
	} else {
		healthyDropped, freedBytes = s.clearAndReleasePoolLocked()
	}
	s.mu.Unlock()

	if freedBytes > 0 {
		st.totalBytes.Add(-freedBytes)
	}
	st.unlinkScope(s)
	st.activeScopes.Add(-1)

	if triggered {
		st.scopesFlushed.Add(1)
		for i, e := range entries {
			includeTruncMeta := truncated && i == 0
			outRec := buildOutputRecord(e.rec, e.groups, e.attrs, s.meta.TraceID, reason, includeTruncMeta, droppedInScope)
			_ = st.baseNext.Handle(ctx, outRec)
		}
		st.recordsFlushed.Add(uint64(len(entries)))
		return
	}

	droppedCount := uint64(healthyDropped) + droppedInScope
	st.scopesHealthyDropped.Add(1)
	st.recordsHealthyDropped.Add(uint64(healthyDropped))

	if !st.opts.DisableHealthySummary {
		summary := slog.NewRecord(now, slog.LevelInfo, "request completed", 0)
		summary.AddAttrs(
			slog.String("flightrec.trace_id", s.meta.TraceID),
			slog.Duration("duration", duration),
			slog.Uint64("flightrec.dropped_records", droppedCount),
		)
		if statusCode > 0 {
			summary.AddAttrs(slog.Int("status", statusCode))
		}
		if s.meta.Method != "" {
			summary.AddAttrs(slog.String("method", s.meta.Method))
		}
		if s.meta.Path != "" {
			summary.AddAttrs(slog.String("path", s.meta.Path))
		}
		_ = st.baseNext.Handle(ctx, summary)
	}
}

func (st *recorderState) linkScope(sc *Scope) {
	st.listMu.Lock()
	sc.prev = st.newest
	sc.next = nil
	if st.newest != nil {
		st.newest.next = sc
	} else {
		st.oldest = sc
	}
	st.newest = sc
	sc.inList = true
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
		if victim == current {
			victim = victim.next
		}
		st.listMu.Unlock()

		if victim == nil {
			return
		}

		victim.mu.Lock()
		if victim.closed || victim.count == 0 {
			victim.mu.Unlock()
			st.unlinkScope(victim)
			continue
		}
		droppedInt, freed := victim.clearAndReleasePoolLocked()
		dropped := uint64(droppedInt)
		victim.truncated = true
		victim.droppedInScope += dropped
		alreadyEvicted := victim.evictedByTotalCap
		victim.evictedByTotalCap = true
		victim.mu.Unlock()

		st.unlinkScope(victim)
		if freed > 0 {
			st.totalBytes.Add(-freed)
		}
		if dropped > 0 {
			st.recordsDroppedTotalCap.Add(dropped)
		}
		if !alreadyEvicted {
			st.scopesEvictedTotalCap.Add(1)
		}
	}
}

func buildOutputRecord(rec slog.Record, groups []string, preAttrs []slog.Attr, traceID, flushReason string, truncated bool, droppedInScope uint64) slog.Record {
	out := slog.NewRecord(rec.Time, rec.Level, rec.Message, rec.PC)
	if traceID != "" {
		out.AddAttrs(slog.String("flightrec.trace_id", traceID))
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
	if len(preAttrs) > 0 {
		out.AddAttrs(preAttrs...)
	}
	if rec.NumAttrs() > 0 {
		if len(groups) == 0 {
			rec.Attrs(func(a slog.Attr) bool {
				out.AddAttrs(a)
				return true
			})
		} else {
			recAttrs := make([]slog.Attr, 0, rec.NumAttrs())
			rec.Attrs(func(a slog.Attr) bool {
				recAttrs = append(recAttrs, a)
				return true
			})
			out.AddAttrs(wrapAttrsInGroups(groups, recAttrs))
		}
	}
	return out
}

func wrapAttrsInGroups(groups []string, attrs []slog.Attr) slog.Attr {
	anyAttrs := make([]any, len(attrs))
	for i, a := range attrs {
		anyAttrs[i] = a
	}
	g := slog.Group(groups[len(groups)-1], anyAttrs...)
	for i := len(groups) - 2; i >= 0; i-- {
		g = slog.Group(groups[i], g)
	}
	return g
}

func estimateRecordSize(rec *slog.Record, preAttrs []slog.Attr) int64 {
	sz := baseRecordOverhead + int64(len(rec.Message))
	for i := range preAttrs {
		sz += estimateAttrSize(preAttrs[i])
	}
	rec.Attrs(func(a slog.Attr) bool {
		sz += estimateAttrSize(a)
		return true
	})
	return sz
}

func estimateAttrSize(a slog.Attr) int64 {
	sz := int64(16 + len(a.Key))
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindString:
		sz += int64(len(v.String()))
	case slog.KindGroup:
		for _, ga := range v.Group() {
			sz += estimateAttrSize(ga)
		}
	case slog.KindAny:
		if s, ok := v.Any().(string); ok {
			sz += int64(len(s))
		} else if b, ok := v.Any().([]byte); ok {
			sz += int64(len(b))
		} else {
			sz += 16
		}
	default:
		sz += 8
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
		if _, err := rand.Read(seed[:]); err != nil {
			binary.LittleEndian.PutUint64(seed[:8], uint64(time.Now().UnixNano()))
			if n, err2 := rand.Int(rand.Reader, big.NewInt(1<<62)); err2 == nil {
				binary.LittleEndian.PutUint64(seed[8:16], n.Uint64())
			}
		}
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
