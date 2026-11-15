// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/divine-ai-com/flightrec/internal/journal"
	"github.com/divine-ai-com/flightrec/internal/snapshot"
)

// Journal size limits.
const (
	// DefaultJournalSize is the journal file size used without
	// WithJournalSize.
	DefaultJournalSize int64 = 16 << 20
	minJournalSize     int64 = 1 << 20
	maxJournalSize     int64 = 1 << 30
	journalStripes           = 8
)

// ErrJournalUnsupported is returned by StartJournal on platforms without
// shared memory mappings (Windows, for now).
var ErrJournalUnsupported = errors.New("flightrec: journal not supported on this platform")

// ErrJournalRunning is returned by StartJournal when the Recorder already has
// a journal.
var ErrJournalRunning = errors.New("flightrec: journal already running")

// JournalOption configures StartJournal.
type JournalOption interface {
	applyJournal(*journalConfig)
}

type journalConfig struct {
	size int64
}

type journalSizeOption int64

func (o journalSizeOption) applyJournal(c *journalConfig) { c.size = int64(o) }

// WithJournalSize sets the journal file size in bytes: from 1 MiB to 1 GiB,
// rounded up to a multiple of the page size. The default is 16 MiB.
//
// Size it as journaled bytes per second times the longest request you want
// to recover; Stats.JournalOldestAge shows the time span the journal covers.
func WithJournalSize(bytes int64) JournalOption { return journalSizeOption(bytes) }

// journalState is a running journal.
type journalState struct {
	file      *journal.File
	unlock    func() // releases path.lock
	records   atomic.Uint64
	oversized atomic.Uint64 // records too large to journal
}

// maxScratch is the largest encode buffer kept for reuse.
const maxScratch = journal.MaxPayload

var journalScratch = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}

// StartJournal writes the buffered records of every scope to a file at path,
// so that a process killed without warning (SIGKILL, an out-of-memory kill, a
// grace period that ran out) does not lose the records of the requests it was
// handling. Scopes started before StartJournal are not journaled.
//
// Before it starts journaling, StartJournal recovers the journal a killed
// process left at path: for each request that was in flight, it writes the
// buffered records to Next, in order, with their original time, level and
// attributes, tagged flightrec.flush_reason=crash_recovery and
// flightrec.recovered=true, then one "request interrupted by crash" summary.
// Recovery writes each record at least once: a crash during a flush or during
// recovery can write some records again on the next start. Recovered records
// have no source location. Problems found while recovering are counted in
// Stats, never returned. Recovery blocks StartJournal; it reads the old file
// one stripe (an eighth) at a time and decodes only the interrupted requests,
// so it takes well under a second for the default size.
//
// The journal file is created with mode 0600 and written with zeros in full,
// then written to through a shared memory mapping, which the kernel keeps
// when the process dies. A lock file, path.lock, is held while the journal
// runs, so a second process or Recorder using the same path gets an error;
// the lock file is left in place. It does not survive the loss of the
// machine, and it must be on a volume that outlives the process: in
// Kubernetes, an emptyDir volume of the pod.
//
// StartJournal returns an error only when no journal can run: an
// unsupported platform (ErrJournalUnsupported), an invalid size, a journal
// already running on this Recorder (ErrJournalRunning), a path in use by
// another process or Recorder, something other than a journal at path or
// path.recovering (it is never deleted), or a file that cannot be created or
// removed. stop stops journaling, zeros the file and removes it.
// Scopes still open when stop is called are not recovered: call stop after
// the server has drained its requests. The Journal* fields of Stats describe
// the running journal and read zero after stop, except the recovery counters.
func (r *Recorder) StartJournal(path string, opts ...JournalOption) (stop func() error, err error) {
	if !journal.Supported {
		return nil, ErrJournalUnsupported
	}
	cfg := journalConfig{size: DefaultJournalSize}
	for _, o := range opts {
		o.applyJournal(&cfg)
	}
	if cfg.size < minJournalSize || cfg.size > maxJournalSize {
		return nil, fmt.Errorf("flightrec: journal size %d out of range [%d, %d]", cfg.size, minJournalSize, maxJournalSize)
	}
	if page := int64(os.Getpagesize()); cfg.size%page != 0 {
		cfg.size += page - cfg.size%page
	}

	st := r.state
	st.journalMu.Lock()
	defer st.journalMu.Unlock()
	if st.journal.Load() != nil {
		return nil, ErrJournalRunning
	}

	unlock, err := journal.AcquireLock(path + ".lock")
	if err != nil {
		return nil, fmt.Errorf("flightrec: start journal: %w", err)
	}
	if err := st.recoverPending(path); err != nil {
		unlock()
		return nil, err
	}

	// Each stripe needs at least two blocks; small journals get fewer stripes.
	stripes := min(int64(journalStripes), (cfg.size-journal.HeaderSize)/(2*journal.BlockSize))
	host, _ := os.Hostname()
	f, err := journal.Create(path, journal.Header{
		Size:      cfg.size,
		Stripes:   uint32(stripes),
		StartedAt: time.Now().UnixNano(),
		Host:      host,
	})
	if err != nil {
		unlock()
		return nil, fmt.Errorf("flightrec: start journal: %w", err)
	}
	js := &journalState{file: f, unlock: unlock}
	st.journal.Store(js)

	var once sync.Once
	return func() error {
		var err error
		once.Do(func() {
			st.journalMu.Lock()
			defer st.journalMu.Unlock()
			defer unlock()
			st.journal.CompareAndSwap(js, nil)
			err = f.Close(true)
			if rmErr := os.Remove(path); err == nil { //nolint:gosec // G703: the path is the journal path the application configured
				err = rmErr
			}
		})
		return err
	}, nil
}

// journalStart writes a start entry for a new scope if a journal is running.
func (st *recorderState) journalStart(sc *Scope) {
	js := st.journal.Load()
	if js == nil {
		return
	}
	sc.jid = st.journalIDs.Add(1)
	sc.journal = js
	bp := journalScratch.Get().(*[]byte)
	b := appendScopeStart((*bp)[:0], scopeStart{start: sc.startTime, traceID: sc.meta.TraceID, method: sc.meta.Method, path: sc.meta.Path})
	js.file.Append(sc.jid, journal.KindStart, sc.jid, b)
	putScratch(bp, b)
}

// journalRecord writes a buffered record of sc to its journal.
func (st *recorderState) journalRecord(sc *Scope, r *Recorder, rec slog.Record, tc TraceContext) {
	js := sc.journal
	if js == nil || st.journal.Load() != js {
		return
	}
	bp := journalScratch.Get().(*[]byte)
	b := appendJournalRecord((*bp)[:0], r, rec, tc)
	if len(b) > journal.MaxPayload {
		// Too large for an entry: leave a marker, so recovery can say the
		// scope lost a record, and count the record as dropped.
		js.oversized.Add(1)
		js.file.Append(sc.jid, journal.KindDropped, sc.jid, nil)
	} else if js.file.Append(sc.jid, journal.KindRecord, sc.jid, b) {
		js.records.Add(1)
	}
	putScratch(bp, b)
}

// putScratch returns an encode buffer to the pool unless it grew past what a
// journal entry can hold.
func putScratch(bp *[]byte, b []byte) {
	if cap(b) > maxScratch {
		return
	}
	*bp = b
	journalScratch.Put(bp)
}

// journalEnd writes the end entry of sc: nothing of it needs recovering.
func (st *recorderState) journalEnd(sc *Scope) {
	js := sc.journal
	if js == nil || st.journal.Load() != js {
		return
	}
	js.file.Append(sc.jid, journal.KindEnd, sc.jid, nil)
}

// journalRekey records that sc adopted a span's trace ID, so recovery labels
// its records the way a live flush would.
func (st *recorderState) journalRekey(sc *Scope, traceID, alias string) {
	js := sc.journal
	if js == nil || st.journal.Load() != js {
		return
	}
	bp := journalScratch.Get().(*[]byte)
	b := appendScopeRekey((*bp)[:0], traceID, alias)
	js.file.Append(sc.jid, journal.KindRekey, sc.jid, b)
	putScratch(bp, b)
}

// journalStats fills the journal fields of s.
func (st *recorderState) journalStats(s *Stats) {
	s.JournalRecoveredScopes = st.journalRecoveredScopes.Load()
	s.JournalRecoveredRecords = st.journalRecoveredRecords.Load()
	s.JournalCorruptEntries = st.journalCorrupt.Load()
	s.JournalDiscardedFiles = st.journalDiscarded.Load()
	s.JournalAbandonedRecoveries = st.journalAbandoned.Load()
	s.JournalRecoveryErrors = st.journalRecoveryErrors.Load()
	js := st.journal.Load()
	if js == nil {
		return
	}
	ws := js.file.Stats()
	s.JournalRecordsWritten = js.records.Load()
	s.JournalBytesWritten = ws.Bytes
	s.JournalWraps = ws.Wraps
	s.JournalDropped = ws.Dropped + js.oversized.Load()
	s.JournalFaults = ws.Faults
	if oldest := js.file.OldestBlockTime(); oldest > 0 {
		s.JournalOldestAge = time.Since(time.Unix(0, oldest)) // the writer stamps blocks with the wall clock
	}
}

// Journal payload encoding. Private: it may change in any release, and the
// version byte at the start of each payload lets a reader reject old entries.
const journalPayloadVersion = 1

var errBadPayload = errors.New("flightrec: invalid journal payload")

// scopeStart is the payload of a journal start entry.
type scopeStart struct {
	start   time.Time
	traceID string
	method  string
	path    string
}

func appendScopeStart(b []byte, s scopeStart) []byte {
	b = append(b, journalPayloadVersion)
	b = appendJTime(b, s.start)
	b = appendJString(b, s.traceID)
	b = appendJString(b, s.method)
	return appendJString(b, s.path)
}

func decodeScopeStart(b []byte) (scopeStart, error) {
	d := decoder{b: b}
	if d.byte() != journalPayloadVersion {
		return scopeStart{}, errBadPayload
	}
	s := scopeStart{start: d.time(), traceID: d.string(), method: d.string(), path: d.string()}
	return s, d.err
}

// appendScopeRekey encodes a rekey entry: the adopted trace ID and the alias
// kept for the old one (empty for a generated ID).
func appendScopeRekey(b []byte, traceID, alias string) []byte {
	b = append(b, journalPayloadVersion)
	b = appendJString(b, traceID)
	return appendJString(b, alias)
}

func decodeScopeRekey(b []byte) (traceID, alias string, err error) {
	d := decoder{b: b}
	if d.byte() != journalPayloadVersion {
		return "", "", errBadPayload
	}
	traceID, alias = d.string(), d.string()
	return traceID, alias, d.err
}

// journalRecord is the payload of a journal record entry: what a flush would
// write, with the child logger's attributes and groups kept apart so the
// output can be rebuilt by buildOutputRecord.
type journalRecord struct {
	time      time.Time
	level     slog.Level
	traceCtx  TraceContext
	message   string
	rootAttrs []slog.Attr
	frames    []groupFrame
	attrs     []slog.Attr
}

func appendJournalRecord(b []byte, r *Recorder, rec slog.Record, tc TraceContext) []byte {
	b = append(b, journalPayloadVersion)
	b = appendJTime(b, rec.Time)
	b = binary.AppendVarint(b, int64(rec.Level))
	b = append(b, tc.TraceID[:]...)
	b = append(b, tc.SpanID[:]...)
	b = appendJString(b, rec.Message)
	b = appendJAttrs(b, r.rootAttrs, 0)
	b = binary.AppendUvarint(b, uint64(len(r.frames)))
	for _, f := range r.frames {
		b = appendJString(b, f.name)
		b = appendJAttrs(b, f.attrs, 0)
	}
	b = binary.AppendUvarint(b, uint64(rec.NumAttrs()))
	rec.Attrs(func(a slog.Attr) bool {
		b = appendJAttr(b, a, 0)
		return true
	})
	return b
}

func decodeJournalRecord(b []byte) (journalRecord, error) {
	d := decoder{b: b}
	if d.byte() != journalPayloadVersion {
		return journalRecord{}, errBadPayload
	}
	var jr journalRecord
	jr.time = d.time()
	jr.level = slog.Level(d.varint())
	copy(jr.traceCtx.TraceID[:], d.bytes(16))
	copy(jr.traceCtx.SpanID[:], d.bytes(8))
	jr.message = d.string()
	jr.rootAttrs = d.attrs(0)
	n := d.count()
	for i := 0; i < n && d.err == nil; i++ {
		jr.frames = append(jr.frames, groupFrame{name: d.string(), attrs: d.attrs(0)})
	}
	jr.attrs = d.attrs(0)
	return jr, d.err
}

// maxJournalValue bounds the two renderings of a slog.Any value in a journal
// record together, so a record with one large value still fits an entry.
const maxJournalValue = 32 << 10

// Attribute value tags. They match neither slog.Kind values nor any order.
const (
	tagString byte = iota + 1
	tagInt
	tagUint
	tagFloat
	tagBool
	tagDuration
	tagTime
	tagGroup
	tagJSON     // JSON only: written by v0.4 and v0.5
	tagSnapshot // JSON and text renderings of a slog.Any value
)

func appendJString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendJAttrs(b []byte, attrs []slog.Attr, depth int) []byte {
	b = binary.AppendUvarint(b, uint64(len(attrs)))
	for _, a := range attrs {
		b = appendJAttr(b, a, depth)
	}
	return b
}

// appendJTime writes t as a flag byte, then Unix seconds and nanoseconds, so
// every time round-trips, the zero time included (UnixNano covers only the
// years 1678 to 2262).
func appendJTime(b []byte, t time.Time) []byte {
	if t.IsZero() {
		return append(b, 0)
	}
	b = append(b, 1)
	b = binary.AppendVarint(b, t.Unix())
	return binary.AppendUvarint(b, uint64(t.Nanosecond()))
}

func appendJAttr(b []byte, a slog.Attr, depth int) []byte {
	b = appendJString(b, a.Key)
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindString:
		b = append(b, tagString)
		b = appendJString(b, v.String())
	case slog.KindInt64:
		b = append(b, tagInt)
		b = binary.AppendVarint(b, v.Int64())
	case slog.KindUint64:
		b = append(b, tagUint)
		b = binary.AppendUvarint(b, v.Uint64())
	case slog.KindFloat64:
		b = append(b, tagFloat)
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(v.Float64()))
	case slog.KindBool:
		b = append(b, tagBool)
		if v.Bool() {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
	case slog.KindDuration:
		b = append(b, tagDuration)
		b = binary.AppendVarint(b, int64(v.Duration()))
	case slog.KindTime:
		b = append(b, tagTime)
		b = appendJTime(b, v.Time())
	case slog.KindGroup:
		if depth >= maxGroupDepth {
			// Deeper than the decoder accepts: kept as text.
			b = append(b, tagString)
			b = appendJString(b, v.String())
			break
		}
		b = append(b, tagGroup)
		b = appendJAttrs(b, v.Group(), depth+1)
	default:
		// slog.Any: the same copy retention takes. Errors and values JSON
		// cannot encode come back as strings; the rest is JSON text.
		sv := snapshot.Of(v.Any())
		if s, ok := sv.Any().(snapshot.Value); ok {
			raw, text := snapshot.Parts(s)
			// Both renderings must fit one journal entry: drop the text
			// first (text handlers then print the JSON), then the JSON.
			if len(raw)+len(text) > maxJournalValue {
				text = string(raw)
				if 2*len(raw) > maxJournalValue {
					raw, _ = json.Marshal(snapshot.Truncated)
					text = snapshot.Truncated
				}
			}
			b = append(b, tagSnapshot)
			b = appendJString(b, string(raw))
			b = appendJString(b, text)
		} else {
			b = append(b, tagString)
			b = appendJString(b, sv.String())
		}
	}
	return b
}

// decoder reads a journal payload. The first error sticks; later reads return
// zero values, so callers check err once at the end.
type decoder struct {
	b   []byte
	err error
}

func (d *decoder) fail() { d.err = errBadPayload; d.b = nil }

func (d *decoder) byte() byte {
	if d.err != nil || len(d.b) < 1 {
		d.fail()
		return 0
	}
	c := d.b[0]
	d.b = d.b[1:]
	return c
}

func (d *decoder) bytes(n int) []byte {
	if d.err != nil || n < 0 || len(d.b) < n {
		d.fail()
		return nil
	}
	s := d.b[:n]
	d.b = d.b[n:]
	return s
}

func (d *decoder) varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

// count reads a length and rejects values larger than the bytes left, so
// hostile input cannot make the decoder allocate unbounded memory.
func (d *decoder) count() int {
	n := d.uvarint()
	if n > uint64(len(d.b)) {
		d.fail()
		return 0
	}
	return int(n)
}

func (d *decoder) string() string { return string(d.bytes(d.count())) }

func (d *decoder) time() time.Time {
	switch d.byte() {
	case 0:
		return time.Time{}
	case 1:
		sec := d.varint()
		nsec := d.uvarint()
		if nsec >= 1e9 {
			d.fail()
			return time.Time{}
		}
		return time.Unix(sec, int64(nsec))
	default:
		d.fail()
		return time.Time{}
	}
}

// maxGroupDepth bounds nested groups in decoded attributes.
const maxGroupDepth = 32

func (d *decoder) attrs(depth int) []slog.Attr {
	n := d.count()
	if depth > maxGroupDepth {
		d.fail()
		return nil
	}
	// Each attribute takes at least two bytes; capping the preallocation
	// keeps a hostile count from allocating far more than the payload.
	out := make([]slog.Attr, 0, min(n, 16))
	for i := 0; i < n && d.err == nil; i++ {
		key := d.string()
		var v slog.Value
		switch d.byte() {
		case tagString:
			v = slog.StringValue(d.string())
		case tagInt:
			v = slog.Int64Value(d.varint())
		case tagUint:
			v = slog.Uint64Value(d.uvarint())
		case tagFloat:
			if p := d.bytes(8); p != nil {
				v = slog.Float64Value(math.Float64frombits(binary.LittleEndian.Uint64(p)))
			}
		case tagBool:
			v = slog.BoolValue(d.byte() == 1)
		case tagDuration:
			v = slog.DurationValue(time.Duration(d.varint()))
		case tagTime:
			v = slog.TimeValue(d.time())
		case tagGroup:
			v = slog.GroupValue(d.attrs(depth + 1)...)
		case tagJSON:
			raw := d.string()
			v = slog.AnyValue(snapshot.Make([]byte(raw), raw))
		case tagSnapshot:
			raw := d.string()
			v = slog.AnyValue(snapshot.Make([]byte(raw), d.string()))
		default:
			d.fail()
		}
		out = append(out, slog.Attr{Key: key, Value: v})
	}
	return out
}

// maxRecoveryAttempts is how many times a journal file is recovered before it
// is abandoned, so a crash during recovery cannot become a crash loop.
const maxRecoveryAttempts = 3

// flushReasonCrashRecovery is the flush reason of recovered records.
const flushReasonCrashRecovery = "crash_recovery"

// recoverPending recovers the journal files a previous process left at path,
// and leaves path free for a new journal. The caller holds the lock file.
//
// A file at path.recovering is from a recovery that did not finish; it is
// retried first. A file at path is then moved to path.recovering before it is
// recovered, so a crash during recovery leaves it there for the next start,
// which counts the attempt. If it cannot be moved, it is recovered in place;
// the attempt counter is in the file itself.
//
// It returns an error only when a file that is not a journal is in the way,
// or a recovered file cannot be removed: deleting it is not ours to do, and
// leaving it would block every later recovery.
func (st *recorderState) recoverPending(path string) error {
	recovering := path + ".recovering"
	if err := st.recoverFile(recovering); err != nil {
		return err
	}
	switch journal.Classify(path) {
	case journal.ClassMissing:
		return nil
	case journal.ClassJournal:
		if os.Rename(path, recovering) == nil { //nolint:gosec // G703: the path is the journal path the application configured
			return st.recoverFile(recovering)
		}
	}
	return st.recoverFile(path)
}

// recoverFile recovers the journal file at path, if there is one, then zeros
// and removes it.
func (st *recorderState) recoverFile(path string) error {
	switch journal.Classify(path) {
	case journal.ClassMissing:
		return nil
	case journal.ClassSymlink:
		st.journalDiscarded.Add(1)
	case journal.ClassDead:
		// Creation or zeroing was cut short: nothing to recover.
	case journal.ClassJournal:
		st.recoverJournal(path)
	default:
		return fmt.Errorf("flightrec: start journal: %s is in the way and is not a journal file; remove it", path)
	}
	if err := journal.Discard(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("flightrec: start journal: remove %s: %w", path, err)
	}
	return nil
}

// recoveredScope is one interrupted scope read from a journal.
type recoveredScope struct {
	id      uint64
	start   scopeStart
	started bool   // the start entry survived
	dropped int    // records too large to journal
	rekeyed string // trace ID adopted after the start, if any
	alias   string // header ID kept as an alias by that adoption
	records []journalRecord
}

// first returns the time the scope is ordered by: its start, or its oldest
// surviving record when the start entry was overwritten.
func (sc *recoveredScope) first() time.Time {
	if sc.started || len(sc.records) == 0 {
		return sc.start.start
	}
	return sc.records[0].time
}

// recoverJournal writes the scopes left open in the journal file at path to
// Next. Problems are counted, never returned: recovery must not stop the
// service from starting.
//
// The file is read one stripe at a time. A scope's entries all sit in one
// stripe, in the order they were written, so a first pass over a stripe finds
// the scopes that ended and a second decodes only the others: memory stays at
// one stripe plus the interrupted scopes.
func (st *recorderState) recoverJournal(path string) {
	r, err := journal.OpenRecovering(path, maxJournalSize)
	if err != nil {
		st.journalDiscarded.Add(1)
		return
	}
	defer func() { _ = r.Close() }()
	attempts, err := r.IncrementAttempts()
	if err != nil {
		st.journalDiscarded.Add(1)
		return
	}
	if attempts > maxRecoveryAttempts {
		st.journalAbandoned.Add(1)
		return
	}

	// The old file's header gives its size and layout, so a changed
	// WithJournalSize does not lose it.
	h := r.Header
	var open []*recoveredScope
	var buf []byte
	for s := 0; s < int(h.Stripes); s++ {
		ended := map[uint64]struct{}{}
		var rs journal.ReadStats
		buf, rs, err = r.Stripe(s, buf, func(e journal.Entry) {
			if e.Kind == journal.KindEnd {
				ended[e.Scope] = struct{}{}
			}
		})
		if err != nil {
			st.journalCorrupt.Add(1)
			continue
		}
		st.journalCorrupt.Add(uint64(rs.Invalid))

		scopes := map[uint64]*recoveredScope{}
		journal.WalkStripe(h, buf, s, func(e journal.Entry) {
			if _, done := ended[e.Scope]; done {
				return
			}
			sc := scopes[e.Scope]
			if sc == nil {
				sc = &recoveredScope{id: e.Scope}
				scopes[e.Scope] = sc
				open = append(open, sc)
			}
			switch e.Kind {
			case journal.KindStart:
				s, err := decodeScopeStart(e.Payload)
				if err != nil {
					st.journalCorrupt.Add(1)
					return
				}
				sc.start, sc.started = s, true
			case journal.KindDropped:
				sc.dropped++
			case journal.KindRekey:
				tid, alias, err := decodeScopeRekey(e.Payload)
				if err != nil {
					st.journalCorrupt.Add(1)
					return
				}
				sc.rekeyed, sc.alias = tid, alias
			case journal.KindRecord:
				jr, err := decodeJournalRecord(e.Payload)
				if err != nil {
					st.journalCorrupt.Add(1)
					return
				}
				sc.records = append(sc.records, jr)
			}
		})
	}

	// Within a scope, records stay in the order they were journaled, which is
	// the order a flush writes them.
	slices.SortFunc(open, func(a, b *recoveredScope) int {
		return cmp.Or(a.first().Compare(b.first()), cmp.Compare(a.id, b.id))
	})
	for _, sc := range open {
		st.recoverScope(sc, h.Host, time.Unix(0, h.StartedAt))
	}
}

// recoverScope writes the records of one interrupted scope to Next, then its
// summary.
func (st *recorderState) recoverScope(sc *recoveredScope, host string, processStart time.Time) {
	traceID := sc.start.traceID
	if sc.rekeyed != "" {
		traceID = sc.rekeyed // adopted a span's trace after it started
	}
	if traceID == "" {
		// The start entry was overwritten: take the trace from a record.
		for _, jr := range sc.records {
			if jr.traceCtx.HasTraceID() {
				traceID = jr.traceCtx.TraceIDHex()
				break
			}
		}
	}
	scopeTrace, hasScopeTrace := ParseTraceIDHex(traceID)
	recovered := []slog.Attr{
		slog.Bool("flightrec.recovered", true),
		slog.String("flightrec.recovered_host", host),
		slog.Time("flightrec.recovered_process_start", processStart),
	}
	if traceID == "" {
		// Nothing else tells apart the records of scopes without a trace.
		recovered = append(recovered, slog.Uint64("flightrec.recovered_scope", sc.id))
	}
	if sc.alias != "" {
		recovered = append(recovered, slog.String("flightrec.request_id", sc.alias))
	}

	written := 0
	for i, jr := range sc.records {
		rec := slog.NewRecord(jr.time, jr.level, jr.message, 0)
		rec.AddAttrs(jr.attrs...)
		r := &Recorder{state: st, rootAttrs: jr.rootAttrs, frames: jr.frames}
		tc := jr.traceCtx
		if !tc.HasTraceID() && hasScopeTrace {
			tc.TraceID = scopeTrace
		}
		out := r.buildOutputRecord(rec, traceID, tc.SpanIDHex(), flushReasonCrashRecovery, false, 0)
		if i == 0 && (!sc.started || sc.dropped > 0) {
			// The oldest records were overwritten, or some were too large
			// to journal.
			out.AddAttrs(slog.Bool("flightrec.truncated", true))
			if sc.dropped > 0 {
				out.AddAttrs(slog.Int("flightrec.dropped_records", sc.dropped))
			}
		}
		out.AddAttrs(recovered...)
		if st.handleRecovered(st.injectTraceContext(context.Background(), tc), out) {
			written++
		}
	}
	st.journalRecoveredRecords.Add(uint64(written))
	st.journalRecoveredScopes.Add(1)

	summary := slog.NewRecord(st.now(), slog.LevelError, "request interrupted by crash", 0)
	if traceID != "" {
		summary.AddAttrs(slog.String("flightrec.trace_id", traceID))
	}
	summary.AddAttrs(
		slog.String("flightrec.flush_reason", flushReasonCrashRecovery),
		slog.Int("flightrec.recovered_records", len(sc.records)),
	)
	if sc.start.method != "" {
		summary.AddAttrs(slog.String("method", sc.start.method))
	}
	if sc.start.path != "" {
		summary.AddAttrs(slog.String("path", sc.start.path))
	}
	if sc.started {
		summary.AddAttrs(slog.Time("start", sc.start.start))
	}
	if !sc.started || sc.dropped > 0 {
		summary.AddAttrs(slog.Bool("flightrec.truncated", true))
	}
	if sc.dropped > 0 {
		summary.AddAttrs(slog.Int("flightrec.dropped_records", sc.dropped))
	}
	summary.AddAttrs(recovered...)
	ctx := context.Background()
	if hasScopeTrace {
		ctx = st.injectTraceContext(ctx, TraceContext{TraceID: scopeTrace})
	}
	st.handleRecovered(ctx, summary)
}

// handleRecovered writes a recovered record to Next and reports whether it
// was written. A panic in Next is recovered: one bad record must not cost the
// rest of the journal, which is deleted after recovery.
func (st *recorderState) handleRecovered(ctx context.Context, rec slog.Record) (ok bool) {
	defer func() {
		if recover() != nil {
			st.journalRecoveryErrors.Add(1)
			ok = false
		}
	}()
	if err := st.baseNext.Handle(ctx, rec); err != nil {
		st.journalRecoveryErrors.Add(1)
		return false
	}
	return true
}
