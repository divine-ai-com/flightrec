// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/divine-ai-com/flightrec/internal/snapshot"
	"log/slog"
	"math"
	"time"
)

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
