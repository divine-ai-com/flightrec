// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"log/slog"
	"unsafe"
)

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
