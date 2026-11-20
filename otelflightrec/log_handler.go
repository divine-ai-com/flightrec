// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package otelflightrec

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"

	"github.com/haawklabs/flightrec"
)

const defaultInstrumentationName = "github.com/haawklabs/flightrec/otelflightrec"

// LogHandlerOptions configures a LogHandler.
type LogHandlerOptions struct {
	// LoggerProvider is the OpenTelemetry LoggerProvider used to emit log
	// records. If nil, otel.GetLoggerProvider() is used.
	LoggerProvider otellog.LoggerProvider

	// InstrumentationName is the logger scope name passed to
	// LoggerProvider.Logger. Defaults to
	// "github.com/haawklabs/flightrec/otelflightrec".
	InstrumentationName string

	// InstrumentationVersion is the optional version passed to
	// LoggerProvider.Logger.
	InstrumentationVersion string

	// Level is the minimum slog.Level enabled when Enabled is called directly
	// (such as for logs outside a flightrec.Scope). Defaults to slog.LevelInfo
	// when nil. Inside a flightrec.Scope, flightrec.Recorder governs buffering
	// and sends flushed records directly to Handle regardless of this level.
	Level slog.Leveler

	// Next is an optional secondary slog.Handler (for example,
	// slog.NewJSONHandler(os.Stdout, nil)) that also receives every record
	// handled by this LogHandler.
	Next slog.Handler
}

type logGroupFrame struct {
	name  string
	attrs []slog.Attr
}

// LogHandler is a log/slog.Handler that bridges slog records (including
// records flushed by flightrec.Recorder) to OpenTelemetry Logs
// (go.opentelemetry.io/otel/log).
//
// Because flightrec captures the active OpenTelemetry TraceID and SpanID at
// log time and restores that SpanContext into ctx when flushing buffered
// records, emitted OpenTelemetry log records carry the exact TraceID and
// SpanID that were active when the log call was made, along with the original
// timestamp, severity, and structured attributes.
type LogHandler struct {
	logger    otellog.Logger
	level     slog.Leveler
	next      slog.Handler
	rootAttrs []slog.Attr
	frames    []logGroupFrame
}

var _ slog.Handler = (*LogHandler)(nil)

// NewLogHandler creates a new OpenTelemetry LogHandler with the given options.
func NewLogHandler(opts LogHandlerOptions) *LogHandler {
	provider := opts.LoggerProvider
	if provider == nil {
		provider = otel.GetLoggerProvider()
	}
	name := opts.InstrumentationName
	if name == "" {
		name = defaultInstrumentationName
	}
	var loggerOpts []otellog.LoggerOption
	if opts.InstrumentationVersion != "" {
		loggerOpts = append(loggerOpts, otellog.WithInstrumentationVersion(opts.InstrumentationVersion))
	}
	lvl := opts.Level
	if lvl == nil {
		lvl = slog.LevelInfo
	}
	return &LogHandler{
		logger: provider.Logger(name, loggerOpts...),
		level:  lvl,
		next:   opts.Next,
	}
}

// Enabled implements slog.Handler.
func (h *LogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if level >= h.level.Level() {
		return true
	}
	if h.next != nil {
		return h.next.Enabled(ctx, level)
	}
	return false
}

// Handle implements slog.Handler.
func (h *LogHandler) Handle(ctx context.Context, rec slog.Record) error {
	if ctx == nil {
		ctx = context.Background()
	}

	var r otellog.Record
	if !rec.Time.IsZero() {
		r.SetTimestamp(rec.Time)
	}
	r.SetObservedTimestamp(time.Now())
	r.SetSeverity(slogLevelToOTelSeverity(rec.Level))
	r.SetSeverityText(rec.Level.String())
	r.SetBody(attribute.StringValue(rec.Message))

	kvs := h.buildOTelAttributes(rec)
	if len(kvs) > 0 {
		r.AddAttributes(kvs...)
	}

	emitCtx := ctx
	if !trace.SpanContextFromContext(emitCtx).IsValid() {
		if tidHex := flightrec.TraceID(emitCtx); tidHex != "" {
			if tidBytes, ok := flightrec.ParseTraceIDHex(tidHex); ok {
				emitCtx = InjectTraceContext(emitCtx, flightrec.TraceContext{TraceID: tidBytes})
			}
		}
	}

	h.logger.Emit(emitCtx, r)

	if h.next != nil {
		return h.next.Handle(ctx, rec)
	}
	return nil
}

// WithAttrs implements slog.Handler.
func (h *LogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	cleaned := cleanSlogAttrs(attrs)
	var nextHandler slog.Handler
	if h.next != nil {
		nextHandler = h.next.WithAttrs(attrs)
	}
	if len(cleaned) == 0 {
		return &LogHandler{
			logger:    h.logger,
			level:     h.level,
			next:      nextHandler,
			rootAttrs: h.rootAttrs,
			frames:    h.frames,
		}
	}
	if len(h.frames) == 0 {
		newRoot := make([]slog.Attr, len(h.rootAttrs), len(h.rootAttrs)+len(cleaned))
		copy(newRoot, h.rootAttrs)
		newRoot = append(newRoot, cleaned...)
		return &LogHandler{
			logger:    h.logger,
			level:     h.level,
			next:      nextHandler,
			rootAttrs: newRoot,
			frames:    h.frames,
		}
	}
	newFrames := make([]logGroupFrame, len(h.frames))
	copy(newFrames, h.frames)
	last := &newFrames[len(newFrames)-1]
	merged := make([]slog.Attr, len(last.attrs), len(last.attrs)+len(cleaned))
	copy(merged, last.attrs)
	merged = append(merged, cleaned...)
	last.attrs = merged
	return &LogHandler{
		logger:    h.logger,
		level:     h.level,
		next:      nextHandler,
		rootAttrs: h.rootAttrs,
		frames:    newFrames,
	}
}

// WithGroup implements slog.Handler.
func (h *LogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	newFrames := make([]logGroupFrame, len(h.frames)+1)
	copy(newFrames, h.frames)
	newFrames[len(h.frames)] = logGroupFrame{name: name}
	var nextHandler slog.Handler
	if h.next != nil {
		nextHandler = h.next.WithGroup(name)
	}
	return &LogHandler{
		logger:    h.logger,
		level:     h.level,
		next:      nextHandler,
		rootAttrs: h.rootAttrs,
		frames:    newFrames,
	}
}

func (h *LogHandler) buildOTelAttributes(rec slog.Record) []attribute.KeyValue {
	var recAttrs []slog.Attr
	if n := rec.NumAttrs(); n > 0 {
		raw := make([]slog.Attr, 0, n)
		rec.Attrs(func(a slog.Attr) bool {
			raw = append(raw, a)
			return true
		})
		recAttrs = cleanSlogAttrs(raw)
	}

	inner := recAttrs
	for i := len(h.frames) - 1; i >= 0; i-- {
		f := h.frames[i]
		if len(f.attrs) == 0 && len(inner) == 0 {
			continue
		}
		combined := make([]slog.Attr, 0, len(f.attrs)+len(inner))
		combined = append(combined, f.attrs...)
		combined = append(combined, inner...)
		inner = []slog.Attr{{Key: f.name, Value: slog.GroupValue(combined...)}}
	}

	total := len(h.rootAttrs) + len(inner)
	if total == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, total)
	for _, a := range h.rootAttrs {
		out = append(out, slogAttrToOTelKV(a))
	}
	for _, a := range inner {
		out = append(out, slogAttrToOTelKV(a))
	}
	return out
}

func cleanSlogAttrs(attrs []slog.Attr) []slog.Attr {
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
			sub := cleanSlogAttrs(a.Value.Group())
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

func slogAttrToOTelKV(a slog.Attr) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   attribute.Key(a.Key),
		Value: slogValueToOTelValue(a.Value),
	}
}

func slogValueToOTelValue(v slog.Value) attribute.Value {
	v = v.Resolve()
	switch v.Kind() {
	case slog.KindString:
		return attribute.StringValue(v.String())
	case slog.KindInt64:
		return attribute.Int64Value(v.Int64())
	case slog.KindUint64:
		u := v.Uint64()
		if u > math.MaxInt64 {
			// OpenTelemetry attributes have no unsigned type; keep the exact
			// value as a string rather than wrap it to a negative number.
			return attribute.StringValue(strconv.FormatUint(u, 10))
		}
		return attribute.Int64Value(int64(u))
	case slog.KindFloat64:
		return attribute.Float64Value(v.Float64())
	case slog.KindBool:
		return attribute.BoolValue(v.Bool())
	case slog.KindDuration:
		return attribute.Int64Value(int64(v.Duration()))
	case slog.KindTime:
		return attribute.StringValue(v.Time().Format(time.RFC3339Nano))
	case slog.KindGroup:
		group := v.Group()
		kvs := make([]attribute.KeyValue, 0, len(group))
		for _, ga := range group {
			kvs = append(kvs, slogAttrToOTelKV(ga))
		}
		return attribute.MapValue(kvs...)
	case slog.KindAny:
		anyVal := v.Any()
		if anyVal == nil {
			return attribute.Value{}
		}
		switch x := anyVal.(type) {
		case error:
			return attribute.StringValue(x.Error())
		case []byte:
			return attribute.ByteSliceValue(x)
		case fmt.Stringer:
			return attribute.StringValue(x.String())
		default:
			return attribute.StringValue(fmt.Sprint(anyVal))
		}
	default:
		return attribute.StringValue(v.String())
	}
}

func slogLevelToOTelSeverity(level slog.Level) otellog.Severity {
	switch {
	case level >= slog.LevelError+4:
		return otellog.SeverityFatal1
	case level >= slog.LevelError:
		return otellog.SeverityError1
	case level >= slog.LevelWarn:
		return otellog.SeverityWarn1
	case level >= slog.LevelInfo:
		return otellog.SeverityInfo1
	default:
		return otellog.SeverityDebug1
	}
}
