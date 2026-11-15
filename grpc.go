// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
)

// GRPCMetadataExtractor extracts a trace ID from a gRPC request context
// (for example via google.golang.org/grpc/metadata.FromIncomingContext
// combined with ExtractTraceIDFromMetadata). It is not consulted when the
// context already carries a trace (an OpenTelemetry span, as reported by
// Options.TraceExtractor or the registered TraceBridge): that trace is used.
//
// For ready-to-use grpc.UnaryServerInterceptor and grpc.StreamServerInterceptor
// implementations that read incoming gRPC metadata directly, see subpackage
// github.com/divine-ai-com/flightrec/grpcflightrec.
type GRPCMetadataExtractor func(ctx context.Context) string

// GRPCUnaryHandler matches the handler function passed to a gRPC unary
// server interceptor.
type GRPCUnaryHandler func(ctx context.Context, req any) (any, error)

// GRPCStreamHandler runs a gRPC streaming handler with a scope-enriched context.
type GRPCStreamHandler func(ctx context.Context) error

type incomingMetadataContextKey struct{}

// WithIncomingMetadata attaches a gRPC metadata map to ctx so that
// InvokeGRPCUnary and InvokeGRPCStream can extract W3C traceparent,
// x-trace-id, or x-request-id headers when extractTraceID is nil.
func WithIncomingMetadata(ctx context.Context, md map[string][]string) context.Context {
	return context.WithValue(ctx, incomingMetadataContextKey{}, md)
}

// ExtractTraceIDFromMetadata extracts a trace ID from a gRPC-style metadata
// map (checking "traceparent", "x-trace-id", and "x-request-id"
// case-insensitively). Values of x-trace-id and x-request-id follow the same
// rules as in HTTPMiddleware: the first value of 1 to 128 bytes of letters,
// digits and _ . : - / + = @ wins.
func ExtractTraceIDFromMetadata(md map[string][]string) string {
	if len(md) == 0 {
		return ""
	}
	if vals := lookupMetadataValues(md, "traceparent"); len(vals) > 0 {
		for _, v := range vals {
			if tid := parseW3CTraceparent(strings.TrimSpace(v)); tid != "" {
				return tid
			}
		}
	}
	for _, key := range []string{"x-trace-id", "x-request-id"} {
		if vals := lookupMetadataValues(md, key); len(vals) > 0 {
			for _, v := range vals {
				if trimmed := strings.TrimSpace(v); validTraceID(trimmed) {
					return trimmed
				}
			}
		}
	}
	return ""
}

func lookupMetadataValues(md map[string][]string, targetLower string) []string {
	if vals, ok := md[targetLower]; ok && len(vals) > 0 {
		return vals
	}
	for k, vals := range md {
		if strings.EqualFold(k, targetLower) && len(vals) > 0 {
			return vals
		}
	}
	return nil
}

// InvokeGRPCUnary wraps a gRPC unary handler in a flight-recorder Scope
// without requiring google.golang.org/grpc as a dependency of flightrec itself.
//
// Only server-side fault codes (Unknown, DeadlineExceeded, Unimplemented,
// Internal, Unavailable, DataLoss) or non-gRPC errors (excluding
// context.Canceled) trigger a buffer flush; client errors such as NotFound,
// InvalidArgument, PermissionDenied, Unauthenticated, AlreadyExists, or
// Canceled end the scope as healthy with "grpc.code" recorded on the summary.
func InvokeGRPCUnary(
	ctx context.Context,
	rec *Recorder,
	fullMethod string,
	extractTraceID GRPCMetadataExtractor,
	req any,
	handler GRPCUnaryHandler,
) (resp any, err error) {
	traceID := resolveGRPCTraceID(ctx, rec.state, extractTraceID)
	scopeCtx, sc := StartScope(ctx, rec, ScopeMeta{
		TraceID: traceID,
		Method:  "gRPC",
		Path:    fullMethod,
	})

	defer func() {
		if p := recover(); p != nil {
			sc.Trigger("panic")
			sc.endWithSummaryAttr(scopeCtx, 0, slog.Int("grpc.code", 2))
			panic(p)
		}
		if err != nil {
			code, isServerFault := classifyGRPCError(err)
			if isServerFault {
				sc.Trigger("grpc_error")
			}
			sc.endWithSummaryAttr(scopeCtx, 0, slog.Int("grpc.code", int(code)))
			return
		}
		sc.endWithSummaryAttr(scopeCtx, 0, slog.Int("grpc.code", 0))
	}()

	return handler(scopeCtx, req)
}

// InvokeGRPCStream wraps a gRPC streaming server handler in a flight-recorder
// Scope without requiring google.golang.org/grpc as a dependency.
func InvokeGRPCStream(
	ctx context.Context,
	rec *Recorder,
	fullMethod string,
	extractTraceID GRPCMetadataExtractor,
	handler GRPCStreamHandler,
) (err error) {
	traceID := resolveGRPCTraceID(ctx, rec.state, extractTraceID)
	scopeCtx, sc := StartScope(ctx, rec, ScopeMeta{
		TraceID: traceID,
		Method:  "gRPC",
		Path:    fullMethod,
	})

	defer func() {
		if p := recover(); p != nil {
			sc.Trigger("panic")
			sc.endWithSummaryAttr(scopeCtx, 0, slog.Int("grpc.code", 2))
			panic(p)
		}
		if err != nil {
			code, isServerFault := classifyGRPCError(err)
			if isServerFault {
				sc.Trigger("grpc_error")
			}
			sc.endWithSummaryAttr(scopeCtx, 0, slog.Int("grpc.code", int(code)))
			return
		}
		sc.endWithSummaryAttr(scopeCtx, 0, slog.Int("grpc.code", 0))
	}()

	return handler(scopeCtx)
}

func resolveGRPCTraceID(ctx context.Context, st *recorderState, extractTraceID GRPCMetadataExtractor) string {
	if st.opts.IgnoreRequestTraceIDs {
		// Extractors read incoming metadata, which the client controls.
		return TraceID(ctx)
	}
	if st.extractTraceContext(ctx).HasTraceID() {
		// A trace in the context (an OpenTelemetry stats handler or
		// interceptor in front) wins over metadata; StartScope takes it from
		// ctx, unless a parent scope's ID applies.
		return ""
	}
	if extractTraceID != nil {
		if tid := extractTraceID(ctx); validTraceID(tid) {
			return tid
		}
	}
	if ctx != nil {
		if md, ok := ctx.Value(incomingMetadataContextKey{}).(map[string][]string); ok {
			if tid := ExtractTraceIDFromMetadata(md); tid != "" {
				return tid
			}
		}
	}
	return TraceID(ctx)
}

// classifyGRPCError returns the gRPC status code and whether the error
// represents a server-side fault that should flush the flight-recorder buffer.
func classifyGRPCError(err error) (code uint32, isServerFault bool) {
	if c, ok := extractGRPCStatusCode(err); ok {
		return c, isGRPCServerFaultCode(c)
	}
	if errors.Is(err, context.Canceled) {
		return 1, false // codes.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return 4, true // codes.DeadlineExceeded
	}
	return 2, true // codes.Unknown
}

func isGRPCServerFaultCode(code uint32) bool {
	switch code {
	case 2, // Unknown
		4,  // DeadlineExceeded
		12, // Unimplemented
		13, // Internal
		14, // Unavailable
		15: // DataLoss
		return true
	default:
		return false
	}
}

func extractGRPCStatusCode(err error) (uint32, bool) {
	for cur := err; cur != nil; cur = errors.Unwrap(cur) {
		rv := reflect.ValueOf(cur)
		if !rv.IsValid() {
			continue
		}
		m := rv.MethodByName("GRPCStatus")
		if !m.IsValid() || m.Type().NumIn() != 0 || m.Type().NumOut() != 1 {
			continue
		}
		out := m.Call(nil)
		if len(out) != 1 || !out[0].IsValid() {
			continue
		}
		st := out[0]
		if (st.Kind() == reflect.Pointer || st.Kind() == reflect.Interface) && st.IsNil() {
			return 0, true
		}
		cm := st.MethodByName("Code")
		if !cm.IsValid() || cm.Type().NumIn() != 0 || cm.Type().NumOut() != 1 {
			continue
		}
		cout := cm.Call(nil)
		if len(cout) != 1 || !cout[0].IsValid() {
			continue
		}
		switch cout[0].Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return uint32(cout[0].Uint()), true
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return uint32(cout[0].Int()), true
		}
	}
	return 0, false
}
