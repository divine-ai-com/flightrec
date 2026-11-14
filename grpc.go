// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"context"
)

// GRPCMetadataExtractor extracts a trace ID from a gRPC request context
// (for example via google.golang.org/grpc/metadata.FromIncomingContext).
type GRPCMetadataExtractor func(ctx context.Context) string

// GRPCUnaryHandler matches the handler function passed to a gRPC unary
// server interceptor.
type GRPCUnaryHandler func(ctx context.Context, req any) (any, error)

// GRPCUnaryInterceptor wraps a gRPC unary handler in a flight-recorder Scope
// without requiring google.golang.org/grpc as a dependency of flightrec itself.
//
// It can be adapted to grpc.UnaryServerInterceptor in one line:
//
//	grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
//	    return flightrec.InvokeGRPCUnary(ctx, rec, info.FullMethod, nil, req, handler)
//	})
func InvokeGRPCUnary(
	ctx context.Context,
	rec *Recorder,
	fullMethod string,
	extractTraceID GRPCMetadataExtractor,
	req any,
	handler GRPCUnaryHandler,
) (resp any, err error) {
	var traceID string
	if extractTraceID != nil {
		traceID = extractTraceID(ctx)
	}
	scopeCtx, sc := StartScope(ctx, rec, ScopeMeta{
		TraceID: traceID,
		Method:  "gRPC",
		Path:    fullMethod,
	})

	defer func() {
		if p := recover(); p != nil {
			sc.Trigger("panic")
			sc.End(scopeCtx, 500)
			panic(p)
		}
		if err != nil {
			sc.Trigger("grpc_error")
			sc.End(scopeCtx, 500)
			return
		}
		sc.End(scopeCtx, 200)
	}()

	return handler(scopeCtx, req)
}
