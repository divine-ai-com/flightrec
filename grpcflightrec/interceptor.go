// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package grpcflightrec provides gRPC unary and stream server interceptors for
// flightrec in a separate Go module so that the core flightrec module remains
// free of external dependencies.
package grpcflightrec

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/divine-ai-com/flightrec"
)

// UnaryServerInterceptor returns a grpc.UnaryServerInterceptor that opens a
// flightrec Scope for each unary RPC, extracts the trace ID from incoming gRPC
// metadata ("traceparent", "x-trace-id", or "x-request-id"), and flushes the
// buffer only on server-side faults (Unknown, DeadlineExceeded, Unimplemented,
// Internal, Unavailable, DataLoss), error logs, panics, slow RPCs, or explicit
// calls to flightrec.Flush.
func UnaryServerInterceptor(rec *flightrec.Recorder) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		return flightrec.InvokeGRPCUnary(
			ctx,
			rec,
			info.FullMethod,
			extractTraceID,
			req,
			flightrec.GRPCUnaryHandler(handler),
		)
	}
}

// StreamServerInterceptor returns a grpc.StreamServerInterceptor that opens a
// flightrec Scope for each streaming RPC, enriches stream.Context() with the
// scope, extracts the trace ID from incoming gRPC metadata, and flushes the
// buffer only on server-side faults, error logs, panics, slow streams, or
// explicit calls to flightrec.Flush.
func StreamServerInterceptor(rec *flightrec.Recorder) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		return flightrec.InvokeGRPCStream(
			ss.Context(),
			rec,
			info.FullMethod,
			extractTraceID,
			func(ctx context.Context) error {
				return handler(srv, &scopedServerStream{
					ServerStream: ss,
					ctx:          ctx,
				})
			},
		)
	}
}

func extractTraceID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	return flightrec.ExtractTraceIDFromMetadata(md)
}

type scopedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *scopedServerStream) Context() context.Context {
	return s.ctx
}
