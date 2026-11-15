// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package grpcflightrec provides gRPC interceptors for flightrec in a separate
// Go module, so that the core flightrec module stays free of external
// dependencies.
//
// Server interceptors open a flightrec Scope per RPC and, when the scope is
// triggered, set the TrailerKey trailer on the response. Client interceptors
// read that trailer and trigger the caller's scope, so a failure in a service
// you call keeps your logs for the same request too.
package grpcflightrec

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/divine-ai-com/flightrec"
)

// TrailerKey is the response trailer a triggered scope sets, the gRPC
// counterpart of flightrec.FlushHeader. Its value is a built-in flush reason.
const TrailerKey = "flightrec-flush"

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
		var sc *flightrec.Scope
		resp, err := flightrec.InvokeGRPCUnary(
			ctx,
			rec,
			info.FullMethod,
			extractTraceID,
			req,
			func(ctx context.Context, req any) (any, error) {
				sc = flightrec.ScopeFromContext(ctx)
				return handler(ctx, req)
			},
		)
		sc.SendFlushMarker(func(value string) error {
			return grpc.SetTrailer(ctx, metadata.Pairs(TrailerKey, value))
		})
		return resp, err
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
		var sc *flightrec.Scope
		err := flightrec.InvokeGRPCStream(
			ss.Context(),
			rec,
			info.FullMethod,
			extractTraceID,
			func(ctx context.Context) error {
				sc = flightrec.ScopeFromContext(ctx)
				return handler(srv, &scopedServerStream{
					ServerStream: ss,
					ctx:          ctx,
				})
			},
		)
		sc.SendFlushMarker(func(value string) error {
			ss.SetTrailer(metadata.Pairs(TrailerKey, value))
			return nil
		})
		return err
	}
}

// UnaryClientInterceptor returns a grpc.UnaryClientInterceptor that reads the
// TrailerKey trailer from each response. A built-in value triggers the scope
// in the call's context with reason "downstream:<value>"; anything else is
// ignored and counted in Stats.MarkersRejected (see flightrec.ReceiveMarker).
//
// Only use it on connections to services you operate.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var trailer metadata.MD
		err := invoker(ctx, method, req, reply, cc, append(opts, grpc.Trailer(&trailer))...)
		flightrec.ReceiveMarker(ctx, trailer.Get(TrailerKey))
		return err
	}
}

// StreamClientInterceptor returns a grpc.StreamClientInterceptor that reads
// the TrailerKey trailer when a stream ends and applies it like
// UnaryClientInterceptor does.
func StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &markerClientStream{ClientStream: cs, ctx: ctx}, nil
	}
}

// markerClientStream applies the flush trailer once the stream has ended,
// which is when RecvMsg first returns an error (io.EOF on success).
type markerClientStream struct {
	grpc.ClientStream
	ctx  context.Context
	once sync.Once
}

func (s *markerClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		s.once.Do(func() {
			flightrec.ReceiveMarker(s.ctx, s.ClientStream.Trailer().Get(TrailerKey))
		})
	}
	return err
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
