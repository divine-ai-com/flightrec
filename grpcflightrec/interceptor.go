// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package grpcflightrec provides gRPC interceptors for flightrec in a separate
// Go module, so that the core flightrec module stays free of external
// dependencies.
//
// Server interceptors open a flightrec Scope per RPC and, when the scope is
// triggered, set the FlushTrailer trailer on the response if
// Options.EnableFlushMarker is set. Client interceptors
// read that trailer and trigger the caller's scope, so a failure in a service
// you call keeps your logs for the same request too.
package grpcflightrec

import (
	"context"
	"slices"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/haawklabs/flightrec"
)

// FlushTrailer is the response trailer a triggered scope sets, the gRPC
// counterpart of flightrec.FlushHeader. Its value is a built-in flush reason.
const FlushTrailer = "flightrec-flush"

// ClientOption configures UnaryClientInterceptor and StreamClientInterceptor.
// No options are defined yet; the parameter exists so they can be added
// without changing the function signatures.
type ClientOption interface {
	applyClient(*clientConfig)
}

// clientConfig holds settings from ClientOption values. It is empty until the
// first option is added.
type clientConfig struct{}

func newClientConfig(opts []ClientOption) clientConfig {
	var cfg clientConfig
	for _, o := range opts {
		o.applyClient(&cfg)
	}
	return cfg
}

// UnaryServerInterceptor returns a grpc.UnaryServerInterceptor that opens a
// flightrec Scope for each unary RPC, takes the trace ID from the context's
// OpenTelemetry trace or, with Options.TrustRequestTraceIDs, from incoming gRPC
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
		// Deferred so the trailer is also set when the handler panics and an
		// outer recovery interceptor turns the panic into a status.
		defer func() {
			sc.SendFlushMarker(func(value string) error {
				return grpc.SetTrailer(ctx, metadata.Pairs(FlushTrailer, value))
			})
		}()
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
		return resp, err
	}
}

// StreamServerInterceptor returns a grpc.StreamServerInterceptor that opens a
// flightrec Scope for each streaming RPC, enriches stream.Context() with the
// scope, takes the trace ID as UnaryServerInterceptor does, and flushes the
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
		// Deferred so the trailer is also set when the handler panics. grpc
		// may still drop it if the stream already ended, for example on
		// cancellation; it is counted as sent either way.
		defer func() {
			sc.SendFlushMarker(func(value string) error {
				ss.SetTrailer(metadata.Pairs(FlushTrailer, value))
				return nil
			})
		}()
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
		return err
	}
}

// UnaryClientInterceptor returns a grpc.UnaryClientInterceptor that reads the
// FlushTrailer trailer from each response. A built-in value triggers the scope
// in the call's context with reason "downstream:<value>"; anything else is
// ignored and counted in Stats.MarkersRejected (see flightrec.ReceiveMarker).
//
// Only use it on connections to services you operate.
func UnaryClientInterceptor(opts ...ClientOption) grpc.UnaryClientInterceptor {
	_ = newClientConfig(opts)
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var trailer metadata.MD
		// A new slice, so the caller's backing array is never written.
		err := invoker(ctx, method, req, reply, cc, slices.Concat(opts, []grpc.CallOption{grpc.Trailer(&trailer)})...)
		flightrec.ReceiveMarker(ctx, trailer.Get(FlushTrailer))
		return err
	}
}

// StreamClientInterceptor returns a grpc.StreamClientInterceptor that reads
// the FlushTrailer trailer when a stream ends and applies it like
// UnaryClientInterceptor does. The stream ends when RecvMsg returns an error
// (io.EOF on success), or after the single response of a client-streaming
// RPC. A stream the caller stops reading early, or cancels, never applies the
// trailer.
func StreamClientInterceptor(opts ...ClientOption) grpc.StreamClientInterceptor {
	_ = newClientConfig(opts)
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
		return &markerClientStream{ClientStream: cs, ctx: ctx, serverStreams: desc.ServerStreams}, nil
	}
}

// markerClientStream applies the flush trailer once the stream has ended:
// when RecvMsg first returns an error (io.EOF on success), or, for an RPC
// without server streaming, after its single response, by which time grpc
// has already read the trailer.
type markerClientStream struct {
	grpc.ClientStream
	ctx           context.Context
	serverStreams bool
	once          sync.Once
}

func (s *markerClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil || !s.serverStreams {
		s.once.Do(func() {
			flightrec.ReceiveMarker(s.ctx, s.ClientStream.Trailer().Get(FlushTrailer))
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
