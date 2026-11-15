// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package grpcflightrec_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/divine-ai-com/flightrec"
	"github.com/divine-ai-com/flightrec/grpcflightrec"
)

// lockedBuffer is a concurrency-safe log sink.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) lines(t *testing.T) []map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := bytes.NewBuffer(append([]byte(nil), b.buf.Bytes()...))
	return parseLines(t, cp)
}

// testService is a hand-written service with one unary and one server
// streaming method, so the tests need no generated code.
type testService struct {
	unary  func(ctx context.Context) error
	stream func(ctx context.Context, send func() error) error
}

var testServiceDesc = grpc.ServiceDesc{
	ServiceName: "flightrec.test.Service",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Call",
		Handler: func(srv any, ctx context.Context, dec func(any) error, ic grpc.UnaryServerInterceptor) (any, error) {
			in := new(emptypb.Empty)
			if err := dec(in); err != nil {
				return nil, err
			}
			h := func(ctx context.Context, _ any) (any, error) {
				return new(emptypb.Empty), srv.(*testService).unary(ctx)
			}
			if ic == nil {
				return h(ctx, in)
			}
			return ic(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/flightrec.test.Service/Call"}, h)
		},
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Watch",
		ServerStreams: true,
		Handler: func(srv any, ss grpc.ServerStream) error {
			if err := ss.RecvMsg(new(emptypb.Empty)); err != nil {
				return err
			}
			return srv.(*testService).stream(ss.Context(), func() error { return ss.SendMsg(new(emptypb.Empty)) })
		},
	}},
}

// node is one gRPC service with its own recorder.
type node struct {
	rec  *flightrec.Recorder
	log  *slog.Logger
	out  *lockedBuffer
	conn *grpc.ClientConn // connection to this node
}

func startNode(t *testing.T, svc *testService) *node {
	t.Helper()
	n := &node{out: &lockedBuffer{}}
	n.rec = flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(n.out, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})
	n.log = slog.New(n.rec)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(grpcflightrec.UnaryServerInterceptor(n.rec)),
		grpc.StreamInterceptor(grpcflightrec.StreamServerInterceptor(n.rec)),
	)
	srv.RegisterService(&testServiceDesc, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(grpcflightrec.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(grpcflightrec.StreamClientInterceptor()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	n.conn = conn
	return n
}

func call(ctx context.Context, conn *grpc.ClientConn, opts ...grpc.CallOption) error {
	return conn.Invoke(ctx, "/flightrec.test.Service/Call", new(emptypb.Empty), new(emptypb.Empty), opts...)
}

func flushed(t *testing.T, n *node) map[string]string {
	t.Helper()
	got := map[string]string{}
	for _, ln := range n.out.lines(t) {
		if r, ok := ln["flightrec.flush_reason"].(string); ok {
			got[ln["msg"].(string)] = r
		}
	}
	return got
}

func TestUnaryFlushPropagatesThroughThreeServices(t *testing.T) {
	var c, b, a *node
	c = startNode(t, &testService{unary: func(ctx context.Context) error {
		c.log.DebugContext(ctx, "c: reading stock")
		return status.Error(codes.Internal, "database unavailable")
	}})
	b = startNode(t, &testService{unary: func(ctx context.Context) error {
		b.log.DebugContext(ctx, "b: pricing")
		_ = call(ctx, c.conn) // B handles C's failure and succeeds
		return nil
	}})
	a = startNode(t, &testService{unary: func(ctx context.Context) error {
		a.log.DebugContext(ctx, "a: checkout")
		return call(ctx, b.conn)
	}})

	var trailer metadata.MD
	if err := call(context.Background(), a.conn, grpc.Trailer(&trailer)); err != nil {
		t.Fatalf("call to A: %v", err)
	}
	for _, tc := range []struct {
		n          *node
		msg, want  string
		downstream uint64
	}{
		{c, "c: reading stock", "grpc_error", 0},
		{b, "b: pricing", "downstream:grpc_error", 1},
		{a, "a: checkout", "downstream:grpc_error", 1},
	} {
		if got := flushed(t, tc.n)[tc.msg]; got != tc.want {
			t.Errorf("%q flushed with %q, want %q", tc.msg, got, tc.want)
		}
		if got := tc.n.rec.Stats().DownstreamTriggers; got != tc.downstream {
			t.Errorf("%q: DownstreamTriggers = %d, want %d", tc.msg, got, tc.downstream)
		}
	}
	if got := trailer.Get(grpcflightrec.TrailerKey); len(got) != 1 || got[0] != "grpc_error" {
		t.Errorf("A's trailer %s = %v, want [grpc_error]", grpcflightrec.TrailerKey, got)
	}
	if st := c.rec.Stats(); st.FlushMarkersSent != 1 {
		t.Errorf("C FlushMarkersSent = %d, want 1", st.FlushMarkersSent)
	}
}

func TestStreamFlushPropagates(t *testing.T) {
	var down, up *node
	down = startNode(t, &testService{stream: func(ctx context.Context, send func() error) error {
		down.log.DebugContext(ctx, "down: streaming")
		_ = send()
		down.log.ErrorContext(ctx, "down: lost the cursor")
		return nil
	}})
	up = startNode(t, &testService{unary: func(ctx context.Context) error {
		up.log.DebugContext(ctx, "up: reading stream")
		cs, err := down.conn.NewStream(ctx, &testServiceDesc.Streams[0], "/flightrec.test.Service/Watch")
		if err != nil {
			return err
		}
		if err := cs.SendMsg(new(emptypb.Empty)); err != nil {
			return err
		}
		_ = cs.CloseSend()
		for {
			if err := cs.RecvMsg(new(emptypb.Empty)); err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
		}
	}})

	if err := call(context.Background(), up.conn); err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := flushed(t, up)["up: reading stream"]; got != "downstream:error_log" {
		t.Errorf("upstream flushed with %q, want downstream:error_log", got)
	}
}

func TestClientInterceptorRejectsUnknownTrailer(t *testing.T) {
	var down, up *node
	down = startNode(t, &testService{unary: func(ctx context.Context) error {
		_ = grpc.SetTrailer(ctx, metadata.Pairs(grpcflightrec.TrailerKey, "owned"))
		return nil
	}})
	up = startNode(t, &testService{unary: func(ctx context.Context) error {
		up.log.DebugContext(ctx, "up: calling")
		return call(ctx, down.conn)
	}})
	if err := call(context.Background(), up.conn); err != nil {
		t.Fatal(err)
	}
	if len(flushed(t, up)) != 0 {
		t.Errorf("unknown trailer value triggered a flush")
	}
	if got := up.rec.Stats().MarkersRejected; got != 1 {
		t.Errorf("MarkersRejected = %d, want 1", got)
	}
}

func TestServerTrailerDisabled(t *testing.T) {
	out := &lockedBuffer{}
	rec := flightrec.New(flightrec.Options{Next: slog.NewJSONHandler(out, nil), DisableFlushMarker: true})
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnaryInterceptor(grpcflightrec.UnaryServerInterceptor(rec)))
	srv.RegisterService(&testServiceDesc, &testService{unary: func(context.Context) error {
		return status.Error(codes.Internal, "x")
	}})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	var trailer metadata.MD
	_ = call(context.Background(), conn, grpc.Trailer(&trailer))
	if got := trailer.Get(grpcflightrec.TrailerKey); len(got) != 0 {
		t.Errorf("trailer sent with DisableFlushMarker: %v", got)
	}
}
