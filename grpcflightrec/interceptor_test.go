// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package grpcflightrec_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/divine-ai-com/flightrec"
	"github.com/divine-ai-com/flightrec/grpcflightrec"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func parseLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	raw := strings.TrimSpace(buf.String())
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, "\n")
	out := make([]map[string]any, 0, len(parts))
	for _, p := range parts {
		var m map[string]any
		if err := json.Unmarshal([]byte(p), &m); err != nil {
			t.Fatalf("unmarshal %q: %v", p, err)
		}
		out = append(out, m)
	}
	return out
}

func TestUnaryServerInterceptor(t *testing.T) {
	t.Run("client error NotFound does not flush and extracts metadata trace ID", func(t *testing.T) {
		var buf bytes.Buffer
		rec := flightrec.New(flightrec.Options{
			Next:          slog.NewJSONHandler(&buf, nil),
			PassLeveler:   slog.LevelWarn,
			BufferLeveler: slog.LevelDebug,
		})
		logger := slog.New(rec)
		ic := grpcflightrec.UnaryServerInterceptor(rec)

		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
			"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		))
		_, err := ic(
			ctx,
			"req",
			&grpc.UnaryServerInfo{FullMethod: "/orders.Service/GetOrder"},
			func(ctx context.Context, req any) (any, error) {
				logger.DebugContext(ctx, "checked order cache", "req", req)
				return nil, status.Error(codes.NotFound, "order not found")
			},
		)
		if status.Code(err) != codes.NotFound {
			t.Fatalf("expected NotFound, got %v", err)
		}

		lines := parseLines(t, &buf)
		if len(lines) != 1 {
			t.Fatalf("expected 1 summary line, got %+v", lines)
		}
		if lines[0]["msg"] != "request completed" ||
			lines[0]["grpc.code"] != float64(codes.NotFound) ||
			lines[0]["flightrec.trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Fatalf("unexpected summary line: %+v", lines[0])
		}
	})

	t.Run("server error Internal flushes buffered records", func(t *testing.T) {
		var buf bytes.Buffer
		rec := flightrec.New(flightrec.Options{
			Next:          slog.NewJSONHandler(&buf, nil),
			PassLeveler:   slog.LevelWarn,
			BufferLeveler: slog.LevelDebug,
		})
		logger := slog.New(rec)
		ic := grpcflightrec.UnaryServerInterceptor(rec)

		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
			"x-trace-id", "unary-err-01",
		))
		_, err := ic(
			ctx,
			"req",
			&grpc.UnaryServerInfo{FullMethod: "/orders.Service/CreateOrder"},
			func(ctx context.Context, req any) (any, error) {
				logger.DebugContext(ctx, "validating order")
				return nil, status.Error(codes.Internal, "database unreachable")
			},
		)
		if status.Code(err) != codes.Internal {
			t.Fatalf("expected Internal, got %v", err)
		}

		lines := parseLines(t, &buf)
		if len(lines) != 1 ||
			lines[0]["msg"] != "validating order" ||
			lines[0]["flightrec.flush_reason"] != "grpc_error" ||
			lines[0]["flightrec.trace_id"] != "unary-err-01" {
			t.Fatalf("expected flushed debug log, got %+v", lines)
		}
	})
}

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context {
	return f.ctx
}

func TestStreamServerInterceptor(t *testing.T) {
	var buf bytes.Buffer
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(&buf, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})
	logger := slog.New(rec)
	ic := grpcflightrec.StreamServerInterceptor(rec)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-request-id", "stream-trace-77",
	))
	ss := &fakeServerStream{ctx: ctx}

	err := ic(
		nil,
		ss,
		&grpc.StreamServerInfo{FullMethod: "/orders.Service/StreamOrders"},
		func(srv any, stream grpc.ServerStream) error {
			logger.DebugContext(stream.Context(), "sent stream batch 1")
			return status.Error(codes.Unavailable, "upstream closed")
		},
	)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}

	lines := parseLines(t, &buf)
	if len(lines) != 1 ||
		lines[0]["msg"] != "sent stream batch 1" ||
		lines[0]["flightrec.flush_reason"] != "grpc_error" ||
		lines[0]["flightrec.trace_id"] != "stream-trace-77" {
		t.Fatalf("expected flushed stream debug log, got %+v", lines)
	}
}
