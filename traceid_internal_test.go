// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"context"
	"github.com/divine-ai-com/flightrec/internal/traceid"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExtractHTTPTraceIDLimits(t *testing.T) {
	long := strings.Repeat("a", traceid.MaxLen)
	for _, tc := range []struct {
		name      string
		traceIDs  []string
		requestID string
		want      string
	}{
		{"plain request ID", nil, "req-1041", "req-1041"},
		{"maximum length", []string{long}, "", long},
		{"too long falls through", []string{long + "a"}, "req-1", "req-1"},
		{"invalid characters fall through", []string{"a b"}, "req-2", "req-2"},
		{"first valid repeated value wins", []string{"bad value", "trace-3"}, "", "trace-3"},
		{"request ID middleware format", nil, "api-7f9c/AbCdEf1234-000042", "api-7f9c/AbCdEf1234-000042"},
		{"base64", []string{"q83vEjRWeJA+/w=="}, "", "q83vEjRWeJA+/w=="},
		{"control characters", []string{"id\x00x"}, "", ""},
		{"non-ASCII", []string{"trace-é"}, "", ""},
		{"JSON breaking", []string{`x","admin":true`}, "", ""},
		{"trimmed", []string{"  trace-9  "}, "", "trace-9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			if tc.traceIDs != nil {
				h["X-Trace-Id"] = tc.traceIDs
			}
			if tc.requestID != "" {
				h["X-Request-Id"] = []string{tc.requestID}
			}
			if got := extractHTTPTraceID(h); got != tc.want {
				t.Errorf("extractHTTPTraceID = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestExtractTraceIDFromMetadataLimits(t *testing.T) {
	md := map[string][]string{
		"x-trace-id":   {strings.Repeat("z", traceid.MaxLen+1)},
		"x-request-id": {"bad value", "req-77"},
	}
	if got := ExtractTraceIDFromMetadata(md); got != "req-77" {
		t.Errorf("ExtractTraceIDFromMetadata = %q, want req-77", got)
	}
}

func TestMiddlewareTraceIDSource(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opts   Options
		header string
		want   func(string) bool
	}{
		{"valid header is used", Options{}, "req-1041", func(id string) bool { return id == "req-1041" }},
		{"invalid header is replaced by a generated ID", Options{}, "a b c", isGeneratedID},
		{"IgnoreRequestTraceIDs ignores a valid header", Options{IgnoreRequestTraceIDs: true}, "req-1041", isGeneratedID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.opts.Next = slog.NewJSONHandler(io.Discard, nil)
			rec := New(tc.opts)
			var got string
			h := HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = TraceID(r.Context())
			}))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("X-Request-Id", tc.header)
			h.ServeHTTP(httptest.NewRecorder(), req)
			if !tc.want(got) {
				t.Errorf("trace ID = %q", got)
			}
		})
	}
}

func TestGRPCTraceIDSource(t *testing.T) {
	st := func(o Options) *recorderState { o.Next = slog.DiscardHandler; return New(o).state }
	plain, ignore := st(Options{}), st(Options{IgnoreRequestTraceIDs: true})
	ctx := WithIncomingMetadata(context.Background(), map[string][]string{"x-request-id": {"req-9"}})
	if got := resolveGRPCTraceID(ctx, plain, nil); got != "req-9" {
		t.Errorf("metadata trace ID = %q, want req-9", got)
	}
	if got := resolveGRPCTraceID(ctx, ignore, nil); got != "" {
		t.Errorf("with ignore set, trace ID = %q, want empty (generated later)", got)
	}
	custom := func(context.Context) string { return "custom-id" }
	if got := resolveGRPCTraceID(ctx, ignore, custom); got != "" {
		t.Errorf("with ignore set, a custom extractor was used: %q", got)
	}
	bad := func(context.Context) string { return "has space" }
	if got := resolveGRPCTraceID(ctx, plain, bad); got != "req-9" {
		t.Errorf("invalid custom extractor result was used: %q", got)
	}
	// An active span wins over metadata: StartScope takes the span's trace.
	span := st(Options{TraceExtractor: func(context.Context) TraceContext { return TraceContext{TraceID: [16]byte{7}} }})
	if got := resolveGRPCTraceID(ctx, span, custom); got != "" {
		t.Errorf("with an active span, trace ID = %q, want empty (taken from the span)", got)
	}
}

// TestActiveSpanBeatsRequestHeaders checks that HTTPMiddleware files the
// scope under the active span's trace, not under a request header such as
// the X-Request-Id a proxy adds.
func TestActiveSpanBeatsRequestHeaders(t *testing.T) {
	spanTC := TraceContext{TraceID: [16]byte{0xab, 1}}
	rec := New(Options{Next: slog.DiscardHandler, TraceExtractor: func(context.Context) TraceContext { return spanTC }})
	var got string
	h := HTTPMiddleware(rec)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = TraceID(r.Context()) }))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-Id", "lb-req-1")
	req.Header.Set("Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	h.ServeHTTP(httptest.NewRecorder(), req)
	if got != spanTC.TraceIDHex() {
		t.Errorf("trace ID = %q, want the active span's %q", got, spanTC.TraceIDHex())
	}
}

func isGeneratedID(id string) bool {
	if len(id) != 32 {
		return false
	}
	_, ok := ParseTraceIDHex(id)
	return ok
}

// FuzzExtractHTTPTraceID checks that the trace ID is empty or one of the
// inputs, within the limits, and that a valid X-Trace-Id beats X-Request-Id.
func FuzzExtractHTTPTraceID(f *testing.F) {
	f.Add("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "t-1", "r-1")
	f.Add("", "a b", "\x00")
	f.Add("00-00000000000000000000000000000000-00f067aa0ba902b7-01", strings.Repeat("x", 200), "ok")
	f.Fuzz(func(t *testing.T, traceparent, traceID, requestID string) {
		h := http.Header{}
		h["Traceparent"] = []string{traceparent}
		h["X-Trace-Id"] = []string{traceID}
		h["X-Request-Id"] = []string{requestID}
		got := extractHTTPTraceID(h)
		if got == "" {
			return
		}
		if !traceid.Valid(got) {
			t.Fatalf("result %q is outside the limits", got)
		}
		tp := traceid.ParseTraceparent(strings.TrimSpace(traceparent))
		tid, rid := strings.TrimSpace(traceID), strings.TrimSpace(requestID)
		switch {
		case tp != "":
			if got != tp {
				t.Fatalf("got %q, want traceparent ID %q", got, tp)
			}
		case traceid.Valid(tid):
			if got != tid {
				t.Fatalf("got %q, want X-Trace-Id %q", got, tid)
			}
		default:
			if got != rid {
				t.Fatalf("got %q, want X-Request-Id %q", got, rid)
			}
		}
	})
}

// FuzzExtractTraceIDFromMetadata applies the same property to gRPC metadata.
func FuzzExtractTraceIDFromMetadata(f *testing.F) {
	f.Add("t-1", "r-1")
	f.Add("bad value", strings.Repeat("y", 300))
	f.Fuzz(func(t *testing.T, traceID, requestID string) {
		got := ExtractTraceIDFromMetadata(map[string][]string{
			"x-trace-id":   {traceID},
			"x-request-id": {requestID},
		})
		if got == "" {
			return
		}
		if !traceid.Valid(got) || (got != strings.TrimSpace(traceID) && got != strings.TrimSpace(requestID)) {
			t.Fatalf("result %q is not a valid input value", got)
		}
	})
}
