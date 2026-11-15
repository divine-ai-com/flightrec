// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"net/http"
	"strings"
	"testing"
)

func TestExtractHTTPTraceIDLimits(t *testing.T) {
	long := strings.Repeat("a", maxTraceIDLen)
	for _, tc := range []struct {
		name      string
		traceID   string
		requestID string
		want      string
	}{
		{"plain request ID", "", "req-1041", "req-1041"},
		{"maximum length", long, "", long},
		{"too long falls through", long + "a", "req-1", "req-1"},
		{"invalid characters fall through", "a b", "req-2", "req-2"},
		{"control characters", "id\x00x", "", ""},
		{"non-ASCII", "trace-é", "", ""},
		{"JSON breaking", `x","admin":true`, "", ""},
		{"trimmed", "  trace-9  ", "", "trace-9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			if tc.traceID != "" {
				h["X-Trace-Id"] = []string{tc.traceID}
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
		"x-trace-id":   {strings.Repeat("z", maxTraceIDLen+1)},
		"x-request-id": {"bad value", "req-77"},
	}
	if got := ExtractTraceIDFromMetadata(md); got != "req-77" {
		t.Errorf("ExtractTraceIDFromMetadata = %q, want req-77", got)
	}
}

// FuzzExtractHTTPTraceID checks that whatever a client sends, the trace ID is
// either empty (a generated one is used) or within the documented limits.
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
		if got != "" && !validTraceID(got) {
			t.Fatalf("extractHTTPTraceID returned %q, outside the limits", got)
		}
	})
}
