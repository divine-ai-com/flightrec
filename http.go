// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"bufio"
	"net"
	"net/http"
	"strings"
)

// HTTPMiddleware wraps an http.Handler and opens a flight-recorder Scope for
// every incoming request.
//
// The scope is keyed by the request's trace ID (extracted from W3C traceparent,
// X-Trace-Id, or X-Request-Id headers, or generated when absent). The buffer
// is flushed if an error log is recorded, the response status is 5xx, the
// handler panics, the duration exceeds SlowAfter, or Flush(ctx, reason) is
// called explicitly.
func HTTPMiddleware(rec *Recorder) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			traceID := extractHTTPTraceID(r.Header)
			ctx, sc := StartScope(r.Context(), rec, ScopeMeta{
				TraceID: traceID,
				Method:  r.Method,
				Path:    r.URL.Path,
			})
			rw := &statusResponseWriter{
				ResponseWriter: w,
				status:         http.StatusOK,
			}

			defer func() {
				if p := recover(); p != nil {
					sc.Trigger("panic")
					sc.End(ctx, http.StatusInternalServerError)
					panic(p)
				}
				sc.End(ctx, rw.status)
			}()

			next.ServeHTTP(rw, r.WithContext(ctx))
		})
	}
}

func extractHTTPTraceID(h http.Header) string {
	if tp := strings.TrimSpace(h.Get("Traceparent")); tp != "" {
		if tid := parseW3CTraceparent(tp); tid != "" {
			return tid
		}
	}
	if tid := strings.TrimSpace(h.Get("X-Trace-Id")); tid != "" {
		return tid
	}
	if rid := strings.TrimSpace(h.Get("X-Request-Id")); rid != "" {
		return rid
	}
	return ""
}

// parseW3CTraceparent extracts the 32-hex-char trace-id from a W3C traceparent
// header ("00-<32hex>-<16hex>-<2hex>").
func parseW3CTraceparent(tp string) string {
	parts := strings.Split(tp, "-")
	if len(parts) < 4 || len(parts[1]) != 32 {
		return ""
	}
	tid := parts[1]
	allZero := true
	for i := 0; i < len(tid); i++ {
		c := tid[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return ""
		}
		if c != '0' {
			allZero = false
		}
	}
	if allZero {
		return ""
	}
	return strings.ToLower(tid)
}

type statusResponseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (w *statusResponseWriter) Push(target string, opts *http.PushOptions) error {
	if p, ok := w.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (w *statusResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
