// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"bufio"
	"io"
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
// handler panics (except http.ErrAbortHandler), the duration exceeds SlowAfter,
// or Flush(ctx, reason) is called explicitly.
func HTTPMiddleware(rec *Recorder) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			traceID := extractHTTPTraceID(r.Header)
			ctx, sc := StartScope(r.Context(), rec, ScopeMeta{
				TraceID: traceID,
				Method:  r.Method,
				Path:    r.URL.Path,
			})
			rw, sw := wrapResponseWriter(w)

			defer func() {
				if p := recover(); p != nil {
					if p == http.ErrAbortHandler {
						sc.End(ctx, sw.status)
						panic(p)
					}
					sc.Trigger("panic")
					sc.End(ctx, http.StatusInternalServerError)
					panic(p)
				}
				sc.End(ctx, sw.status)
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

func (w *statusResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *statusResponseWriter) flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusResponseWriter) hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (w *statusResponseWriter) readFrom(r io.Reader) (int64, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	return w.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
}

type rwFlusher struct{ statusResponseWriter }

func (w *rwFlusher) Flush() { w.flush() }

type rwHijacker struct{ statusResponseWriter }

func (w *rwHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) { return w.hijack() }

type rwReaderFrom struct{ statusResponseWriter }

func (w *rwReaderFrom) ReadFrom(r io.Reader) (int64, error) { return w.readFrom(r) }

type rwFlusherHijacker struct{ statusResponseWriter }

func (w *rwFlusherHijacker) Flush()                                       { w.flush() }
func (w *rwFlusherHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) { return w.hijack() }

type rwFlusherReaderFrom struct{ statusResponseWriter }

func (w *rwFlusherReaderFrom) Flush()                              { w.flush() }
func (w *rwFlusherReaderFrom) ReadFrom(r io.Reader) (int64, error) { return w.readFrom(r) }

type rwHijackerReaderFrom struct{ statusResponseWriter }

func (w *rwHijackerReaderFrom) Hijack() (net.Conn, *bufio.ReadWriter, error) { return w.hijack() }
func (w *rwHijackerReaderFrom) ReadFrom(r io.Reader) (int64, error)          { return w.readFrom(r) }

type rwFlusherHijackerReaderFrom struct{ statusResponseWriter }

func (w *rwFlusherHijackerReaderFrom) Flush() { w.flush() }
func (w *rwFlusherHijackerReaderFrom) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.hijack()
}
func (w *rwFlusherHijackerReaderFrom) ReadFrom(r io.Reader) (int64, error) { return w.readFrom(r) }

func wrapResponseWriter(w http.ResponseWriter) (http.ResponseWriter, *statusResponseWriter) {
	base := statusResponseWriter{
		ResponseWriter: w,
		status:         http.StatusOK,
	}
	_, hasFlusher := w.(http.Flusher)
	_, hasHijacker := w.(http.Hijacker)
	_, hasReaderFrom := w.(io.ReaderFrom)

	switch {
	case hasFlusher && hasHijacker && hasReaderFrom:
		v := &rwFlusherHijackerReaderFrom{base}
		return v, &v.statusResponseWriter
	case hasFlusher && hasHijacker:
		v := &rwFlusherHijacker{base}
		return v, &v.statusResponseWriter
	case hasFlusher && hasReaderFrom:
		v := &rwFlusherReaderFrom{base}
		return v, &v.statusResponseWriter
	case hasHijacker && hasReaderFrom:
		v := &rwHijackerReaderFrom{base}
		return v, &v.statusResponseWriter
	case hasFlusher:
		v := &rwFlusher{base}
		return v, &v.statusResponseWriter
	case hasHijacker:
		v := &rwHijacker{base}
		return v, &v.statusResponseWriter
	case hasReaderFrom:
		v := &rwReaderFrom{base}
		return v, &v.statusResponseWriter
	default:
		v := &base
		return v, v
	}
}
