// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
)

// FlushHeader is the response header a triggered scope sets so that callers
// can flush their own buffers for the same request. Its value is the flush
// reason. See Transport.
//
// HTTPMiddleware removes FlushHeader from incoming requests before the handler
// runs: only responses from services you call can trigger a flush, never a
// request from a client.
const FlushHeader = "Flightrec-Flush"

// maxFlushReasonLen bounds the length of the FlushHeader value.
const maxFlushReasonLen = 64

// HTTPMiddleware wraps an http.Handler and opens a flight-recorder Scope for
// every incoming request.
//
// When the scope is triggered before the response headers are written, or
// the handler writes a status of 500 or above, the response carries
// FlushHeader with the flush reason. A scope triggered after the headers are
// written cannot add it; Stats.FlushMarkersMissed counts those.
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
			r.Header.Del(FlushHeader)
			rw, sw := wrapResponseWriter(w, sc)

			defer func() {
				if p := recover(); p != nil {
					if err, ok := p.(error); ok && errors.Is(err, http.ErrAbortHandler) {
						sc.End(ctx, sw.status)
						panic(p)
					}
					sc.Trigger("panic")
					sc.End(ctx, http.StatusInternalServerError)
					panic(p)
				}
				sw.finish()
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
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
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
	scope       *Scope
	status      int
	wroteHeader bool
	markerSent  bool
	hijacked    bool
}

// beforeHeaders runs once, just before the final response headers are sent.
// It triggers the scope for a 5xx status and sets FlushHeader when the scope
// is triggered.
func (w *statusResponseWriter) beforeHeaders(code int) {
	w.status = code
	w.wroteHeader = true
	if w.scope == nil {
		return
	}
	if code >= 500 {
		w.scope.Trigger(httpStatusReason(code))
	}
	if triggered, reason := w.scope.TriggerState(); triggered {
		w.Header().Set(FlushHeader, sanitizeFlushReason(reason))
		w.markerSent = true
		w.scope.state.flushMarkersSent.Add(1)
	}
}

// finish runs after the handler returns normally. If the handler wrote
// nothing, net/http sends the headers after this point, so the marker can
// still be added. Otherwise a triggered scope without a marker is counted as
// missed.
func (w *statusResponseWriter) finish() {
	if w.scope == nil || w.hijacked {
		return
	}
	if !w.wroteHeader {
		w.beforeHeaders(w.status)
		return
	}
	if triggered, _ := w.scope.TriggerState(); triggered && !w.markerSent {
		w.scope.state.flushMarkersMissed.Add(1)
	}
}

func (w *statusResponseWriter) WriteHeader(code int) {
	// Informational responses (1xx except 101 Switching Protocols) are sent
	// ahead of the final response and do not fix its status or headers.
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if !w.wroteHeader {
		w.beforeHeaders(code)
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.beforeHeaders(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// sanitizeFlushReason limits a flush reason to characters that are safe in a
// header value and to maxFlushReasonLen bytes.
func sanitizeFlushReason(reason string) string {
	if len(reason) > maxFlushReasonLen {
		reason = reason[:maxFlushReasonLen]
	}
	b := []byte(reason)
	for i, c := range b {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '_', c == '-', c == '.', c == ':':
		default:
			b[i] = '_'
		}
	}
	if len(b) == 0 {
		return "triggered"
	}
	return string(b)
}

func (w *statusResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *statusResponseWriter) flush() {
	f, ok := w.ResponseWriter.(http.Flusher)
	if !ok {
		return
	}
	// Flushing before any write sends a 200 status with the current headers.
	if !w.wroteHeader {
		w.beforeHeaders(http.StatusOK)
	}
	f.Flush()
}

func (w *statusResponseWriter) hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		conn, rw, err := hj.Hijack()
		if err == nil {
			w.hijacked = true
		}
		return conn, rw, err
	}
	return nil, nil, http.ErrNotSupported
}

func (w *statusResponseWriter) readFrom(r io.Reader) (int64, error) {
	if !w.wroteHeader {
		w.beforeHeaders(http.StatusOK)
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

func wrapResponseWriter(w http.ResponseWriter, sc *Scope) (http.ResponseWriter, *statusResponseWriter) {
	base := statusResponseWriter{
		ResponseWriter: w,
		scope:          sc,
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
