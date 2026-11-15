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
// can flush their own buffers for the same request.
//
// Its value is a built-in flush reason such as error_log, http_502, panic or
// slow_request. Reasons passed to Flush by application code are sent as
// explicit_flush, so custom reason text never leaves the process. Set
// Options.DisableFlushMarker on services that answer clients outside your
// system.
//
// HTTPMiddleware removes FlushHeader from incoming requests before the handler
// runs, so a handler that proxies requests cannot forward one.
const FlushHeader = "Flightrec-Flush"

// markerReasons are the reasons sent unchanged in FlushHeader. Any other
// reason is sent as explicit_flush.
var markerReasons = map[string]bool{
	"error_log": true, "panic": true, "slow_request": true, "grpc_error": true,
	"http_500": true, "http_502": true, "http_503": true, "http_504": true, "http_5xx": true,
	"span_error": true, "span_exception": true, "slow_trace": true,
	"explicit_flush": true,
}

// ValidMarker reports whether a value received in FlushHeader or the gRPC
// flush trailer is exactly one of the built-in reasons a sender produces with
// MarkerValue. Anything else, including prefixed or empty values, is invalid
// and must be ignored.
func ValidMarker(value string) bool {
	return markerReasons[value]
}

// MarkerValue maps a scope's flush reason to the value an integration sends
// in FlushHeader or the gRPC flush trailer. Prefixes added by cross-service
// flushes (downstream:, remote:) are removed, so the value names the original
// failure and does not grow at each hop. ok reports whether the reason is a
// built-in one; any other reason maps to explicit_flush.
//
// To check a value received from another service, use ValidMarker.
func MarkerValue(reason string) (value string, ok bool) {
	for {
		switch {
		case strings.HasPrefix(reason, "downstream:"):
			reason = reason[len("downstream:"):]
		case strings.HasPrefix(reason, "remote:"):
			reason = reason[len("remote:"):]
		default:
			if markerReasons[reason] {
				return reason, true
			}
			return "explicit_flush", false
		}
	}
}

// HTTPMiddleware wraps an http.Handler and opens a flight-recorder Scope for
// every incoming request.
//
// When the scope is triggered before the response headers are written, the
// handler writes a status of 500 or above, or the request has already run for
// SlowAfter, the response carries FlushHeader. A scope triggered after the
// headers are written cannot add it, and a panic sends no response at all;
// Stats.FlushMarkersMissed counts both.
//
// The scope is keyed by the request's trace ID. When the request context
// already carries a trace (an OpenTelemetry span started by middleware that
// wraps this one, as reported by Options.TraceExtractor or the registered
// TraceBridge), that trace is used. Otherwise the ID is taken from the W3C
// traceparent, X-Trace-Id or X-Request-Id header, in that order (the first
// valid value wins), or generated. X-Trace-Id and X-Request-Id values are
// valid when they are 1 to 128 bytes of letters, digits and _ . : - / + = @.
// Put OpenTelemetry middleware outside HTTPMiddleware so the two agree.
// Services that receive requests from outside your system should set
// Options.IgnoreRequestTraceIDs. The buffer
// is flushed if an error log is recorded, the response status is 5xx, the
// handler panics (except http.ErrAbortHandler), the duration exceeds SlowAfter,
// or Flush(ctx, reason) is called explicitly.
func HTTPMiddleware(rec *Recorder) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var traceID string
			// A trace in the context (OpenTelemetry middleware in front)
			// wins over headers; StartScope takes it from ctx, unless a
			// parent scope's ID applies.
			if !rec.state.opts.IgnoreRequestTraceIDs && !rec.state.extractTraceContext(r.Context()).HasTraceID() {
				traceID = extractHTTPTraceID(r.Header)
			}
			ctx, sc := StartScope(r.Context(), rec, ScopeMeta{
				TraceID: traceID,
				Method:  r.Method,
				Path:    r.URL.Path,
			})
			if _, ok := r.Header[FlushHeader]; ok {
				// Clone so the caller's header map is left as received.
				r = r.Clone(r.Context())
				r.Header.Del(FlushHeader)
			}
			rw, sw := wrapResponseWriter(w, sc)

			defer func() {
				if p := recover(); p != nil {
					if err, ok := p.(error); ok && errors.Is(err, http.ErrAbortHandler) {
						sc.End(ctx, sw.status)
						panic(p)
					}
					sc.Trigger("panic")
					sw.finishPanic()
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
	for _, key := range []string{"X-Trace-Id", "X-Request-Id"} {
		for _, v := range h.Values(key) {
			if id := strings.TrimSpace(v); validTraceID(id) {
				return id
			}
		}
	}
	return ""
}

// maxTraceIDLen bounds trace IDs taken from X-Trace-Id and X-Request-Id.
const maxTraceIDLen = 128

// validTraceID reports whether a trace ID taken from a request header or
// metadata is usable: 1 to maxTraceIDLen bytes of letters, digits and
// _ . : - / + = @. This covers UUIDs, ULIDs, base64 and the host/sequence form
// some request ID middleware produces, and excludes whitespace, quotes,
// control and non-ASCII bytes. The ID is written to every log record and,
// with cross-service flush, identifies scopes across processes.
func validTraceID(id string) bool {
	if id == "" || len(id) > maxTraceIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '_', c == '-', c == '.', c == ':', c == '/', c == '+', c == '=', c == '@':
		default:
			return false
		}
	}
	return true
}

// parseW3CTraceparent returns the trace ID of a W3C traceparent header
// ("00-<32hex>-<16hex>-<2hex>"), or "" if the value is not valid as the W3C
// Trace Context specification defines it: lowercase hex, non-zero trace and
// parent IDs, version ff rejected, and exactly four fields for version 00.
// A higher version may append fields after a dash.
func parseW3CTraceparent(tp string) string {
	const n = 55 // len("00-") + 32 + len("-") + 16 + len("-") + 2
	if len(tp) < n || tp[2] != '-' || tp[35] != '-' || tp[52] != '-' {
		return ""
	}
	version, tid, pid, flags := tp[0:2], tp[3:35], tp[36:52], tp[53:55]
	if !lowerHex(version) || version == "ff" || !lowerHex(flags) {
		return ""
	}
	if version == "00" && len(tp) != n || version != "00" && len(tp) > n && tp[n] != '-' {
		return ""
	}
	if !lowerHex(tid) || !lowerHex(pid) || strings.Trim(tid, "0") == "" || strings.Trim(pid, "0") == "" {
		return ""
	}
	return tid
}

// lowerHex reports whether s is non-empty and consists of 0-9 and a-f.
func lowerHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
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
	w.scope.triggerIfSlow()
	w.markerSent = w.scope.SendFlushMarker(func(value string) error {
		w.Header().Set(FlushHeader, value)
		return nil
	})
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
	w.scope.triggerIfSlow()
	w.countMissed()
}

// finishPanic counts the marker as missed: net/http sends no response for a
// handler that panics.
func (w *statusResponseWriter) finishPanic() {
	if w.scope == nil || w.hijacked {
		return
	}
	w.countMissed()
}

func (w *statusResponseWriter) countMissed() {
	if w.markerSent || w.scope.state.opts.DisableFlushMarker {
		return
	}
	if triggered, _ := w.scope.TriggerState(); triggered {
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
