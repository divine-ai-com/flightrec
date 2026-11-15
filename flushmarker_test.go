// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/divine-ai-com/flightrec"
)

// markerServer starts a server whose handler is wrapped in HTTPMiddleware and
// returns the recorder, the server URL and a buffer with the log output.
func markerServer(t *testing.T, mk func(log *slog.Logger) http.HandlerFunc) (*flightrec.Recorder, string, *syncBuffer) {
	t.Helper()
	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})
	srv := httptest.NewServer(flightrec.HTTPMiddleware(rec)(mk(slog.New(rec))))
	t.Cleanup(srv.Close)
	return rec, srv.URL, out
}

func get(t *testing.T, url string, header http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}

func TestFlushMarkerOnResponses(t *testing.T) {
	tests := []struct {
		name       string
		handler    func(log *slog.Logger) http.HandlerFunc
		wantMarker string
		wantSent   uint64
		wantMissed uint64
	}{
		{
			name: "error log before headers",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					log.DebugContext(r.Context(), "loaded cart")
					log.ErrorContext(r.Context(), "payment provider timed out")
					w.WriteHeader(http.StatusOK)
				}
			},
			wantMarker: "error_log",
			wantSent:   1,
		},
		{
			name: "5xx status",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusServiceUnavailable)
				}
			},
			wantMarker: "http_503",
			wantSent:   1,
		},
		{
			name: "explicit flush with a reason that needs sanitizing",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					flightrec.Flush(r.Context(), "payment mismatch\r\nX-Injected: 1")
					_, _ = w.Write([]byte("ok"))
				}
			},
			wantMarker: "payment_mismatch__X-Injected:_1",
			wantSent:   1,
		},
		{
			name: "triggered when the handler wrote nothing",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					flightrec.Flush(r.Context(), "late_but_before_headers")
				}
			},
			wantMarker: "late_but_before_headers",
			wantSent:   1,
		},
		{
			name: "triggered after headers were written",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte("partial"))
					flightrec.Flush(r.Context(), "too_late")
				}
			},
			wantMissed: 1,
		},
		{
			name: "healthy request",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte("ok"))
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec, url, _ := markerServer(t, tc.handler)
			resp := get(t, url, nil)

			if got := resp.Header.Get(flightrec.FlushHeader); got != tc.wantMarker {
				t.Errorf("%s = %q, want %q", flightrec.FlushHeader, got, tc.wantMarker)
			}
			st := rec.Stats()
			if st.FlushMarkersSent != tc.wantSent || st.FlushMarkersMissed != tc.wantMissed {
				t.Errorf("FlushMarkersSent=%d FlushMarkersMissed=%d, want %d and %d",
					st.FlushMarkersSent, st.FlushMarkersMissed, tc.wantSent, tc.wantMissed)
			}
		})
	}
}

func TestFlushMarkerOnIncomingRequestIsIgnored(t *testing.T) {
	var seen string
	rec, url, out := markerServer(t, func(log *slog.Logger) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			seen = r.Header.Get(flightrec.FlushHeader)
			log.DebugContext(r.Context(), "loaded cart")
			w.WriteHeader(http.StatusOK)
		}
	})
	resp := get(t, url, http.Header{flightrec.FlushHeader: {"forced_by_client"}})

	if seen != "" {
		t.Errorf("handler saw incoming %s = %q; it must be removed", flightrec.FlushHeader, seen)
	}
	if got := resp.Header.Get(flightrec.FlushHeader); got != "" {
		t.Errorf("response carried %s = %q for a healthy request", flightrec.FlushHeader, got)
	}
	if st := rec.Stats(); st.ScopesFlushed != 0 || st.FlushMarkersSent != 0 {
		t.Errorf("client header triggered a flush: %+v", st)
	}
	if strings.Contains(out.String(), "flush_reason") {
		t.Errorf("unexpected flushed records: %s", out.String())
	}
}

func TestInformationalStatusDoesNotFixFinalStatus(t *testing.T) {
	_, url, out := markerServer(t, func(*slog.Logger) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Link", "</style.css>; rel=preload")
			w.WriteHeader(http.StatusEarlyHints)
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	resp := get(t, url, nil)

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if got := resp.Header.Get(flightrec.FlushHeader); got != "http_503" {
		t.Errorf("%s = %q, want http_503 on the final response", flightrec.FlushHeader, got)
	}
	if strings.Contains(out.String(), `"status":103`) {
		t.Errorf("summary recorded the informational status: %s", out.String())
	}
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
