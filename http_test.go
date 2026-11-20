// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/haawklabs/flightrec"
)

func TestScopeSendFlushMarker(t *testing.T) {
	record := func(dst *string) func(string) error {
		return func(v string) error { *dst = v; return nil }
	}

	rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler, TrustRequestTraceIDs: true, EnableFlushMarker: true})
	ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
	var sent string
	if sc.SendFlushMarker(record(&sent)) || sent != "" {
		t.Errorf("untriggered scope sent %q", sent)
	}
	flightrec.Flush(ctx, "downstream:http_503")
	if !sc.SendFlushMarker(record(&sent)) || sent != "http_503" {
		t.Errorf("sent %q, want http_503 with the prefix removed", sent)
	}
	if sc.SendFlushMarker(func(string) error { return errors.New("trailer already sent") }) {
		t.Error("SendFlushMarker reported success for a failed send")
	}
	sc.End(ctx, 200)
	if st := rec.Stats(); st.FlushMarkersSent != 1 || st.FlushMarkersMissed != 1 {
		t.Errorf("FlushMarkersSent=%d FlushMarkersMissed=%d, want 1 and 1", st.FlushMarkersSent, st.FlushMarkersMissed)
	}

	ctx, sc = flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
	flightrec.Flush(ctx, "payment mismatch")
	if sc.SendFlushMarker(record(&sent)); sent != "explicit_flush" {
		t.Errorf("custom reason sent as %q, want explicit_flush", sent)
	}
	sc.End(ctx, 200)

	off := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
	ctx, sc = flightrec.StartScope(context.Background(), off, flightrec.ScopeMeta{})
	flightrec.Flush(ctx, "x")
	called := false
	if sc.SendFlushMarker(func(string) error { called = true; return nil }) || called {
		t.Error("SendFlushMarker sent by default")
	}
	sc.End(ctx, 200)
	if st := off.Stats(); st.FlushMarkersSent != 0 || st.FlushMarkersMissed != 0 {
		t.Errorf("counters moved by default: %+v", st)
	}

	var nilScope *flightrec.Scope
	if nilScope.SendFlushMarker(record(&sent)) {
		t.Error("nil scope sent a marker")
	}
}

func TestReceiveMarker(t *testing.T) {
	for _, tc := range []struct {
		name       string
		values     []string
		want       bool
		wantReason string
		rejected   uint64
	}{
		{"http_502", []string{"http_502"}, true, "downstream:http_502", 0},
		{"slow_request", []string{"slow_request"}, true, "downstream:slow_request", 0},
		{"none", nil, false, "", 0},
		{"unknown", []string{"nope"}, false, "", 1},
		{"several", []string{"http_502", "http_502"}, false, "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
			ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
			if got := flightrec.ReceiveMarker(ctx, tc.values); got != tc.want {
				t.Errorf("ReceiveMarker = %v, want %v", got, tc.want)
			}
			triggered, reason := sc.TriggerState()
			if triggered != tc.want || reason != tc.wantReason {
				t.Errorf("TriggerState = %v %q, want %v %q", triggered, reason, tc.want, tc.wantReason)
			}
			st := rec.Stats()
			wantDown := uint64(0)
			if tc.want {
				wantDown = 1
			}
			if st.MarkersRejected != tc.rejected || st.DownstreamTriggers != wantDown {
				t.Errorf("MarkersRejected=%d DownstreamTriggers=%d, want %d and %d", st.MarkersRejected, st.DownstreamTriggers, tc.rejected, wantDown)
			}
			sc.End(ctx, 200)
		})
	}

	t.Run("already triggered", func(t *testing.T) {
		rec := flightrec.New(flightrec.Options{Next: slog.DiscardHandler})
		ctx, sc := flightrec.StartScope(context.Background(), rec, flightrec.ScopeMeta{})
		flightrec.Flush(ctx, "error_log")
		if flightrec.ReceiveMarker(ctx, []string{"http_502"}) {
			t.Error("ReceiveMarker reported a trigger on an already triggered scope")
		}
		if st := rec.Stats(); st.DownstreamTriggers != 0 {
			t.Errorf("DownstreamTriggers = %d, want 0", st.DownstreamTriggers)
		}
		sc.End(ctx, 200)
	})

	if flightrec.ReceiveMarker(context.Background(), []string{"http_502"}) {
		t.Error("ReceiveMarker without a scope reported a trigger")
	}
}

// markerServer starts a server whose handler is wrapped in HTTPMiddleware and
// returns the recorder, the server URL and a buffer with the log output.
func markerServer(t *testing.T, mk func(log *slog.Logger) http.HandlerFunc) (*flightrec.Recorder, string, *syncBuffer) {
	t.Helper()
	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{
		TrustRequestTraceIDs: true,
		EnableFlushMarker:    true,
		Next:                 slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLeveler:          slog.LevelWarn,
		BufferLeveler:        slog.LevelDebug,
	})
	srv := httptest.NewUnstartedServer(flightrec.HTTPMiddleware(rec)(mk(slog.New(rec))))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the panic case is expected
	srv.Start()
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
		// A handler panic makes the server close the connection.
		return &http.Response{Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}
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
			name: "custom reason text is not sent",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					flightrec.Flush(r.Context(), "payment mismatch\r\nX-Injected: 1")
					_, _ = w.Write([]byte("ok"))
				}
			},
			wantMarker: "explicit_flush",
			wantSent:   1,
		},
		{
			name: "triggered when the handler wrote nothing",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					flightrec.Flush(r.Context(), "late_but_before_headers")
				}
			},
			wantMarker: "explicit_flush",
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
			name: "prefixed reason sends the original failure",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					flightrec.Flush(r.Context(), "downstream:remote:http_502")
				}
			},
			wantMarker: "http_502",
			wantSent:   1,
		},
		{
			name: "flush before any write",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					log.ErrorContext(r.Context(), "stream setup failed")
					_ = http.NewResponseController(w).Flush()
					_, _ = w.Write([]byte("data"))
				}
			},
			wantMarker: "error_log",
			wantSent:   1,
		},
		{
			name: "body written with ReadFrom",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					log.ErrorContext(r.Context(), "cache unavailable")
					// LimitReader has no WriteTo, so io.Copy uses the writer's ReadFrom.
					_, _ = io.Copy(w, io.LimitReader(strings.NewReader("body"), 4))
				}
			},
			wantMarker: "error_log",
			wantSent:   1,
		},
		{
			name: "panic sends no response",
			handler: func(log *slog.Logger) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					panic("boom")
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

func TestFlushMarkerOffByDefault(t *testing.T) {
	out := &syncBuffer{}
	rec := flightrec.New(flightrec.Options{Next: slog.NewJSONHandler(out, nil)})
	srv := httptest.NewServer(flightrec.HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})))
	t.Cleanup(srv.Close)

	resp := get(t, srv.URL, nil)
	if got := resp.Header.Get(flightrec.FlushHeader); got != "" {
		t.Errorf("%s = %q by default", flightrec.FlushHeader, got)
	}
	if st := rec.Stats(); st.FlushMarkersSent != 0 || st.FlushMarkersMissed != 0 || st.ScopesFlushed != 1 {
		t.Errorf("want the scope flushed without marker counters, got %+v", st)
	}
}

func TestFlushMarkerForSlowRequest(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }

	rec := flightrec.New(flightrec.Options{
		TrustRequestTraceIDs: true,
		EnableFlushMarker:    true,
		Next:                 slog.NewJSONHandler(io.Discard, nil),
		SlowAfter:            time.Second,
		Now:                  clock,
	})
	srv := httptest.NewServer(flightrec.HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		advance(2 * time.Second)
		_, _ = w.Write([]byte("slow but successful"))
	})))
	t.Cleanup(srv.Close)

	resp := get(t, srv.URL, nil)
	if got := resp.Header.Get(flightrec.FlushHeader); got != "slow_request" {
		t.Errorf("%s = %q, want slow_request", flightrec.FlushHeader, got)
	}
}

func TestFlushMarkerLeavesCallerHeadersIntact(t *testing.T) {
	rec := flightrec.New(flightrec.Options{Next: slog.NewJSONHandler(io.Discard, nil)})
	var seen string
	h := flightrec.HTTPMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get(flightrec.FlushHeader)
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(flightrec.FlushHeader, "forced_by_client")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if seen != "" {
		t.Errorf("handler saw %q", seen)
	}
	if got := req.Header.Get(flightrec.FlushHeader); got != "forced_by_client" {
		t.Errorf("caller's request header was modified: %q", got)
	}
}

// service is one process in a multi-service test: its own recorder, log
// output and HTTP server.
type service struct {
	rec *flightrec.Recorder
	log *slog.Logger
	out *syncBuffer
	srv *httptest.Server
}

func newService(t *testing.T, handler func(s *service) http.HandlerFunc) *service {
	t.Helper()
	s := &service{out: &syncBuffer{}}
	s.rec = flightrec.New(flightrec.Options{
		TrustRequestTraceIDs: true,
		EnableFlushMarker:    true,
		Next:                 slog.NewJSONHandler(s.out, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLeveler:          slog.LevelWarn,
		BufferLeveler:        slog.LevelDebug,
	})
	s.log = slog.New(s.rec)
	s.srv = httptest.NewServer(flightrec.HTTPMiddleware(s.rec)(handler(s)))
	t.Cleanup(s.srv.Close)
	return s
}

// client returns an HTTP client whose transport reads flush markers into the
// service's scopes.
func (s *service) client() *http.Client {
	return &http.Client{Transport: flightrec.NewTransport(nil)}
}

// call makes a GET request from inside a handler, propagating its context.
func (s *service) call(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client().Do(req)
	if err != nil {
		return nil, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp, nil
}

// flushed returns the msg of every flushed record and the reason they share.
func flushed(t *testing.T, out *syncBuffer) (msgs []string, reason string) {
	t.Helper()
	for _, ln := range out.Lines(t) {
		if r, ok := ln["flightrec.flush_reason"].(string); ok {
			msgs = append(msgs, ln["msg"].(string))
			reason = r
		}
	}
	return msgs, reason
}

func TestTransportPropagatesFlushThroughThreeServices(t *testing.T) {
	c := newService(t, func(s *service) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			s.log.DebugContext(r.Context(), "c: reading inventory")
			w.WriteHeader(http.StatusBadGateway)
		}
	})
	b := newService(t, func(s *service) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			s.log.DebugContext(r.Context(), "b: pricing cart")
			if _, err := s.call(r.Context(), c.srv.URL); err != nil {
				t.Errorf("b calling c: %v", err)
			}
			// B handles C's failure and still answers 200.
			w.WriteHeader(http.StatusOK)
		}
	})
	a := newService(t, func(s *service) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			s.log.DebugContext(r.Context(), "a: checkout started")
			if _, err := s.call(r.Context(), b.srv.URL); err != nil {
				t.Errorf("a calling b: %v", err)
			}
			w.WriteHeader(http.StatusOK)
		}
	})

	resp, err := http.Get(a.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	for _, tc := range []struct {
		name       string
		svc        *service
		wantMsg    string
		wantReason string
		wantDown   uint64
	}{
		{"c", c, "c: reading inventory", "http_502", 0},
		{"b", b, "b: pricing cart", "downstream:http_502", 1},
		{"a", a, "a: checkout started", "downstream:http_502", 1},
	} {
		msgs, reason := flushed(t, tc.svc.out)
		if len(msgs) != 1 || msgs[0] != tc.wantMsg || reason != tc.wantReason {
			t.Errorf("service %s flushed %v with reason %q, want [%s] with %q", tc.name, msgs, reason, tc.wantMsg, tc.wantReason)
		}
		if got := tc.svc.rec.Stats().DownstreamTriggers; got != tc.wantDown {
			t.Errorf("service %s DownstreamTriggers = %d, want %d", tc.name, got, tc.wantDown)
		}
	}
	if got := resp.Header.Get(flightrec.FlushHeader); got != "http_502" {
		t.Errorf("A's response %s = %q, want the original failure http_502", flightrec.FlushHeader, got)
	}
}

func TestTransportRejectsUnknownMarkerValues(t *testing.T) {
	for name, values := range map[string][]string{
		"unknown":       {"owned"},
		"prefixed":      {"downstream:http_502"},
		"empty":         {""},
		"several":       {"http_502", "owned"},
		"several valid": {"http_502", "http_503"},
	} {
		t.Run(name, func(t *testing.T) {
			down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header()[flightrec.FlushHeader] = values
			}))
			t.Cleanup(down.Close)

			var got *http.Response
			up := newService(t, func(s *service) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					s.log.DebugContext(r.Context(), "calling downstream")
					got, _ = s.call(r.Context(), down.URL)
				}
			})
			resp, err := http.Get(up.srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()

			if msgs, _ := flushed(t, up.out); len(msgs) != 0 {
				t.Errorf("values %q triggered a flush: %v", values, msgs)
			}
			st := up.rec.Stats()
			if st.DownstreamTriggers != 0 || st.MarkersRejected != 1 {
				t.Errorf("DownstreamTriggers=%d MarkersRejected=%d, want 0 and 1", st.DownstreamTriggers, st.MarkersRejected)
			}
			if got == nil || len(got.Header.Values(flightrec.FlushHeader)) != 0 {
				t.Errorf("%s was not removed from the response returned to the caller", flightrec.FlushHeader)
			}
		})
	}
}

func TestTransportWithoutScope(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(flightrec.FlushHeader, "http_502")
	}))
	t.Cleanup(down.Close)
	rec := flightrec.New(flightrec.Options{Next: slog.NewJSONHandler(io.Discard, nil)})

	resp, err := (&http.Client{Transport: flightrec.NewTransport(nil)}).Get(down.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if v := resp.Header.Get(flightrec.FlushHeader); v != "" {
		t.Errorf("%s = %q, want it removed", flightrec.FlushHeader, v)
	}
	if st := rec.Stats(); st.DownstreamTriggers != 0 || st.MarkersRejected != 0 {
		t.Errorf("unexpected counters without a scope: %+v", st)
	}
}

func TestMarkerValue(t *testing.T) {
	for _, tc := range []struct {
		in     string
		want   string
		wantOK bool
	}{
		{"http_502", "http_502", true},
		{"downstream:remote:slow_request", "slow_request", true},
		{"payment mismatch", "explicit_flush", false},
		{"", "explicit_flush", false},
		{"downstream:", "explicit_flush", false},
	} {
		got, ok := flightrec.MarkerValue(tc.in)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("MarkerValue(%q) = %q, %v; want %q, %v", tc.in, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestTransportCountsOnlyScopesItTriggers(t *testing.T) {
	downTriggered := newService(t, func(s *service) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(flightrec.FlushHeader, "http_503")
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})

	up := newService(t, func(s *service) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			// Three failing calls in one request trigger the scope once.
			for i := 0; i < 3; i++ {
				_, _ = s.call(r.Context(), downTriggered.srv.URL)
			}
		}
	})
	resp, err := http.Get(up.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if got := up.rec.Stats().DownstreamTriggers; got != 1 {
		t.Errorf("DownstreamTriggers = %d for three markers on one scope, want 1", got)
	}
}

func TestTransportTransportError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // connections to url now fail

	up := newService(t, func(s *service) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if _, err := s.call(r.Context(), url); err == nil {
				t.Error("expected a connection error")
			}
		}
	})
	resp, err := http.Get(up.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if st := up.rec.Stats(); st.DownstreamTriggers != 0 || st.MarkersRejected != 0 {
		t.Errorf("unexpected counters after a transport error: %+v", st)
	}
}

func TestValidMarker(t *testing.T) {
	for _, v := range []string{"http_502", "error_log", "slow_request", "explicit_flush"} {
		if !flightrec.ValidMarker(v) {
			t.Errorf("ValidMarker(%q) = false", v)
		}
	}
	for _, v := range []string{"", "downstream:http_502", "remote:panic", "HTTP_502", "http_502 "} {
		if flightrec.ValidMarker(v) {
			t.Errorf("ValidMarker(%q) = true", v)
		}
	}
}
