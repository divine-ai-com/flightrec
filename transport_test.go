// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/divine-ai-com/flightrec"
)

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
		Next:          slog.NewJSONHandler(s.out, &slog.HandlerOptions{Level: slog.LevelDebug}),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
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
