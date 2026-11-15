// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/divine-ai-com/flightrec/internal/notify"
	"github.com/divine-ai-com/flightrec/internal/traceid"
)

var update = flag.Bool("update", false, "rewrite testdata/golden files")

// normalize replaces the values that change between runs: times, durations
// and the host name.
func normalize(b []byte) []byte {
	for _, r := range []struct {
		re   *regexp.Regexp
		repl string
	}{
		{regexp.MustCompile(`"(time|start|flightrec.recovered_process_start)":"[^"]*"`), `"$1":"<time>"`},
		{regexp.MustCompile(`"duration":\d+`), `"duration":<duration>`},
		{regexp.MustCompile(`"flightrec.recovered_host":"[^"]*"`), `"flightrec.recovered_host":"<host>"`},
	} {
		b = r.re.ReplaceAll(b, []byte(r.repl))
	}
	return b
}

// checkGolden compares out with testdata/golden/name.jsonl.
func checkGolden(t *testing.T, name string, out []byte) {
	t.Helper()
	path := filepath.Join("testdata", "golden", name+".jsonl")
	got := normalize(out)
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run go test -run TestGolden -update)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s output changed:\n got:\n%s\nwant:\n%s", name, got, want)
	}
}

type goldenOrder struct {
	ID    int    `json:"id"`
	State string `json:"state"`
}

// TestGolden pins the exact lines flightrec writes in each situation.
func TestGolden(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
		run  func(rec *Recorder, log *slog.Logger)
	}{
		{"healthy", Options{}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-1", Method: "GET", Path: "/orders"})
			log.DebugContext(ctx, "loaded cart", "items", 3)
			sc.End(ctx, 200)
		}},
		{"error_log", Options{}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-2", Method: "POST", Path: "/checkout"})
			log.DebugContext(ctx, "loaded cart", "items", 3)
			log.InfoContext(ctx, "calling provider", "provider", "acme")
			log.ErrorContext(ctx, "provider timed out", "err", errors.New("deadline exceeded"))
			sc.End(ctx, 200)
		}},
		{"status_5xx", Options{}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-3"})
			log.With("service", "checkout").WithGroup("req").DebugContext(ctx, "step", "n", 1, "order", goldenOrder{ID: 7, State: "new"})
			sc.End(ctx, 503)
		}},
		{"explicit_flush", Options{}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-4"})
			log.DebugContext(ctx, "totals differ")
			Flush(ctx, "payment mismatch")
			sc.End(ctx, 200)
		}},
		// One record larger than MaxPerScope is dropped by itself; unlike
		// filling the ring, this does not depend on the platform's slot size.
		{"truncated", Options{MaxPerScope: 8 << 10}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-5"})
			log.DebugContext(ctx, "small-1")
			log.DebugContext(ctx, "huge", "blob", strings.Repeat("x", 16<<10))
			log.DebugContext(ctx, "small-2")
			sc.End(ctx, 500)
		}},
		{"retained_flush", Options{Retain: time.Minute}, func(rec *Recorder, log *slog.Logger) {
			ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "req-6"})
			log.DebugContext(ctx, "queued email", "order", &goldenOrder{ID: 9, State: "paid"})
			sc.End(ctx, 200)
			rec.FlushTrace(context.Background(), "req-6", "downstream failure")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			tc.opts.Next = slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})
			tc.opts.PassLeveler = slog.LevelWarn
			tc.opts.BufferLeveler = slog.LevelDebug
			rec := New(tc.opts)
			tc.run(rec, slog.New(rec))
			checkGolden(t, tc.name, out.Bytes())
		})
	}
}

func TestWithSigningKeyRejectsShortKeys(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("short key accepted")
		}
	}()
	WithSigningKey([]byte("short"))
}

// FuzzNotifierReceive feeds arbitrary bus messages to the receive path, with
// and without a signing key: it must never panic, and only well-formed
// messages may reach the inbox.
func FuzzNotifierReceive(f *testing.F) {
	key := bytes.Repeat([]byte("k"), 32)
	good, _ := json.Marshal(notify.Message{Version: notify.Version, Origin: "other", Notices: []notify.Notice{{TraceID: "t-1", Reason: "http_500"}}})
	f.Add(good)
	f.Add(notify.Sign(key, good, time.Now()))
	f.Add([]byte(`{"v":1,"o":"x","n":[{"t":"a b","r":"zzz"}]}`))
	f.Fuzz(func(t *testing.T, msg []byte) {
		for _, keys := range [][][]byte{nil, {key}} {
			rec := New(Options{Next: slog.DiscardHandler})
			h := &notifierHub{
				st:       rec.state,
				origin:   "self",
				keys:     keys,
				inbox:    make(chan notify.Notice, notifierQueueSize),
				received: notify.NewRecentSet(notifierRecentSize, notifierRecentTTL, rec.state.now),
			}
			h.receive(msg)
			for len(h.inbox) > 0 {
				n := <-h.inbox
				if !traceid.Valid(n.TraceID) || !ValidMarker(n.Reason) {
					t.Fatalf("invalid notice reached the inbox: %+v", n)
				}
			}
		}
	})
}

func TestRetainedScopesExpireWithoutTraffic(t *testing.T) {
	rec := New(Options{Next: slog.NewJSONHandler(io.Discard, nil), Retain: 30 * time.Millisecond})
	log := slog.New(rec)
	ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: "t"})
	log.DebugContext(ctx, "x")
	sc.End(ctx, 200)

	set := &rec.state.retained
	deadline := time.Now().Add(2 * time.Second)
	for {
		set.mu.Lock()
		n, timerSet := set.order.Len(), set.timer != nil
		set.mu.Unlock()
		if n == 0 && !timerSet {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained scope still held after its window with no other calls: n=%d timer=%v", n, timerSet)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

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
