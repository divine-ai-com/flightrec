// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Command cross-service shows flightrec keeping the debug logs of every
// service that handled a failed request, not only the one that failed.
//
// Three services take part in a checkout:
//
//	gateway  ->  orders  ->  inventory
//
// Two requests are sent:
//
//  1. A synchronous failure. inventory answers 503; orders handles it and
//     answers 200; gateway answers 200. The Flightrec-Flush response header
//     carries the failure up the chain, so all three services write their
//     debug logs (reason downstream:http_503 for gateway and orders).
//
//  2. An asynchronous failure. orders answers 200 at once and calls
//     inventory in the background; by the time inventory fails, gateway and
//     orders have finished and kept their buffers for Options.Retain.
//     inventory logs an error; the notifier announces it, and both write the
//     logs they kept (reason remote:error_log).
//
// By default the three services run in this process and share an in-memory
// notifier:
//
//	go run .
//
// To run them as separate processes connected through Redis, see the
// docker-compose.yml next to this file, or run each role yourself:
//
//	go run . -role inventory -listen :8083 -redis localhost:6379
//	go run . -role orders -listen :8082 -next http://localhost:8083 -redis localhost:6379
//	go run . -role gateway -listen :8081 -next http://localhost:8082 -redis localhost:6379
//	curl -s localhost:8081/checkout?fail=sync
//	curl -s localhost:8081/checkout?fail=async
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/divine-ai-com/flightrec"
	"github.com/divine-ai-com/flightrec/notifier/memnotifier"
	"github.com/divine-ai-com/flightrec/notifier/redisnotifier"
)

func main() {
	role := flag.String("role", "all", "service to run: gateway, orders, inventory, or all in one process")
	listen := flag.String("listen", ":8080", "listen address (single role only)")
	next := flag.String("next", "", "URL of the next service (gateway and orders only)")
	redisAddr := flag.String("redis", "", "Redis address for the notifier (single role only)")
	flag.Parse()

	if *role == "all" {
		if err := runAll(os.Stdout); err != nil {
			log.Fatal(err)
		}
		return
	}

	if *redisAddr == "" {
		log.Fatal("-redis is required when running a single role")
	}
	client := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer func() { _ = client.Close() }()
	n, err := redisnotifier.New(client, redisnotifier.WithChannel("flightrec:example"))
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	svc := newService(*role, *next, os.Stdout)
	defer svc.rec.StartNotifier(ctx, n)()

	srv := &http.Server{Addr: *listen, Handler: svc.handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	log.Printf("%s listening on %s", *role, *listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// service is one of the three services, with its own flightrec Recorder.
type service struct {
	name   string
	next   string
	rec    *flightrec.Recorder
	log    *slog.Logger
	client *http.Client
	async  sync.WaitGroup
}

func newService(name, next string, out io.Writer) *service {
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewTextHandler(&prefixWriter{prefix: fmt.Sprintf("%-9s | ", name), w: out}, &slog.HandlerOptions{Level: slog.LevelDebug, ReplaceAttr: dropTime}),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
		Retain:        30 * time.Second,
	})
	return &service{
		name:   name,
		next:   next,
		rec:    rec,
		log:    slog.New(rec),
		client: &http.Client{Transport: flightrec.NewTransport(nil), Timeout: 5 * time.Second},
	}
}

func (s *service) handler() http.Handler {
	mux := http.NewServeMux()
	switch s.name {
	case "gateway":
		mux.HandleFunc("/checkout", s.gateway)
	case "orders":
		mux.HandleFunc("/order", s.orders)
	case "inventory":
		mux.HandleFunc("/reserve", s.inventory)
	}
	return flightrec.HTTPMiddleware(s.rec)(mux)
}

func (s *service) gateway(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	s.log.DebugContext(ctx, "authenticated customer", "customer", "c-1041")
	s.log.DebugContext(ctx, "forwarding to orders")
	if err := s.call(ctx, s.next+"/order?"+r.URL.RawQuery); err != nil {
		s.log.WarnContext(ctx, "orders unavailable", "err", err)
	}
	_, _ = io.WriteString(w, "ok\n")
}

func (s *service) orders(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	s.log.DebugContext(ctx, "priced cart", "items", 3, "total_cents", 4598)
	switch r.URL.Query().Get("fail") {
	case "sync":
		// Reserve stock before answering. A failure is handled here, but the
		// response from inventory tells flightrec to keep this request's logs.
		_ = s.call(ctx, s.next+"/reserve?fail=sync")
	case "async":
		// Answer now and reserve stock in the background, under the same
		// trace ID, after this request has finished.
		traceID := flightrec.TraceID(ctx)
		s.async.Add(1)
		go func() {
			defer s.async.Done()
			time.Sleep(50 * time.Millisecond)
			bg, sc := flightrec.StartScope(context.Background(), s.rec, flightrec.ScopeMeta{TraceID: traceID, Method: "JOB", Path: "reserve-stock"})
			s.log.DebugContext(bg, "reserving stock in the background")
			err := s.call(bg, s.next+"/reserve?fail=async")
			status := 200
			if err != nil {
				status = 500
			}
			sc.End(bg, status)
		}()
	}
	s.log.DebugContext(ctx, "order stored", "order", "o-77")
	_, _ = io.WriteString(w, "ok\n")
}

func (s *service) inventory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	s.log.DebugContext(ctx, "checking warehouse", "warehouse", "eu-central-1")
	switch r.URL.Query().Get("fail") {
	case "sync":
		http.Error(w, "warehouse busy", http.StatusServiceUnavailable)
	case "async":
		s.log.ErrorContext(ctx, "stock record locked")
		http.Error(w, "stock record locked", http.StatusInternalServerError)
	default:
		_, _ = io.WriteString(w, "reserved\n")
	}
}

// call sends a GET with the request's trace ID, so the next service joins the
// same trace, and returns an error for a non-2xx response.
func (s *service) call(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Request-Id", flightrec.TraceID(ctx))
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	return nil
}

// runAll starts the three services in this process on random ports, sends
// both requests, and prints what each service wrote.
func runAll(out io.Writer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := memnotifier.New()

	inventory := newService("inventory", "", out)
	orders := newService("orders", "", out)
	gateway := newService("gateway", "", out)
	services := []*service{inventory, orders, gateway}

	urls := make([]string, len(services))
	for i, svc := range services {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		urls[i] = "http://" + ln.Addr().String()
		srv := &http.Server{Handler: svc.handler(), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = srv.Serve(ln) }()
		defer func() { _ = srv.Close() }()
		defer svc.rec.StartNotifier(ctx, bus)()
	}
	orders.next, gateway.next = urls[0], urls[1]
	for deadline := time.Now().Add(2 * time.Second); bus.Subscribers() < len(services); {
		if time.Now().After(deadline) {
			return errors.New("notifier subscriptions did not start")
		}
		time.Sleep(time.Millisecond)
	}

	for _, tc := range []struct{ title, query string }{
		{"1. Synchronous failure: inventory answers 503, the others answer 200", "fail=sync"},
		{"2. Asynchronous failure: inventory fails after the others have answered", "fail=async"},
	} {
		_, _ = fmt.Fprintf(out, "\n%s\n%s\n", tc.title, strings.Repeat("-", len(tc.title)))
		resp, err := http.Get(urls[2] + "/checkout?" + tc.query)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		orders.async.Wait()
		time.Sleep(100 * time.Millisecond) // let remote flushes arrive
	}
	return nil
}

// prefixWriter prefixes each line with the service name. slog handlers write
// one record per Write call.
type prefixWriter struct {
	mu     sync.Mutex
	prefix string
	w      io.Writer
}

func (p *prefixWriter) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := io.WriteString(p.w, p.prefix); err != nil {
		return 0, err
	}
	return p.w.Write(b)
}

// dropTime removes the time attribute so the output is easy to read.
func dropTime(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && a.Key == slog.TimeKey {
		return slog.Attr{}
	}
	return a
}
