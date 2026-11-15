// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Command crash-recovery shows flightrec recovering the debug logs of
// requests that were in flight when the process was killed with SIGKILL.
//
// By default it runs the whole story and checks it:
//
//		go run .
//
//	 1. It starts a server (this program with -serve) that journals every
//	    request with StartJournal.
//	 2. It sends a checkout request. The handler logs two debug lines, which
//	    are buffered, then waits on a slow payment provider.
//	 3. It kills the server with SIGKILL while the request is in flight: no
//	    defer, signal handler or flush runs.
//	 4. It starts the server again. StartJournal finds the journal, writes
//	    the two debug lines tagged flightrec.flush_reason=crash_recovery, then
//	    "request interrupted by crash".
//
// The program exits with an error if the recovered lines do not appear, so
// CI runs it as a test.
//
// To do it by hand:
//
//	go run . -serve -journal /tmp/flightrec.journal
//	curl localhost:8080/checkout          # in a second terminal
//	kill -9 <pid>                         # while curl waits
//	go run . -serve -journal /tmp/flightrec.journal
package main

import (
	"bufio"
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
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/divine-ai-com/flightrec"
)

func main() {
	serve := flag.Bool("serve", false, "run the server")
	journal := flag.String("journal", filepath.Join(os.TempDir(), "flightrec-example.journal"), "journal file path")
	listen := flag.String("listen", "localhost:8080", "listen address")
	flag.Parse()
	log.SetFlags(0)

	if *serve {
		if err := runServer(*journal, *listen); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := runDemo(); err != nil {
		log.Fatal(err)
	}
}

// runServer serves /checkout with a journal at path until SIGINT or SIGTERM.
func runServer(path, listen string) error {
	rec := flightrec.New(flightrec.Options{
		Next:          slog.NewJSONHandler(os.Stdout, nil),
		PassLeveler:   slog.LevelWarn,
		BufferLeveler: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(rec))

	// Recovers the journal a killed process left at path, then journals
	// every request. In Kubernetes, put path on an emptyDir volume.
	stopJournal, err := rec.StartJournal(path)
	if errors.Is(err, flightrec.ErrJournalUnsupported) {
		fmt.Fprintln(os.Stderr, notSupported)
		return nil
	}
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		slog.DebugContext(ctx, "loaded cart", "items", 3, "total_cents", 4598)
		slog.DebugContext(ctx, "calling payment provider", "provider", "acme-pay", "attempt", 1)
		fmt.Fprintln(os.Stderr, handlerWaiting) // tells the demo when to kill
		select {
		case <-time.After(30 * time.Second): // a payment provider that hangs
		case <-ctx.Done():
		}
		w.WriteHeader(http.StatusOK)
	})

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: flightrec.HTTPMiddleware(rec)(mux), ReadHeaderTimeout: 5 * time.Second}
	fmt.Fprintf(os.Stderr, "%s%s\n", listeningOn, ln.Addr())

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() {
		<-ctx.Done()
		shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = srv.Shutdown(shutdown)
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	// After the server has drained: a clean stop leaves nothing to recover.
	return stopJournal()
}

// Status lines the server writes to stderr for the demo.
const (
	listeningOn    = "listening on "
	handlerWaiting = "handler waiting"
	notSupported   = "journal not supported on this platform"
)

var errUnsupported = errors.New(notSupported)

// server is a running child process started with -serve.
type server struct {
	cmd    *exec.Cmd
	addr   string
	lines  chan string // stdout: the log
	status chan string // stderr: status lines
}

func startServer(journal string) (*server, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	// This program again, as the server.
	cmd := exec.Command(self, "-serve", "-journal", journal, "-listen", "127.0.0.1:0")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	s := &server{cmd: cmd, lines: make(chan string, 100), status: make(chan string, 10)}
	for pipe, ch := range map[io.Reader]chan string{stdout: s.lines, stderr: s.status} {
		go func() {
			sc := bufio.NewScanner(pipe)
			for sc.Scan() {
				ch <- sc.Text()
			}
			close(ch)
		}()
	}
	// Recovery runs inside StartJournal, before the server listens.
	line, err := s.wait(listeningOn)
	if err != nil {
		s.kill()
		if line == notSupported {
			return nil, errUnsupported
		}
		return nil, err
	}
	s.addr = strings.TrimPrefix(line, listeningOn)
	return s, nil
}

// wait returns the next status line, or an error if it does not start with
// prefix or does not come within 10 seconds.
func (s *server) wait(prefix string) (string, error) {
	select {
	case line, ok := <-s.status:
		if !ok || !strings.HasPrefix(line, prefix) {
			return line, fmt.Errorf("server: got %q, want %q", line, prefix)
		}
		return line, nil
	case <-time.After(10 * time.Second):
		return "", fmt.Errorf("server: no %q after 10s", prefix)
	}
}

// kill sends SIGKILL and waits for the process to exit. It is safe to call
// more than once.
func (s *server) kill() {
	_ = s.cmd.Process.Kill()
	for range s.lines { // Wait must come after the pipes are drained
	}
	_ = s.cmd.Wait()
}

func runDemo() error {
	dir, err := os.MkdirTemp("", "flightrec-crash-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	journal := filepath.Join(dir, "flightrec.journal")

	fmt.Println("== starting the server")
	first, err := startServer(journal)
	if errors.Is(err, errUnsupported) {
		fmt.Println(notSupported)
		return nil
	}
	if err != nil {
		return err
	}
	defer first.kill()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+first.addr+"/checkout", nil)
	if err != nil {
		return err
	}
	go func() {
		if resp, err := http.DefaultClient.Do(req); err == nil { // fails: the server is killed mid-request
			_ = resp.Body.Close()
		}
	}()
	if _, err := first.wait(handlerWaiting); err != nil {
		return err
	}

	fmt.Println("== kill -9 while /checkout is in flight")
	first.kill() // SIGKILL: buffered debug lines never reached the output

	fmt.Println("== starting the server again")
	second, err := startServer(journal)
	if err != nil {
		return err
	}
	defer func() {
		_ = second.cmd.Process.Signal(os.Interrupt)
		second.kill()
	}()
	recovered := 0
	timeout := time.After(10 * time.Second)
	for {
		select {
		case l, ok := <-second.lines:
			if !ok {
				return errors.New("server exited before recovering")
			}
			fmt.Println(l)
			if strings.Contains(l, `"flightrec.flush_reason":"crash_recovery"`) {
				recovered++
			}
			if strings.Contains(l, "request interrupted by crash") {
				if recovered != 3 {
					return fmt.Errorf("recovered %d lines, want 2 debug lines and a summary", recovered)
				}
				fmt.Println("== recovered the in-flight request's debug logs")
				return nil
			}
		case <-timeout:
			return errors.New("no recovered logs after 10s")
		}
	}
}
