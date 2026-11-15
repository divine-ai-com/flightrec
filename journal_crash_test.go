// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package flightrec

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const crashChildEnv = "FLIGHTREC_CRASH_CHILD"

// crashPayload is the attribute value written with record seq of worker w,
// so the parent can check that every recovered record is intact.
func crashPayload(w, seq int) string {
	return strings.Repeat(strconv.Itoa(w*1_000_003+seq), 1+seq%7)
}

// TestMain runs the crash child when the parent test starts this binary
// again with crashChildEnv set.
func TestMain(m *testing.M) {
	if path := os.Getenv(crashChildEnv); path != "" {
		runCrashChild(path)
		return
	}
	os.Exit(m.Run())
}

// runCrashChild journals scopes from several goroutines until it is killed.
// Workers end most scopes and leave the last one of each open.
func runCrashChild(path string) {
	rec := New(Options{Next: slog.DiscardHandler, BufferLeveler: slog.LevelDebug})
	if _, err := rec.StartJournal(path, WithJournalSize(1<<20)); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
	log := slog.New(rec).With("child", true).WithGroup("work")
	for w := range 8 {
		go func() {
			seq := 0
			for {
				ctx, sc := StartScope(context.Background(), rec, ScopeMeta{TraceID: fmt.Sprintf("w%d-s%d", w, seq), Method: "GET", Path: "/crash"})
				n := 1 + seq%5
				for range n {
					log.DebugContext(ctx, "step", "w", w, "seq", seq, "payload", crashPayload(w, seq))
					seq++
				}
				if seq%3 == 0 {
					sc.Trigger("test")
				}
				sc.End(ctx, 200)
			}
		}()
	}
	fmt.Println("ready")
	select {}
}

func TestCrashRecoveryAfterKill(t *testing.T) {
	if testing.Short() {
		t.Skip("starts child processes")
	}
	path := filepath.Join(t.TempDir(), "j")
	for round := range 5 {
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), crashChildEnv+"="+path)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		if strings.TrimSpace(line) != "ready" {
			_ = cmd.Process.Kill()
			t.Fatalf("child: %q", line)
		}
		time.Sleep(time.Duration(20+rand.IntN(200)) * time.Millisecond)
		if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		_ = cmd.Wait()

		var out lineBuffer
		rec := New(Options{Next: slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})})
		stop, err := rec.StartJournal(path, WithJournalSize(2<<20))
		if err != nil {
			t.Fatal(err)
		}
		st := rec.Stats()
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if st.JournalRecoveredScopes == 0 || st.JournalDiscardedFiles != 0 || st.JournalAbandonedRecoveries != 0 {
			t.Fatalf("round %d: stats %+v", round, st)
		}
		records, summaries := 0, 0
		for _, l := range out.lines(t) {
			if l["flightrec.recovered"] != true || l["flightrec.flush_reason"] != "crash_recovery" {
				t.Fatalf("round %d: untagged line %v", round, l)
			}
			if l["msg"] == "request interrupted by crash" {
				summaries++
				continue
			}
			records++
			work, _ := l["work"].(map[string]any)
			w, _ := work["w"].(float64)
			seq, _ := work["seq"].(float64)
			if l["msg"] != "step" || l["child"] != true || work["payload"] != crashPayload(int(w), int(seq)) {
				t.Fatalf("round %d: damaged record %v", round, l)
			}
			// A scope whose start entry the ring overwrote has no trace ID:
			// these IDs are not W3C trace IDs, so records do not carry them.
			if tid, ok := l["flightrec.trace_id"]; ok && !strings.HasPrefix(fmt.Sprint(tid), fmt.Sprintf("w%d-s", int(w))) {
				t.Fatalf("round %d: record of worker %d under trace %v", round, int(w), l["flightrec.trace_id"])
			}
		}
		if uint64(summaries) != st.JournalRecoveredScopes || uint64(records) != st.JournalRecoveredRecords {
			t.Fatalf("round %d: %d summaries, %d records; stats %+v", round, summaries, records, st)
		}
		// At most one open scope per worker; a scope may also look open if the
		// kill came between its last record and its end entry.
		if summaries > 8 {
			t.Errorf("round %d: %d interrupted scopes from 8 workers", round, summaries)
		}
		t.Logf("round %d: recovered %d scopes, %d records, %d corrupt", round, summaries, records, st.JournalCorruptEntries)
	}
}
