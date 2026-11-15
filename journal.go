// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/divine-ai-com/flightrec/internal/journal"
)

// Journal size limits.
const (
	// DefaultJournalSize is the journal file size used without
	// WithJournalSize.
	DefaultJournalSize int64 = 16 << 20
	minJournalSize     int64 = 1 << 20
	maxJournalSize     int64 = 1 << 30
	journalStripes           = 8
)

// ErrJournalUnsupported is returned by StartJournal on platforms without
// shared memory mappings (Windows, for now).
var ErrJournalUnsupported = errors.New("flightrec: journal not supported on this platform")

// ErrJournalRunning is returned by StartJournal when the Recorder already has
// a journal.
var ErrJournalRunning = errors.New("flightrec: journal already running")

// JournalOption configures StartJournal.
type JournalOption interface {
	applyJournal(*journalConfig)
}

type journalConfig struct {
	size int64
}

type journalSizeOption int64

func (o journalSizeOption) applyJournal(c *journalConfig) { c.size = int64(o) }

// WithJournalSize sets the journal file size in bytes: from 1 MiB to 1 GiB,
// rounded up to a multiple of the page size. The default is 16 MiB.
//
// Size it as journaled bytes per second times the longest request you want
// to recover; Stats.JournalOldestAge shows the time span the journal covers.
func WithJournalSize(bytes int64) JournalOption { return journalSizeOption(bytes) }

// journalState is a running journal.
type journalState struct {
	file    *journal.File
	unlock  func() // releases path.lock
	records atomic.Uint64
}

// maxScratch is the largest encode buffer kept for reuse.
const maxScratch = journal.MaxPayload

var journalScratch = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}

// StartJournal writes the buffered records of every scope to a file at path,
// so that a process killed without warning (SIGKILL, an out-of-memory kill, a
// grace period that ran out) does not lose the records of the requests it was
// handling. Scopes started before StartJournal are not journaled.
//
// Before it starts journaling, StartJournal recovers the journal a killed
// process left at path: for each request that was in flight, it writes the
// buffered records to Next, in order, with their original time, level and
// attributes, tagged flightrec.flush_reason=crash_recovery and
// flightrec.recovered=true, then one "request interrupted by crash" summary.
// Recovery writes each record at least once: a crash during a flush or during
// recovery can write some records again on the next start. Recovered records
// have no source location. Problems found while recovering are counted in
// Stats, never returned. Recovery blocks StartJournal; it reads the old file
// one stripe (an eighth) at a time and decodes only the interrupted requests,
// so it takes well under a second for the default size.
//
// The journal file is created with mode 0600 and written with zeros in full,
// then written to through a shared memory mapping, which the kernel keeps
// when the process dies. A lock file, path.lock, is held while the journal
// runs, so a second process or Recorder using the same path gets an error;
// the lock file is left in place. It does not survive the loss of the
// machine, and it must be on a volume that outlives the process: in
// Kubernetes, an emptyDir volume of the pod.
//
// StartJournal returns an error only when no journal can run: an
// unsupported platform (ErrJournalUnsupported), an invalid size, a journal
// already running on this Recorder (ErrJournalRunning), a path in use by
// another process or Recorder, something other than a journal at path or
// path.recovering (it is never deleted), or a file that cannot be created or
// removed. stop stops journaling, zeros the file and removes it.
// Scopes still open when stop is called are not recovered: call stop after
// the server has drained its requests. The Journal* fields of Stats describe
// the running journal and read zero after stop, except the recovery counters.
func (r *Recorder) StartJournal(path string, opts ...JournalOption) (stop func() error, err error) {
	if !journal.Supported {
		return nil, ErrJournalUnsupported
	}
	cfg := journalConfig{size: DefaultJournalSize}
	for _, o := range opts {
		o.applyJournal(&cfg)
	}
	if cfg.size < minJournalSize || cfg.size > maxJournalSize {
		return nil, fmt.Errorf("flightrec: journal size %d out of range [%d, %d]", cfg.size, minJournalSize, maxJournalSize)
	}
	if page := int64(os.Getpagesize()); cfg.size%page != 0 {
		cfg.size += page - cfg.size%page
	}

	st := r.state
	st.journalMu.Lock()
	defer st.journalMu.Unlock()
	if st.journal.Load() != nil {
		return nil, ErrJournalRunning
	}

	unlock, err := journal.AcquireLock(path + ".lock")
	if err != nil {
		return nil, fmt.Errorf("flightrec: start journal: %w", err)
	}
	if err := st.recoverPending(path); err != nil {
		unlock()
		return nil, err
	}

	// Each stripe needs at least two blocks; small journals get fewer stripes.
	stripes := min(int64(journalStripes), (cfg.size-journal.HeaderSize)/(2*journal.BlockSize))
	host, _ := os.Hostname()
	f, err := journal.Create(path, journal.Header{
		Size:      cfg.size,
		Stripes:   uint32(stripes),
		StartedAt: time.Now().UnixNano(),
		Host:      host,
	})
	if err != nil {
		unlock()
		return nil, fmt.Errorf("flightrec: start journal: %w", err)
	}
	js := &journalState{file: f, unlock: unlock}
	st.journal.Store(js)

	var once sync.Once
	return func() error {
		var err error
		once.Do(func() {
			st.journalMu.Lock()
			defer st.journalMu.Unlock()
			defer unlock()
			st.journal.CompareAndSwap(js, nil)
			err = f.Close(true)
			if rmErr := os.Remove(path); err == nil { //nolint:gosec // G703: the path is the journal path the application configured
				err = rmErr
			}
		})
		return err
	}, nil
}

// journalStart writes a start entry for a new scope if a journal is running.
func (st *recorderState) journalStart(sc *Scope) {
	js := st.journal.Load()
	if js == nil {
		return
	}
	sc.jid = st.journalIDs.Add(1)
	sc.journal = js
	bp := journalScratch.Get().(*[]byte)
	b := appendScopeStart((*bp)[:0], scopeStart{start: sc.startTime, traceID: sc.meta.TraceID, method: sc.meta.Method, path: sc.meta.Path})
	js.file.Append(sc.jid, journal.KindStart, sc.jid, b)
	putScratch(bp, b)
}

// journalRecord writes a buffered record of sc to its journal.
func (st *recorderState) journalRecord(sc *Scope, r *Recorder, rec slog.Record, tc TraceContext) {
	js := sc.journal
	if js == nil || st.journal.Load() != js {
		return
	}
	bp := journalScratch.Get().(*[]byte)
	b := appendJournalRecord((*bp)[:0], r, rec, tc)
	if js.file.Append(sc.jid, journal.KindRecord, sc.jid, b) {
		js.records.Add(1)
	}
	putScratch(bp, b)
}

// putScratch returns an encode buffer to the pool unless it grew past what a
// journal entry can hold.
func putScratch(bp *[]byte, b []byte) {
	if cap(b) > maxScratch {
		return
	}
	*bp = b
	journalScratch.Put(bp)
}

// journalEnd writes the end entry of sc: nothing of it needs recovering.
func (st *recorderState) journalEnd(sc *Scope) {
	js := sc.journal
	if js == nil || st.journal.Load() != js {
		return
	}
	js.file.Append(sc.jid, journal.KindEnd, sc.jid, nil)
}

// journalStats fills the journal fields of s.
func (st *recorderState) journalStats(s *Stats) {
	s.JournalRecoveredScopes = st.journalRecoveredScopes.Load()
	s.JournalRecoveredRecords = st.journalRecoveredRecords.Load()
	s.JournalCorruptEntries = st.journalCorrupt.Load()
	s.JournalDiscardedFiles = st.journalDiscarded.Load()
	s.JournalAbandonedRecoveries = st.journalAbandoned.Load()
	s.JournalRecoveryErrors = st.journalRecoveryErrors.Load()
	js := st.journal.Load()
	if js == nil {
		return
	}
	ws := js.file.Stats()
	s.JournalRecordsWritten = js.records.Load()
	s.JournalBytesWritten = ws.Bytes
	s.JournalWraps = ws.Wraps
	s.JournalDropped = ws.Dropped
	s.JournalFaults = ws.Faults
	if oldest := js.file.OldestBlockTime(); oldest > 0 {
		s.JournalOldestAge = time.Since(time.Unix(0, oldest)) // the writer stamps blocks with the wall clock
	}
}
