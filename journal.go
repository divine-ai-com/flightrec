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
	records atomic.Uint64
}

var journalScratch = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}

// StartJournal writes the buffered records of every scope to a file at path,
// so that a process killed without warning (SIGKILL, an out-of-memory kill, a
// grace period that ran out) does not lose the records of the requests it was
// handling. The next StartJournal on the same path, in the restarted process,
// recovers them.
//
// The file is created with mode 0600 and locked; a second process or
// Recorder using the same path gets an error. It is written with zeros in
// full when created, then written to through a shared memory mapping, which
// the kernel keeps when the process dies. It does not survive the loss of the
// machine, and it must be on a volume that outlives the process: in
// Kubernetes, an emptyDir volume of the pod.
//
// StartJournal returns an error only when no journal can run: an
// unsupported platform (ErrJournalUnsupported), an invalid size, a journal
// already running on this Recorder (ErrJournalRunning), or a file that
// cannot be created. stop stops journaling, zeros the file and removes it.
// Scopes still open when stop is called are not recovered.
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

	if err := st.prepareJournalPath(path); err != nil {
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
		return nil, fmt.Errorf("flightrec: start journal: %w", err)
	}
	js := &journalState{file: f}
	st.journal.Store(js)

	var once sync.Once
	return func() error {
		var err error
		once.Do(func() {
			st.journalMu.Lock()
			defer st.journalMu.Unlock()
			st.journal.CompareAndSwap(js, nil)
			err = f.Close(true)
			if rmErr := os.Remove(path); err == nil {
				err = rmErr
			}
		})
		return err
	}, nil
}

// prepareJournalPath makes path free for a new journal. A file left there by
// a previous process is moved aside for recovery.
func (st *recorderState) prepareJournalPath(path string) error {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	// A journal still locked belongs to a running process or Recorder; it is
	// left alone. The lock is held until the file has been moved.
	unlock, err := journal.LockFile(path)
	if err != nil {
		return fmt.Errorf("flightrec: start journal: %w", err)
	}
	defer unlock()
	recovering := path + ".recovering"
	if _, err := os.Lstat(recovering); err == nil {
		// An earlier recovery did not finish; it is retried, and the newer
		// file is discarded.
		return os.Remove(path)
	}
	return os.Rename(path, recovering)
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
	*bp = b
	journalScratch.Put(bp)
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
		s.JournalOldestAge = st.now().Sub(time.Unix(0, oldest))
	}
}
