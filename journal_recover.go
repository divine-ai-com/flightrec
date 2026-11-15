// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/divine-ai-com/flightrec/internal/journal"
)

// maxRecoveryAttempts is how many times a journal file is recovered before it
// is abandoned, so a crash during recovery cannot become a crash loop.
const maxRecoveryAttempts = 3

// flushReasonCrashRecovery is the flush reason of recovered records.
const flushReasonCrashRecovery = "crash_recovery"

// recoverPending recovers the journal files a previous process left at path,
// and leaves path free for a new journal. The caller holds the lock file.
//
// A file at path.recovering is from a recovery that did not finish; it is
// retried first. A file at path is then moved to path.recovering before it is
// recovered, so a crash during recovery leaves it there for the next start,
// which counts the attempt. If it cannot be moved, it is recovered in place;
// the attempt counter is in the file itself.
//
// It returns an error only when a file that is not a journal is in the way,
// or a recovered file cannot be removed: deleting it is not ours to do, and
// leaving it would block every later recovery.
func (st *recorderState) recoverPending(path string) error {
	recovering := path + ".recovering"
	if err := st.recoverFile(recovering); err != nil {
		return err
	}
	switch journal.Classify(path) {
	case journal.ClassMissing:
		return nil
	case journal.ClassJournal:
		if os.Rename(path, recovering) == nil { //nolint:gosec // G703: the path is the journal path the application configured
			return st.recoverFile(recovering)
		}
	}
	return st.recoverFile(path)
}

// recoverFile recovers the journal file at path, if there is one, then zeros
// and removes it.
func (st *recorderState) recoverFile(path string) error {
	switch journal.Classify(path) {
	case journal.ClassMissing:
		return nil
	case journal.ClassSymlink:
		st.journalDiscarded.Add(1)
	case journal.ClassDead:
		// Creation or zeroing was cut short: nothing to recover.
	case journal.ClassJournal:
		st.recoverJournal(path)
	default:
		return fmt.Errorf("flightrec: start journal: %s is in the way and is not a journal file; remove it", path)
	}
	if err := journal.Discard(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("flightrec: start journal: remove %s: %w", path, err)
	}
	return nil
}

// recoveredScope is one interrupted scope read from a journal.
type recoveredScope struct {
	id      uint64
	start   scopeStart
	started bool // the start entry survived
	dropped int  // records too large to journal
	records []journalRecord
}

// first returns the time the scope is ordered by: its start, or its oldest
// surviving record when the start entry was overwritten.
func (sc *recoveredScope) first() time.Time {
	if sc.started || len(sc.records) == 0 {
		return sc.start.start
	}
	return sc.records[0].time
}

// recoverJournal writes the scopes left open in the journal file at path to
// Next. Problems are counted, never returned: recovery must not stop the
// service from starting.
//
// The file is read one stripe at a time. A scope's entries all sit in one
// stripe, in the order they were written, so a first pass over a stripe finds
// the scopes that ended and a second decodes only the others: memory stays at
// one stripe plus the interrupted scopes.
func (st *recorderState) recoverJournal(path string) {
	r, err := journal.OpenRecovering(path, maxJournalSize)
	if err != nil {
		st.journalDiscarded.Add(1)
		return
	}
	defer func() { _ = r.Close() }()
	attempts, err := r.IncrementAttempts()
	if err != nil {
		st.journalDiscarded.Add(1)
		return
	}
	if attempts > maxRecoveryAttempts {
		st.journalAbandoned.Add(1)
		return
	}

	// The old file's header gives its size and layout, so a changed
	// WithJournalSize does not lose it.
	h := r.Header
	var open []*recoveredScope
	var buf []byte
	for s := 0; s < int(h.Stripes); s++ {
		ended := map[uint64]struct{}{}
		var rs journal.ReadStats
		buf, rs, err = r.Stripe(s, buf, func(e journal.Entry) {
			if e.Kind == journal.KindEnd {
				ended[e.Scope] = struct{}{}
			}
		})
		if err != nil {
			st.journalCorrupt.Add(1)
			continue
		}
		st.journalCorrupt.Add(uint64(rs.Invalid))

		scopes := map[uint64]*recoveredScope{}
		journal.WalkStripe(h, buf, s, func(e journal.Entry) {
			if _, done := ended[e.Scope]; done {
				return
			}
			sc := scopes[e.Scope]
			if sc == nil {
				sc = &recoveredScope{id: e.Scope}
				scopes[e.Scope] = sc
				open = append(open, sc)
			}
			switch e.Kind {
			case journal.KindStart:
				s, err := decodeScopeStart(e.Payload)
				if err != nil {
					st.journalCorrupt.Add(1)
					return
				}
				sc.start, sc.started = s, true
			case journal.KindDropped:
				sc.dropped++
			case journal.KindRecord:
				jr, err := decodeJournalRecord(e.Payload)
				if err != nil {
					st.journalCorrupt.Add(1)
					return
				}
				sc.records = append(sc.records, jr)
			}
		})
	}

	// Within a scope, records stay in the order they were journaled, which is
	// the order a flush writes them.
	slices.SortFunc(open, func(a, b *recoveredScope) int {
		return cmp.Or(a.first().Compare(b.first()), cmp.Compare(a.id, b.id))
	})
	for _, sc := range open {
		st.recoverScope(sc, h.Host, time.Unix(0, h.StartedAt))
	}
}

// recoverScope writes the records of one interrupted scope to Next, then its
// summary.
func (st *recorderState) recoverScope(sc *recoveredScope, host string, processStart time.Time) {
	traceID := sc.start.traceID
	// The records' own trace context wins over the start entry: it reflects
	// a span's trace adopted after the scope started (Scope.AdoptGeneratedTraceID),
	// and it survives when the start entry was overwritten.
	for i := len(sc.records) - 1; i >= 0; i-- {
		if tc := sc.records[i].traceCtx; tc.HasTraceID() {
			traceID = tc.TraceIDHex()
			break
		}
	}
	scopeTrace, hasScopeTrace := ParseTraceIDHex(traceID)
	recovered := []slog.Attr{
		slog.Bool("flightrec.recovered", true),
		slog.String("flightrec.recovered_host", host),
		slog.Time("flightrec.recovered_process_start", processStart),
	}
	if traceID == "" {
		// Nothing else tells apart the records of scopes without a trace.
		recovered = append(recovered, slog.Uint64("flightrec.recovered_scope", sc.id))
	}

	written := 0
	for i, jr := range sc.records {
		rec := slog.NewRecord(jr.time, jr.level, jr.message, 0)
		rec.AddAttrs(jr.attrs...)
		r := &Recorder{state: st, rootAttrs: jr.rootAttrs, frames: jr.frames}
		tc := jr.traceCtx
		if !tc.HasTraceID() && hasScopeTrace {
			tc.TraceID = scopeTrace
		}
		out := r.buildOutputRecord(rec, traceID, tc.SpanIDHex(), flushReasonCrashRecovery, false, 0)
		if i == 0 && (!sc.started || sc.dropped > 0) {
			// The oldest records were overwritten, or some were too large
			// to journal.
			out.AddAttrs(slog.Bool("flightrec.truncated", true))
			if sc.dropped > 0 {
				out.AddAttrs(slog.Int("flightrec.dropped_records", sc.dropped))
			}
		}
		out.AddAttrs(recovered...)
		if st.handleRecovered(st.injectTraceContext(context.Background(), tc), out) {
			written++
		}
	}
	st.journalRecoveredRecords.Add(uint64(written))
	st.journalRecoveredScopes.Add(1)

	summary := slog.NewRecord(st.now(), slog.LevelError, "request interrupted by crash", 0)
	if traceID != "" {
		summary.AddAttrs(slog.String("flightrec.trace_id", traceID))
	}
	summary.AddAttrs(
		slog.String("flightrec.flush_reason", flushReasonCrashRecovery),
		slog.Int("flightrec.recovered_records", len(sc.records)),
	)
	if sc.start.method != "" {
		summary.AddAttrs(slog.String("method", sc.start.method))
	}
	if sc.start.path != "" {
		summary.AddAttrs(slog.String("path", sc.start.path))
	}
	if sc.started {
		summary.AddAttrs(slog.Time("start", sc.start.start))
	}
	if !sc.started || sc.dropped > 0 {
		summary.AddAttrs(slog.Bool("flightrec.truncated", true))
	}
	if sc.dropped > 0 {
		summary.AddAttrs(slog.Int("flightrec.dropped_records", sc.dropped))
	}
	summary.AddAttrs(recovered...)
	ctx := context.Background()
	if hasScopeTrace {
		ctx = st.injectTraceContext(ctx, TraceContext{TraceID: scopeTrace})
	}
	st.handleRecovered(ctx, summary)
}

// handleRecovered writes a recovered record to Next and reports whether it
// was written. A panic in Next is recovered: one bad record must not cost the
// rest of the journal, which is deleted after recovery.
func (st *recorderState) handleRecovered(ctx context.Context, rec slog.Record) (ok bool) {
	defer func() {
		if recover() != nil {
			st.journalRecoveryErrors.Add(1)
			ok = false
		}
	}()
	if err := st.baseNext.Handle(ctx, rec); err != nil {
		st.journalRecoveryErrors.Add(1)
		return false
	}
	return true
}
