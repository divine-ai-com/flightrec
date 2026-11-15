// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/divine-ai-com/flightrec/internal/journal"
)

// maxRecoveryAttempts is how many times a journal file is recovered before it
// is abandoned, so a crash during recovery cannot become a crash loop.
const maxRecoveryAttempts = 3

// flushReasonCrashRecovery is the flush reason of recovered records.
const flushReasonCrashRecovery = "crash_recovery"

// recoveredScope collects the entries of one scope read from a journal.
type recoveredScope struct {
	start   scopeStart
	started bool // the start entry survived
	ended   bool
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

// recoverJournal recovers the scopes left open in the journal file at path,
// then zeros and removes the file. Problems are counted, never returned:
// recovery must not stop the service from starting.
func (st *recorderState) recoverJournal(path string) {
	defer func() { _ = journal.Discard(path) }()
	attempts, err := journal.IncrementAttempts(path)
	if err != nil {
		st.journalDiscarded.Add(1)
		return
	}
	if attempts > maxRecoveryAttempts {
		st.journalAbandoned.Add(1)
		return
	}
	data, err := journal.ReadFile(path, maxJournalSize)
	if err != nil {
		st.journalDiscarded.Add(1)
		return
	}
	// The old file's header gives its size and layout, so a changed
	// WithJournalSize does not lose it.
	h, entries, rs, err := journal.Read(data)
	if err != nil {
		st.journalDiscarded.Add(1)
		return
	}
	st.journalCorrupt.Add(uint64(rs.Invalid))

	scopes := map[uint64]*recoveredScope{}
	for _, e := range entries {
		sc := scopes[e.Scope]
		if sc == nil {
			sc = &recoveredScope{}
			scopes[e.Scope] = sc
		}
		switch e.Kind {
		case journal.KindStart:
			s, err := decodeScopeStart(e.Payload)
			if err != nil {
				st.journalCorrupt.Add(1)
				continue
			}
			sc.start, sc.started = s, true
		case journal.KindRecord:
			jr, err := decodeJournalRecord(e.Payload)
			if err != nil {
				st.journalCorrupt.Add(1)
				continue
			}
			sc.records = append(sc.records, jr)
		case journal.KindEnd:
			sc.ended = true
		}
	}

	open := make([]*recoveredScope, 0, len(scopes))
	for _, sc := range scopes {
		if sc.ended {
			continue
		}
		// Records of one scope logged from several goroutines can reach the
		// journal out of order; their own times order them.
		slices.SortStableFunc(sc.records, func(a, b journalRecord) int { return a.time.Compare(b.time) })
		open = append(open, sc)
	}
	slices.SortFunc(open, func(a, b *recoveredScope) int {
		return cmp.Or(a.first().Compare(b.first()), cmp.Compare(a.start.traceID, b.start.traceID))
	})
	for _, sc := range open {
		st.recoverScope(sc, h.Host, time.Unix(0, h.StartedAt))
	}
}

// recoverScope writes the records of one interrupted scope to Next, then its
// summary.
func (st *recorderState) recoverScope(sc *recoveredScope, host string, processStart time.Time) {
	traceID := sc.start.traceID
	if traceID == "" {
		// The start entry was overwritten: take the trace from a record.
		for _, jr := range sc.records {
			if jr.traceCtx.HasTraceID() {
				traceID = jr.traceCtx.TraceIDHex()
				break
			}
		}
	}
	scopeTrace, hasScopeTrace := ParseTraceIDHex(traceID)
	recovered := []slog.Attr{
		slog.Bool("flightrec.recovered", true),
		slog.String("flightrec.recovered_host", host),
		slog.Time("flightrec.recovered_process_start", processStart),
	}

	for i, jr := range sc.records {
		rec := slog.NewRecord(jr.time, jr.level, jr.message, 0)
		rec.AddAttrs(jr.attrs...)
		r := &Recorder{state: st, rootAttrs: jr.rootAttrs, frames: jr.frames}
		tc := jr.traceCtx
		if !tc.HasTraceID() && hasScopeTrace {
			tc.TraceID = scopeTrace
		}
		out := r.buildOutputRecord(rec, traceID, tc.SpanIDHex(), flushReasonCrashRecovery, false, 0)
		if !sc.started && i == 0 {
			// The oldest records of the scope were overwritten.
			out.AddAttrs(slog.Bool("flightrec.truncated", true))
		}
		out.AddAttrs(recovered...)
		_ = st.baseNext.Handle(st.injectTraceContext(context.Background(), tc), out)
	}
	st.journalRecoveredRecords.Add(uint64(len(sc.records)))
	st.journalRecoveredScopes.Add(1)

	summary := slog.NewRecord(st.now(), slog.LevelError, "request interrupted by crash", 0)
	summary.AddAttrs(
		slog.String("flightrec.trace_id", traceID),
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
	} else {
		summary.AddAttrs(slog.Bool("flightrec.truncated", true))
	}
	summary.AddAttrs(recovered...)
	ctx := context.Background()
	if hasScopeTrace {
		ctx = st.injectTraceContext(ctx, TraceContext{TraceID: scopeTrace})
	}
	_ = st.baseNext.Handle(ctx, summary)
}
