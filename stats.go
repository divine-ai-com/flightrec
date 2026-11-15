// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"time"
)

// Stats reports cumulative and point-in-time metrics for a Recorder.
type Stats struct {
	ActiveScopes              int64
	BufferedBytes             int64
	ScopesStarted             uint64
	ScopesFlushed             uint64
	ScopesHealthyDropped      uint64
	ScopesEvictedTotalCap     uint64
	RecordsPassedThrough      uint64
	RecordsBuffered           uint64
	RecordsFlushed            uint64
	RecordsHealthyDropped     uint64
	RecordsDroppedPerScopeCap uint64
	RecordsDroppedTotalCap    uint64

	// FlushMarkersSent counts responses that carried the Flightrec-Flush
	// header because their scope was triggered before headers were written.
	FlushMarkersSent uint64
	// FlushMarkersMissed counts triggered scopes whose response headers were
	// already written, so the Flightrec-Flush header could not be added.
	FlushMarkersMissed uint64
	// DownstreamTriggers counts open scopes that a FlushHeader on a response
	// read through NewTransport triggered. Responses for scopes that were
	// already triggered or had ended are not counted.
	DownstreamTriggers uint64
	// MarkersRejected counts FlushHeader values from responses that were not
	// a built-in reason and were ignored.
	MarkersRejected uint64

	// RetainedScopes and RetainedBytes are the healthy scopes currently held
	// for Options.Retain and the bytes they use.
	RetainedScopes int64
	RetainedBytes  int64
	// RetainedOldestAge is the age of the oldest retained scope, which is the
	// effective retention window under the current load.
	RetainedOldestAge time.Duration
	// RetainedFlushed counts retained scopes written by FlushTrace.
	RetainedFlushed uint64
	// RetainedExpired counts retained scopes dropped after Options.Retain.
	RetainedExpired uint64
	// RetainedEvicted counts retained scopes dropped early, to stay within
	// MaxRetained or the per-trace limit, including scopes larger than
	// MaxRetained on their own, which are dropped instead of retained.
	RetainedEvicted uint64

	// RemoteFlushes counts scopes flushed or triggered because of a failure
	// elsewhere in the same trace: in another process, reported through
	// StartNotifier, or in another scope of this process.
	RemoteFlushes uint64
	// NotifierPublished counts trace IDs announced through the notifier.
	NotifierPublished uint64
	// NotifierPublishDropped counts announcements dropped because the queue
	// was full or the publish rate limit was reached.
	NotifierPublishDropped uint64
	// NotifierPublishErrors counts announcements in failed Notifier.Publish
	// calls. Like NotifierPublished, it counts trace IDs, not calls.
	NotifierPublishErrors uint64
	// NotifierDeliveryDropped counts received notices dropped under load:
	// the delivery queue was full or the remote flush rate limit was reached.
	NotifierDeliveryDropped uint64
	// NotifierRejected counts received messages or notices that were
	// malformed (invalid encoding or version, too large, an invalid trace ID
	// or a reason that is not built in) or, with WithSigningKey, unsigned,
	// wrongly signed or older than two minutes.
	NotifierRejected uint64
	// NotifierSubscribeErrors counts Notifier.Subscribe calls that failed
	// and were retried.
	NotifierSubscribeErrors uint64

	// JournalRecordsWritten counts buffered records written to the journal
	// started with StartJournal; JournalBytesWritten counts all journal
	// bytes, start and end entries included.
	JournalRecordsWritten uint64
	JournalBytesWritten   uint64
	// JournalWraps counts times the journal ring reused its oldest block.
	JournalWraps uint64
	// JournalDropped counts entries not written: larger than a journal
	// block, or written after a fault disabled the journal.
	JournalDropped uint64
	// JournalFaults counts memory faults on the journal file (for example
	// after it was truncated or the disk failed); the journal stops after one.
	JournalFaults uint64
	// JournalOldestAge is the time span the journal currently covers: older
	// records have been overwritten.
	JournalOldestAge time.Duration

	// The counters below describe recovery at StartJournal and add up over
	// the life of the Recorder.
	//
	// JournalRecoveredScopes counts interrupted requests recovered from a
	// journal left by a killed process; JournalRecoveredRecords counts their
	// records Next accepted.
	JournalRecoveredScopes  uint64
	JournalRecoveredRecords uint64
	// JournalCorruptEntries counts journal entries or blocks that failed
	// their checks during recovery: torn writes, or damage to the file.
	JournalCorruptEntries uint64
	// JournalDiscardedFiles counts journal files deleted without recovery:
	// a damaged header, a symlink, or a file not private to this user.
	JournalDiscardedFiles uint64
	// JournalAbandonedRecoveries counts journal files deleted after three
	// recovery attempts that did not finish, such as a crash during recovery.
	JournalAbandonedRecoveries uint64
	// JournalRecoveryErrors counts recovered records that Next failed to
	// write, by returning an error or panicking.
	JournalRecoveryErrors uint64
}

// Stats returns a snapshot of the recorder's current and cumulative metrics.
func (r *Recorder) Stats() Stats {
	st := r.state
	// Gauges first: computing them expires old retained scopes, which the
	// counters below must include.
	retainedScopes, retainedBytes, retainedOldest := st.retainedGauges()
	stats := Stats{
		ActiveScopes:              st.activeScopes.Load(),
		BufferedBytes:             st.totalBytes.Load(),
		ScopesStarted:             st.scopesStarted.Load(),
		ScopesFlushed:             st.scopesFlushed.Load(),
		ScopesHealthyDropped:      st.scopesHealthyDropped.Load(),
		ScopesEvictedTotalCap:     st.scopesEvictedTotalCap.Load(),
		RecordsPassedThrough:      st.recordsPassedThrough.Load(),
		RecordsBuffered:           st.recordsBuffered.Load(),
		RecordsFlushed:            st.recordsFlushed.Load(),
		RecordsHealthyDropped:     st.recordsHealthyDropped.Load(),
		RecordsDroppedPerScopeCap: st.recordsDroppedPerScopeCap.Load(),
		RecordsDroppedTotalCap:    st.recordsDroppedTotalCap.Load(),
		FlushMarkersSent:          st.flushMarkersSent.Load(),
		FlushMarkersMissed:        st.flushMarkersMissed.Load(),
		DownstreamTriggers:        st.downstreamTriggers.Load(),
		MarkersRejected:           st.markersRejected.Load(),
		RetainedFlushed:           st.retainedFlushed.Load(),
		RetainedExpired:           st.retainedExpired.Load(),
		RetainedEvicted:           st.retainedEvicted.Load(),
		RemoteFlushes:             st.remoteFlushes.Load(),
		NotifierPublished:         st.notifierPublished.Load(),
		NotifierPublishDropped:    st.notifierPublishDropped.Load(),
		NotifierPublishErrors:     st.notifierPublishErrors.Load(),
		NotifierDeliveryDropped:   st.notifierDeliveryDropped.Load(),
		NotifierRejected:          st.notifierRejected.Load(),
		NotifierSubscribeErrors:   st.notifierSubscribeErrors.Load(),
	}
	stats.RetainedScopes, stats.RetainedBytes, stats.RetainedOldestAge = retainedScopes, retainedBytes, retainedOldest
	st.journalStats(&stats)
	return stats
}
