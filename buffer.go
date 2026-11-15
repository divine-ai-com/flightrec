// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"log/slog"
	"sync"
)

var ringBufPool = sync.Pool{
	New: func() any {
		s := make([]bufferedEntry, initialRingCapacity)
		return &s
	},
}

type bufferedEntry struct {
	rec      slog.Record
	recorder *Recorder
	size     int64
	traceCtx TraceContext
}

func (s *Scope) bufferRecord(rec slog.Record, recorder *Recorder, payloadSize int64, tc TraceContext) bool {
	st := s.state
	maxPerScope := st.opts.MaxPerScope

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}

	if entrySlotSize+payloadSize > maxPerScope {
		s.truncated = true
		s.droppedInScope++
		s.mu.Unlock()
		st.recordsDroppedPerScopeCap.Add(1)
		return true
	}

	prevBytes := s.bytes
	var evictedCount uint64

	if len(s.buf) == 0 {
		s.initRingLocked(payloadSize, maxPerScope)
	} else if s.count == len(s.buf) {
		if !s.tryGrowRingLocked(payloadSize, maxPerScope) {
			_ = s.popOldestLocked()
			evictedCount++
		}
	}

	for s.count > 0 && s.ringBytes+s.payloadBytes+payloadSize > maxPerScope {
		_ = s.popOldestLocked()
		evictedCount++
	}

	if evictedCount > 0 {
		s.truncated = true
		s.droppedInScope += evictedCount
		st.recordsDroppedPerScopeCap.Add(evictedCount)
	}

	idx := (s.head + s.count) % len(s.buf)
	s.buf[idx] = bufferedEntry{
		rec:      rec,
		recorder: recorder,
		size:     payloadSize,
		traceCtx: tc,
	}
	s.count++
	s.payloadBytes += payloadSize
	s.bytes = s.ringBytes + s.payloadBytes
	delta := s.bytes - prevBytes
	if s.bytes > 0 {
		// Hold s.mu while linking so a scope that is closing concurrently
		// can never be re-linked after End. Lock order is always s.mu -> listMu.
		st.ensureLinked(s)
	}
	s.mu.Unlock()

	st.recordsBuffered.Add(1)
	newTotal := st.totalBytes.Add(delta)
	if newTotal > st.opts.MaxTotal {
		st.enforceTotalCap(s)
	}
	return true
}

func (s *Scope) initRingLocked(payloadSize, maxPerScope int64) {
	if int64(initialRingCapacity)*entrySlotSize+payloadSize <= maxPerScope {
		bp := ringBufPool.Get().(*[]bufferedEntry)
		s.buf = *bp
		s.ringBytes = int64(len(s.buf)) * entrySlotSize
		s.head = 0
		s.count = 0
		return
	}
	maxSlots := int((maxPerScope - payloadSize) / entrySlotSize)
	if maxSlots < 1 {
		maxSlots = 1
	}
	if maxSlots > initialRingCapacity {
		maxSlots = initialRingCapacity
	}
	s.buf = make([]bufferedEntry, maxSlots)
	s.ringBytes = int64(maxSlots) * entrySlotSize
	s.head = 0
	s.count = 0
}

func (s *Scope) tryGrowRingLocked(payloadSize, maxPerScope int64) bool {
	curCap := len(s.buf)
	nextCap := curCap * 2
	if curCap < initialRingCapacity {
		nextCap = initialRingCapacity
		if int64(nextCap)*entrySlotSize+s.payloadBytes+payloadSize > maxPerScope {
			nextCap = curCap + 1
		}
	}
	if int64(nextCap)*entrySlotSize+s.payloadBytes+payloadSize > maxPerScope {
		return false
	}

	var nb []bufferedEntry
	if nextCap == initialRingCapacity {
		bp := ringBufPool.Get().(*[]bufferedEntry)
		nb = *bp
	} else {
		nb = make([]bufferedEntry, nextCap)
	}

	for i := 0; i < s.count; i++ {
		idx := (s.head + i) % curCap
		nb[i] = s.buf[idx]
		s.buf[idx] = bufferedEntry{}
	}
	releaseRingSlice(s.buf)
	s.buf = nb
	s.head = 0
	s.ringBytes = int64(len(nb)) * entrySlotSize
	return true
}

func releaseRingSlice(b []bufferedEntry) {
	if len(b) == initialRingCapacity {
		ringBufPool.Put(&b)
	}
}

func (s *Scope) popOldestLocked() bufferedEntry {
	e := s.buf[s.head]
	s.buf[s.head] = bufferedEntry{}
	s.head = (s.head + 1) % len(s.buf)
	s.count--
	if s.count == 0 {
		s.head = 0
	}
	s.payloadBytes -= e.size
	s.bytes = s.ringBytes + s.payloadBytes
	return e
}

func (s *Scope) clearAndReleasePoolLocked() (droppedCount int, freedBytes int64) {
	droppedCount = s.count
	freedBytes = s.bytes
	for i := 0; i < s.count; i++ {
		idx := (s.head + i) % len(s.buf)
		s.buf[idx] = bufferedEntry{}
	}
	releaseRingSlice(s.buf)
	s.buf = nil
	s.head = 0
	s.count = 0
	s.ringBytes = 0
	s.payloadBytes = 0
	s.bytes = 0
	return droppedCount, freedBytes
}

func (s *Scope) drainAndReleasePoolLocked() ([]bufferedEntry, int64) {
	freed := s.bytes
	if s.count == 0 {
		releaseRingSlice(s.buf)
		s.buf = nil
		s.head = 0
		s.ringBytes = 0
		s.payloadBytes = 0
		s.bytes = 0
		return nil, freed
	}
	out := make([]bufferedEntry, s.count)
	for i := 0; i < s.count; i++ {
		idx := (s.head + i) % len(s.buf)
		out[i] = s.buf[idx]
		s.buf[idx] = bufferedEntry{}
	}
	releaseRingSlice(s.buf)
	s.buf = nil
	s.head = 0
	s.count = 0
	s.ringBytes = 0
	s.payloadBytes = 0
	s.bytes = 0
	return out, freed
}

func (st *recorderState) ensureLinked(sc *Scope) {
	st.listMu.Lock()
	if !sc.inList && !sc.closed {
		sc.prev = st.newest
		sc.next = nil
		if st.newest != nil {
			st.newest.next = sc
		} else {
			st.oldest = sc
		}
		st.newest = sc
		sc.inList = true
	}
	st.listMu.Unlock()
}

func (st *recorderState) unlinkScope(sc *Scope) {
	st.listMu.Lock()
	if sc.inList {
		if sc.prev != nil {
			sc.prev.next = sc.next
		} else {
			st.oldest = sc.next
		}
		if sc.next != nil {
			sc.next.prev = sc.prev
		} else {
			st.newest = sc.prev
		}
		sc.prev = nil
		sc.next = nil
		sc.inList = false
	}
	st.listMu.Unlock()
}

func (st *recorderState) enforceTotalCap(current *Scope) {
	for st.totalBytes.Load() > st.opts.MaxTotal {
		st.listMu.Lock()
		victim := st.oldest
		for victim != nil && victim == current && victim.next != nil {
			victim = victim.next
		}
		if victim == nil {
			st.listMu.Unlock()
			return
		}
		st.listMu.Unlock()

		victim.mu.Lock()
		if victim.closed {
			st.unlinkScope(victim)
			victim.mu.Unlock()
			continue
		}
		var dropped uint64
		var freed int64
		if victim.count > 0 || victim.bytes > 0 {
			c, b := victim.clearAndReleasePoolLocked()
			dropped = uint64(c)
			freed = b
			victim.truncated = true
			victim.droppedInScope += dropped
		}
		alreadyEvicted := victim.evictedByTotalCap
		victim.evictedByTotalCap = true
		st.unlinkScope(victim)
		victim.mu.Unlock()

		if freed > 0 {
			st.totalBytes.Add(-freed)
		}
		if dropped > 0 {
			st.recordsDroppedTotalCap.Add(dropped)
		}
		if !alreadyEvicted {
			st.scopesEvictedTotalCap.Add(1)
		}
		if victim == current {
			return
		}
	}
}
