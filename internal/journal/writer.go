// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package journal

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Writer appends entries to a journal held in a byte slice. It is safe for
// concurrent use. Entries with the same stripe key are kept in order.
type Writer struct {
	data    []byte
	hdr     Header
	stripes []stripe
	now     func() int64

	disabled atomic.Bool
	entries  atomic.Uint64
	bytes    atomic.Uint64
	wraps    atomic.Uint64
	dropped  atomic.Uint64
	faults   atomic.Uint64
}

type stripe struct {
	mu      sync.Mutex
	base    int     // offset of the stripe's first block in data
	nblocks int     // blocks in the stripe
	block   int     // current physical block
	logical uint64  // logical number of the current block
	seed    uint32  // CRC seed of the current block
	off     int     // write offset within the current block
	started []int64 // start time of each physical block, Unix nanoseconds
	_       [64]byte
}

// WriterStats are cumulative counters of a Writer.
type WriterStats struct {
	Entries uint64 // entries written
	Bytes   uint64 // bytes written, padding included
	Wraps   uint64 // times a stripe reused its first block
	Dropped uint64 // entries not written: too large, or the writer is disabled
	Faults  uint64 // memory faults caught while writing; the writer is disabled after one
}

// NewWriter formats data as an empty journal described by h and returns a
// Writer for it. h.Size must equal len(data); Version and BlockSize are set
// by NewWriter, and a zero Salt is replaced by a random one.
func NewWriter(data []byte, h Header) (*Writer, error) {
	return newWriter(data, h, func() int64 { return time.Now().UnixNano() })
}

func newWriter(data []byte, h Header, now func() int64) (*Writer, error) {
	if int64(len(data)) != h.Size {
		return nil, errors.New("journal: header size does not match the data")
	}
	if h.Stripes == 0 {
		h.Stripes = DefaultStripes
	}
	n, err := blocksPerStripe(h.Size, h.Stripes)
	if err != nil {
		return nil, err
	}
	h.Version = version
	h.BlockSize = BlockSize
	for h.Salt == 0 {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		h.Salt = binary.LittleEndian.Uint64(b[:])
	}
	w := &Writer{data: data, hdr: h, stripes: make([]stripe, h.Stripes), now: now}
	if !w.protect(func() {
		encodeHeader(data, h)
		for i := range w.stripes {
			st := &w.stripes[i]
			st.base = HeaderSize + i*n*BlockSize
			st.nblocks = n
			st.logical = 1
			st.seed = blockSeed(h.Salt, st.logical)
			st.off = blockHeaderSize
			st.started = make([]int64, n)
			st.started[0] = now()
			encodeBlockHeader(data[st.base:], st.logical, h.Salt, st.started[0])
			// The rest of the block may hold bytes from an earlier file; they
			// fail the salted CRC. Clearing the first entry slot makes a fresh
			// block read as empty without relying on that.
			clear(data[st.base+blockHeaderSize : st.base+blockHeaderSize+entryHeaderSize])
		}
	}) {
		return nil, errors.New("journal: memory fault while formatting")
	}
	return w, nil
}

// Header returns the header the Writer wrote.
func (w *Writer) Header() Header { return w.hdr }

// Disabled reports whether a fault has disabled the Writer.
func (w *Writer) Disabled() bool { return w.disabled.Load() }

// Stats returns the Writer's counters.
func (w *Writer) Stats() WriterStats {
	return WriterStats{
		Entries: w.entries.Load(),
		Bytes:   w.bytes.Load(),
		Wraps:   w.wraps.Load(),
		Dropped: w.dropped.Load(),
		Faults:  w.faults.Load(),
	}
}

// Append writes one entry to the stripe chosen by key and reports whether it
// was written. Entries larger than MaxPayload, and all entries once the
// Writer is disabled, are dropped and counted.
func (w *Writer) Append(key uint64, kind Kind, scope uint64, payload []byte) bool {
	if w.disabled.Load() || len(payload) > MaxPayload {
		w.dropped.Add(1)
		return false
	}
	n := align8(entryHeaderSize + len(payload))
	st := &w.stripes[key%uint64(len(w.stripes))]

	st.mu.Lock()
	// Checked again under the lock: Close disables the Writer, then takes
	// every stripe lock before it zeros and unmaps the memory.
	ok := !w.disabled.Load() && w.appendProtected(st, kind, scope, payload, n)
	st.mu.Unlock()
	if !ok {
		w.dropped.Add(1)
		return false
	}
	w.entries.Add(1)
	w.bytes.Add(uint64(n))
	return true
}

func (w *Writer) appendLocked(st *stripe, kind Kind, scope uint64, payload []byte, n int) {
	if st.off+n > BlockSize {
		if rest := BlockSize - st.off; rest >= entryHeaderSize {
			w.writeEntry(st, KindPad, 0, nil, rest)
		}
		st.block++
		if st.block == st.nblocks {
			st.block = 0
			w.wraps.Add(1)
		}
		st.logical++
		st.seed = blockSeed(w.hdr.Salt, st.logical)
		st.started[st.block] = w.now()
		encodeBlockHeader(w.data[st.base+st.block*BlockSize:], st.logical, w.hdr.Salt, st.started[st.block])
		st.off = blockHeaderSize
	}
	w.writeEntry(st, kind, scope, payload, n)
}

// writeEntry writes an entry of total length n at the stripe's offset and
// advances it. The CRC covers the header fields after it and the payload,
// not the alignment padding.
func (w *Writer) writeEntry(st *stripe, kind Kind, scope uint64, payload []byte, n int) {
	at := st.base + st.block*BlockSize + st.off
	e := w.data[at : at+n]
	binary.LittleEndian.PutUint32(e[0:], uint32(n))
	binary.LittleEndian.PutUint64(e[8:], scope)
	e[16] = byte(kind)
	e[17], e[18], e[19] = 0, 0, 0
	binary.LittleEndian.PutUint32(e[20:], uint32(len(payload)))
	copy(e[entryHeaderSize:], payload)
	if kind != KindPad {
		clear(e[entryHeaderSize+len(payload):]) // alignment padding only
	}
	binary.LittleEndian.PutUint32(e[4:], entryCRC(st.seed, e[8:entryHeaderSize+len(payload)]))
	st.off += n
	// Mark the end of written data, so a reader stops cleanly here instead of
	// at a stale entry left from an earlier lap of this block.
	if st.off+4 <= BlockSize {
		next := st.base + st.block*BlockSize + st.off
		clear(w.data[next : next+4])
	}
}

// appendProtected is appendLocked under fault protection, without the
// closure protect would need, so appending does not allocate.
func (w *Writer) appendProtected(st *stripe, kind Kind, scope uint64, payload []byte, n int) (ok bool) {
	old := debug.SetPanicOnFault(true)
	defer w.recoverFault(old, &ok)
	w.appendLocked(st, kind, scope, payload, n)
	return true
}

// recoverFault restores the fault setting and turns a memory fault into a
// disabled Writer. It must be deferred directly.
func (w *Writer) recoverFault(old bool, ok *bool) {
	debug.SetPanicOnFault(old)
	if r := recover(); r != nil {
		// Faults raised under SetPanicOnFault carry the faulting address.
		// Only faults inside the journal's memory are expected; anything
		// else is a bug and keeps panicking.
		f, fault := r.(interface{ Addr() uintptr })
		if !fault || !w.contains(f.Addr()) {
			panic(r)
		}
		w.disabled.Store(true)
		w.faults.Add(1)
		*ok = false
	}
}

// contains reports whether addr lies in the journal's memory.
func (w *Writer) contains(addr uintptr) bool {
	if len(w.data) == 0 {
		return false
	}
	start := uintptr(unsafe.Pointer(&w.data[0]))
	return addr >= start && addr < start+uintptr(len(w.data))
}

// OldestBlockTime returns the start time of the oldest block still in the
// ring, across stripes, in Unix nanoseconds: the time span the journal
// currently covers begins there.
func (w *Writer) OldestBlockTime() int64 {
	var oldest int64
	for i := range w.stripes {
		st := &w.stripes[i]
		st.mu.Lock()
		t := st.started[0]
		if next := (st.block + 1) % st.nblocks; st.started[next] != 0 {
			t = st.started[next] // wrapped: the next block to reuse is the oldest
		}
		st.mu.Unlock()
		if oldest == 0 || t < oldest {
			oldest = t
		}
	}
	return oldest
}

// protect runs f, turning a memory fault (SIGBUS or SIGSEGV on the mapping,
// for example after the file was truncated or the disk failed) into a
// disabled Writer instead of a crash. It reports whether f completed.
func (w *Writer) protect(f func()) (ok bool) {
	old := debug.SetPanicOnFault(true)
	defer w.recoverFault(old, &ok)
	f()
	return true
}
