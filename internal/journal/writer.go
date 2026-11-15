// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package journal

import (
	"encoding/binary"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// Writer appends entries to a journal held in a byte slice. It is safe for
// concurrent use. Entries with the same stripe key are kept in order.
type Writer struct {
	data    []byte
	hdr     Header
	stripes []stripe

	disabled atomic.Bool
	entries  atomic.Uint64
	bytes    atomic.Uint64
	wraps    atomic.Uint64
	dropped  atomic.Uint64
	faults   atomic.Uint64
}

type stripe struct {
	mu      sync.Mutex
	base    int    // offset of the stripe's first block in data
	nblocks int    // blocks in the stripe
	block   int    // current physical block
	logical uint64 // logical number of the current block
	seed    uint32 // CRC seed of the current block
	off     int    // write offset within the current block
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
// by NewWriter.
func NewWriter(data []byte, h Header) (*Writer, error) {
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
	w := &Writer{data: data, hdr: h, stripes: make([]stripe, h.Stripes)}
	if !w.protect(func() {
		encodeHeader(data, h)
		for i := range w.stripes {
			st := &w.stripes[i]
			st.base = HeaderSize + i*n*BlockSize
			st.nblocks = n
			st.logical = 1
			st.seed = blockSeed(h.Salt, st.logical)
			st.off = blockHeaderSize
			encodeBlockHeader(data[st.base:], st.logical, h.Salt)
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
	ok := w.appendProtected(st, kind, scope, payload, n)
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
		encodeBlockHeader(w.data[st.base+st.block*BlockSize:], st.logical, w.hdr.Salt)
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
	clear(e[entryHeaderSize+len(payload):])
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
		// Anything else is a bug and keeps panicking.
		if _, fault := r.(interface{ Addr() uintptr }); !fault {
			panic(r)
		}
		w.disabled.Store(true)
		w.faults.Add(1)
		*ok = false
	}
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
