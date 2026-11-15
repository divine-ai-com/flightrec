// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package journal

import (
	"encoding/binary"
	"sort"
)

// Entry is one entry read from a journal. Payload aliases the data passed to
// Read.
type Entry struct {
	Kind      Kind
	Scope     uint64
	Payload   []byte
	Stripe    int
	Logical   uint64 // logical number of the block holding the entry
	BlockTime int64  // start time of that block, Unix nanoseconds
	Offset    int    // offset of the entry within its block
}

// ReadStats describe what Read found.
type ReadStats struct {
	Blocks  int // valid blocks
	Entries int // valid entries, pad entries excluded
	Invalid int // blocks that ended at an invalid entry: a torn write, or the stale tail of an earlier lap
}

// Read parses a journal from data. It returns the entries of every stripe in
// the order they were written within each stripe. It never panics on
// malformed input; invalid blocks and entries are skipped and counted.
func Read(data []byte) (Header, []Entry, ReadStats, error) {
	h, err := DecodeHeader(data)
	if err != nil {
		return Header{}, nil, ReadStats{}, err
	}
	if int64(len(data)) < h.Size {
		return Header{}, nil, ReadStats{}, ErrBadHeader
	}
	n, _ := blocksPerStripe(h.Size, h.Stripes)

	var out []Entry
	var st ReadStats
	for s := 0; s < int(h.Stripes); s++ {
		base := HeaderSize + s*n*BlockSize
		type blk struct {
			at      int
			logical uint64
			started int64
		}
		var blocks []blk
		for b := 0; b < n; b++ {
			at := base + b*BlockSize
			logical, started, ok := decodeBlockHeader(data[at:], h.Salt)
			// The writer puts logical block L at physical block (L-1) mod n;
			// anything else is not a block it wrote.
			if ok && logical > 0 && (logical-1)%uint64(n) == uint64(b) {
				blocks = append(blocks, blk{at, logical, started})
			}
		}
		sort.Slice(blocks, func(i, j int) bool { return blocks[i].logical < blocks[j].logical })
		for _, b := range blocks {
			st.Blocks++
			entries, invalid := readBlock(data[b.at:b.at+BlockSize], h.Salt, b.logical, b.started, s)
			out = append(out, entries...)
			st.Entries += len(entries)
			if invalid {
				st.Invalid++
			}
		}
	}
	return h, out, st, nil
}

// readBlock returns the valid entries of one block, and whether reading
// stopped at an invalid entry rather than at the end of the written data.
func readBlock(b []byte, salt, logical uint64, started int64, stripe int) ([]Entry, bool) {
	var out []Entry
	seed := blockSeed(salt, logical)
	off := blockHeaderSize
	for off+entryHeaderSize <= len(b) {
		e := b[off:]
		length32 := binary.LittleEndian.Uint32(e[0:])
		if length32 == 0 {
			return out, false // never written
		}
		// Compare as unsigned before converting, so hostile values cannot
		// wrap to negative ints on 32-bit platforms.
		payloadLen32 := binary.LittleEndian.Uint32(e[20:])
		if uint64(length32) > uint64(len(e)) || uint64(payloadLen32) > uint64(MaxPayload) {
			return out, true
		}
		length, payloadLen := int(length32), int(payloadLen32)
		kind := Kind(e[16])
		// The length is implied by the payload length except for pad
		// entries, which fill the rest of the block; requiring it closes the
		// one header field the CRC does not cover.
		if kind == KindPad {
			if length != len(e) || payloadLen != 0 {
				return out, true
			}
		} else if length != align8(entryHeaderSize+payloadLen) {
			return out, true
		}
		if entryCRC(seed, e[8:entryHeaderSize+payloadLen]) != binary.LittleEndian.Uint32(e[4:]) {
			return out, true
		}
		if kind == KindPad {
			return out, false
		}
		if !validKind(kind) {
			return out, true
		}
		out = append(out, Entry{
			Kind:      kind,
			Scope:     binary.LittleEndian.Uint64(e[8:]),
			Payload:   e[entryHeaderSize : entryHeaderSize+payloadLen],
			Stripe:    stripe,
			Logical:   logical,
			BlockTime: started,
			Offset:    off,
		})
		off += length
	}
	return out, false
}
