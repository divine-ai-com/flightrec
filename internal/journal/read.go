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
	Kind    Kind
	Scope   uint64
	Payload []byte
	Stripe  int
	Logical uint64 // logical number of the block holding the entry
	Offset  int    // offset of the entry within its block
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
		}
		var blocks []blk
		for b := 0; b < n; b++ {
			at := base + b*BlockSize
			if logical, ok := decodeBlockHeader(data[at:], h.Salt); ok {
				blocks = append(blocks, blk{at, logical})
			}
		}
		sort.Slice(blocks, func(i, j int) bool { return blocks[i].logical < blocks[j].logical })
		for _, b := range blocks {
			st.Blocks++
			entries, invalid := readBlock(data[b.at:b.at+BlockSize], h.Salt, b.logical, s)
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
func readBlock(b []byte, salt, logical uint64, stripe int) ([]Entry, bool) {
	var out []Entry
	seed := blockSeed(salt, logical)
	off := blockHeaderSize
	for off+entryHeaderSize <= len(b) {
		e := b[off:]
		length := int(binary.LittleEndian.Uint32(e[0:]))
		if length == 0 {
			return out, false // never written
		}
		payloadLen := int(binary.LittleEndian.Uint32(e[20:]))
		if length < entryHeaderSize || length%8 != 0 || length > len(e) ||
			payloadLen > length-entryHeaderSize {
			return out, true
		}
		if entryCRC(seed, e[8:entryHeaderSize+payloadLen]) != binary.LittleEndian.Uint32(e[4:]) {
			return out, true
		}
		kind := Kind(e[16])
		if kind == KindPad {
			return out, false
		}
		out = append(out, Entry{
			Kind:    kind,
			Scope:   binary.LittleEndian.Uint64(e[8:]),
			Payload: e[entryHeaderSize : entryHeaderSize+payloadLen],
			Stripe:  stripe,
			Logical: logical,
			Offset:  off,
		})
		off += length
	}
	return out, false
}
