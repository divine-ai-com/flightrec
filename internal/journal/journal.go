// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package journal implements the crash journal file format of proposal 0002:
// a fixed-size region divided into stripes of fixed-size blocks, holding
// checksummed entries that survive the death of the writing process.
//
// The format is private to flightrec and may change in any release.
//
// Layout:
//
//	header (HeaderSize bytes)
//	stripe 0: block, block, ...
//	stripe 1: block, block, ...
//
// Each block starts with a block header and holds whole entries; an entry
// never crosses a block boundary. Each entry's CRC is seeded with the file's
// random salt and the block's logical number, so entries left from an earlier
// lap of the block, or from an earlier file, fail the check. A reader reads
// each block from its start and stops at the first invalid entry, so a torn
// write loses only the entries after it in the same block.
//
// Writer and Read work on byte slices: the caller supplies the memory, which
// is a shared file mapping in production and a plain slice in tests.
package journal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// Format constants.
const (
	HeaderSize      = 4096
	BlockSize       = 64 << 10
	blockHeaderSize = 32
	entryHeaderSize = 24
	// MaxPayload is the largest payload an entry can carry.
	MaxPayload = BlockSize - blockHeaderSize - entryHeaderSize
	// DefaultStripes is the number of stripes used when none is given.
	DefaultStripes = 8

	version    = 1
	magic      = "FLTRJRNL"
	blockMagic = 0x314b4c42 // "BLK1"
)

// Kind identifies an entry.
type Kind uint8

// Entry kinds.
const (
	KindStart    Kind = 1 // scope started; payload: scope metadata
	KindRecord   Kind = 2 // buffered record; payload: encoded record
	KindEnd      Kind = 3 // scope outcome final; payload empty or reason
	KindPad      Kind = 4 // fills the rest of a block
	KindRetained Kind = 5 // reserved for retained scopes
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Header describes a journal file.
type Header struct {
	Version   uint32
	Size      int64  // total bytes, header included
	BlockSize uint32 // bytes per block
	Stripes   uint32 // number of stripes
	Salt      uint64 // random per file; seeds every CRC
	StartedAt int64  // writer start time, Unix nanoseconds
	Attempts  uint32 // recovery attempts made on this file
	Host      string // writer host name, at most 255 bytes
}

// Layout errors.
var (
	ErrTooSmall  = errors.New("journal: size too small for the stripes")
	ErrBadHeader = errors.New("journal: invalid header")
)

// header field offsets
const (
	offMagic     = 0
	offVersion   = 8
	offSize      = 12
	offBlockSize = 20
	offStripes   = 24
	offSalt      = 28
	offStarted   = 36
	offAttempts  = 44
	offHostLen   = 48
	offHost      = 49
	offHeaderCRC = HeaderSize - 4
)

// encodeHeader writes h into b[:HeaderSize].
func encodeHeader(b []byte, h Header) {
	clear(b[:HeaderSize])
	copy(b[offMagic:], magic)
	binary.LittleEndian.PutUint32(b[offVersion:], h.Version)
	binary.LittleEndian.PutUint64(b[offSize:], uint64(h.Size))
	binary.LittleEndian.PutUint32(b[offBlockSize:], h.BlockSize)
	binary.LittleEndian.PutUint32(b[offStripes:], h.Stripes)
	binary.LittleEndian.PutUint64(b[offSalt:], h.Salt)
	binary.LittleEndian.PutUint64(b[offStarted:], uint64(h.StartedAt))
	binary.LittleEndian.PutUint32(b[offAttempts:], h.Attempts)
	host := h.Host
	if len(host) > 255 {
		host = host[:255]
	}
	b[offHostLen] = byte(len(host))
	copy(b[offHost:], host)
	binary.LittleEndian.PutUint32(b[offHeaderCRC:], crc32.Checksum(b[:offHeaderCRC], castagnoli))
}

// DecodeHeader reads and validates the header at the start of b.
func DecodeHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize || string(b[offMagic:offMagic+len(magic)]) != magic {
		return Header{}, ErrBadHeader
	}
	if crc32.Checksum(b[:offHeaderCRC], castagnoli) != binary.LittleEndian.Uint32(b[offHeaderCRC:]) {
		return Header{}, ErrBadHeader
	}
	h := Header{
		Version:   binary.LittleEndian.Uint32(b[offVersion:]),
		Size:      int64(binary.LittleEndian.Uint64(b[offSize:])),
		BlockSize: binary.LittleEndian.Uint32(b[offBlockSize:]),
		Stripes:   binary.LittleEndian.Uint32(b[offStripes:]),
		Salt:      binary.LittleEndian.Uint64(b[offSalt:]),
		StartedAt: int64(binary.LittleEndian.Uint64(b[offStarted:])),
		Attempts:  binary.LittleEndian.Uint32(b[offAttempts:]),
	}
	hostLen := int(b[offHostLen])
	h.Host = string(b[offHost : offHost+hostLen])
	if h.Version != version || h.BlockSize != BlockSize || h.Stripes == 0 || h.Size < HeaderSize {
		return Header{}, ErrBadHeader
	}
	if _, err := blocksPerStripe(h.Size, h.Stripes); err != nil {
		return Header{}, ErrBadHeader
	}
	return h, nil
}

// SetAttempts rewrites the attempt counter in the header at the start of b.
func SetAttempts(b []byte, attempts uint32) error {
	h, err := DecodeHeader(b)
	if err != nil {
		return err
	}
	h.Attempts = attempts
	encodeHeader(b, h)
	return nil
}

// blocksPerStripe returns how many blocks each stripe holds for a file of
// size bytes, and an error if a stripe would hold fewer than two.
func blocksPerStripe(size int64, stripes uint32) (int, error) {
	n := (size - HeaderSize) / int64(stripes) / BlockSize
	if n < 2 {
		return 0, ErrTooSmall
	}
	return int(n), nil
}

// MinSize returns the smallest valid file size for the given stripe count.
func MinSize(stripes uint32) int64 {
	return HeaderSize + int64(stripes)*2*BlockSize
}

// align8 rounds n up to a multiple of 8.
func align8(n int) int { return (n + 7) &^ 7 }

// blockSeed returns the CRC state after the salt and the block's logical
// number. Every entry CRC in the block continues from it.
func blockSeed(salt, logical uint64) uint32 {
	seed := make([]byte, 16)
	binary.LittleEndian.PutUint64(seed[0:], salt)
	binary.LittleEndian.PutUint64(seed[8:], logical)
	return crc32.Update(0, castagnoli, seed)
}

// entryCRC computes the CRC of an entry from its block's seed and the entry
// bytes after the CRC field.
func entryCRC(seed uint32, rest []byte) uint32 {
	return crc32.Update(seed, castagnoli, rest)
}

// encodeBlockHeader writes a block header for the given logical number.
func encodeBlockHeader(b []byte, logical, salt uint64) {
	clear(b[:blockHeaderSize])
	binary.LittleEndian.PutUint64(b[0:], logical)
	binary.LittleEndian.PutUint64(b[8:], salt)
	binary.LittleEndian.PutUint32(b[16:], blockMagic)
	binary.LittleEndian.PutUint32(b[20:], crc32.Checksum(b[:20], castagnoli))
}

// decodeBlockHeader returns the logical number of a valid block header.
func decodeBlockHeader(b []byte, salt uint64) (uint64, bool) {
	if binary.LittleEndian.Uint32(b[16:]) != blockMagic ||
		binary.LittleEndian.Uint64(b[8:]) != salt ||
		crc32.Checksum(b[:20], castagnoli) != binary.LittleEndian.Uint32(b[20:]) {
		return 0, false
	}
	return binary.LittleEndian.Uint64(b[0:]), true
}
