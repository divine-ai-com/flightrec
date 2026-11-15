// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package journal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
)

func newTestWriter(t testing.TB, size int64, stripes uint32, salt uint64) (*Writer, []byte) {
	t.Helper()
	data := make([]byte, size)
	w, err := NewWriter(data, Header{Size: size, Stripes: stripes, Salt: salt, StartedAt: 42, Host: "test-host"})
	if err != nil {
		t.Fatal(err)
	}
	return w, data
}

// seqPayload encodes a key and a sequence number, padded to size bytes.
func seqPayload(key, seq uint64, size int) []byte {
	p := make([]byte, max(size, 16))
	binary.LittleEndian.PutUint64(p, key)
	binary.LittleEndian.PutUint64(p[8:], seq)
	return p
}

func decodeSeq(p []byte) (key, seq uint64) {
	return binary.LittleEndian.Uint64(p), binary.LittleEndian.Uint64(p[8:])
}

func TestRoundTrip(t *testing.T) {
	w, data := newTestWriter(t, MinSize(4), 4, 7)
	kinds := []Kind{KindStart, KindRecord, KindRecord, KindEnd}
	for key := uint64(0); key < 8; key++ {
		for i, k := range kinds {
			if !w.Append(key, k, key*100, seqPayload(key, uint64(i), 40)) {
				t.Fatal("append failed")
			}
		}
	}
	h, entries, st, err := Read(data)
	if err != nil {
		t.Fatal(err)
	}
	if h.Host != "test-host" || h.StartedAt != 42 || h.Stripes != 4 || h.Salt != 7 {
		t.Errorf("header %+v", h)
	}
	if len(entries) != 32 || st.Entries != 32 || st.Invalid != 0 {
		t.Fatalf("read %d entries, stats %+v", len(entries), st)
	}
	next := map[uint64]uint64{}
	for _, e := range entries {
		key, seq := decodeSeq(e.Payload)
		if seq != next[key] || e.Kind != kinds[seq] || e.Scope != key*100 {
			t.Fatalf("entry out of order or wrong: key %d seq %d kind %d scope %d", key, seq, e.Kind, e.Scope)
		}
		next[key]++
	}
}

// TestWrapKeepsNewestInOrder writes far more than fits and checks that each
// stripe returns a gap-free run ending at the newest entry, with nothing
// stale from earlier laps.
func TestWrapKeepsNewestInOrder(t *testing.T) {
	w, data := newTestWriter(t, MinSize(2), 2, 99)
	const per = 5000
	for seq := uint64(0); seq < per; seq++ {
		for key := uint64(0); key < 2; key++ {
			w.Append(key, KindRecord, key, seqPayload(key, seq, 100))
		}
	}
	if w.Stats().Wraps == 0 {
		t.Fatal("test did not wrap")
	}
	_, entries, st, err := Read(data)
	if err != nil {
		t.Fatal(err)
	}
	if st.Invalid != 0 {
		t.Errorf("Invalid = %d; the end of written data must read as a clean end, not as corruption", st.Invalid)
	}
	last := map[uint64]uint64{}
	first := map[uint64]bool{}
	for _, e := range entries {
		key, seq := decodeSeq(e.Payload)
		if first[key] && seq != last[key]+1 {
			t.Fatalf("key %d: seq %d after %d; stale or missing entry", key, seq, last[key])
		}
		first[key] = true
		last[key] = seq
	}
	for key := uint64(0); key < 2; key++ {
		if last[key] != per-1 {
			t.Errorf("key %d: newest seq %d, want %d", key, last[key], per-1)
		}
	}
}

func TestTornLastEntry(t *testing.T) {
	w, data := newTestWriter(t, MinSize(1), 1, 5)
	for seq := uint64(0); seq < 10; seq++ {
		w.Append(0, KindRecord, 1, seqPayload(0, seq, 64))
	}
	// Simulate a write cut short: corrupt the payload of the last entry.
	_, entries, _, _ := Read(data)
	lastOff := entries[len(entries)-1].Offset
	at := HeaderSize + lastOff + entryHeaderSize + 20
	data[at] ^= 0xff

	_, entries, st, err := Read(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 9 || st.Invalid != 1 {
		t.Fatalf("read %d entries, stats %+v; want the 9 complete ones and one invalid block end", len(entries), st)
	}
}

func TestEntriesFromAnEarlierFileAreIgnored(t *testing.T) {
	w, data := newTestWriter(t, MinSize(1), 1, 1)
	for seq := uint64(0); seq < 100; seq++ {
		w.Append(0, KindRecord, 1, seqPayload(0, seq, 64))
	}
	// A new file over the same bytes, as after a restart, with a new salt.
	w2, err := NewWriter(data, Header{Size: int64(len(data)), Stripes: 1, Salt: 2})
	if err != nil {
		t.Fatal(err)
	}
	w2.Append(0, KindRecord, 9, seqPayload(9, 0, 64))

	_, entries, _, err := Read(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Scope != 9 {
		t.Fatalf("read %d entries (first scope %d); old file's entries leaked", len(entries), entries[0].Scope)
	}
}

func TestOversizeEntryDropped(t *testing.T) {
	w, data := newTestWriter(t, MinSize(1), 1, 3)
	if w.Append(0, KindRecord, 1, make([]byte, MaxPayload+1)) {
		t.Error("oversize entry accepted")
	}
	if !w.Append(0, KindRecord, 1, make([]byte, MaxPayload)) {
		t.Error("largest allowed entry rejected")
	}
	if st := w.Stats(); st.Dropped != 1 || st.Entries != 1 {
		t.Errorf("stats %+v", st)
	}
	_, entries, _, _ := Read(data)
	if len(entries) != 1 || len(entries[0].Payload) != MaxPayload {
		t.Errorf("read %d entries", len(entries))
	}
}

func TestHeaderValidation(t *testing.T) {
	if _, err := NewWriter(make([]byte, MinSize(8)-1), Header{Size: MinSize(8) - 1, Stripes: 8}); err == nil {
		t.Error("too small accepted")
	}
	_, data := newTestWriter(t, MinSize(1), 1, 1)
	if _, err := DecodeHeader(data); err != nil {
		t.Fatal(err)
	}
	if err := SetAttempts(data, 2); err != nil {
		t.Fatal(err)
	}
	if h, _ := DecodeHeader(data); h.Attempts != 2 {
		t.Errorf("Attempts = %d", h.Attempts)
	}
	data[100] ^= 1 // inside the header, covered by its CRC
	if _, err := DecodeHeader(data); err == nil {
		t.Error("corrupt header accepted")
	}
	if _, _, _, err := Read(data); err == nil {
		t.Error("Read accepted a corrupt header")
	}
	if _, err := DecodeHeader(make([]byte, 10)); err == nil {
		t.Error("short data accepted")
	}
}

func TestConcurrentAppend(t *testing.T) {
	w, data := newTestWriter(t, 8<<20, 8, 11)
	const goroutines, per = 16, 400
	var wg sync.WaitGroup
	for g := uint64(0); g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := uint64(0); seq < per; seq++ {
				w.Append(g, KindRecord, g, seqPayload(g, seq, 50))
			}
		}()
	}
	wg.Wait()
	_, entries, st, err := Read(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != goroutines*per || st.Invalid != 0 {
		t.Fatalf("read %d entries, want %d; stats %+v", len(entries), goroutines*per, st)
	}
	next := map[uint64]uint64{}
	for _, e := range entries {
		key, seq := decodeSeq(e.Payload)
		if seq != next[key] {
			t.Fatalf("key %d out of order: seq %d, want %d", key, seq, next[key])
		}
		next[key]++
	}
}

// TestRandomCorruptionNeverInventsEntries flips random bytes in a valid
// journal and checks that every entry read back was written.
func TestRandomCorruptionNeverInventsEntries(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for round := 0; round < 50; round++ {
		w, data := newTestWriter(t, MinSize(2), 2, r.Uint64())
		written := map[string]bool{}
		for seq := uint64(0); seq < 2000; seq++ {
			p := seqPayload(seq%2, seq, 30+int(seq%50))
			w.Append(seq%2, KindRecord, seq, p)
			written[string(p)] = true
		}
		for i := 0; i < 20; i++ {
			data[HeaderSize+r.IntN(len(data)-HeaderSize)] ^= byte(1 + r.IntN(255))
		}
		_, entries, _, err := Read(data)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if !written[string(e.Payload)] {
				t.Fatalf("round %d: read an entry that was never written", round)
			}
		}
	}
}

// FuzzRead overlays fuzzer bytes on the block area of a valid, empty
// journal, so the fuzzer exercises the block and entry parser rather than
// stopping at the header CRC it cannot forge.
func FuzzRead(f *testing.F) {
	w, valid := newTestWriter(f, MinSize(1), 1, 1)
	for i := uint64(0); i < 20; i++ {
		w.Append(0, KindRecord, i, seqPayload(0, i, 40))
	}
	f.Add(valid[HeaderSize : HeaderSize+2048])
	f.Add(bytes.Repeat([]byte{0xff}, 512))
	f.Fuzz(func(t *testing.T, overlay []byte) {
		_, data := newTestWriter(t, MinSize(1), 1, 1)
		copy(data[HeaderSize:], overlay)
		_, entries, _, err := Read(data)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if len(e.Payload) > MaxPayload || !validKind(e.Kind) {
				t.Fatalf("bad entry: kind %d, %d byte payload", e.Kind, len(e.Payload))
			}
		}
	})
}

// FuzzReadMutatedJournal mutates bytes of a real journal at fuzzer-chosen
// offsets and requires that only written entries come back.
func FuzzReadMutatedJournal(f *testing.F) {
	f.Add(uint32(5000), byte(1), uint32(100000), byte(7))
	f.Fuzz(func(t *testing.T, off1 uint32, x1 byte, off2 uint32, x2 byte) {
		w, data := newTestWriter(t, MinSize(1), 1, 77)
		written := map[string]bool{}
		for seq := uint64(0); seq < 4000; seq++ { // wraps a two-block stripe
			p := []byte(fmt.Sprintf("entry-%d-%s", seq, bytes.Repeat([]byte{'x'}, int(seq%40))))
			w.Append(0, KindRecord, seq, p)
			written[string(p)] = true
		}
		data[HeaderSize+int(off1)%(len(data)-HeaderSize)] ^= x1
		data[HeaderSize+int(off2)%(len(data)-HeaderSize)] ^= x2
		_, entries, _, err := Read(data)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if !written[string(e.Payload)] {
				t.Fatalf("invented entry %q", e.Payload)
			}
		}
	})
}

func BenchmarkAppend(b *testing.B) {
	w, _ := newTestWriter(b, 16<<20, 8, 1)
	p := make([]byte, 120)
	b.ReportAllocs()
	b.SetBytes(int64(len(p)))
	for i := 0; i < b.N; i++ {
		w.Append(uint64(i), KindRecord, uint64(i), p)
	}
}

func BenchmarkAppendParallel(b *testing.B) {
	w, _ := newTestWriter(b, 16<<20, 8, 1)
	p := make([]byte, 120)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		k := rand.Uint64()
		for pb.Next() {
			w.Append(k, KindRecord, k, p)
		}
	})
}

// TestTornAppendAtEveryPrefix applies an append's byte changes in address
// order, one more byte at a time, as a kill partway through the copy could
// leave them (stores reach the page cache in order; the CRC is stored last
// but sits early in the entry, so address order also covers "CRC before
// payload"). At every prefix the earlier entries must survive and the torn
// entry must be either absent or complete. It covers an append inside a
// block and one that starts a new block after a pad.
func TestTornAppendAtEveryPrefix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prefill int
		size    int
	}{
		{"inside a block", 10, 100},
		{"across a block boundary", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, data := newTestWriter(t, MinSize(1), 1, 3)
			n := tc.prefill
			size := tc.size
			if tc.name == "across a block boundary" {
				// Fill the first block so the next append pads and moves on.
				for w.stripes[0].off+align8(entryHeaderSize+200) <= BlockSize {
					w.Append(0, KindRecord, 0, seqPayload(0, uint64(n), 200))
					n++
				}
				size = 200
			} else {
				for i := 0; i < n; i++ {
					w.Append(0, KindRecord, 0, seqPayload(0, uint64(i), size))
				}
			}
			before := append([]byte(nil), data...)
			w.Append(0, KindRecord, 0, seqPayload(0, uint64(n), size))
			after := append([]byte(nil), data...)

			var changed []int
			for i := range after {
				if after[i] != before[i] {
					changed = append(changed, i)
				}
			}
			for k := 0; k <= len(changed); k++ {
				state := append([]byte(nil), before...)
				for _, i := range changed[:k] {
					state[i] = after[i]
				}
				_, entries, _, err := Read(state)
				if err != nil {
					t.Fatal(err)
				}
				if len(entries) != n && len(entries) != n+1 {
					t.Fatalf("prefix %d/%d: %d entries, want %d or %d", k, len(changed), len(entries), n, n+1)
				}
				for i, e := range entries {
					if _, seq := decodeSeq(e.Payload); seq != uint64(i) || len(e.Payload) != max(size, 16) {
						t.Fatalf("prefix %d: entry %d has seq %d, %d bytes", k, i, seq, len(e.Payload))
					}
				}
			}
		})
	}
}

func TestHostileFieldsAreRejected(t *testing.T) {
	w, data := newTestWriter(t, MinSize(1), 1, 3)
	for i := uint64(0); i < 3; i++ {
		w.Append(0, KindRecord, i, seqPayload(0, i, 40))
	}
	first := HeaderSize + blockHeaderSize
	for _, tc := range []struct {
		name  string
		field int
		value uint32
	}{
		{"payload length that wraps to a negative int on 32-bit", 20, 0x80000000},
		{"length larger than the entry", 0, 96},
		{"length past the block", 0, BlockSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := append([]byte(nil), data...)
			binary.LittleEndian.PutUint32(state[first+tc.field:], tc.value)
			_, entries, st, err := Read(state)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 || st.Invalid != 1 {
				t.Errorf("read %d entries, stats %+v; a corrupt first entry must end the block", len(entries), st)
			}
		})
	}

	t.Run("misplaced block header", func(t *testing.T) {
		state := append([]byte(nil), data...)
		// Put logical block 1's header in physical block 1, where the writer
		// would put logical block 2.
		copy(state[HeaderSize+BlockSize:], state[HeaderSize:HeaderSize+blockHeaderSize])
		_, entries, st, _ := Read(state)
		if st.Blocks != 1 || len(entries) != 3 {
			t.Errorf("blocks %d entries %d; the misplaced header must be ignored", st.Blocks, len(entries))
		}
	})

	t.Run("hostile header with a valid CRC", func(t *testing.T) {
		state := append([]byte(nil), data...)
		h, _ := DecodeHeader(state)
		h.Stripes = 1 << 30
		encodeHeader(state, h)
		if _, err := DecodeHeader(state); err == nil {
			t.Error("header with more stripes than fit was accepted")
		}
		h.Stripes = 1
		h.Size = 1 << 40
		encodeHeader(state, h)
		if _, _, _, err := Read(state); err == nil {
			t.Error("header claiming more bytes than the data was accepted")
		}
	})
}

func TestZeroSaltIsReplaced(t *testing.T) {
	w, _ := newTestWriter(t, MinSize(1), 1, 0)
	if w.Header().Salt == 0 {
		t.Fatal("zero salt kept")
	}
}

func TestOldestBlockTime(t *testing.T) {
	clock := int64(1000)
	data := make([]byte, MinSize(1))
	w, err := newWriter(data, Header{Size: int64(len(data)), Stripes: 1, Salt: 1}, func() int64 { clock++; return clock })
	if err != nil {
		t.Fatal(err)
	}
	if got := w.OldestBlockTime(); got != 1001 {
		t.Fatalf("oldest %d, want the first block's 1001", got)
	}
	for i := 0; i < 3000; i++ { // wraps the two blocks several times
		w.Append(0, KindRecord, 0, make([]byte, 100))
	}
	_, entries, _, _ := Read(data)
	oldestRead := entries[0].BlockTime
	if got := w.OldestBlockTime(); got != oldestRead {
		t.Errorf("OldestBlockTime %d, oldest block read back %d", got, oldestRead)
	}
}
