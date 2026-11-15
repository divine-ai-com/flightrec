// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestSignatureEnvelope(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	other := bytes.Repeat([]byte("o"), 32)
	now := time.Unix(1_800_000_000, 0)
	payload := []byte(`{"v":1,"o":"x","n":[]}`)
	msg := Sign(key, payload, now)

	if got, ok := Open([][]byte{key}, msg, now.Add(time.Minute)); !ok || !bytes.Equal(got, payload) {
		t.Fatalf("valid message rejected: %q %v", got, ok)
	}
	if _, ok := Open([][]byte{other, key}, msg, now); !ok {
		t.Error("message signed with a verify-also key rejected")
	}
	for name, tc := range map[string]struct {
		keys [][]byte
		msg  []byte
		at   time.Time
	}{
		"wrong key":   {[][]byte{other}, msg, now},
		"unsigned":    {[][]byte{key}, payload, now},
		"stale":       {[][]byte{key}, msg, now.Add(3 * time.Minute)},
		"future":      {[][]byte{key}, msg, now.Add(-3 * time.Minute)},
		"tampered":    {[][]byte{key}, bytes.Replace(msg, []byte("FRS1.e"), []byte("FRS1.f"), 1), now},
		"truncated":   {[][]byte{key}, msg[:len(msg)-10], now},
		"prefix only": {[][]byte{key}, []byte("FRS1."), now},
	} {
		if _, ok := Open(tc.keys, tc.msg, tc.at); ok {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestRecentSet(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	r := NewRecentSet(2, time.Minute, func() time.Time { return now })
	r.Add("a", "1")
	r.Add("b", "2")
	if v, ok := r.Get("a"); !ok || v != "1" {
		t.Errorf("Get(a) = %q, %v", v, ok)
	}
	r.Add("c", "3") // full: the oldest key goes
	if _, ok := r.Get("a"); ok {
		t.Error("oldest key kept past the limit")
	}
	for _, k := range []string{"b", "c"} {
		if _, ok := r.Get(k); !ok {
			t.Errorf("%s evicted although newer than a", k)
		}
	}
	now = now.Add(2 * time.Minute)
	if _, ok := r.Get("c"); ok {
		t.Error("key kept past its TTL")
	}
	r.Add("d", "4") // expired keys make room without evicting live ones
	if v, ok := r.Get("d"); !ok || v != "4" {
		t.Errorf("Get(d) = %q, %v", v, ok)
	}
}

func TestTokenBucket(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	b := NewTokenBucket(10, 2, func() time.Time { return now })
	for i := range 2 {
		if !b.Take() {
			t.Fatalf("token %d of the burst refused", i+1)
		}
	}
	if b.Take() {
		t.Fatal("burst of 2 exceeded")
	}
	now = now.Add(100 * time.Millisecond) // one token at 10 per second
	if !b.Take() {
		t.Error("refilled token refused")
	}
	if b.Take() {
		t.Error("refill rate exceeded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if b.Wait(ctx) {
		t.Error("Wait returned a token after ctx was done")
	}
}
