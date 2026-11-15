// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"bytes"
	"encoding/json"
	"github.com/divine-ai-com/flightrec/internal/traceid"
	"log/slog"
	"testing"
	"time"
)

func TestNotifierSignatureEnvelope(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	other := bytes.Repeat([]byte("o"), 32)
	now := time.Unix(1_800_000_000, 0)
	payload := []byte(`{"v":1,"o":"x","n":[]}`)
	msg := signNotifierMessage(key, payload, now)

	if got, ok := openNotifierMessage([][]byte{key}, msg, now.Add(time.Minute)); !ok || !bytes.Equal(got, payload) {
		t.Fatalf("valid message rejected: %q %v", got, ok)
	}
	if _, ok := openNotifierMessage([][]byte{other, key}, msg, now); !ok {
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
		if _, ok := openNotifierMessage(tc.keys, tc.msg, tc.at); ok {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestWithSigningKeyRejectsShortKeys(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("short key accepted")
		}
	}()
	WithSigningKey([]byte("short"))
}

// FuzzNotifierReceive feeds arbitrary bus messages to the receive path, with
// and without a signing key: it must never panic, and only well-formed
// messages may reach the inbox.
func FuzzNotifierReceive(f *testing.F) {
	key := bytes.Repeat([]byte("k"), 32)
	good, _ := json.Marshal(notifierMessage{Version: notifierMessageVersion, Origin: "other", Notices: []notice{{TraceID: "t-1", Reason: "http_500"}}})
	f.Add(good)
	f.Add(signNotifierMessage(key, good, time.Now()))
	f.Add([]byte(`{"v":1,"o":"x","n":[{"t":"a b","r":"zzz"}]}`))
	f.Fuzz(func(t *testing.T, msg []byte) {
		for _, keys := range [][][]byte{nil, {key}} {
			rec := New(Options{Next: slog.DiscardHandler})
			h := &notifierHub{
				st:       rec.state,
				origin:   "self",
				keys:     keys,
				inbox:    make(chan notice, notifierQueueSize),
				received: newRecentSet(notifierRecentSize, notifierRecentTTL, rec.state.now),
			}
			h.receive(msg)
			for len(h.inbox) > 0 {
				n := <-h.inbox
				if !traceid.Valid(n.TraceID) || !ValidMarker(n.Reason) {
					t.Fatalf("invalid notice reached the inbox: %+v", n)
				}
			}
		}
	})
}
