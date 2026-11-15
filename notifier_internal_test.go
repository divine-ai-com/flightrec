// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/divine-ai-com/flightrec/internal/notify"
	"github.com/divine-ai-com/flightrec/internal/traceid"
)

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
	good, _ := json.Marshal(notify.Message{Version: notify.Version, Origin: "other", Notices: []notify.Notice{{TraceID: "t-1", Reason: "http_500"}}})
	f.Add(good)
	f.Add(notify.Sign(key, good, time.Now()))
	f.Add([]byte(`{"v":1,"o":"x","n":[{"t":"a b","r":"zzz"}]}`))
	f.Fuzz(func(t *testing.T, msg []byte) {
		for _, keys := range [][][]byte{nil, {key}} {
			rec := New(Options{Next: slog.DiscardHandler})
			h := &notifierHub{
				st:       rec.state,
				origin:   "self",
				keys:     keys,
				inbox:    make(chan notify.Notice, notifierQueueSize),
				received: notify.NewRecentSet(notifierRecentSize, notifierRecentTTL, rec.state.now),
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
