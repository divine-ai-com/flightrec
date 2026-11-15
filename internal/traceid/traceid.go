// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package traceid validates, parses and generates the trace IDs flightrec
// keys request scopes by.
package traceid

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	mrand "math/rand/v2"
	"strings"
	"sync"
)

// MaxLen bounds trace IDs taken from X-Trace-Id and X-Request-Id.
const MaxLen = 128

// Valid reports whether a trace ID taken from a request header or
// metadata is usable: 1 to MaxLen bytes of letters, digits and
// _ . : - / + = @. This covers UUIDs, ULIDs, base64 and the host/sequence form
// some request ID middleware produces, and excludes whitespace, quotes,
// control and non-ASCII bytes. The ID is written to every log record and,
// with cross-service flush, identifies scopes across processes.
func Valid(id string) bool {
	if id == "" || len(id) > MaxLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '_', c == '-', c == '.', c == ':', c == '/', c == '+', c == '=', c == '@':
		default:
			return false
		}
	}
	return true
}

// ParseTraceparent returns the trace ID of a W3C traceparent header
// ("00-<32hex>-<16hex>-<2hex>"), or "" if the value is not valid as the W3C
// Trace Context specification defines it: lowercase hex, non-zero trace and
// parent IDs, version ff rejected, and exactly four fields for version 00.
// A higher version may append fields after a dash.
func ParseTraceparent(tp string) string {
	const n = 55 // len("00-") + 32 + len("-") + 16 + len("-") + 2
	if len(tp) < n || tp[2] != '-' || tp[35] != '-' || tp[52] != '-' {
		return ""
	}
	version, tid, pid, flags := tp[0:2], tp[3:35], tp[36:52], tp[53:55]
	if !lowerHex(version) || version == "ff" || !lowerHex(flags) {
		return ""
	}
	if version == "00" && len(tp) != n || version != "00" && len(tp) > n && tp[n] != '-' {
		return ""
	}
	if !lowerHex(tid) || !lowerHex(pid) || strings.Trim(tid, "0") == "" || strings.Trim(pid, "0") == "" {
		return ""
	}
	return tid
}

// lowerHex reports whether s is non-empty and consists of 0-9 and a-f.
func lowerHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

var rngPool = sync.Pool{
	New: func() any {
		var seed [32]byte
		_, _ = rand.Read(seed[:])
		return mrand.NewChaCha8(seed)
	},
}

func Generate() string {
	ch := rngPool.Get().(*mrand.ChaCha8)
	var b [16]byte
	binary.LittleEndian.PutUint64(b[:8], ch.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], ch.Uint64())
	rngPool.Put(ch)
	return hex.EncodeToString(b[:])
}
