// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"time"
)

// signedPrefix starts a signed notifier message:
// FRS1.<base64url payload>.<unix seconds>.<hex HMAC-SHA256 of everything before it>
const signedPrefix = "FRS1."

// maxSkew bounds how old (or how far in the future) a signed message
// may be, which limits replay of captured messages.
const maxSkew = 2 * time.Minute

// MinKeyLen is the shortest key WithSigningKey accepts.
const MinKeyLen = 32

// Sign wraps payload in a signed envelope.
func Sign(key, payload []byte, now time.Time) []byte {
	b := make([]byte, 0, len(signedPrefix)+base64.RawURLEncoding.EncodedLen(len(payload))+16+1+hex.EncodedLen(sha256.Size)+1)
	b = append(b, signedPrefix...)
	b = base64.RawURLEncoding.AppendEncode(b, payload)
	b = append(b, '.')
	b = strconv.AppendInt(b, now.Unix(), 10)
	mac := hmac.New(sha256.New, key)
	mac.Write(b)
	b = append(b, '.')
	return hex.AppendEncode(b, mac.Sum(nil))
}

// Open verifies a signed envelope against keys and returns
// its payload.
func Open(keys [][]byte, msg []byte, now time.Time) ([]byte, bool) {
	if !bytes.HasPrefix(msg, []byte(signedPrefix)) {
		return nil, false
	}
	sigAt := bytes.LastIndexByte(msg, '.')
	if sigAt < len(signedPrefix) {
		return nil, false
	}
	signed, sigHex := msg[:sigAt], msg[sigAt+1:]
	if len(sigHex) != hex.EncodedLen(sha256.Size) {
		return nil, false
	}
	var sig [sha256.Size]byte
	if _, err := hex.Decode(sig[:], sigHex); err != nil {
		return nil, false
	}
	ok := false
	for _, k := range keys {
		mac := hmac.New(sha256.New, k)
		mac.Write(signed)
		if hmac.Equal(mac.Sum(nil), sig[:]) {
			ok = true
		}
	}
	if !ok {
		return nil, false
	}
	tsAt := bytes.LastIndexByte(signed, '.')
	if tsAt < len(signedPrefix) {
		return nil, false
	}
	ts, err := strconv.ParseInt(string(signed[tsAt+1:]), 10, 64)
	if err != nil {
		return nil, false
	}
	if d := now.Sub(time.Unix(ts, 0)); d > maxSkew || d < -maxSkew {
		return nil, false
	}
	payload, err := base64.RawURLEncoding.AppendDecode(nil, signed[len(signedPrefix):tsAt])
	if err != nil {
		return nil, false
	}
	return payload, true
}
