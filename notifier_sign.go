// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"time"
)

// notifierSignedPrefix starts a signed notifier message:
// FRS1.<base64url payload>.<unix seconds>.<hex HMAC-SHA256 of everything before it>
const notifierSignedPrefix = "FRS1."

// notifierMaxSkew bounds how old (or how far in the future) a signed message
// may be, which limits replay of captured messages.
const notifierMaxSkew = 2 * time.Minute

// minSigningKeyLen is the shortest key WithSigningKey accepts.
const minSigningKeyLen = 32

type signingKeyOption struct{ keys [][]byte }

func (o signingKeyOption) applyNotifier(c *notifierConfig) { c.keys = o.keys }

// WithSigningKey authenticates notifier messages. Every message is signed
// with key (HMAC-SHA256, with a timestamp), and a message is accepted only
// if it is signed with key or one of verifyAlso and is at most two minutes
// old. Unsigned, forged and stale messages are dropped and counted in
// Stats.NotifierRejected.
//
// Without it, anyone who can publish on the notifier's channel can make
// every service flush or trigger scopes for any trace ID. Use the same key,
// at least 32 random bytes (not 32 characters of a password), in every
// process on the channel; WithSigningKey panics on a shorter key.
//
// Signed and unsigned processes reject each other's messages, so enable
// signing on every process of a channel in one deploy (or move to a new
// channel). To rotate keys with rolling deploys, take three steps, each
// rolled out everywhere before the next:
//
//  1. WithSigningKey(old, new): sign with old, accept both.
//  2. WithSigningKey(new, old): sign with new, accept both.
//  3. WithSigningKey(new).
func WithSigningKey(key []byte, verifyAlso ...[]byte) NotifierOption {
	keys := [][]byte{key}
	keys = append(keys, verifyAlso...)
	for _, k := range keys {
		if len(k) < minSigningKeyLen {
			panic("flightrec: notifier signing key shorter than 32 bytes")
		}
	}
	cp := make([][]byte, len(keys))
	for i, k := range keys {
		cp[i] = bytes.Clone(k)
	}
	return signingKeyOption{keys: cp}
}

// signNotifierMessage wraps payload in a signed envelope.
func signNotifierMessage(key, payload []byte, now time.Time) []byte {
	b := make([]byte, 0, len(notifierSignedPrefix)+base64.RawURLEncoding.EncodedLen(len(payload))+16+1+hex.EncodedLen(sha256.Size)+1)
	b = append(b, notifierSignedPrefix...)
	b = base64.RawURLEncoding.AppendEncode(b, payload)
	b = append(b, '.')
	b = strconv.AppendInt(b, now.Unix(), 10)
	mac := hmac.New(sha256.New, key)
	mac.Write(b)
	b = append(b, '.')
	return hex.AppendEncode(b, mac.Sum(nil))
}

// openNotifierMessage verifies a signed envelope against keys and returns
// its payload.
func openNotifierMessage(keys [][]byte, msg []byte, now time.Time) ([]byte, bool) {
	if !bytes.HasPrefix(msg, []byte(notifierSignedPrefix)) {
		return nil, false
	}
	sigAt := bytes.LastIndexByte(msg, '.')
	if sigAt < len(notifierSignedPrefix) {
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
	if tsAt < len(notifierSignedPrefix) {
		return nil, false
	}
	ts, err := strconv.ParseInt(string(signed[tsAt+1:]), 10, 64)
	if err != nil {
		return nil, false
	}
	if d := now.Sub(time.Unix(ts, 0)); d > notifierMaxSkew || d < -notifierMaxSkew {
		return nil, false
	}
	payload, err := base64.RawURLEncoding.AppendDecode(nil, signed[len(notifierSignedPrefix):tsAt])
	if err != nil {
		return nil, false
	}
	return payload, true
}
