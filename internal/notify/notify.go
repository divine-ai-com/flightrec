// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package notify implements the notifier's wire format, its signed envelope,
// and the rate limiter and recent-set the notifier hub uses.
package notify

import (
	"context"
	"sync"
	"time"
)

// Wire format limits.
const (
	Version        = 1
	MaxBatch       = 64 // notices per message
	MaxMessageSize = 64 << 10
	MaxSignedSize  = MaxMessageSize*4/3 + 256 // base64 and the envelope
)

// Notice is one flushed trace in a notifier message.
type Notice struct {
	TraceID string `json:"t"`
	Reason  string `json:"r"`
}

// Message is the wire format. Fields are short because every
// process in a fleet receives every message.
type Message struct {
	Version int      `json:"v"`
	Origin  string   `json:"o"`
	Notices []Notice `json:"n"`
}

// RecentSet remembers keys for a fixed time, up to a fixed count, evicting
// the oldest first. It is not safe for concurrent use.
type RecentSet struct {
	ttl   time.Duration
	limit int
	now   func() time.Time
	items map[string]recentItem
	order []string
}

type recentItem struct {
	value string
	at    time.Time
}

// NewRecentSet returns a set that keeps up to limit keys for ttl.
func NewRecentSet(limit int, ttl time.Duration, now func() time.Time) *RecentSet {
	return &RecentSet{ttl: ttl, limit: limit, now: now, items: make(map[string]recentItem)}
}

// Get returns the value stored for key, if it has not expired.
func (r *RecentSet) Get(key string) (string, bool) {
	it, ok := r.items[key]
	if !ok || r.now().Sub(it.at) > r.ttl {
		return "", false
	}
	return it.value, true
}

// Add stores value for key, evicting the oldest key when full.
func (r *RecentSet) Add(key, value string) {
	now := r.now()
	for len(r.order) > 0 {
		oldest := r.order[0]
		it := r.items[oldest]
		if len(r.items) < r.limit && now.Sub(it.at) <= r.ttl {
			break
		}
		r.order = r.order[1:]
		delete(r.items, oldest)
	}
	if _, exists := r.items[key]; !exists {
		r.order = append(r.order, key)
	}
	r.items[key] = recentItem{value: value, at: now}
}

// TokenBucket is a simple rate limiter.
type TokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

// NewTokenBucket returns a limiter of rate tokens per second with burst.
func NewTokenBucket(rate, burst int, now func() time.Time) *TokenBucket {
	return &TokenBucket{rate: float64(rate), burst: float64(burst), tokens: float64(burst), last: now(), now: now}
}

// Wait blocks until a token is available or ctx is done, and reports
// whether it got one.
func (b *TokenBucket) Wait(ctx context.Context) bool {
	for !b.Take() {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Duration(float64(time.Second) / b.rate)):
		}
	}
	return true
}

// Take takes a token if one is available.
func (b *TokenBucket) Take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
