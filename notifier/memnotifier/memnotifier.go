// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package memnotifier provides an in-process flightrec.Notifier. Every
// Recorder started with the same Bus receives the notices of the others. It
// is meant for tests, examples and services that run several Recorders in one
// process; use a network notifier such as redisnotifier between processes.
package memnotifier

import (
	"context"
	"sync"
)

// Bus delivers every published message to every current subscriber,
// including the publisher. The zero value is ready to use.
type Bus struct {
	mu   sync.RWMutex
	next int
	subs map[int]func([]byte)
}

// New returns an empty Bus.
func New() *Bus { return &Bus{} }

// Publish delivers a copy of msg to every subscriber.
func (b *Bus) Publish(_ context.Context, msg []byte) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, deliver := range b.subs {
		deliver(append([]byte(nil), msg...))
	}
	return nil
}

// Subscribe calls deliver for every published message until ctx is done.
// deliver is never called after Subscribe returns.
func (b *Bus) Subscribe(ctx context.Context, deliver func([]byte)) error {
	b.mu.Lock()
	if b.subs == nil {
		b.subs = make(map[int]func([]byte))
	}
	id := b.next
	b.next++
	b.subs[id] = deliver
	b.mu.Unlock()

	<-ctx.Done()

	b.mu.Lock()
	delete(b.subs, id)
	b.mu.Unlock()
	return nil
}

// Subscribers returns the number of active subscriptions. Tests use it to
// wait until every Recorder is listening before publishing.
func (b *Bus) Subscribers() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}
