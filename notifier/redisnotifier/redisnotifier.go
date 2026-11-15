// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package redisnotifier implements flightrec.Notifier with Redis pub/sub, so
// that a failure in one service flushes the logs other services retained for
// the same trace.
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	n := redisnotifier.New(client, redisnotifier.WithChannel("flightrec:prod"))
//	stop := rec.StartNotifier(ctx, n)
//	defer stop()
//
// Redis pub/sub is fire and forget: a process that is disconnected when a
// notice is published does not receive it. That matches flightrec's best
// effort cross-service flush.
package redisnotifier

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// DefaultChannel is the Redis channel used when WithChannel is not given.
const DefaultChannel = "flightrec:flush"

// Notifier publishes and receives flightrec flush notices on one Redis
// channel. It implements flightrec.Notifier.
type Notifier struct {
	client  redis.UniversalClient
	channel string
}

// Option configures a Notifier.
type Option func(*Notifier)

// WithChannel sets the Redis channel. Use a different channel per
// environment so that staging and production sharing a Redis server do not
// flush each other's logs.
func WithChannel(channel string) Option {
	return func(n *Notifier) {
		if channel != "" {
			n.channel = channel
		}
	}
}

// New returns a Notifier that uses client. The client is not closed by the
// Notifier.
func New(client redis.UniversalClient, opts ...Option) *Notifier {
	n := &Notifier{client: client, channel: DefaultChannel}
	for _, o := range opts {
		o(n)
	}
	return n
}

// Publish sends msg to every process subscribed to the channel.
func (n *Notifier) Publish(ctx context.Context, msg []byte) error {
	return n.client.Publish(ctx, n.channel, msg).Err()
}

// Subscribe calls deliver for every message on the channel until ctx is done,
// then returns nil. It returns an error if the subscription cannot be
// established or the connection is lost; flightrec subscribes again.
func (n *Notifier) Subscribe(ctx context.Context, deliver func(msg []byte)) error {
	sub := n.client.Subscribe(ctx, n.channel)
	defer func() { _ = sub.Close() }()
	// A blocked read does not watch ctx, so closing the subscription is what
	// ends it when ctx is done.
	stop := context.AfterFunc(ctx, func() { _ = sub.Close() })
	defer stop()

	// Wait for the subscription to be confirmed so that errors surface here.
	if _, err := sub.Receive(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	for {
		msg, err := sub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		deliver([]byte(msg.Payload))
	}
}
