// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package redisnotifier implements flightrec.Notifier with Redis pub/sub, so
// that a failure in one service flushes the logs other services retained for
// the same trace.
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	n, err := redisnotifier.New(client, redisnotifier.WithChannel("flightrec:prod"))
//	if err != nil {
//		return err
//	}
//	stop := rec.StartNotifier(ctx, n)
//	defer stop()
//
// Redis pub/sub is fire and forget: a process that is disconnected when a
// notice is published does not receive it. That matches flightrec's best
// effort cross-service flush. Plain PUBLISH and SUBSCRIBE are used, which a
// Redis Cluster broadcasts to every node.
package redisnotifier

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultChannel is the Redis channel used when WithChannel is not given.
const DefaultChannel = "flightrec:flush"

// DefaultHealthCheckInterval is how long a subscription may be idle before it
// is checked with a PING.
const DefaultHealthCheckInterval = 30 * time.Second

// Notifier publishes and receives flightrec flush notices on one Redis
// channel. It implements flightrec.Notifier.
type Notifier struct {
	client      redis.UniversalClient
	channel     string
	healthCheck time.Duration
}

// Option configures a Notifier.
type Option func(*Notifier) error

// WithChannel sets the Redis channel. Use a different channel per
// environment so that staging and production sharing a Redis server do not
// flush each other's logs. An empty name is an error.
func WithChannel(channel string) Option {
	return func(n *Notifier) error {
		if channel == "" {
			return errors.New("redisnotifier: empty channel name")
		}
		n.channel = channel
		return nil
	}
}

// WithHealthCheckInterval sets how long a subscription may be idle before it
// is checked with a PING (DefaultHealthCheckInterval by default). A
// subscription that does not answer within a second interval is reported as
// failed, so a connection that died without being closed is detected.
func WithHealthCheckInterval(d time.Duration) Option {
	return func(n *Notifier) error {
		if d <= 0 {
			return errors.New("redisnotifier: health check interval must be positive")
		}
		n.healthCheck = d
		return nil
	}
}

// New returns a Notifier that uses client. The client is not closed by the
// Notifier.
func New(client redis.UniversalClient, opts ...Option) (*Notifier, error) {
	if client == nil {
		return nil, errors.New("redisnotifier: nil client")
	}
	n := &Notifier{client: client, channel: DefaultChannel, healthCheck: DefaultHealthCheckInterval}
	for _, o := range opts {
		if err := o(n); err != nil {
			return nil, err
		}
	}
	return n, nil
}

// Publish sends msg to every process subscribed to the channel.
func (n *Notifier) Publish(ctx context.Context, msg []byte) error {
	return n.client.Publish(ctx, n.channel, msg).Err()
}

// Subscribe calls deliver for every message on the channel until ctx is done,
// then returns nil. It returns an error if the subscription cannot be
// established, the connection fails, or the server stops answering health
// checks; flightrec then subscribes again.
//
// On a connection error go-redis reconnects the subscription before
// returning it; Subscribe closes that subscription and leaves resubscribing
// to flightrec, which costs one extra dial per failure.
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

	awaitingPong := false
	for {
		msg, err := sub.ReceiveTimeout(ctx, n.healthCheck)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			var ne net.Error
			if !errors.As(err, &ne) || !ne.Timeout() {
				return err
			}
			// Idle: check the connection is still alive.
			if awaitingPong {
				return errors.New("redisnotifier: subscription did not answer a health check")
			}
			if err := sub.Ping(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			awaitingPong = true
			continue
		}
		awaitingPong = false
		if m, ok := msg.(*redis.Message); ok {
			deliver([]byte(m.Payload))
		}
	}
}
