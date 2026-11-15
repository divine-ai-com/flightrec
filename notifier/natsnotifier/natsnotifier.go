// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package natsnotifier implements flightrec.Notifier with NATS core
// publish/subscribe, so that a failure in one service flushes the logs other
// services retained for the same trace.
//
//	nc, err := nats.Connect(nats.DefaultURL)
//	if err != nil {
//		return err
//	}
//	n, err := natsnotifier.New(nc, natsnotifier.WithSubject("flightrec.prod"))
//	if err != nil {
//		return err
//	}
//	stop := rec.StartNotifier(ctx, n)
//	defer stop()
//
// NATS core messaging is fire and forget, which matches flightrec's best
// effort cross-service flush. The NATS client reconnects and resubscribes on
// its own and detects dead connections with its PING interval; messages
// published while a process is disconnected are not delivered to it.
package natsnotifier

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
)

// DefaultSubject is the NATS subject used when WithSubject is not given.
const DefaultSubject = "flightrec.flush"

// deliveryBuffer is the number of received messages held for delivery. When
// it is full, NATS drops further messages for this subscription.
const deliveryBuffer = 1024

// Notifier publishes and receives flightrec flush notices on one NATS
// subject. It implements flightrec.Notifier.
type Notifier struct {
	nc      *nats.Conn
	subject string
}

// Option configures a Notifier.
type Option func(*Notifier) error

// WithSubject sets the NATS subject. Use a different subject per environment
// so that staging and production sharing a NATS server do not flush each
// other's logs. An empty subject is an error.
func WithSubject(subject string) Option {
	return func(n *Notifier) error {
		if subject == "" {
			return errors.New("natsnotifier: empty subject")
		}
		n.subject = subject
		return nil
	}
}

// New returns a Notifier that uses nc. The connection is not closed by the
// Notifier.
func New(nc *nats.Conn, opts ...Option) (*Notifier, error) {
	if nc == nil {
		return nil, errors.New("natsnotifier: nil connection")
	}
	n := &Notifier{nc: nc, subject: DefaultSubject}
	for _, o := range opts {
		if err := o(n); err != nil {
			return nil, err
		}
	}
	return n, nil
}

// Publish sends msg to every process subscribed to the subject.
func (n *Notifier) Publish(_ context.Context, msg []byte) error {
	return n.nc.Publish(n.subject, msg)
}

// Subscribe calls deliver for every message on the subject until ctx is done,
// then returns nil. It returns an error if the subscription cannot be created
// or the connection is closed for good; flightrec then subscribes again.
// deliver is only called from Subscribe's own goroutine, so it is never
// called after Subscribe returns.
func (n *Notifier) Subscribe(ctx context.Context, deliver func(msg []byte)) error {
	closed := n.nc.StatusChanged(nats.CLOSED)
	defer n.nc.RemoveStatusListener(closed)
	if n.nc.IsClosed() {
		return nats.ErrConnectionClosed
	}

	msgs := make(chan *nats.Msg, deliveryBuffer)
	sub, err := n.nc.ChanSubscribe(n.subject, msgs)
	if err != nil {
		return err
	}
	defer func() { _ = sub.Unsubscribe() }()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-closed:
			return nats.ErrConnectionClosed
		case m := <-msgs:
			deliver(m.Data)
		}
	}
}
