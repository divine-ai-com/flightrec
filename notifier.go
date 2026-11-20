// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	mrand "math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/haawklabs/flightrec/internal/notify"
	"github.com/haawklabs/flightrec/internal/traceid"
)

// Notifier carries flush notices between processes, so that a failure in one
// service can flush the logs other services kept for the same trace (see
// Options.Retain). flightrec owns the message format; a Notifier only moves
// bytes, so a backend is a thin wrapper around a pub/sub system.
//
// Publish sends msg to every subscribed process, including this one. It
// should return quickly; flightrec calls it from a background goroutine with
// a timeout.
//
// Subscribe calls deliver for every message published by any process until
// ctx is done, then returns nil. It returns an error if the subscription
// fails; flightrec then subscribes again with exponential backoff. deliver
// must not be called after Subscribe returns.
//
// Both methods must return promptly once ctx is done: stopping the notifier
// waits for them.
type Notifier interface {
	Publish(ctx context.Context, msg []byte) error
	Subscribe(ctx context.Context, deliver func(msg []byte)) error
}

const (
	notifierQueueSize      = 1024
	notifierPublishTimeout = time.Second
	notifierPublishRate    = 100 // messages per second
	notifierPublishBurst   = 200
	notifierDeliveryRate   = 50 // remote flushes per second
	notifierDeliveryBurst  = 100
	notifierRecentSize     = 4096
	notifierRecentTTL      = 30 * time.Second
	notifierBackoffMin     = 100 * time.Millisecond
	notifierBackoffMax     = 30 * time.Second
)

// NotifierOption configures StartNotifier.
type NotifierOption interface {
	applyNotifier(*notifierConfig)
}

// notifierConfig holds settings from NotifierOption values.
type notifierConfig struct {
	keys     [][]byte // signing key first, then keys accepted for verification
	unsigned bool     // WithoutSigning
}

const activeShards = 32

type activeShard struct {
	mu     sync.Mutex
	scopes map[string][]*Scope
}

func (h *notifierHub) shard(traceID string) *activeShard {
	// FNV-1a over the trace ID.
	var x uint32 = 2166136261
	for i := 0; i < len(traceID); i++ {
		x ^= uint32(traceID[i])
		x *= 16777619
	}
	return &h.active[x%activeShards]
}

// notifierHub connects a Recorder to a Notifier.
type notifierHub struct {
	st     *recorderState
	n      Notifier
	origin string
	keys   [][]byte // WithSigningKey: signing key first

	outbox  chan notify.Notice
	inbox   chan notify.Notice
	publish *notify.TokenBucket
	deliver *notify.TokenBucket

	// active indexes running scopes by trace ID, sharded so that starting
	// and ending scopes on many goroutines does not contend on one lock.
	active [activeShards]activeShard

	mu        sync.Mutex
	published *notify.RecentSet // trace IDs this process already announced
	received  *notify.RecentSet // trace IDs announced by other processes

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// StartNotifier connects the Recorder to n until ctx is done or stop is
// called. It panics unless opts include WithSigningKey, or WithoutSigning for
// a channel that only trusted processes can publish on. While connected:
//
//   - a scope triggered by a local failure announces its trace ID once;
//     scopes triggered by a downstream marker or a remote notice do not, so
//     notices cannot echo between services. Generated trace IDs are
//     announced too, since services forward them to the services they call;
//   - a notice from another process flushes this process's retained scopes
//     for that trace (Options.Retain) and triggers its running ones, with
//     reason "remote:<reason>"; scopes for that trace that start or end
//     within 30 seconds of the notice are triggered too;
//   - a local failure also flushes retained scopes for the same trace in
//     this process, with reason "remote:<reason>". When the failure is an
//     error log, this happens inside the logging call.
//
// Publishing is queued and rate limited; delivery is rate limited too.
// Drops and errors are counted in Stats. Calling StartNotifier again stops
// the previous notifier first. stop waits for the background goroutines,
// which requires n to honor ctx (see Notifier).
func (r *Recorder) StartNotifier(ctx context.Context, n Notifier, opts ...NotifierOption) (stop func()) {
	var cfg notifierConfig
	for _, o := range opts {
		o.applyNotifier(&cfg)
	}
	if len(cfg.keys) == 0 && !cfg.unsigned {
		panic("flightrec: StartNotifier needs WithSigningKey, or WithoutSigning for a trusted channel")
	}
	st := r.state
	st.hubMu.Lock()
	defer st.hubMu.Unlock()
	if prev := st.hub.Swap(nil); prev != nil {
		prev.stop()
	}
	hctx, cancel := context.WithCancel(ctx)
	h := &notifierHub{
		st:        st,
		n:         n,
		origin:    newOriginID(),
		keys:      cfg.keys,
		outbox:    make(chan notify.Notice, notifierQueueSize),
		inbox:     make(chan notify.Notice, notifierQueueSize),
		publish:   notify.NewTokenBucket(notifierPublishRate, notifierPublishBurst, st.now),
		deliver:   notify.NewTokenBucket(notifierDeliveryRate, notifierDeliveryBurst, st.now),
		published: notify.NewRecentSet(notifierRecentSize, notifierRecentTTL, st.now),
		received:  notify.NewRecentSet(notifierRecentSize, notifierRecentTTL, st.now),
		cancel:    cancel,
	}
	st.hub.Store(h)
	// Detach as soon as ctx is done, so scopes stop being indexed and
	// announcements stop queueing for a notifier that is no longer running.
	context.AfterFunc(hctx, func() { st.hub.CompareAndSwap(h, nil) })
	h.wg.Add(3)
	go h.runPublisher(hctx)
	go h.runSubscriber(hctx)
	go h.runDelivery(hctx)
	var once sync.Once
	return func() {
		once.Do(func() {
			st.hubMu.Lock()
			defer st.hubMu.Unlock()
			st.hub.CompareAndSwap(h, nil)
			h.stop()
		})
	}
}

func (h *notifierHub) stop() {
	h.cancel()
	h.wg.Wait()
}

func newOriginID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// scopeStarted indexes an active scope and triggers it if its trace was
// announced by another process within notifierRecentTTL.
func (h *notifierHub) scopeStarted(sc *Scope, traceID string) {
	s := h.shard(traceID)
	s.mu.Lock()
	if s.scopes == nil {
		s.scopes = make(map[string][]*Scope)
	}
	s.scopes[traceID] = append(s.scopes[traceID], sc)
	s.mu.Unlock()
	if reason, announced := h.announced(traceID); announced && sc.trigger("remote:"+reason) {
		h.st.remoteFlushes.Add(1)
	}
}

// announced returns the reason of a notice received from another process for
// traceID within notifierRecentTTL.
func (h *notifierHub) announced(traceID string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.received.Get(traceID)
}

// scopeRekeyed moves an active scope to its new trace ID, or, when the old ID
// stays an alias, indexes it under both.
func (h *notifierHub) scopeRekeyed(sc *Scope, oldID, newID string, keepOld bool) {
	if !keepOld {
		h.removeActive(sc, oldID)
	}
	h.scopeStarted(sc, newID)
	// End may have run between the trace ID change and the insert above.
	sc.mu.Lock()
	closed := sc.closed
	sc.mu.Unlock()
	if closed {
		h.removeActive(sc, newID)
		h.removeActive(sc, oldID)
	}
}

// scopeEnded removes a scope from the active index.
func (h *notifierHub) scopeEnded(sc *Scope, traceID string, triggered bool, reason string) {
	h.removeActive(sc, traceID)
	if triggered {
		// Status and slow-request triggers are decided at End.
		h.localTrigger(traceID, reason)
	}
}

// afterRetain flushes a scope that was retained just after a notice for its
// trace, from another process or a local failure, was handled.
func (h *notifierHub) afterRetain(traceID string) {
	h.mu.Lock()
	reason, ok := h.received.Get(traceID)
	if !ok {
		reason, ok = h.published.Get(traceID)
	}
	h.mu.Unlock()
	if !ok {
		return
	}
	if n := h.st.flushRetained(context.Background(), traceID, "remote:"+reason); n > 0 {
		h.st.remoteFlushes.Add(uint64(n))
	}
}

func (h *notifierHub) removeActive(sc *Scope, traceID string) {
	s := h.shard(traceID)
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.scopes[traceID]
	for i, other := range list {
		if other == sc {
			list[i] = list[len(list)-1]
			list = list[:len(list)-1]
			break
		}
	}
	if len(list) == 0 {
		delete(s.scopes, traceID)
	} else {
		s.scopes[traceID] = list
	}
}

func (h *notifierHub) activeScopes(traceID string) []*Scope {
	s := h.shard(traceID)
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*Scope(nil), s.scopes[traceID]...)
}

// localTrigger announces a locally originated failure once per trace and
// flushes this process's retained scopes for the same trace.
func (h *notifierHub) localTrigger(traceID, reason string) {
	if strings.HasPrefix(reason, "remote:") || strings.HasPrefix(reason, "downstream:") {
		return
	}
	// Generated IDs are announced too: services forward them to the services
	// they call (X-Request-Id, gRPC metadata), which then know them.
	if !traceid.Valid(traceID) {
		return
	}
	value, _ := MarkerValue(reason)
	h.mu.Lock()
	_, already := h.published.Get(traceID)
	if !already {
		h.published.Add(traceID, value)
	}
	h.mu.Unlock()
	if already {
		return
	}
	if n := h.st.flushRetained(context.Background(), traceID, "remote:"+value); n > 0 {
		h.st.remoteFlushes.Add(uint64(n))
	}
	select {
	case h.outbox <- notify.Notice{TraceID: traceID, Reason: value}:
	default:
		h.st.notifierPublishDropped.Add(1)
	}
}

// runPublisher batches queued notices into messages and publishes them.
func (h *notifierHub) runPublisher(ctx context.Context) {
	defer h.wg.Done()
	for {
		var batch []notify.Notice
		select {
		case <-ctx.Done():
			return
		case first := <-h.outbox:
			batch = append(batch, first)
		}
	fill:
		for len(batch) < notify.MaxBatch {
			select {
			case next := <-h.outbox:
				batch = append(batch, next)
			default:
				break fill
			}
		}
		// Wait for a token rather than dropping: notices keep queueing and
		// batching meanwhile, and the bounded queue does the dropping.
		if !h.publish.Wait(ctx) {
			return
		}
	refill:
		for len(batch) < notify.MaxBatch {
			select {
			case next := <-h.outbox:
				batch = append(batch, next)
			default:
				break refill
			}
		}
		msg, err := json.Marshal(notify.Message{Version: notify.Version, Origin: h.origin, Notices: batch})
		if err != nil {
			h.st.notifierPublishErrors.Add(uint64(len(batch)))
			continue
		}
		if len(h.keys) > 0 {
			msg = notify.Sign(h.keys[0], msg, h.st.now())
		}
		pctx, cancel := context.WithTimeout(ctx, notifierPublishTimeout)
		err = h.n.Publish(pctx, msg)
		cancel()
		if err != nil {
			h.st.notifierPublishErrors.Add(uint64(len(batch)))
			continue
		}
		h.st.notifierPublished.Add(uint64(len(batch)))
	}
}

// runSubscriber keeps a subscription open, re-subscribing with backoff.
func (h *notifierHub) runSubscriber(ctx context.Context) {
	defer h.wg.Done()
	backoff := notifierBackoffMin
	for {
		started := time.Now()
		err := h.n.Subscribe(ctx, h.receive)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			h.st.notifierSubscribeErrors.Add(1)
		}
		// A subscription that ran for a while was healthy; start over.
		if err == nil || time.Since(started) > notifierBackoffMax {
			backoff = notifierBackoffMin
		}
		// Up to 20% jitter so a fleet does not reconnect in step.
		wait := backoff + time.Duration(mrand.Int64N(int64(backoff)/5+1)) //nolint:gosec // jitter, not a security value
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if backoff *= 2; backoff > notifierBackoffMax {
			backoff = notifierBackoffMax
		}
	}
}

// receive decodes a message from the Notifier and queues its notices. It
// runs on the backend's goroutine, so it never blocks.
func (h *notifierHub) receive(msg []byte) {
	if len(msg) > notify.MaxSignedSize {
		h.st.notifierRejected.Add(1)
		return
	}
	if len(h.keys) > 0 {
		payload, ok := notify.Open(h.keys, msg, h.st.now())
		if !ok {
			h.st.notifierRejected.Add(1)
			return
		}
		msg = payload
	}
	if len(msg) > notify.MaxMessageSize {
		h.st.notifierRejected.Add(1)
		return
	}
	var m notify.Message
	if err := json.Unmarshal(msg, &m); err != nil || m.Version != notify.Version || len(m.Notices) > notify.MaxBatch {
		h.st.notifierRejected.Add(1)
		return
	}
	if m.Origin == h.origin {
		return
	}
	for _, n := range m.Notices {
		if !traceid.Valid(n.TraceID) || !ValidMarker(n.Reason) {
			h.st.notifierRejected.Add(1)
			continue
		}
		select {
		case h.inbox <- n:
		default:
			h.st.notifierDeliveryDropped.Add(1)
		}
	}
}

// runDelivery applies remote notices: flush retained scopes and trigger
// active ones for each trace, once per trace per notifierRecentTTL.
func (h *notifierHub) runDelivery(ctx context.Context) {
	defer h.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case n := <-h.inbox:
			h.apply(n)
		}
	}
}

func (h *notifierHub) apply(n notify.Notice) {
	h.mu.Lock()
	if _, seen := h.received.Get(n.TraceID); seen {
		h.mu.Unlock()
		return
	}
	// Recorded even when nothing matches now, so scopes that start or end
	// shortly after are triggered.
	h.received.Add(n.TraceID, n.Reason)
	h.mu.Unlock()

	scopes := h.activeScopes(n.TraceID)
	if len(scopes) == 0 && !h.st.hasRetained(n.TraceID) {
		return
	}
	// The budget limits flushing work, not notices about traces this
	// process never saw.
	if !h.deliver.Take() {
		h.st.notifierDeliveryDropped.Add(1)
		return
	}

	reason := "remote:" + n.Reason
	flushed := uint64(h.st.flushRetained(context.Background(), n.TraceID, reason))
	for _, sc := range scopes {
		if sc.trigger(reason) {
			flushed++
		}
	}
	h.st.remoteFlushes.Add(flushed)
}

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
		if len(k) < notify.MinKeyLen {
			panic("flightrec: notifier signing key shorter than 32 bytes")
		}
	}
	cp := make([][]byte, len(keys))
	for i, k := range keys {
		cp[i] = bytes.Clone(k)
	}
	return signingKeyOption{keys: cp}
}

type withoutSigningOption struct{}

func (withoutSigningOption) applyNotifier(c *notifierConfig) { c.unsigned = true }

// WithoutSigning runs a notifier without message signatures, for tests and
// for channels that only trusted processes can publish on.
func WithoutSigning() NotifierOption { return withoutSigningOption{} }
