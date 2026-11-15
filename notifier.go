// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	mrand "math/rand/v2"
	"strings"
	"sync"
	"time"
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
	notifierBatchSize      = 64
	notifierPublishTimeout = time.Second
	notifierPublishRate    = 100 // messages per second
	notifierPublishBurst   = 200
	notifierDeliveryRate   = 50 // remote flushes per second
	notifierDeliveryBurst  = 100
	notifierRecentSize     = 4096
	notifierRecentTTL      = 30 * time.Second
	notifierBackoffMin     = 100 * time.Millisecond
	notifierBackoffMax     = 30 * time.Second
	notifierMessageVersion = 1
	notifierMaxMessageSize = 64 << 10
)

// NotifierOption configures StartNotifier. No options are defined yet; the
// parameter exists so limits can be made configurable without changing the
// signature.
type NotifierOption interface {
	applyNotifier(*notifierConfig)
}

// notifierConfig holds settings from NotifierOption values. It is empty until
// the first option is added.
type notifierConfig struct{}

// notice is one flushed trace in a notifier message.
type notice struct {
	TraceID string `json:"t"`
	Reason  string `json:"r"`
}

// notifierMessage is the wire format. Fields are short because every
// process in a fleet receives every message.
type notifierMessage struct {
	Version int      `json:"v"`
	Origin  string   `json:"o"`
	Notices []notice `json:"n"`
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

	outbox  chan notice
	inbox   chan notice
	publish *tokenBucket
	deliver *tokenBucket

	// active indexes running scopes by trace ID, sharded so that starting
	// and ending scopes on many goroutines does not contend on one lock.
	active [activeShards]activeShard

	mu        sync.Mutex
	published *recentSet // trace IDs this process already announced
	received  *recentSet // trace IDs announced by other processes

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// StartNotifier connects the Recorder to n until ctx is done or stop is
// called. While connected:
//
//   - a scope triggered by a local failure announces its trace ID once;
//     scopes triggered by a downstream marker or a remote notice do not, so
//     notices cannot echo between services, and generated trace IDs, which
//     no other process knows, are not announced;
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
		outbox:    make(chan notice, notifierQueueSize),
		inbox:     make(chan notice, notifierQueueSize),
		publish:   newTokenBucket(notifierPublishRate, notifierPublishBurst, st.now),
		deliver:   newTokenBucket(notifierDeliveryRate, notifierDeliveryBurst, st.now),
		published: newRecentSet(notifierRecentSize, notifierRecentTTL, st.now),
		received:  newRecentSet(notifierRecentSize, notifierRecentTTL, st.now),
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
	return h.received.get(traceID)
}

// scopeRekeyed moves an active scope to its new trace ID.
func (h *notifierHub) scopeRekeyed(sc *Scope, oldID, newID string) {
	h.removeActive(sc, oldID)
	h.scopeStarted(sc, newID)
	// End may have run between the trace ID change and the insert above.
	sc.mu.Lock()
	closed := sc.closed
	sc.mu.Unlock()
	if closed {
		h.removeActive(sc, newID)
	}
}

// scopeEnded removes a scope from the active index.
func (h *notifierHub) scopeEnded(sc *Scope, traceID string, triggered bool, reason string, generated bool) {
	h.removeActive(sc, traceID)
	if triggered {
		// Status and slow-request triggers are decided at End.
		h.localTrigger(traceID, reason, generated)
	}
}

// afterRetain flushes a scope that was retained just after a notice for its
// trace, from another process or a local failure, was handled.
func (h *notifierHub) afterRetain(traceID string) {
	h.mu.Lock()
	reason, ok := h.received.get(traceID)
	if !ok {
		reason, ok = h.published.get(traceID)
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
func (h *notifierHub) localTrigger(traceID, reason string, generated bool) {
	if strings.HasPrefix(reason, "remote:") || strings.HasPrefix(reason, "downstream:") {
		return
	}
	if generated || !validTraceID(traceID) {
		// No other process can know a generated ID.
		return
	}
	value, _ := MarkerValue(reason)
	h.mu.Lock()
	_, already := h.published.get(traceID)
	if !already {
		h.published.add(traceID, value)
	}
	h.mu.Unlock()
	if already {
		return
	}
	if n := h.st.flushRetained(context.Background(), traceID, "remote:"+value); n > 0 {
		h.st.remoteFlushes.Add(uint64(n))
	}
	select {
	case h.outbox <- notice{TraceID: traceID, Reason: value}:
	default:
		h.st.notifierPublishDropped.Add(1)
	}
}

// runPublisher batches queued notices into messages and publishes them.
func (h *notifierHub) runPublisher(ctx context.Context) {
	defer h.wg.Done()
	for {
		var batch []notice
		select {
		case <-ctx.Done():
			return
		case first := <-h.outbox:
			batch = append(batch, first)
		}
	fill:
		for len(batch) < notifierBatchSize {
			select {
			case next := <-h.outbox:
				batch = append(batch, next)
			default:
				break fill
			}
		}
		// Wait for a token rather than dropping: notices keep queueing and
		// batching meanwhile, and the bounded queue does the dropping.
		if !h.publish.wait(ctx) {
			return
		}
	refill:
		for len(batch) < notifierBatchSize {
			select {
			case next := <-h.outbox:
				batch = append(batch, next)
			default:
				break refill
			}
		}
		msg, err := json.Marshal(notifierMessage{Version: notifierMessageVersion, Origin: h.origin, Notices: batch})
		if err != nil {
			h.st.notifierPublishErrors.Add(uint64(len(batch)))
			continue
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
	if len(msg) > notifierMaxMessageSize {
		h.st.notifierRejected.Add(1)
		return
	}
	var m notifierMessage
	if err := json.Unmarshal(msg, &m); err != nil || m.Version != notifierMessageVersion || len(m.Notices) > notifierBatchSize {
		h.st.notifierRejected.Add(1)
		return
	}
	if m.Origin == h.origin {
		return
	}
	for _, n := range m.Notices {
		if !validTraceID(n.TraceID) || !ValidMarker(n.Reason) {
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

func (h *notifierHub) apply(n notice) {
	h.mu.Lock()
	if _, seen := h.received.get(n.TraceID); seen {
		h.mu.Unlock()
		return
	}
	// Recorded even when nothing matches now, so scopes that start or end
	// shortly after are triggered.
	h.received.add(n.TraceID, n.Reason)
	h.mu.Unlock()

	scopes := h.activeScopes(n.TraceID)
	if len(scopes) == 0 && !h.st.hasRetained(n.TraceID) {
		return
	}
	// The budget limits flushing work, not notices about traces this
	// process never saw.
	if !h.deliver.take() {
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

// recentSet remembers keys for a fixed time, up to a fixed count, evicting
// the oldest first. It is not safe for concurrent use.
type recentSet struct {
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

func newRecentSet(limit int, ttl time.Duration, now func() time.Time) *recentSet {
	return &recentSet{ttl: ttl, limit: limit, now: now, items: make(map[string]recentItem)}
}

func (r *recentSet) get(key string) (string, bool) {
	it, ok := r.items[key]
	if !ok || r.now().Sub(it.at) > r.ttl {
		return "", false
	}
	return it.value, true
}

func (r *recentSet) add(key, value string) {
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

// tokenBucket is a simple rate limiter.
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

func newTokenBucket(rate, burst int, now func() time.Time) *tokenBucket {
	return &tokenBucket{rate: float64(rate), burst: float64(burst), tokens: float64(burst), last: now(), now: now}
}

// wait blocks until a token is available or ctx is done, and reports whether
// it got a token.
func (b *tokenBucket) wait(ctx context.Context) bool {
	for !b.take() {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Duration(float64(time.Second) / b.rate)):
		}
	}
	return true
}

func (b *tokenBucket) take() bool {
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
