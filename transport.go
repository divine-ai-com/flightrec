// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import "net/http"

// TransportOption configures NewTransport. No options are defined yet; the
// parameter exists so policies can be added without changing the signature.
type TransportOption interface {
	applyTransport(*transportConfig)
}

type transportConfig struct{}

// NewTransport returns an http.RoundTripper that sends requests through next
// (http.DefaultTransport when nil) and reads FlushHeader from each response.
//
// When the response carries a built-in flush reason and the request context
// holds a Scope, the scope is triggered with reason "downstream:<value>", so a
// failure in a service you call keeps your logs for the same request too, and
// your own response carries the marker further up the call chain. A value
// that is not a built-in reason is ignored and counted in
// Stats.MarkersRejected, because it comes from another service.
//
// FlushHeader is removed from the response before it is returned, so code
// that proxies responses cannot forward it.
//
// Only wrap clients that call services you operate. A downstream service can
// make your service keep its logs for a request; it cannot do anything else.
func NewTransport(rec *Recorder, next http.RoundTripper, opts ...TransportOption) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	var cfg transportConfig
	for _, o := range opts {
		o.applyTransport(&cfg)
	}
	return &transport{rec: rec, next: next}
}

type transport struct {
	rec  *Recorder
	next http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(req)
	if resp == nil {
		return resp, err
	}
	values := resp.Header.Values(FlushHeader)
	if len(values) == 0 {
		return resp, err
	}
	resp.Header.Del(FlushHeader)

	sc := scopeFromContext(req.Context())
	if sc == nil {
		return resp, err
	}
	st := sc.state
	if t.rec != nil {
		st = t.rec.state
	}
	value, ok := MarkerValue(values[0])
	if !ok || values[0] != value {
		st.markersRejected.Add(1)
		return resp, err
	}
	sc.Trigger("downstream:" + value)
	st.downstreamTriggers.Add(1)
	return resp, err
}

// CloseIdleConnections forwards to next when it supports it, so
// http.Client.CloseIdleConnections keeps working through the wrapper.
func (t *transport) CloseIdleConnections() {
	type closeIdler interface{ CloseIdleConnections() }
	if c, ok := t.next.(closeIdler); ok {
		c.CloseIdleConnections()
	}
}
