// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import "net/http"

// TransportOption configures NewTransport. None are defined yet.
type TransportOption interface {
	applyTransport(*transportConfig)
}

type transportConfig struct{}

// NewTransport returns an http.RoundTripper that sends requests through next
// (http.DefaultTransport when nil) and reads FlushHeader from each response.
//
// When the response carries exactly one FlushHeader value that is a built-in
// flush reason (see ValidMarker), and the request context holds an open
// Scope, the scope is triggered with reason "downstream:<value>". If your own
// response headers have not been written yet, your response carries the
// marker further up the call chain.
//
// Any other FlushHeader value, or more than one, is ignored and counted in
// Stats.MarkersRejected of the scope's Recorder, because it comes from
// another service. FlushHeader is removed from every response before it is
// returned, so code that proxies responses cannot forward it.
//
// Only wrap clients that call services you operate. A downstream service can
// make your service keep its logs for a request; it cannot do anything else.
func NewTransport(next http.RoundTripper, opts ...TransportOption) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	var cfg transportConfig
	for _, o := range opts {
		o.applyTransport(&cfg)
	}
	return &transport{next: next}
}

type transport struct {
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

	ReceiveMarker(req.Context(), values)
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
