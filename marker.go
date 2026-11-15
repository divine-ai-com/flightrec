// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import "context"

// FlushMarker returns the flush marker value to send on this scope's
// response, and true, if the scope is triggered and Options.DisableFlushMarker
// is not set. It counts the marker in Stats.FlushMarkersSent, so call it only
// when the value will actually be sent.
//
// HTTPMiddleware uses it for FlushHeader. Integrations for other protocols,
// such as grpcflightrec, use it for their own response metadata.
func (s *Scope) FlushMarker() (string, bool) {
	if s == nil || s.state == nil || s.state.opts.DisableFlushMarker {
		return "", false
	}
	triggered, reason := s.TriggerState()
	if !triggered {
		return "", false
	}
	value, _ := MarkerValue(reason)
	s.state.flushMarkersSent.Add(1)
	return value, true
}

// ReceiveMarker applies the flush marker values read from a response to the
// scope in ctx: exactly one built-in value (see ValidMarker) triggers the
// scope with reason "downstream:<value>" and counts in
// Stats.DownstreamTriggers if the scope was open and not yet triggered;
// anything else is counted in Stats.MarkersRejected and ignored. It reports
// whether the scope was triggered. Without a scope in ctx, or with no values,
// it does nothing.
//
// NewTransport uses it for FlushHeader. Integrations for other protocols use
// it for the marker in their response metadata.
func ReceiveMarker(ctx context.Context, values []string) bool {
	if len(values) == 0 {
		return false
	}
	sc := scopeFromContext(ctx)
	if sc == nil {
		return false
	}
	if len(values) != 1 || !ValidMarker(values[0]) {
		sc.state.markersRejected.Add(1)
		return false
	}
	if sc.trigger("downstream:" + values[0]) {
		sc.state.downstreamTriggers.Add(1)
		return true
	}
	return false
}
