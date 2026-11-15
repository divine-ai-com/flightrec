// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package flightrec

import "context"

// SendFlushMarker sends this scope's flush marker if the scope is triggered
// and Options.DisableFlushMarker is not set: it calls send with the marker
// value (a built-in reason, see MarkerValue) and reports whether a marker was
// sent. A nil error from send counts in Stats.FlushMarkersSent; an error
// counts in Stats.FlushMarkersMissed. An untriggered scope calls nothing.
//
// HTTPMiddleware uses it to set FlushHeader. Integrations for other
// protocols, such as grpcflightrec, call it where they set response metadata.
func (s *Scope) SendFlushMarker(send func(value string) error) bool {
	if s == nil || s.state == nil || s.state.opts.DisableFlushMarker {
		return false
	}
	triggered, reason := s.TriggerState()
	if !triggered {
		return false
	}
	value, _ := MarkerValue(reason)
	if err := send(value); err != nil {
		s.state.flushMarkersMissed.Add(1)
		return false
	}
	s.state.flushMarkersSent.Add(1)
	return true
}

// ReceiveMarker applies the flush marker values read from a response to the
// scope in ctx: exactly one built-in value (see ValidMarker) triggers the
// scope with reason "downstream:<value>" and counts in
// Stats.DownstreamTriggers if the scope was open and not yet triggered;
// anything else is counted in Stats.MarkersRejected and ignored. It reports
// whether the scope was triggered. Without a scope in ctx, or with no values,
// it does nothing. Integrations for other protocols call it as NewTransport
// does for FlushHeader.
func ReceiveMarker(ctx context.Context, values []string) bool {
	if len(values) == 0 {
		return false
	}
	sc := scopeFromContext(ctx)
	if sc == nil || sc.state == nil {
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
