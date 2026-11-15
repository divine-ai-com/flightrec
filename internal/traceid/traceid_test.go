// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package traceid

import (
	"strings"
	"testing"
)

func TestParseW3CTraceparent(t *testing.T) {
	const tid = "4bf92f3577b34da6a3ce929d0e0e4736"
	for _, tc := range []struct {
		in, want string
	}{
		{"00-" + tid + "-00f067aa0ba902b7-01", tid},
		{"01-" + tid + "-00f067aa0ba902b7-01", tid},
		{"01-" + tid + "-00f067aa0ba902b7-01-extra", tid},
		{"cc-" + tid + "-00f067aa0ba902b7-01-what-the-future-holds", tid},
		{"00-" + tid + "-00f067aa0ba902b7-01-extra", ""}, // version 00 has exactly four fields
		{"01-" + tid + "-00f067aa0ba902b7-01x", ""},      // a later field must follow a dash
		{"ff-" + tid + "-00f067aa0ba902b7-01", ""},       // version ff is invalid
		{"00-" + strings.ToUpper(tid) + "-00f067aa0ba902b7-01", ""},
		{"00-" + tid + "-0000000000000000-01", ""}, // zero parent ID
		{"00-00000000000000000000000000000000-00f067aa0ba902b7-01", ""},
		{"00-" + tid + "-00f067aa0ba902b7-0g", ""},
		{"0-" + tid + "-00f067aa0ba902b7-01", ""},
		{"00_" + tid + "-00f067aa0ba902b7-01", ""},
		{"", ""},
	} {
		if got := ParseTraceparent(tc.in); got != tc.want {
			t.Errorf("ParseTraceparent(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
