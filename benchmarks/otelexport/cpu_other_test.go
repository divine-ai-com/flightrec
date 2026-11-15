// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package otelexport

import "time"

// processCPU is not measured on this platform.
func processCPU() (time.Duration, bool) { return 0, false }
