// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package otelexport

import (
	"syscall"
	"time"
)

// processCPU returns the CPU time the process has used, user and system,
// across all threads.
func processCPU() (time.Duration, bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, false
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), true
}
