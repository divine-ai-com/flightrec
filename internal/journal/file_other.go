// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package journal

import "errors"

// Supported reports whether journal files work on this platform.
const Supported = false

var errUnsupported = errors.New("journal: not supported on this platform")

// File is unavailable on this platform.
type File struct{ *Writer }

// Create always fails on this platform.
func Create(string, Header) (*File, error) { return nil, errUnsupported }

// Close does nothing on this platform.
func (*File) Close(bool) error { return nil }

// ErrNotPrivate means a journal file is not a regular file owned by the
// current user.
var ErrNotPrivate = errors.New("journal: not a regular file owned by the current user")

// ReadFile always fails on this platform.
func ReadFile(string, int64) ([]byte, error) { return nil, errUnsupported }

// Class says what is at a path where a journal file is expected.
type Class int

// File classes.
const (
	ClassMissing Class = iota
	ClassSymlink
	ClassJournal
	ClassDead
	ClassOther
)

// Classify reports ClassOther on this platform.
func Classify(string) Class { return ClassOther }

// Recovering is unavailable on this platform.
type Recovering struct{ Header Header }

// OpenRecovering always fails on this platform.
func OpenRecovering(string, int64) (*Recovering, error) { return nil, errUnsupported }

// IncrementAttempts always fails on this platform.
func (*Recovering) IncrementAttempts() (uint32, error) { return 0, errUnsupported }

// Stripe always fails on this platform.
func (*Recovering) Stripe(int, []byte, func(Entry)) ([]byte, ReadStats, error) {
	return nil, ReadStats{}, errUnsupported
}

// Close does nothing on this platform.
func (*Recovering) Close() error { return nil }

// ErrNotJournal means a file is not a journal, so it is left in place.
var ErrNotJournal = errors.New("journal: not a journal file")

// Discard always fails on this platform.
func Discard(string) error { return errUnsupported }

// AcquireLock always fails on this platform.
func AcquireLock(string) (func(), error) { return nil, errUnsupported }
