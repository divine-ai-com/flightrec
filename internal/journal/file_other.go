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

// LockFile always fails on this platform.
func LockFile(string) (func(), error) { return nil, errUnsupported }
