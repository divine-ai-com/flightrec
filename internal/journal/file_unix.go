// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package journal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// Supported reports whether journal files work on this platform.
const Supported = true

// File is a journal backed by a shared memory mapping of a file, so entries
// reach the kernel page cache as they are written and survive the process
// being killed.
type File struct {
	*Writer
	f    *os.File
	data []byte
}

// Create creates a journal file of h.Size bytes at path and maps it. The file
// must not exist. It is created with mode 0600, written with zeros in full so
// no page is sparse, and locked with flock so a second writer gets an error.
func Create(path string, h Header) (*File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600) //nolint:gosec // G304: the path is the journal path the application configured
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*File, error) {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(fmt.Errorf("journal: lock %s: %w", path, err))
	}
	// Write zeros through the file rather than truncating it, so every block
	// is allocated now. A full disk fails here instead of as SIGBUS later.
	zeros := make([]byte, 1<<20)
	for left := h.Size; left > 0; {
		n := min(left, int64(len(zeros)))
		if _, err := f.Write(zeros[:n]); err != nil {
			return fail(fmt.Errorf("journal: preallocate %s: %w", path, err))
		}
		left -= n
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(h.Size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fail(fmt.Errorf("journal: map %s: %w", path, err))
	}
	w, err := NewWriter(data, h)
	if err != nil {
		_ = syscall.Munmap(data)
		return fail(err)
	}
	return &File{Writer: w, f: f, data: data}, nil
}

// Close stops writing, zeros the journal if zero is true (a clean stop leaves
// nothing to recover and no data at rest), unmaps and closes the file.
func (j *File) Close(zero bool) error {
	j.disabled.Store(true)
	for i := range j.stripes {
		j.stripes[i].mu.Lock() // wait for appends in progress
	}
	if zero {
		j.protect(func() { clear(j.data) })
	}
	for i := range j.stripes {
		j.stripes[i].mu.Unlock()
	}
	err := syscall.Munmap(j.data)
	if cerr := j.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// ErrNotPrivate means a journal file is not a regular file owned by the
// current user.
var ErrNotPrivate = errors.New("journal: not a regular file owned by the current user")

// ReadFile reads a journal file without mapping it, after checking that it
// is a regular file owned by the current user and not larger than maxSize.
func ReadFile(path string, maxSize int64) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0) //nolint:gosec // G304: the path is the journal path the application configured
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !fi.Mode().IsRegular() || !ok || int(st.Uid) != os.Getuid() {
		return nil, ErrNotPrivate
	}
	if fi.Size() > maxSize {
		return nil, fmt.Errorf("journal: %s is %d bytes, more than %d", path, fi.Size(), maxSize)
	}
	return io.ReadAll(io.LimitReader(f, maxSize))
}

// AcquireLock creates the lock file at path if needed (mode 0600, never
// through a symlink) and takes an exclusive lock on it. unlock releases it;
// the file stays.
func AcquireLock(path string) (unlock func(), err error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600) //nolint:gosec // G304: the path is derived from the journal path the application configured
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("journal: %s is in use: %w", path, err)
	}
	return func() { _ = f.Close() }, nil
}
