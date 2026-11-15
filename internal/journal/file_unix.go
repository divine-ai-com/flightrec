// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package journal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
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
		// Synced so the zeros reach the disk, not only the page cache.
		j.protect(func() { clear(j.data) })
		_ = j.f.Sync()
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
// current user, or has more than one link.
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

// Class says what is at a path where a journal file is expected.
type Class int

// File classes.
const (
	ClassMissing Class = iota // nothing there
	ClassSymlink              // a symbolic link; never followed
	ClassJournal              // a private regular file starting with the journal magic
	ClassDead                 // a private regular file starting with zeros: a journal whose creation or zeroing was cut short
	ClassOther                // anything else, left alone
)

// Classify says what is at path, without following a symlink or blocking on
// a FIFO.
func Classify(path string) Class {
	fi, err := os.Lstat(path)
	if err != nil {
		return ClassMissing
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return ClassSymlink
	}
	f, _, err := openPrivate(path, os.O_RDONLY)
	if err != nil {
		return ClassOther
	}
	defer func() { _ = f.Close() }()
	b := make([]byte, len(magic))
	n, _ := io.ReadFull(f, b)
	switch {
	case n == len(magic) && string(b) == magic:
		return ClassJournal
	case !slices.ContainsFunc(b[:n], func(c byte) bool { return c != 0 }):
		return ClassDead
	}
	return ClassOther
}

// openPrivate opens path without following a symlink or blocking on a FIFO,
// and checks that it is a regular file owned by the current user with a
// single link, so a hard link to another file is never written.
func openPrivate(path string, flag int) (*os.File, int64, error) {
	f, err := os.OpenFile(path, flag|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0) //nolint:gosec // G304: the path is derived from the journal path the application configured
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !fi.Mode().IsRegular() || !ok || int(st.Uid) != os.Getuid() || st.Nlink != 1 {
		_ = f.Close()
		return nil, 0, ErrNotPrivate
	}
	return f, fi.Size(), nil
}

// Recovering is a journal file opened for recovery. It is read with
// read(2), one stripe at a time, never mapped.
type Recovering struct {
	f      *os.File
	Header Header
}

// OpenRecovering opens the journal file at path for recovery, checking that
// it is private and its header valid, and that it is not larger than
// maxSize.
func OpenRecovering(path string, maxSize int64) (*Recovering, error) {
	f, size, err := openPrivate(path, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	b := make([]byte, HeaderSize)
	h, err := func() (Header, error) {
		if size > maxSize {
			return Header{}, fmt.Errorf("journal: %s is %d bytes, more than %d", path, size, maxSize)
		}
		if _, err := io.ReadFull(f, b); err != nil {
			return Header{}, ErrBadHeader
		}
		h, err := DecodeHeader(b)
		if err == nil && h.Size > size {
			err = ErrBadHeader
		}
		return h, err
	}()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Recovering{f: f, Header: h}, nil
}

// IncrementAttempts adds one to the recovery attempt counter in the file's
// header and returns the new count. The write reaches the page cache, which
// survives the death of the process.
func (r *Recovering) IncrementAttempts() (uint32, error) {
	if r.Header.Attempts < ^uint32(0) {
		r.Header.Attempts++
	}
	b := make([]byte, HeaderSize)
	encodeHeader(b, r.Header)
	if _, err := r.f.WriteAt(b, 0); err != nil {
		return 0, err
	}
	return r.Header.Attempts, nil
}

// Stripe reads stripe s into buf, growing it as needed, and calls fn for its
// valid entries in write order. Entry payloads point into the returned
// buffer and are valid until the next call.
func (r *Recovering) Stripe(s int, buf []byte, fn func(Entry)) ([]byte, ReadStats, error) {
	off, n := StripeRange(r.Header, s)
	if int64(cap(buf)) < n {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	if _, err := r.f.ReadAt(buf, off); err != nil {
		return buf, ReadStats{}, err
	}
	return buf, WalkStripe(r.Header, buf, s, fn), nil
}

// Close closes the file.
func (r *Recovering) Close() error { return r.f.Close() }

// ErrNotJournal means a file is not a journal, so it is left in place.
var ErrNotJournal = errors.New("journal: not a journal file")

// Discard removes the journal file at path, first overwriting it with zeros
// and syncing them, since it holds unredacted debug data. A symlink is
// removed without being followed. Anything else that is not a journal
// (ClassJournal or ClassDead) is left alone and reported with
// ErrNotJournal.
func Discard(path string) error {
	switch Classify(path) {
	case ClassMissing:
		return nil
	case ClassSymlink:
		return os.Remove(path)
	case ClassJournal, ClassDead:
	default:
		return fmt.Errorf("%w: %s", ErrNotJournal, path)
	}
	if f, size, err := openPrivate(path, os.O_RDWR); err == nil {
		zeros := make([]byte, min(size, 1<<20))
		for off := int64(0); off < size; off += int64(len(zeros)) {
			n := min(int64(len(zeros)), size-off)
			if _, err := f.WriteAt(zeros[:n], off); err != nil {
				break
			}
		}
		_ = f.Sync()
		_ = f.Close()
	}
	return os.Remove(path)
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
