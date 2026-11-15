// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package journal

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestFileRoundTripAndPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	j, err := Create(path, Header{Size: MinSize(2), Stripes: 2, Salt: 5})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 10; i++ {
		j.Append(i, KindRecord, i, seqPayload(i, 0, 32))
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 || fi.Size() != MinSize(2) {
		t.Errorf("mode %v size %d", fi.Mode().Perm(), fi.Size())
	}
	if _, err := Create(path, Header{Size: MinSize(2), Stripes: 2}); err == nil {
		t.Error("Create over an existing file succeeded")
	}
	if tryFlock(t, path) {
		t.Error("the journal file is not locked while the writer runs")
	}
	if err := j.Close(false); err != nil {
		t.Fatal(err)
	}

	data, err := ReadFile(path, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if _, entries, _, err := Read(data); err != nil || len(entries) != 10 {
		t.Fatalf("read %d entries, err %v", len(entries), err)
	}
	if !tryFlock(t, path) {
		t.Error("the journal file is still locked after Close")
	}
	lock := filepath.Join(filepath.Dir(path), "lock")
	unlock, err := AcquireLock(lock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireLock(lock); err == nil {
		t.Error("AcquireLock succeeded twice")
	}
	unlock()
	if unlock, err = AcquireLock(lock); err != nil {
		t.Fatalf("AcquireLock after unlock: %v", err)
	}
	unlock()
}

// tryFlock reports whether an exclusive lock on path can be taken now.
func tryFlock(t *testing.T, path string) bool {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil
}

func TestCloseZeroes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	j, err := Create(path, Header{Size: MinSize(1), Stripes: 1, Salt: 5})
	if err != nil {
		t.Fatal(err)
	}
	j.Append(0, KindRecord, 1, []byte("secret debug data"))
	if err := j.Close(true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range data {
		if b != 0 {
			t.Fatal("file not zeroed on a clean close")
		}
	}
}

func TestReadFileRejectsSymlinkAndLargeFiles(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, make([]byte, HeaderSize), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFile(link, 1<<30); err == nil {
		t.Error("ReadFile followed a symlink")
	}
	if _, err := ReadFile(dir, 1<<30); err == nil {
		t.Error("ReadFile accepted a directory")
	}
	if _, err := ReadFile(target, HeaderSize-1); err == nil {
		t.Error("ReadFile accepted a file over the limit")
	}
}

// TestFaultDisablesWriter truncates the file under a live mapping. Writing to
// the lost pages raises SIGBUS, which must disable the writer instead of
// crashing the process.
func TestFaultDisablesWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	j, err := Create(path, Header{Size: MinSize(1), Stripes: 1, Salt: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close(false) }()
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	if j.Append(0, KindRecord, 1, make([]byte, 64)) {
		t.Fatal("append to a truncated mapping reported success")
	}
	if st := j.Stats(); st.Faults != 1 || !j.Disabled() {
		t.Fatalf("stats %+v disabled %v; want one fault and a disabled writer", st, j.Disabled())
	}
	if j.Append(0, KindRecord, 1, make([]byte, 64)) {
		t.Error("disabled writer accepted an entry")
	}
}

func TestErrNotPrivateIsDistinct(t *testing.T) {
	if errors.Is(ErrNotPrivate, ErrBadHeader) {
		t.Fatal("errors must differ")
	}
}
