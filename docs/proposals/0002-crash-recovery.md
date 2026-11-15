# Proposal 0002: Recover buffered logs after a hard crash (v0.4)

| | |
| --- | --- |
| Status | Accepted, revised after design review |
| Milestone | v0.4.0 |
| Scope | core module only |

## Problem

flightrec holds a request's debug records in memory until the request ends. A panic is already handled: the middleware flushes the scope and re-panics. A hard kill is not: `SIGKILL`, the kernel out-of-memory killer, a container runtime enforcing a memory limit, a Kubernetes grace period expiring after `SIGTERM`, or a fatal runtime error ends the process with no chance to run deferred code. Every buffered record of every in-flight request is lost, and those are the requests an incident is about.

## Goals

- After a hard kill, the next start of the same service writes the buffered records of the requests that were in flight, in order, with their original timestamps and attributes, tagged as recovered, plus one summary line per interrupted request.
- Opt-in. Without it, nothing changes, including performance.
- No sidecar and no new dependency: recovery runs inside the restarted process, using only the standard library.
- Bounded: a fixed-size file, written in full at creation, never growing.
- Corruption-safe: torn writes, stale data from an earlier run, a damaged file, a full disk or a crash during recovery never crash the service, loop, or produce garbage output. Problems are counted, not returned.

## Non-goals

- Surviving loss of the machine or the kernel. Records live in the kernel page cache until written back; flightrec does not `fsync`. A process kill does not lose page cache contents; a machine crash can.
- Recovering scopes that ended, healthy or not, or retained scopes (`Retain`). An entry kind is reserved so retained scopes can be added later.
- Recovering spans (`otelflightrec`).
- Windows in v0.4. The journal uses `syscall.Mmap` (Linux, macOS, BSDs); elsewhere `StartJournal` returns `ErrJournalUnsupported`.
- Network file systems. `flock` emulation and stale handles make NFS unsafe; the docs say so.
- A stable file format. The format is private and may change in any release; on a version mismatch the old file is discarded and counted. The public contract is the attribute and stat names.

## What survives what

| Event | Covered |
| --- | --- |
| `SIGKILL`, OOM kill, container memory limit, grace period expiry after `SIGTERM` | yes |
| Unrecovered panic, `os.Exit`, fatal runtime error | yes |
| Container restart in the same pod with the file on an `emptyDir` volume | yes |
| Pod eviction, rollout, reschedule to another node (`emptyDir` is deleted) | no |
| Machine crash, power loss, kernel panic | no |

I verified the first row on Linux: after `SIGKILL`, all bytes copied into a `MAP_SHARED` mapping were in the file, on disk and on tmpfs.

Two resource caveats, documented: on tmpfs (`emptyDir` with `medium: Memory`, `/dev/shm`) the whole file is memory charged to the container, which pushes towards the OOM kill it guards against; on disk, dirty pages count towards the container's memory limit until written back. Mapped writes are normally plain memory copies, but can block on a page fault after writeback or under dirty-page throttling.

## Design

### Overview

While a journal is running, each scope writes three kinds of entries: a start entry (scope metadata), one entry per buffered record, and an end entry once the scope's outcome is final. After a crash, scopes with a start or record entry but no end entry were in flight. The next start of the service reads the old file, writes those scopes' records, and starts a new journal.

### File layout

```
header        4 KiB   magic, format version, size, block size, stripe count,
                      generation salt (random, per file), writer host name and start time
stripes       N equal regions, one per stripe (default 8)
  blocks      64 KiB each
    block header   32 bytes: logical block number in the stripe, salt
    entries        packed; never cross a block boundary
    pad            fills the unused tail of a block
```

Each entry is:

```
length   uint32   total length, 8-byte aligned, at most block size - 32
crc32c   uint32   seeded with salt and the block's logical number, over the rest
kind     uint8    start | record | end | pad (retained reserved)
scope    uint64   scope ID, unique within the file
payload
```

All integers are little-endian.

### Writing

- A scope always writes to the stripe chosen by its ID, so its entries are ordered. Each stripe has a mutex held only for the copy into the mapping; there is no global lock. (The core already does two global atomic adds per buffered record, so this adds no new kind of contention.)
- An entry that does not fit in the current block is preceded by a pad entry and starts the next block, whose header gets the next logical number. Wrapping reuses the oldest block of the stripe.
- An entry larger than a block is dropped and counted.
- Because the CRC is seeded with the block's logical number, entries left over from an earlier lap of the same physical block fail the check; because it is also seeded with the file's salt, entries from an earlier file fail too. A reader never needs to scan byte by byte, and log payload cannot be crafted into a valid entry.
- The file is written with zeros in full when it is created, so no block is sparse. Each copy into the mapping runs with `debug.SetPanicOnFault(true)`; if the kernel raises `SIGBUS` anyway (I/O error, file truncated by someone else), the panic is recovered, the journal is disabled, and the fault is counted. The service keeps running.
- The write position lives in process memory, not in the file.

### Record encoding

A compact binary encoding of what a flush would write: time (Unix nanoseconds), level, message, trace ID, span ID, and the record's own attributes. Attribute values keep their kind for strings, integers, unsigned integers, floats, booleans, durations and times; `slog.Any` values are encoded as JSON, as retention does. The child logger's `With` attributes and groups are encoded once per child Recorder, into its start entry for a scope, and referenced by a small ID in each record entry, so they are not re-encoded per record.

Encoding happens in `Handle`, outside the scope lock, folded into the existing pass over the record's attributes.

### When the end entry is written

- A healthy scope writes its end entry at `End`: nothing of it needs recovering.
- A triggered scope writes its end entry after its records have been written to `Next`. A crash while they are being written therefore recovers them again on the next start: duplicates, never loss. This covers the panic path, where the middleware flushes and then re-panics.
- A scope evicted by `MaxPerScope` or `MaxTotal` keeps its journal entries; the reader reports truncation from what it actually finds.

### Recovery

`StartJournal` recovers before it starts journaling:

1. Open the path with `O_NOFOLLOW`; require a regular file owned by the current user. Read it with `read(2)`, never by mapping it. If the header is unreadable or the format version differs, discard the file and count it.
2. If `path.recovering` exists, a previous recovery did not finish. Recover it first, counting an attempt in its header; after three attempts, delete it and count it as abandoned. This stops a crash loop caused by recovery itself.
3. Then rename the file at `path` to `path.recovering` and recover it (in place if it cannot be renamed), then create the new journal at `path` (full size, zeros) and start journaling. A file that is not a journal, at either path, is never deleted: `StartJournal` returns an error.
4. Read every stripe of the recovering file: order its blocks by logical number, validate each block header and entry CRC, and stop each block at its first invalid entry. Use the size, block size and stripe count from the old file's header, so a changed `Size` setting does not lose data.
5. For each scope without an end entry, write its records to `Next` in order, with `flightrec.flush_reason=crash_recovery`, `flightrec.recovered=true`, the original trace and span IDs, and the previous writer's host name and start time. If the oldest blocks no longer hold the scope's start, its first record carries `flightrec.truncated=true`. Then write one summary record, "request interrupted by crash", with the scope's method, path, start time and the number of records recovered. A scope with zero surviving records still gets its summary.
6. Zero and delete `path.recovering`.

Recovered records go to `Next` with a background context carrying the recorded trace context, as retained flushes do. Recovery runs synchronously inside `StartJournal`, so it finishes before the service takes traffic.

### API

```go
var ErrJournalUnsupported = errors.New("flightrec: journal not supported on this platform")

// StartJournal recovers the records of scopes that were in flight when the
// previous process using path stopped abruptly, then journals buffered records
// until stop is called.
func (r *Recorder) StartJournal(path string, opts ...JournalOption) (stop func() error, err error)

func WithJournalSize(bytes int64) JournalOption // default 16 MiB, rounded to the page size, 1 MiB to 1 GiB
```

- It returns an error only when no journal can run (platform, path, permissions, disk). Problems with the old file are counted in `Stats`, never returned.
- Calling it again on the same Recorder while a journal is running returns an error.
- `stop` stops journaling, zeros the ring and removes the file. Scopes still open at that point are not recovered, so `stop` belongs after the server has drained its requests. A lock file, `path.lock`, is held while the journal runs.
- The file is created with mode `0600` and locked with `flock`; a second process or Recorder opening the same path gets an error.

### Stats

`JournalRecordsWritten`, `JournalBytesWritten`, `JournalWraps`, `JournalOldestAge` (time span the ring currently covers), `JournalDropped` (entries too large or written while disabled), `JournalFaults`, `JournalRecoveredRecords`, `JournalRecoveredScopes`, `JournalCorruptEntries`, `JournalDiscardedFiles`, `JournalAbandonedRecoveries`.

### Operating it

- Put the file on a volume that survives a container restart and is private to the pod: an `emptyDir` per pod, not a shared volume. Shared volumes leave orphaned files when pods are replaced.
- Size it as journaled bytes per second times the longest request you want to recover. `JournalOldestAge` shows the window you actually get; healthy traffic shares the ring with long-lived scopes such as streams.
- The file holds healthy requests' debug data, before any redaction your handler applies. The directory must be private; the file is zeroed on clean stop and the recovering file is zeroed after recovery.

## Proof

- Format: round trip of every attribute kind; wrap within and across blocks; torn final entry in every stripe; stale entries from an earlier lap and an earlier file; random corruption; oversize entries; changed size settings.
- Fuzzing: any file content produces no panic and only entries that were written.
- Concurrency: many goroutines writing through wraps under `-race`.
- Crash tests in CI on Linux and macOS: a child process writes records under load and is sent `SIGKILL` at a random moment; the restarted process must recover only records that were written, with no garbage. Also: a crash during recovery (the attempt counter), a size change between runs, the file truncated under a running writer (fault handling).
- `examples/crash-recovery`: `kill -9` the service and see the last debug logs of in-flight requests after restart.
- Benchmarks through the Benchmarks workflow, on disk, single-threaded and parallel.

## Risks

| Risk | Response |
| --- | --- |
| Encoding cost on the healthy path | Opt-in; logger attributes encoded once; measured first (step 1) |
| `SIGBUS` on mapped writes | File written in full at creation; `SetPanicOnFault` with recover; journal disables itself |
| Duplicates after a crash during a flush or recovery | Documented: at-least-once, never loss |
| Crash loop caused by recovery | Rename before recovering; three attempts, then abandon |
| Sensitive data on disk | `0600`, `O_NOFOLLOW`, owner check, zeroing, documentation |
| Ring too small for long requests | Size option, `JournalOldestAge`, truncation reported |

## Delivery plan

1. Spike and benchmark: encode records in `Handle` and measure the healthy-path cost against the current path and against `slog.JSONHandler`. If the cost is too high, change the design (for example, journal only scopes open longer than a threshold) before building the rest.
2. `feat(core): add the journal file format, writer and reader` (internal package, fuzzed, concurrency-tested)
3. `feat(core): journal scopes with StartJournal`
4. `feat(core): recover interrupted scopes on StartJournal`
5. `test(core): kill a child process and recover its journal in CI`
6. `docs: add the crash recovery example and README section`
7. Release v0.4.0
