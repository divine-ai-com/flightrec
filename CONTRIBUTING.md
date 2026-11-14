# Contributing to flightrec

Thank you for your interest in contributing to `flightrec`.

## Ground Rules

1. **One promise:** Full debug detail for failed requests, near-zero cost for healthy ones.
2. **Zero external dependencies in the root module:** Keep the core package dependent only on the Go standard library.
3. **Proof with numbers:** Any change to the hot buffering or flushing path must include `go test -bench=. -benchmem` before/after output.
4. **Race-clean:** All pull requests must pass `go test -race ./...`.

## Local Development

```bash
go test -race -v ./...
go test -bench=. -benchmem -run=^$ ./...
go run ./cmd/demo
```
