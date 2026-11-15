# Contributing to flightrec

Thanks for your interest in improving flightrec. This guide covers how to propose a change and what a pull request needs before it can be merged.

## Before you start

- **Bugs:** open an issue with the bug report form, including a minimal reproduction.
- **Features:** open an issue with the feature request form first, so the design can be agreed before you write code.
- **Security issues:** do not open a public issue. Follow [SECURITY.md](SECURITY.md).

Issues labeled [`good first issue`](https://github.com/divine-ai-com/flightrec/labels/good%20first%20issue) are a good place to start.

## Ground rules

1. **One promise.** Full debug detail for failed requests, one line for healthy ones. Features that do not serve it wait for a later version.
2. **No dependencies in the core module.** The root package uses only the standard library. Integrations that need a dependency live in their own module, like `grpcflightrec` and `otelflightrec`.
3. **Numbers for performance claims.** A change to the buffering, flushing or pass-through path includes `benchstat` output from before and after.
4. **Race-clean.** All tests pass with `-race`.

## Repository layout

| Path | Contents |
| --- | --- |
| `*.go` at the root | The core module: `Recorder`, scopes, HTTP middleware, notifier hub, journal |
| `internal/journal/` | The crash journal file format, writer and reader |
| `grpcflightrec/`, `otelflightrec/` | gRPC interceptors; OpenTelemetry span processor and log bridge (separate modules) |
| `notifier/` | Notifiers: `memnotifier` (core module), `redisnotifier` and `natsnotifier` (separate modules) |
| `examples/`, `cmd/demo/` | Runnable examples and the volume demo, all run in CI |
| `benchmarks/otelexport/` | otelflightrec benchmarks against a real OTLP exporter (unpublished module) |
| `docs/` | Reference, guides, benchmarks, design and proposals; `docs/media/src` generates the README visuals |

## Development

Requires the two latest Go releases (currently 1.26 and 1.27).

```bash
make test    # every module, with the race detector
make cover   # coverage floors for core and otelflightrec
make lint    # needs golangci-lint v2.15.0
make bench
make demo
```

For benchmark comparisons, run each side with `-count=10` and compare with [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat).

## Pull requests

- Branch from `main` with a name like `fix/short-description` or `feat/short-description`.
- Keep one logical change per PR, closing one issue.
- Title the PR as a [Conventional Commit](https://www.conventionalcommits.org/): `<type>(<scope>): <summary>`, lower case, imperative, no period. Types: `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `build`, `ci`, `chore`. Scopes: `core`, `http`, `grpc`, `otel`, `notifier`, `deps`. The title becomes the commit message on `main`.
- Fill in the PR template: what and why, the issue it closes, and how it was tested.
- Add an entry of one or two lines under `## [Unreleased]` in `CHANGELOG.md`, unless the change is invisible to users; detail belongs in the PR.
- A bug fix includes a test that fails without the fix.
- Resolve every review thread before merge. PRs are squash-merged.

## Releases

Maintainers follow [RELEASING.md](RELEASING.md).
