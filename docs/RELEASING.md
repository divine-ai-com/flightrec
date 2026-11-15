# Releasing

This repository holds several Go modules, each versioned and tagged on its own:

| Module | Path | Tag format |
| --- | --- | --- |
| Core | `github.com/divine-ai-com/flightrec` | `vX.Y.Z` |
| gRPC | `github.com/divine-ai-com/flightrec/grpcflightrec` | `grpcflightrec/vX.Y.Z` |
| OpenTelemetry | `github.com/divine-ai-com/flightrec/otelflightrec` | `otelflightrec/vX.Y.Z` |
| Redis notifier | `github.com/divine-ai-com/flightrec/notifier/redisnotifier` | `notifier/redisnotifier/vX.Y.Z` |
| NATS notifier | `github.com/divine-ai-com/flightrec/notifier/natsnotifier` | `notifier/natsnotifier/vX.Y.Z` |

Tags are protected: once pushed, a tag is never moved or deleted. A broken release is fixed with a new patch version and a `retract` directive.

## Order

The submodules depend on the core module. Their `replace` directives point at the local core for development, but consumers ignore `replace`, so each submodule must `require` a core version that is already tagged.

1. Release the core module first if it changed.
2. In each submodule, `require` that core version and run `go mod tidy`.
3. Release the submodules.

The `Submodules build without replace` CI job builds each submodule against the core version it requires. It must pass on `main` before a submodule is tagged.

## Steps

1. Check that every issue in the milestone is closed or moved to the next one.
2. Open a PR titled `chore: release vX.Y.Z` that:
   - renames `## [Unreleased]` in `CHANGELOG.md` to `## [vX.Y.Z] - YYYY-MM-DD`, listing breaking changes first;
   - regenerates the numbers in `README.md` if the hot path changed.
3. Merge it once CI is green.
4. Tag the merge commit on `main` with an annotated tag and push it:

   ```bash
   git switch main && git pull
   git tag -a vX.Y.Z -m vX.Y.Z
   git push origin vX.Y.Z
   ```

   For a submodule, use `git tag -a grpcflightrec/vX.Y.Z -m grpcflightrec/vX.Y.Z`.

5. Create the GitHub release from the tag, using the changelog section as the notes.
6. Verify the release installs from the module proxy in an empty module:

   ```bash
   cd "$(mktemp -d)" && go mod init example.com/check
   go get github.com/divine-ai-com/flightrec@vX.Y.Z
   go get github.com/divine-ai-com/flightrec/otelflightrec@vX.Y.Z
   go build ./...
   ```

7. Request the new version on pkg.go.dev so the documentation is indexed.
8. Close the milestone.
