# Developer and CI entry points. CI runs these same targets.

MODULES := . grpcflightrec otelflightrec notifier/redisnotifier notifier/natsnotifier
COVER_MIN_CORE := 80
COVERPROFILE := coverage.out

.PHONY: all
all: lint test

.PHONY: lint
lint: ## Run golangci-lint in every module.
	@for m in $(MODULES); do echo "== lint $$m"; (cd $$m && golangci-lint run ./...) || exit 1; done

.PHONY: test
test: ## Run tests with the race detector in every module.
	@for m in $(MODULES); do echo "== test $$m"; (cd $$m && go test -race -shuffle=on ./...) || exit 1; done

.PHONY: cover
cover: ## Write coverage.out for the core module and fail below COVER_MIN_CORE percent.
	go test -race -covermode=atomic -coverprofile=$(COVERPROFILE) .
	@total=$$(go tool cover -func=$(COVERPROFILE) | awk '/^total:/ {sub("%","",$$3); print $$3}'); \
	echo "core coverage: $$total% (minimum $(COVER_MIN_CORE)%)"; \
	awk -v t="$$total" -v m="$(COVER_MIN_CORE)" 'BEGIN { exit (t+0 < m+0) }' || { echo "coverage below minimum"; exit 1; }

.PHONY: bench
bench: ## Run benchmarks in every module.
	@for m in $(MODULES); do echo "== bench $$m"; (cd $$m && go test -run='^$$' -bench=. -benchmem ./...) || exit 1; done

.PHONY: tidy
tidy: ## Fail if go mod tidy would change go.mod or go.sum in any module.
	@for m in $(MODULES); do echo "== tidy $$m"; (cd $$m && go mod tidy -diff) || exit 1; done

.PHONY: vuln
vuln: ## Run govulncheck in every module.
	@for m in $(MODULES); do echo "== vuln $$m"; (cd $$m && govulncheck ./...) || exit 1; done

.PHONY: demo
demo: ## Run the volume and fidelity demo.
	go run ./cmd/demo

.PHONY: help
help:
	@grep -E '^[a-z]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-8s %s\n", $$1, $$2}'
