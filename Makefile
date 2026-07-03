# prism Makefile — the single interface for build/test/lint tasks.
# Every repetitive task lives here so nobody memorizes go incantations.
# See docs/TESTING.md for what each target covers.

BINARY      := prism
BIN_DIR     := bin
PKG         := github.com/elk-utilities/prism
CMD         := ./cmd/prism
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS     := -s -w -X main.version=$(VERSION)
GOFLAGS     :=
FUZZTIME    ?= 30s
COMPOSE     := docker compose -f deploy/docker-compose.integration.yml

# Static, CGO-free build so the artifact runs on bare metal and in scratch/distroless.
export CGO_ENABLED := 0

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the static binary into ./bin/prism
	@mkdir -p $(BIN_DIR)
	go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(BINARY) $(CMD)

.PHONY: test
test: ## Fast tests: unit + golden + fuzz seeds, with the race detector
	CGO_ENABLED=1 go test $(GOFLAGS) -race ./...

.PHONY: lint
lint: ## Run golangci-lint (config in .golangci.yml)
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found — https://golangci-lint.run/welcome/install/"; exit 1; }
	golangci-lint run ./...

.PHONY: tidy
tidy: ## go mod tidy and fail if it produced a diff (CI-safe)
	go mod tidy
	@git diff --exit-code go.mod go.sum || { echo "go.mod/go.sum not tidy — commit the changes"; exit 1; }

.PHONY: cover
cover: ## Coverage report (coverage.txt + coverage.html)
	go test $(GOFLAGS) -coverprofile=coverage.txt -covermode=atomic ./...
	go tool cover -html=coverage.txt -o coverage.html
	@go tool cover -func=coverage.txt | tail -n 1

.PHONY: bench
bench: ## Hot-path benchmarks (prints allocs/op and bytes/op)
	go test $(GOFLAGS) -run '^$$' -bench . -benchmem ./...

.PHONY: fuzz
fuzz: ## Longer fuzz soak (override with FUZZTIME=2m). Runs each Fuzz target it finds.
	@for pkg in $$(go list ./... ); do \
		for fn in $$(go test -list '^Fuzz' $$pkg 2>/dev/null | grep '^Fuzz' || true); do \
			echo ">> $$pkg $$fn"; \
			go test $$pkg -run '^$$' -fuzz "^$$fn$$" -fuzztime=$(FUZZTIME) || exit 1; \
		done; \
	done

.PHONY: integration
integration: ## Integration layer only: compose up -> tagged tests -> down
	@if [ -z "$$(find test/integration -name '*.go' 2>/dev/null)" ]; then \
		echo "integration: no tests under test/integration yet — skipping"; \
	else \
		command -v docker >/dev/null 2>&1 || { echo "docker required for integration tests"; exit 1; }; \
		$(COMPOSE) up -d --wait; \
		trap '$(COMPOSE) down -v' EXIT; \
		go test $(GOFLAGS) -tags integration ./test/integration/...; \
	fi

.PHONY: e2e
e2e: ## End-to-end pipeline tests (build tag: e2e)
	@if [ -z "$$(find test/e2e -name '*.go' 2>/dev/null)" ]; then \
		echo "e2e: no tests under test/e2e yet — skipping"; \
	else \
		go test $(GOFLAGS) -tags e2e ./test/e2e/...; \
	fi

.PHONY: full-tests
full-tests: lint test integration e2e ## The phase-completion gate: everything
	@echo "full-tests: OK"

.PHONY: golden-update
golden-update: ## Regenerate golden fixtures (REVIEW the diff before committing)
	go test $(GOFLAGS) ./... -update

.PHONY: docker
docker: ## Build the container image (tag: prism:$(VERSION))
	docker build -t prism:$(VERSION) --build-arg VERSION=$(VERSION) .

.PHONY: clean
clean: ## Remove build + coverage artifacts
	rm -rf $(BIN_DIR) coverage.txt coverage.html
