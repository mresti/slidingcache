# Makefile - Go library
#
# Tools (golangci-lint, etc.) live in their own module under tools/, so
# their dependencies never pollute the library's go.mod. `make tools`
# installs them into $(go env GOPATH)/bin (see GOBIN below).

.DEFAULT_GOAL := help

# Allows overriding flags: make test TESTFLAGS="-run TestX"
TESTFLAGS ?= -race -cover

GOBIN ?= $(shell go env GOPATH)/bin

# Fuzz knobs: FUZZTIME per target, FUZZ picks one target for test-fuzz-one/long.
FUZZTIME ?= 10s
FUZZ ?= FuzzStoreGetInvariants

.PHONY: help
help: ## Shows this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: tools
tools: ## Installs the tools declared in tools/go.mod (golangci-lint, etc.)
	cd tools && go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint

.PHONY: fmt
fmt: ## Formats the code
	go fmt ./...

.PHONY: vet
vet: ## Basic static analysis
	go vet ./...

.PHONY: lint
lint: ## Runs golangci-lint (run `make tools` first)
	$(GOBIN)/golangci-lint run

.PHONY: lint-fix
lint-fix: ## golangci-lint with auto-fix (run `make tools` first)
	$(GOBIN)/golangci-lint run --fix

.PHONY: test
test: ## Runs the tests
	go test $(TESTFLAGS) ./...

# Extra flags forwarded to every benchmark command (e.g. BENCHFLAGS="-benchtime=100ms").
BENCHFLAGS ?=

.PHONY: test-bench
test-bench: ## Runs benchmarks: serial ones at -cpu=1, parallel ones at -cpu=1,4,8
	go test -run '^$$' -bench=. -benchmem -cpu=1 $(BENCHFLAGS)
	go test -run '^$$' -bench '^BenchmarkParallel' -benchmem -cpu=1,4,8 $(BENCHFLAGS)

.PHONY: test-bench-stat
test-bench-stat: ## benchstat summary: serial benches at -cpu=1, parallel ones at -cpu=4, both -count=10 (no go.mod pollution)
	@serial=$$(mktemp); parallel=$$(mktemp); \
	go test -run '^$$' -bench '^Benchmark(Store|Get|Sweep|Memory)' -benchmem -count=10 -cpu=1 $(BENCHFLAGS) | tee $$serial; \
	go test -run '^$$' -bench '^BenchmarkParallel' -benchmem -count=10 -cpu=4 $(BENCHFLAGS) | tee $$parallel; \
	go run golang.org/x/perf/cmd/benchstat@latest $$serial; \
	go run golang.org/x/perf/cmd/benchstat@latest $$parallel; \
	rm -f $$serial $$parallel

.PHONY: test-fuzz
test-fuzz: ## Replays Fuzz* seed + saved corpus only (fast, deterministic, safe for CI)
	go test -run '^Fuzz' -v .

.PHONY: test-fuzz-one
test-fuzz-one: ## Actually fuzzes one target for FUZZTIME (make test-fuzz-one FUZZ=FuzzStoreGetInvariants FUZZTIME=1m)
	go test -run '^$$' -fuzz "^$(FUZZ)$$" -fuzztime $(FUZZTIME) .

.PHONY: test-fuzz-all
test-fuzz-all: ## Fuzzes every Fuzz* target for FUZZTIME each, one after another (make test-fuzz-all FUZZTIME=1m)
	@for f in $$(go test -list '^Fuzz' . | grep '^Fuzz'); do \
		echo "==> $$f ($(FUZZTIME))"; \
		go test -run '^$$' -fuzz "^$$f$$" -fuzztime $(FUZZTIME) . || exit 1; \
	done

.PHONY: build
build: ## Builds all packages
	go build ./...

.PHONY: tidy
tidy: ## Tidies go.mod / go.sum dependencies
	go mod tidy
	cd tools && go mod tidy

.PHONY: check
check: fmt vet lint test ## Full local pipeline (fmt + vet + lint + tests)

.PHONY: ci
ci: vet lint test ## CI pipeline (no reformatting)
