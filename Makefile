PACKAGE  = github.com/Azure/azure-event-hubs-go
DATE    ?= $(shell date +%FT%T%z)
VERSION ?= $(shell git describe --tags --always --dirty --match=v* 2> /dev/null || \
			cat $(CURDIR)/.version 2> /dev/null || echo v0)
BIN      = $(GOPATH)/bin
GO_FILES = find . -iname '*.go' -type f | grep -v /vendor/

GO      = go
GODOC   = godoc
GOFMT   = gofmt
GOCYCLO = gocyclo
GOLINT  = $(BIN)/golint
GOSTATICCHECK = $(BIN)/staticcheck

V = 0
Q = $(if $(filter 1,$V),,@)
M = $(shell printf "\033[34;1m▶\033[0m")
TIMEOUT = 360

.PHONY: all
all: fmt lint vet tidy build

.PHONY: build
build: | ; $(info $(M) building library…) @ ## Build program
	$Q $(GO) build all

# Tests

TEST_TARGETS := test-default test-bench test-verbose test-race test-debug test-cover
.PHONY: $(TEST_TARGETS) test-xml check test tests
test-bench:   ARGS=-run=__absolutelynothing__ -bench=. 		## Run benchmarks
test-verbose: ARGS=-v            							## Run tests in verbose mode
test-debug:   ARGS=-v -debug     							## Run tests in verbose mode with debug output
test-race:    ARGS=-race         							## Run tests with race detector
test-cover:   ARGS=-cover -coverprofile=cover.out -v     	## Run tests in verbose mode with coverage
$(TEST_TARGETS): NAME=$(MAKECMDGOALS:test-%=%)
$(TEST_TARGETS): test
check test tests: cyclo lint vet ; $(info $(M) running $(NAME:%=% )tests…) @ ## Run tests
	$Q $(GO) test -timeout $(TIMEOUT)s $(ARGS) ./...

.PHONY: vet
vet: ; $(info $(M) running vet…) @ ## Run vet
	$Q $(GO) vet ./...

.PHONY: tidy
tidy: ; $(info $(M) running go mod tidy…) @ ## Run tidy
	$Q $(GO) mod tidy

.PHONY: lint
lint: ; $(info $(M) running golint…) @ ## Run golint
	$Q $(GOLINT) ./...

.PHONY: staticcheck
staticcheck: ; $(info $(M) running staticcheck…) @ ## Run staticcheck
	$Q $(GOSTATICCHECK) ./...

.PHONY: fmt
fmt: ; $(info $(M) running gofmt…) @ ## Run gofmt on all source files
	@ret=0 && for d in $$($(GO) list -f '{{.Dir}}' ./... | grep -v /vendor/); do \
		$(GOFMT) -l -w $$d/*.go || ret=$$? ; \
	 done ; exit $$ret

.PHONY: cyclo
cyclo: ; $(info $(M) running gocyclo...) @ ## Run gocyclo on all source files
	$Q $(GOCYCLO) -over 19 $$($(GO_FILES))

# Misc

.PHONY: clean
clean: ; $(info $(M) cleaning…)	@ ## Cleanup everything
	@rm -rf test/tests.* test/coverage.*

.PHONY: help
help:
	@grep -E '^[ a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

.PHONY: version
version:
	@echo $(VERSION)