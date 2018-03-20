PACKAGE  = github.com/Azure/azure-event-hubs-go/internal/test/testhub
DATE    ?= $(shell date +%FT%T%z)
VERSION ?= $(shell git rev-list -1 HEAD)
BIN      = $(GOPATH)/bin
BASE     = $(GOPATH)/src/$(PACKAGE)

all: build

build:
	go build -ldflags "-X $(PACKAGE)/cmd.GitCommit=$(VERSION)" -o $(BASE)/bin/testhub