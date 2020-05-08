## Setup
SHELL := /bin/bash
SRC = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

setup:
	go get github.com/golang/lint/golint
	go get github.com/Songmu/make2help/cmd/make2help

## Benchmark
profile:
	go test -tags=sfdebug -v -run none -bench . -benchtime 3s -benchmem -cpuprofile cpu.out -memprofile mem.out -stderrthreshold=INFO -vmodule=*=2
	@echo "For CPU usage, run 'go tool pprof jsonresultset.test cpu.out'"
	@echo "For Memory usage, run 'go tool pprof -alloc_space jsonresultset.test mem.out'"

## Trace
trace:
	go test -trace trace.out
	@echo "Run 'go tool trace jsonresultset.test trace.out'"

## Lint
lint: setup
	go vet $(SRC)
	for pkg in $$(go list ./... | grep -v vendor); do \
		golint -set_exit_status $$pkg || exit $$?; \
	done

## Format source codes using gfmt
fmt: setup
	@gofmt -l -w $(SRC)

## Show help
help:
	@make2help $(MAKEFILE_LIST)

.PHONY: install run
