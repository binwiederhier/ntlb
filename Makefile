# ntlb: build, test, lint. "make" runs fmt-check + vet + tests + a linux/amd64 build.

BINARY  := ntlb

.PHONY: all build test race vet fmt fmt-check lint bench clean release release-snapshot

all: fmt-check vet test build

build:
	mkdir -p build && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o build/$(BINARY) .

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	test -z "$$(gofmt -l .)"

lint:
	golangci-lint run ./...

# Benchmarks of the request path, 6 runs each, for benchstat: save one run before a change
# (make bench > old.txt) and one after, then compare: benchstat old.txt new.txt
bench:
	go test -run '^$$' -bench . -benchmem -count 6 ./...

# A release from the checked-out tag: the .deb and a tarball on the GitHub release (.goreleaser.yml)
# Built with go.mod's toolchain line, not whatever Go is installed (v0.2.0 was built with an
# older one by accident)
release: export GOTOOLCHAIN := $(shell awk '/^toolchain/ {print $$2}' go.mod)
release: all
	GITHUB_TOKEN=$$(gh auth token) goreleaser release --clean

# The same packages in dist/, without a tag or a GitHub release
release-snapshot:
	goreleaser release --snapshot --clean

clean:
	rm -rf build dist
