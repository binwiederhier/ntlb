# ntfy-lb: build, test, lint. "make" runs fmt-check + vet + tests + a linux/amd64 build.

BINARY  := ntfy-lb

.PHONY: all build test race vet fmt fmt-check lint clean

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

clean:
	rm -rf build
