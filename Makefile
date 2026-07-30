# raft-kv — Makefile
# Uses the Walmart sysproxy for module fetches when needed.

GOPATH_BIN := $(shell go env GOPATH)/bin
PROXY_ENV  := HTTPS_PROXY=http://sysproxy.wal-mart.com:8080 HTTP_PROXY=http://sysproxy.wal-mart.com:8080

.PHONY: all build test race vet fmt cover proto tidy demo install clean

all: vet test build

build:
	go build -o bin/raftkvd ./cmd/raftkvd
	go build -o bin/raftkvctl ./cmd/raftkvctl

test:
	go test ./... -count=1 -timeout 180s

race:
	go test ./... -race -count=1 -timeout 300s

vet:
	go vet ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './internal/transport/pb/*')

cover:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out | tail -1

# Regenerate gRPC stubs from proto/raftkv.proto.
proto:
	PATH="$(PATH):$(GOPATH_BIN)" protoc \
		--go_out=. --go_opt=module=github.com/prashant/raft-kv \
		--go-grpc_out=. --go-grpc_opt=module=github.com/prashant/raft-kv \
		proto/raftkv.proto

tidy:
	$(PROXY_ENV) go mod tidy

demo: build
	./scripts/demo.sh

install: build
	install -d $(GOPATH_BIN)
	install -m 755 bin/raftkvd bin/raftkvctl $(GOPATH_BIN)/

clean:
	rm -rf bin data coverage.out coverage.html
